package node

import (
	"fmt"
	"sync"

	"github.com/vhlebnikov/raft/internal/statemachine"
)

const (
	_appendEntriesMaxBatchSize = 4096
)

type AppendEntriesRequest struct {
	RPCMessage
	LeaderID     uint64  // So follower can redirect clients
	PrevLogIndex uint64  // Index of log entry immediately preceding new ones
	PrevLogTerm  uint64  // Term of prevLogIndex entry
	Entries      []Entry // Log entries to store. Empty for heartbeat.
	LeaderCommit uint64  // Leader's commitIndex
}

type AppendEntriesResponse struct {
	RPCMessage
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (n *Node) Apply(commands []statemachine.Command) ([]statemachine.ApplyResult, error) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return nil, fmt.Errorf("can't apply message, only leader can")
	}
	n.logger.Debug().Msgf("start processing %d commands", len(commands))

	resultChs := make([]chan statemachine.ApplyResult, len(commands))
	for i, cmd := range commands {
		resultChs[i] = make(chan statemachine.ApplyResult)
		n.log = append(n.log, Entry{
			Command:  cmd,
			Term:     n.currentTerm,
			resultCh: resultChs[i],
		})
	}

	if err := n.saveMetadata(); err != nil {
		n.mu.Unlock()
		return nil, fmt.Errorf("can't save metadata: %w", err)
	}

	n.mu.Unlock()

	n.logger.Debug().Msg("waiting for entries applying")

	n.appendEntries()

	results := make([]statemachine.ApplyResult, len(commands))
	var wg sync.WaitGroup
	for i, ch := range resultChs {
		wg.Add(1)
		go func(i int, c chan statemachine.ApplyResult) {
			defer wg.Done()
			results[i] = <-c
		}(i, ch)
	}

	wg.Wait()
	return results, nil
}

func (n *Node) appendEntries() {
	for id := range n.cluster {
		if id == n.id {
			continue
		}

		go func(nodeID uint64) {
			n.mu.Lock()

			nextIdx := n.cluster[nodeID].nextIndex
			lastLogIdx := uint64(len(n.log) - 1)
			prevLogIdx := nextIdx - 1
			prevLogTerm := n.log[prevLogIdx].Term

			var entries []Entry
			if lastLogIdx >= nextIdx {
				n.logger.Debug().Msgf("log len: %d, next: %d, node: %d", len(n.log), nextIdx, nodeID)
				entries = n.log[nextIdx:]
			}

			if len(entries) > _appendEntriesMaxBatchSize {
				entries = entries[:_appendEntriesMaxBatchSize]
			}

			req := AppendEntriesRequest{
				RPCMessage: RPCMessage{
					Term: n.currentTerm,
				},
				LeaderID:     n.id,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}

			n.mu.Unlock()

			var res AppendEntriesResponse
			n.logger.Debug().Msgf("sending %d entries to nodeID=%d with %d term", len(entries), nodeID, req.Term)
			if err := n.rpcCall(nodeID, "Node.HandleAppendEntriesRequest", req, &res); err != nil {
				n.logger.Err(err).Msg("can't make rpc Node.HandleAppendEntriesRequest call")
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.updateTerm(res.RPCMessage) || (req.Term != res.Term && n.state == Leader) {
				return
			}

			if res.Success {
				prevIdx := nextIdx
				newNextIdx := max(nextIdx+uint64(len(entries)), 1)
				n.cluster[nodeID].nextIndex = newNextIdx
				n.cluster[nodeID].matchIndex = newNextIdx - 1

				n.logger.Debug().Msgf("messages (%d) accepted for %d node; prev=%d, next=%d, match=%d",
					len(entries), nodeID, prevIdx, n.cluster[nodeID].nextIndex, n.cluster[nodeID].matchIndex)
			} else {
				n.cluster[nodeID].nextIndex = max(n.cluster[nodeID].nextIndex-1, 1)
				n.logger.Debug().Msgf("forced to go back to %d for %d node", n.cluster[nodeID].nextIndex, nodeID)
			}
		}(id)
	}
}

func (n *Node) HandleAppendEntriesRequest(req AppendEntriesRequest, res *AppendEntriesResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.updateTerm(req.RPCMessage)

	// if AppendEntries RPC received from new leader: convert to follower
	if req.Term == n.currentTerm && n.state == Candidate {
		n.state = Follower
	}

	res.Term = n.currentTerm
	res.Success = false

	if n.state != Follower {
		n.logger.Error().Msgf("can't append entries as not follower: current node is %s", n.state)
		return nil
	}

	if req.Term < n.currentTerm {
		n.logger.Debug().Msgf("drop stale request from old leader %d: term %d", req.LeaderID, req.Term)
		return nil
	}

	n.resetElectionTimeout()

	logLen := uint64(len(n.log))
	validPreviousLog := req.PrevLogIndex == 0 || // induction step
		(req.PrevLogIndex < logLen && n.log[req.PrevLogIndex].Term == req.PrevLogTerm)
	if !validPreviousLog {
		n.logger.Warn().Msgf("invalid log")
		return nil
	}

	next := req.PrevLogIndex + 1
	// If an existing entry conflicts with a new
	// one (same index but different terms),
	// delete the existing entry and all that
	// follow it (§5.3)
	if next < uint64(len(n.log)) && n.log[next].Term != req.Term {
		n.log = n.log[:next]
	}
	n.log = append(n.log, req.Entries...)

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, uint64(len(n.log)-1))
	}

	if err := n.saveMetadata(); err != nil {
		n.logger.Err(err).Msg("can't save metadata")
		return nil
	}
	res.Success = true
	return nil
}
