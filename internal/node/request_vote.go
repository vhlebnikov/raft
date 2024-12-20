package node

import (
	"fmt"
)

type RequestVoteRequest struct {
	RPCMessage
	CandidateID  uint64 // Candidate requesting vote
	LastLogIndex uint64 // Index of candidate's last log entry
	LastLogTerm  uint64 // Term of candidate's last log entry
}

type RequestVoteResponse struct {
	RPCMessage
	VoteGranted bool // True means candidate received vote
}

/*
Принцип работы:
- сервер не запрашивает голос у самого себя
- асинхронно запрашиваем голоса у остальных серверов
- если в ответе других нод терм больше, чем у текущей, то текущая нода становится follower
- если терм ответа от другой ноды не совпадает с термом запроса, то его нельзя считать актуальным, поэтому он не идет в расчет
- иначе устанавливаем голос от другой ноды за текущую ноду
*/
func (n *Node) requestVote() {
	for _, c := range n.cluster {
		if c.ID == n.id {
			continue
		}

		go func(nodeID uint64) {
			n.mu.Lock()
			n.logger.Debug().Msgf("requesting vote from %d", nodeID)

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: n.currentTerm,
				},
				CandidateID:  n.id,
				LastLogIndex: uint64(len(n.log) - 1),
				LastLogTerm:  n.log[len(n.log)-1].Term,
			}
			n.mu.Unlock()

			var res RequestVoteResponse
			if err := n.rpcCall(nodeID, "Node.HandleRequestVoteRequest", req, &res); err != nil {
				n.logger.Err(err).Msg("can't make rpc Node.HandleRequestVoteRequest call")
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// update term if got more recent term or ignore if got stale term
			if n.updateTerm(res.RPCMessage) || req.Term != res.Term {
				return
			}

			if res.VoteGranted {
				n.logger.Debug().Msgf("vote granted by %d", nodeID)
				n.setVotedFor(nodeID, n.id)
			}
		}(c.ID)
	}
}

// HandleRequestVoteRequest
// Принцип работы:
// - если пришедший запрос на голос имеет терм больше терма текущей ноды, то текущая нода становится follower
// - если пришедший запрос на голос имеет терм меньше терма текущей ноды, то это старый запрос, и голосовать за эту ноду не надо
// - проверяем, что лог ноды, которая запросила голос, хотя бы такой же, как у текущей ноды, и что текущая нода не голосовала за себя
func (n *Node) HandleRequestVoteRequest(req RequestVoteRequest, res *RequestVoteResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.updateTerm(req.RPCMessage)
	n.logger.Debug().Msgf("received vote request from %d", req.CandidateID)

	res.VoteGranted = false
	res.Term = n.currentTerm

	if req.Term < n.currentTerm {
		n.logger.Debug().Msgf("got stale term [%d < %d] from %d node", req.Term, n.currentTerm, req.CandidateID)
		return nil
	}

	lastLogIndex := uint64(len(n.log) - 1)
	lastLogTerm := n.log[lastLogIndex].Term

	// check if request node's log at least equals to current node's log
	logCheck := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	// check that current node didn't vote for anyone except requested node;
	// and after updateTerm() term equals to req term && log check
	if votedFor := n.getVotedFor(n.id); (votedFor == 0 || votedFor == req.CandidateID) && req.Term == n.currentTerm && logCheck {
		n.logger.Debug().Msgf("voted for %d node", req.CandidateID)
		n.setVotedFor(n.id, req.CandidateID)
		res.VoteGranted = true
		n.resetElectionTimeout()
		if err := n.saveMetadata(); err != nil {
			n.logger.Err(err).Msg("can't save metadata")
			return fmt.Errorf("can't save metadata: %w", err)
		}
		return nil
	}

	n.logger.Debug().Msgf("didn't vote for %d node", req.CandidateID)
	return nil
}
