package node

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/vhlebnikov/raft/internal/statemachine"
)

const (
	_chunkSizeBytes = 1024
)

type StateMachine interface {
	Apply(cmd *statemachine.Command) (string, error)
}

type Entry struct {
	Command  statemachine.Command `json:"command"`
	Term     uint64               `json:"term"`
	resultCh chan statemachine.ApplyResult
}

type RPCMessage struct {
	Term uint64
}

type ClusterMember struct {
	ID         uint64
	Address    string
	nextIndex  uint64 // the next likely message to send to the follower
	matchIndex uint64 // the last confirmed message stored on the follower
	votedFor   uint64 // Who was voted for in the most recent term
	rpcClient  *rpc.Client
}

type State string

const (
	Leader    State = "leader"
	Candidate State = "candidate"
	Follower  State = "follower"
)

type Node struct {
	server *http.Server // for serving rpc requests
	logger zerolog.Logger
	mu     sync.Mutex

	currentTerm uint64 // current term
	log         []Entry

	id                uint64        // Unique identifier for this Server
	address           string        // The TCP address for RPC
	electionInterval  time.Duration // When to start elections after no append entry messages
	elTicker          *time.Ticker
	heartbeatInterval time.Duration // How often to send empty messages
	hbTicker          *time.Ticker
	sm                *statemachine.StateMachine
	metadataDir       string   // Metadata directory
	fd                *os.File // For restore purposes

	commitIndex uint64                    // Index of the highest log entry known to be committed
	lastApplied uint64                    // Index of the highest log entry applied to state machine
	state       State                     // Leader, candidate or follower
	cluster     map[uint64]*ClusterMember // Nodes in cluster including self
}

func NewNode(
	selfID uint64,
	clusterCfg []ClusterMember,
	heartbeatInterval time.Duration,
	metadataDir string,
	logger zerolog.Logger,
) *Node {
	cluster := make(map[uint64]*ClusterMember, len(clusterCfg))
	for _, c := range clusterCfg {
		if c.ID == 0 {
			logger.Fatal().Msg("cluster node id mustn't be zero")
		}
		cluster[c.ID] = &c
	}

	return &Node{
		logger:            logger.With().Str("id", strconv.FormatUint(selfID, 10)).Logger(),
		id:                selfID,
		address:           cluster[selfID].Address,
		heartbeatInterval: heartbeatInterval,
		sm:                statemachine.NewStateMachine(),
		metadataDir:       metadataDir,
		state:             Follower,
		cluster:           cluster,
	}
}

func (n *Node) getVotedFor(nodeID uint64) uint64 {
	c, ok := n.cluster[nodeID]
	if !ok {
		n.logger.Warn().Msgf("can't get 'voted for' value for id: %s", n.id)
		return 0
	}

	return c.votedFor
}

func (n *Node) setVotedFor(nodeID, votedForID uint64) {
	if _, ok := n.cluster[nodeID]; !ok {
		n.logger.Error().Msgf("can't set 'voted for' value for id: %s", n.id)
		return
	}
	n.cluster[nodeID].votedFor = votedForID
	return
}

func (n *Node) updateTerm(msg RPCMessage) bool {
	if msg.Term > n.currentTerm {
		n.currentTerm = msg.Term
		n.state = Follower
		n.setVotedFor(n.id, 0)

		if err := n.saveMetadata(); err != nil {
			n.logger.Err(err).Msg("can't save metadata")
			return false
		}

		n.resetElectionTimeout()

		n.logger.Debug().Msg("node has begun follower")
		return true
	}
	return false
}

func (n *Node) resetElectionTimeout() {
	hbMs := int(n.heartbeatInterval.Milliseconds())
	n.electionInterval = time.Duration(rand.Intn(hbMs))*time.Millisecond + n.heartbeatInterval*2
	n.logger.Debug().Msgf("new election interval: %d ms", n.electionInterval.Milliseconds())
	n.elTicker.Reset(n.electionInterval)
}

// startElection for every election timeout knocks off
func (n *Node) startElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Debug().Msgf("start new election")
	n.state = Candidate
	n.currentTerm++
	for _, node := range n.cluster {
		if node.ID == n.id {
			n.setVotedFor(n.id, n.id)
			continue
		}
		n.setVotedFor(node.ID, 0)
	}

	n.resetElectionTimeout()
	if err := n.saveMetadata(); err != nil {
		n.logger.Err(err).Msg("can't save metadata")
		return
	}

	n.requestVote()
}

func (n *Node) heartbeatAction() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.logger.Debug().Msg("sending heartbeat")
	n.appendEntries()
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	var votedForThis int
	for _, c := range n.cluster {
		if c.votedFor == n.id {
			votedForThis++
		}
	}

	if votedForThis >= len(n.cluster)/2+1 {
		// must reset all cluster state
		for _, c := range n.cluster {
			c.nextIndex = uint64(len(n.log)) // TODO: вроде же не надо +1
			c.matchIndex = 0
		}
		n.state = Leader
		n.logger.Debug().Msg("became leader")

		// From Section 8 Client Interaction:
		// > First, a leader must have the latest information on
		// > which entries are committed. The Leader
		// > Completeness Property guarantees that a leader has
		// > all committed entries, but at the start of its
		// > term, it may not know which those are. To find out,
		// > it needs to commit an entry from its term. Raft
		// > handles this by having each leader commit a blank
		// > no-op entry into the log at the start of its term.
		n.log = append(n.log,
			Entry{
				Term: n.currentTerm,
				Command: statemachine.Command{
					Type: statemachine.Noop,
				},
			})
		if err := n.saveMetadata(); err != nil {
			n.logger.Err(err).Msg("can't save metadata")
		}

		n.hbTicker.Reset(n.heartbeatInterval)
	}
}

func (n *Node) advanceCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Leader {
		lastLogIndex := uint64(len(n.log) - 1)
		for i := lastLogIndex; i > n.commitIndex; i-- {
			quorum := len(n.cluster)/2 + 1

			for nodeID := range n.cluster {
				if quorum == 0 {
					break
				}
				// leader always has the log
				if nodeID == n.id || n.cluster[nodeID].matchIndex >= i {
					quorum--
				}
			}
			if quorum == 0 {
				n.commitIndex = i
				n.logger.Debug().Msgf("new commit index: %d", i)
				break
			}
		}
	}

	if n.lastApplied <= n.commitIndex {
		entry := n.log[n.lastApplied]
		if entry.Command.Type != statemachine.Noop {
			n.logger.Debug().Msgf("entry applied: %d", n.lastApplied)

			applyResult := n.sm.Apply(&entry.Command)
			if entry.resultCh != nil {
				entry.resultCh <- applyResult
			}
		}
		n.lastApplied++
	}
}

func (n *Node) Start(ctx context.Context) error {
	n.elTicker = time.NewTicker(n.heartbeatInterval * 2) // will change after resetElectionTimeout
	defer n.elTicker.Stop()

	n.hbTicker = time.NewTicker(n.heartbeatInterval)
	defer n.hbTicker.Stop()

	n.state = Follower
	if err := n.restoreMetadata(); err != nil {
		return fmt.Errorf("can't restore metadata: %w", err)
	}

	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(n); err != nil {
		return fmt.Errorf("can't register rpc service: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	n.server = &http.Server{
		Handler: mux,
		Addr:    n.address,
	}

	go func() {
		defer n.server.Shutdown(ctx)
		if err := n.server.ListenAndServe(); err != nil {
			n.logger.Fatal().Msgf("can't listen rpc server: %s", err)
		}
	}()

	n.resetElectionTimeout()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.hbTicker.C:
			if n.state == Leader {
				n.heartbeatAction()
			}
		case <-n.elTicker.C:
			if n.state == Follower || n.state == Candidate {
				n.startElection()
			}
		default:
			switch n.state {
			case Leader, Follower:
				n.advanceCommitIndex()
			case Candidate:
				n.becomeLeader()
			}
		}
	}
}
