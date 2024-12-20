package node

import (
	"fmt"
	"net/rpc"
)

func (n *Node) rpcCall(nodeID uint64, name string, req, res any) error {
	n.mu.Lock()
	c := n.cluster[nodeID]
	var err error
	if c.rpcClient == nil {
		if n.cluster[nodeID].rpcClient, err = rpc.DialHTTP("tcp", c.Address); err != nil {
			n.mu.Unlock()
			return fmt.Errorf("can't dial tcp conn: %w", err)
		}
		c.rpcClient = n.cluster[nodeID].rpcClient
	}
	n.mu.Unlock()

	if err = c.rpcClient.Call(name, req, res); err != nil {
		return fmt.Errorf("can't perform rpc call: %w", err)
	}

	return nil
}
