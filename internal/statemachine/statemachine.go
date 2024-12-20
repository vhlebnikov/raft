package statemachine

import (
	"fmt"
	"sync"
)

type StateMachine struct {
	m *sync.Map
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		m: &sync.Map{},
	}
}

type CommandType string

const (
	SetCommand CommandType = "set"
	GetCommand CommandType = "get"
	Noop       CommandType = "noop"
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value"`
}

type ApplyResult struct {
	Result string `json:"result"`
	Err    error  `json:"err"`
}

func (s *StateMachine) Apply(cmd *Command) ApplyResult {
	if cmd == nil {
		return ApplyResult{
			Err: fmt.Errorf("empty command"),
		}
	}

	switch cmd.Type {
	case GetCommand:
		v, ok := s.m.Load(cmd.Key)
		if !ok {
			return ApplyResult{
				Err: fmt.Errorf("key not found"),
			}
		}
		return ApplyResult{
			Result: v.(string),
			Err:    nil,
		}
	case SetCommand:
		s.m.Store(cmd.Key, cmd.Value)
		return ApplyResult{}
	default:
		return ApplyResult{
			Err: fmt.Errorf("unknown command type: %s", cmd.Type),
		}
	}
}
