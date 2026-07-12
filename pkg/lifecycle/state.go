package lifecycle

import "fmt"

// state represent the current state of lifecycle component
type State string

const (
	StateNew      State = "NEW"
	StateIdle     State = "IDLE"
	StateRunning  State = "RUNNING"
	StateDraining State = "DRAINING"
	StateClosed   State = "CLOSED"
)

var validTransitions = map[State][]State{
	StateNew:      {StateIdle},
	StateIdle:     {StateRunning, StateClosed}, // idle can running or closed if aborted
	StateRunning:  {StateIdle, StateDraining},  // idle for recovery, draining for graceful stop
	StateDraining: {StateClosed},
	StateClosed:   {},
}

func (s State) CanTransition(next State) bool {
	allowed, exist := validTransitions[s]
	if !exist {
		return false
	}
	for _, validNext := range allowed {
		if validNext == next {
			return true
		}
	}
	return false
}

func (s State) String() string {
	return string(s)
}

// is returned when the transition is not allowed.
type ErrInvalidTransition struct {
	From State
	To   State
}

func (e *ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid state transition: %s -> %s", e.From, e.To)
}
