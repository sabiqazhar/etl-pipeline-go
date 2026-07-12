package lifecycle

import (
	"context"
	"sync"
)

// Lifecycle is the universal contract for all pipeline components.
// Transitions: Init (NEW->IDLE) -> Run (IDLE->RUNNING) -> Drain (RUNNING->DRAINING) -> Close (DRAINING->CLOSED)
type Lifecycle interface {
	Init(context.Context) error
	Run(context.Context) error
	Drain(context.Context) error
	Close() error
}

// StateManager provides thread-safe state transition management.
// Components can embed this to automatically satisfy state rules.
type StateManager struct {
	mu    sync.RWMutex
	state State
}

// NewStateManager creates a new StateManager starting in NEW state.
func NewStateManager() *StateManager {
	return &StateManager{state: StateNew}
}

// CurrentState returns the current state safely.
func (sm *StateManager) CurrentState() State {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state
}

// Transition attempts to move to the next state. Returns error if invalid.
func (sm *StateManager) Transition(next State) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.state == StateClosed && next == StateClosed {
		// idempotency rule: Close() can be called multiple time safely
		return nil
	}

	if !sm.state.CanTransition(next) {
		return &ErrInvalidTransition{From: sm.state, To: next}
	}

	sm.state = next
	return nil
}
