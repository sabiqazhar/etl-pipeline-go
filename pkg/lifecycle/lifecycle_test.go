package lifecycle

import (
	"testing"
)

func TestStateMachine_Transitions(t *testing.T) {
	tests := []struct {
		name        string
		transitions []State
		wantErr     bool
		errType     error
	}{
		{
			name:        "Happy Path: Full Lifecycle",
			transitions: []State{StateIdle, StateRunning, StateDraining, StateClosed},
			wantErr:     false,
		},
		{
			name:        "Invalid: NEW to RUNNING directly",
			transitions: []State{StateRunning},
			wantErr:     true,
			errType:     &ErrInvalidTransition{},
		},
		{
			name:        "Invalid: RUNNING to CLOSED directly (must drain first)",
			transitions: []State{StateIdle, StateRunning, StateClosed},
			wantErr:     true,
			errType:     &ErrInvalidTransition{},
		},
		{
			name:        "Recovery Path: RUNNING back to IDLE",
			transitions: []State{StateIdle, StateRunning, StateIdle, StateRunning, StateDraining, StateClosed},
			wantErr:     false,
		},
		{
			name:        "Invalid: CLOSED to anything",
			transitions: []State{StateIdle, StateRunning, StateDraining, StateClosed, StateIdle},
			wantErr:     true,
			errType:     &ErrInvalidTransition{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := NewStateManager()

			// Verify initial state
			if sm.CurrentState() != StateNew {
				t.Fatalf("expected initial state NEW, got %s", sm.CurrentState())
			}

			var lastErr error
			for _, nextState := range tt.transitions {
				lastErr = sm.Transition(nextState)
				if lastErr != nil {
					break // Stop on first error to check if it matches expectation
				}
			}

			if (lastErr != nil) != tt.wantErr {
				t.Errorf("Transition() error = %v, wantErr %v", lastErr, tt.wantErr)
			}

			if tt.wantErr && lastErr != nil {
				if _, ok := lastErr.(*ErrInvalidTransition); !ok {
					t.Errorf("Expected ErrInvalidTransition, got %T", lastErr)
				}
			}
		})
	}
}

func TestStateMachine_CloseIdempotency(t *testing.T) {
	sm := NewStateManager()

	// Fast forward to CLOSED state
	_ = sm.Transition(StateIdle)
	_ = sm.Transition(StateRunning)
	_ = sm.Transition(StateDraining)
	_ = sm.Transition(StateClosed)

	// Calling Close/Transition to CLOSED again should NOT return an error
	err := sm.Transition(StateClosed)
	if err != nil {
		t.Errorf("Close() idempotency failed: expected nil error, got %v", err)
	}

	if sm.CurrentState() != StateClosed {
		t.Errorf("Expected state to remain CLOSED, got %s", sm.CurrentState())
	}
}
