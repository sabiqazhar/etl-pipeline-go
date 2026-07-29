package supervisor

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
)

// mockComponent is a test double for Lifecycle.
type mockComponent struct {
	sm         *lifecycle.StateManager
	runFunc    func(ctx context.Context) error
	initCalled int32
	runCalled  int32
}

func newMockComponent(runFunc func(ctx context.Context) error) *mockComponent {
	return &mockComponent{
		sm:      lifecycle.NewStateManager(),
		runFunc: runFunc,
	}
}

func (m *mockComponent) Init(ctx context.Context) error {
	atomic.AddInt32(&m.initCalled, 1)
	return m.sm.Transition(lifecycle.StateIdle)
}

func (m *mockComponent) Run(ctx context.Context) error {
	atomic.AddInt32(&m.runCalled, 1)
	if err := m.sm.Transition(lifecycle.StateRunning); err != nil {
		return err
	}
	if m.runFunc != nil {
		return m.runFunc(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (m *mockComponent) Drain(ctx context.Context) error {
	return m.sm.Transition(lifecycle.StateDraining)
}

func (m *mockComponent) Close() error {
	return m.sm.Transition(lifecycle.StateClosed)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// Test 1: Child panics → Supervisor restarts with backoff
func TestSupervisor_PanicRecovery(t *testing.T) {
	var callCount int32

	comp := newMockComponent(func(ctx context.Context) error {
		count := atomic.AddInt32(&callCount, 1)
		if count <= 2 {
			panic("simulated panic!")
		}
		// 3rd call: run cleanly until context cancelled
		<-ctx.Done()
		return nil
	})

	sup := New(testLogger())
	sup.AddChild("panic-child", comp,
		WithBackoff(BackoffPolicy{
			InitialDelay: 10 * time.Millisecond, // Fast backoff for tests
			MaxDelay:     50 * time.Millisecond,
			Multiplier:   2.0,
			Jitter:       0,
		}),
		WithMaxFailures(5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := sup.Serve(ctx)
	if err != nil {
		t.Fatalf("Expected nil error (clean exit after recovery), got: %v", err)
	}

	if atomic.LoadInt32(&callCount) < 3 {
		t.Errorf("Expected at least 3 Run calls (2 panics + 1 clean), got %d", callCount)
	}
}

// Test 2: 5 consecutive failures → permanent failure
func TestSupervisor_PermanentFailure(t *testing.T) {
	comp := newMockComponent(func(ctx context.Context) error {
		return errors.New("always fails")
	})

	sup := New(testLogger())
	sup.AddChild("failing-child", comp,
		WithBackoff(BackoffPolicy{
			InitialDelay: 1 * time.Millisecond,
			MaxDelay:     5 * time.Millisecond,
			Multiplier:   1.0,
			Jitter:       0,
		}),
		WithMaxFailures(5),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := sup.Serve(ctx)
	if !errors.Is(err, lifecycle.ErrPermanentFailure) {
		t.Fatalf("Expected ErrPermanentFailure, got: %v", err)
	}

	if atomic.LoadInt32(&comp.runCalled) != 5 {
		t.Errorf("Expected exactly 5 Run calls, got %d", atomic.LoadInt32(&comp.runCalled))
	}
}

// Test 3: Child hangs after ctx cancel → watchdog kills
func TestSupervisor_HangDetection(t *testing.T) {
	comp := newMockComponent(func(ctx context.Context) error {
		// Ignore context cancellation — simulate a hang
		time.Sleep(10 * time.Second)
		return nil
	})

	sup := New(testLogger())
	sup.AddChild("hanging-child", comp,
		WithDrainTimeout(100*time.Millisecond),
		WithMaxFailures(1), // Fail fast for test
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := sup.Serve(ctx)
	elapsed := time.Since(start)

	// Watchdog should trigger at ~200ms (2x 100ms DrainTimeout)
	// Not 10 seconds (the hang duration)
	if elapsed > 2*time.Second {
		t.Errorf("Watchdog took too long: %v (expected ~200ms)", elapsed)
	}

	if err == nil {
		t.Fatal("Expected error from hang detection, got nil")
	}

	t.Logf("Hang detected in %v with error: %v", elapsed, err)
}

// Test 4: Clean shutdown via context cancellation
func TestSupervisor_GracefulShutdown(t *testing.T) {
	comp := newMockComponent(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	sup := New(testLogger())
	sup.AddChild("clean-child", comp)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- sup.Serve(ctx)
	}()

	// Let it run for a bit
	time.Sleep(50 * time.Millisecond)

	// Cancel → should trigger graceful drain
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Expected nil error on graceful shutdown, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Supervisor did not shut down within timeout")
	}
}

// Test 5: Backoff delay calculation
func TestBackoffPolicy_Delay(t *testing.T) {
	bp := BackoffPolicy{
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		Jitter:       0, // No jitter for deterministic test
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 30 * time.Second},  // Capped
		{10, 30 * time.Second}, // Still capped
	}

	for _, tt := range tests {
		got := bp.Delay(tt.attempt)
		if got != tt.expected {
			t.Errorf("Delay(%d) = %v, want %v", tt.attempt, got, tt.expected)
		}
	}
}
