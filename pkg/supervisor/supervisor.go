package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/aula-id/etl-pipeline-go/pkg/lifecycle"
)

// child wraps a Lifecycle component with its supervision state.
type child struct {
	name      string
	component lifecycle.Lifecycle
	config    ChildConfig
	failures  int
}

// Supervisor monitors child Lifecycle instances and applies restart policy on failure.
// Implements dual-layer recovery:
//
//	Layer 1: panic recovery (deferred recover + stack trace + restart with backoff)
//	Layer 2: hang detection (watchdog goroutine with 2x DrainTimeout)
type Supervisor struct {
	mu       sync.Mutex
	children []*child
	logger   *slog.Logger
}

// New creates a new Supervisor.
func New(logger *slog.Logger) *Supervisor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Supervisor{logger: logger}
}

// AddChild registers a Lifecycle component for supervision.
func (s *Supervisor) AddChild(name string, component lifecycle.Lifecycle, opts ...ChildOption) {
	cfg := DefaultChildConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.children = append(s.children, &child{
		name:      name,
		component: component,
		config:    cfg,
	})
}

// Serve starts all children and blocks until the context is cancelled
// or a child reaches permanent failure.
func (s *Supervisor) Serve(ctx context.Context) error {
	s.mu.Lock()
	children := make([]*child, len(s.children))
	copy(children, s.children)
	s.mu.Unlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(children))

	for _, c := range children {
		wg.Add(1)
		go func(c *child) {
			defer wg.Done()
			if err := s.superviseChild(ctx, c); err != nil {
				errCh <- fmt.Errorf("child %q: %w", c.name, err)
			}
		}(c)
	}

	// Wait for all children to finish
	wg.Wait()
	close(errCh)

	// Return the first permanent failure, if any
	for err := range errCh {
		return err
	}
	return nil
}

// superviseChild runs the supervision loop for a single child.
// This is where the dual-layer recovery magic happens.
func (s *Supervisor) superviseChild(ctx context.Context, c *child) error {
	for {
		// Check for shutdown BEFORE starting a new run
		select {
		case <-ctx.Done():
			return s.drainChild(ctx, c)
		default:
		}

		// Run the child (with panic recovery + watchdog)
		err := s.runChildWithPanicRecovery(ctx, c)

		// Clean exit — Run() returned nil
		if err == nil {
			s.logger.Info("child exited cleanly", "child", c.name)
			return nil
		}

		// Only treat as graceful shutdown if the error IS context cancellation.
		// "hang detected", "panic", "connection refused" etc. are FAILURES, not shutdowns.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.logger.Info("child stopped due to context cancellation", "child", c.name)
			return s.drainChild(ctx, c)
		}

		// --- Real failure: apply backoff and restart ---
		c.failures++
		s.logger.Error("child failed",
			"child", c.name,
			"error", err,
			"consecutive_failures", c.failures,
			"max_failures", c.config.MaxFailures,
		)

		if c.failures >= c.config.MaxFailures {
			s.logger.Error("permanent failure reached",
				"child", c.name,
				"failures", c.failures,
			)
			return lifecycle.ErrPermanentFailure
		}

		// Calculate backoff delay
		delay := c.config.Backoff.Delay(c.failures - 1)
		s.logger.Info("restarting child after backoff",
			"child", c.name,
			"delay", delay,
			"attempt", c.failures,
		)

		select {
		case <-time.After(delay):
			// Backoff complete, loop back to restart
		case <-ctx.Done():
			// Shutdown requested during backoff wait
			return s.drainChild(ctx, c)
		}
	}
}

// runChildWithPanicRecovery executes Init → Run with panic recovery.
// Returns the error from Run(), or a wrapped panic error.
func (s *Supervisor) runChildWithPanicRecovery(ctx context.Context, c *child) (err error) {
	// LAYER 1: Catch panics
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			s.logger.Error("PANIC recovered",
				"child", c.name,
				"panic", r,
				"stack", string(stack),
			)
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	// Init
	if err := c.component.Init(ctx); err != nil {
		return fmt.Errorf("init failed: %w", err)
	}

	// LAYER 2: Hang Detection (Watchdog)
	// We run Run() in a separate goroutine so we can detect if it hangs
	// after context cancellation.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	runDone := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				s.logger.Error("PANIC in Run goroutine",
					"child", c.name,
					"panic", r,
					"stack", string(stack),
				)
				runDone <- fmt.Errorf("panic in Run: %v", r)
			}
		}()
		runDone <- c.component.Run(runCtx)
	}()

	// Wait for Run to finish or context to cancel
	select {
	case err := <-runDone:
		return err
	case <-ctx.Done():
		// Context cancelled — Run should return soon.
		// Start watchdog: if Run doesn't return within 2x DrainTimeout, give up.
		runCancel() // Signal Run to stop

		watchdogTimeout := time.Duration(float64(c.config.DrainTimeout) * c.config.HangMultiplier)
		s.logger.Info("watchdog started",
			"child", c.name,
			"timeout", watchdogTimeout,
		)

		select {
		case err := <-runDone:
			return err
		case <-time.After(watchdogTimeout):
			s.logger.Error("WATCHDOG: child hung, abandoning goroutine",
				"child", c.name,
				"timeout", watchdogTimeout,
			)
			return fmt.Errorf("hang detected: Run did not return within %v", watchdogTimeout)
		}
	}
}

// drainChild performs graceful shutdown: Drain → Close.
func (s *Supervisor) drainChild(ctx context.Context, c *child) error {
	drainCtx, drainCancel := context.WithTimeout(context.Background(), c.config.DrainTimeout)
	defer drainCancel()

	s.logger.Info("draining child", "child", c.name, "timeout", c.config.DrainTimeout)

	if err := c.component.Drain(drainCtx); err != nil {
		s.logger.Error("drain failed", "child", c.name, "error", err)
	}

	if err := c.component.Close(); err != nil {
		s.logger.Error("close failed", "child", c.name, "error", err)
	}

	return nil
}
