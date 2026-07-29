package supervisor

import "time"

// ChildConfig holds per-child supervision settings.
type ChildConfig struct {
	Backoff        BackoffPolicy
	MaxFailures    int           // consecutive failures before permanent failure
	DrainTimeout   time.Duration // how long to wait for graceful drain
	HangMultiplier float64       // watchdog timeout = DrainTimeout * HangMultiplier
}

func DefaultChildConfig() ChildConfig {
	return ChildConfig{
		Backoff:        DefaultBackoff(),
		MaxFailures:    5,
		DrainTimeout:   30 * time.Second,
		HangMultiplier: 2.0,
	}
}

// ChildOption is a functional option for configuring a child.
type ChildOption func(*ChildConfig)

// WithBackoff overrides the default backoff policy.
func WithBackoff(bp BackoffPolicy) ChildOption {
	return func(c *ChildConfig) {
		c.Backoff = bp
	}
}

// WithMaxFailures overrides the max consecutive failures threshold.
func WithMaxFailures(n int) ChildOption {
	return func(c *ChildConfig) {
		c.MaxFailures = n
	}
}

// WithDrainTimeout overrides the graceful drain timeout.
func WithDrainTimeout(d time.Duration) ChildOption {
	return func(c *ChildConfig) {
		c.DrainTimeout = d
	}
}
