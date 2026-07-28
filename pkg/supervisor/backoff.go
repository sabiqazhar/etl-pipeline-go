package supervisor

import (
	"math"
	"math/rand"
	"time"
)

// define how to calculate delay between restrats
type BackoffPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	Jitter       float64
}

func DefaultBackoff() BackoffPolicy {
	return BackoffPolicy{
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.1,
	}
}

func (bp BackoffPolicy) Delay(attemp int) time.Duration {
	// Exponential: initial * multiplier^attempt
	delay := float64(bp.InitialDelay) * math.Pow(bp.Multiplier, float64(attemp))

	if delay > float64(bp.MaxDelay) {
		delay = float64(bp.MaxDelay)
	}

	if bp.Jitter > 0 {
		jitterRange := delay * bp.Jitter
		delay = delay - jitterRange + (rand.Float64() * 2 * jitterRange)
	}

	return time.Duration(delay)
}
