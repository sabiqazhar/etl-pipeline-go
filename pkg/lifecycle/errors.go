package lifecycle

import "errors"

var (
	// ErrPermanentFailure indicates the component has failed too many times and should not be restarted.
	ErrPermanentFailure = errors.New("lifecycle: permanent failure reached")

	// ErrNotInitialized indicates an operation was called before Init().
	ErrNotInitialized = errors.New("lifecycle: component not initialized")
)
