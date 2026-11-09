package job

import (
	"context"
	"slices"
)

// Handler receives a Result after an attempt. It is the observability seam of
// job: metrics, logging and tracing are built on top of it, so the package
// depends on none of them.
//
// The Runner never calls a Handler. Whoever owns the attempt delivers its
// Result exactly once - a one-shot process straight from Run, a scheduler
// through its own richer record - so there is no hidden second delivery.
type Handler interface {
	Handle(ctx context.Context, r Result)
}

// HandlerFunc adapts an ordinary function to a Handler, like http.HandlerFunc.
type HandlerFunc func(ctx context.Context, r Result)

// Handle calls f. A nil HandlerFunc is a no-op.
func (f HandlerFunc) Handle(ctx context.Context, r Result) {
	if f != nil {
		f(ctx, r)
	}
}

// MakeMultiHandler returns a Handler that passes each Result to every handler in
// turn, skipping nil ones. Handlers run in order and are not isolated: a panic
// in one propagates to the caller, so keep them panic-free.
// The slice is copied, but handler instances remain shared. Concurrent Handle
// calls are safe only if the supplied handlers support them. Typed nil handlers
// are passed through; only nil interfaces are skipped.
func MakeMultiHandler(handlers ...Handler) Handler {
	handlers = slices.Clone(handlers)

	return HandlerFunc(
		func(ctx context.Context, r Result) {
			for _, h := range handlers {
				if h != nil {
					h.Handle(ctx, r)
				}
			}
		},
	)
}
