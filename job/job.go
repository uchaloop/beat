package job

import (
	"context"
	"slices"
)

// Func is one attempt at the work. It reports how many items it processed -
// return 0 when the work is not item-oriented - and an error if the attempt
// failed.
// The count is expected to be non-negative; the runner preserves it as returned
// rather than validating or clamping it.
//
// A Func must respect ctx: the runner bounds it with the configured timeout,
// and whoever owns the process cancels it on shutdown. The bound is
// cooperative, as every deadline in Go is: it cancels the context, it does not
// interrupt the running function. A Func must return when its context is done,
// and must wait for its own goroutines before it returns.
type Func func(ctx context.Context) (int64, error)

// Middleware wraps a Func to add behaviour around it - recovery, context
// enrichment, extra logging. It is classic Go composition: a Middleware
// receives the next Func and returns a non-nil Func that calls it.
//
// A Middleware must keep the one-attempt contract: it may suppress the call to
// next, but must not call it twice or concurrently. Deciding when the next
// attempt happens is the caller's business, not a Middleware's.
type Middleware func(next Func) Func

// chain applies mws around fn so that the first middleware passed to
// WithMiddleware is the outermost layer and therefore runs first. Nil entries
// are skipped.
func chain(fn Func, mws []Middleware) Func {
	for _, mw := range slices.Backward(mws) {
		if mw != nil {
			fn = mw(fn)

			if fn == nil {
				return nil
			}
		}
	}

	return fn
}
