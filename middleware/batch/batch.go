/*
Package batch provides a beat.Middleware that pauses after a run for a delay
chosen from how much the run processed and whether it failed - draining a queue
fast while it has work, and backing off once it empties.

	batch.Middleware(func(processed int, err error) time.Duration {
		if processed == 0 {
			return 3 * time.Second
		}

		return 0
	})

That shape is what lets one schedule serve both states. A job polling a queue
would otherwise have to be scheduled for the busy case and waste wake-ups when
it is empty, or for the idle case and lag when it is full.

A zero delay pauses not at all, and a nil strategy is the same as a strategy
that always returns zero. The pause ends early when the loop's context is
cancelled, so a shutdown does not wait it out.

Use middleware/idle instead when only the error matters.
*/
package batch

import (
	"context"
	"time"

	"github.com/uchaloop/beat"
)

// DelayStrategy returns how long to pause after a run, given how many items it
// processed and its error.
type DelayStrategy func(processed int, err error) time.Duration

// Middleware pauses after the wrapped Job returns, for the duration the strategy
// reports. The pause honours context cancellation. A nil strategy is a no-op.
func Middleware(strategy DelayStrategy) beat.Middleware {
	return func(next beat.Job) beat.Job {
		return func(ctx context.Context) (int, error) {
			processed, err := next(ctx)

			if strategy != nil {
				beat.Wait(ctx, strategy(processed, err))
			}

			return processed, err
		}
	}
}
