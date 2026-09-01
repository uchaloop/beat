/*
Package idle provides a beat.Middleware that pauses after a run for a delay
chosen from the run's error - a backoff on failure, most often.

	idle.Middleware(func(err error) time.Duration {
		if err != nil {
			return 5 * time.Second
		}

		return 0
	})

The delay is decided per run rather than configured once, so a schedule stays
what the deployment set it to and the backoff is a property of what happened.
A zero delay pauses not at all, and a nil strategy is the same as a strategy
that always returns zero, so a middleware assembled conditionally does not have
to be left out.

The pause ends early when the loop's context is cancelled, so a shutdown does
not wait out a backoff.

Use middleware/batch instead when the delay should follow how much a run
processed rather than whether it failed.
*/
package idle

import (
	"context"
	"time"

	"github.com/uchaloop/beat"
)

// DelayStrategy returns how long to pause after a run, given its error.
type DelayStrategy func(err error) time.Duration

// Middleware pauses after the wrapped Job returns, for the duration the strategy
// reports. The pause honours context cancellation. A nil strategy is a no-op.
func Middleware(strategy DelayStrategy) beat.Middleware {
	return func(next beat.Job) beat.Job {
		return func(ctx context.Context) (int, error) {
			processed, err := next(ctx)

			if strategy != nil {
				beat.Wait(ctx, strategy(err))
			}

			return processed, err
		}
	}
}
