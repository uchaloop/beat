// Package idle provides a beat.Middleware that pauses after each run for a delay
// chosen from the run's error - a backoff on failure, for example.
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
