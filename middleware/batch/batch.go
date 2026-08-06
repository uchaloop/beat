// Package batch provides a beat.Middleware that pauses after each run for a
// delay chosen from how much the run processed and its error - useful for
// draining a queue fast while it is full and backing off when it empties.
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
