// Package recovery provides a beat.Middleware that recovers from a panic in the
// Job and turns it into a *beat.PanicError, so one bad run does not crash the
// process. The beat core does not recover panics itself; add this middleware to
// keep the scheduler alive.
package recovery

import (
	"context"
	"log/slog"
	"runtime/debug"

	"github.com/uchaloop/beat"
)

type settings struct {
	log *slog.Logger
}

// Option configures the middleware built by Middleware.
type Option func(*settings)

// WithLogger logs each recovered panic, with its stack, at Error level. A nil
// logger (the default) disables logging.
func WithLogger(l *slog.Logger) Option {
	return func(s *settings) { s.log = l }
}

// Middleware recovers from a panic in the wrapped Job so the loop keeps running.
// The panic becomes the run's error - always a *beat.PanicError carrying the
// value and stack, so downstream handlers can classify it - and, if WithLogger
// is set, is logged with its stack.
func Middleware(opts ...Option) beat.Middleware {
	var s settings
	for _, o := range opts {
		if o != nil {
			o(&s)
		}
	}

	return func(next beat.Job) beat.Job {
		return func(ctx context.Context) (processed int, err error) {
			defer func() {
				if p := recover(); p != nil {
					stack := debug.Stack()

					if s.log != nil {
						s.log.Error("job panic recovered", "panic", p, "stack", string(stack))
					}

					err = &beat.PanicError{Value: p, Stack: stack}
				}
			}()

			return next(ctx)
		}
	}
}
