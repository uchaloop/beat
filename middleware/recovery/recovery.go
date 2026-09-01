/*
Package recovery provides a beat.Middleware that recovers from a panic in the
Job and turns it into a *beat.PanicError, so one bad run does not take the
process with it.

The beat core deliberately does not recover panics: a panic that propagates
crashes the process with a stack on stderr, which is the honest outcome for user
code that misbehaved and one a supervisor restarts. That default is wrong for a
scheduler whose job touches input it does not control, where a single malformed
record should not stop every later run. This middleware is how that choice is
made explicitly rather than by default.

	beatfx.Module(
		beat.WithMiddleware(recovery.Middleware(recovery.WithLogger(logger))),
	)

The recovered value and its stack are reported as a *beat.PanicError in the
run's error, so a Handler tells a panic from an ordinary failure with errors.As.
[WithLogger] additionally logs it where it happened, with the stack, which is
the only place the stack is still complete.
*/
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
