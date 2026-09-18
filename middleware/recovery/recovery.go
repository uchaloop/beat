// Package recovery supplies middleware that converts panics in the wrapped
// Job call into beat.PanicError, including the recovered value and stack.
// WithLogger also logs the panic. Recovery does not cover Handler, lifecycle
// hooks, backoff callbacks, or goroutines created by Job.
//
// Place recovery before middleware whose panics it should catch:
//
//	beat.WithMiddleware(recovery.Middleware(recovery.WithLogger(logger)))
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
