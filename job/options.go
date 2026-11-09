package job

import (
	"context"
	"slices"
)

// settings holds the resolved options for one Runner.
type settings struct {
	middleware   []Middleware
	errorHandler ErrorHandler
}

// Option customizes a Runner built by MakeRunner.
type Option interface{ apply(*settings) }

type optionFunc func(*settings)

func (f optionFunc) apply(s *settings) { f(s) }

// WithMiddleware wraps the work. The first middleware is the outermost layer
// and therefore runs first, which is the order the call reads in. Nil entries
// are skipped, and the chain is composed once, when the Runner is built.
// The input slice is copied; referenced middleware state remains caller-owned.
func WithMiddleware(middleware ...Middleware) Option {
	middleware = slices.Clone(middleware)

	return optionFunc(func(s *settings) { s.middleware = append(s.middleware, middleware...) })
}

// ErrorHandler handles a non-nil error returned by the work and middleware.
// It runs synchronously with an independent, bounded context. It must respect
// cancellation. Its own error is recorded separately and is never handled again.
// Panics propagate; work recovery middleware does not cover this callback.
type ErrorHandler func(context.Context, error) error

// WithErrorHandler sets the optional error callback. A nil callback disables it.
// It is not called for skipped work or a nil work error, even on timeout.
func WithErrorHandler(handler ErrorHandler) Option {
	return optionFunc(func(s *settings) { s.errorHandler = handler })
}
