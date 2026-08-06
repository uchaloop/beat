package beat

import "context"

// settings holds the resolved options for one Beat.
type settings struct {
	middleware []Middleware
	handler    Handler

	onStart func(context.Context) error
	onStop  func(context.Context) error

	graceful bool
}

// Option customizes a Beat built by Module.
type Option interface{ apply(*settings) }

type optionFunc func(*settings)

func (f optionFunc) apply(s *settings) { f(s) }

// WithMiddleware wraps the Job. The first middleware is the outermost layer and
// runs first.
func WithMiddleware(mw ...Middleware) Option {
	return optionFunc(func(s *settings) { s.middleware = append(s.middleware, mw...) })
}

// WithHandler sets the observability Handler. It overrides a Handler provided
// through the Fx container.
func WithHandler(h Handler) Option {
	return optionFunc(func(s *settings) { s.handler = h })
}

// WithOnStart registers a hook run once before the loop starts. A returned
// error aborts startup.
func WithOnStart(h func(context.Context) error) Option {
	return optionFunc(func(s *settings) { s.onStart = h })
}

// WithOnStop registers a hook run once after the loop has stopped.
func WithOnStop(h func(context.Context) error) Option {
	return optionFunc(func(s *settings) { s.onStop = h })
}

// WithGracefulStop lets the in-flight run finish on shutdown instead of having
// its context cancelled. beat stops scheduling new runs immediately; the current
// run continues until it returns or the stop deadline (fx.StopTimeout) elapses,
// at which point its context is cancelled as a last resort.
func WithGracefulStop() Option {
	return optionFunc(func(s *settings) { s.graceful = true })
}
