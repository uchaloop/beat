package beat

import (
	"context"
	"time"
)

// settings holds the resolved options for one Beat.
type settings struct {
	middleware []Middleware
	handler    Handler
	backoff    func(Record) time.Duration

	mode Mode

	// offset is nil until WithOffset sets one, which is how an explicit zero is
	// told apart from "draw one from Config.Jitter".
	offset *time.Duration

	onStart func(context.Context) error
	onStop  func(context.Context) error

	graceful bool
}

// Option customizes a Beat built by MakeBeat.
type Option interface{ apply(*settings) }

type optionFunc func(*settings)

func (f optionFunc) apply(s *settings) { f(s) }

// WithMode selects the scheduling policy. ModeFixedRate is the default;
// ModeFixedDelay waits at least Period from the end of the previous Job.
func WithMode(m Mode) Option {
	return optionFunc(func(s *settings) { s.mode = m })
}

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

// WithBackoff computes a minimum pause from Job completion. The callback runs
// inline after Handler and must return promptly. A non-positive result adds no
// restriction; it does not remove Period. The next target must satisfy both
// the schedule and the pause. Intentionally bypassed points are not Missed.
// Backoff is outside JobTimeout and Record.Duration.
func WithBackoff(fn func(Record) time.Duration) Option {
	return optionFunc(func(s *settings) { s.backoff = fn })
}

// WithOffset sets the offset within the period explicitly, instead of drawing
// one below Config.Jitter. Use it to give each replica a slot that survives a
// restart - see OffsetFor - or to spread replicas evenly by their index. The
// offset must be in [0, Period).
func WithOffset(d time.Duration) Option {
	return optionFunc(func(s *settings) { s.offset = &d })
}

// WithOnStart registers a hook run once before the loop starts. A returned
// error aborts startup and leaves the Beat stopped, without running the OnStop
// hook - the two are a pair, and this one did not complete. If the hook
// succeeds after the startup context is cancelled, Start arranges cleanup;
// see Beat.Start.
func WithOnStart(h func(context.Context) error) Option {
	return optionFunc(func(s *settings) { s.onStart = h })
}

// WithOnStop registers a hook run once after the loop has stopped.
//
// "After the loop has stopped" is exact: if Stop's context elapses while a run
// is still going, the hook does not run at all, because it would otherwise race
// the Job for whatever it is meant to release. Watch Beat.Done for the moment
// the run finally ends, after Stop returned ErrStillRunning. Done does not
// wait for OnStop. The hook runs synchronously and must respect its context.
func WithOnStop(h func(context.Context) error) Option {
	return optionFunc(func(s *settings) { s.onStop = h })
}

// WithGracefulStop lets the active Job finish while Stop waits. JobTimeout
// still applies. If Stop's context expires before startup/the loop finishes,
// Stop cancels the Job context, returns ErrStillRunning and skips OnStop.
func WithGracefulStop() Option {
	return optionFunc(func(s *settings) { s.graceful = true })
}
