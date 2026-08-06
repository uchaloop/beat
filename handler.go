package beat

import (
	"context"
	"time"
)

// Mode is the scheduling mode of the Beat that produced a run.
type Mode string

const (
	// ModeInterval is an "@every" interval schedule.
	ModeInterval Mode = "interval"
	// ModeCron is a cron-expression schedule.
	ModeCron Mode = "cron"
)

// Record describes a single completed run of the Job. It is handed to a Handler
// after each run, mirroring the shape of slog.Record: the library produces the
// data, the caller decides what to do with it (metrics, logging, tracing).
type Record struct {
	// Iteration is the 1-based run counter for this Beat instance.
	Iteration int64
	// Start is the wall-clock time the run began.
	Start time.Time
	// Duration is how long the Job took.
	Duration time.Duration
	// Processed is the number of items the Job reported for this run.
	Processed int
	// Err is the error the Job returned, or nil on success. A panic recovered
	// by the recovery middleware is reported as a *PanicError.
	Err error
	// Mode is the scheduling mode that produced this run.
	Mode Mode
}

// Handler receives a Record after every run. It is the single observability
// seam of beat: metrics, logging and tracing are all built on top of it, so the
// core depends on none of them. Handle must not block for long - it runs inline
// on the run loop. It mirrors slog.Handler.
type Handler interface {
	Handle(ctx context.Context, r Record)
}

// HandlerFunc adapts an ordinary function to a Handler, like http.HandlerFunc.
type HandlerFunc func(ctx context.Context, r Record)

// Handle calls f. A nil HandlerFunc is a no-op.
func (f HandlerFunc) Handle(ctx context.Context, r Record) {
	if f != nil {
		f(ctx, r)
	}
}

// noopHandler is used when no Handler is provided.
type noopHandler struct{}

func (noopHandler) Handle(context.Context, Record) {}

// MultiHandler returns a Handler that passes each Record to every handler in
// turn, skipping nil ones. Use it to run several sinks - metrics and logging,
// say - behind Module's single Handler slot. Handlers run in order and are not
// isolated: the core does not recover a panicking handler, so one panic
// propagates and crashes the process. Keep handlers panic-free.
func MultiHandler(handlers ...Handler) Handler {
	return HandlerFunc(
		func(ctx context.Context, r Record) {
			for _, h := range handlers {
				if h != nil {
					h.Handle(ctx, r)
				}
			}
		},
	)
}
