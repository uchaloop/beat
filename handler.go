package beat

import (
	"context"
	"time"
)

// Outcome classifies how a run ended. It is the one label an observability
// adapter needs; Record.Err carries the detail behind it.
type Outcome string

const (
	// OutcomeOK is a run that finished and returned no error.
	OutcomeOK Outcome = "ok"

	// OutcomeError is a run that returned an error of its own.
	OutcomeError Outcome = "error"

	// OutcomePanic is a run whose panic the recovery middleware turned into a
	// *PanicError. Without that middleware a panic never reaches a Record - it
	// crashes the process.
	OutcomePanic Outcome = "panic"

	// OutcomeTimeout means the Job's context deadline expired before the
	// outcome was classified. It is reported only after Job returns, even if
	// Job returns nil. It does not imply that execution was forcibly stopped.
	OutcomeTimeout Outcome = "timeout"

	// OutcomeCanceled means the Job context was cancelled by shutdown before
	// the outcome was classified. It is separate from a Job-returned error.
	OutcomeCanceled Outcome = "canceled"
)

// Record describes a single completed run of the Job. It is handed to a Handler
// after each run, mirroring the shape of slog.Record: the library produces the
// data, the caller decides what to do with it (metrics, logging, tracing).
type Record struct {
	// Iteration is the 1-based run counter for this Beat, useful as a
	// correlation id. It counts runs, not scheduled points - see Missed.
	Iteration int64

	// ScheduledFor is the point this run was aiming at. Start minus this is how
	// late the run actually began.
	ScheduledFor time.Time

	// Start is the wall-clock time the run began.
	Start time.Time

	// Duration is how long the Job took. It excludes the Handler and any
	// backoff, so it stays comparable with Period.
	Duration time.Duration

	// Period is the configured period, so a Handler can report Duration over
	// Period - how close the job is to outgrowing its schedule - without being
	// handed the configuration separately.
	Period time.Duration

	// Mode is the timing policy that produced this run.
	Mode Mode

	// Processed is the number of items the Job reported for this run.
	Processed int

	// Err is the error the Job returned, or nil on success. A panic recovered
	// by the recovery middleware is reported as a *PanicError.
	Err error

	// Outcome classifies the run and is authoritative: a run can time out with
	// a nil Err, and a *PanicError reads as OutcomePanic.
	Outcome Outcome

	// Missed is how many scheduled points passed without a run between the
	// previous run and this one - because that run and its Handler outlasted
	// them, or because the clock moved.
	//
	// Points the loop chose not to serve are not counted: a backoff asking it to
	// wait is the schedule the application requested, not a loss, and counting
	// those would make this number loudest on a healthy idle poller. It is
	// always 0 under ModeFixedDelay, which has no grid to miss.
	//
	// This Record's timestamps and Duration describe the current run, not the
	// preceding work that caused Missed. A single Record cannot identify the
	// cause; comparing successive records may help diagnose it.
	Missed int
}

// Handler receives a Record after every run. It is the single observability
// seam of beat: metrics, logging and tracing are all built on top of it, so the
// core depends on none of them. It mirrors slog.Handler.
//
// Handle runs inline on the run loop, so its time comes out of the schedule: a
// slow Handler delays the next run and can cost a scheduled point. It is handed
// a context that is deliberately not cancellable, so that the last record of a
// shutdown still arrives - which means a Handler that blocks holds the loop, and
// Stop will give up on it with ErrStillRunning. Return promptly, and give any
// network call a budget of its own.
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
// say - behind the single Handler slot. Handlers run in order and are not
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
