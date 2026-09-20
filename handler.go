package beat

import (
	"context"
	"time"

	"github.com/uchaloop/job"
)

// Mode is the timing policy of the Beat that produced a run.
type Mode string

const (
	// ModeFixedRate starts runs on an absolute grid: every Period since the
	// Unix epoch, shifted by the offset. The cadence does not depend on how
	// long a run takes, so two processes that share a Period agree on the
	// points without sharing anything else - which is what lets a per-replica
	// offset hold them apart for as long as they live, and what lets an
	// optional cluster rotation name an owner for each point.
	//
	// Runs still never overlap. A run that outlives its period covers the
	// points it ran through, and they are reported as Record.Missed rather
	// than queued for later.
	ModeFixedRate Mode = "fixed_rate"

	// ModeFixedDelay starts the next run one Period after the previous one
	// ended, so a slow run pushes the whole schedule back. Use it for a job
	// that should rest between runs rather than keep a cadence.
	//
	// There is no grid, so nothing is ever missed, an offset only delays the
	// first run, and a cluster rotation cannot be used: replicas have no shared
	// numbering to rotate over.
	ModeFixedDelay Mode = "fixed_delay"
)

// Record describes a single completed run. It is handed to a Handler after each
// run: the scheduling around the work, plus the job.Result the work produced.
//
// The nesting is the boundary made visible. Everything under Result belongs to
// the attempt and means the same in a one-shot process; everything beside it
// belongs to the schedule and exists only here.
type Record struct {
	// Result is what the attempt itself reported: duration, count, error and
	// outcome. See job.Result.
	Result job.Result

	// Iteration is the 1-based run counter for this Beat, useful as a
	// correlation id. It counts runs, not scheduled points - see Missed - and
	// does not advance for a point another cluster owns.
	Iteration int64

	// GridPoint is the point of the shared grid this run served, before the
	// per-replica offset. It is what every replica and every cluster agrees on,
	// and therefore what a cluster rotation decides over. Under ModeFixedDelay
	// it equals ScheduledFor, which is not a shared tick at all.
	GridPoint time.Time

	// ScheduledFor is the point this replica aimed at: GridPoint plus the
	// offset. Result.Start minus this is how late the run actually began.
	ScheduledFor time.Time

	// Period is the configured spacing of the shared grid.
	Period time.Duration

	// LocalPeriod is the spacing of the points assigned to this process: Period
	// times the number of clusters when a rotation is in use, and Period itself
	// otherwise. A handler reporting how much of its schedule the work uses
	// divides by this, not by Period.
	LocalPeriod time.Duration

	// Mode is the timing policy that produced this run.
	Mode Mode

	// Missed is how many scheduled points belonging to this process passed
	// without a run between the previous run and this one - because that run
	// and its Handler outlasted them, or because the clock moved.
	//
	// Points the loop chose not to serve are not counted: a backoff asking it to
	// wait is the schedule the application requested, and a point another
	// cluster owns was never this process's to serve. It is always 0 under
	// ModeFixedDelay, which has no grid to miss.
	//
	// This Record's timestamps and Result describe the current attempt, not the
	// earlier work that lost the points. A single Record therefore cannot
	// identify the cause - the previous attempt may have been long while this
	// one is short and on time, and under a rotation other clusters' points sit
	// between them. Comparing successive records can.
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
