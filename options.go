package beat

import (
	"context"
	"time"

	"github.com/uchaloop/job/assignment"
)

// settings holds the resolved options for one Beat.
type settings struct {
	handler Handler
	backoff func(Record) time.Duration

	mode Mode
	// offset is nil until WithOffset sets one, which is how an explicit zero is
	// told apart from "draw one from Config.Jitter".
	offset *time.Duration

	rotation   *assignment.Rotation
	onDecision func(assignment.Decision)

	onStart func(context.Context) error
	onStop  func(context.Context) error

	graceful bool
}

// Option customizes a Beat built by MakeBeat.
type Option interface{ apply(*settings) }

type optionFunc func(*settings)

func (f optionFunc) apply(s *settings) { f(s) }

// WithMode selects the timing policy. The default is ModeFixedRate, which keeps
// a cadence on an absolute grid; ModeFixedDelay measures the period from the
// end of the previous run instead.
//
// This is an option rather than a Config field because the choice follows from
// what the job is - a poller that should rest between runs against a scraper
// that should keep a rate - and not from how a particular deployment is tuned.
func WithMode(m Mode) Option {
	return optionFunc(func(s *settings) { s.mode = m })
}

// WithHandler sets the observability Handler. It overrides a Handler provided
// through the Fx container.
func WithHandler(h Handler) Option {
	return optionFunc(func(s *settings) { s.handler = h })
}

// WithBackoff pauses the loop after a run, for the duration fn reports from the
// Record of that run. Returning 0 - or not setting it at all - leaves the
// schedule as configured.
//
//	beat.WithBackoff(func(r beat.Record) time.Duration {
//		if r.Result.Processed == 0 {
//			return 30 * time.Second // the queue was empty, come back later
//		}
//
//		return 0
//	})
//
// It sets a floor rather than a delay: the next run starts no earlier than the
// pause allows and no earlier than the schedule says, whichever is later. Under
// ModeFixedRate a pause that outlives the next grid points simply moves the run
// past them; they are not counted as Record.Missed, because a pause the
// application asked for is its schedule and not a loss.
//
// The pause belongs here and not in a job.Middleware because the loop owns the
// timing. A pause taken inside the work would spend the job timeout, land in
// job.Result.Duration and be invisible to the schedule; this one does none of
// that. A point another cluster owns produces no Record and no backoff.
func WithBackoff(fn func(Record) time.Duration) Option {
	return optionFunc(func(s *settings) { s.backoff = fn })
}

// WithOffset sets the offset within the period explicitly, instead of drawing
// one below Config.Jitter. Use it to give each replica a slot that survives a
// restart - see job/assignment for the cluster-level equivalent - or to spread
// replicas evenly by their index. The offset must be in [0, Period).
func WithOffset(d time.Duration) Option {
	return optionFunc(func(s *settings) { s.offset = &d })
}

// WithAssignment makes the loop ask a cluster rotation who owns each scheduled
// point, and skip the ones it does not own. It is off by default: without it
// every instance runs every point, which is what a deployment scaling for
// throughput wants.
//
// It requires ModeFixedRate - replicas have no shared numbering under
// ModeFixedDelay - and the rotation's period must match Config.Period, since
// they describe the same grid. Both are checked by MakeBeat.
//
// A point owned by another cluster produces no run, no Record and no backoff,
// and is not counted as Record.Missed: it was never this process's to serve.
// Use WithDecisionHandler to observe those points.
//
// The rotation names an owner; it does not guarantee the owner runs. A cluster
// that is down simply leaves its points unserved - no other cluster takes over,
// because that would need shared state the policy deliberately does not have.
func WithAssignment(r *assignment.Rotation) Option {
	return optionFunc(func(s *settings) { s.rotation = r })
}

// WithDecisionHandler observes every decision the cluster rotation makes,
// including the ones that allow the run. It is the only way to tell a point
// this process skipped from one it never had, since neither produces a Record.
//
// A decision that allows a run is not a promise that one followed. The loop
// decides, reports here, and only then takes the last word on whether a new
// attempt may begin, so a Stop landing in between leaves a reported decision
// with no Record behind it. The order cannot be reversed without reopening the
// window in which a graceful stop would start fresh work.
//
// It runs inline on the loop and must return promptly; a panic propagates.
func WithDecisionHandler(fn func(assignment.Decision)) Option {
	return optionFunc(func(s *settings) { s.onDecision = fn })
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
// the work for whatever it is meant to release. Watch Beat.Done for the moment
// the run finally ends, after Stop returned ErrStillRunning. Done does not
// wait for OnStop. The hook runs synchronously and must respect its context.
func WithOnStop(h func(context.Context) error) Option {
	return optionFunc(func(s *settings) { s.onStop = h })
}

// WithGracefulStop lets the in-flight run finish on shutdown instead of having
// its context cancelled. beat stops scheduling new runs immediately; the current
// run continues until it returns or the stop deadline (fx.StopTimeout, 15s
// unless the application raises it) elapses, at which point its context is
// cancelled as a last resort and Stop reports ErrStillRunning.
//
// A job whose duration varies should be matched by a stop timeout above its
// worst case, or the drain only sometimes happens - and a stop that runs out of
// time skips the OnStop hook, which is exactly when a graceful stop was supposed
// to help.
func WithGracefulStop() Option {
	return optionFunc(func(s *settings) { s.graceful = true })
}
