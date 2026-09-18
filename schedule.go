package beat

import (
	"context"
	"hash/fnv"
	"math/rand/v2"
	"time"
)

// Mode selects how the start of one run relates to the next.
type Mode string

const (
	// ModeFixedRate starts runs on an absolute grid: every Period since the
	// Unix epoch, shifted by the offset. The cadence does not depend on how
	// long a run takes, so two processes that share a Period agree on the
	// points without sharing anything else - which is what lets a per-replica
	// offset hold them apart for as long as they live.
	//
	// Runs still never overlap. A run that outlives its period covers the
	// points it ran through, and they are reported as Record.Missed rather
	// than queued for later.
	ModeFixedRate Mode = "fixed_rate"

	// ModeFixedDelay starts the next run one Period after the previous one
	// ended, so a slow run pushes the whole schedule back. Use it for a job
	// that should rest between runs rather than keep a cadence.
	//
	// There is no grid, so nothing is ever missed and an offset only delays
	// the first run: the phase follows the job's duration and is not held.
	ModeFixedDelay Mode = "fixed_delay"
)

// schedule decides when each run starts. It never reads the clock - every input
// is a parameter. That keeps the whole timing policy in one place and lets it
// be tested against a table instead of against timers.
type schedule struct {
	mode   Mode
	period time.Duration
	offset time.Duration
}

// firstTarget reports when the first run starts.
//
// A fixed-rate schedule waits for the next point of the grid, at most one
// period away. It does not run immediately on start: an off-grid first run
// would undo the offset that keeps replicas apart, and a deployment restarting
// every replica at once is exactly when that matters. A fixed-delay schedule
// has no grid to join, so it starts as soon as the offset has elapsed.
func (s schedule) firstTarget(now time.Time) time.Time {
	if s.mode == ModeFixedDelay {
		return now.Add(s.offset)
	}

	return s.nextGridPoint(now)
}

// nextTarget reports when the run following the one aimed at target should start,
// and how many grid points went by unserved in the meantime.
//
// now is when scheduling resumes after Handler and the backoff callback.
// jobEnd is when the Job returned, and backoff is the pause WithBackoff asked for
// after it, measured from jobEnd.
func (s schedule) nextTarget(now, target, jobEnd time.Time, backoff time.Duration) (time.Time, int) {
	notBefore := now
	if backoff > 0 {
		if backoffUntil := jobEnd.Add(backoff); backoffUntil.After(notBefore) {
			notBefore = backoffUntil
		}
	}

	if s.mode == ModeFixedDelay {
		next := jobEnd.Add(s.period)
		if next.Before(notBefore) {
			next = notBefore
		}

		return next, 0
	}

	// What was lost is measured against now, never against the backoff. Points a
	// backoff holds back have not happened yet, and they are the schedule the
	// application asked for rather than a loss - counting them would make the
	// number loudest on a healthy idle poller, which is the one place it should
	// stay quiet.
	_, missed := s.advanceGrid(target, now)

	next, _ := s.advanceGrid(target, notBefore)

	return next, missed
}

// advanceGrid walks the grid from target to the first point at or after notBefore,
// and reports how many points it stepped over on the way.
func (s schedule) advanceGrid(target, notBefore time.Time) (time.Time, int) {
	next := target.Add(s.period)
	if !next.Before(notBefore) {
		return next, 0
	}

	// Jump straight to the first point at or after notBefore instead of
	// stepping one period at a time: a clock that moved by a day would
	// otherwise cost a day's worth of iterations.
	bypassedPoints := int64((notBefore.Sub(next) + s.period - 1) / s.period)

	return next.Add(time.Duration(bypassedPoints) * s.period), int(bypassedPoints)
}

// nextGridPoint reports the first grid point strictly after now. Points sit
// at every multiple of the period since the Unix epoch, shifted by the
// offset. Nothing about the grid depends on when the process started, which is
// what makes the offset - and only the offset - decide where a replica sits.
func (s schedule) nextGridPoint(now time.Time) time.Time {
	k := now.Add(-s.offset).UnixNano() / int64(s.period)

	return time.Unix(0, (k+1)*int64(s.period)).Add(s.offset)
}

// wait blocks for d or until ctx is done, whichever comes first. A non-positive
// d returns immediately.
func wait(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// randomOffset returns a uniform offset in [0, max). A non-positive max yields
// 0. The draw is deliberately not cryptographic: an offset spreads load, it
// does not protect anything.
func randomOffset(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}

	return rand.N(max)
}

// OffsetFor derives an offset in [0, max) from id, so a replica keeps the same
// slot for as long as that id is the same, instead of drawing a new one every
// time it boots:
//
//	beat.WithOffset(beat.OffsetFor(os.Getenv("POD_NAME"), 30*time.Second))
//
// The result is stable but still arbitrary, so two replicas can land close
// together. When the replicas are numbered - a StatefulSet, or any deployment
// that can tell a pod its index - spacing them by hand is strictly better,
// because it is the only way to guarantee a gap:
//
//	beat.WithOffset(time.Duration(ordinal) * max / time.Duration(replicas))
//
// An empty id or a non-positive max yields 0.
func OffsetFor(id string, max time.Duration) time.Duration {
	if len(id) == 0 || max <= 0 {
		return 0
	}

	h := fnv.New64a()
	h.Write([]byte(id))

	return time.Duration(h.Sum64() % uint64(max))
}
