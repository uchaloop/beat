package beat

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/uchaloop/job"
	"github.com/uchaloop/job/assignment"
)

const (
	// startupCleanupTimeout gives rollback a fresh, cooperative budget when
	// OnStart succeeded but the startup context was cancelled before the loop
	// could be launched.
	startupCleanupTimeout = 15 * time.Second

	// maxClockRecheckInterval bounds one sleep so the loop re-reads the clock at
	// least this often. A fixed-rate schedule aims at a wall-clock point, so a
	// clock that moved under a running process would otherwise go unnoticed
	// until the sleep ended, however long that was.
	//
	// This bounds how long such a discrepancy goes unnoticed while the process
	// runs. It is not a promise to recover within 30 seconds of a suspended
	// host: whether a monotonic clock advances across suspend is up to the
	// platform, and on waking the target is simply already in the past - see
	// sleepUntil. A fixed-delay schedule aims at a monotonic point and needs no
	// correction at all, but slicing costs it nothing and keeps one code path.
	maxClockRecheckInterval = 30 * time.Second
)

var (
	// ErrAlreadyStarted reports a second Start on a Beat that is already
	// running. One Beat runs one loop: a second one would break the guarantee
	// that runs never overlap.
	ErrAlreadyStarted = errors.New("beat is already started")

	// ErrStopped reports a Start on a Beat that has been stopped. A stopped
	// Beat is not restartable - build a new one with MakeBeat.
	ErrStopped = errors.New("beat is stopped")

	// ErrStillRunning reports that Stop returned before the run did: the run
	// was cancelled, but had not returned by the time the stop deadline
	// elapsed. The OnStop hook is not run in that case, and [Beat.Done] is how
	// an application learns when the run finally ends.
	ErrStillRunning = errors.New("stop deadline elapsed before the run finished")
)

// lifecycleState is the lifecycle of one Beat. It only moves forward: a stopped
// Beat stays stopped, because reviving cancelled contexts is more surprising
// than building a new Beat.
type lifecycleState int

const (
	stateNew lifecycleState = iota
	stateStarting
	stateRunning
	stateStopping
	stateStopped
)

// Beat runs one job.Runner on a schedule. Build it with MakeBeat and drive it
// with Start and Stop; the separate beatfx module does that from the Fx lifecycle.
//
// It keeps two independent contexts: loopCtx gates scheduling (cancelled to stop
// launching new runs) and workCtx is the parent of each run (cancelled to
// abort an in-flight run). Splitting them lets stop drain - end scheduling while
// letting the current run finish - or cancel, depending on the graceful flag.
type Beat struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc
	workCtx    context.Context
	workCancel context.CancelFunc

	graceful bool

	runner  *job.Runner
	handler Handler
	backoff func(Record) time.Duration

	schedule    schedule
	localPeriod time.Duration

	rotation   *assignment.Rotation
	onDecision func(assignment.Decision)

	onStart func(context.Context) error
	onStop  func(context.Context) error

	// mu guards the lifecycle state, the result Stop leaves behind for any
	// later caller, and the error a loop that stopped by itself left.
	mu               sync.Mutex
	state            lifecycleState
	stopErr          error
	loopErr          error
	startupSucceeded bool

	// loopDone closes when no run is in flight and none ever will be. stopDone
	// closes once the first Stop has finished, so a second one can report its
	// result rather than run the hook again.
	loopDone     chan struct{}
	loopDoneOnce sync.Once
	stopDone     chan struct{}
	stopDoneOnce sync.Once

	// iteration is touched only by the run loop, of which there is one.
	iteration int64
}

// MakeBeat resolves cfg against opts and builds a Beat around runner. runner is
// required and carries the work, its middleware and its timeout; handler may be
// supplied with WithHandler, and a no-op is used without it.
//
// Drive the returned Beat with Start and Stop; the separate beatfx module does this
// from the Fx lifecycle.
func MakeBeat(cfg Config, runner *job.Runner, opts ...Option) (*Beat, error) {
	if runner == nil {
		return nil, errors.New("runner is required")
	}

	s := settings{mode: ModeFixedRate}
	for _, o := range opts {
		if o != nil {
			o.apply(&s)
		}
	}

	// Validate here too: the direct fx.Supply path (without confmaker) never
	// went through Config.Validate.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if s.mode != ModeFixedRate && s.mode != ModeFixedDelay {
		return nil, fmt.Errorf("unknown mode %q", s.mode)
	}

	offset := randomOffset(cfg.Jitter)
	if s.offset != nil {
		offset = *s.offset
	}

	if offset < 0 || offset >= cfg.Period {
		return nil, fmt.Errorf("offset %v must be in [0, period %v)", offset, cfg.Period)
	}

	if s.rotation == nil && s.onDecision != nil {
		return nil, errors.New("a decision handler needs WithAssignment: without a rotation there are no decisions")
	}

	localPeriod, err := localPeriodFor(cfg.Period, s.mode, s.rotation)
	if err != nil {
		return nil, err
	}

	if s.handler == nil {
		s.handler = noopHandler{}
	}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	workCtx, workCancel := context.WithCancel(context.Background())

	return &Beat{
		loopCtx:    loopCtx,
		loopCancel: loopCancel,
		workCtx:    workCtx,
		workCancel: workCancel,

		graceful: s.graceful,

		runner:  runner,
		handler: s.handler,
		backoff: s.backoff,

		schedule:    schedule{mode: s.mode, period: cfg.Period, offset: offset},
		localPeriod: localPeriod,

		rotation:   s.rotation,
		onDecision: s.onDecision,

		onStart: s.onStart,
		onStop:  s.onStop,

		loopDone: make(chan struct{}),
		stopDone: make(chan struct{}),
	}, nil
}

// localPeriodFor reports the spacing of the points this process is responsible
// for, and rejects a rotation that cannot describe the same grid the schedule
// does.
func localPeriodFor(period time.Duration, mode Mode, rotation *assignment.Rotation) (time.Duration, error) {
	if rotation == nil {
		return period, nil
	}

	if mode != ModeFixedRate {
		return 0, fmt.Errorf("a cluster rotation needs %q: %q has no shared grid to rotate over", ModeFixedRate, mode)
	}

	if rotation.Period() != period {
		return 0, fmt.Errorf("rotation period %v does not match beat period %v", rotation.Period(), period)
	}

	clusters := int64(rotation.ClusterCount())
	if clusters > int64(math.MaxInt64)/int64(period) {
		return 0, fmt.Errorf("period %v across %d clusters is not representable", period, clusters)
	}

	return time.Duration(clusters) * period, nil
}

// Start runs the OnStart hook and launches the run loop. The loop runs in its
// own goroutine, so Start returns once it is launched. ctx bounds the hook, and
// a returned error aborts startup.
//
// It may be called once. A second call reports [ErrAlreadyStarted], and a call
// on a stopped Beat reports [ErrStopped]; neither launches a second loop, which
// is what keeps runs from overlapping. If the hook fails the Beat is left
// stopped, and the OnStop hook is not run - the hooks are a pair, and the first
// one did not complete. If OnStart succeeds but ctx has been cancelled, Start
// invokes Stop with a fresh 15-second cleanup context and returns both errors.
// A concurrent Stop owns cleanup instead. Hooks must respect their contexts.
func (b *Beat) Start(ctx context.Context) error {
	b.mu.Lock()

	switch b.state {
	case stateStarting, stateRunning, stateStopping:
		b.mu.Unlock()

		return ErrAlreadyStarted
	case stateStopped:
		b.mu.Unlock()

		return ErrStopped
	}

	b.state = stateStarting
	b.mu.Unlock()

	startupErr := b.runOnStart(ctx)

	b.mu.Lock()
	b.startupSucceeded = startupErr == nil

	if b.state != stateStarting {
		// Stop owns its result and stopDone, even if startup fails meanwhile.
		b.mu.Unlock()
		b.closeLoopDone()

		return errors.Join(startupErr, ctx.Err(), ErrStopped)
	}

	if startupErr != nil {
		b.state = stateStopped
		b.loopCancel()
		b.workCancel()
		b.closeLoopDone()
		b.closeStopDone()
		b.mu.Unlock()

		return startupErr
	}

	b.state = stateRunning

	if err := ctx.Err(); err != nil {
		// The hook completed, so its resources need the paired cleanup even if
		// the caller never calls Stop after this failed Start.
		b.closeLoopDone()
		b.mu.Unlock()

		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), startupCleanupTimeout)
		defer cancel()

		return errors.Join(err, b.Stop(cleanupCtx))
	}

	go func() {
		defer b.closeLoopDone()

		b.runLoop()
	}()

	b.mu.Unlock()

	return nil
}

// Stop ends scheduling, waits for the run loop to leave (bounded by ctx), then
// runs the OnStop hook. By default the in-flight run's context is cancelled at
// once; with graceful stop the run is left to finish and is only cancelled if
// it outlives ctx.
//
// If ctx elapses while a run is still going, Stop cancels the run, returns
// [ErrStillRunning] joined with ctx's error, and does not run the OnStop hook:
// the hook is documented to run after the loop has stopped, and running it
// against work that is still going would race it for the very resources the
// hook exists to release. [Beat.Done] reports when the run finally ends.
//
// A loop that stopped by itself - a cluster rotation that could not decide a
// point - leaves its error here, joined with whatever the hook reports.
//
// Stop is safe to call more than once and from several goroutines: the first
// call does the work, and the others report its result without running the hook
// again. Stopping a Beat that was never started does nothing and reports no
// error. Waiting callers may return their own context error before the first
// Stop finishes. The deadline bounds waiting; OnStop runs synchronously and
// must respect ctx. Stop during OnStart waits for that hook before cleanup.
func (b *Beat) Stop(ctx context.Context) error {
	b.mu.Lock()

	switch b.state {
	case stateNew:
		// Nothing was started, so there is nothing to stop and no completed
		// OnStart to pair an OnStop with.
		b.state = stateStopped
		b.mu.Unlock()

		b.closeLoopDone()
		b.closeStopDone()

		return nil

	case stateStopping, stateStopped:
		b.mu.Unlock()

		return b.awaitStopResult(ctx)
	}

	b.state = stateStopping
	b.mu.Unlock()

	err := b.shutdown(ctx)

	b.mu.Lock()
	b.stopErr = err
	b.state = stateStopped
	b.mu.Unlock()

	b.closeStopDone()

	return err
}

// Done returns a channel that closes when the run loop has left and no work is
// in flight - or, for a Beat that never started, as soon as it is stopped.
//
// It closes for two reasons: a Stop you called, and a loop that ended by
// itself after an error it cannot recover from. Stop returns on its own
// deadline and may return first, because a job timeout cancels an attempt's
// context but cannot interrupt it. Watch this to learn that the run finally
// ended - so a resource the work was still using can be released - or that a
// loop stopped without being asked to, in which case Stop reports why.
func (b *Beat) Done() <-chan struct{} { return b.loopDone }

// shutdown does the work of the first Stop: end scheduling, wait for the
// loop, and run the hook only if the loop actually left.
func (b *Beat) shutdown(ctx context.Context) error {
	b.loopCancel()

	if !b.graceful {
		b.workCancel()
	}

	select {
	case <-b.loopDone:
	case <-ctx.Done():
		b.workCancel()

		// Completion wins if both notifications are already available.
		select {
		case <-b.loopDone:
		default:
			return errors.Join(ctx.Err(), ErrStillRunning)
		}
	}

	b.workCancel()

	b.mu.Lock()
	startupSucceeded := b.startupSucceeded
	loopErr := b.loopErr
	b.mu.Unlock()

	if !startupSucceeded {
		return loopErr
	}

	return errors.Join(loopErr, b.runOnStop(ctx))
}

// awaitStopResult waits within this caller's budget. An already published result
// takes precedence over cancellation of the caller's context.
func (b *Beat) awaitStopResult(ctx context.Context) error {
	select {
	case <-b.stopDone:
	case <-ctx.Done():
		select {
		case <-b.stopDone:
		default:
			return ctx.Err()
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.stopErr
}

func (b *Beat) closeLoopDone() { b.loopDoneOnce.Do(func() { close(b.loopDone) }) }

func (b *Beat) closeStopDone() { b.stopDoneOnce.Do(func() { close(b.stopDone) }) }

// runLoop runs until scheduling stops. It owns the timing: nothing below it -
// not the work, not a middleware, not the Handler - decides when the next run
// happens, which is why a backoff is an option here rather than a pause taken
// inside the work.
func (b *Beat) runLoop() {
	target := b.schedule.firstTarget(time.Now())
	missed := 0

	for {
		if !b.sleepUntil(target) {
			return
		}

		decision, err := b.decide(target)
		if err != nil {
			b.endLoopWithError(err)

			return
		}

		// The last word on whether a new attempt may begin, taken under the
		// mutex Stop moves the state with. Checking a cancelled context here
		// would leave a window: a Stop landing between the check and the call
		// would still be followed by a fresh attempt.
		if !b.acceptsAttempt() {
			return
		}

		jobEnd := time.Now()

		var backoff time.Duration
		if decision.Execute {
			record := b.runAttempt(target, missed)
			missed = 0

			if b.backoff != nil {
				backoff = b.backoff(record)
			}

			jobEnd = attemptEnd(record.Result)
		}

		next, skipped := b.schedule.nextTarget(time.Now(), target, jobEnd, backoff)
		missed += b.ownedPointsSkipped(decision.Slot, skipped)
		target = next
	}
}

// attemptEnd reports when the work stopped, for the backoff to measure from.
//
// A Result the runner never measured - the caller's context was already done,
// so nothing ran - carries a zero Start. Adding a duration to that yields a
// timestamp from year one, which would silently swallow any backoff asked for
// on the way out. Today only a shutdown reaches here, and the loop leaves
// straight after, but the schedule should never be handed a meaningless point.
func attemptEnd(result job.Result) time.Time {
	if result.Start.IsZero() {
		return time.Now()
	}

	return result.Start.Add(result.Duration)
}

// decide asks the cluster rotation who owns the point behind target. Without a
// rotation every point is this process's own.
func (b *Beat) decide(target time.Time) (assignment.Decision, error) {
	if b.rotation == nil {
		return assignment.Decision{Execute: true}, nil
	}

	decision, err := b.rotation.Decide(assignment.Invocation{
		ScheduledFor: b.schedule.sharedGridPoint(target),
	})
	if err != nil {
		return assignment.Decision{}, err
	}

	if b.onDecision != nil {
		b.onDecision(decision)
	}

	return decision, nil
}

// ownedPointsSkipped reports how many of the points passed over belonged to
// this process. Without a rotation they all did.
func (b *Beat) ownedPointsSkipped(slot int64, skipped int) int {
	if b.rotation == nil {
		return skipped
	}

	return b.rotation.OwnedBetween(slot, slot+int64(skipped))
}

// endLoopWithError records an error the loop cannot carry on past. Scheduling
// ends, the error waits for Stop, and Done closes as the loop leaves.
//
// The lifecycle state is deliberately left alone: it belongs to Start and Stop,
// and moving it from the loop would let a Beat reach stopped without anyone
// having asked, which Stop and its result channel are built around. So between
// a loop that ended by itself and the Stop that follows, the state still reads
// running while nothing is running - Done is what tells them apart.
func (b *Beat) endLoopWithError(err error) {
	b.mu.Lock()
	b.loopErr = err
	b.mu.Unlock()

	b.loopCancel()
}

// sleepUntil waits for target, waking at least every maxClockRecheckInterval to
// re-read the clock. It reports whether the run should still happen.
//
// A target already in the past returns at once, so a process that was away for a
// while runs once against a stale point - the Record says so, with a large gap
// between ScheduledFor and Result.Start - and the run after it can follow soon.
// For work that drains a queue that is the useful behaviour; catching up point
// by point would not be.
func (b *Beat) sleepUntil(target time.Time) bool {
	for {
		if b.schedulingStopped() {
			return false
		}

		d := time.Until(target)
		if d <= 0 {
			return true
		}

		waitFor(b.loopCtx, min(d, maxClockRecheckInterval))
	}
}

// acceptsAttempt reports whether a new attempt may begin.
//
// Stop moves the state to stopping under this same mutex, so an attempt is
// either accepted before that - and a graceful stop then lets it finish - or
// refused after it. There is no in between, which a context check could not
// promise. The mutex is not held for the work itself.
func (b *Beat) acceptsAttempt() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.state == stateRunning
}

func (b *Beat) schedulingStopped() bool {
	select {
	case <-b.loopCtx.Done():
		return true
	default:
		return false
	}
}

// runAttempt performs one attempt and reports the result to the Handler.
func (b *Beat) runAttempt(target time.Time, missed int) Record {
	b.iteration++

	result := b.runner.Run(b.workCtx)

	record := Record{
		Result:       result,
		Iteration:    b.iteration,
		GridPoint:    b.schedule.sharedGridPoint(target),
		ScheduledFor: target,
		Period:       b.schedule.period,
		LocalPeriod:  b.localPeriod,
		Mode:         b.schedule.mode,
		Missed:       missed,
	}

	// Deliver the Record with a context that is not cancelled, so the final run
	// at shutdown still reaches a Handler that does ctx-bound work. beat does not
	// recover the work or the Handler: a panic propagates and crashes the process
	// (with a stack on stderr) unless job/middleware/recovery is used.
	b.handler.Handle(context.WithoutCancel(b.workCtx), record)

	return record
}

// runOnStart and runOnStop invoke hooks synchronously with their caller's
// context. Start supplies a fresh budget for rollback after cancelled startup.
func (b *Beat) runOnStart(ctx context.Context) error {
	if b.onStart == nil {
		return nil
	}

	return b.onStart(ctx)
}

func (b *Beat) runOnStop(ctx context.Context) error {
	if b.onStop == nil {
		return nil
	}

	return b.onStop(ctx)
}
