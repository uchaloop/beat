package beat

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// defaultJobTimeout bounds a single run when the effective JobTimeout is
	// unset (<= 0). Every run is bounded, so a Job that respects its context
	// cannot wedge the loop; see the package documentation on what the bound
	// does and does not promise.
	defaultJobTimeout = time.Minute

	// startupCleanupTimeout gives rollback a fresh, cooperative budget when
	// OnStart succeeds after its startup context has been cancelled.
	startupCleanupTimeout = 15 * time.Second

	// maxClockRecheckInterval bounds one sleep so the loop re-reads the clock at least this
	// often. A fixed-rate schedule aims at a wall-clock point, so a clock that
	// moved under a running process would otherwise go unnoticed until the
	// sleep ended, however long that was.
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

	// ErrStillRunning reports that startup or the run loop had not finished
	// when Stop stopped waiting. In-flight Job contexts are cancelled; an
	// in-progress OnStart still follows the context supplied to Start.
	// OnStop is not run in that case, and [Beat.Done] is how
	// an application learns when the run finally ends.
	ErrStillRunning = errors.New("stop deadline elapsed before the run finished")
)

// lifecycleState is the lifecycle of one Beat. It only moves forward: a stopped Beat
// stays stopped, because reviving cancelled contexts is more surprising than
// building a new Beat.
type lifecycleState int

const (
	stateNew lifecycleState = iota
	stateStarting
	stateRunning
	stateStopping
	stateStopped
)

// PanicError wraps a value recovered from a panic in the Job. The recovery
// middleware returns it as the run error, so a Handler can tell a panic from an
// ordinary error - Record.Outcome reads OutcomePanic - and inspect the
// recovered value and its stack. The beat core does not recover panics; without
// the recovery middleware a panic crashes the process (with a stack on stderr).
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("recovered from panic: %v", e.Value)
}

// Beat is a scheduled runner for one Job. Build it with MakeBeat and drive it
// with Start and Stop; the beatfx subpackage does that from the Fx lifecycle.
//
// It keeps two independent contexts: loopCtx gates scheduling (cancelled to stop
// launching new runs) and jobParentCtx parents each run (cancelled to abort
// an in-flight run). Splitting them lets stop drain - end scheduling while
// letting the current run finish - or cancel, depending on the graceful flag.
type Beat struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc

	jobParentCtx context.Context
	jobCancel    context.CancelFunc

	graceful bool

	job     Job
	handler Handler
	backoff func(Record) time.Duration

	schedule schedule

	jobTimeout time.Duration

	onStart func(context.Context) error
	onStop  func(context.Context) error

	// mu guards the lifecycle state and the result Stop leaves behind for any
	// later caller.
	mu               sync.Mutex
	state            lifecycleState
	stopErr          error
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

// MakeBeat resolves cfg against opts and builds a Beat. job is required; handler
// may be nil (a no-op handler is used). Drive the returned Beat with Start and
// Stop; the beatfx subpackage does this from the Fx lifecycle.
func MakeBeat(cfg Config, job Job, handler Handler, opts ...Option) (*Beat, error) {
	if job == nil {
		return nil, errors.New("job is required")
	}

	s := settings{handler: handler, mode: ModeFixedRate}
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

	jobTimeout := cfg.JobTimeout
	if jobTimeout <= 0 {
		jobTimeout = defaultJobTimeout
	}

	if s.handler == nil {
		s.handler = noopHandler{}
	}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	jobParentCtx, jobCancel := context.WithCancel(context.Background())

	return &Beat{
		loopCtx:      loopCtx,
		loopCancel:   loopCancel,
		jobParentCtx: jobParentCtx,
		jobCancel:    jobCancel,

		graceful: s.graceful,

		job:     chain(job, s.middleware),
		handler: s.handler,
		backoff: s.backoff,

		schedule: schedule{mode: s.mode, period: cfg.Period, offset: offset},

		jobTimeout: jobTimeout,

		onStart: s.onStart,
		onStop:  s.onStop,

		loopDone: make(chan struct{}),
		stopDone: make(chan struct{}),
	}, nil
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
		b.signalLoopDone()

		return errors.Join(startupErr, ctx.Err(), ErrStopped)
	}

	if startupErr != nil {
		b.state = stateStopped
		b.loopCancel()
		b.jobCancel()
		b.signalLoopDone()
		b.signalStopDone()
		b.mu.Unlock()

		return startupErr
	}

	b.state = stateRunning

	if err := ctx.Err(); err != nil {
		// The hook completed, so its resources need the paired cleanup even if
		// the caller never calls Stop after this failed Start.
		b.signalLoopDone()
		b.mu.Unlock()

		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), startupCleanupTimeout)
		defer cancel()

		return errors.Join(err, b.Stop(cleanupCtx))
	}

	go func() {
		defer b.signalLoopDone()

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
// against a Job that is still working would race it for the very resources the
// hook exists to release. [Beat.Done] reports when the run finally ends.
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

		b.signalLoopDone()
		b.signalStopDone()

		return nil

	case stateStopping, stateStopped:
		b.mu.Unlock()

		return b.awaitStopResult(ctx)
	}

	b.state = stateStopping
	b.mu.Unlock()

	err := b.finishStop(ctx)

	b.mu.Lock()
	b.stopErr = err
	b.state = stateStopped
	b.mu.Unlock()

	b.signalStopDone()

	return err
}

// Done returns a channel that closes when the run loop has left and no Job is
// in flight - or, for a Beat that never started, as soon as it is stopped.
//
// Stop returns on its own deadline and may return first, because a job timeout
// cancels a run's context but cannot interrupt it. This is how an application
// learns that the run has finally ended, so it can release, after the fact, a
// resource the Job was still using. Done does not wait for OnStop. Only use it
// for fallback cleanup after Stop returned ErrStillRunning; otherwise cleanup
// could overlap with OnStop.
func (b *Beat) Done() <-chan struct{} { return b.loopDone }

// finishStop does the work of the first Stop: end scheduling, wait for the loop, and
// run the hook only if the loop actually left.
func (b *Beat) finishStop(ctx context.Context) error {
	b.loopCancel()

	if !b.graceful {
		b.jobCancel()
	}

	select {
	case <-b.loopDone:
	case <-ctx.Done():
		b.jobCancel()

		// Completion wins if both notifications are already available.
		select {
		case <-b.loopDone:
		default:
			return errors.Join(ctx.Err(), ErrStillRunning)
		}
	}

	b.jobCancel()

	b.mu.Lock()
	startupSucceeded := b.startupSucceeded
	b.mu.Unlock()

	if !startupSucceeded {
		return nil
	}

	return b.runOnStop(ctx)
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

func (b *Beat) signalLoopDone() { b.loopDoneOnce.Do(func() { close(b.loopDone) }) }

func (b *Beat) signalStopDone() { b.stopDoneOnce.Do(func() { close(b.stopDone) }) }

// runLoop runs until scheduling stops. It owns the timing: nothing below it - not
// the Job, not a Middleware, not the Handler - decides when the next run
// happens, which is why a backoff is an option here rather than a pause taken
// inside the Job.
func (b *Beat) runLoop() {
	target := b.schedule.firstTarget(time.Now())
	missed := 0

	for {
		if !b.sleepUntil(target) {
			return
		}

		record := b.runOnce(target, missed)

		var backoff time.Duration
		if b.backoff != nil {
			backoff = b.backoff(record)
		}

		jobEnd := record.Start.Add(record.Duration)
		target, missed = b.schedule.nextTarget(time.Now(), target, jobEnd, backoff)
	}
}

// sleepUntil waits for target, waking at least every maxClockRecheckInterval to re-read the
// clock. It reports whether the run should still happen.
//
// A target already in the past returns at once, so a process that was away for a
// while runs once against a stale point - the Record says so, with a large gap
// between ScheduledFor and Start - and the run after it can follow soon. For
// work that drains a queue that is the useful behaviour; catching up point by
// point would not be.
func (b *Beat) sleepUntil(target time.Time) bool {
	for {
		if b.schedulingStopped() {
			return false
		}

		d := time.Until(target)
		if d <= 0 {
			return true
		}

		wait(b.loopCtx, min(d, maxClockRecheckInterval))
	}
}

func (b *Beat) schedulingStopped() bool {
	select {
	case <-b.loopCtx.Done():
		return true
	default:
		return false
	}
}

// runOnce executes the Job once and reports the result to the Handler.
func (b *Beat) runOnce(target time.Time, missed int) Record {
	b.iteration++

	// jobTimeout is always positive (MakeBeat defaults it), so every run is
	// bounded - as far as a context can bound anything. See the package
	// documentation: the deadline cancels the run, it does not interrupt it.
	runCtx, cancel := context.WithTimeout(b.jobParentCtx, b.jobTimeout)
	defer cancel()

	start := time.Now()
	processed, err := b.job(runCtx)
	duration := time.Since(start)

	record := Record{
		Iteration:    b.iteration,
		ScheduledFor: target,
		Start:        start,
		Duration:     duration,
		Period:       b.schedule.period,
		Mode:         b.schedule.mode,
		Processed:    processed,
		Err:          err,
		Outcome:      outcomeOf(runCtx, err),
		Missed:       missed,
	}

	// Deliver the Record with a context that is not cancelled, so the final run
	// at shutdown still reaches a Handler that does ctx-bound work. beat does not
	// recover the Job or the Handler: a panic propagates and crashes the process
	// (with a stack on stderr) unless the recovery middleware is used.
	b.handler.Handle(context.WithoutCancel(b.jobParentCtx), record)

	return record
}

// outcomeOf classifies a finished run. A panic outranks the rest because it is
// a defect and always worth surfacing. The deadline and the shutdown outrank
// the Job's own error because both mean the run was cut short, whatever it
// managed to return on the way out.
func outcomeOf(ctx context.Context, err error) Outcome {
	if _, ok := errors.AsType[*PanicError](err); ok {
		return OutcomePanic
	}

	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return OutcomeTimeout
	case errors.Is(ctx.Err(), context.Canceled):
		return OutcomeCanceled
	case err != nil:
		return OutcomeError
	}

	return OutcomeOK
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
