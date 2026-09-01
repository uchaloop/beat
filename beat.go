package beat

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// defaultJobTimeout bounds a single run when the effective JobTimeout is unset
// (<= 0). Every run is bounded, so a Job that ignores ctx cannot silently wedge
// the loop; a legitimately long Job should set a larger JobTimeout.
const defaultJobTimeout = time.Minute

// PanicError wraps a value recovered from a panic in the Job. The recovery
// middleware returns it as the run error, so a Handler can tell a panic from an
// ordinary error with errors.As and inspect the recovered value and its stack.
// The beat core does not recover panics; without the recovery middleware a
// panic crashes the process (with a stack on stderr).
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("recovered from panic: %v", e.Value)
}

// Beat is a scheduled runner for one Job. Build it with Module; the Fx
// lifecycle drives its hooks and loop.
//
// It keeps two independent contexts: loopCtx gates scheduling (cancelled to stop
// launching new runs) and jobCtx is the parent of each run (cancelled to abort
// an in-flight run). Splitting them lets stop drain - end scheduling while
// letting the current run finish - or cancel, depending on the graceful flag.
type Beat struct {
	loopCtx    context.Context
	loopCancel context.CancelFunc
	jobCtx     context.Context
	jobCancel  context.CancelFunc

	graceful bool

	job     Job
	handler Handler

	sched cron.Schedule
	mode  Mode

	jobTimeout time.Duration
	delay      time.Duration

	onStart func(context.Context) error
	onStop  func(context.Context) error

	iteration int64
	wg        sync.WaitGroup
}

// MakeBeat resolves cfg against opts and builds a Beat. job is required; handler
// may be nil (a no-op handler is used). Drive the returned Beat with Start and
// Stop; the beatfx subpackage does this from the Fx lifecycle.
func MakeBeat(cfg Config, job Job, handler Handler, opts ...Option) (*Beat, error) {
	if job == nil {
		return nil, errors.New("job is required")
	}

	s := settings{handler: handler}
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

	jobTimeout := cfg.JobTimeout
	if jobTimeout <= 0 {
		jobTimeout = defaultJobTimeout
	}

	sched, err := parseSchedule(cfg.Spec)
	if err != nil {
		return nil, fmt.Errorf("parse spec: %w", err)
	}

	delay, err := randomDelay(cfg.Jitter)
	if err != nil {
		return nil, fmt.Errorf("generate jitter: %w", err)
	}

	if s.handler == nil {
		s.handler = noopHandler{}
	}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	jobCtx, jobCancel := context.WithCancel(context.Background())

	mode := ModeCron
	if isEverySpec(cfg.Spec) {
		mode = ModeInterval
	}

	return &Beat{
		loopCtx:    loopCtx,
		loopCancel: loopCancel,
		jobCtx:     jobCtx,
		jobCancel:  jobCancel,

		graceful: s.graceful,

		job:     chain(job, s.middleware),
		handler: s.handler,

		sched: sched,
		mode:  mode,

		jobTimeout: jobTimeout,
		delay:      delay,

		onStart: s.onStart,
		onStop:  s.onStop,
	}, nil
}

// Start runs the OnStart hook and launches the run loop. It is the OnStart half
// of the lifecycle; the loop runs in its own goroutine, so Start returns once it
// is launched. ctx bounds the hook. A returned error aborts startup.
func (b *Beat) Start(ctx context.Context) error {
	if err := b.runOnStart(ctx); err != nil {
		return err
	}

	select {
	case <-b.loopCtx.Done():
		return b.loopCtx.Err()
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	b.wg.Go(b.loop)

	return nil
}

// Stop ends scheduling, waits for the loop to finish (bounded by ctx), then runs
// the OnStop hook. By default the in-flight run's context is cancelled at once;
// with graceful stop the run is left to finish and is only cancelled if it
// outlives ctx.
func (b *Beat) Stop(ctx context.Context) error {
	b.loopCancel()

	if !b.graceful {
		b.jobCancel()
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	var loopErr error
	select {
	case <-done:
		b.jobCancel() // release the run context (idempotent)
	case <-ctx.Done():
		b.jobCancel() // last resort: abort a run that outlived the deadline
		loopErr = ctx.Err()
	}

	return errors.Join(loopErr, b.runOnStop(ctx))
}

func (b *Beat) loop() {
	if b.mode == ModeInterval {
		b.loopInterval()

		return
	}

	b.loopCron()
}

// loopInterval runs the first tick after the one-time start delay, then every
// interval.
func (b *Beat) loopInterval() {
	Wait(b.loopCtx, b.delay)
	b.runOnce()

	for {
		if b.stopped() {
			return
		}

		Wait(b.loopCtx, time.Until(b.sched.Next(time.Now())))
		b.runOnce()
	}
}

// loopCron runs at each cron point, offset by the per-iteration delay.
func (b *Beat) loopCron() {
	for {
		if b.stopped() {
			return
		}

		Wait(b.loopCtx, time.Until(b.sched.Next(time.Now()).Add(b.delay)))
		b.runOnce()
	}
}

func (b *Beat) stopped() bool {
	select {
	case <-b.loopCtx.Done():
		return true
	default:
		return false
	}
}

// runOnce executes the Job once and reports the result to the Handler. It skips
// the run if scheduling has already stopped.
func (b *Beat) runOnce() {
	if b.stopped() {
		return
	}

	b.iteration++

	// jobTimeout is always positive (makeBeat defaults it), so every run is
	// bounded.
	jobCtx, cancel := context.WithTimeout(b.jobCtx, b.jobTimeout)
	defer cancel()

	start := time.Now()
	processed, err := b.job(jobCtx)

	// Deliver the Record with a context that is not cancelled, so the final run
	// at shutdown still reaches a Handler that does ctx-bound work. beat does not
	// recover the Job or the Handler: a panic propagates and crashes the process
	// (with a stack on stderr) unless the recovery middleware is used.
	b.handler.Handle(
		context.WithoutCancel(b.jobCtx),
		Record{
			Iteration: b.iteration,
			Start:     start,
			Duration:  time.Since(start),
			Processed: processed,
			Err:       err,
			Mode:      b.mode,
		},
	)
}

// runOnStart and runOnStop invoke the OnStart/OnStop hooks with the context Fx
// passes in. That context is already bounded by the application's
// fx.StartTimeout / fx.StopTimeout (default 15s each), shared across all of the
// app's hooks - beat adds no timeout of its own. A hook that needs longer than
// the default must be matched by raising fx.StartTimeout / fx.StopTimeout on the
// top-level App.
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
