package beat

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// recorder collects the records a Beat produces. The mutex is not about the
// loop racing the assertions - inside a synctest bubble the loop is durably
// blocked by the time a test looks - it is about saying so to the race
// detector.
type recorder struct {
	mu   sync.Mutex
	recs []Record
}

func (r *recorder) Handle(_ context.Context, rec Record) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.recs = append(r.recs, rec)
}

func (r *recorder) all() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.recs)
}

func noopJob(context.Context) (int, error) { return 0, nil }

// start builds a Beat and launches it, failing the test if either step does.
func start(t *testing.T, cfg Config, job Job, h Handler, opts ...Option) *Beat {
	t.Helper()

	b, err := MakeBeat(cfg, job, h, opts...)
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	return b
}

// offsets reports when each run was scheduled, relative to from.
func offsets(recs []Record, from time.Time) []time.Duration {
	out := make([]time.Duration, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.ScheduledFor.Sub(from))
	}

	return out
}

func TestBeat_FixedRateKeepsTheGrid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Second},
			func(context.Context) (int, error) { return 7, nil }, &rec)

		synctest.Sleep(350 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		got := offsets(rec.all(), begin)
		want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}
		if !slices.Equal(got, want) {
			t.Fatalf("runs at %v, want %v", got, want)
		}

		for _, r := range rec.all() {
			if r.Processed != 7 || r.Outcome != OutcomeOK || r.Missed != 0 {
				t.Errorf("unexpected record %+v", r)
			}
			if r.Period != 100*time.Millisecond {
				t.Errorf("Period = %v, want 100ms", r.Period)
			}
			if r.Mode != ModeFixedRate {
				t.Errorf("Mode = %q, want %q", r.Mode, ModeFixedRate)
			}
		}
	})
}

func TestBeat_LongRunMissesPointsAndReportsThem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Second},
			func(ctx context.Context) (int, error) {
				// Two and a half periods: the run covers the points at +200ms
				// and +300ms, which must be reported, not queued.
				time.Sleep(250 * time.Millisecond)

				return 1, nil
			}, &rec)

		synctest.Sleep(500 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) != 2 {
			t.Fatalf("got %d runs, want 2: %v", len(recs), offsets(recs, begin))
		}

		if got, want := recs[0].ScheduledFor.Sub(begin), 100*time.Millisecond; got != want {
			t.Errorf("first run at %v, want %v", got, want)
		}
		if got, want := recs[1].ScheduledFor.Sub(begin), 400*time.Millisecond; got != want {
			t.Errorf("second run at %v, want %v", got, want)
		}
		if recs[1].Missed != 2 {
			t.Errorf("Missed = %d, want 2", recs[1].Missed)
		}
		if recs[0].Missed != 0 {
			t.Errorf("first run Missed = %d, want 0", recs[0].Missed)
		}
	})
}

// TestBeat_SlowHandlerCostsPoints is the case a review found empirically: the
// Handler runs inside the loop, after the Job, so one slow enough to outlast a
// point costs it - and the loop then arrives on time for the next, which is why
// the lateness of a run says nothing about it. Only Missed does.
func TestBeat_SlowHandlerCostsPoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder

		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Minute},
			noopJob,
			MultiHandler(&rec, HandlerFunc(func(context.Context, Record) {
				time.Sleep(150 * time.Millisecond)
			})))

		synctest.Sleep(700 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) < 2 {
			t.Fatalf("got %d runs, want at least 2", len(recs))
		}

		if recs[1].Missed != 1 {
			t.Errorf("Missed = %d, want 1", recs[1].Missed)
		}
		// The run after a lost point is still on the grid, so it is not late.
		if late := recs[1].Start.Sub(recs[1].ScheduledFor); late != 0 {
			t.Errorf("lateness = %v, want 0 - the loop caught the next point", late)
		}
	})
}

func TestBeat_FixedDelayMeasuresFromTheEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Second},
			func(context.Context) (int, error) {
				time.Sleep(50 * time.Millisecond)

				return 1, nil
			}, &rec, WithMode(ModeFixedDelay))

		synctest.Sleep(320 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		// No grid: the first run is immediate and each period starts counting
		// when the previous run ended, so the cadence is 150ms.
		got := offsets(rec.all(), begin)
		want := []time.Duration{0, 150 * time.Millisecond, 300 * time.Millisecond}
		if !slices.Equal(got, want) {
			t.Fatalf("runs at %v, want %v", got, want)
		}

		for _, r := range rec.all() {
			if r.Missed != 0 {
				t.Errorf("fixed delay reported Missed = %d", r.Missed)
			}
		}
	})
}

func TestBeat_OffsetShiftsTheGrid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Second},
			noopJob, &rec, WithOffset(30*time.Millisecond))

		synctest.Sleep(250 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		got := offsets(rec.all(), begin)
		want := []time.Duration{30 * time.Millisecond, 130 * time.Millisecond, 230 * time.Millisecond}
		if !slices.Equal(got, want) {
			t.Fatalf("runs at %v, want %v", got, want)
		}
	})
}

func TestBeat_BackoffHoldsTheLoopWithoutTouchingDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Second},
			func(context.Context) (int, error) { return 0, nil }, &rec,
			WithBackoff(func(r Record) time.Duration {
				if r.Processed == 0 {
					return 250 * time.Millisecond
				}

				return 0
			}))

		synctest.Sleep(500 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		got := offsets(rec.all(), begin)
		want := []time.Duration{100 * time.Millisecond, 400 * time.Millisecond}
		if !slices.Equal(got, want) {
			t.Fatalf("runs at %v, want %v", got, want)
		}

		// The pause is the loop's, not the job's: it must not show up as work.
		for _, r := range rec.all() {
			if r.Duration != 0 {
				t.Errorf("Duration = %v, want 0 - the backoff leaked into it", r.Duration)
			}
		}
		// The pause moved the run past two points, but the application asked for
		// the pause - they are its schedule, not a loss.
		if got := rec.all()[1].Missed; got != 0 {
			t.Errorf("Missed = %d, want 0 - a backoff is not a loss", got)
		}
	})
}

func TestBeat_TimeoutIsReportedEvenWhenTheJobSwallowsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		b := start(t, Config{Period: time.Second, JobTimeout: 100 * time.Millisecond},
			func(ctx context.Context) (int, error) {
				// Ignores ctx entirely and reports success, the way a job that
				// forgot to propagate cancellation does.
				time.Sleep(300 * time.Millisecond)

				return 3, nil
			}, &rec)

		synctest.Sleep(1500 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) == 0 {
			t.Fatal("no runs")
		}
		if got := recs[0].Outcome; got != OutcomeTimeout {
			t.Errorf("Outcome = %q, want %q", got, OutcomeTimeout)
		}
		if recs[0].Err != nil {
			t.Errorf("Err = %v, want nil - the job reported success", recs[0].Err)
		}
	})
}

func TestBeat_ShutdownIsCanceledNotAnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Minute},
			func(ctx context.Context) (int, error) {
				<-ctx.Done() // returns only once stop cancels the run

				return 0, ctx.Err()
			}, &rec)

		synctest.Sleep(150 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) != 1 {
			t.Fatalf("got %d runs, want 1", len(recs))
		}
		if got := recs[0].Outcome; got != OutcomeCanceled {
			t.Errorf("Outcome = %q, want %q", got, OutcomeCanceled)
		}
	})
}

func TestBeat_GracefulStopLetsTheRunFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		var sawCancel bool

		b := start(t, Config{Period: 100 * time.Millisecond, JobTimeout: time.Minute},
			func(ctx context.Context) (int, error) {
				time.Sleep(200 * time.Millisecond)
				sawCancel = ctx.Err() != nil

				return 5, nil
			}, &rec, WithGracefulStop())

		synctest.Sleep(150 * time.Millisecond) // the run is in flight

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		if sawCancel {
			t.Error("graceful stop cancelled the in-flight run's context")
		}

		recs := rec.all()
		if len(recs) != 1 || recs[0].Outcome != OutcomeOK || recs[0].Processed != 5 {
			t.Fatalf("records = %+v, want one finished run", recs)
		}
	})
}

func TestBeat_ErrorReachesTheRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond},
			func(context.Context) (int, error) { return 0, errors.New("boom") }, &rec)

		synctest.Sleep(150 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) != 1 {
			t.Fatalf("got %d runs, want 1", len(recs))
		}
		if recs[0].Err == nil || recs[0].Err.Error() != "boom" {
			t.Errorf("Err = %v, want boom", recs[0].Err)
		}
		if recs[0].Outcome != OutcomeError {
			t.Errorf("Outcome = %q, want %q", recs[0].Outcome, OutcomeError)
		}
	})
}

func TestBeat_HooksRunAroundTheLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var order []string

		b := start(t, Config{Period: 100 * time.Millisecond}, noopJob, nil,
			WithOnStart(func(context.Context) error {
				order = append(order, "start")

				return nil
			}),
			WithOnStop(func(context.Context) error {
				order = append(order, "stop")

				return nil
			}))

		synctest.Sleep(150 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		if !slices.Equal(order, []string{"start", "stop"}) {
			t.Errorf("hooks ran %v, want [start stop]", order)
		}
	})
}

func TestStart_FailingHookAbortsStartup(t *testing.T) {
	b, err := MakeBeat(Config{Period: time.Second}, noopJob, nil,
		WithOnStart(func(context.Context) error { return errors.New("no") }))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded despite a failing hook")
	}
}

// The recovery middleware lives in a subpackage that imports beat, so the
// mapping is asserted here with a middleware that returns what recovery
// returns: a *PanicError.
func TestBeat_PanicErrorReadsAsOutcomePanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond}, noopJob, &rec,
			WithMiddleware(func(Job) Job {
				return func(context.Context) (int, error) {
					return 0, &PanicError{Value: "boom", Stack: []byte("stack")}
				}
			}))

		synctest.Sleep(150 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) != 1 {
			t.Fatalf("got %d runs, want 1", len(recs))
		}
		if recs[0].Outcome != OutcomePanic {
			t.Errorf("Outcome = %q, want %q", recs[0].Outcome, OutcomePanic)
		}
	})
}

func TestChainOrder_FirstMiddlewareRunsFirst(t *testing.T) {
	var order []string

	mw := func(name string) Middleware {
		return func(next Job) Job {
			return func(ctx context.Context) (int, error) {
				order = append(order, name)

				return next(ctx)
			}
		}
	}

	job := chain(
		func(context.Context) (int, error) {
			order = append(order, "job")

			return 0, nil
		},
		[]Middleware{mw("a"), mw("b"), mw("c")},
	)

	if _, err := job(context.Background()); err != nil {
		t.Fatalf("job: %v", err)
	}

	if want := []string{"a", "b", "c", "job"}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestChain_SkipsNil(t *testing.T) {
	called := false
	job := chain(
		func(context.Context) (int, error) { called = true; return 0, nil },
		[]Middleware{nil, nil},
	)

	if _, err := job(context.Background()); err != nil {
		t.Fatalf("job: %v", err)
	}
	if !called {
		t.Fatal("job was not called")
	}
}

func TestConfigNameIsBeat(t *testing.T) {
	if got := (Config{}).ConfigName(); got != "beat" {
		t.Fatalf("ConfigName() = %q, want beat", got)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{name: "a period is enough", cfg: Config{Period: time.Second}, ok: true},
		{name: "no period", cfg: Config{}},
		{name: "negative period", cfg: Config{Period: -time.Second}},
		{name: "negative job timeout", cfg: Config{Period: time.Second, JobTimeout: -1}},
		{name: "negative jitter", cfg: Config{Period: time.Second, Jitter: -1}},
		{name: "jitter at the period", cfg: Config{Period: time.Second, Jitter: time.Second}, ok: true},
		{name: "jitter past the period", cfg: Config{Period: time.Second, Jitter: 2 * time.Second}},
		{name: "jitter below the period", cfg: Config{Period: time.Second, Jitter: 999 * time.Millisecond}, ok: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()

			if tc.ok && err != nil {
				t.Errorf("rejected a valid config: %v", err)
			}
			if !tc.ok && err == nil {
				t.Error("accepted an invalid config")
			}
		})
	}
}

func TestMakeBeat_Rejects(t *testing.T) {
	if _, err := MakeBeat(Config{Period: time.Second}, nil, nil); err == nil {
		t.Error("accepted a nil job")
	}
	if _, err := MakeBeat(Config{}, noopJob, nil); err == nil {
		t.Error("accepted a config without a period")
	}
	if _, err := MakeBeat(Config{Period: time.Second}, noopJob, nil, WithMode("hourly")); err == nil {
		t.Error("accepted an unknown mode")
	}
	if _, err := MakeBeat(Config{Period: time.Second}, noopJob, nil, WithOffset(time.Second)); err == nil {
		t.Error("accepted an offset equal to the period")
	}
	if _, err := MakeBeat(Config{Period: time.Second}, noopJob, nil, WithOffset(-1)); err == nil {
		t.Error("accepted a negative offset")
	}
}

// TestMakeBeat_JitterMayEqualThePeriod covers the rule that used to be one step
// too strict. The draw is half-open, so a jitter equal to the period still only
// produces offsets inside it - and spreading replicas over the whole period is
// exactly what a polling deployment wants.
func TestMakeBeat_JitterMayEqualThePeriod(t *testing.T) {
	const period = time.Second

	for range 100 {
		b, err := MakeBeat(Config{Period: period, Jitter: period}, noopJob, nil)
		if err != nil {
			t.Fatalf("MakeBeat: %v", err)
		}

		if b.schedule.offset < 0 || b.schedule.offset >= period {
			t.Fatalf("offset %v is outside [0, %v)", b.schedule.offset, period)
		}
	}

	// A concrete offset stays stricter: exactly one period is degenerate.
	if _, err := MakeBeat(Config{Period: period}, noopJob, nil, WithOffset(period)); err == nil {
		t.Error("accepted an offset equal to the period")
	}
}

// TestSleepUntil_APastTargetRunsAtOnce pins what a process does after being away
// for a while: it serves the stale point immediately rather than waiting for the
// next one, and the Record carries the gap. Catching up point by point is the
// behaviour this deliberately does not have.
func TestSleepUntil_APastTargetRunsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, err := MakeBeat(Config{Period: time.Hour}, noopJob, nil)
		if err != nil {
			t.Fatalf("MakeBeat: %v", err)
		}

		begin := time.Now()
		if !b.sleepUntil(begin.Add(-24 * time.Hour)) {
			t.Fatal("sleepUntil refused a target that is already past")
		}

		if waited := time.Since(begin); waited != 0 {
			t.Errorf("waited %v for a point already past, want none", waited)
		}
	})
}

func TestMakeBeat_Defaults(t *testing.T) {
	b, err := MakeBeat(Config{Period: time.Second}, noopJob, nil)
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if b.jobTimeout != defaultJobTimeout {
		t.Errorf("jobTimeout = %v, want %v", b.jobTimeout, defaultJobTimeout)
	}
	if b.schedule.mode != ModeFixedRate {
		t.Errorf("mode = %q, want %q", b.schedule.mode, ModeFixedRate)
	}
}
