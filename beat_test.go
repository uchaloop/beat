package beat

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/uchaloop/job"
	"github.com/uchaloop/job/assignment"
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

func noopWork(context.Context) (int, error) { return 0, nil }

// runnerFor builds a job.Runner with a generous timeout, which is the runner's
// business and not the schedule's.
func runnerFor(t *testing.T, fn job.Func, opts ...job.Option) *job.Runner {
	t.Helper()

	r, err := job.MakeRunner(job.Config{Timeout: time.Minute}, fn, opts...)
	if err != nil {
		t.Fatalf("MakeRunner: %v", err)
	}

	return r
}

// start builds a Beat and launches it, failing the test if either step does.
func start(t *testing.T, cfg Config, fn job.Func, h Handler, opts ...Option) *Beat {
	t.Helper()

	if h != nil {
		opts = append([]Option{WithHandler(h)}, opts...)
	}

	b, err := MakeBeat(cfg, runnerFor(t, fn), opts...)
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
		b := start(t, Config{Period: 100 * time.Millisecond},
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
			if r.Result.Processed != 7 || r.Result.Outcome != job.OutcomeOK || r.Missed != 0 {
				t.Errorf("unexpected record %+v", r)
			}
			if r.Period != 100*time.Millisecond || r.LocalPeriod != 100*time.Millisecond {
				t.Errorf("Period = %v, LocalPeriod = %v, want 100ms each", r.Period, r.LocalPeriod)
			}
			if r.Mode != ModeFixedRate {
				t.Errorf("Mode = %q, want %q", r.Mode, ModeFixedRate)
			}
			// Without a rotation the nominal point is the offset point.
			if !r.GridPoint.Equal(r.ScheduledFor) {
				t.Errorf("GridPoint = %v, ScheduledFor = %v", r.GridPoint, r.ScheduledFor)
			}
		}
	})
}

func TestBeat_LongRunMissesPointsAndReportsThem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond},
			func(context.Context) (int, error) {
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

// The Handler runs inside the loop, after the work, so one slow enough to
// outlast a point costs it - and the loop then arrives on time for the next,
// which is why the lateness of a run says nothing about it. Only Missed does.
func TestBeat_SlowHandlerCostsPoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder

		b := start(t, Config{Period: 100 * time.Millisecond}, noopWork,
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
		if late := recs[1].Result.Start.Sub(recs[1].ScheduledFor); late != 0 {
			t.Errorf("lateness = %v, want 0 - the loop caught the next point", late)
		}
	})
}

func TestBeat_FixedDelayMeasuresFromTheEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond},
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
		b := start(t, Config{Period: 100 * time.Millisecond}, noopWork, &rec,
			WithOffset(30*time.Millisecond))

		synctest.Sleep(250 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		got := offsets(rec.all(), begin)
		want := []time.Duration{30 * time.Millisecond, 130 * time.Millisecond, 230 * time.Millisecond}
		if !slices.Equal(got, want) {
			t.Fatalf("runs at %v, want %v", got, want)
		}

		// The nominal point is the shared grid, which the offset does not move.
		for _, r := range rec.all() {
			if r.ScheduledFor.Sub(r.GridPoint) != 30*time.Millisecond {
				t.Errorf("GridPoint %v is not one offset before ScheduledFor %v", r.GridPoint, r.ScheduledFor)
			}
		}
	})
}

func TestBeat_BackoffHoldsTheLoopWithoutCountingItAsLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond},
			func(context.Context) (int, error) { return 0, nil }, &rec,
			WithBackoff(func(r Record) time.Duration {
				if r.Result.Processed == 0 {
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

		// The pause is the loop's, not the work's: it must not show up as work.
		for _, r := range rec.all() {
			if r.Result.Duration != 0 {
				t.Errorf("Duration = %v, want 0 - the backoff leaked into it", r.Result.Duration)
			}
		}

		// The pause moved the run past two points, but the application asked for
		// the pause - they are its schedule, not a loss.
		if got := rec.all()[1].Missed; got != 0 {
			t.Errorf("Missed = %d, want 0 - a backoff is not a loss", got)
		}
	})
}

func TestBeat_TimeoutReachesTheRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder

		runner, err := job.MakeRunner(job.Config{Timeout: 100 * time.Millisecond},
			func(context.Context) (int, error) {
				// Ignores ctx entirely and reports success.
				time.Sleep(300 * time.Millisecond)

				return 3, nil
			})
		if err != nil {
			t.Fatalf("MakeRunner: %v", err)
		}

		b, err := MakeBeat(Config{Period: time.Second}, runner, WithHandler(&rec))
		if err != nil {
			t.Fatalf("MakeBeat: %v", err)
		}
		if err := b.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		synctest.Sleep(1500 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) == 0 {
			t.Fatal("no runs")
		}
		if got := recs[0].Result.Outcome; got != job.OutcomeTimeout {
			t.Errorf("Outcome = %q, want %q", got, job.OutcomeTimeout)
		}
		if recs[0].Result.Err != nil {
			t.Errorf("Err = %v, want nil - the work reported success", recs[0].Result.Err)
		}
	})
}

func TestBeat_ShutdownIsCanceledNotAnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		b := start(t, Config{Period: 100 * time.Millisecond},
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
		if got := recs[0].Result.Outcome; got != job.OutcomeCanceled {
			t.Errorf("Outcome = %q, want %q", got, job.OutcomeCanceled)
		}
	})
}

func TestBeat_GracefulStopLetsTheRunFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder
		var sawCancel bool

		b := start(t, Config{Period: 100 * time.Millisecond},
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
		if len(recs) != 1 || recs[0].Result.Outcome != job.OutcomeOK || recs[0].Result.Processed != 5 {
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
		if recs[0].Result.Err == nil || recs[0].Result.Err.Error() != "boom" {
			t.Errorf("Err = %v, want boom", recs[0].Result.Err)
		}
		if recs[0].Result.Outcome != job.OutcomeError {
			t.Errorf("Outcome = %q, want %q", recs[0].Result.Outcome, job.OutcomeError)
		}
	})
}

func TestBeat_HooksRunAroundTheLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var order []string

		b := start(t, Config{Period: 100 * time.Millisecond}, noopWork, nil,
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
	b, err := MakeBeat(Config{Period: time.Second}, runnerFor(t, noopWork),
		WithOnStart(func(context.Context) error { return errors.New("no") }))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded despite a failing hook")
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
		{name: "negative jitter", cfg: Config{Period: time.Second, Jitter: -1}},
		{name: "jitter at the period", cfg: Config{Period: time.Second, Jitter: time.Second}, ok: true},
		{name: "jitter past the period", cfg: Config{Period: time.Second, Jitter: 2 * time.Second}},
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
	runner := runnerFor(t, noopWork)

	if _, err := MakeBeat(Config{Period: time.Second}, nil); err == nil {
		t.Error("accepted a nil runner")
	}
	if _, err := MakeBeat(Config{}, runner); err == nil {
		t.Error("accepted a config without a period")
	}
	if _, err := MakeBeat(Config{Period: time.Second}, runner, WithMode("hourly")); err == nil {
		t.Error("accepted an unknown mode")
	}
	if _, err := MakeBeat(Config{Period: time.Second}, runner, WithOffset(time.Second)); err == nil {
		t.Error("accepted an offset equal to the period")
	}
	if _, err := MakeBeat(Config{Period: time.Second}, runner, WithOffset(-1)); err == nil {
		t.Error("accepted a negative offset")
	}
}

func TestMakeBeat_JitterMayEqualThePeriod(t *testing.T) {
	const period = time.Second

	for range 100 {
		b, err := MakeBeat(Config{Period: period, Jitter: period}, runnerFor(t, noopWork))
		if err != nil {
			t.Fatalf("MakeBeat: %v", err)
		}

		if b.schedule.offset < 0 || b.schedule.offset >= period {
			t.Fatalf("offset %v is outside [0, %v)", b.schedule.offset, period)
		}
	}
}

func TestSleepUntil_APastTargetRunsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork))
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
	b, err := MakeBeat(Config{Period: time.Second}, runnerFor(t, noopWork))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if b.schedule.mode != ModeFixedRate {
		t.Errorf("mode = %q, want %q", b.schedule.mode, ModeFixedRate)
	}
	if b.localPeriod != time.Second {
		t.Errorf("localPeriod = %v, want 1s without a rotation", b.localPeriod)
	}
	if b.rotation != nil {
		t.Error("a rotation was configured without being asked for")
	}
}

// --- cluster rotation ---

func rotationFor(t *testing.T, current string, period time.Duration) *assignment.Rotation {
	t.Helper()

	r, err := assignment.MakeRotation(assignment.Config{
		Clusters: []string{"el", "xc", "dm"}, // sorted: dm, el, xc
		Current:  current,
		Period:   period,
	})
	if err != nil {
		t.Fatalf("MakeRotation: %v", err)
	}

	return r
}

func TestMakeBeat_RejectsAnUnusableRotation(t *testing.T) {
	runner := runnerFor(t, noopWork)

	_, err := MakeBeat(Config{Period: time.Second}, runner,
		WithMode(ModeFixedDelay), WithAssignment(rotationFor(t, "el", time.Second)))
	if err == nil {
		t.Error("accepted a rotation under fixed delay, which has no shared grid")
	}

	_, err = MakeBeat(Config{Period: time.Second}, runner,
		WithAssignment(rotationFor(t, "el", 2*time.Second)))
	if err == nil {
		t.Error("accepted a rotation describing a different grid")
	}
}

func TestMakeBeat_LocalPeriodFollowsTheRotation(t *testing.T) {
	b, err := MakeBeat(Config{Period: time.Second}, runnerFor(t, noopWork),
		WithAssignment(rotationFor(t, "el", time.Second)))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if b.localPeriod != 3*time.Second {
		t.Errorf("localPeriod = %v, want 3s across three clusters", b.localPeriod)
	}
}

// Sorted the clusters are dm, el, xc, so el owns every third point. The bubble
// starts at midnight UTC 2000-01-01, which is slot 0 of a 100ms grid.
func TestBeat_RotationRunsOnlyItsOwnPoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		begin := time.Now()

		var rec recorder
		var decisionCount atomic.Int64

		b := start(t, Config{Period: 100 * time.Millisecond}, noopWork, &rec,
			WithAssignment(rotationFor(t, "el", 100*time.Millisecond)),
			WithDecisionHandler(func(assignment.Decision) { decisionCount.Add(1) }))

		synctest.Sleep(650 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		// el owns slots 1, 4, 7 ... of the grid.
		got := offsets(rec.all(), begin)
		want := []time.Duration{100 * time.Millisecond, 400 * time.Millisecond}
		if !slices.Equal(got, want) {
			t.Fatalf("runs at %v, want %v", got, want)
		}

		// Every point produced a decision, including the ones it declined.
		if n := decisionCount.Load(); n < 6 {
			t.Errorf("decisions = %d, want one per point (>= 6)", n)
		}

		for _, r := range rec.all() {
			if r.LocalPeriod != 300*time.Millisecond {
				t.Errorf("LocalPeriod = %v, want 300ms", r.LocalPeriod)
			}
			// Foreign points are not this process's to miss.
			if r.Missed != 0 {
				t.Errorf("Missed = %d, want 0 - the skipped points belong to others", r.Missed)
			}
			if r.Iteration == 0 {
				t.Error("Iteration did not advance for a run that happened")
			}
		}
	})
}

// A run long enough to cover a whole turn of the rotation loses one of its own
// points, and only that one.
func TestBeat_RotationCountsOnlyItsOwnMissedPoints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var rec recorder

		b := start(t, Config{Period: 100 * time.Millisecond},
			func(context.Context) (int, error) {
				// Covers the next three points: two foreign, one of ours.
				time.Sleep(350 * time.Millisecond)

				return 1, nil
			}, &rec,
			WithAssignment(rotationFor(t, "el", 100*time.Millisecond)))

		synctest.Sleep(1200 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		recs := rec.all()
		if len(recs) < 2 {
			t.Fatalf("got %d runs, want at least 2", len(recs))
		}
		if recs[1].Missed != 1 {
			t.Errorf("Missed = %d, want 1 - only our own lost point counts", recs[1].Missed)
		}
	})
}

// The loop cannot reach this through its own grid - MakeBeat pins the rotation
// to the same period, so every nominal point is a valid one - but the contract
// for a loop that stops by itself is worth holding to.
func TestBeat_ATerminalLoopErrorSurfacesAtStop(t *testing.T) {
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fatal := errors.New("policy cannot decide this point")
	b.endLoopWithError(fatal)

	// Scheduling ended, so the loop leaves and Done closes without a Stop.
	select {
	case <-b.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the loop did not leave after a terminal error")
	}

	if err := b.Stop(context.Background()); !errors.Is(err, fatal) {
		t.Errorf("Stop = %v, want the terminal error", err)
	}
}

// soleOwner is a rotation of one cluster, which therefore owns every point. It
// exists so a test can reach the decision handler at all.
func soleOwner(t *testing.T, period time.Duration) *assignment.Rotation {
	t.Helper()

	rotation, err := assignment.MakeRotation(assignment.Config{
		Clusters: []string{"el"}, Current: "el", Period: period,
	})
	if err != nil {
		t.Fatalf("MakeRotation: %v", err)
	}

	return rotation
}

// A graceful stop promises to let the run in flight finish while starting no
// new one. The decision handler is application code between the wait and the
// work, so a stop landing inside it used to be followed by a fresh attempt.
func TestStop_GracefulDuringTheDecisionHandlerStartsNoWork(t *testing.T) {
	var calls atomic.Int64

	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool

	b, err := MakeBeat(Config{Period: 50 * time.Millisecond},
		runnerFor(t, func(context.Context) (int, error) {
			calls.Add(1)

			return 0, nil
		}),
		WithGracefulStop(),
		WithAssignment(soleOwner(t, 50*time.Millisecond)),
		WithDecisionHandler(func(assignment.Decision) {
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		}))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	<-entered // the loop is inside the handler, before any work

	stopped := make(chan error, 1)
	go func() { stopped <- b.Stop(context.Background()) }()

	<-b.loopCtx.Done() // Stop has actually cancelled scheduling.
	close(release)     // The handler returns.

	if err := <-stopped; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if n := calls.Load(); n != 0 {
		t.Errorf("the work ran %d times after a graceful stop, want 0", n)
	}
}

// A decision handler without a rotation could never fire, so it is a
// configuration mistake rather than a quiet no-op.
func TestMakeBeat_RejectsADecisionHandlerWithoutARotation(t *testing.T) {
	_, err := MakeBeat(Config{Period: time.Second}, runnerFor(t, noopWork),
		WithDecisionHandler(func(assignment.Decision) {}))
	if err == nil {
		t.Error("accepted a decision handler that can never be called")
	}
}

// The accept boundary is the lifecycle state, read under the mutex Stop moves
// it with - not a context, which could be cancelled between a check and the
// call that follows it.
func TestAcceptsAttempt_FollowsTheLifecycleState(t *testing.T) {
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	tests := []struct {
		name  string
		state lifecycleState
		want  bool
	}{
		{"new", stateNew, false},
		{"starting", stateStarting, false},
		{"running", stateRunning, true},
		{"stopping", stateStopping, false},
		{"stopped", stateStopped, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b.mu.Lock()
			b.state = tc.state
			b.mu.Unlock()

			if got := b.acceptsAttempt(); got != tc.want {
				t.Errorf("acceptsAttempt() = %v in %s, want %v", got, tc.name, tc.want)
			}
		})
	}
}

// job.Runner reports a zero Start when the caller's context was already done,
// because nothing ran and nothing was measured. That value must not reach the
// schedule: a backoff measured from year one would never hold anything back.
//
// Only a shutdown reaches this in the loop, and the loop leaves straight after,
// so the contract is pinned here rather than through a run.
func TestAttemptEnd_NeverReturnsAMeaninglessPoint(t *testing.T) {
	before := time.Now()

	got := attemptEnd(job.Result{}) // nothing ran

	if got.Before(before) {
		t.Errorf("attemptEnd of an unmeasured result = %v, want a point at or after now", got)
	}

	start := time.Now().Add(-time.Minute)
	measured := job.Result{Start: start, Duration: 30 * time.Second}

	if got := attemptEnd(measured); !got.Equal(start.Add(30 * time.Second)) {
		t.Errorf("attemptEnd = %v, want %v", got, start.Add(30*time.Second))
	}
}

// decisionLog collects what the rotation decided, for a test that checks the
// decisions against the runs they were supposed to produce.
type decisionLog struct {
	mu   sync.Mutex
	seen []assignment.Decision
}

func (d *decisionLog) record(decision assignment.Decision) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.seen = append(d.seen, decision)
}

func (d *decisionLog) all() []assignment.Decision {
	d.mu.Lock()
	defer d.mu.Unlock()

	return slices.Clone(d.seen)
}

// A sweep across several turns of the rotation, checking the parts against each
// other rather than one at a time: which points were decided, which of them ran,
// how the runs are spaced, and what LocalPeriod, Missed and Iteration say about
// it. Each of those is covered alone elsewhere; agreeing over three turns is a
// different claim.
func TestBeat_RotationStaysConsistentAcrossTurns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const period = 100 * time.Millisecond

		// Sorted, because that is the order the rotation works in.
		sorted := []string{"dm", "el", "xc"}
		const ours = "el"

		rotation, err := assignment.MakeRotation(assignment.Config{
			Clusters: []string{"el", "xc", "dm"},
			Current:  ours,
			Period:   period,
		})
		if err != nil {
			t.Fatalf("MakeRotation: %v", err)
		}

		var rec recorder
		var log decisionLog

		b := start(t, Config{Period: period}, noopWork, &rec,
			WithAssignment(rotation),
			WithDecisionHandler(log.record))

		// Ten points, so three full turns plus the start of a fourth.
		synctest.Sleep(1050 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		decisions := log.all()
		if len(decisions) < 9 {
			t.Fatalf("saw %d decisions, want at least three turns (9)", len(decisions))
		}

		// Every point of the grid was decided, once, in order.
		for i, d := range decisions {
			if i > 0 && d.Slot != decisions[i-1].Slot+1 {
				t.Fatalf("decision %d is for slot %d, after slot %d", i, d.Slot, decisions[i-1].Slot)
			}

			owner := sorted[d.Slot%int64(len(sorted))]
			if d.Owner != owner {
				t.Errorf("slot %d owner = %q, want %q", d.Slot, d.Owner, owner)
			}
			if d.Execute != (owner == ours) {
				t.Errorf("slot %d Execute = %v for owner %q", d.Slot, d.Execute, owner)
			}
		}

		// The runs are exactly the points this process was given.
		var wanted []assignment.Decision
		for _, d := range decisions {
			if d.Execute {
				wanted = append(wanted, d)
			}
		}

		records := rec.all()
		if len(records) != len(wanted) {
			t.Fatalf("%d runs for %d owned points", len(records), len(wanted))
		}
		if len(records) < 3 {
			t.Fatalf("only %d runs, want at least three turns", len(records))
		}

		for i, r := range records {
			if !r.GridPoint.Equal(wanted[i].Invocation.ScheduledFor) {
				t.Errorf("run %d served %v, but the decision was for %v",
					i, r.GridPoint, wanted[i].Invocation.ScheduledFor)
			}
			if got, want := r.Iteration, int64(i+1); got != want {
				t.Errorf("run %d has Iteration %d, want %d", i, got, want)
			}
			if r.Period != period {
				t.Errorf("run %d Period = %v, want %v", i, r.Period, period)
			}
			// Three clusters, so a point comes round every third one.
			if want := time.Duration(len(sorted)) * period; r.LocalPeriod != want {
				t.Errorf("run %d LocalPeriod = %v, want %v", i, r.LocalPeriod, want)
			}
			// Nothing overran, and the points between belong to others.
			if r.Missed != 0 {
				t.Errorf("run %d reported Missed = %d", i, r.Missed)
			}
			if r.Result.Outcome != job.OutcomeOK {
				t.Errorf("run %d Outcome = %q", i, r.Result.Outcome)
			}
		}

		// The cadence this process actually keeps is its LocalPeriod.
		for i := 1; i < len(records); i++ {
			gap := records[i].ScheduledFor.Sub(records[i-1].ScheduledFor)
			if want := records[i].LocalPeriod; gap != want {
				t.Errorf("runs %d and %d are %v apart, want %v", i-1, i, gap, want)
			}
		}
	})
}
