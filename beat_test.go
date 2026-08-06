package beat

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

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

	got := order
	want := []string{"a", "b", "c", "job"}
	if len(got) != len(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
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

func TestIsEverySpec(t *testing.T) {
	cases := map[string]bool{
		"@every 5s":     true,
		"  @every 1m":   true,
		"@daily":        false,
		"@hourly":       false,
		"*/5 * * * * *": false,
		"0 0 * * *":     false,
	}
	for spec, want := range cases {
		if got := isEverySpec(spec); got != want {
			t.Errorf("isEverySpec(%q) = %v, want %v", spec, got, want)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	if err := (Config{Spec: "@every 1s"}).Validate(); err != nil {
		t.Errorf("valid spec rejected: %v", err)
	}
	if err := (Config{Spec: ""}).Validate(); err == nil {
		t.Error("empty spec accepted")
	}
	if err := (Config{Spec: "not a spec"}).Validate(); err == nil {
		t.Error("bad spec accepted")
	}
	if err := (Config{Spec: "@every 1s", JobTimeout: -1}).Validate(); err == nil {
		t.Error("negative run_timeout accepted")
	}
}

func TestMakeBeat_RequiresJob(t *testing.T) {
	if _, err := MakeBeat(Config{Spec: "@every 1s"}, nil, nil); err == nil {
		t.Fatal("expected error for nil job")
	}
}

func TestMakeBeat_DefaultsJobTimeout(t *testing.T) {
	b, err := MakeBeat(Config{Spec: "@every 1s"}, noopJob, nil)
	if err != nil {
		t.Fatalf("makeBeat: %v", err)
	}
	if b.jobTimeout != defaultJobTimeout {
		t.Fatalf("jobTimeout = %v, want %v", b.jobTimeout, defaultJobTimeout)
	}
}

func TestMakeBeat_ValidatesConfig(t *testing.T) {
	if _, err := MakeBeat(Config{Spec: "nonsense"}, noopJob, nil); err == nil {
		t.Fatal("expected error for invalid spec")
	}
	if _, err := MakeBeat(Config{Spec: "@every 1s", JobTimeout: -1}, noopJob, nil); err == nil {
		t.Fatal("expected error for negative run timeout")
	}
}

func TestStop_CancelAbortsInFlightRun(t *testing.T) {
	ran := make(chan struct{}, 1)
	b, err := MakeBeat(
		Config{Spec: "@every 10ms", JobTimeout: time.Minute},
		func(ctx context.Context) (int, error) {
			select {
			case ran <- struct{}{}:
			default:
			}
			<-ctx.Done() // returns only once stop cancels the run

			return 0, ctx.Err()
		},
		nil,
	)
	if err != nil {
		t.Fatalf("makeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-ran

	// Default (non-graceful) stop must cancel the run, so this returns.
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestStop_GracefulLetsRunFinish(t *testing.T) {
	ran := make(chan struct{}, 1)
	release := make(chan struct{})
	sawCancel := make(chan bool, 1)

	b, err := MakeBeat(
		Config{Spec: "@every 10ms", JobTimeout: time.Minute},
		func(ctx context.Context) (int, error) {
			select {
			case ran <- struct{}{}:
			default:
			}
			<-release
			sawCancel <- ctx.Err() != nil

			return 0, nil
		},
		nil,
		WithGracefulStop(),
	)
	if err != nil {
		t.Fatalf("makeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-ran

	stopDone := make(chan error, 1)
	go func() { stopDone <- b.Stop(context.Background()) }()

	time.Sleep(20 * time.Millisecond) // let stop fire loopCancel
	close(release)                    // now let the run finish

	if err := <-stopDone; err != nil {
		t.Fatalf("stop: %v", err)
	}
	if <-sawCancel {
		t.Error("graceful stop cancelled the in-flight run's context")
	}
}

func noopJob(context.Context) (int, error) { return 0, nil }

func TestBeat_RunsAndReportsToHandler(t *testing.T) {
	var runs atomic.Int64
	var records atomic.Int64

	b, err := MakeBeat(
		Config{Spec: "@every 20ms", JobTimeout: time.Second},
		func(context.Context) (int, error) {
			runs.Add(1)

			return 7, nil
		},
		HandlerFunc(func(_ context.Context, r Record) {
			if r.Processed != 7 {
				t.Errorf("Processed = %d, want 7", r.Processed)
			}
			records.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("makeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(t, &runs, 2, time.Second)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if records.Load() < runs.Load() {
		t.Errorf("records = %d < runs = %d", records.Load(), runs.Load())
	}
}

func TestRunOnce_ReportsJobError(t *testing.T) {
	// A Job error must propagate into the Record delivered to the Handler.
	var got error
	b, err := MakeBeat(
		Config{Spec: "@every 20ms"},
		func(context.Context) (int, error) { return 0, errors.New("boom") },
		HandlerFunc(func(_ context.Context, r Record) { got = r.Err }),
	)
	if err != nil {
		t.Fatalf("makeBeat: %v", err)
	}
	b.runOnce()
	if got == nil || got.Error() != "boom" {
		t.Fatalf("Record.Err = %v, want boom", got)
	}
}

func waitFor(t *testing.T, c *atomic.Int64, want int64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Load() >= want {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("counter = %d, want >= %d", c.Load(), want)
}
