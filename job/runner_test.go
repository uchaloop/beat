package job

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"
)

func mustRunner(t *testing.T, cfg Config, fn Func, opts ...Option) *Runner {
	t.Helper()

	r, err := MakeRunner(cfg, fn, opts...)
	if err != nil {
		t.Fatalf("MakeRunner: %v", err)
	}

	return r
}

func TestRun_ReportsTheWork(t *testing.T) {
	r := mustRunner(t, Config{}, func(context.Context) (int64, error) { return 7, nil })

	result := r.Run(context.Background())

	if result.Processed != 7 {
		t.Errorf("Processed = %d, want 7", result.Processed)
	}
	if result.Outcome != OutcomeOK {
		t.Errorf("Outcome = %q, want %q", result.Outcome, OutcomeOK)
	}
	if result.Err != nil {
		t.Errorf("Err = %v, want nil", result.Err)
	}
	if result.StartedAt.IsZero() {
		t.Error("StartedAt is zero although the work ran")
	}
}

// A partial batch that then fails still reports what it managed to do.
func TestRun_ErrorKeepsPartialProcessed(t *testing.T) {
	boom := errors.New("boom")
	r := mustRunner(t, Config{}, func(context.Context) (int64, error) { return 4, boom })

	result := r.Run(context.Background())

	if result.Processed != 4 {
		t.Errorf("Processed = %d, want 4", result.Processed)
	}
	if !errors.Is(result.Err, boom) {
		t.Errorf("Err = %v, want boom", result.Err)
	}
	if result.Outcome != OutcomeError {
		t.Errorf("Outcome = %q, want %q", result.Outcome, OutcomeError)
	}
}

// The deadline cannot interrupt the work: Run waits for it, and reports the
// timeout even though the work chose to say nothing about it.
func TestRun_TimeoutWaitsAndIsReportedAnyway(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := mustRunner(t, Config{Timeout: 100 * time.Millisecond},
			func(context.Context) (int64, error) {
				time.Sleep(300 * time.Millisecond) // ignores cancellation

				return 3, nil
			})

		begin := time.Now()
		result := r.Run(context.Background())

		if elapsed := time.Since(begin); elapsed != 300*time.Millisecond {
			t.Errorf("Run returned after %v, want 300ms", elapsed)
		}
		if result.Outcome != OutcomeTimeout {
			t.Errorf("Outcome = %q, want %q", result.Outcome, OutcomeTimeout)
		}
		if result.Err != nil {
			t.Errorf("Err = %v, want nil - the work reported success", result.Err)
		}
		if result.Processed != 3 {
			t.Errorf("Processed = %d, want 3", result.Processed)
		}
		if result.Duration != 300*time.Millisecond {
			t.Errorf("Duration = %v, want 300ms", result.Duration)
		}
	})
}

func TestRun_CallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		r := mustRunner(t, Config{Timeout: time.Minute},
			func(ctx context.Context) (int64, error) {
				<-ctx.Done()

				return 0, ctx.Err()
			})

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		result := r.Run(ctx)

		if result.Outcome != OutcomeCanceled {
			t.Errorf("Outcome = %q, want %q", result.Outcome, OutcomeCanceled)
		}
	})
}

// A context that is already done skips the work entirely; nothing ran, so
// nothing is measured.
func TestRun_DoneContextSkipsTheWork(t *testing.T) {
	isCalled := false
	r := mustRunner(t, Config{}, func(context.Context) (int64, error) {
		isCalled = true

		return 9, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := r.Run(ctx)

	if isCalled {
		t.Error("the work ran although the context was already done")
	}
	if result.Outcome != OutcomeCanceled {
		t.Errorf("Outcome = %q, want %q", result.Outcome, OutcomeCanceled)
	}
	if !errors.Is(result.Err, context.Canceled) {
		t.Errorf("Err = %v, want context.Canceled", result.Err)
	}
	if !result.StartedAt.IsZero() || result.Duration != 0 || result.Processed != 0 {
		t.Errorf("measured an attempt that never happened: %+v", result)
	}
}

// One Run is one attempt - no retry hides inside it - and a Runner is reusable.
func TestRun_OneAttemptPerCall(t *testing.T) {
	calls := 0
	r := mustRunner(t, Config{}, func(context.Context) (int64, error) {
		calls++

		return 0, errors.New("always fails")
	})

	r.Run(context.Background())
	if calls != 1 {
		t.Fatalf("one Run made %d calls, want 1", calls)
	}

	r.Run(context.Background())
	if calls != 2 {
		t.Fatalf("two Runs made %d calls, want 2", calls)
	}
}

func TestRun_PanicPropagatesWithoutRecovery(t *testing.T) {
	r := mustRunner(t, Config{}, func(context.Context) (int64, error) { panic("boom") })

	defer func() {
		if p := recover(); p == nil {
			t.Error("the panic did not reach the caller")
		}
	}()

	r.Run(context.Background())
}

func TestMakeRunner_Rejects(t *testing.T) {
	if _, err := MakeRunner(Config{}, nil); err == nil {
		t.Error("accepted a nil func")
	}
	if _, err := MakeRunner(Config{Timeout: -time.Second}, noop); err == nil {
		t.Error("accepted a negative timeout")
	}
}

func TestMakeRunner_DefaultsTheTimeout(t *testing.T) {
	r := mustRunner(t, Config{}, noop)

	if r.timeout != defaultTimeout {
		t.Errorf("timeout = %v, want %v", r.timeout, defaultTimeout)
	}
}

func TestRun_PreservesInt64CountOnError(t *testing.T) {
	workErr := errors.New("partial failure")
	for _, count := range []int64{0, 1 << 40, math.MaxInt64, -1} {
		runner := mustRunner(t, Config{}, func(context.Context) (int64, error) {
			return count, workErr
		}, WithErrorHandler(func(context.Context, error) error { return nil }))
		result := runner.Run(context.Background())
		if result.Processed != count || result.Outcome != OutcomeError || !errors.Is(result.Err, workErr) {
			t.Fatalf("count=%d result=%+v", count, result)
		}
	}
}
