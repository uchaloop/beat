package beat

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/uchaloop/job"
)

// hooks counts how often each lifecycle hook ran.
type hooks struct {
	started atomic.Int64
	stopped atomic.Int64
}

func (h *hooks) options() []Option {
	return []Option{
		WithOnStart(func(context.Context) error { h.started.Add(1); return nil }),
		WithOnStop(func(context.Context) error { h.stopped.Add(1); return nil }),
	}
}

func TestStart_SecondCallDoesNotLaunchASecondLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var h hooks
		var live, peak atomic.Int64

		b, err := MakeBeat(Config{Period: 100 * time.Millisecond},
			runnerFor(t, func(context.Context) (int64, error) {
				n := live.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				live.Add(-1)

				return 0, nil
			}), h.options()...)
		if err != nil {
			t.Fatalf("MakeBeat: %v", err)
		}

		if err := b.Start(context.Background()); err != nil {
			t.Fatalf("first Start: %v", err)
		}
		if err := b.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("second Start = %v, want ErrAlreadyStarted", err)
		}

		synctest.Sleep(350 * time.Millisecond)

		if err := b.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}

		if got := peak.Load(); got != 1 {
			t.Errorf("peak concurrent jobs = %d, want 1", got)
		}
		if got := h.started.Load(); got != 1 {
			t.Errorf("OnStart ran %d times, want 1", got)
		}
	})
}

func TestStart_AfterStopIsRefused(t *testing.T) {
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := b.Start(context.Background()); !errors.Is(err, ErrStopped) {
		t.Errorf("Start after Stop = %v, want ErrStopped", err)
	}

	// A Beat stopped without ever running refuses just the same.
	fresh, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}
	if err := fresh.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := fresh.Start(context.Background()); !errors.Is(err, ErrStopped) {
		t.Errorf("Start on a never-run stopped Beat = %v, want ErrStopped", err)
	}
}

func TestStart_ConcurrentCallsElectOneWinner(t *testing.T) {
	const callers = 8

	var h hooks
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork), h.options()...)
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	var wg sync.WaitGroup
	var won atomic.Int64

	for range callers {
		wg.Go(func() {
			switch err := b.Start(context.Background()); {
			case err == nil:
				won.Add(1)
			case errors.Is(err, ErrAlreadyStarted), errors.Is(err, ErrStopped):
			default:
				t.Errorf("Start = %v, want nil or a lifecycle error", err)
			}
		})
	}
	wg.Wait()

	if got := won.Load(); got != 1 {
		t.Errorf("%d callers started the Beat, want 1", got)
	}
	if got := h.started.Load(); got != 1 {
		t.Errorf("OnStart ran %d times, want 1", got)
	}

	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStop_SecondCallReportsTheFirstResult(t *testing.T) {
	var h hooks
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork), h.options()...)
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	first := b.Stop(context.Background())
	second := b.Stop(context.Background())

	if first != nil || second != nil {
		t.Errorf("Stop results = %v, %v, want nil, nil", first, second)
	}
	if got := h.stopped.Load(); got != 1 {
		t.Errorf("OnStop ran %d times, want 1", got)
	}
}

func TestStop_ConcurrentCallsRunTheHookOnce(t *testing.T) {
	const callers = 8

	var h hooks
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork), h.options()...)
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			if err := b.Stop(context.Background()); err != nil {
				t.Errorf("Stop: %v", err)
			}
		})
	}
	wg.Wait()

	if got := h.stopped.Load(); got != 1 {
		t.Errorf("OnStop ran %d times, want 1", got)
	}
}

func TestStop_NeverStartedRunsNoHook(t *testing.T) {
	var h hooks
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork), h.options()...)
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Stop(context.Background()); err != nil {
		t.Errorf("Stop = %v, want nil", err)
	}
	if got := h.stopped.Load(); got != 0 {
		t.Errorf("OnStop ran %d times on a Beat that never started, want 0", got)
	}

	select {
	case <-b.Done():
	default:
		t.Error("Done() is open although nothing will ever run")
	}
}

func TestStart_FailingHookLeavesTheBeatStopped(t *testing.T) {
	want := errors.New("no")

	var stops atomic.Int64
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork),
		WithOnStart(func(context.Context) error { return want }),
		WithOnStop(func(context.Context) error { stops.Add(1); return nil }))
	if err != nil {
		t.Fatalf("MakeBeat: %v", err)
	}

	if err := b.Start(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Start = %v, want the hook's error", err)
	}

	// The hooks are a pair and the first one did not complete.
	if err := b.Stop(context.Background()); err != nil {
		t.Errorf("Stop after a failed start = %v, want nil", err)
	}
	if got := stops.Load(); got != 0 {
		t.Errorf("OnStop ran %d times after a failed start, want 0", got)
	}

	select {
	case <-b.Done():
	default:
		t.Error("Done() is open although the loop never started")
	}
}

// stillRunning is the shape shared by the two deadline tests: something in the
// run - the Job in one, the Handler in the other - outlives the stop deadline.
func stillRunning(t *testing.T, fn job.Func, handler Handler) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		var stops atomic.Int64

		b, err := MakeBeat(Config{Period: 100 * time.Millisecond},
			runnerFor(t, fn),
			WithHandler(handler),
			WithOnStop(func(context.Context) error { stops.Add(1); return nil }))
		if err != nil {
			t.Fatalf("MakeBeat: %v", err)
		}

		if err := b.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		synctest.Sleep(150 * time.Millisecond) // a run is in flight

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err = b.Stop(ctx)

		if !errors.Is(err, ErrStillRunning) {
			t.Errorf("Stop = %v, want ErrStillRunning", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Stop = %v, want it to carry the deadline too", err)
		}
		if got := stops.Load(); got != 0 {
			t.Errorf("OnStop ran %d times while the run was still going, want 0", got)
		}

		select {
		case <-b.Done():
			t.Error("Done() closed although the run had not finished")
		default:
		}

		// The run ends on its own; Done is how an application hears about it.
		<-b.Done()

		if got := stops.Load(); got != 0 {
			t.Errorf("OnStop ran %d times after Stop returned, want 0", got)
		}
	})
}

func TestStop_DeadlineWithAHangingJob(t *testing.T) {
	stillRunning(t, func(context.Context) (int64, error) {
		time.Sleep(10 * time.Second) // ignores cancellation, as a bad Job does

		return 0, nil
	}, nil)
}

func TestStop_DeadlineWithASlowHandler(t *testing.T) {
	stillRunning(t, noopWork, HandlerFunc(func(context.Context, Record) {
		time.Sleep(10 * time.Second)
	}))
}

func TestStop_DuringStartup(t *testing.T) {
	for _, fails := range []bool{false, true} {
		name := "success"
		if fails {
			name = "failure"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				startupErr, cleanupErr := errors.New("startup"), errors.New("cleanup")

				var stops, jobs atomic.Int64
				b, err := MakeBeat(Config{Period: time.Second}, runnerFor(t, func(context.Context) (int64, error) {
					jobs.Add(1)

					return 0, nil
				}),
					WithOnStart(func(context.Context) error {
						close(entered)
						<-release
						if fails {
							return startupErr
						}

						return nil
					}),
					WithOnStop(func(context.Context) error {
						stops.Add(1)

						return cleanupErr
					}))
				if err != nil {
					t.Fatal(err)
				}

				started, stopped, second := make(chan error, 1), make(chan error, 1), make(chan error, 1)

				go func() { started <- b.Start(context.Background()) }()
				<-entered

				go func() { stopped <- b.Stop(context.Background()) }()

				synctest.Wait()

				go func() { second <- b.Stop(context.Background()) }()

				synctest.Wait()

				select {
				case err := <-second:
					t.Fatalf("Stop finished before startup: %v", err)
				default:
				}

				close(release)
				startErr := <-started
				if !errors.Is(startErr, ErrStopped) {
					t.Fatalf("Start=%v", startErr)
				}

				firstErr, secondErr := <-stopped, <-second
				if fails {
					if !errors.Is(startErr, startupErr) || firstErr != nil || secondErr != nil || stops.Load() != 0 {
						t.Fatalf("failed startup: start=%v stop=%v second=%v hooks=%d", startErr, firstErr, secondErr, stops.Load())
					}
				} else if !errors.Is(firstErr, cleanupErr) || !errors.Is(secondErr, cleanupErr) || stops.Load() != 1 {
					t.Fatalf("successful startup: stop=%v second=%v hooks=%d", firstErr, secondErr, stops.Load())
				}

				if jobs.Load() != 0 {
					t.Fatal("Job ran after Stop during startup")
				}
			})
		})
	}
}

func TestStart_CanceledAfterSuccessfulHookRollsBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stops int
	cleanupErr := errors.New("cleanup")
	b, err := MakeBeat(Config{Period: time.Second}, runnerFor(t, noopWork),
		WithOnStart(func(context.Context) error {
			cancel()

			return nil
		}),
		WithOnStop(func(ctx context.Context) error {
			stops++
			if ctx.Err() != nil {
				t.Error("cleanup inherited cancellation")
			}

			if _, ok := ctx.Deadline(); !ok {
				t.Error("cleanup has no deadline")
			}

			return cleanupErr
		}))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Start(ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cleanupErr) || stops != 1 {
		t.Fatalf("Start=%v cleanup calls=%d", err, stops)
	}

	if err = b.Stop(context.Background()); !errors.Is(err, cleanupErr) || stops != 1 {
		t.Fatalf("Stop=%v cleanup calls=%d", err, stops)
	}
}

func TestStop_CompletedLoopWinsOverCanceledContext(t *testing.T) {
	b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork))
	if err != nil {
		t.Fatal(err)
	}

	if err = b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	b.loopCancel()
	<-b.Done()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = b.Stop(ctx); err != nil {
		t.Fatalf("completed loop: %v", err)
	}

	for range 100 {
		if err = b.Stop(ctx); err != nil {
			t.Fatalf("published Stop result: %v", err)
		}
	}
}

func TestStop_WaitsForCleanupResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		want := errors.New("cleanup failed")
		b, err := MakeBeat(Config{Period: time.Hour}, runnerFor(t, noopWork), WithOnStop(func(context.Context) error {
			close(entered)
			<-release

			return want
		}))
		if err != nil {
			t.Fatal(err)
		}

		if err = b.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		first, second := make(chan error, 1), make(chan error, 1)

		go func() { first <- b.Stop(context.Background()) }()
		<-entered

		select {
		case <-b.Done():
		default:
			t.Fatal("Done must precede cleanup completion")
		}

		go func() { second <- b.Stop(context.Background()) }()

		synctest.Wait()

		select {
		case err := <-second:
			t.Fatalf("Stop returned before cleanup: %v", err)
		default:
		}

		close(release)
		if err = <-first; !errors.Is(err, want) {
			t.Fatal(err)
		}

		if err = <-second; !errors.Is(err, want) {
			t.Fatal(err)
		}
	})
}
