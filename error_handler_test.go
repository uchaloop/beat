package beat

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/uchaloop/job"
	"github.com/uchaloop/job/assignment"
)

func TestErrorHandler_RotationAccountsForEntireAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var records recorder
		workCalls, handlerCalls := 0, 0
		runner, err := job.MakeRunner(job.Config{}, func(context.Context) (int, error) { workCalls++; return 1, errors.New("work") },
			job.WithErrorHandler(func(context.Context, error) error { handlerCalls++; time.Sleep(350 * time.Millisecond); return nil }))
		if err != nil {
			t.Fatal(err)
		}
		b, err := MakeBeat(Config{Period: 100 * time.Millisecond}, runner, WithHandler(&records), WithAssignment(rotationFor(t, "el", 100*time.Millisecond)))
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(1200 * time.Millisecond)
		if err = b.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		recs := records.all()
		if len(recs) < 2 || recs[1].Missed != 1 {
			t.Fatalf("records=%+v", recs)
		}
		if workCalls != len(recs) || handlerCalls != workCalls {
			t.Fatalf("work=%d handlers=%d records=%d", workCalls, handlerCalls, len(recs))
		}
		for _, r := range recs {
			decision, err := b.rotation.Decide(assignment.Invocation{ScheduledFor: r.GridPoint})
			if err != nil || !decision.Execute {
				t.Fatalf("foreign run: %+v %v", r, err)
			}
			if r.Result.Duration != 350*time.Millisecond || r.Result.WorkDuration != 0 {
				t.Fatalf("duration=%+v", r.Result)
			}
		}
	})
}

func TestErrorHandler_BackoffStartsAfterDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var records recorder
		runner, err := job.MakeRunner(job.Config{}, func(context.Context) (int, error) { return 0, errors.New("work") },
			job.WithErrorHandler(func(context.Context, error) error { time.Sleep(200 * time.Millisecond); return nil }))
		if err != nil {
			t.Fatal(err)
		}
		b, err := MakeBeat(Config{Period: 100 * time.Millisecond}, runner, WithMode(ModeFixedDelay), WithHandler(&records), WithBackoff(func(Record) time.Duration { return 300 * time.Millisecond }))
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Sleep(800 * time.Millisecond)
		if err = b.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		recs := records.all()
		if len(recs) != 2 || recs[1].Result.Start.Sub(recs[0].Result.Start) != 500*time.Millisecond {
			t.Fatalf("records=%+v", recs)
		}
	})
}

func TestErrorHandler_StopWaitsForIndependentDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		cleaned := false
		runner, err := job.MakeRunner(job.Config{ErrorHandlerTimeout: time.Second}, func(context.Context) (int, error) { return 0, errors.New("work") },
			job.WithErrorHandler(func(ctx context.Context, _ error) error { close(entered); <-ctx.Done(); return ctx.Err() }))
		if err != nil {
			t.Fatal(err)
		}
		b, err := MakeBeat(Config{Period: time.Second}, runner, WithMode(ModeFixedDelay), WithOnStop(func(context.Context) error { cleaned = true; return nil }))
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		<-entered
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if err = b.Stop(ctx); !errors.Is(err, ErrStillRunning) {
			t.Fatal(err)
		}
		if cleaned {
			t.Fatal("cleanup overlapped error handler")
		}
		select {
		case <-b.Done():
			t.Fatal("loop exited before error delivery")
		default:
		}
		<-b.Done()
	})
}
