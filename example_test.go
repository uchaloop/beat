package beat_test

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/uchaloop/beat"
)

func ExampleMakeBeat() {
	shutdown, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	job := func(ctx context.Context) (int, error) {
		// Replace this timer with one bounded batch of application work.
		timer := time.NewTimer(20 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	handler := beat.HandlerFunc(func(_ context.Context, r beat.Record) {
		slog.Info("job completed", "outcome", r.Outcome,
			"processed", r.Processed, "duration", r.Duration, "missed", r.Missed)
	})
	runner, err := beat.MakeBeat(beat.Config{
		Period: 5 * time.Second, JobTimeout: 2 * time.Second, Jitter: time.Second,
	}, job, handler, beat.WithGracefulStop())
	if err != nil {
		slog.Error("configure beat", "error", err)
		return
	}
	if err := runner.Start(shutdown); err != nil {
		slog.Error("start beat", "error", err)
		return
	}
	<-shutdown.Done()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	if err := runner.Stop(stopCtx); err != nil {
		slog.Error("stop beat", "error", err)
	}
}

func ExampleWithBackoff() {
	job := func(ctx context.Context) (int, error) { return 0, ctx.Err() }
	runner, err := beat.MakeBeat(beat.Config{
		Period: time.Second, JobTimeout: 30 * time.Second,
	}, job, nil,
		beat.WithMode(beat.ModeFixedDelay),
		beat.WithBackoff(func(r beat.Record) time.Duration {
			if r.Outcome != beat.OutcomeOK {
				return 30 * time.Second
			}
			if r.Processed < 1000 {
				return 5 * time.Minute
			}
			return 0
		}),
	)
	if err != nil {
		panic(err)
	}
	// Call runner.Start and runner.Stop from the application's lifecycle.
	_ = runner
}

func ExampleOffsetFor() {
	cfg := beat.Config{Period: 5 * time.Minute, Jitter: 5 * time.Minute}
	offset := beat.OffsetFor("orders/cluster-a/worker-0", cfg.Jitter)
	runner, err := beat.MakeBeat(cfg,
		func(ctx context.Context) (int, error) { return 0, ctx.Err() },
		nil, beat.WithOffset(offset),
	)
	if err != nil {
		panic(err)
	}
	_ = runner
}
