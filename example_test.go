package beat_test

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/job"
	"github.com/uchaloop/job/assignment"
	"github.com/uchaloop/job/middleware/recovery"
)

func work(ctx context.Context) (int, error) {
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

func ExampleMakeBeat() {
	shutdown, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// The work, its middleware and its timeout belong to the runner, so the
	// same attempt can also be driven by a one-shot process.
	runner, err := job.MakeRunner(
		job.Config{Timeout: 2 * time.Second},
		work,
		job.WithMiddleware(recovery.Middleware()),
	)
	if err != nil {
		slog.Error("configure runner", "error", err)

		return
	}

	handler := beat.HandlerFunc(func(_ context.Context, r beat.Record) {
		slog.Info("attempt completed",
			"outcome", r.Result.Outcome,
			"processed", r.Result.Processed,
			"duration", r.Result.Duration,
			"missed", r.Missed,
		)
	})

	scheduler, err := beat.MakeBeat(
		beat.Config{Period: 5 * time.Second, Jitter: time.Second},
		runner,
		beat.WithHandler(handler),
		beat.WithGracefulStop(),
	)
	if err != nil {
		slog.Error("configure beat", "error", err)

		return
	}

	if err := scheduler.Start(shutdown); err != nil {
		slog.Error("start beat", "error", err)

		return
	}

	<-shutdown.Done()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()

	if err := scheduler.Stop(stopCtx); err != nil {
		slog.Error("stop beat", "error", err)
	}
}

func ExampleWithBackoff() {
	runner, err := job.MakeRunner(job.Config{Timeout: 30 * time.Second}, work)
	if err != nil {
		panic(err)
	}

	// Drain while batches come back full, rest once the queue empties.
	scheduler, err := beat.MakeBeat(
		beat.Config{Period: time.Second},
		runner,
		beat.WithMode(beat.ModeFixedDelay),
		beat.WithBackoff(func(r beat.Record) time.Duration {
			switch {
			case r.Result.Outcome != job.OutcomeOK:
				return 30 * time.Second
			case r.Result.Processed < 1000:
				return 5 * time.Minute
			}

			return 0
		}),
	)
	if err != nil {
		panic(err)
	}

	// Call Start and Stop from the application's lifecycle.
	_ = scheduler
}

func ExampleWithAssignment() {
	// One cluster owns each point; every replica of that cluster runs it. With
	// three clusters on a five-minute period each one attempts every fifteen
	// minutes, and the deployment as a whole every five.
	rotation, err := assignment.MakeRotation(assignment.Config{
		Clusters: []string{"el", "xc", "dm"},
		Current:  os.Getenv("CLUSTER"),
		Period:   5 * time.Minute,
	})
	if err != nil {
		panic(err)
	}

	runner, err := job.MakeRunner(job.Config{Timeout: 4 * time.Minute}, work)
	if err != nil {
		panic(err)
	}

	scheduler, err := beat.MakeBeat(
		beat.Config{Period: 5 * time.Minute, Jitter: 30 * time.Second},
		runner,
		beat.WithAssignment(rotation),
		beat.WithDecisionHandler(func(d assignment.Decision) {
			slog.Debug("scheduled point", "slot", d.Slot, "owner", d.Owner, "ours", d.Execute)
		}),
	)
	if err != nil {
		panic(err)
	}

	_ = scheduler
}

func ExampleOffsetFor() {
	cfg := beat.Config{Period: 5 * time.Minute, Jitter: 5 * time.Minute}

	offset := beat.OffsetFor("orders/cluster-a/worker-0", cfg.Jitter)

	runner, err := job.MakeRunner(job.Config{}, work)
	if err != nil {
		panic(err)
	}

	scheduler, err := beat.MakeBeat(cfg, runner, beat.WithOffset(offset))
	if err != nil {
		panic(err)
	}

	_ = scheduler
}
