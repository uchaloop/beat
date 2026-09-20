package beatfx_test

import (
	"context"
	"log/slog"
	"time"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/beat/beatfx"
	"github.com/uchaloop/job"
	"github.com/uchaloop/job/middleware/recovery"
	"go.uber.org/fx"
)

func ExampleModule() {
	app := fx.New(
		fx.Supply(beat.Config{Period: time.Minute}),
		fx.Supply(slog.Default()),

		// The runner carries the work, its middleware and its timeout.
		fx.Provide(func(log *slog.Logger) (*job.Runner, error) {
			return job.MakeRunner(
				job.Config{Timeout: 45 * time.Second},
				func(ctx context.Context) (int, error) { return 0, ctx.Err() },
				job.WithMiddleware(recovery.Middleware(recovery.WithLogger(log))),
			)
		}),

		fx.Provide(func() beatfx.Options {
			return beatfx.Options{beat.WithGracefulStop()}
		}),

		beatfx.Module(),
		fx.StopTimeout(time.Minute),
		fx.NopLogger,
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := app.Start(ctx); err != nil {
		panic(err)
	}
	if err := app.Stop(ctx); err != nil {
		panic(err)
	}
	// Output:
}
