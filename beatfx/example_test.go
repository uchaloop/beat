package beatfx_test

import (
	"context"
	"log/slog"
	"time"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/beat/beatfx"
	"github.com/uchaloop/beat/middleware/recovery"
	"go.uber.org/fx"
)

func ExampleModule() {
	job := beat.Job(func(ctx context.Context) (int, error) { return 0, ctx.Err() })
	app := fx.New(
		fx.Supply(beat.Config{Period: time.Minute, JobTimeout: 45 * time.Second}),
		fx.Provide(func() beat.Job { return job }),
		fx.Provide(func(log *slog.Logger) beatfx.Options {
			return beatfx.Options{
				beat.WithMiddleware(recovery.Middleware(recovery.WithLogger(log))),
				beat.WithGracefulStop(),
			}
		}),
		fx.Supply(slog.Default()),
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
