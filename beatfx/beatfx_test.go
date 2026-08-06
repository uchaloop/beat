package beatfx_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/beat/beatfx"
)

// TestModule_StartsBeat exercises the full Fx wiring: a Config value and a Job
// provided into the container, consumed by beatfx.Module. This is the path the
// predecessor library got wrong (it supplied the config by value but consumed a
// pointer, so the graph never built).
func TestModule_StartsBeat(t *testing.T) {
	var runs atomic.Int64

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Spec: "@every 20ms", JobTimeout: time.Second}),

		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) {
				runs.Add(1)

				return 1, nil
			}
		}),

		beatfx.Module(),
	)

	startCtx, cancelStart := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStart()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && runs.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if runs.Load() < 1 {
		t.Fatalf("job did not run")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStop()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// TestModule_HandlerIsOptional verifies the app starts without providing a
// Handler.
func TestModule_HandlerIsOptional(t *testing.T) {
	app := fxtest.New(
		t,
		fx.Supply(beat.Config{Spec: "@every 1s"}),
		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) { return 0, nil }
		}),
		beatfx.Module(),
	)
	app.RequireStart()
	app.RequireStop()
}

// TestModule_AsOption verifies a DI-built option contributed through AsOption is
// applied to the Beat.
func TestModule_AsOption(t *testing.T) {
	var started atomic.Bool

	app := fxtest.New(
		t,
		fx.Supply(beat.Config{Spec: "@every 1s"}),
		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) { return 0, nil }
		}),
		beatfx.AsOption(func() beat.Option {
			return beat.WithOnStart(func(context.Context) error {
				started.Store(true)

				return nil
			})
		}),
		beatfx.Module(),
	)

	app.RequireStart()
	if !started.Load() {
		t.Fatal("option from AsOption was not applied")
	}
	app.RequireStop()
}

// TestModule_AsOptionNil_Errors verifies AsOption(nil) fails the graph with a
// clear message instead of a cryptic Fx error.
func TestModule_AsOptionNil_Errors(t *testing.T) {
	app := fx.New(
		fx.Supply(beat.Config{Spec: "@every 1s"}),
		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) { return 0, nil }
		}),
		beatfx.AsOption(nil),
		beatfx.Module(),
		fx.NopLogger,
	)

	if app.Err() == nil {
		t.Fatal("expected an error from AsOption(nil)")
	}
}

// TestModule_AsOption_BareOption verifies AsOption accepts a ready Option value
// without a constructor wrapper.
func TestModule_AsOption_BareOption(t *testing.T) {
	var started atomic.Bool

	app := fxtest.New(
		t,
		fx.Supply(beat.Config{Spec: "@every 1s"}),
		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) { return 0, nil }
		}),
		beatfx.AsOption(beat.WithOnStart(func(context.Context) error {
			started.Store(true)

			return nil
		})),
		beatfx.Module(),
	)

	app.RequireStart()
	if !started.Load() {
		t.Fatal("bare option from AsOption was not applied")
	}
	app.RequireStop()
}
