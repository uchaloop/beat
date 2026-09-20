package beatfx_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/beat/beatfx"
	"github.com/uchaloop/job"
)

// testPeriod keeps a run close enough to the start of the app that a test does
// not wait on it, while staying above the scheduling noise of a busy machine.
const testPeriod = 20 * time.Millisecond

// ran closes the first time the work is called.
type ran struct {
	ch   chan struct{}
	once sync.Once
}

func newRan() *ran { return &ran{ch: make(chan struct{})} }

func (r *ran) work(context.Context) (int, error) {
	r.once.Do(func() { close(r.ch) })

	return 0, nil
}

func (r *ran) wait(t *testing.T) {
	t.Helper()

	select {
	case <-r.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("the work did not run")
	}
}

func runnerFor(t *testing.T, fn job.Func) *job.Runner {
	t.Helper()

	runner, err := job.MakeRunner(job.Config{Timeout: time.Second}, fn)
	if err != nil {
		t.Fatalf("MakeRunner: %v", err)
	}

	return runner
}

func countingHandler(c *atomic.Int64) beat.Handler {
	return beat.HandlerFunc(func(context.Context, beat.Record) { c.Add(1) })
}

// TestModule_StartsBeat exercises the full Fx wiring: a Config value and a
// Runner provided into the container, consumed by beatfx.Module. This is the
// path the predecessor library got wrong (it supplied the config by value but
// consumed a pointer, so the graph never built).
func TestModule_StartsBeat(t *testing.T) {
	r := newRan()

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Period: testPeriod}),
		fx.Provide(func() *job.Runner { return runnerFor(t, r.work) }),

		beatfx.Module(),
	)

	app.RequireStart()
	r.wait(t)
	app.RequireStop()
}

// TestModule_HandlerIsOptional verifies the app starts without providing a
// Handler or any Options.
func TestModule_HandlerIsOptional(t *testing.T) {
	app := fxtest.New(
		t,
		fx.Supply(beat.Config{Period: time.Second}),
		fx.Provide(func() *job.Runner { return runnerFor(t, func(context.Context) (int, error) { return 0, nil }) }),
		beatfx.Module(),
	)
	app.RequireStart()
	app.RequireStop()
}

// TestModule_OptionsOverrideStatic pins the one ordering rule: options passed
// to Module are applied first, so the container-built ones can still override
// them. This is the regression the type exists for - the options used to arrive
// through an Fx value group, which Fx fills in an unspecified order, so which
// of two writes survived was left to chance.
func TestModule_OptionsOverrideStatic(t *testing.T) {
	var static, fromContainer atomic.Int64

	r := newRan()

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Period: testPeriod}),
		fx.Provide(func() *job.Runner { return runnerFor(t, r.work) }),

		fx.Provide(func() beatfx.Options {
			return beatfx.Options{beat.WithHandler(countingHandler(&fromContainer))}
		}),

		beatfx.Module(beat.WithHandler(countingHandler(&static))),
	)

	app.RequireStart()
	r.wait(t)
	app.RequireStop()

	if static.Load() != 0 {
		t.Errorf("the static handler ran %d times, want 0 - the container overrides it", static.Load())
	}
	if fromContainer.Load() == 0 {
		t.Error("the container-built handler never ran")
	}
}

// TestModule_LastWriteWins covers two writes inside one ordered set. Which one
// survived used to depend on the order Fx happened to produce; now it is the
// one written last.
func TestModule_LastWriteWins(t *testing.T) {
	var first, second atomic.Int64

	r := newRan()

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Period: testPeriod}),
		fx.Provide(func() *job.Runner { return runnerFor(t, r.work) }),

		fx.Provide(func() beatfx.Options {
			return beatfx.Options{
				beat.WithHandler(countingHandler(&first)),
				beat.WithHandler(countingHandler(&second)),
			}
		}),

		beatfx.Module(),
	)

	app.RequireStart()
	r.wait(t)
	app.RequireStop()

	if first.Load() != 0 {
		t.Errorf("the overwritten handler ran %d times, want 0", first.Load())
	}
	if second.Load() == 0 {
		t.Error("the last handler never ran")
	}
}

// TestModule_SuppliedOptionsReachModule verifies that a ready set, needing
// nothing from the container, can be handed over with fx.Supply.
func TestModule_SuppliedOptionsReachModule(t *testing.T) {
	var started atomic.Bool

	app := fxtest.New(
		t,
		fx.Supply(beat.Config{Period: time.Second}),
		fx.Provide(func() *job.Runner { return runnerFor(t, func(context.Context) (int, error) { return 0, nil }) }),

		fx.Supply(beatfx.Options{
			beat.WithOnStart(func(context.Context) error {
				started.Store(true)

				return nil
			}),
		}),

		beatfx.Module(),
	)

	app.RequireStart()
	if !started.Load() {
		t.Error("the supplied options never reached the Beat")
	}
	app.RequireStop()
}
