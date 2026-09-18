package beatfx_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/beat/beatfx"
)

// testPeriod keeps a run close enough to the start of the app that a test does
// not wait on it, while staying above the scheduling noise of a busy machine.
const testPeriod = 20 * time.Millisecond

// order records the names middleware announce as they wrap the Job. The
// wrapping order is not reachable from outside a Beat any other way, so a run
// has to happen for it to be observed.
type order struct {
	mu   sync.Mutex
	seen []string

	ran  chan struct{}
	once sync.Once
}

func newOrder() *order { return &order{ran: make(chan struct{})} }

func (o *order) mark(name string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.seen = append(o.seen, name)
}

func (o *order) middleware(name string) beat.Middleware {
	return func(next beat.Job) beat.Job {
		return func(ctx context.Context) (int, error) {
			o.mark(name)

			return next(ctx)
		}
	}
}

func (o *order) job() beat.Job {
	return func(context.Context) (int, error) {
		o.mark("job")
		o.once.Do(func() { close(o.ran) })

		return 0, nil
	}
}

// firstRun reports the names recorded by the first run, in order.
func (o *order) firstRun(t *testing.T, n int) []string {
	t.Helper()

	select {
	case <-o.ran:
	case <-time.After(2 * time.Second):
		t.Fatal("the job did not run")
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.seen) < n {
		t.Fatalf("recorded %v, want at least %d entries", o.seen, n)
	}

	return slices.Clone(o.seen[:n])
}

// TestModule_StartsBeat exercises the full Fx wiring: a Config value and a Job
// provided into the container, consumed by beatfx.Module. This is the path the
// predecessor library got wrong (it supplied the config by value but consumed a
// pointer, so the graph never built).
func TestModule_StartsBeat(t *testing.T) {
	o := newOrder()

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Period: testPeriod, JobTimeout: time.Second}),
		fx.Provide(func() beat.Job { return o.job() }),

		beatfx.Module(),
	)

	app.RequireStart()
	o.firstRun(t, 1)
	app.RequireStop()
}

// TestModule_HandlerIsOptional verifies the app starts without providing a
// Handler or any Options.
func TestModule_HandlerIsOptional(t *testing.T) {
	app := fxtest.New(
		t,
		fx.Supply(beat.Config{Period: time.Second}),
		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) { return 0, nil }
		}),
		beatfx.Module(),
	)
	app.RequireStart()
	app.RequireStop()
}

// TestModule_OptionsKeepTheirOrder is the regression this type exists for. The
// options used to arrive through an Fx value group, which Fx fills in an
// unspecified order, so which middleware wrapped which was left to chance.
func TestModule_OptionsKeepTheirOrder(t *testing.T) {
	o := newOrder()

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Period: testPeriod, JobTimeout: time.Second}),
		fx.Provide(func() beat.Job { return o.job() }),

		fx.Provide(func() beatfx.Options {
			return beatfx.Options{
				beat.WithMiddleware(o.middleware("first")),
				beat.WithMiddleware(o.middleware("second")),
			}
		}),

		beatfx.Module(),
	)

	app.RequireStart()
	got := o.firstRun(t, 3)
	app.RequireStop()

	if want := []string{"first", "second", "job"}; !slices.Equal(got, want) {
		t.Errorf("wrapping order = %v, want %v", got, want)
	}
}

// TestModule_StaticOptionsComeFirst pins the one ordering rule: options passed
// to Module wrap outside the ones the container builds. That is what keeps a
// recovery middleware given to Module in a position to catch a panic from them.
func TestModule_StaticOptionsComeFirst(t *testing.T) {
	o := newOrder()

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Period: testPeriod, JobTimeout: time.Second}),
		fx.Provide(func() beat.Job { return o.job() }),

		fx.Provide(func() beatfx.Options {
			return beatfx.Options{beat.WithMiddleware(o.middleware("from-container"))}
		}),

		beatfx.Module(beat.WithMiddleware(o.middleware("static"))),
	)

	app.RequireStart()
	got := o.firstRun(t, 3)
	app.RequireStop()

	if want := []string{"static", "from-container", "job"}; !slices.Equal(got, want) {
		t.Errorf("wrapping order = %v, want %v", got, want)
	}
}

// TestModule_LastWriteWins covers the options that overwrite rather than
// accumulate. Which one survived used to depend on the order Fx happened to
// produce; now it is the one written last.
func TestModule_LastWriteWins(t *testing.T) {
	var first, second atomic.Int64

	count := func(c *atomic.Int64) beat.Handler {
		return beat.HandlerFunc(func(context.Context, beat.Record) { c.Add(1) })
	}

	ran := make(chan struct{})
	var once sync.Once

	app := fxtest.New(
		t,

		fx.Supply(beat.Config{Period: testPeriod, JobTimeout: time.Second}),
		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) {
				once.Do(func() { close(ran) })

				return 0, nil
			}
		}),

		fx.Provide(func() beatfx.Options {
			return beatfx.Options{
				beat.WithHandler(count(&first)),
				beat.WithHandler(count(&second)),
			}
		}),

		beatfx.Module(),
	)

	app.RequireStart()
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("the job did not run")
	}
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
		fx.Provide(func() beat.Job {
			return func(context.Context) (int, error) { return 0, nil }
		}),

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
