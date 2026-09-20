// Package beatfx connects one beat.Beat to an Fx lifecycle.
// Module requires beat.Config and a *job.Runner. beat.Handler and Options are optional.
// Provide Options once as an ordered slice; static Module options are applied
// first, followed by the slice. Repeated setter options keep this order, with
// wrapper outermost; repeated setter options use their last value.
//
// A ready set can be supplied with fx.Supply(beatfx.Options{...}). A constructor
// returning Options can use dependencies from the container. The application
// owns Fx startup and shutdown budgets. Use one Module per Fx application.
package beatfx

import (
	"context"

	"github.com/uchaloop/beat"
	"github.com/uchaloop/job"
	"go.uber.org/fx"
)

// Options is the ordered set of beat options an application builds from the
// container. Provide it once, and the order inside it is the order applied:
//
//	fx.Provide(func(db *sql.DB) beatfx.Options {
//		return beatfx.Options{beat.WithOnStart(db.PingContext)}
//	})
//
// A set that needs nothing from the container can be supplied outright with
// fx.Supply(beatfx.Options{...}), though such options can also go straight to
// [Module].
type Options []beat.Option

// params are the container dependencies Module consumes. Config and Runner are
// required; Handler is optional and defaults to a no-op; Options are the
// container-built options, if the application provides any.
type params struct {
	fx.In

	Config  beat.Config
	Runner  *job.Runner
	Handler beat.Handler `optional:"true"`
	Options Options      `optional:"true"`
}

// Module provides a private Beat and registers its Start and Stop hooks.
// Static opts are applied before the optional container-provided Options.
func Module(opts ...beat.Option) fx.Option {
	return fx.Module(
		"beat",

		fx.Provide(
			fx.Private,

			func(p params) (*beat.Beat, error) {
				all := make([]beat.Option, 0, len(opts)+len(p.Options)+1)

				// A Handler from the container comes first so that an explicit
				// WithHandler, static or container-built, still overrides it.
				if p.Handler != nil {
					all = append(all, beat.WithHandler(p.Handler))
				}

				all = append(all, opts...)
				all = append(all, p.Options...)

				return beat.MakeBeat(p.Config, p.Runner, all...)
			},
		),

		fx.Invoke(register),
	)
}

// register drives the Beat from the Fx lifecycle, and asks the application to
// shut down if the loop ends without being asked to.
//
// A scheduler whose loop has left has nothing further to do, so leaving the
// process up would hide the failure until something else stopped it: the daemon
// would look healthy while its queue went unserved.
//
// It is a request, not a shutdown. fx.Shutdowner broadcasts a signal; an
// application using fx.App.Run receives it and stops, and one driving the
// lifecycle by hand must wait on fx.App.Wait and call Stop itself, or the
// request goes unanswered. The stop runs the OnStop hook, which calls
// beat.Beat.Stop, which reports the error the loop left behind - so the reason
// reaches the application the ordinary way.
func register(lc fx.Lifecycle, b *beat.Beat, shutdowner fx.Shutdowner) {
	// Closed before Stop, so the watcher can tell an ordinary shutdown from a
	// loop that ended by itself.
	stopping := make(chan struct{})

	lc.Append(
		fx.Hook{
			OnStart: func(ctx context.Context) error {
				if err := b.Start(ctx); err != nil {
					return err
				}

				go watchLoop(b.Done(), stopping, shutdowner)

				return nil
			},

			OnStop: func(ctx context.Context) error {
				close(stopping)

				return b.Stop(ctx)
			},
		},
	)
}

func watchLoop(done, stopping <-chan struct{}, shutdowner fx.Shutdowner) {
	select {
	case <-stopping:
		return
	case <-done:
	}

	select {
	case <-stopping:
		// An ordinary shutdown asked for this; Fx is already on its way out.
	default:
		// The code states the reason for anyone reading fx.App.Wait; the loop's
		// own error still surfaces through Stop either way. The result is
		// ignored deliberately: this is a request, and there is nobody left to
		// report a failed one to.
		_ = shutdowner.Shutdown(fx.ExitCode(1))
	}
}
