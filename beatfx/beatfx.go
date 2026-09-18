// Package beatfx connects one beat.Beat to an Fx lifecycle.
// Module requires beat.Config and beat.Job. beat.Handler and Options are optional.
// Provide Options once as an ordered slice; static Module options are applied
// first, followed by the slice. Middleware keeps this order, with the first
// wrapper outermost; repeated setter options use their last value.
//
// A ready set can be supplied with fx.Supply(beatfx.Options{...}). A constructor
// returning Options can use dependencies from the container. The application
// owns Fx startup and shutdown budgets. Use one Module per Fx application.
package beatfx

import (
	"github.com/uchaloop/beat"
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

// params are the container dependencies Module consumes. Config and Job are
// required; Handler is optional and defaults to a no-op; Options are the
// container-built options, if the application provides any.
type params struct {
	fx.In

	Config  beat.Config
	Job     beat.Job
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
				all := make([]beat.Option, 0, len(opts)+len(p.Options))
				all = append(all, opts...)
				all = append(all, p.Options...)

				return beat.MakeBeat(p.Config, p.Job, p.Handler, all...)
			},
		),

		fx.Invoke(register),
	)
}

func register(lc fx.Lifecycle, b *beat.Beat) {
	lc.Append(
		fx.Hook{
			OnStart: b.Start,
			OnStop:  b.Stop,
		},
	)
}
