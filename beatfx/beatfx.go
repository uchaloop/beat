// Package beatfx wires a beat.Beat into an Uber Fx application: it builds the
// Beat from the container's Config, Job and optional Handler, and drives its
// Start/Stop from the Fx lifecycle. The core beat package has no Fx dependency;
// this package is the Fx integration, mirroring confmaker/confx.
package beatfx

import (
	"errors"

	"github.com/uchaloop/beat"
	"go.uber.org/fx"
)

// optionGroup is the Fx value group Module reads DI-built options from. Register
// into it with AsOption. It must stay in sync with the group tag on
// params.Options, which cannot reference this constant because a struct tag must
// be a literal.
const optionGroup = `group:"beat_options"`

// params are the container dependencies Module consumes. Config and Job are
// required; Handler is optional and defaults to a no-op; Options are the
// DI-built options contributed through AsOption.
type params struct {
	fx.In

	Config  beat.Config
	Job     beat.Job
	Handler beat.Handler `optional:"true"`
	// The group name must match optionGroup (a struct tag must be a literal).
	Options []beat.Option `group:"beat_options"`
}

// Module wires a Beat into an Fx application. It consumes a Config (typically
// provided by confmaker/confx), a Job and an optional Handler, then drives the
// run loop from the Fx lifecycle. beat is single-instance: use one Module per
// application.
//
// Options can be supplied two ways, and both are applied (static first, then the
// group): pass static ones - that need no dependencies - directly here, and
// register ones that must be built from other container values with AsOption.
//
//	fx.New(
//		confx.Module(),
//		confx.Provide[beat.Config]("beat"),
//		fx.Provide(func() beat.Job { return work }),
//		fx.Provide(func() beat.Handler { return metricsHandler }),
//		beatfx.AsOption(func(db *sql.DB) beat.Option { // DI-built option
//			return beat.WithOnStart(db.PingContext)
//		}),
//		beatfx.Module(beat.WithMiddleware(recovery.Middleware())), // static option
//	)
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

// AsOption registers an option into Module's option group. It accepts either:
//
//   - a ready beat.Option - for an option that needs no container dependencies
//     (though such options can also be passed straight to Module):
//
//     beatfx.AsOption(beat.WithMiddleware(recovery.Middleware()))
//
//   - a constructor func(deps...) beat.Option - for hooks and middleware built
//     from other container values, which Fx resolves and injects:
//
//     beatfx.AsOption(func(log *slog.Logger) beat.Option {
//     return beat.WithMiddleware(logging.Middleware(log))
//     })
func AsOption(optionOrCtor any) fx.Option {
	if optionOrCtor == nil {
		return fx.Error(errors.New("AsOption called with nil; pass a beat.Option or a func(...) beat.Option"))
	}

	ctor := optionOrCtor
	if opt, ok := optionOrCtor.(beat.Option); ok {
		ctor = func() beat.Option { return opt }
	}

	return fx.Provide(fx.Annotate(ctor, fx.ResultTags(optionGroup)))
}

func register(lc fx.Lifecycle, b *beat.Beat) {
	lc.Append(
		fx.Hook{
			OnStart: b.Start,
			OnStop:  b.Stop,
		},
	)
}
