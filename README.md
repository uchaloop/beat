<p align="center">
  <img src="logo.png" alt="beat" width="320">
</p>

<p align="center">
  <a href="https://github.com/uchaloop/beat/actions/workflows/ci.yml"><img src="https://github.com/uchaloop/beat/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pkg.go.dev/github.com/uchaloop/beat"><img src="https://pkg.go.dev/badge/github.com/uchaloop/beat.svg" alt="Go Reference"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/uchaloop/beat" alt="License: MIT"></a>
</p>

A background-job scheduler for Go: one job, an interval or cron schedule,
middleware, a run timeout, and Uber Fx integration.

- **No built-in metrics** - after every run a `Record` goes to a `Handler`, the
  way slog hands a record to its handler. Metrics, logging and tracing are
  adapters you supply.
- **It reads no config source** - `Config` is a plain struct with env tags the
  application loads and supplies.
- **Every run is bounded**, so a job that ignores its context cannot wedge the
  loop.
- **Panics crash the process** unless you say otherwise, which is a middleware
  away.

```bash
go get github.com/uchaloop/beat
```

## Quick start

```text
BEAT_SPEC=@every 5s
BEAT_JOB_TIMEOUT=1m
```

```go
fx.New(
	confx.Module(),
	confx.Provide[beat.Config]("beat"),

	fx.Provide(func() beat.Job {
		return func(ctx context.Context) (processed int, err error) {
			// Do one unit of work.
			return processed, nil
		}
	}),

	fx.Provide(func(log *slog.Logger) beat.Handler {
		return beat.HandlerFunc(func(_ context.Context, r beat.Record) {
			log.Info("beat run",
				"iteration", r.Iteration,
				"processed", r.Processed,
				"duration", r.Duration,
				"error", r.Err,
			)
		})
	}),

	beatfx.Module(beat.WithMiddleware(recovery.Middleware())),
).Run()
```

Without Fx:

```go
scheduler, err := beat.MakeBeat(cfg, job, handler, opts...)
```

## Configuration

| Field | Variable | Default |
|---|---|---|
| `Spec` | `SPEC` | none - the deployment has to supply it |
| `JobTimeout` | `JOB_TIMEOUT` | `1m`, from `Config.SetDefaults` |
| `Jitter` | `JITTER` | `0` |

The prefix comes from the instance name, so `confx.Provide[beat.Config]("beat")`
reads `BEAT_SPEC` and the rest, and `confx.Manifest[beat.Config]("beat")` lists
the same set from the type.

## Middleware

```go
beat.WithMiddleware(
	recovery.Middleware(recovery.WithLogger(logger)),  // keep the loop alive
	idle.Middleware(backoff),                          // pause on failure
	batch.Middleware(drain),                           // pause when there is nothing to do
)
```

The first is outermost. A middleware is `func(next beat.Job) beat.Job`, so
writing your own needs nothing from this package.

## OpenTelemetry

[otelbeat](https://github.com/uchaloop/otelbeat) records run duration, processed
counts, status and schedule mode:

```go
fx.New(
	otelbeatfx.Module(),
	beatfx.Module(),
)
```

## Documentation

The scheduling modes, the options, the record and the reasons behind them are in
the package documentation:
**[pkg.go.dev/github.com/uchaloop/beat](https://pkg.go.dev/github.com/uchaloop/beat)**.
Each middleware and the Fx integration document themselves.

## Acknowledgements

I am grateful to the authors of [robfig/cron](https://github.com/robfig/cron)
and [Uber Fx](https://github.com/uber-go/fx). Their work made this library
possible.

## License

[MIT](LICENSE)
