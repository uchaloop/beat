# beat

[![CI](https://github.com/uchaloop/beat/actions/workflows/ci.yml/badge.svg)](https://github.com/uchaloop/beat/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/uchaloop/beat.svg)](https://pkg.go.dev/github.com/uchaloop/beat)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`beat` runs a single background job on a schedule inside an
[Uber Fx](https://github.com/uber-go/fx) application: once on an interval
(`@every 5s`) or a cron expression (`*/5 * * * * *`), bounded by a per-run
timeout, wrapped by a chain of middleware.

It has **no built-in metrics**. After every run it hands a `Record` to a
`Handler`, the same way `slog` hands a record to its handler - metrics, logging
and tracing are adapters you supply. Configuration is a plain struct with
`koanf` and `env` tags loaded by the application through
[`confmaker/confx`](https://github.com/uchaloop/confmaker); `beat` never reads
files or the environment itself.

The core package `github.com/uchaloop/beat` is the scheduler and has no Fx
dependency - drive it directly with `MakeBeat` + `(*Beat).Start`/`Stop`. The Fx
integration (`Module`, `AsOption`) lives in `github.com/uchaloop/beat/beatfx`.

## Install

```bash
go get github.com/uchaloop/beat
```

## Core contracts

```go
type Job func(ctx context.Context) (int, error)     // your work, one run
type Middleware func(next Job) Job                   // classic composition

type Record struct {                                 // one completed run
    Iteration int64
    Start     time.Time
    Duration  time.Duration
    Processed int         // items processed this run; 0 if not applicable
    Err       error       // a recovered panic arrives as *beat.PanicError
    Mode      Mode        // "interval" or "cron"
}

type Handler interface { Handle(ctx context.Context, r Record) }
type HandlerFunc func(ctx context.Context, r Record) // adapter, like slog
```

## Quick start (with confmaker/confx)

```go
fx.New(
    // The application owns the config source.
    confx.LoadModule("config/local.toml"),
    confx.ProvideDefault[beat.Config]("beat"), // [beat] table + BEAT_* env
    fx.Supply(slog.Default()),                 // a *slog.Logger for the Handler

    // The business logic.
    fx.Provide(
        func() beat.Job {
            return func(ctx context.Context) (int, error) {
                var processed int
                // ... payload ...
                return processed, nil
            }
        },
    ),

    // Observability, injected from outside.
    fx.Provide(
        func(log *slog.Logger) beat.Handler {
            return beat.HandlerFunc(
                func(_ context.Context, r beat.Record) {
                    log.Info(
                        "beat run",
                        "iteration", r.Iteration,
                        "processed", r.Processed,
                        "duration", r.Duration,
                        "err", r.Err,
                    )
                },
            )
        },
    ),

    beatfx.Module(),
).Run()
```

```toml
# config/local.toml
[beat]
spec        = "@every 5s"
job_timeout = "1m"
jitter      = "0s"
```

### Without a config file

Supply the `Config` directly - for a service with no config file, or in tests:

```go
fx.New(
    fx.Supply(beat.Config{Spec: "@every 5s", JobTimeout: time.Minute}),
    fx.Provide(func() beat.Job { return work }),
    beatfx.Module(),
).Run()
```

## Configuration

`beat.Config` declares both tag namespaces; `confx` fills `koanf` from the file
and `env` from the environment (env overrides file).

| Field | koanf | env | Default | Description |
|---|---|---|---|---|
| `Spec` | `spec` | `SPEC` (required) | | `@every 5s` interval or cron `*/5 * * * * *` |
| `JobTimeout` | `job_timeout` | `JOB_TIMEOUT` | `1m` | timeout on one Job execution; `0`/unset uses the default |
| `Jitter` | `jitter` | `JITTER` | `0` | max random delay to stagger instances |

`JobTimeout`: every execution is bounded, so a Job that ignores `ctx` cannot
silently wedge the loop. Set a larger value for a legitimately long Job.

`Jitter`: for an interval it is applied once before the first run; for cron it is
added to every tick, so keep it below the cron interval.

## Scheduling semantics

- **`@every` (interval)** measures the gap from the end of one run to the start
  of the next, so the effective period grows by the Job's duration. The first
  run fires right after the start delay.
- **cron** fires at fixed wall-clock points and skips a point a long run
  overruns - it never queues catch-up runs.

The core does not recover panics. A panic in the Job (or the `Handler`)
propagates and crashes the process, with a full stack on stderr - the honest
default for user code that misbehaves; let the orchestrator restart. To keep the
scheduler alive instead, add `middleware/recovery`: it recovers the panic,
optionally logs it with its stack (your logger), and reports it as a
`*beat.PanicError` on the `Record`.

**Stopping.** By default the in-flight run's context is cancelled on shutdown.
`WithGracefulStop()` instead lets the current run finish and only cancels it if
it outlives `fx.StopTimeout` - use it for jobs that should not be interrupted
mid-unit-of-work.

## Lifecycle hooks and timeouts

`WithOnStart` / `WithOnStop` run during the Fx lifecycle and get the context Fx
passes in. That context is bounded by the application's `fx.StartTimeout` /
`fx.StopTimeout` (both default 15s) and is **shared across all of the app's
hooks** - beat adds no per-hook timeout. A hook that needs longer than the
default must be matched by raising the timeout on the top-level App:

```go
fx.New( /* ... */, fx.StartTimeout(time.Minute), fx.StopTimeout(30*time.Second))
```

The run loop is not affected by this: it runs in its own goroutine, so a Job (and
any middleware around it) can run as long as it likes, bounded only by
`JobTimeout` per run and cancellation on shutdown. Keep the hooks for fast
readiness work (open a pool, ping a dependency); put the real workload in the Job.

## Options

Passed to `beatfx.Module(...)`:

- `WithMiddleware(mw ...Middleware)` - the first middleware is the outermost layer and runs first.
- `WithHandler(h Handler)` - overrides a `Handler` from the container.
- `WithOnStart(fn)` / `WithOnStop(fn)` - lifecycle hooks.
- `WithGracefulStop()` - let the in-flight run finish on shutdown instead of cancelling it.

Options that need no dependencies go straight to `Module`. Options built from
other container values are registered with `AsOption`, which accepts either a
ready `Option` or a `func(deps...) Option` constructor Fx injects into; static
and DI-built options are mixed (static apply first):

```go
fx.New(
    // built from a dependency - Fx injects *sql.DB
    beatfx.AsOption(
        func(db *sql.DB) beat.Option {
            return beat.WithOnStart(db.PingContext)
        },
    ),
    beatfx.Module(beat.WithMiddleware(recovery.Middleware())), // no deps
)
```

## Middleware

A `Middleware` is `func(next Job) Job` - standard Go composition, so each one is
an ordinary function you can test in isolation. In `WithMiddleware(a, b, c)` the
first is the outermost layer and runs first: `a -> b -> c -> Job`.

Built-in packages under `github.com/uchaloop/beat/middleware`:

### recovery

The core lets a panic crash the process; add this middleware to keep the loop
alive. It recovers the panic, optionally logs it with its stack (`WithLogger`),
and always reports it as a `*beat.PanicError` on the `Record` - so downstream
handlers (e.g. `otelbeat`) can classify it as a panic:

```go
recovery.Middleware(recovery.WithLogger(logger))
```

Put it first in `WithMiddleware` so it also catches panics from the middleware
inside it. For a custom panic-to-result mapping, write your own middleware (see
*Writing your own*).

### idle

Pauses after each run for a duration chosen from the run's error - a backoff on
failure. The pause honours context cancellation:

```go
idle.Middleware(func(err error) time.Duration {
    if err != nil {
        return 5 * time.Second
    }
	
    return 0
})
```

### batch

Like `idle`, but the delay is chosen from how much the run processed and its
error - drain a queue fast while it is full, back off when it empties:

```go
batch.Middleware(func(processed int, err error) time.Duration {
    if processed == 0 {
        return 3 * time.Second
    }
	
    return 0
})
```

### Writing your own

A `Middleware` is an ordinary function - no framework needed. A before/after
wrapper that enriches the context and rewrites the result is just:

```go
func WithOp(name string) beat.Middleware {
    return func(next beat.Job) beat.Job {
        return func(ctx context.Context) (int, error) {
            ctx = context.WithValue(ctx, opKey{}, name)

            processed, err := next(ctx)
            // inspect / wrap the result here
            return processed, err
        }
    }
}
```

### Composing and injecting

Static middleware goes straight to `Module`:

```go
beatfx.Module(beat.WithMiddleware(
    recovery.Middleware(),
    idle.Middleware(backoff),
))
```

Middleware that needs a container dependency is built with `AsOption`, where Fx
injects the dependency:

```go
beatfx.AsOption(
    func(log *slog.Logger) beat.Option {
        return beat.WithMiddleware(logging.Middleware(log))
    },
)
```

## Metrics

The core has no metrics - it hands each run to a `Handler`. For OpenTelemetry,
use the companion module
[`github.com/uchaloop/otelbeat`](https://github.com/uchaloop/otelbeat): a ready
`Handler` recording run duration and processed counts, sliced by `status` and
`mode` attributes. For any other backend, implement `Handler` yourself.

Run several sinks at once - metrics plus logging, say - with `MultiHandler`,
which fans each `Record` out behind `Module`'s single `Handler` slot:

```go
beat.MultiHandler(metrics, logging)
```

## Acknowledgements

`beat` builds on [robfig/cron](https://github.com/robfig/cron) and
[uber-go/fx](https://github.com/uber-go/fx). Thanks to their authors and
maintainers.

## License

MIT.
