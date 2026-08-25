# beat

[![CI](https://github.com/uchaloop/beat/actions/workflows/ci.yml/badge.svg)](https://github.com/uchaloop/beat/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/uchaloop/beat.svg)](https://pkg.go.dev/github.com/uchaloop/beat)
[![License: MIT](https://img.shields.io/badge/github/license/uchaloop/beat)](LICENSE)

A background-job scheduler for Go with interval and cron schedules, middleware,
run timeouts, result handlers, and Uber Fx integration.

## Installation

```bash
go get github.com/uchaloop/beat
```

## Fx

```text
BEAT_SPEC=@every 5s
BEAT_JITTER=0s
```

```go
fx.New(
	confx.Module(),
	confx.Provide[beat.Config]("beat"),

	fx.Provide(func() beat.Job {
		return func(ctx context.Context) (int, error) {
			processed := 0
			// Do one unit of work.

			return processed, nil
		}
	}),

	fx.Provide(func(log *slog.Logger) beat.Handler {
		return beat.HandlerFunc(func(_ context.Context, record beat.Record) {
			log.Info(
				"beat run",
				"iteration", record.Iteration,
				"processed", record.Processed,
				"duration", record.Duration,
				"error", record.Err,
			)
		})
	}),

	beatfx.Module(),
).Run()
```

Without a configuration file:

```go
fx.New(
	fx.Supply(beat.Config{
		Spec:       "@every 5s",
		JobTimeout: time.Minute,
	}),
	fx.Provide(func() beat.Job { return work }),
	beatfx.Module(),
)
```

## Configuration

| Field | Variable | Default |
|---|---|---|
| `Spec` | `SPEC` | none - the deployment supplies it |
| `JobTimeout` | `JOB_TIMEOUT` | `1m` |
| `Jitter` | `JITTER` | `0` |

The variables carry the prefix the application gives the instance, so
`confx.Provide[beat.Config]("beat")` reads `BEAT_SPEC` and the rest.

Examples:

```text
BEAT_SPEC=@every 5s
BEAT_SPEC=*/5 * * * * *
```

For interval schedules, the delay is measured after the previous run finishes.
Cron schedules follow wall-clock times and do not queue missed runs.

`JobTimeout` limits each execution. `Jitter` delays the first interval run or
each cron tick to reduce synchronized work across replicas.

## Results

A job returns the number of processed items and an error:

```go
type Job func(context.Context) (int, error)
```

After every run, a handler receives:

```go
type Record struct {
	Iteration int64
	Start     time.Time
	Duration  time.Duration
	Processed int
	Err       error
	Mode      beat.Mode
}
```

Send a record to several destinations:

```go
beat.MultiHandler(metricsHandler, loggingHandler)
```

## Options

Pass static options to `beatfx.Module`:

```go
beatfx.Module(
	beat.WithMiddleware(
		recovery.Middleware(),
		idle.Middleware(backoff),
	),
	beat.WithGracefulStop(),
)
```

Available options:

- `WithMiddleware`
- `WithHandler`
- `WithOnStart`
- `WithOnStop`
- `WithGracefulStop`

Build an option from Fx dependencies:

```go
beatfx.AsOption(func(db *sql.DB) beat.Option {
	return beat.WithOnStart(db.PingContext)
})
```

By default, shutdown cancels the running job. `WithGracefulStop` lets it finish
within the application's Fx stop timeout.

## Middleware

Middleware wraps a job:

```go
type Middleware func(next beat.Job) beat.Job
```

In `WithMiddleware(a, b, c)`, `a` is the outermost middleware.

### Panic recovery

By default, a panic terminates the process. Add recovery middleware to keep the
scheduler running:

```go
recovery.Middleware(
	recovery.WithLogger(logger),
)
```

The recovered panic is reported as `*beat.PanicError`.

### Idle backoff

```go
idle.Middleware(func(err error) time.Duration {
	if err != nil {
		return 5 * time.Second
	}

	return 0
})
```

### Batch delay

```go
batch.Middleware(func(processed int, err error) time.Duration {
	if processed == 0 {
		return 3 * time.Second
	}

	return 0
})
```

### Custom middleware

```go
func WithOperation(name string) beat.Middleware {
	return func(next beat.Job) beat.Job {
		return func(ctx context.Context) (int, error) {
			ctx = context.WithValue(ctx, operationKey{}, name)

			return next(ctx)
		}
	}
}
```

## Without Fx

```go
scheduler, err := beat.MakeBeat(cfg, job, handler, opts...)
if err != nil {
	return err
}

if err := scheduler.Start(ctx); err != nil {
	return err
}
defer scheduler.Stop(context.Background())
```

## OpenTelemetry

Use [`otelbeat`](https://github.com/uchaloop/otelbeat) to record run duration,
processed item counts, status, and schedule mode:

```go
fx.New(
	otelbeatfx.Module(),
	beatfx.Module(),
)
```

## Acknowledgements

I am grateful to the authors of [robfig/cron](https://github.com/robfig/cron)
and [Uber Fx](https://github.com/uber-go/fx). Their work made this library
possible.

## License

[MIT](LICENSE)
