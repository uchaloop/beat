# beat

<!--suppress HtmlDeprecatedAttribute -->
<p align="center"><img src="logo.svg" alt="beat — Go gopher holding a heartbeat line" width="240"></p>

[![Go Reference](https://pkg.go.dev/badge/github.com/uchaloop/beat.svg)](https://pkg.go.dev/github.com/uchaloop/beat) [![CI](https://github.com/uchaloop/beat/actions/workflows/ci.yml/badge.svg)](https://github.com/uchaloop/beat/actions/workflows/ci.yml) [![Release](https://img.shields.io/github/v/tag/uchaloop/beat?label=release)](https://github.com/uchaloop/beat/tags) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[Install](#installation) · [Quick start](#quick-start) · [Scheduling](#scheduling) · [Configuration](#configuration-and-options) · [Shutdown](#lifecycle-and-shutdown)

Run one recurring job in a long-lived Go process. beat provides sequential
execution, fixed-rate or fixed-delay scheduling, replica offsets, cooperative
timeouts, and optional Fx integration. Requires Go 1.27 or later.

Use it for frequent polling and background work that benefits from reusable
connections and in-memory state. beat has no calendar expressions, persistent
schedule, replay after downtime, or coordination between replicas.

## Installation

```sh
go get github.com/uchaloop/beat
```

## Quick start

```go
package main

import (
    "context"
    "log/slog"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/uchaloop/beat"
    "github.com/uchaloop/job"
)

func main() {
    shutdown, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    work := func(ctx context.Context) (int64, error) {
        // Replace this timer with one bounded batch of application work.
        timer := time.NewTimer(20 * time.Millisecond)
        defer timer.Stop()
        select {
        case <-timer.C:
            return 1, nil
        case <-ctx.Done():
            return 0, ctx.Err()
        }
    }
    handler := beat.HandlerFunc(func(_ context.Context, r beat.Record) {
        slog.Info("attempt completed", "outcome", r.Result.Outcome,
            "processed", r.Result.Processed, "duration", r.Result.Duration,
            "missed", r.Missed)
    })

    // The work, its middleware and its timeout belong to the runner.
    runner, err := job.MakeRunner(job.Config{Timeout: 2 * time.Second}, work)
    if err != nil {
        slog.Error("configure runner", "error", err)
        return
    }

    scheduler, err := beat.MakeBeat(
        beat.Config{Period: 5 * time.Second, Jitter: time.Second},
        runner,
        beat.WithHandler(handler),
        beat.WithGracefulStop(),
    )
    if err != nil {
        slog.Error("configure beat", "error", err)
        return
    }
    if err := scheduler.Start(shutdown); err != nil {
        slog.Error("start beat", "error", err)
        return
    }
    select {
    case <-shutdown.Done():
    case <-scheduler.Done(): // A terminal loop error is returned by Stop.
    }
    stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer stopCancel()
    if err := scheduler.Stop(stopCtx); err != nil {
        slog.Error("stop beat", "error", err)
    }
}
```

`Start`'s context applies to startup only. To stop a running Beat, call `Stop`
with a fresh context. A nil Handler is allowed.

## Scheduling

| Mode | First run | Following runs |
|---|---|---|
| `ModeFixedRate` (default) | Next grid point strictly after startup | Grid points `k × Period + offset`, anchored to the Unix epoch |
| `ModeFixedDelay` | Startup time + offset | At least `Period` after the previous Job returns |

> [!NOTE]
> An attempt error does not stop the schedule or trigger an immediate retry.

A Beat never overlaps its Job calls. Fixed-rate runs that overrun grid points
move to the first available point; no backlog of runs is queued. A pending target
that is already past when the loop wakes runs once immediately, so the following
run may be close behind. A restart joins the grid afresh.

For example, with a 5-minute period, a 25-second offset and a 4m50s Job:

```text
Fixed rate:   10:00:25 ── Job ── 10:05:15   → next start 10:05:25
Fixed delay:  10:00:25 ── Job ── 10:05:15   → next start 10:10:15
```

### Run loop

```mermaid
flowchart TD
    A[Start: run OnStart] --> B[Choose first target]
    B --> C[Wait for target or stop request]
    C --> D{Stopping?}
    D -->|yes| Z[Exit loop and close Done]
    D -->|no| E{Point owned here?}
    E -->|no| H
    E -->|yes| Q{"Attempt admitted before Stop?"}
    Q -->|no| Z
    Q -->|yes| R["job.Runner.Run: middleware and work"]
    R --> F[Build Record and call Handler inline]
    F --> G[Evaluate backoff from Record]
    G --> H[Compute next target and Missed for the next Record]
    H --> C
```

`Result.Duration` covers work, middleware and optional job.ErrorHandler.
It excludes the observability Handler and backoff.
The observability Handler and backoff callback still occupy the loop: keep both short.
Configure error processing with `job.WithErrorHandler` on the Runner. Its separate
timeout also consumes scheduling and shutdown time; foreign cluster points never
call it because they do not run the work.
Fixed-rate waits periodically recheck wall time; fixed-delay waits use monotonic
time. Neither mode is a real-time execution guarantee.

## Configuration and options

beat accepts an ordinary `Config` and never reads environment variables.
The env names below apply to the optional loader described separately.

| Field | Default env name | Default | Constraint |
|---|---|---|---|
| `Period` | `BEAT_PERIOD` | Required | Greater than zero |
| `Jitter` | `BEAT_JITTER` | `0` | Between zero and Period, inclusive |

The attempt's timeout is not here: it belongs to the `job.Runner` beat drives,
and is configured with `job.Config.Timeout`. It cancels the work's context and cannot interrupt
the function - work must respect cancellation and join its own goroutines before
returning. Work that never returns blocks subsequent runs and their records.

Options select the mode, offset, backoff, Handler, cluster rotation and
lifecycle hooks. Repeated setter options use the last value. `WithGracefulStop`
enables draining on shutdown. Middleware belongs to the Runner, not here.

### Polling with backoff

With a short fixed-delay period, continue polling when a batch is full and rest
longer after an empty or partial batch:

```go
opts := []beat.Option{
    beat.WithMode(beat.ModeFixedDelay), // for example, Config.Period = 1s
    beat.WithBackoff(func(r beat.Record) time.Duration {
        if r.Result.Outcome != job.OutcomeOK {
            return 30 * time.Second
        }
        if r.Result.Processed < 1000 {
            return 5 * time.Minute
        }
        return 0
    }),
}
```

Backoff is a minimum pause measured from completion of the entire attempt
(including job.ErrorHandler), not an addition to
Period. In fixed-delay mode the next target is the latest of Job end + Period,
Job end + positive backoff, and the time scheduling resumes. In fixed-rate mode
it is the next eligible grid point no earlier than scheduling resumes or the
backoff permits. Returning zero or a negative value adds no restriction; it
does not remove Period. Deliberately bypassed backoff points are not Missed.

### Replica offsets

Jitter draws one offset in `[0, Jitter)` per Beat; zero gives no offset.
In fixed-rate mode it shifts every grid point. In fixed-delay mode it delays
only the first run. `Jitter == Period` permits spreading across the full period.
A wider range spreads planned starts but can increase initial waiting time.

To derive an offset from an application-provided identity:

```go
offset := beat.OffsetFor(service+"/"+cluster+"/"+instance, cfg.Jitter)
scheduler, err := beat.MakeBeat(cfg, runner,
    beat.WithHandler(handler),
    beat.WithOffset(offset),
)
```

`WithOffset` requires `0 <= offset < Period`. `OffsetFor` is stable only while
its input is stable; replacing a Deployment Pod usually changes its name.
Hashing and random draws do not guarantee gaps between replicas. Services may
share the same Jitter range.

Offsets spread starts; they do not divide work or prevent duplicate effects.
Queue claiming, retries and idempotency belong to the application, including
when consumers run in different clusters.

## Records and observability

A Handler receives one Record after each completed Job:

| Fields | Meaning |
|---|---|
| `Iteration`, `Mode`, `Period`, `LocalPeriod` | Attempt number, mode, base step and local nominal step |
| `GridPoint`, `ScheduledFor` | Shared point before offset and this replica's target |
| `Result.Start`, `Result.Duration` | Actual start and total attempt time, including ErrorHandler |
| `Result.WorkDuration`, `Result.ErrorHandlerDuration` | Duration of each execution stage |
| `Result.ErrorHandlerCalled` | Whether error processing ran, even if its duration is zero |
| `Result.ErrorHandlerErr` | Failure of error processing, separate from Result.Err |
| `Result.Processed`, `Result.Err` | Values returned by the work |
| `Result.Outcome` | `ok`, `error`, `panic`, `timeout` or `canceled` |
| `Missed` | Unintentional grid losses computed after the preceding run |

`Missed` is a `uint64`; counts exceeding `MaxUint64` saturate instead of wrapping.

`Result.Processed` is an `int64`, preserved as returned by `job.Func`. The work
should report a non-negative count, including partial progress on error.

Outcome is authoritative even if Err is nil. A recovered panic takes precedence,
then cancellation of the Job context (timeout or shutdown), then the Job's error.
Missed is zero in fixed-delay mode. It excludes intentional backoff and is
reported with the next completed Job, so a shutdown or a stuck Job may prevent
its delivery. The current Record's duration does not identify the cause of its
Missed count.

Handlers run inline with a context without cancellation or a deadline. Give any
I/O its own budget. `MultiHandler` calls sinks sequentially. The application owns its observability handlers and exporters.

## Lifecycle and shutdown

- Start may run once. Repeated Start returns `ErrAlreadyStarted`; a stopped
  Beat returns `ErrStopped` and cannot be restarted.
- Stop prevents further scheduling. Normally it cancels the active attempt at
  once; `WithGracefulStop` lets it finish within Stop's budget. The runner's own
  timeout still applies.
- Concurrent Stop callers share the first Stop's result, but each waiting caller
  may return early if its own context expires.
- Stop waits for an in-progress OnStart. If OnStart fails, OnStop is not called;
  the startup hook must clean up partially acquired resources on failure.
- If OnStart succeeds but startup's context is cancelled, Start arranges OnStop
  with a fresh 15-second context unless a concurrent Stop already owns cleanup.
- If startup or the loop has not finished when Stop stops waiting, Stop returns
  `ErrStillRunning` joined with its context error. OnStop is skipped permanently.
- Otherwise OnStop runs synchronously after the loop exits. Hooks must respect
  their contexts; a blocked hook can outlive the supplied deadline.

> [!IMPORTANT]
> `Done` means loop activity has ended. It does not mean cleanup has completed.

Do not call `Stop` synchronously from the work, handlers, backoff callback or
lifecycle hooks: it waits for that callback to return. Signal the application's
lifecycle owner through a channel, return, and let the owner call `Stop`.

`Done()` closes after startup/run-loop activity finishes, **before OnStop**. It
closes for two reasons: a `Stop` you called, and a loop that ended by itself
after an error it cannot carry on past - `Stop` then reports that error. Use it
for fallback cleanup after Stop reports `ErrStillRunning`, and to notice the
second case early. Bound any additional wait according to the application's
shutdown policy:

```go
if errors.Is(stopErr, beat.ErrStillRunning) {
    select {
    case <-scheduler.Done():
        // Startup and the loop have ended; perform application-owned cleanup.
    case <-cleanupCtx.Done():
        // Leave final termination to the process supervisor.
    }
}
```

Under `beatfx` this is connected for you: a loop that ends by itself asks the Fx
application to stop, with exit code 1, and its error surfaces through the OnStop
hook. It is a **request** - `fx.Shutdowner` broadcasts a signal. An application
using `app.Run()` receives it and stops; one driving the lifecycle by hand must
wait on `app.Wait()` and call `Stop` itself, or the request goes unanswered.

## Fx and panic recovery

The optional [beatfx](https://github.com/uchaloop/beatfx) adapter is a separate
module. Install it with `go get github.com/uchaloop/beatfx@v0.1.0`.

The adapter supplies lifecycle wiring; the application supplies `beat.Config`,
a `*job.Runner` and optional handlers. See the
[beatfx quick start](https://github.com/uchaloop/beatfx#quick-start) for a complete
Fx daemon. The standalone example above needs no DI framework.

Panics propagate by default. Recovery middleware catches panics only inside the
wrapped work and returns `*job.PanicError`; it does not protect the Handler,
the backoff callback, hooks, or goroutines the work created.

## Running in several clusters

`WithAssignment` adds an optional rotation from `job/assignment`: one cluster
owns each grid point and all of its replicas run it, while the others skip. It
is off by default, requires `ModeFixedRate` and must share `Config.Period`.

```go
rotation, err := assignment.MakeRotation(assignment.Config{
    Clusters: []string{"el", "xc", "dm"},
    Current:  "el",
    Period:   5 * time.Minute,
})
```

A point another cluster owns produces no run, no Record and no backoff, and is
not counted as `Missed` - it was never this process's to serve. Use
`WithDecisionHandler` to observe those decisions, since nothing else reports
them. With N clusters `Record.LocalPeriod` is `N * Period`: three clusters on a
five-minute period means each attempts every fifteen minutes.

The rotation names an owner; it does not guarantee the owner runs. A cluster
that is down leaves its points unserved, and no other cluster takes over - that
would need shared state the policy deliberately does not have. Work must
therefore survive a skipped attempt.

When changing the cluster set or period, stop scheduling in all clusters and
wait for old attempts to finish before starting the new configuration. A rolling
restart can leave old and new ownership rules active together, producing both
duplicate executions and unserved points.

## Documentation

[API reference](https://pkg.go.dev/github.com/uchaloop/beat) ·
[Compilable examples](example_test.go)

## Recommended configuration

> [!TIP]
> We recommend [confmaker](https://github.com/uchaloop/confmaker) for typed ENV
> configuration and [confx](https://github.com/uchaloop/confx) for its Fx integration.
> Configuration loading stays in the application; it is optional for the work libraries.

<details>
<summary><strong>Configure from ENV with confmaker / confx</strong></summary>

The ordinary `beat.Config{...}` in the quick start can be replaced with:

```go
cfg, err := confmaker.Load[beat.Config]()
if err != nil {
    return err
}
// Pass cfg to beat.MakeBeat.
```

Import `github.com/uchaloop/confmaker`. No Fx dependency is needed.

Example environment: `BEAT_PERIOD=5m BEAT_JITTER=30s`. Configure the Runner
separately with `job.Config` (`JOB_TIMEOUT`).

For an Fx application, replace `fx.Supply(beat.Config{...})` with:

```go
confx.Module(),
confx.Provide[beat.Config](),
```

Import `github.com/uchaloop/confx` and include `confx.Module()` once per application.

</details>

## Related libraries

| Library | Purpose |
|---|---|
| [job](https://github.com/uchaloop/job) | One attempt, middleware and timeout |
| [beatfx](https://github.com/uchaloop/beatfx) | Connect the scheduler to Fx |

## License

[MIT](LICENSE)
