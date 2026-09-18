// Package beat schedules one Job in a long-lived process. Job calls are
// sequential within a Beat. The package accepts configuration and emits Records;
// it does not load configuration, export telemetry, or coordinate replicas.
//
// # Scheduling
//
// ModeFixedRate is the default: targets lie on an absolute grid of Period since
// the Unix epoch, shifted by an offset. The first target is strictly after
// startup. After each run, unavailable points are bypassed without queuing runs.
// If a pending target is already past when the loop wakes, it runs once at once.
// ModeFixedDelay starts after the initial offset, then waits at least Period
// from the end of each Job. Only fixed-rate mode preserves the grid offset.
//
// Config.Jitter draws an offset once in [0, Jitter); zero disables it.
// WithOffset supplies an explicit offset and OffsetFor derives one from an
// identity. Offsets spread starts but do not guarantee separation or ownership
// of work. The application owns queue claiming and idempotency.
//
// WithBackoff sets a minimum pause from Job completion, outside JobTimeout and
// Record.Duration. It cannot shorten the configured schedule. Non-positive
// values add no pause. Backoff callbacks and Handlers execute inline.
//
// # Jobs and records
//
// Job returns a processed count and an error. JobTimeout defaults to one minute
// and cancels the Job context; it cannot interrupt a function. Jobs must observe
// cancellation and join their own goroutines before returning.
//
// Handler receives a Record after each completed Job. Duration includes Job
// middleware, but not Handler or backoff. Outcome is authoritative even when Err
// is nil. Missed carries previously computed unintentional grid losses, excluding
// backoff; it is always zero in fixed-delay mode. Delivery waits for the next
// completed Job, so the final losses can go unreported. A single Record cannot
// identify their cause. Handler gets a context without cancellation or deadline;
// keep it short and bound any I/O separately. MultiHandler calls sinks in order.
//
// # Lifecycle
//
// MakeBeat builds a runner; Start calls OnStart and launches its loop. Start's
// context bounds startup only. A second Start fails; a stopped Beat cannot be
// restarted. Call Stop explicitly to stop scheduling and cancel the active Job,
// or use WithGracefulStop to drain within Stop's context. JobTimeout still applies.
//
// Stop calls OnStop only after startup succeeded and the loop ended. If waiting
// expires first, it returns ErrStillRunning with the context error and skips
// OnStop permanently. Done closes when startup/loop activity has ended, not when
// OnStop has completed. Use Done for fallback cleanup only after ErrStillRunning.
// Concurrent Stop callers share the first result unless their own wait expires.
//
// A failing OnStart is responsible for its partial cleanup; OnStop is not run.
// If OnStart succeeds but startup's context is cancelled, Start initiates cleanup
// with a fresh 15-second context unless concurrent Stop already owns cleanup.
// All hooks run synchronously and must respect their contexts.
//
// # Integration
//
// The beatfx package connects the runner to Fx. The recovery middleware catches
// panics in the wrapped Job call as PanicError; panics otherwise propagate.
// It does not recover Handler, hook, backoff, or child-goroutine panics.
// github.com/uchaloop/otelbeat implements Handler for OpenTelemetry metrics.
//
// beat supports frequent polling with reusable process state. It provides no
// calendar scheduling, persistence, replay after downtime, or delivery guarantee
// for business work. See the README for diagrams and usage examples.
package beat
