// Package beat schedules one job.Runner in a long-lived process. Attempts are
// sequential and never overlap. beat suits frequent polling with reusable
// process state; rare or calendar work belongs in a Kubernetes CronJob.
//
// The work, its middleware and its timeout live in the Runner, so the same
// attempt can also be driven by a one-shot process. beat adds only the
// schedule.
//
// # Scheduling
//
// ModeFixedRate, the default, places runs on an absolute grid of Period since
// the Unix epoch, shifted by an offset. Cadence does not depend on run
// duration, and a run that outlives its period reports the points it covered as
// Record.Missed rather than queueing them. ModeFixedDelay measures Period from
// the end of each run; only fixed rate preserves a shared grid.
//
// The grid is anchored in UTC, so there is no time zone and no daylight saving.
// A point already past is served at once - one late run, no catch-up queue.
//
// Config.Jitter draws an offset in [0, Jitter) once per process; WithOffset
// sets one outright. It may reach Period. Because the grid is absolute, the
// offset is the only thing separating replicas, and it holds for the life of
// the process. It spreads load and guards nothing: queue claiming and
// idempotency belong to the application.
//
// WithBackoff sets a minimum pause from the end of the work, outside the job
// timeout and outside Result.Duration. It cannot shorten the configured
// schedule, and the points it holds back are not counted as losses.
//
// # Running in several clusters
//
// WithAssignment adds an optional job/assignment rotation: one cluster owns
// each grid point and all of its replicas run it, while the others skip. It is
// off by default, needs ModeFixedRate, and must share Config.Period. A point
// another cluster owns produces no run, no Record and no backoff, and is not a
// Missed point; WithDecisionHandler observes those decisions. With N clusters
// Record.LocalPeriod is N*Period - the spacing a handler should measure a run
// against. An owner that is down simply leaves its points unserved.
//
// # Records
//
// Handler receives a Record after each completed attempt. Record.Result is what
// the work reported - see job.Result - and everything beside it describes the
// schedule. Result.Outcome is authoritative even when Result.Err is nil.
//
// Missed carries previously computed losses of this process's own points,
// excluding intentional backoff and points owned elsewhere. It travels with the
// next completed attempt, so the final losses can go unreported. A single
// Record cannot say why a point was lost; successive records can.
//
// # Lifecycle
//
// MakeBeat builds a scheduler; Start calls OnStart and launches its loop.
// Start's context bounds startup only. A second Start fails; a stopped Beat
// cannot be restarted. Call Stop explicitly to stop scheduling and cancel the
// active attempt, or use WithGracefulStop to drain within Stop's context.
//
// Whether a new attempt may begin is decided under the same mutex Stop moves
// the lifecycle state with, so a graceful stop lets an attempt already accepted
// finish and refuses every later one.
//
// Stop calls OnStop only after startup succeeded and the loop ended. If waiting
// expires first, it returns ErrStillRunning with the context error and skips
// OnStop permanently. Done closes when loop activity has ended, not when OnStop
// has completed; it also closes when a loop ends by itself, and Stop then
// reports why. Concurrent Stop callers share the first result unless their own
// wait expires.
//
// A failing OnStart is responsible for its partial cleanup; OnStop is not run.
// If OnStart succeeds but startup's context is cancelled, Start initiates
// cleanup with a fresh 15-second context unless a concurrent Stop already owns
// it. All hooks run synchronously and must respect their contexts.
//
// # Integration
//
// The beatfx package connects the scheduler to Fx. job/middleware/recovery
// catches panics in the wrapped work as job.PanicError; panics elsewhere -
// Handler, hooks, backoff, child goroutines - propagate.
//
// beat provides no calendar scheduling, persistence, replay after downtime, or
// delivery guarantee for business work. See the README for usage examples.
package beat
