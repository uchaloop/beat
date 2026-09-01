// Package beat runs a single background Job on a schedule: on an interval
// ("@every 5s") or a cron expression ("*/5 * * * * *"), optionally after a start
// hook, bounded by a per-run timeout, wrapped by a chain of Middleware. Build a
// Beat with [MakeBeat] and drive it with Start/Stop, or use the beat/beatfx
// subpackage to wire it into an Uber Fx application.
//
// One Beat runs one Job. beat is single-instance per application: a process that
// needs two schedules is two processes, which is also how a deployment scales
// and stops them independently.
//
// # The job and its record
//
// A Job reports how much it processed and whether it failed:
//
//	type Job func(context.Context) (int, error)
//
// beat has no built-in metrics. After every run it hands a [Record] - iteration,
// start, duration, processed, error, scheduling mode - to a [Handler], the same
// way slog hands a Record to its handler. Metrics, logging and tracing are
// adapters the caller supplies; [MultiHandler] sends one record to several.
// github.com/uchaloop/otelbeat is such an adapter for OpenTelemetry.
//
// # Scheduling
//
// An "@every" interval measures the gap from the end of one run to the start of
// the next, so the effective period grows by the Job's duration. A cron spec
// fires at fixed wall-clock points and skips a point a long run overruns - it
// never queues catch-up runs.
//
// Config.Jitter delays the first interval run, or every cron tick, by a random
// amount drawn once from [0, Jitter). Replicas of one deployment read identical
// configuration, so a fixed offset could not stagger them; the draw is what
// does.
//
// Every run is bounded by Config.JobTimeout, one minute unless set, so a Job
// that ignores its context cannot silently wedge the loop.
//
// # Configuration
//
// Config is a plain struct with env tags that beat itself never reads. An
// application loads it - typically through github.com/uchaloop/confmaker, under
// the prefix it gives the instance - and supplies the filled value:
//
//	SPEC          the schedule; declared notEmpty, so a deployment that forgets
//	              it is told which variable is missing before anything is built
//	JOB_TIMEOUT   bounds one run; one minute from Config.SetDefaults
//	JITTER        the upper bound of the start delay; zero disables it
//
// A Config built in Go by hand never goes through SetDefaults, which is why
// MakeBeat treats a zero JobTimeout as the default too.
//
// # Options and middleware
//
// [WithMiddleware], [WithHandler], [WithOnStart], [WithOnStop] and
// [WithGracefulStop] configure what a Config cannot carry. Middleware wraps the
// Job:
//
//	type Middleware func(next Job) Job
//
// In WithMiddleware(a, b, c) the first is outermost, so it is the order the
// wrapping reads in. Three are provided: beat/middleware/recovery,
// beat/middleware/idle and beat/middleware/batch.
//
// Shutdown cancels the running job by default. WithGracefulStop lets it finish
// instead, within whatever stop timeout the application allows.
//
// # Panics
//
// The core does not recover them. A panic in the Job, or in a Handler,
// propagates and crashes the process with a full stack on stderr - the honest
// default for user code that misbehaves, and one that a supervisor restarts. To
// keep the scheduler alive instead, add beat/middleware/recovery, which reports
// the panic as a [PanicError] on the Record.
package beat
