# Changelog

## [Unreleased]

## [0.6.0] - 2026-09-20

- Document and verify job.ErrorHandler integration: scheduling and backoff use
  total attempt duration; cluster rotation counts only locally owned missed
  points, and shutdown waits for error processing before cleanup.

- **Breaking:** the Fx adapter now lives in `github.com/uchaloop/beatfx`.
  Use that independent module; this repository contains only the core library.
- Updated documentation and CI for independent core and adapter releases.

## [0.5.0] - 2026-09-20

Execution moved out to github.com/uchaloop/job, so one attempt means the same
whether a scheduler or a one-shot process runs it. beat keeps the schedule.

### Breaking changes

- `MakeBeat` takes a `*job.Runner` instead of a `Job` and a `Handler`. The work,
  its middleware and its timeout are configured on the runner; supply a Handler
  with `WithHandler`.
- `Config.JobTimeout` is gone, and with it `BEAT_JOB_TIMEOUT`. The bound belongs
  to `job.Config` and is read as `JOB_TIMEOUT`.
- `Job`, `Middleware`, `WithMiddleware`, `Outcome`, `PanicError` and
  `beat/middleware/recovery` moved to job and `job/middleware/recovery`.
- `Record` carries the attempt under `Result`: `record.Result.Duration`,
  `record.Result.Outcome`, `record.Result.Processed`, `record.Result.Err`.

### Added

- `WithAssignment` and `WithDecisionHandler`: an optional cluster rotation from
  `job/assignment` picks one cluster to own each grid point, and all of its
  replicas run it. Off by default, requires `ModeFixedRate` and the same Period.
  A point owned elsewhere produces no run, no Record, no backoff and no missed
  point, so `WithDecisionHandler` is the only way to observe those decisions.
- `Record.GridPoint`, the shared grid point before the per-replica offset. It
  is what every replica and cluster agrees on, and therefore what the rotation
  decides over - an offset staggers replicas, it must not move a point to
  another cluster.
- `Record.LocalPeriod`, the spacing of the points this process is responsible
  for: `N * Period` under a rotation, `Period` without one. A handler reporting
  how much of its schedule the work uses divides by this, not by `Period`, or it
  overstates the load by the number of clusters.

### Fixed

- A backoff asked for after an attempt that never ran is no longer dropped. The
  runner reports a zero Start when the caller's context was already done, and
  adding a duration to that yielded a timestamp from year one, which no backoff
  could ever push past. Only a shutdown reaches it today, but the schedule is no
  longer handed a meaningless point.
- A graceful stop no longer lets a new attempt begin. Scheduling was checked
  before the wait but not again after the decision handler, which is application
  code and may take as long as it likes, so a Stop landing inside it was followed
  by a fresh attempt - the opposite of what a graceful stop promises.

  Whether an attempt may begin is now decided under the mutex Stop moves the
  lifecycle state with, so an attempt is either accepted before the stop - and a
  graceful stop then lets it finish - or refused after it. A context check could
  not promise that: a Stop landing between the check and the call would still
  have been followed by a fresh attempt. The mutex is not held for the work.

### Changed

- `MakeBeat` rejects a `WithDecisionHandler` given without `WithAssignment`.
  Without a rotation there are no decisions, so the handler could never fire;
  that is a configuration mistake rather than a quiet no-op.
- `beatfx` asks the application to stop when the loop ends without being asked
  to, with `fx.ExitCode(1)`. A scheduler whose loop has left has nothing further
  to do, and a daemon that looks healthy while its queue goes unserved is worse
  than one that exits. It is a request: `app.Run()` answers it, while an
  application driving the lifecycle by hand must wait on `app.Wait()` and call
  `Stop`. The stop runs the OnStop hook, so the loop's error still reaches the
  application through `Stop` the ordinary way.
- A loop that cannot carry on - a rotation that cannot decide a point - ends
  scheduling, closes `Done` and leaves its error for `Stop` to report. `Done`
  therefore closes for two reasons now: a Stop you called, and a loop that
  stopped by itself.

## [0.4.0] - 2026-09-18

### Breaking changes

- `Config.Period` replaces `Spec`; scheduling uses `ModeFixedRate` (default) or
  `ModeFixedDelay`. Calendar expressions and the cron dependency are removed.
- `Record` includes `ScheduledFor`, `Period`, `Outcome` and `Missed`.
- `WithBackoff` replaces the idle/batch middleware; exported `Wait` is removed.
- `beatfx.Options` replaces `AsOption` with one ordered container-provided slice.

### Added

- Absolute fixed-rate grid, fixed-delay scheduling, `WithOffset` and `OffsetFor`.
  Jitter accepts the full period; intentional backoff is excluded from Missed.
- Single-start lifecycle, repeatable Stop, lifecycle errors and `Done()`.
- Current API examples, execution diagram and documented shutdown contracts.

### Fixed

- Offsets no longer reduce the time available before the next fixed-rate target.
- Concurrent startup/shutdown and repeated Stop preserve one cleanup owner.
  A successful startup cancelled before launch triggers bounded cooperative cleanup.
- Timeout and shutdown outcomes are reported even when Job returns a nil error.
- Histogram consumers can distinguish Job duration from Handler/backoff time.

## [0.3.2] - 2026-09-17

- Added `ConfigName()` with default instance name `beat`.
- Updated confmaker examples.

## [0.3.1] - 2026-09-02

- Added README logo.

## [0.3.0] - 2026-09-01

- Required Go 1.27 and adopted `validate` for accumulated config errors.
- Expanded package documentation.

## [0.2.0] - 2026-08-25

- Added `Config.SetDefaults` and accumulated validation errors.
- Required an explicit schedule and removed koanf tags.

## [0.1.2] - 2026-08-07

- Allowed schedule configuration without a SPEC environment override.

## [0.1.1] - 2026-08-06

- Updated README.

## [0.1.0] - 2026-08-06

- Initial release: interval/cron scheduling, Job middleware, Handler records,
  standalone and Fx lifecycles, cooperative timeouts and graceful stop.
- Added recovery, idle and batch middleware, plus MultiHandler.

[Unreleased]: https://github.com/uchaloop/beat/compare/v0.6.0...HEAD
[0.5.0]: https://github.com/uchaloop/beat/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/uchaloop/beat/compare/v0.3.2...v0.4.0
[0.3.2]: https://github.com/uchaloop/beat/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/uchaloop/beat/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/uchaloop/beat/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/uchaloop/beat/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/uchaloop/beat/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/uchaloop/beat/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/uchaloop/beat/releases/tag/v0.1.0

[0.6.0]: https://github.com/uchaloop/beat/compare/v0.5.0...v0.6.0
