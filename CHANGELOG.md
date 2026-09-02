# Changelog

All notable changes to this module are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-09-02

### Added

- A logo in the README.

## [0.3.0] - 2026-09-01

### Changed

- The package documentation carries the scheduling modes, the config, the
  options and the record; the README is a landing page. beatfx and the three
  middleware document themselves, so a subpackage opened on its own says what it
  is and when to reach for it.
- The package comment moved from `beat.go` into `doc.go`, in line comments: a
  cron spec contains `*/`, which ends a block comment.
- `Config.Validate` accumulates through `github.com/uchaloop/validate` instead of
  a hand-rolled slice and `errors.Join`. The messages and their order are
  unchanged, and `errors` and `fmt` are no longer imported here.
- The module is built with Go 1.27, which the new dependency requires. A module
  that depends on this one has to declare 1.27 as well.

## [0.2.0] - 2026-08-25

### Added

- `Config.SetDefaults` establishes `JobTimeout`, so a loader starts from `1m`
  and a generated `.env.example` carries the real default rather than a blank.
  A `Config` built in Go by hand still gets the same value from `MakeBeat`,
  which keeps treating a zero timeout as the default; both apply one constant.

### Changed

- `Spec` declares `notEmpty`, so a deployment that forgets it is told which
  variable is missing before anything is built. `Validate` still reports an
  empty `Spec` as well, for a `Config` assembled in Go that never goes near a
  loader.
- `Validate` reports every problem at once instead of the first.

### Removed

- The `koanf` struct tags. Configuration is read from the environment only.

## [0.1.2] - 2026-08-07

### Fixed

- Made the `SPEC` environment override optional. `Config.Spec` can now be
  supplied by a TOML file through `confmaker/confx` without also requiring the
  prefixed environment variable; `Config.Validate` still rejects an empty
  effective value.

## [0.1.1] - 2026-08-06

### Changed

- Reworked the README as concise, user-focused documentation.

## [0.1.0] - 2026-08-06

### Added

- Initial release: `beat` runs one background `Job` on a schedule inside an Uber Fx application.
- Core contracts `Job func(ctx) (int, error)` and `Middleware func(next Job) Job` (classic composition).
- `Handler`/`HandlerFunc` observability seam receiving a `Record` after every run, mirroring `slog`; no built-in metrics.
- `Record.Mode` (`interval`/`cron`) so handlers can label the scheduling mode (used by the companion metrics module).
- `Config` with `koanf` and `env` tags (`Spec`, `JobTimeout`, `Jitter`), loadable through `confmaker/confx`; the core reads neither files nor the environment.
- Standalone lifecycle `MakeBeat` + `(*Beat).Start`/`Stop`; the core package has no Fx dependency.
- Fx integration in `beat/beatfx`: `beatfx.Module` (one entry point consuming `Config`, `Job`, and an optional `Handler`) and `beatfx.AsOption` (register DI-built options through the `beat_options` value group, mixable with the static options passed to `Module`).
- Options `WithMiddleware`, `WithHandler`, `WithOnStart`, `WithOnStop`.
- `WithGracefulStop` to let the in-flight run finish on shutdown instead of cancelling its context.
- Middleware packages `middleware/{recovery,idle,batch}`.
- Exported `Wait` helper for cancellable delays in custom middleware.
- `MultiHandler` to fan a `Record` out to several handlers (e.g. metrics + logging) behind the single `Handler` slot.
- Panics are not recovered by the core: a panic in the Job (or Handler) crashes the process with a stack on stderr. Add `middleware/recovery` to keep the loop alive.
- `middleware/recovery` recovers a Job panic into a `*PanicError{Value, Stack}` on the `Record` (detectable via `errors.As`) and, with `WithLogger`, logs it with its stack.
- `JobTimeout` defaults to 1m and every execution is bounded, so a Job that ignores its context cannot silently wedge the loop.
- Lifecycle hooks rely on the application's `fx.StartTimeout` / `fx.StopTimeout`.
- Interval (`@every`) and cron scheduling, with a cryptographically random start delay bounded by `Jitter`.

[Unreleased]: https://github.com/uchaloop/beat/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/uchaloop/beat/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/uchaloop/beat/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/uchaloop/beat/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/uchaloop/beat/releases/tag/v0.1.0
