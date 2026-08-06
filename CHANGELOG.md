# Changelog

All notable changes to this module are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/uchaloop/beat/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/uchaloop/beat/releases/tag/v0.1.0
