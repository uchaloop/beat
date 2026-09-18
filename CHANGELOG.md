# Changelog

## [Unreleased]

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

[Unreleased]: https://github.com/uchaloop/beat/compare/v0.3.2...HEAD
[0.4.0]: https://github.com/uchaloop/beat/compare/v0.3.2...v0.4.0
[0.3.2]: https://github.com/uchaloop/beat/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/uchaloop/beat/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/uchaloop/beat/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/uchaloop/beat/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/uchaloop/beat/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/uchaloop/beat/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/uchaloop/beat/releases/tag/v0.1.0
