# Changelog

All notable changes to this project will be documented in this file.

## v0.1.0 - 2026-10-01

### Changed

- SDK extracted from the `genesis-sandbox` monorepo (`sdks/typescript`) into
  this standalone npm-managed repository. Public API surface is unchanged.
- Distribution renamed from the private monorepo package `@genesis-ai/sandbox`
  to `genesis-sandbox-client-typescript`, matching the Go/Python/Java client
  distribution names.
- Published artifacts are now compiled by `tsc` to `dist/` (ESM + `.d.ts` +
  sourcemaps) instead of exporting raw `.ts` sources, so plain JavaScript
  consumers can use the package without a TypeScript toolchain.
- Supported runtime baseline declared as Node.js `>=22` (stable global `fetch`,
  `AbortSignal.any`); CI verifies Node 22 and 24.

### Added

- npm scripts (`build`, `typecheck`, `test`, `lint`, `prepack`), ESLint
  (flat config with typescript-eslint), and a GitHub Actions CI workflow.
- `examples/` with quickstart and session-restore walkthroughs.
- `CHANGELOG.md`, Apache-2.0 `LICENSE`, and repository metadata.
- `EffectiveEnvironment` is now defined (matching `api/openapi.yaml`) and
  exported; it was referenced by `ExecSessionResult` but missing from the
  source, which had never passed `tsc`.

### Fixed

- `dispose()` now treats HTTP 404 as already deleted for both session and
  workspace cleanup, matching the Go/Python/Java client convention for
  idempotent DELETEs. Previously a session reclaimed by the server TTL would
  make `dispose()` throw inside `finally` and mask the run's primary outcome;
  non-404 failures still propagate.
