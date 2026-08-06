# Changelog

All notable changes will be documented here.

## [Unreleased]

### Added

- DataFrame diagnostics and metadata-driven partition planning.
- Direct distributed writes for Spark-compatible DataFrames.
- Bounded-memory concurrent S3 multipart uploads.
- Durable upload checkpoints and reconciliation with remote S3 parts.
- CRC32 and SHA-256 part validation.
- Retry behavior, failure cleanup, progress callbacks, and CLI commands.
- Adaptive concurrency with structured tuning telemetry.
- Transfer identities for recovering ambiguous multipart completion responses.
- Optional post-upload verification and expected-bucket-owner enforcement.

### Changed

- Resume now validates remote part sizes and checksum metadata before reuse.
- Checkpoints use restrictive permissions where supported and fail closed when corrupt.
- Non-retryable S3 authorization and validation failures fail immediately.

### Fixed

- Detect and abort when a source file changes during a multipart upload.
- Prevent upload creation options from overriding Dude-managed S3 request fields.
