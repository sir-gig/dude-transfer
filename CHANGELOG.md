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
- Ordered S3-to-S3 composition using concurrent multipart copy with zero payload
  bytes downloaded by the client.
- Source ETag/version pinning, resumable copy-part reconciliation, and ambiguous
  completion recovery for server-side composition.
- `plan-compose` and `compose-s3` CLI commands.

### Changed

- Resume now validates remote part sizes and checksum metadata before reuse.
- Checkpoints use restrictive permissions where supported and fail closed when corrupt.
- Non-retryable S3 authorization and validation failures fail immediately.
- S3 URIs containing query strings or fragments are rejected instead of being
  interpreted ambiguously.
- Requester-pays settings propagate across multipart requests, and unsupported
  SSE-C settings fail before an upload begins.

### Fixed

- Detect and abort when a source file changes during a multipart upload.
- Prevent upload creation options from overriding Dude-managed S3 request fields.
