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
- A high-level `send_dataframe_as_single_object` path that stages uncompressed
  JSON Lines/text from Spark executors and composes one verified S3 object.
- Run-scoped staging discovery and optional post-verification cleanup, with
  failure-safe retention and cleanup status in result metadata.
- Atomic create-only and compare-and-swap destination commits using S3
  `If-None-Match`/`If-Match` on `PutObject` and `CompleteMultipartUpload`.
- Ambiguous-response recovery for single-request `PutObject` uploads.
- Restart recovery when S3 committed an object before the process could remove
  its multipart checkpoint.
- Full-commit pinning for GitHub Actions and Sigstore-backed build provenance for
  tagged release artifacts.
- Measured-size skew repair for the DataFrame single-object path, using bounded
  Spark reshuffles without downloading payload bytes to the driver.
- Per-attempt staging manifests, skew decisions, structured events, and aggregate
  cleanup results for repaired exports.

### Changed

- Resume now validates remote part sizes and checksum metadata before reuse.
- Checkpoints use restrictive permissions where supported and fail closed when corrupt.
- Non-retryable S3 authorization and validation failures fail immediately.
- S3 URIs containing query strings or fragments are rejected instead of being
  interpreted ambiguously.
- Requester-pays settings propagate across multipart requests, and unsupported
  SSE-C settings fail before an upload begins.
- `maxRecordsPerFile` is rejected for single-object DataFrame writes because it
  can recreate illegal undersized chunks after partition repair.

### Fixed

- Detect and abort when a source file changes during a multipart upload.
- Prevent upload creation options from overriding Dude-managed S3 request fields.
