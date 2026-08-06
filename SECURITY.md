# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
vulnerability reporting for the repository so the report and any proposed patch
remain private until maintainers coordinate disclosure.

## Data-handling guarantees

- Dude relies on the standard AWS credential chain and does not accept raw AWS
  access keys as API parameters.
- Resume checkpoints contain paths, bucket names, object keys, upload IDs, file
  metadata, source ETags/version IDs, and completed-part metadata. Store them
  according to your data classification policy.
- Transfer callbacks and result objects report sizes and locations; callers are
  responsible for keeping those values out of unapproved logs.
- Checksums detect transfer corruption. They are not encryption or signatures.
- Server-side encryption must be selected through S3 policy or `create_args`.
- Set `ExpectedBucketOwner` in `create_args` (or use the matching CLI option) to
  protect against sending to an unexpected account after a naming/configuration error.
- Set `expected_source_bucket_owner` while composing to protect reads from an
  unexpected source account. Each copy is conditioned on the inspected ETag and,
  for versioned buckets, the inspected version ID.
- Dude does not delete composition sources. Cleanup is a separate, explicitly
  authorized operation after callers validate the resulting object.

Dude is alpha software and has not received an independent security audit.
