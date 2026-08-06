# Security policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Until a dedicated
security contact is published, retain the report and contact the repository
maintainer privately through the hosting platform.

A public repository must configure a private vulnerability-reporting channel
before its first announced release.

## Data-handling guarantees

- Dude relies on the standard AWS credential chain and does not accept raw AWS
  access keys as API parameters.
- Resume checkpoints contain paths, bucket names, object keys, upload IDs, file
  metadata, and completed-part metadata. Store them according to your data
  classification policy.
- Transfer callbacks and result objects report sizes and locations; callers are
  responsible for keeping those values out of unapproved logs.
- Checksums detect transfer corruption. They are not encryption or signatures.
- Server-side encryption must be selected through S3 policy or `create_args`.

Dude is alpha software and has not received an independent security audit.
