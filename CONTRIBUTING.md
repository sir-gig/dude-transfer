# Contributing to Dude

Thanks for helping make bulk data transfers less mysterious.

## Development setup

```bash
python -m venv .venv
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
```

Python 3.10 or newer is required. Spark and live AWS credentials are not needed
for the unit test suite.

An opt-in live contract test writes 7 MiB of synthetic data under a random S3
prefix, verifies server-side composition byte for byte, and issues cleanup only
for its three exact object keys:

```bash
DUDE_INTEGRATION_BUCKET=my-disposable-test-bucket python -m pytest -m integration
```

Set `DUDE_INTEGRATION_PREFIX`, `DUDE_EXPECTED_BUCKET_OWNER`, or
`DUDE_INTEGRATION_KMS_KEY` when the disposable environment requires them. Never
run this test against a production-data prefix.
Versioned buckets may retain prior object versions or delete markers according
to their own lifecycle policy.

## Pull requests

1. Open an issue before beginning a large behavioral or API change.
2. Add tests for changed behavior, including interruption and retry paths.
3. Run the test and lint commands above.
4. Never include patient, laboratory, customer, credential, bucket, account, or
   production workload data in code, fixtures, logs, issues, or benchmarks.
5. Describe performance claims with the dataset shape, format, cluster, regions,
   encryption settings, and baseline command needed to reproduce them.

Contributions are licensed under Apache-2.0 when submitted.
