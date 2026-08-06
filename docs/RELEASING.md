# Release process

Dude publishes immutable Python versions only from a `v*` tag whose commit is
already contained in protected `main`. The release workflow builds without OIDC
permissions, hands the exact distributions to isolated attestation and publishing
jobs, publishes through PyPI Trusted Publishing, and finally attaches the same
files to a GitHub release.

## One-time PyPI setup

Sign in to PyPI and create a pending GitHub Trusted Publisher with these exact
values:

- PyPI project name: `dude-transfer`
- GitHub owner: `sir-gig`
- Repository: `dude-transfer`
- Workflow: `release.yml`
- Environment: `pypi`

A pending publisher does not reserve the project name. Configure it only from
the official [PyPI pending-publisher page](https://pypi.org/manage/account/publishing/)
and publish the first release promptly. Do not create a PyPI API token or add a
`PYPI_TOKEN` GitHub secret.

The GitHub `pypi` environment must allow only `v*` tags. Add a required reviewer
when another maintainer is available. PyPI verifies the repository, workflow,
environment, and repository-owner identity claims before issuing a short-lived
upload token.

## Release gates

Do not push the first version tag until all of these are attached to the release
decision:

1. A successful protected `Live S3 contract` run against a disposable SSE-S3
   bucket.
2. Complete raw JSON from the Databricks DataFrame, single-object, composition,
   and skew-repair benchmark harnesses, including cluster/runtime and region.
3. A specific private conduct-reporting contact in `CODE_OF_CONDUCT.md`.
4. A reviewed changelog and version update merged through protected `main`.
5. Zero open CodeQL, Dependabot, and secret-scanning alerts.
6. The pending PyPI publisher values above verified character for character.

An independent security review remains required before changing the package from
alpha to a stable-development classifier.

## Publish

After the gates pass, create the version tag from synchronized `main` and push
only that tag:

```bash
git switch main
git pull --ff-only origin main
git tag -s v0.1.0 -m "Dude 0.1.0"
git push origin v0.1.0
```

Use a signed tag when the maintainer's signing identity is configured. The
workflow independently checks that `v0.1.0` matches `pyproject.toml` and that the
tagged commit is an ancestor of `origin/main`.

## Verify

The release is incomplete unless both registries contain the same version and
the attestations verify:

```bash
python -m pip install --no-cache-dir "dude-transfer==0.1.0"
gh release view v0.1.0 --repo sir-gig/dude-transfer
gh attestation verify dude_transfer-0.1.0-py3-none-any.whl \
  --repo sir-gig/dude-transfer
```

PyPI Trusted Publishing also uploads PEP 740 attestations for each distribution
by default. Never replace or silently rebuild a published version; increment the
version and release again.
