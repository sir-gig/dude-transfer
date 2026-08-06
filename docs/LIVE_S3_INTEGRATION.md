# Live S3 integration setup

The `Live S3 contract` workflow is manually dispatched and writes only synthetic
bytes beneath random children of the selected prefix. It verifies server-side
composition and a three-part multipart file upload byte for byte, proves that an
atomic create-only collision cannot replace the uploaded object, and requests
deletion only for each test's exact object keys. Use a disposable bucket with a
lifecycle policy; never target production data.

The workflow accepts no AWS access keys. It exchanges GitHub's short-lived OIDC
token for an IAM role session and applies an additional inline session policy
restricted to the submitted bucket and prefix. See the official
[`configure-aws-credentials` OIDC guidance](https://github.com/aws-actions/configure-aws-credentials#oidc)
and [Amazon S3 API permission mapping](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html).

## 1. Create the GitHub OIDC trust

Configure the AWS account's GitHub OIDC provider for
`https://token.actions.githubusercontent.com` with audience `sts.amazonaws.com`.
Create a role whose trust policy pins the repository and GitHub environment:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub": "repo:sir-gig/dude-transfer:environment:live-s3-integration"
        }
      }
    }
  ]
}
```

Replace the account ID. The role's identity policy must allow the S3 operations
listed by `scripts/live_integration_policy.py` on the disposable bucket. The
workflow further narrows each session to the exact submitted prefix and verifies
that the assumed-role account matches the expected bucket-owner input.

## 2. Configure the protected environment

In repository settings, create an environment named `live-s3-integration`:

1. Allow deployments from protected branches only.
2. Add a required reviewer when another maintainer is available.
3. Add the environment variable `DUDE_AWS_ROLE_ARN` containing the role ARN.

An IAM role ARN is an identifier rather than a credential; do not add long-lived
AWS access keys as repository or environment secrets.

## 3. Run the contract

Open **Actions > Live S3 contract > Run workflow** on `main`. Supply the bucket,
`dude-integration` prefix (or a child of it), AWS region, and twelve-digit
bucket-owner account ID, then confirm that the scope contains no production data.
The validator runs before OIDC role assumption and refuses arbitrary roots,
malformed values, cross-account roles, or traversal-like inputs.

Use a disposable bucket with SSE-S3 encryption for this initial contract. A
bucket that mandates a customer-managed KMS key also requires an explicitly
reviewed KMS session-policy statement, which this least-privilege workflow does
not generate.

Versioned buckets can retain previous object versions or delete markers after the
test's exact-key deletion requests. Configure bucket lifecycle rules for those
versions and for abandoned multipart uploads.
