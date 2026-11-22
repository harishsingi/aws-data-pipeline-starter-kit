# AWS Data Pipeline Starter Kit — Terraform module

Infrastructure-as-code for a production-grade AWS data pipeline: S3 data lake
(raw → curated → marts), Lambda ingest/validation, Glue ETL job, Athena
querying, and CloudWatch monitoring.

## Prerequisites

- **AWS CLI** installed and configured (`aws sts get-caller-identity` works)
- **Terraform >= 1.5** installed
- An AWS account with permissions to create IAM roles, S3 buckets, Lambda
  functions, Glue jobs, Athena workgroups, and CloudWatch resources

## Backend setup (remote state — recommended for teams)

This module ships with local state so you can deploy with zero setup. Before
production use:

1. Create an S3 bucket to hold state, e.g. `mycompany-terraform-state`
   (must already exist before `init`).
2. Uncomment the `backend "s3"` block in `terraform.tf` and fill in your
   **own** `bucket`, `key`, and `region` values (the file shows example values
   to follow — do not copy them verbatim).

## Deploy

```bash
cd terraform

terraform init        # downloads providers; add -reconfigure if you changed the backend

terraform plan        # review what will be created

terraform apply       # type "yes" to confirm
```

### Useful overrides

```bash
terraform apply -var="environment=prod" -var="glue_num_workers=4" -var="notification_email=you@example.com"
```

## Variables

| Name | Default | Description |
|------|---------|-------------|
| `project_name` | `data-pipeline-kit` | Prefix for all resource names (S3-safe, lowercase) |
| `environment` | `dev` | One of `dev`, `staging`, `prod` |
| `aws_region` | `us-east-1` | AWS region to deploy into |
| `glue_worker_type` | `G.1X` | Glue worker type (`G.1X`, `G.2X`, `G.4X`, `G.8X`) |
| `glue_num_workers` | `2` | Number of Glue workers |
| `lambda_runtime` | `python3.12` | Lambda runtime |
| `notification_email` | `""` | Email for alarm notifications; empty = no SNS topic created |

## Resources created

- **S3** — 4 buckets (`-raw`, `-curated`, `-marts`, `-artifacts`): versioning on,
  SSE-S3 encryption, public access fully blocked, lifecycle tiering
  (raw → STANDARD_IA 30d / GLACIER 90d; curated & marts → STANDARD_IA 60d),
  incomplete multipart uploads aborted after 7 days
- **Glue** — catalog database (`<project>_<env>`), ETL job (Glue 4.0, Python 3)
  with the script auto-uploaded from `../glue/etl_job.py`
- **Lambda** — ingest function triggered by `s3:ObjectCreated:*` on the raw
  bucket's `landing/` prefix; package auto-zipped from `../lambda/src`
- **IAM** — least-privilege roles for Glue and Lambda (no wildcard data access)
- **Athena** — workgroup with enforced configuration; results go to
  `s3://<artifacts>/athena-results/`, SSE-S3 encrypted
- **Monitoring** — CloudWatch log groups (30-day retention), Lambda
  Errors/Throttles alarms, optional SNS email alerts when
  `notification_email` is set

## Expected apply time

~10–20 minutes on a first apply. The bulk of that is the Glue job creation and
the Lambda packaging/notification wiring; subsequent applies with only script
changes take 1–2 minutes.

## Tear down

```bash
terraform destroy
```

`force_destroy = false` on the buckets means `destroy` **refuses to delete
buckets that still contain data** — this is deliberate so you can never wipe
your data lake by accident. To tear everything down, empty the buckets first
(`aws s3 rm s3://<bucket> --recursive`) and re-run `terraform destroy`.

## Troubleshooting

- **S3 notification / Lambda permission ordering** — the bucket notification
  (`aws_s3_bucket_notification`) explicitly `depends_on` the Lambda permission
  (`aws_lambda_permission.allow_s3`). If you modify the permission, let apply
  finish fully; a partially-applied notification can leave S3 firing at a
  function that rejects the call. Re-run `terraform apply` to reconcile.
- **Bucket name collisions** — S3 bucket names are globally unique. If apply
  fails with `BucketAlreadyOwnedByYou`/`BucketAlreadyExists`, someone (possibly
  another account) already owns `<project>-<env>-raw` etc. Change
  `project_name` (e.g. `data-pipeline-kit-acme`) and re-apply.
- **Glue job script not updating** — the `etag = filemd5(...)` on
  `aws_s3_object.glue_script` forces a new object whenever `../glue/etl_job.py`
  changes, and the job references the object, so `apply` picks it up
  automatically. No manual re-upload needed.
- **SNS email not arriving** — check your inbox (and spam) for the SNS
  subscription confirmation and click it; alarms only fire after confirmation.
