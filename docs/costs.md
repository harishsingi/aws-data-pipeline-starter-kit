# Costs

Every figure below is an **estimate — check current AWS pricing** before budgeting. Actuals depend on region, data volume, run frequency, and how aggressively you use the cost-saving tips at the end.

## Service-by-service

| Service | How it's priced | Estimate for this kit |
|---|---|---|
| S3 (raw, curated, marts, artifacts buckets) | Storage (~$0.023/GB-mo standard, less with lifecycle tiers) + requests | Dev: under $1/mo for sample data. Small prod (say 500 GB): ~$12/mo storage |
| Glue ETL (G.1X workers, ≈ $0.44/DPU-hr) | Per DPU-hour; 1 G.1X worker = 1 DPU | 2 workers × 10 min/day ≈ 10 DPU-hr/mo ≈ **$4.50/mo**. 2 workers × 1 hr/day ≈ $26/mo |
| Lambda (ingest) | Requests + GB-seconds; free tier covers 1M requests and 400k GB-seconds/mo | Free tier covers this workload; essentially $0 at dev and small-prod scale |
| Athena | $5/TB of data scanned | Depends entirely on query patterns; with partition pruning, ad-hoc dev usage is usually <$5/mo |
| CloudWatch logs/metrics | Log ingestion/storage + custom metrics | Dev: <$1–2/mo. Alarms and dashboards add a little |
| Glue Data Catalog | First 1M objects free | $0 for this kit |
| Secrets / KMS / IAM | Nominal or free at this scale | $0–1/mo |

Worked example — the default Glue job (G.1X, 2 workers, runs ~10 min once a day):
2 DPU × (10/60 hr) × 30 days = 10 DPU-hr/mo × $0.44/DPU-hr ≈ **$4.50/mo**. This is the kit's default shape; longer runs or more workers scale it linearly.

## Scenarios

### Dev / idle — ~$5–15/mo (estimate)

- Sample data generation and one Glue run per day (or less).
- S3: a few GB, <$1.
- Glue: 2 workers × 10 min/day ≈ $4.50/mo.
- Lambda: free tier, $0.
- Athena: occasional queries with partition pruning, <$5.
- CloudWatch: <$2.

If you run the pipeline only when developing (not on a daily schedule), it drops toward the low end. Deleting the stack with `make tf-destroy` between dev sessions takes it near $0 (storage only).

### Small production — ~$50–150/mo (estimate)

Assumptions: daily runs of ~30–60 min, a few hundred GB in S3, several analysts querying Athena, dbt running daily, modest CloudWatch alarms/dashboards.

- Glue: 2 workers × 1 hr/day ≈ $26/mo.
- S3: ~500 GB ≈ $12/mo (less with lifecycle to Infrequent Access).
- Athena: $5/TB scanned; a few TB/mo of scanned data with pruning ≈ $10–25/mo.
- CloudWatch, catalog, misc: ~$5–10/mo.

Scale beyond this mostly scales Glue run time and S3 storage; Athena is the wild card and is controlled by query discipline (see tips).

## Cost-saving tips

1. **S3 lifecycle policies.** Move bronze/silver data to Standard-IA after ~30 days and to Glacier for long-term retention; keep gold and marts in Standard longer since they're queried.
2. **Athena workgroup limits.** The `data-pipeline-kit-dev-wg` workgroup should have a per-query byte-scanned cap and an alert — one unpartitioned `SELECT *` over bronze shouldn't be able to burn the monthly budget.
3. **Partition pruning.** Always filter on `dt` in Athena queries and Spark reads. This is the single biggest Athena cost lever: scanning one daily partition instead of a year of history cuts scanned bytes ~365×.
4. **Right-size the Glue job.** G.1X with 2 workers (the default) handles sample and modest real volumes. Only increase `glue_num_workers` after a run actually runs out of capacity — don't preemptively size for data you don't have.
5. **Run less often.** If daily freshness isn't needed, schedule the Glue job and dbt build every N days or on-demand via `make glue-run`.
6. **Clean up dev stacks.** `make tf-destroy` when a dev environment is idle; `make tf-apply` recreates it in ~10–20 min when needed. Storage buckets are the only cost that persists, and you can empty them first.
