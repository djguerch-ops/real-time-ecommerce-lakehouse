# Real-Time E-commerce Lakehouse

A production-style lakehouse pipeline built on **AWS S3 + Databricks + Unity Catalog**, simulating an e-commerce event stream and turning raw JSON events into business-ready KPIs.

> **Status:** V1 complete ✅ — batch ingestion with Auto Loader, medallion architecture (Bronze/Silver/Gold), and a live SQL dashboard. **V2 in progress 🚧** — real-time ingestion (Kinesis + Lambda), CI/CD (GitHub Actions + Terraform), dual orchestration (Databricks Job + Delta Live Tables), and observability (alerting + freshness monitoring) implemented; advanced governance planned.

## 📑 Contents

| | Section | Status |
|---|---|---|
| 🟢 | [V1 — Batch Lakehouse](#-v1--batch-lakehouse) | ✅ Complete |
| 🔵 | [V2 — Real-time Ingestion](#-v2--real-time-ingestion-kinesis--lambda) | ✅ Complete |
| 🟣 | [V2 — CI/CD](#-v2--cicd-github-actions--terraform) | ✅ Complete |
| 🟠 | [V2 — Orchestration: Job vs DLT](#-v2--orchestration-databricks-job-vs-delta-live-tables) | ✅ Complete |
| 🔴 | [V2 — Observability](#-v2--observability-alerting--data-freshness-monitoring) | ✅ Complete |
| 🔵 | [V2 — Planned Enhancements](#v2--planned-enhancements) | ⬜ Planned |

---

## 🟢 V1 — Batch Lakehouse

Everything below — architecture, data model, pipeline, dashboard — describes the **V1 batch
pipeline**: a Python script drops a file in S3, Databricks Auto Loader picks it up on demand.
This is the complete, working baseline. V2 (real-time ingestion) is documented further down
and builds on top of it without modifying it.

## Architecture

```
Python event generator → AWS S3 (raw) → Databricks Auto Loader
        → Bronze (Delta) → Silver (Delta) → Gold (Delta, KPI tables)
        → Databricks SQL Dashboard
```

| Layer | Purpose | Technology |
|---|---|---|
| **Source** | Simulated e-commerce events (`page_view`, `add_to_cart`, `order_created`) | Python |
| **Storage** | Raw event landing zone | AWS S3 |
| **Ingestion** | Schema-enforced, incremental file ingestion | Databricks Auto Loader |
| **Bronze** | Raw events, as-is | Delta Lake |
| **Silver** | Cleaned, deduplicated, typed | Delta Lake |
| **Gold** | Business KPIs | Delta Lake |
| **Serving** | Visual reporting | Databricks SQL Dashboard |
| **Infra** | Reproducible cloud resources | Terraform |
| **Security** | AWS ↔ Databricks auth | Unity Catalog + IAM Role (no static keys) |

---

## Why this project

This project was built to demonstrate practical, production-oriented Data Engineering skills beyond simple ETL scripting:

- Designing a **medallion architecture** (Bronze → Silver → Gold) with clear separation of concerns
- Using **Databricks Auto Loader** for schema-enforced, incremental ingestion from cloud storage
- Connecting AWS and Databricks the **secure way** — via Unity Catalog Storage Credentials and an IAM Role assumed through STS, with zero static credentials in code or cluster config
- Provisioning cloud infrastructure as code with **Terraform**
- Turning raw events into **business KPIs** that a real e-commerce team would track

---

## Data model

Each event follows a JSON schema with 3 event types:

```json
{
  "event_id": "uuid",
  "event_type": "page_view | add_to_cart | order_created",
  "event_ts": "ISO 8601 timestamp",
  "user_id": "string",
  "session_id": "string",
  "product_id": "string | null",
  "order_id": "string | null",
  "device_type": "mobile | desktop | tablet",
  "country": "FR | DE | ES | IT",
  "amount": "number | null",
  "currency": "EUR"
}
```

Full schema: [`src/event_generator/ecommerce_event.json`](src/event_generator/ecommerce_event.json)

---

## Project structure

**V1 (batch) — complete:**
```
real-time-ecommerce-lakehouse/
├── data/sample/events.json
├── src/
│   ├── event_generator/
│   │   ├── producer.py
│   │   └── ecommerce_event.json
│   └── databricks/
│       ├── bronze/01_bronze_ingestion.py
│       ├── silver/02_silver_transform.py
│       └── gold/03_gold_kpi.py
├── terraform/envs/dev/
│   ├── main.tf
│   ├── variables.tf
│   └── terraform.tfvars.example
└── README.md
```

**V2 (streaming) — additive, on top of the structure above:**
```
real-time-ecommerce-lakehouse/
├── src/
│   ├── event_generator/
│   │   └── producer_streaming.py            # NEW — streams to Kinesis
│   ├── lambda/
│   │   └── kinesis_to_s3/handler.py         # NEW — Kinesis consumer
│   └── databricks/
│       └── bronze/04_bronze_streaming_ingestion.py  # NEW
└── terraform/envs/dev/
    ├── streaming.tf                          # NEW — Kinesis + Lambda + IAM
    └── streaming_variables.tf                # NEW
```

**V2 (CI/CD) — additive, on top of the structure above:**
```
real-time-ecommerce-lakehouse/
├── .github/
│   └── workflows/
│       └── terraform.yml                     # NEW — plan + apply on every push
└── terraform/envs/dev/
    ├── backend.tf                            # NEW — remote S3 state backend
    └── cicd_oidc.tf                          # NEW — OIDC provider + IAM role for GitHub Actions
```

**V2 (orchestration) — additive, on top of the structure above:**
```
real-time-ecommerce-lakehouse/
└── src/
    └── databricks/
        └── dlt/
            └── dlt_pipeline.py                # NEW — declarative Bronze/Silver/Gold with
                                                 #       data quality expectations
```
*(The Databricks Job `ecommerce-pipeline-v1` is configured directly in the Databricks UI —
chaining the existing `01_bronze_ingestion` → `02_silver_transform` → `03_gold_kpi` notebooks
with no code changes — so it has no corresponding file in this repo.)*

**V2 (observability) — additive, on top of the structure above:**
```
real-time-ecommerce-lakehouse/
└── src/
    └── databricks/
        └── monitoring/
            └── data_freshness_monitoring.py   # NEW — 4th task in ecommerce-pipeline-v1,
                                                 #       checks Bronze/Silver freshness
```
*(Job failure email notifications are configured directly in the Databricks Job UI —
no corresponding file in this repo.)*

---

## Pipeline details (V1)

### 1. Event generation

`producer.py` generates realistic e-commerce events with weighted probabilities matching a typical funnel:

- `page_view` — 70%
- `add_to_cart` — 20%
- `order_created` — 10%

```bash
python src/event_generator/producer.py
```

### 2. Infrastructure (Terraform)

Minimal AWS infrastructure: one S3 bucket for raw events and checkpoints.

```bash
cd terraform/envs/dev
terraform init
terraform apply
```

### 3. AWS ↔ Databricks connection

Databricks accesses S3 through a **Unity Catalog Storage Credential** backed by an IAM Role — no Access Keys stored anywhere.

- IAM Role `databricks-s3-role`, assumed by Databricks' Unity Catalog master role via `sts:AssumeRole`
- Scoped IAM policy granting S3 read/write + SQS/SNS (for Auto Loader file events) on the specific bucket only
- Unity Catalog **External Location** `rtl-dev-raw` pointing to the bucket

### 4. Bronze — raw ingestion

Auto Loader reads JSON files from S3 incrementally and writes to `formation.bronze.events`, enforcing the event schema.

### 5. Silver — cleaning

- Casts `event_ts` to a proper timestamp
- Deduplicates on `event_id`
- Filters invalid `event_type` values and null timestamps

### 6. Gold — business KPIs

Four KPI tables built from the Silver layer:

| Table | Description |
|---|---|
| `conversion_rate` | Per-user conversion: did the user place an order? |
| `ca_par_pays` | Revenue, order count, and average basket per country |
| `top_produits` | Top 10 products by cart additions |
| `distribution_device` | Event distribution across mobile / desktop / tablet |

### 7. Dashboard

A Databricks SQL Dashboard with 4 visualizations built on the Gold tables:

- Conversion rate (counter)
- Revenue by country (bar chart)
- Top products (bar chart)
- Device distribution (donut chart)

![Dashboard](docs/images/dashboard.png)

---

## Key results (V1, sample run, 1000 events)

| Metric | Value |
|---|---|
| Total users | 943 |
| Conversion rate | 11.88% |
| Top product | p_005 |
| Top country by revenue | IT (8.6k€) |

---

## Technical decisions

**Why Unity Catalog instead of cluster-level Spark config for S3 access?**
Storing Access Keys in cluster Spark config is a common anti-pattern — secrets end up in plaintext, visible to anyone with cluster access. Unity Catalog Storage Credentials use IAM Role assumption via STS, which is the AWS-recommended approach for cross-account access and leaves no static credentials anywhere.

**Why `trigger(availableNow=True)` instead of continuous streaming?**
For V1, batch-style incremental processing is sufficient and cost-effective. Continuous streaming is planned for V2 with Kinesis as the ingestion source.

**Why Delta Lake for every layer?**
ACID transactions, schema enforcement, and time travel are available out of the box, which matters even at small scale for data reliability.

---

## 🔵 V2 — Real-time ingestion (Kinesis + Lambda)

> Everything from here on is **V2** — additive on top of V1, nothing above this point was modified.

V2 replaces the manual file-drop pattern with an event-driven ingestion path, while keeping
the V1 batch pipeline untouched for comparison.

```
producer_streaming.py → Kinesis Data Stream → Lambda → S3 (streaming/)
        → Auto Loader → formation.bronze.events_streaming
```

### What was built

- **Kinesis Data Stream** (`rtl-dev-events-stream`) — receives one record per event, in real time
- **Lambda** (`rtl-dev-kinesis-to-s3`) — triggered automatically by Kinesis via an event source
  mapping (`batch_size=100`, `maximum_batching_window=5s`); decodes each batch and writes it as
  newline-delimited JSON to `s3://.../streaming/events/date=YYYY-MM-DD/`
- All infrastructure is provisioned in [`terraform/envs/dev/streaming.tf`](terraform/envs/dev/streaming.tf),
  with IAM permissions scoped to the Kinesis stream and the `streaming/` prefix only — same
  least-privilege principle used for the Databricks ↔ S3 connection
- A new notebook, [`04_bronze_streaming_ingestion`](src/databricks/bronze/04_bronze_streaming_ingestion.py),
  reads from this new prefix into a dedicated Bronze table, so V1 (batch) and V2 (streaming)
  results can be inspected side by side

### Ingestion latency: two honest layers

This pipeline has **two different latencies**, and it's worth being explicit about both:

| Hop | Latency | Real-time? |
|---|---|---|
| Producer → Kinesis → Lambda → S3 | ~5 seconds | ✅ Yes — events land in S3 within seconds of being generated |
| S3 → Auto Loader → Bronze | On-demand (`trigger(availableNow=True)`) | ❌ No — only runs when the notebook is manually executed |

The first hop is genuinely event-driven. The second hop, as configured, is still **batch-style
on-demand processing**: Auto Loader picks up whatever has accumulated in S3 since the last run,
then stops.

### Making the second hop real-time too

Auto Loader supports a continuous trigger that keeps the stream running indefinitely, polling
for new files every few seconds instead of running once and stopping:

```python
query = (
    df_bronze_streaming
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")   # <- continuous, instead of availableNow=True
    .toTable("formation.bronze.events_streaming")
)
```

This was tested directly: with this trigger active, the cluster keeps the stream alive and
ingests new files within ~10 seconds of Lambda writing them — genuinely real-time, end to end.

**Why it isn't run this way by default in this project:** a continuous trigger requires the
Databricks cluster to stay **running permanently**, since the streaming query never terminates.
On a dev/demo project, that means paying for compute 24/7 to process a trickle of simulated
events — a cost that isn't justified outside of latency-critical use cases (e.g. fraud detection).

**What this would look like with `processingTime="10 seconds"` running continuously**, simulated
from the actual test:

```
22:58:48  Lambda writes batch (23 events) → s3://.../date=2026-06-19/9afc...json
22:58:58  Auto Loader picks it up (within 10s) → formation.bronze.events_streaming (+23 rows)
22:59:02  Lambda writes batch (25 events) → s3://.../date=2026-06-19/f4f4...json
22:59:12  Auto Loader picks it up (within 10s) → formation.bronze.events_streaming (+25 rows)
...
```

**What's used instead, and why it's a reasonable trade-off for this project:** `trigger(availableNow=True)`,
run on demand (or schedulable via a Databricks Job every 1-2 minutes for a "near real-time"
middle ground without 24/7 compute cost). This keeps the demonstrated architecture fully
production-realistic — the continuous-trigger code above is the only line that would change to
flip it to true real-time — without paying for an idle cluster.

---

## 🟣 V2 — CI/CD (GitHub Actions + Terraform)

> Additive on top of V1 and the Kinesis/Lambda ingestion above — automates the manual
> `terraform plan` / `terraform apply` steps a developer would otherwise run by hand.

Every push to this repository now triggers an automated pipeline:

```
git push → GitHub Actions
   1. Authenticate to AWS via OIDC (no stored credentials)
   2. terraform plan   — runs on every push and pull request
   3. terraform apply  — runs only on a direct push to main
```

### Authenticating without a stored AWS key (OIDC)

Rather than storing a long-lived AWS Access Key in GitHub Secrets, this project uses
**OpenID Connect (OIDC)**: AWS is configured to trust GitHub's token service directly. On each
workflow run, GitHub presents a short-lived, automatically-rotated token proving its identity;
AWS exchanges it for temporary credentials (15min–1h) via `sts:AssumeRoleWithWebIdentity`. No
AWS secret ever exists in GitHub — the same zero-static-credential principle used for the
Databricks ↔ S3 connection, just with a different trusted party.

This is provisioned in [`terraform/envs/dev/cicd_oidc.tf`](terraform/envs/dev/cicd_oidc.tf):

- An **OIDC Identity Provider** registering `token.actions.githubusercontent.com` as trusted
- An **IAM Role** (`github-actions-terraform-role`) whose trust policy restricts *who* can
  assume it: only this exact repository, and only the `main` branch — a forked repo or a PR
  branch could never authenticate, even knowing the role's ARN
- **Read access** via AWS-managed `ReadOnly` policies (S3, Kinesis, Lambda) — broad but
  inherently safe, since they only grant `Get`/`List`/`Describe` actions
- **Write access** via a scoped custom policy, restricted to resources prefixed `rtl-dev-*` —
  never `AdministratorAccess`

### Remote state backend

Terraform's state file (`terraform.tfstate`) is its memory of what already exists. Previously
it lived only on the developer's machine — meaning GitHub Actions, running on a fresh server
each time, had no way to know existing resources and would try to recreate them. State is now
stored in a dedicated, versioned S3 bucket shared between local development and CI:

```hcl
terraform {
  backend "s3" {
    bucket = "rtl-dev-terraform-state-563683519302"
    key    = "envs/dev/terraform.tfstate"
    region = "eu-west-1"
  }
}
```

### The workflow

[`.github/workflows/terraform.yml`](.github/workflows/terraform.yml) defines two jobs:

| Job | Trigger | What it does |
|---|---|---|
| `terraform-plan` | Every push or pull request | Builds the Lambda zip, runs `terraform plan`, comments the plan on PRs |
| `terraform-apply` | Push directly to `main` only | Same setup, then `terraform apply -auto-approve` |

`terraform-apply` deliberately never runs on `pull_request` events — applying infrastructure
changes from an unreviewed branch would defeat the purpose of having a review process at all.

### What this demonstrates

- End-to-end automated infrastructure deployment, not just local `terraform apply`
- Secretless cloud authentication (OIDC) — the same pattern AWS recommends for any CI system
- Least-privilege IAM: separating broad read access (safe) from narrow write access (scoped)
- Awareness of remote state as a prerequisite for any multi-runner Terraform setup

---

## 🟠 V2 — Orchestration: Databricks Job vs Delta Live Tables

> Additive on top of V1 — same Bronze/Silver/Gold logic, demonstrated through **two different
> orchestration approaches** on purpose, to compare them directly.

### Approach 1 — Databricks Job

The simplest way to schedule an *existing* pipeline: the three V1 notebooks
(`01_bronze_ingestion` → `02_silver_transform` → `03_gold_kpi`) are chained as tasks in a
Databricks Job (`ecommerce-pipeline-v1`), with dependencies declared explicitly in the Job UI.

```
bronze_ingestion → silver_transform → gold_kpi
```

No code changes required — this works with any notebook as-is. The developer is responsible
for declaring the task order; if a new table were added downstream, its dependency would need
to be added manually.

### Approach 2 — Delta Live Tables (DLT)

The same logic, rewritten declaratively in
[`src/databricks/dlt/dlt_pipeline.py`](src/databricks/dlt/dlt_pipeline.py). Tables are defined
as `@dlt.table` functions; DLT **infers the dependency graph automatically** from the code —
when `dlt_silver_events` calls `dlt.read("dlt_bronze_events")`, DLT understands the ordering
without any manual task configuration.

The key addition over the Job approach is **declarative data quality**:

```python
@dlt.table(name="dlt_silver_events", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_event_type", "event_type IN ('page_view', 'add_to_cart', 'order_created')")
@dlt.expect_or_drop("valid_device_type", "device_type IN ('mobile', 'desktop', 'tablet')")
@dlt.expect_or_drop("non_null_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("non_null_timestamp", "event_ts IS NOT NULL")
@dlt.expect_or_drop("order_has_amount", "event_type != 'order_created' OR amount IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount IS NULL OR amount > 0")
def dlt_silver_events():
    return dlt.read_stream("dlt_bronze_events")...
```

Six quality rules are enforced inline. Any row violating one is automatically dropped *before*
reaching Silver — and unlike a manual `.filter()` (which silently discards rows), DLT tracks
**exactly how many rows passed or failed each individual rule**, visible in a built-in
dashboard with zero custom logging code.

Tables live in a separate schema (`formation.dlt`) with a `dlt_` prefix, so this pipeline runs
independently without touching the V1 tables.

### Side-by-side comparison

| | Databricks Job | DLT Pipeline |
|---|---|---|
| Orchestration | Manual — developer declares task order | Automatic — inferred from code |
| Code reuse | Existing notebooks, unmodified | Rewritten using `@dlt.table` syntax |
| Data quality tracking | None built-in (would require custom code) | Native — pass/drop counts per rule |
| Tested result | ✅ Succeeded, 1m17s, all 3 tasks green | ✅ Completed — Bronze 1K rows (1 expectation met), Silver 1K rows (6 expectations met, 0 dropped) |
| Best fit | Quick scheduling of an existing pipeline | Pipelines where data quality must be enforced and audited |

Both were run end-to-end against the same source data, producing matching results (e.g. 943
users in `conversion_rate` either way) — confirming the two approaches are functionally
equivalent, differing only in how they're built and what they track.

---

## 🔴 V2 — Observability (alerting + data freshness monitoring)

> Additive on top of V1 and the Databricks Job orchestration above — adds visibility into
> whether the pipeline is healthy, not just whether it ran.

A pipeline that runs without errors isn't necessarily healthy — it might be processing stale
or empty data without anyone noticing. This section adds two complementary checks: did the job
fail, and is the data itself actually fresh.

### Failure alerting

`ecommerce-pipeline-v1` has an **on-failure email notification** configured directly in the
Databricks Job UI — no custom code required. This was tested directly: an early version of
the monitoring task below failed with a timestamp parsing error, and the failure email arrived
automatically within seconds, confirming the alert path works end-to-end rather than just
being configured and untested.

### Data freshness monitoring

[`src/databricks/monitoring/data_freshness_monitoring.py`](src/databricks/monitoring/data_freshness_monitoring.py)
runs as the 4th task in the pipeline, after `gold_kpi`:

```
bronze_ingestion → silver_transform → gold_kpi → monitoring
```

For each monitored layer (Bronze, Silver), it checks the **actual most recent event timestamp
present in the data** — not just "did the notebook run" — and classifies freshness:

```python
FRESHNESS_OK_DAYS = 14
FRESHNESS_WARNING_DAYS = 30
# status = OK / WARNING / STALE based on hours since the last event in the table
```

Results are **appended** (not overwritten) to `formation.monitoring.data_freshness`, building a
history of freshness checks over time rather than just the latest snapshot. If any layer comes
back `STALE`, the task raises an exception — which fails the Job task and triggers the
email alert already configured above.

**Why the thresholds are generous (14/30 days, not 24/48 hours):** this project's source data
is generated manually via `producer.py`, not on a continuous schedule — tight hourly thresholds
would constantly false-alarm on a dataset that's intentionally static between manual runs. A
production system with a real continuous source (e.g. the Kinesis pipeline above) would use
much tighter thresholds, closer to 24h/48h.

**Proof this actually works**, captured from two consecutive runs:

| checked_at | layer | hours_since_last_event | status |
|---|---|---|---|
| 2026-06-20 21:28:44 | Bronze | 164.34 | ❌ STALE *(old 48h threshold, before the fix)* |
| 2026-06-20 21:28:44 | Silver | 164.34 | ❌ STALE *(old 48h threshold, before the fix)* |
| 2026-06-20 21:36:52 | Bronze | 164.47 | ✅ OK *(new 30-day threshold)* |
| 2026-06-20 21:36:52 | Silver | 164.47 | ✅ OK *(new 30-day threshold)* |

The first run's `STALE` status triggered a real failure email — visible proof the alert isn't
just configured, it actually fires.

### What this demonstrates

- Distinguishing "the job succeeded" from "the data is actually healthy" — a job can run
  clean and still process stale or empty input
- A freshness check based on data content (`event_ts`), not job execution time, which would
  miss a source that's silently gone quiet
- An alert path that was triggered and verified, not just configured and assumed to work
- Historized monitoring data (`append`, not `overwrite`) enabling trend analysis over time

---

## V2 — Planned enhancements

- ~~Kinesis + Lambda — real-time event-driven ingestion~~ ✅ Done (see above)
- ~~CI/CD — GitHub Actions + Terraform automated deployment~~ ✅ Done (see above)
- ~~Delta Live Tables — declarative pipelines with data quality expectations~~ ✅ Done (see above)
- ~~Observability — pipeline monitoring, freshness checks, alerting~~ ✅ Done (see above)
- **Advanced governance** — Unity Catalog ACLs, data lineage *(most valuable with multiple
  users/teams accessing the catalog — limited demo value as a single-user project)*
- **Scheduled near-real-time** — Databricks Job running the streaming Bronze notebook every
  1-2 minutes, as a cost-conscious middle ground between on-demand batch and 24/7 streaming
- **Databricks Asset Bundles (DAB)** — automate notebook and DLT pipeline deployment as part
  of the CI/CD pipeline

---

## Tech stack

`AWS S3` · `AWS IAM` · `AWS Kinesis` · `AWS Lambda` · `Databricks` · `Unity Catalog` · `Delta Lake` · `Auto Loader` · `PySpark` · `Databricks SQL` · `Terraform` · `Python`

---

## Author

Built as part of a Data Engineer reconversion project, alongside Databricks certification preparation.
