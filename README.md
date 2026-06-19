# Real-Time E-commerce Lakehouse

A production-style lakehouse pipeline built on **AWS S3 + Databricks + Unity Catalog**, simulating an e-commerce event stream and turning raw JSON events into business-ready KPIs.

> **Status:** V1 complete ✅ — batch ingestion with Auto Loader, medallion architecture (Bronze/Silver/Gold), and a live SQL dashboard.

---

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

```
real-time-ecommerce-lakehouse/
├── data/
│   └── sample/
│       └── events.json              # Generated sample events
├── src/
│   ├── event_generator/
│   │   ├── producer.py              # Event generator
│   │   └── ecommerce_event.json     # JSON schema
│   └── databricks/
│       ├── bronze/
│       │   └── 01_bronze_ingestion.py
│       ├── silver/
│       │   └── 02_silver_transform.py
│       └── gold/
│           └── 03_gold_kpi.py
├── terraform/
│   └── envs/
│       └── dev/
│           ├── main.tf
│           ├── variables.tf
│           └── terraform.tfvars
└── README.md
```

---

## Pipeline details

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

## Key results (sample run, 1000 events)

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

## V2 — Planned enhancements

- **Kinesis + Lambda** — real-time event-driven ingestion, replacing the file-drop pattern
- **CI/CD** — GitHub Actions + Databricks Asset Bundles for automated deployment
- **Advanced governance** — Unity Catalog ACLs, data lineage
- **Delta Live Tables** — declarative pipelines with data quality expectations
- **Observability** — pipeline monitoring, freshness SLAs, alerting

---

## Tech stack

`AWS S3` · `AWS IAM` · `Databricks` · `Unity Catalog` · `Delta Lake` · `Auto Loader` · `PySpark` · `Databricks SQL` · `Terraform` · `Python`

---

## Author

Built as part of a Data Engineer reconversion project, alongside Databricks certification preparation.
