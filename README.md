# Retail Analytics Data Pipeline

An **ELT** pipeline, not ETL — and that distinction is the whole design. Two sources land in Postgres completely untouched, and **100% of the transformation happens after that, in dbt**: cleaning, normalization, quality classification, and the star schema itself, all as version-controlled, tested SQL. PySpark's only job is extraction and type-casting — it never makes a business decision about the data.

**[Live project page →](https://arnavmodi-msit.github.io/retail-analytics-pipeline/)** &nbsp;·&nbsp; **[Live dashboard →](https://app.powerbi.com/view?r=eyJrIjoiYzZkMTA2MzYtNjEzZi00Y2U3LWE2N2YtNDAwZTcwNmQ4Zjg3IiwidCI6IjNiMjk5M2Q3LTQ5YmYtNGYyOS1iNzk0LWRkNTcyN2Y0NWVlMiJ9)**

---

## Why ELT, not ETL

Transform-before-load (ETL) means business rules live in Python/Spark code — outside the easy reach of SQL-literate teammates, and hard to unit-test as pure SQL. This pipeline draws the line differently:

- **PySpark does E+L only.** Extraction and type-casting — the mechanical work of getting a typed column into Postgres. Every row lands, good or bad, no `NOT NULL` gatekeeping, no filtering.
- **dbt owns every actual decision about the data.** What counts as valid vs. quarantine. How to fill in the API source's missing fields. How the dimensional model is shaped. Deterministic surrogate keys instead of auto-increment, since every table gets rebuilt from scratch on every run.

That split means the transformation logic is SQL an analyst can read and dbt can test — not Python only an engineer can safely change. Nothing is silently dropped either: quarantined and return rows stay queryable in `int_sales_classified`, they just don't reach the star schema.

## What it does

Two ingestion paths run independently and land in Postgres in their own native shape — no cross-source harmonization before landing:

- **CSV** — [Online Retail II (UCI)](https://archive.ics.uci.org/dataset/502/online+retail+ii), ~1.07M UK e-commerce transactions, loaded as a historical bulk batch.
- **API** — [DummyJSON `/carts`](https://dummyjson.com/carts), pulled daily as a live incremental feed.

Everything past that point — reshaping the API source to match the CSV's shape, classifying every row into `valid`/`return`/`quarantine`, deduping dimensions, building the star schema — is dbt SQL.

## Architecture

```
CSV bulk ──▶ raw_csv_sales  ──┐
                                ├──▶ dbt: staging → intermediate → marts ──▶ Power BI
API daily ─▶ raw_api_carts ──┘        (normalize · classify · model)
```

Orchestrated by **Airflow** (Docker, LocalExecutor) — 6 tasks, 2 parallel ingestion branches converging through `load_postgres` into `dbt_run` → `dbt_test`, daily schedule, 3 retries, email alert on failure. A failed dbt test fails the DAG run.

| Stage | Tool | What happens |
|---|---|---|
| Ingest | Python / `requests` | CSV historical load + daily API pull, landed untouched |
| Load | PySpark + JDBC | Type casting only — each source lands in its own raw table, in its own native shape |
| Orchestrate | Airflow | Parallel branches merge before the dbt tasks |
| Transform (staging) | dbt | Light 1:1 cleanup per source — trimming, no filtering |
| Transform (intermediate) | dbt | API reshaped to match CSV's columns, sources unioned, every row classified `valid`/`return`/`quarantine` |
| Transform (marts) | dbt | Dimension dedup, surrogate keys (hashed, not auto-increment), star schema built from `valid` rows only |
| Test | dbt | Generic + singular tests — referential integrity, business-rule assertions, a WARN-level bound on the quarantine rate |
| Report | Power BI | Direct Postgres connection, Import mode |

## Data at a glance

| Metric | Value |
|---|---|
| Raw CSV rows landed | 1,067,371 |
| Valid CSV rows in `fact_sales` | 805,620 |
| Quarantined by dbt (documented, not dropped) | 22.7% |
| Daily API rows merged | 800 |
| Products (deduplicated by mode) | 4,820 |
| Unique customers | 6,089 |
| Countries | 42 |
| Total `fact_sales` rows | 806,420 |

## Star schema

Four dimensions (`dim_date`, `dim_product`, `dim_customer`, `dim_country`), one fact table (`fact_sales`) at invoice-line-item grain, all built and tested by dbt from `int_sales_classified`. A `source` column on the fact table tracks which pipeline branch each row came from. Surrogate keys are deterministic hashes (`dbt_utils.generate_surrogate_key`) of the natural key, not auto-increment — dbt rebuilds every table from scratch each run, so keys can't depend on insertion order.

<img src="docs/assets/star-schema.png" alt="Power BI model view — star schema relationships" width="700">

### dbt lineage

```
raw_csv_sales ──▶ stg_csv_sales ─────────────────────┐
                                                        ├──▶ int_sales_unioned ──▶ int_sales_classified ──▶ dim_product / dim_customer / dim_country / fact_sales
raw_api_carts ──▶ stg_api_carts ──▶ int_api_carts_normalized ──┘                                                           ▲
                                                                                                      dim_date (dbt_utils.date_spine) ──┘
```

Run `dbt docs generate && dbt docs serve` from `dbt/` for the interactive version of this graph.

## Orchestration

<img src="docs/assets/airflow-dag.png" alt="Airflow DAG graph — all 6 tasks succeeded: load_csv_raw, api_ingest, cast_api_raw, load_postgres, dbt_run, dbt_test" width="700">

## Getting started

**Prerequisites:** Docker Desktop, Python 3.11, PostgreSQL (local instance for the warehouse — separate from Airflow's own metadata Postgres, which runs in Docker).

```bash
git clone https://github.com/ArnavModi-MSIT/retail-analytics-pipeline.git
cd retail-analytics-pipeline

# Local Python environment (for running scripts directly / testing)
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt

# Environment config — copy and fill in real values
cp .env.local.example .env          # for local `python` runs
cp .env.docker.example .env.docker  # for the Airflow containers

# Warehouse schema — raw landing table only; dbt builds everything else
psql -U postgres -d retail_analytics -f sql/ddl.sql

# dbt (star schema: dims + fact), separate venv — see dbt/
python -m venv .venv-dbt
.venv-dbt\Scripts\activate      # Windows
pip install -r dbt/requirements-dbt.txt
cd dbt && dbt deps && dbt run && dbt test

# Orchestration
docker compose up airflow-init
docker compose up -d
```

Airflow UI: `http://localhost:8080` (default `admin` / `admin`, set during `airflow-init`). Trigger `retail_analytics_pipeline` from the DAGs list.

## Testing

```bash
# Python side — type-casting logic + DAG structure
pip install -r requirements-dev.txt
ruff check .
pytest tests/ -v

# dbt side — the actual transformation logic (27 tests: generic + singular)
cd dbt && dbt test
```

CI runs lint, Python unit tests, DAG import validation, dbt (against a disposable Postgres service container, seeded with small fixture data — see `dbt/seeds/`), and a Docker build check on every push — see [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

> PySpark's local worker sockets can be unreliable on native Windows (a known PySpark/Windows limitation). If `pytest` hangs or times out locally, trust CI's Ubuntu runner — the same suite passes cleanly there.

## Known limitations

Documented deliberately, not discovered accidentally:

- **Truncate-and-load, not incremental.** Every run reloads dimensions and the fact table from scratch. Correct for this scale and demo purpose; a production version would need merge/upsert logic and slowly-changing dimensions.
- **API source has fabricated fields.** DummyJSON has no `Country` or invoice-style ID — defaulted to `"Unknown"` and a synthesized `API-{cart_id}` respectively. See [`dbt/models/intermediate/int_api_carts_normalized.sql`](dbt/models/intermediate/int_api_carts_normalized.sql).
- **Storage abstraction is partially scaffolded.** A local/S3 backend interface exists ([`ingestion/storage_backend.py`](ingestion/storage_backend.py)) and is used for path resolution, but the Spark-level read/write methods aren't yet exercised end-to-end. AWS S3 integration is intentionally deferred to a later phase.
- **API customer IDs aren't namespace-protected** against the CSV source the way `StockCode` and `Invoice` are. No collision today given current ID ranges — a known, low-risk simplification.

## Tech stack

Python · PySpark · dbt · Apache Airflow · PostgreSQL · Docker Compose · Power BI · GitHub Actions · pytest · ruff

## Project structure

```
├── dags/                  # Airflow DAG
├── ingestion/              # API ingestion + storage backend abstraction
├── transform/               # PySpark ingest + type-cast (E+L only, no business rules)
├── dbt/                       # All transformation — staging, intermediate, marts, tests
│   ├── models/
│   │   ├── staging/              # 1:1 cleanup per raw source
│   │   ├── intermediate/          # normalization, union, valid/return/quarantine classification
│   │   └── marts/                  # star schema (4 dims + fact_sales)
│   └── seeds/                    # small fixture data — CI only, never run against real data
├── sql/                      # DDL (raw landing tables only), migrations
├── tests/                     # pytest suite (type-casting, DAG import) — dbt owns transform-logic tests
├── docs/                       # GitHub Pages project site
├── docker-compose.yml
├── Dockerfile               # Airflow + JDK 17 (PySpark) + isolated dbt venv
└── .github/workflows/ci.yml
```

## Author

**Arnav Modi** — B.Tech Information Technology, Maharaja Surajmal Institute of Technology
