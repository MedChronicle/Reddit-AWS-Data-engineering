# Reddit AWS Data Engineering Pipeline

An end-to-end data engineering project that collects Reddit posts, cleans and enriches the data, validates it, and delivers analytics-ready datasets to **Amazon S3** and **PostgreSQL**. **Apache Airflow** orchestrates the daily workflow, while optional Slack notifications report pipeline success or failure.

The repository is designed for local development with Docker Compose and can be configured to use AWS S3 or a local S3-compatible endpoint such as LocalStack.

## Table of Contents

- [Project Overview](#project-overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Pipeline Workflow](#pipeline-workflow)
- [Repository Structure](#repository-structure)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
  - [1. Configure credentials](#1-configure-credentials)
  - [2. Build the containers](#2-build-the-containers)
  - [3. Initialize Airflow](#3-initialize-airflow)
  - [4. Start the services](#4-start-the-services)
  - [5. Run the DAG](#5-run-the-dag)
- [Configuration Reference](#configuration-reference)
- [Data Outputs and Storage Layout](#data-outputs-and-storage-layout)
- [Analytics and SQL](#analytics-and-sql)
- [Testing and Code Quality](#testing-and-code-quality)
- [Useful Commands](#useful-commands)
- [Troubleshooting](#troubleshooting)
- [Security Notes](#security-notes)
- [Project Notes](#project-notes)

## Project Overview

The pipeline retrieves posts from one or more configured subreddits through the Reddit API. It transforms the extracted records into a consistent tabular dataset, derives engagement and topic features, applies data-quality checks, and writes three local artifacts:

- **CSV** for raw-layer storage and straightforward inspection.
- **Parquet** for compact, columnar analytics and querying through Athena.
- **JSON summary** with run-level statistics and notable posts.

The artifacts are uploaded to partitioned S3 paths. In parallel, the pipeline can upsert post records and daily snapshots into PostgreSQL, where SQL views support reporting and trend analysis.

## Key Features

- **Scheduled orchestration:** an Airflow DAG named `etl_reddit_pipeline` runs daily and supports manual runs with parameters.
- **Reddit extraction:** retrieves post metadata using PRAW, with retry handling for transient API errors.
- **Data transformation:** standardizes fields, handles missing values, and removes duplicate post IDs.
- **Feature engineering:** adds attributes such as post age, engagement ratio, comments per hour, title statistics, URL domain, and rule-based topic tags.
- **Data-quality gate:** checks required fields, empty or duplicate IDs, future timestamps, and valid upvote ratios. Configure the pipeline to abort or drop invalid rows and continue.
- **Multiple outputs:** generates CSV, Parquet, and JSON summary files.
- **S3 data lake layout:** separates raw, processed, and report artifacts using `subreddit` and date partitions.
- **PostgreSQL analytics:** performs idempotent upserts into the posts and snapshots tables and maintains analytical views.
- **Bucket safeguards:** attempts to configure S3 public-access blocking, default encryption, versioning, and lifecycle settings when permissions allow.
- **Operational support:** retries, structured logging, local-file housekeeping, optional Slack notifications, and automated tests.
- **Containerized environment:** Docker Compose runs Airflow, PostgreSQL, Redis, and optional LocalStack for local S3 development.

## Architecture

```text
                         Reddit API
                             |
                             v
                  Extract posts with PRAW
                             |
                             v
                    Transform and clean
                             |
                             v
                    Enrich and tag data
                             |
                             v
                     Data-quality gate
                             |
                             v
              Write CSV + Parquet + JSON summary
                         /           \
                        /             \
                       v               v
                Amazon S3          PostgreSQL
             raw / processed /     reddit.posts
                  reports          post_snapshots
                       \               /
                        \             /
                         v           v
                      Cleanup local artifacts
                  (runs after successful branches)
```

**Orchestration:** Airflow coordinates extraction, S3 upload, the optional PostgreSQL load, and cleanup. For each configured subreddit, the S3 upload and PostgreSQL load branches depend on the extraction task. Cleanup runs only after the applicable branches succeed.

## Pipeline Workflow

1. **Extract:** connect to Reddit and collect posts for each configured subreddit. The default DAG parameters are `time_filter=day` and `limit=100`; these can be overridden when triggering a run.
2. **Transform:** normalize the records, standardize timestamps and fields, and remove duplicate posts.
3. **Enrich:** calculate derived metrics and rule-based topic labels from post metadata and text.
4. **Validate:** run the configured quality checks before writing output files. In `abort` mode, a failed quality check fails the task. In `continue` mode, invalid rows are removed and processing continues when possible.
5. **Write local artifacts:** save CSV and JSON summary files and, by default, a Parquet file under `data/output/`.
6. **Publish to S3:** upload CSV to the raw layer, Parquet to the processed layer, and the JSON summary to the reports layer.
7. **Load PostgreSQL:** upsert post records and append/update daily snapshots so repeat runs do not simply duplicate the same post rows.
8. **Housekeeping and notification:** remove local artifacts older than the configured retention period after the required branches succeed. Airflow callbacks log task or DAG outcomes and can send Slack notifications if a webhook is configured.

## Repository Structure

The following table describes the main directories and files included in this repository.

| Path | Purpose |
|---|---|
| `dags/` | Airflow DAG definitions and task dependencies. |
| `dags/reddit_dag.py` | Defines the daily `etl_reddit_pipeline` DAG and its per-subreddit tasks. |
| `pipelines/` | Airflow task-level orchestration functions that connect the ETL modules. |
| `pipelines/reddit_pipeline.py` | Runs extraction, transformation, enrichment, validation, and local output generation. |
| `pipelines/aws_s3_pipeline.py` | Uploads generated artifacts to the S3 data lake. |
| `pipelines/postgres_pipeline.py` | Loads the generated dataset into PostgreSQL. |
| `pipelines/housekeeping_pipeline.py` | Removes expired local output files. |
| `etls/` | Core extraction, transformation, storage, analytics, and quality logic. |
| `etls/reddit_etl.py` | Reddit connection, extraction, transformation, enrichment, and CSV/Parquet writing. |
| `etls/data_quality.py` | Data-quality rules and quality reports. |
| `etls/aws_etl.py` | S3 connection, bucket creation/hardening, partition keys, and uploads. |
| `etls/postgres_etl.py` | PostgreSQL tables, upsert logic, snapshots, and analytical views. |
| `etls/analytics_etl.py` | Builds and writes the per-run JSON summary. |
| `etls/housekeeping.py` | Local output retention and cleanup utilities. |
| `utils/` | Shared configuration, logging, constants, and alert callbacks. |
| `utils/constants.py` | Loads settings using environment variables, `config/config.conf`, and defaults. |
| `utils/logger.py` | Shared logging setup. |
| `utils/alerts.py` | Airflow success/failure logging and optional Slack messages. |
| `config/config.conf.example` | Example configuration file; copy it to `config/config.conf` for local overrides. |
| `.env.example` | Example environment variables used by Docker Compose. |
| `airflow.env` | Non-secret Airflow service settings for the local Compose environment. |
| `sql/athena_ddl.sql` | Athena external-table DDL for querying processed Parquet files in S3. |
| `sql/analytics_queries.sql` | Example PostgreSQL and Athena analytical queries. |
| `tests/` | Unit, DAG, data-quality, S3, PostgreSQL, and end-to-end tests. |
| `.github/workflows/ci.yml` | GitHub Actions workflow for linting, tests, Docker Compose validation, and image build. |
| `Dockerfile` | Builds the custom Airflow image with project dependencies. |
| `docker-compose.yml` | Defines Airflow, PostgreSQL, Redis, and optional LocalStack services. |
| `requirements.txt` | Runtime Python dependencies. |
| `requirements-dev.txt` | Runtime dependencies plus testing and linting tools. |
| `Makefile` | Shortcuts for common Docker, test, lint, and cleanup commands. |
| `pyproject.toml` | Ruff lint configuration. |
| `pytest.ini` | Pytest configuration. |
| `IMPROVEMENTS.md` | Summary of fixes and additions made to the project. |
| `data/output/` | Local pipeline output directory, including a sample CSV file. |

## Technology Stack

| Technology | Role in the project |
|---|---|
| Python 3.9 | Main implementation language in the supplied Docker image. |
| Apache Airflow 2.7.2 | Workflow scheduling, orchestration, retries, and task monitoring. |
| PRAW | Access to the Reddit API. |
| pandas | Tabular transformation, enrichment, and validation support. |
| PyArrow / Parquet | Columnar output for analytics. |
| Amazon S3 | Object storage for raw, processed, and report artifacts. |
| Amazon Athena | SQL querying over Parquet data in S3. |
| PostgreSQL 12 | Structured storage for post records and daily snapshots. |
| SQLAlchemy | Database connections, schema definitions, and upsert operations. |
| Redis + CeleryExecutor | Airflow task execution infrastructure in Docker Compose. |
| Docker Compose | Local multi-service development environment. |
| pytest, moto, Ruff | Automated tests, S3-compatible test infrastructure, and linting. |

## Prerequisites

Install or arrange the following before starting:

- Docker Desktop with Docker Compose support.
- A Reddit application with API credentials. Create a **script** application at [Reddit App Preferences](https://www.reddit.com/prefs/apps).
- An AWS account and a bucket name if writing to real S3, plus credentials or an IAM role available to the containers. For local-only experimentation, LocalStack can provide an S3-compatible endpoint.
- Git, if cloning the repository rather than downloading the ZIP.

## Getting Started

Run these commands from the project root, the directory containing `docker-compose.yml` and `Makefile`.

### 1. Configure credentials

Create a local environment file:

```bash
cp .env.example .env
```

Edit `.env` and set your Reddit credentials and storage settings. For example:

```dotenv
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_SECRET_KEY=your_reddit_client_secret
AWS_REGION=ap-south-1
AWS_BUCKET_NAME=your-unique-s3-bucket-name
ETL_SUBREDDITS=dataengineering
AIRFLOW_ADMIN_PASSWORD=change-this-password
```

For multiple subreddits, use a comma-separated value such as `dataengineering,aws,analytics`. Do not include the `r/` prefix.

For real AWS, provide credentials securely or use an IAM role available to the container. Do not add AWS keys to source code. A host AWS profile is not automatically mounted into the containers by the supplied Compose file; configure a secure mount or use another credential method if you rely on a local profile.

Generate a unique Airflow Fernet key and set `AIRFLOW_FERNET_KEY` in `.env`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

You can alternatively configure settings in `config/config.conf` by copying the example:

```bash
cp config/config.conf.example config/config.conf
```

The Python configuration loader resolves values in this order: environment variable, `config/config.conf`, then built-in default. Docker Compose explicitly forwards the common variables shown in `.env.example`; for other settings, edit `config/config.conf` or update the Compose environment mapping so the setting reaches the container.

### 2. Build the containers

```bash
make build
```

This builds the custom Airflow image using `Dockerfile` and the pinned runtime dependencies.

### 3. Initialize Airflow

Run the one-time initialization command:

```bash
docker compose up airflow-init
```

This initializes the Airflow metadata database and creates the local administrator user if it does not already exist. The service may exit after successful initialization; that is expected.

### 4. Start the services

```bash
make up
```

Open the Airflow web UI at **http://localhost:8080**. The default username is `admin`; the password is `admin` unless you changed `AIRFLOW_ADMIN_PASSWORD` before initialization.

Check service status or view scheduler logs:

```bash
docker compose ps
make logs
```

### 5. Run the DAG

In the Airflow web UI:

1. Find `etl_reddit_pipeline` in the DAG list.
2. Enable/unpause the DAG if necessary.
3. Trigger a run manually, keeping the default parameters or overriding them with valid values such as `{"time_filter": "week", "limit": 50}`.
4. Open the DAG run to inspect task states and logs.

The scheduled interval is daily. To change the subreddits, update `ETL_SUBREDDITS` in `.env` and restart or recreate the relevant services so the updated environment is applied; alternatively, edit `config/config.conf` and use the appropriate service restart. The DAG creates a task branch for each configured subreddit.

### Optional: Use LocalStack instead of AWS S3

1. Set a bucket name and region in `.env` (for example, `AWS_REGION=us-east-1`).
2. Set the endpoint to `AWS_ENDPOINT_URL=http://localstack:4566`.
3. Start the local AWS profile:

```bash
make up-local
```

The pipeline creates the bucket if it does not exist. LocalStack is intended for local development and does not replace testing against real AWS permissions and behavior.

## Configuration Reference

| Setting | Purpose | Example / Default |
|---|---|---|
| `REDDIT_CLIENT_ID` | Reddit application client ID. | Set in `.env`. |
| `REDDIT_SECRET_KEY` | Reddit application secret. | Set in `.env`; keep private. |
| `REDDIT_USER_AGENT` | Descriptive Reddit API user agent. | Built-in project default if omitted. |
| `ETL_SUBREDDITS` | Comma-separated subreddit names. | `dataengineering` |
| `AWS_BUCKET_NAME` | Destination S3 bucket. | Required for S3 uploads. |
| `AWS_REGION` | AWS or LocalStack region. | `.env.example` uses `ap-south-1`. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Explicit AWS credentials when needed. | Prefer a role or secure credential provider where possible. |
| `AWS_SESSION_TOKEN` | Session token for temporary AWS credentials. | Optional. |
| `AWS_ENDPOINT_URL` | S3-compatible endpoint, mainly for LocalStack/MinIO. | `http://localstack:4566` for LocalStack. |
| `AWS_HARDEN_BUCKET` | Attempts public-access blocking, encryption, versioning, and lifecycle configuration. | `true` in the example config. |
| `LOAD_TO_POSTGRES` | Enables/disables the PostgreSQL load branch. | `true` in the example config. |
| `ETL_ERROR_HANDLING` | Quality-check behavior: `abort` or `continue`. | `abort` |
| `ETL_RETENTION_DAYS` | Age limit for local output files. | `14` |
| `ETL_MIN_ROWS` | Minimum acceptable number of rows. | `1` |
| `ETL_BATCH_SIZE` | Batch size for database loading. | `100` |
| `ETL_WRITE_PARQUET` | Whether to write Parquet output. | `true` |
| `SLACK_WEBHOOK_URL` | Optional Slack webhook for outcome notifications. | Empty disables Slack messages. |
| `AIRFLOW_ADMIN_PASSWORD` | Password created for the local Airflow administrator. | `admin` in the development setup; change it. |
| `AIRFLOW_FERNET_KEY` | Encrypts protected Airflow connection/config values. | Generate a unique key for your environment. |

For additional options, see `utils/constants.py` and `config/config.conf.example`.

## Data Outputs and Storage Layout

### Local files

Generated files are written under `data/output/` by default:

| Artifact | Purpose |
|---|---|
| `reddit_<subreddit>_<date>.csv` | CSV dataset for inspection and raw-layer upload. |
| `reddit_<subreddit>_<date>.parquet` | Parquet dataset for analytics, when enabled. |
| `reddit_<subreddit>_<date>_summary.json` | Aggregated metrics and top-post summaries for the run. |

The date suffix is based on the Airflow run date. Older generated CSV, Parquet, and JSON files can be removed according to the retention setting.

### Amazon S3 layout

Files are stored using Hive-style partitions:

```text
s3://<bucket>/
├── raw/
│   └── subreddit=<subreddit>/dt=<YYYY-MM-DD>/*.csv
├── processed/
│   └── subreddit=<subreddit>/dt=<YYYY-MM-DD>/*.parquet
└── reports/
    └── subreddit=<subreddit>/dt=<YYYY-MM-DD>/*_summary.json
```

| Layer | Format | Intended use |
|---|---|---|
| `raw/` | CSV | Readable extract and downstream ingestion. |
| `processed/` | Parquet | Efficient columnar analytics and Athena queries. |
| `reports/` | JSON | Run-level metrics, top posts, tags, and other summary statistics. |

### PostgreSQL warehouse

When the PostgreSQL branch is enabled, the loader creates or updates these objects in the configured `reddit` schema:

| Object | Purpose |
|---|---|
| `reddit.posts` | Latest stored record and enriched attributes for each Reddit post ID. |
| `reddit.post_snapshots` | Daily score/comment snapshots used to analyze changes over time. |
| `reddit.v_daily_top_posts` | Top ten posts for each post date, ranked by score. |
| `reddit.v_author_leaderboard` | Author posting and score summary. |
| `reddit.v_tag_trends` | Daily post counts and engagement grouped by primary tag. |
| `reddit.v_score_velocity` | Score and comment changes between successive snapshots. |

A post's score velocity becomes useful after multiple daily snapshots have been collected.

## Analytics and SQL

### PostgreSQL queries

Use `sql/analytics_queries.sql` to explore examples for daily top posts, author leaderboards, topic trends, score velocity, post timing, and question-versus-non-question engagement. Run the queries in a PostgreSQL client connected to the configured database after the pipeline has created the schema and views.

### Amazon Athena

Before running `sql/athena_ddl.sql`:

1. Replace the placeholder S3 location `s3://<your-bucket>/processed/` with your actual processed-data path.
2. Execute the DDL in Athena to create the `reddit` database and external table.
3. Run `MSCK REPAIR TABLE reddit.posts_processed;` after new partitions are uploaded, unless you use another partition-registration approach.

The SQL file is a starting point and has **not been verified against a live Athena environment** in this repository. Confirm the table definition and permissions in your own AWS account before relying on it for production workloads.

## Testing and Code Quality

Install the development dependencies in a Python environment compatible with the pinned project dependencies:

```bash
pip install -r requirements-dev.txt
```

Run the test suite and linter:

```bash
make test
make lint
```

The repository currently contains **46 test functions**. Tests cover data transformation, data-quality checks, DAG structure, S3 behavior, PostgreSQL loading, alerts/configuration, and an end-to-end flow using test doubles and local test services.

To include the integration test that connects to a real PostgreSQL instance, start the Compose PostgreSQL service and run:

```bash
docker compose up -d postgres
make test-pg
```

`make test-pg` uses the connection URL configured in the Makefile. The GitHub Actions workflow also runs tests and linting and validates/builds the Docker Compose image.

## Useful Commands

| Command | Action |
|---|---|
| `make help` | Show available Makefile targets. |
| `make build` | Build the custom Airflow image. |
| `make up` | Start the default Compose services. |
| `make up-local` | Start the services with the LocalStack profile. |
| `make down` | Stop the Compose services while retaining the named PostgreSQL data volume. |
| `make logs` | Follow Airflow scheduler logs. |
| `make test` | Run the local pytest suite. |
| `make test-pg` | Run tests with the PostgreSQL integration URL configured. |
| `make lint` | Run Ruff checks. |
| `make fmt` | Automatically fix Ruff findings where possible. |
| `make clean` | Remove Python, pytest, and Ruff cache files. |

To inspect logs for a specific service, run `docker compose logs -f <service-name>`, for example `docker compose logs -f airflow-scheduler`.

## Troubleshooting

| Problem | Things to check |
|---|---|
| DAG does not appear in Airflow. | Confirm `dags/reddit_dag.py` is mounted, check scheduler logs with `make logs`, and verify the DAG parses without import errors. |
| Reddit authentication fails. | Verify `REDDIT_CLIENT_ID` and `REDDIT_SECRET_KEY`, confirm the Reddit app type is `script`, and use a descriptive user agent. |
| S3 upload fails. | Confirm `AWS_BUCKET_NAME` and `AWS_REGION`, check that credentials are available inside the container, verify bucket/account permissions, and make sure `AWS_ENDPOINT_URL` is correct for LocalStack. |
| PostgreSQL load fails. | Check that the Compose `postgres` service is healthy and that database host/name/user/password match the active configuration. Inside Compose, the hostname is `postgres`, not `localhost`. |
| Airflow UI is unavailable. | Run `docker compose ps`, inspect webserver logs with `docker compose logs -f airflow-webserver`, and confirm port `8080` is available. |
| A run fails the data-quality gate. | Read the task logs for the quality report. Check the required columns, record IDs, duplicate IDs, timestamps, upvote ratios, and configured minimum row count. |
| LocalStack is unreachable. | Start with `make up-local` and use `http://localstack:4566` from inside the Airflow containers. `localhost:4566` refers to the container itself when used from a service. |

## Security Notes

- Do not commit `.env`, `config/config.conf`, Reddit secrets, AWS keys, session tokens, or real webhook URLs.
- Replace the development Airflow administrator password and default Fernet key before sharing the environment or exposing it beyond a trusted local machine.
- The Compose file contains development database credentials and exposes service ports for local use. Change credentials and networking before using a shared or production environment.
- Use least-privilege AWS permissions. Bucket hardening is best-effort; the pipeline can log warnings if the identity lacks permission to apply a setting.
- Review Reddit's API terms and privacy requirements before collecting, storing, or redistributing data.

## Project Notes

- The main schedule is daily and `catchup=False`, so Airflow does not backfill all historical dates automatically.
- The project stores the fields and metadata returned through the Reddit API; it does not collect full historical Reddit data beyond what the configured API query returns.
- The topic classifier uses keyword patterns in `utils/constants.py`; it is rule-based, not a trained machine-learning model.
- See [`IMPROVEMENTS.md`](IMPROVEMENTS.md) for documented fixes and additions, including known next steps that are not implemented yet.

---

**Built for learning and demonstrating an end-to-end data engineering workflow:** API extraction, data quality, transformation, orchestration, cloud object storage, SQL analytics, automated testing, and containerized development.
