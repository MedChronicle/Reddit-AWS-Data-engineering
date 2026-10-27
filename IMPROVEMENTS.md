# What was wrong, and what was added

Nothing from the original project was removed: every original function, constant name, DAG id,
task id (`run_reddit_pipeline`, `s3_upload`), CSV column and config key still exists.

## A. Bugs found in the original (and fixed)

| # | Where | Problem | Effect | Fix |
|---|-------|---------|--------|-----|
| 1 | `pipelines/aws_s3_pipeline.py` | XCom pulled from task id `reddit_extraction`, which does not exist (the DAG task is `run_reddit_pipeline`) | S3 task could never find the file | task id is now a parameter, defaults to the real one |
| 2 | `pipelines/reddit_pipeline.py` | function returned nothing, so XCom `return_value` was `None` | `None.split(...)` crash in the S3 task | returns the CSV path (+ pushes extra XCom keys) |
| 3 | `reddit_pipeline.py` + `reddit_etl.py` | `.csv` was added twice -> `reddit_20241019.csv.csv` (the sample file in `data/output` shows it) | S3 upload would look for a different file name than the one written | extension added only when missing |
| 4 | `dags/reddit_dag.py` | `datetime.now()` at DAG *parse* time | file name frozen at whenever the scheduler last parsed the file; reruns/backfills reuse a stale name | Jinja `{{ ds_nodash }}`, rendered when the task runs |
| 5 | `reddit_etl.transform_data` | `edited` column: every edit timestamp was replaced by the column's most common value | **edited posts were reported as "not edited"** | `edited` = real boolean, new `edited_at` keeps the timestamp |
| 6 | `etls/aws_etl.py` | every error was `print`ed and swallowed; `connect_to_s3` returned `None` on failure | task "succeeds", then dies later with `NoneType` errors; upload failures went unnoticed | errors are logged and raised; upload size is verified |
| 7 | `etls/aws_etl.py` | `aws_session_token` was in the config but never used | temporary (STS/SSO) credentials always failed | token is used; empty keys fall back to the IAM-role/profile chain |
| 8 | `etls/reddit_etl.connect_reddit` | `sys.exit(1)` inside an Airflow task; credentials never actually checked (praw is lazy) | kills the worker process, no retry | raises `ConfigError`, credentials verified up front |
| 9 | `etls/reddit_etl.extract_posts` | `post_dict[key]` -> `KeyError` if Reddit omits a field; no retry on 429/5xx | random failures on a bad API day | `.get()` + exponential-backoff retry (tenacity) |
| 10 | `utils/constants.py` | `config.conf` mandatory at import; `%` in a password crashes configparser; `[etl_settings]` was never read | tests/CI cannot import the package; some passwords break | env > file > default, `interpolation=None`, placeholders ignored, etl settings wired up |
| 11 | `docker-compose.yml` | Postgres had no volume (data and Airflow metadata lost on `down`); init and services started in parallel (race); `pip install` on every init; image 2.7.1 vs `apache-airflow==2.7.2` in requirements | flaky first start, silent in-place Airflow upgrade | volume, healthchecks, `service_completed_successfully`, image aligned to 2.7.2 |
| 12 | `config.conf.example` | password typo `postrgres`; host `localhost` (wrong inside Docker) | connection refused | corrected |
| 13 | `airflow.env` | Fernet key committed | anyone can decrypt Airflow connections | still a dev default, but overridable via `AIRFLOW_FERNET_KEY` |
| 14 | S3 bucket creation (new code, caught by tests) | `us-east-1` rejects an explicit `LocationConstraint` | `InvalidLocationConstraint` for the most common region | bucket created via the low-level client with correct region handling |

## B. Advancements added

**Reliability** - retries with exponential backoff (DAG + API calls), execution/DAG timeouts, `max_active_runs=1`,
atomic file writes, idempotent S3 overwrite and Postgres upsert (re-running a day never duplicates), failure
callback (log + optional Slack), per-run local file cleanup (`retention_days`).

**Data quality** - `etls/data_quality.py` gate: required columns, minimum rows, blank/duplicate ids, future
timestamps (errors) and negative scores, empty titles, bad `upvote_ratio` (warnings). `error_handling = abort|continue`
now actually works. Empty extractions fail the run instead of uploading an empty file.

**Richer data** - 9 extra Reddit fields (subreddit, upvote_ratio, flair, domain, is_self, permalink, awards,
crossposts, selftext) and ~15 derived features: `hotness`, `engagement_ratio`, `comments_per_hour`, `age_hours`,
`title_length`, `title_word_count`, `is_question`, `post_hour_utc`, `post_weekday`, `url_domain`, rule-based
`tags` / `primary_tag` (edit `TOPIC_KEYWORDS` to add topics), `extracted_at`.

**Storage / lakehouse** - CSV *and* Parquet (snappy); Hive-partitioned S3 layout
`raw|processed|reports/subreddit=<name>/dt=<date>/...`; bucket hardening (block public access, AES256, versioning,
lifecycle); per-run JSON summary; Athena DDL (`sql/athena_ddl.sql`).

**Warehouse** - Postgres loader (`reddit.posts` upsert keeps `first_seen_at`; `reddit.post_snapshots` keeps one
row per post per day) and four views: `v_daily_top_posts`, `v_author_leaderboard`, `v_tag_trends`,
`v_score_velocity` (how fast a post keeps gaining score). Example queries in `sql/analytics_queries.sql`.

**Scale-out** - `ETL_SUBREDDITS=dataengineering,python,aws` creates one parallel branch per subreddit.
Run-time overrides: trigger the DAG with `{"limit": 50, "time_filter": "week"}`.

**Engineering** - 46 tests (unit, moto S3 server, end-to-end chain, DAG integrity, real-Postgres), ruff config,
GitHub Actions CI, Makefile, `requirements-dev.txt`, `.env.example`, LocalStack profile for local S3.

## C. Good next steps (not done - they need decisions or infrastructure from you)

1. Replace the plain-text secrets path with Airflow Connections / AWS Secrets Manager.
2. Add dbt models (or Athena views) on top of `processed/` for the gold layer.
3. Great Expectations / Soda if the quality rules grow beyond what `data_quality.py` covers.
4. Pull top comments per post (extra API calls - mind Reddit rate limits) and add NLP sentiment/topic models.
5. Terraform for the bucket, IAM role, Glue database and Athena workgroup.
6. Reddit's `top(time_filter='day')` is relative to *now*, so true historical backfills are impossible with this API;
   use Pushshift-style archives or accept forward-only history.
7. Move from CeleryExecutor + Redis to KubernetesExecutor / MWAA if this becomes more than a handful of DAGs.
