# NYC taxi lakehouse: DuckDB + dbt + Iceberg in one container

One Docker image that turns the raw NYC yellow taxi files in S3 into a bronze / silver / gold
lakehouse of **Apache Iceberg** tables. **dbt** orchestrates the SQL, **DuckDB** does the compute,
and the tables are registered in the **AWS Glue Data Catalog**, so Athena (or any other
Glue-aware Iceberg engine) can query them.

```
s3://nyc-test-iceberg/landing/yellow_taxi/*.parquet          raw monthly files (input)
        │  bronze: only new or re-delivered files, values untouched
        ▼
s3://nyc-test-iceberg/bronze-duck/   Glue db nyc_bronze_duck   bronze_yellow_tripdata
        │  silver: typed, renamed, de-duplicated, data-quality rules
        ▼
s3://nyc-test-iceberg/silver-duck/   Glue db nyc_silver_duck   silver_yellow_trips
                                                               silver_yellow_trips_rejected
        │  gold: star schema
        ▼
s3://nyc-test-iceberg/gold-duck/     Glue db nyc_gold_duck     fact_trips, fact_daily_zone_summary,
                                                               dim_date, dim_time_of_day, dim_location,
                                                               dim_vendor, dim_rate_code, dim_payment_type
```

## Tables

| Layer | Table | Grain / content | Load |
|---|---|---|---|
| bronze | `bronze_yellow_tripdata` | Landing rows as-is (lower-case names) plus `_source_file`, `_source_row_number`, `_source_modified_at`, `_source_period`, `_batch_id`, `_ingested_at`. Partitioned by `_source_period`. | incremental by file |
| silver | `silver_yellow_trips` | One row per valid trip: snake_case, proper types, money as `DECIMAL(10,2)`, duplicates removed. Partitioned by pickup month. | incremental by file |
| silver | `silver_yellow_trips_rejected` | Rows that failed a rule, with `_reject_reason`. | incremental by file |
| gold | `fact_trips` | One row per trip, keys to every dimension, measures, `avg_speed_mph`, `tip_percentage`. Partitioned by pickup month. | incremental by file |
| gold | `fact_daily_zone_summary` | Pickup day × pickup zone: trips, passengers, distance, revenue, averages. | rebuilt every run |
| gold | `dim_date` | One row per day (`date_key` = yyyymmdd) for every year with trips. | rebuilt every run |
| gold | `dim_time_of_day` | 24 hours with day part and rush-hour flag. | rebuilt every run |
| gold | `dim_location` | 265 TLC taxi zones, `is_airport`. | rebuilt every run |
| gold | `dim_vendor`, `dim_rate_code`, `dim_payment_type` | TLC data dictionary codes, plus any unknown code seen in the data. | rebuilt every run |

`trip_id` = `yyyymm` of the source file × 10^8 + row number inside that file, for example
`20230100000123`. It is stable across re-runs and points back to the exact raw row.

### Silver cleaning rules

A row is rejected for the first rule it breaks. Thresholds are dbt vars in `nyc_taxi/dbt_project.yml`.

| `_reject_reason` | Rule |
|---|---|
| `duplicate` | Same vendor, timestamps, zones, distance, fare, total and payment type as an earlier row in the file |
| `unknown_source_period` | File name has no `YYYY_MM` / `YYYY-MM` |
| `missing_timestamp` | Pickup or dropoff is NULL |
| `pickup_outside_file_month` | Pickup is not in the file's month (catches 2001/2008 timestamps) |
| `invalid_duration` | Shorter than 1 minute or longer than 6 hours |
| `invalid_distance` | 0 or less, or more than 500 miles |
| `implausible_speed` | Average speed above 80 mph |
| `invalid_amount` | Fare or total 0 or negative (refunds/voids), or fare above 2000 |
| `unknown_location` | Zone id outside 1–265 |

Valid rows are also standardised: passenger count 0 or above 6 becomes NULL, a missing rate code
becomes 99 (unknown), `store_and_fwd_flag` becomes a boolean, and missing surcharges become 0.

## How it works

* `profiles.yml` attaches the Glue Data Catalog to DuckDB through Glue's Iceberg REST endpoint
  (`ATTACH ':' AS lakehouse (TYPE iceberg, ENDPOINT_TYPE glue)`). Every model is created in that catalog.
* Glue needs an explicit table location, cannot rename tables and refuses DROP with purge, so
  dbt-duckdb's built-in `table`/`incremental` materializations do not work there. The project ships
  two of its own (`macros/iceberg/`):
  * `iceberg_table`: the first run uses `CREATE TABLE ... PARTITIONED BY ... WITH ('location' = ...) AS SELECT`.
    Later runs do `DELETE` + `INSERT` in **one transaction**, which is one atomic Iceberg commit:
    readers never see an empty table.
  * `iceberg_incremental`: `delete+insert` on `unique_key` (here `_source_file`), or `append`.
    New rows are first staged in a table in the container's local DuckDB file (compressed on
    disk), then deleted and inserted in one transaction.
* **Rebuilds** (`--full-refresh`, or an `iceberg_table` whose columns changed) first compute the
  new rows into a local staging table. Only after that succeeds is the old table dropped, its S3
  folder emptied, and the table re-created. A broken model never leaves a table missing.
* Emptying a folder goes through `plugins/iceberg_helpers.py`, a small dbt-duckdb plugin. It only
  deletes strictly inside the `bronze-duck/`, `silver-duck/` and `gold-duck/` folders.
* **Incremental by file**: bronze compares the landing listing (name + S3 last-modified time) with
  what it already loaded and reads only new or re-delivered files. Each layer then processes the
  source files whose bronze `_batch_id` it has not seen yet. Re-uploading a corrected file
  therefore replaces its rows in bronze, silver and gold.
* Reference lookups (zones, vendors, rate codes, payment types) are CSVs in `nyc_taxi/reference/`,
  read directly by DuckDB as dbt sources.

## Run it locally

```powershell
docker build --platform linux/amd64 -t nyc-taxi-duckdb-dbt .

# whole pipeline (dbt build = models + tests), using your local AWS credentials
docker run --rm -v "$env:USERPROFILE\.aws:/home/dbt/.aws:ro" nyc-taxi-duckdb-dbt

# any dbt command
docker run --rm -v "$env:USERPROFILE\.aws:/home/dbt/.aws:ro" nyc-taxi-duckdb-dbt build --select gold
docker run --rm -v "$env:USERPROFILE\.aws:/home/dbt/.aws:ro" nyc-taxi-duckdb-dbt build --full-refresh --select silver_yellow_trips+
```

With an AWS profile other than `default`, add `-e AWS_PROFILE=<name>`.

## Run on AWS (ECR + ECS Fargate)

`infra/ecs-stack.yaml` (CloudFormation) holds the whole AWS setup. There is **no schedule**; runs start only when you ask.

| Resource | Name |
|---|---|
| ECR repository (scan on push, keeps the 10 newest images) | `nyc-taxi-duckdb-dbt` |
| ECS cluster (Fargate, Fargate Spot) | `nyc-taxi-duckdb-dbt` |
| Task definition: 4 vCPU / 16 GB, container `dbt` | `nyc-taxi-duckdb-dbt` |
| Task role: read `landing/`, write only the three `*-duck` folders, read the Glue catalog, change tables only in `nyc_*_duck` | `nyc-taxi-duckdb-dbt-task` |
| Execution role: pull the image, write logs | `nyc-taxi-duckdb-dbt-execution` |
| Security group: outbound only | `nyc-taxi-duckdb-dbt-task` |
| Log group, 30-day retention | `/ecs/nyc-taxi-duckdb-dbt` |

```powershell
.\scripts\deploy_ecs.ps1        # create/update the stack (default VPC, public subnets)
.\scripts\push_to_ecr.ps1       # build linux/amd64 and push :latest (push_to_ecr.sh for Linux/macOS/CI)
.\scripts\run_on_ecs.ps1        # start one run, stream its logs, return the exit code

.\scripts\run_on_ecs.ps1 -DbtArgs build, --select, gold
.\scripts\run_on_ecs.ps1 -DbtArgs build, --full-refresh
.\scripts\run_on_ecs.ps1 -Environment @{ DBT_THREADS = "2" } -NoWait
.\scripts\run_on_ecs.ps1 -Spot                              # Fargate Spot: ~70% cheaper, can be interrupted
.\scripts\deploy_ecs.ps1 -TaskCpu 2048 -TaskMemory 8192     # resize the task
```

* Tasks run in the default VPC's public subnets with a public IP (no NAT gateway needed). S3
  traffic stays in the region, so there are no transfer charges.
* Credentials come from the task role automatically: DuckDB and boto3 both use the standard AWS
  credential chain. If Lake Formation permissions are enforced on the catalog, also grant the task
  role access to the `nyc_*_duck` databases.
* Each run ends with a `run_summary` log line: time, CPU, peak memory, network.
* Measured: a fresh load of the full 2023 year takes 4.3 minutes on 4 vCPU / 16 GB (peak memory
  10.2 GB), and a one-month run about 40 seconds. Fargate cost is ~$0.02 per full load and under
  $0.01 per monthly run. See [docs/laptop-vs-ecs.md](docs/laptop-vs-ecs.md) for the full laptop vs ECS comparison.
* To remove everything: `aws cloudformation delete-stack --stack-name nyc-taxi-duckdb-dbt --region us-east-1`
  (this also deletes the ECR images). The data in S3 and the Glue databases are not part of the stack.

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `AWS_REGION` | `us-east-1` | Region of the bucket and the Glue catalog |
| `LAKEHOUSE_ROOT` | `s3://nyc-test-iceberg` | Parent of `bronze-duck/`, `silver-duck/`, `gold-duck/` |
| `LANDING_GLOB` | `s3://nyc-test-iceberg/landing/yellow_taxi/*.parquet` | Raw files to ingest |
| `GLUE_CATALOG_ID` | `:` (caller's account) | Glue catalog to attach |
| `DBT_THREADS` | `4` | Models built in parallel |
| `DUCKDB_MEMORY_LIMIT` | 60% of the container's memory | DuckDB memory cap (e.g. `8GB`); DuckDB spills to `/tmp` beyond it |

### dev target

`--target dev` runs the same pipeline into `dev_nyc_*_duck` databases under `<LAKEHOUSE_ROOT>/dev/`.
Its purge allow-list only covers `dev/`, so it can never delete prod files. Point `LANDING_GLOB` at a
small sample when testing.

## Operations

* **New month**: drop the file into `landing/yellow_taxi/` and run the container. Only that file
  flows through bronze, silver and `fact_trips`; the dimensions and the daily summary are recomputed.
* **Corrected file**: overwrite it in landing. The next run replaces its rows in every layer.
* **Full rebuild**: `build --full-refresh`. **Changing an incremental model's columns** requires that
  rebuild (the run fails with the exact command). `iceberg_table` models rebuild themselves.
* **Table maintenance**: DuckDB writes deletes as positional delete files (merge-on-read), and every
  run adds a snapshot. Enable the Glue Data Catalog table optimizers (compaction, snapshot retention,
  orphan file deletion) on the `nyc_*_duck` tables, or run Athena `OPTIMIZE` / `VACUUM` periodically.

## Query it

```sql
-- Athena (or DuckDB after ATTACH ':' AS lakehouse (TYPE iceberg, ENDPOINT_TYPE glue))
select d.month_name, l.borough, count(*) as trips, sum(f.total_amount) as revenue
from nyc_gold_duck.fact_trips f
join nyc_gold_duck.dim_date d on f.pickup_date_key = d.date_key
join nyc_gold_duck.dim_location l on f.pickup_location_id = l.location_id
group by 1, 2
order by revenue desc;
```

## Project layout

```
Dockerfile, requirements.txt, docker/entrypoint.sh    image (python 3.12 + dbt-core + dbt-duckdb + DuckDB extensions baked in)
scripts/deploy_ecs.ps1                                create/update the AWS setup (CloudFormation)
scripts/push_to_ecr.ps1 | .sh                         build + push the image to ECR
scripts/run_on_ecs.ps1                                start one run on ECS Fargate and follow its logs
infra/ecs-stack.yaml                                  ECR, ECS cluster + task definition, IAM roles, security group, logs
docs/laptop-vs-ecs.md                                 laptop vs ECS Fargate: speed, network, cost, reliability, security
nyc_taxi/                                             dbt project
  dbt_project.yml, profiles.yml                       layers -> Glue databases + S3 folders; DuckDB/Glue connection
  models/bronze | silver | gold                       the models (+ tests in the *.yml files)
  macros/iceberg/                                     iceberg_table / iceberg_incremental materializations
  macros/incremental_files.sql                        file-level incremental logic
  macros/classify_yellow_trips.sql                    silver cleaning rules
  plugins/iceberg_helpers.py                          guarded S3 folder purge used by rebuilds
  reference/                                          TLC lookup CSVs
```
