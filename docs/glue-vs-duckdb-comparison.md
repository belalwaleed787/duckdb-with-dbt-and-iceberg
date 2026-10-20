# Glue/Spark project vs DuckDB/dbt project: full comparison

Two implementations of the same NYC yellow-taxi lakehouse (landing → bronze → silver → gold, Apache
Iceberg tables in the AWS Glue Data Catalog, same S3 bucket `nyc-test-iceberg`, us-east-1):

| Name in this document | What it is |
|---|---|
| **Glue project** | [NYC_aws_iceberg_project](https://github.com/belalwaleed787/NYC_aws_iceberg_project): AWS Glue (PySpark) jobs for bronze and silver, dbt-athena in AWS Lambda for gold, AWS Step Functions orchestration, SageMaker Processing jobs for profiling and quality checks. Tables in `nyc_*_db`, files under `warehouse/`. |
| **DuckDB project** | This repository: dbt + DuckDB in one container, run as an ECS Fargate task. Tables in `nyc_*_duck`, files under `bronze-duck/`, `silver-duck/`, `gold-duck/`. |

Everything here was checked on 2026-10-05:

* **The code:** the GitHub repository at commit `9732857` (2026-09-09).
* **What is deployed in AWS:**
  * the Glue job scripts in S3;
  * the Step Functions state machine;
  * the run history of the Glue jobs, Step Functions, Lambda and SageMaker;
  * the Iceberg metadata of every table;
  * a few read-only Athena queries, whose result files were deleted afterwards.

[Section 15](#15-methodology-and-caveats) has the details.

## Verdict by aspect

| Aspect | Glue project | DuckDB project | Better |
|---|---|---|---|
| Speed, one month of data | 8 min 38 s landing → gold; 13 min 43 s with the SageMaker checks | 68 s landing → gold, 67 s including 48 tests | **DuckDB**, 8–12× |
| Speed, full year | ≈2 h 45 min (12 executions, one file each; estimate) | 4 min 52 s in one run | **DuckDB**, ≈34× |
| Cost per monthly run | ≈$0.13 (Glue is $0.12 of it) | ≈$0.005 | **DuckDB**, ≈25× |
| Cost to build and test | $3.11 of Glue alone (54 job runs) | $0.06 of Fargate | **DuckDB** |
| Data actually loaded today | silver and gold hold **one month** (March 2023); bronze holds March **12 times** | all 12 months, each once | **DuckDB** |
| Incremental loading | one file per execution, path written into the state machine; gold uses a date watermark that skips older or late files | finds new and re-delivered files on its own; re-runs are safe | **DuckDB** |
| Cleaning rules | rows that fail vanish without a trace; 137,499 valid March trips dropped for a missing passenger count; no upper limits (a 14,258-mile trip is in gold) | every dropped row is kept with its reason; upper limits for distance, speed and amounts | **DuckDB** |
| Reference data | borough worked out from ID ranges: **wrong for 215 of 265 zones** | official TLC lookup files | **DuckDB** |
| Tests | 113 dbt tests, richer test types, but the pipeline runs `dbt run`, so **they never execute** | 48 tests, run on every build; a failing test stops the models that depend on it | **DuckDB** in practice, Glue project on paper |
| Gold model breadth | 3 facts (adds an hourly vendor mart and zone-to-zone pairs); a richer date dimension | 2 facts; a simpler date dimension | **Glue project** |
| Orchestration and alerting | Step Functions with retries and an SNS alert on failure | a script starts the task; no alerts | **Glue project** |
| Governance | column-level lineage to Amazon DataZone (OpenLineage); profiling and quality reports in S3 | none beyond tests and the rejected-rows table | **Glue project** |
| Observability | Spark UI logs, Glue job insights, a visual execution history | CloudWatch logs, a run summary line, task timestamps | **Glue project** |
| Security | roles shared with another project; one uses an AWS permissive admin policy; account ARNs in the public repo | one least-privilege task role; no ARNs in the code | **DuckDB** |
| Infrastructure as code | none: built in the console, and the deployed version differs from the repo | everything in one CloudFormation stack | **DuckDB** |
| Local development | needs AWS Glue and Athena to run anything | the same container runs on a laptop | **DuckDB** |
| Scaling to much more data | Spark adds workers; Athena is serverless | one machine, up to 16 vCPU / 120 GiB | **Glue project** |
| Number of moving parts | 11 AWS services, 3 engines (Spark, Athena/Trino, pandas) | 6 AWS services, 1 engine (DuckDB) | **DuckDB** |
| Storage | 4.48 GB for one month (old table versions never cleaned up) | 2.33 GB for twelve months | **DuckDB** |
| Custom code to maintain | standard dbt-athena materializations | 409 lines of custom Iceberg materializations | **Glue project** |

**In short:**

* **The DuckDB project** is faster, cheaper, simpler, and produces correct numbers for the whole year.
* **The Glue project** has better operations around the data: an orchestrator with retries and
  alerts, lineage, profiling reports, Spark's room to grow, and a richer data model. However, its
  deployed version has bugs, so it loads the same month again and again, and its location
  dimension puts most trips in the wrong borough.

## 1. Architecture

| | Glue project | DuckDB project |
|---|---|---|
| Landing → bronze | Glue job `NYC_landing_into_bronze` (PySpark, Glue 5.1, 3 × G.1X workers) | dbt model `bronze_yellow_tripdata` (DuckDB) |
| Bronze → silver | Glue job `NYC_bronze_into_silver` (PySpark, 4 × G.1X), `MERGE INTO` on `trip_id` | dbt models `silver_yellow_trips` and `silver_yellow_trips_rejected` |
| Silver → gold | dbt-athena run in AWS Lambda (container image, 1 GB); the SQL runs in Athena | dbt models in the same DuckDB process |
| Checks after load | two SageMaker Processing jobs (`ml.t3.medium`): profiling and 6 quality queries, results as JSON in S3 | 48 dbt tests in the same `dbt build` |
| Orchestration | Step Functions `nyc-pipeline` (Glue → Glue → Lambda → parallel SageMaker), SNS topic on failure | `scripts/run_on_ecs.ps1` starts one Fargate task; dbt orders the models |
| Compute | Glue Spark clusters, Athena, Lambda, SageMaker instances | one Fargate task (4 vCPU, 16 GiB) |
| AWS services | 11: S3, Glue ETL, Glue Data Catalog, Athena, Lambda, ECR, Step Functions, SNS, SageMaker, DataZone, CloudWatch | 6: S3, Glue Data Catalog, ECR, ECS Fargate, CloudWatch Logs, CloudFormation (Athena is optional, for queries) |
| Engines and SQL dialects | Spark SQL, Athena (Trino) SQL, pandas | DuckDB SQL only |
| Trigger | start the state machine by hand | run the script by hand |

## 2. Speed

### One month of data

The Glue project's only fully successful execution (`execution_13`, 2026-09-01) against the DuckDB
project's one-month runs on ECS:

| Step | Glue project | DuckDB project |
|---|---:|---:|
| Landing → bronze | 295.0 s (Glue job) | one dbt run for all layers: 38.2 s<br>(68.2 s from task start to finish) |
| Bronze → silver | 172.6 s (Glue job) | (included above) |
| Silver → gold | 50.5 s (Lambda + Athena) | (included above) |
| **Landing → gold** | **518.1 s (8 min 38 s)** | **68.2 s** |
| Quality checks | 304.6 s (2 SageMaker jobs in parallel) | 48 tests; the full run with tests took 67.0 s |
| **Whole pipeline** | **822.9 s (13 min 43 s)** | **67.0 s** |

Where the Glue project's time goes:

* **Start-up, not work.** The landing job ran Spark for 166 s, but the step took 295 s: Glue needs
  time to provision a cluster before every job.
* **Small, sequential queries.** Each gold model is a separate set of Athena queries, so even tiny
  dimensions take 10–19 s each. The same dimensions take 1.3–3.0 s in DuckDB.
* **Instances for short scripts.** SageMaker starts two instances to run scripts that need only
  seconds of work.

### Full year

* **Glue project:** it handles one landing file per execution, so 12 months need 12 executions.
  At the measured 13 min 43 s each, that is about **2 h 45 min** (estimate).
* **DuckDB project:** it loaded all 12 months in **one run of 4 min 52 s** (260.1 s of dbt).

### Per gold model (one month)

| Model | Glue project (Athena) | DuckDB project, equivalent model |
|---|---:|---:|
| trip fact | 13.4 s (`fact_taxi_trips`) | 7.4 s (`fact_trips`) |
| date dimension | 10.6 s | 3.0 s |
| location dimension | 11.8 s | 1.3 s |
| payment / rate code / vendor dimensions | 9.8–12.0 s each | 2.1–2.8 s each |
| time-of-day dimension | 19.1 s | 1.4 s |
| daily zone aggregate | 11.7 s (`fact_daily_zone_metrics`) | 4.1 s (`fact_daily_zone_summary`) |

## 3. Cost

Prices: AWS Price List API, us-east-1:

| Service | Price |
|---|---|
| Glue | $0.44 per DPU-hour |
| SageMaker `ml.t3.medium` | $0.05 per hour |
| Lambda | $0.0000167 per GB-second |
| Step Functions | $0.000025 per state transition |
| Athena | $5 per TB scanned (10 MB minimum per query) |
| Fargate (4 vCPU / 16 GiB) | $0.233 per hour |

### One monthly run

| Item | Glue project (`execution_13`) | DuckDB project |
|---|---:|---:|
| Glue landing → bronze (498 DPU-seconds; typical run 297) | $0.061 | – |
| Glue bronze → silver (479 DPU-seconds; typical 484) | $0.059 | – |
| Lambda running dbt (52 GB-seconds) | $0.001 | – |
| Athena queries from dbt and the checks (estimate) | ≈$0.002 | – |
| SageMaker Processing (2 jobs × ≈3 min) | $0.005 | – |
| Step Functions (≈8 transitions) | $0.0002 | – |
| Fargate task (78–83 s billed) and its public IP | – | $0.005 |
| **Total** | **≈$0.13** | **≈$0.005** |

On Fargate Spot (up to 70% off), the DuckDB project's monthly run drops to about $0.0015.

### Full year and other costs

| | Glue project | DuckDB project |
|---|---:|---:|
| Full year | ≈$1.53 (12 × $0.13, estimate) | $0.020 (one run) |
| Building and testing in AWS | $3.11: Glue alone, 54 job runs, 7.07 DPU-hours (26 failed runs during development); plus SageMaker, Lambda, Athena | $0.06: 7 Fargate tasks; development ran in Docker on a laptop |
| S3 storage | 4.48 GB → $0.10/month, for one month of data | 2.33 GB → $0.054/month, for twelve months |
| Container image in ECR | 226 MB (`nyc-taxi-dbt-runner`) | 240 MB (`nyc-taxi-duckdb-dbt`) |

## 4. What is actually in the tables

Iceberg metadata and Athena counts, 2026-10-05:

| Table | Glue project | DuckDB project |
|---|---|---|
| Bronze rows | **46,825,913**: January once, February once, **March 12 times** (40,845,192 rows, 37.4M of them duplicates) | 38,310,226: all 12 months, each once |
| Silver rows | **3,189,538**: March only, from one batch | 37,038,677 + 1,271,549 rejected rows |
| Gold trip fact | 3,189,538 rows, **5 duplicate `trip_id`s**, pickups 2023-02-23 to 2023-04-05 | 37,038,677 rows; `trip_id` is unique, and a test checks it |

**Why the Glue project holds one month:** the deployed version is older than the code in GitHub,
and it has two bugs.

1. **The bronze job always loads March.** The deployed state machine passes `--S3_LANDING_PATH`,
   but the deployed bronze script reads `--S3_LANDING_FILE`. Every run therefore falls back to the
   script's default, the March file, even though the state machine names April. Bronze appends,
   so March was added once per run.
2. **Silver never sees new data.** The deployed silver script filters bronze by a hard-coded batch
   ID (`fa674650-…`), so every run re-merges the same March batch.

The GitHub version (commit 2026-09-09) fixes both bugs, but it was never deployed. Its README
also says that bronze "still appends on every run", so re-running a file still duplicates it.

## 5. Data quality: the same March file through both pipelines

Both pipelines read the same raw file, `yellow_tripdata_2023_03.parquet` (3,403,766 rows).

| | Glue project | DuckDB project |
|---|---:|---:|
| Kept in silver | 3,189,538 (93.7%) | 3,314,209 (97.4%) |
| Removed | 214,228, with no record of which rows or why | 89,557, stored in `silver_yellow_trips_rejected` with a reason |
| Valid trips removed only because `passenger_count` is missing or 0 | **137,499** | 0 (the count is set to NULL instead) |
| Trips faster than 120 mph left in gold | 934 | 0 (anything over 80 mph is rejected) |
| Trips longer than 500 miles left in gold | 28 (longest 14,257.91 miles) | 0 |

The DuckDB project's March rejects by reason:

| Reason | Rows |
|---|---:|
| invalid duration | 41,649 |
| invalid amount | 23,913 |
| invalid distance | 23,625 |
| pickup outside the file's month | 189 |
| implausible speed | 181 |

The Glue project's silver rules, applied to the raw file, reproduce its silver count exactly
(3,189,538). The breakdown is: 87,619 rows with no passenger count, 58,365 with 0 passengers,
48,528 with zero or negative distance, 9,764 with a bad duration and 29,837 with negative
amounts. Some rows break more than one rule.

### Wrong boroughs in the Glue project's gold layer

`dim_locations` guesses the borough from ID ranges (2–48 Manhattan, 49–90 Brooklyn, and so on).
TLC location IDs are ordered alphabetically by zone name, not grouped by borough. Compared with
the official TLC zone lookup:

* **the borough is wrong for 215 of the 265 zones (81%)**;
* 260 zones have no real name, only "Zone ID: n".

March 2023 pickups by borough, from each project's gold layer:

| Borough | Glue project: trips | Glue project: revenue | DuckDB project: trips | DuckDB project: revenue |
|---|---:|---:|---:|---:|
| Manhattan | 187,440 | $4.7M | **2,953,268** | **$69.4M** |
| Queens | 1,262,719 | $43.7M | 307,492 | $22.3M |
| Brooklyn | 300,127 | $7.9M | 19,427 | $0.6M |
| Bronx | 231,779 | $5.5M | 4,423 | $0.1M |
| Staten Island | 119,520 | $3.0M | 168 | $0.01M |
| Unknown / other | 1,087,781 | $25.2M | 29,406 | $0.9M |

Any borough-level report from the Glue project is wrong. The DuckDB project uses the official TLC
lookup, with 265 zones, their names, boroughs and service zones.

Other reference data:

| Dimension | Glue project | DuckDB project |
|---|---|---|
| Vendors | only IDs 1 and 2, with 2 named "VeriFone Inc." (an older TLC name); any other vendor is "Unknown Vendor" | IDs 1, 2, 6 and 7 with the current TLC names (2 = Curb Mobility, 6 = Myle Technologies, 7 = Helix) |
| Payment type 0 | "Other" | "Flex Fare trip" |
| Rate code 99 | "Special / Unknown" | "Null/unknown" |

## 6. Incremental loading and re-runs

| | Glue project | DuckDB project |
|---|---|---|
| What a run processes | one file, whose path is written into the state machine definition | every new or re-delivered file in `landing/`, found by file name and last-modified time |
| Loading a new month | edit the state machine (or the job arguments), then start it | drop the file in `landing/`, run the task |
| Running the same file twice | bronze gets a second copy | nothing changes |
| A corrected (re-delivered) file | appended again in bronze; silver merges on `trip_id` | its rows are replaced in every layer |
| Gold fact | dbt-athena `merge`, reading silver rows with `pickup_date >= max(pickup_date)` in gold | only the rows from new files, tracked by file and batch |
| Loading an older month after a newer one | **skipped in gold**: after March, the watermark is 2023-04-05, so January rows never pass the filter | loaded like any other file |
| Backfill of 12 months | 12 executions | one run |
| Full rebuild | manual | `dbt build --full-refresh` |
| Trip key | SHA-256 of vendor, timestamps, zones and total (64-character string); identical trips merge into one | `BIGINT` from the file's month and row number; every source row keeps its own key |

## 7. Tests and checks

| | Glue project | DuckDB project |
|---|---|---|
| dbt tests defined | **113**: 49 not_null, 8 unique, 9 relationships, 7 accepted_values, 36 dbt_expectations, 3 dbt_utils, 1 custom calendar test | **48**: not_null, unique, relationships, accepted_values |
| Run by the pipeline | **No.** The Lambda receives `{"command": "run"}`, so tests never run in production | **Yes.** `dbt build` runs the tests after each model; a failure skips the models that depend on it |
| Would they pass today? | At least 3 would fail on the current gold data (unique `trip_id`, `trip_distance` ≤ 300, `avg_speed_mph` ≤ 120) | all 48 pass |
| Checks after loading | SageMaker quality job: 6 SQL checks. The last report says "WARNING" (934 speed outliers), but nothing stops or alerts | none separate |
| Profiling | SageMaker report (row counts, averages, payment mix) as JSON in S3 | none; `run_summary` covers resources, not data |

The Glue project has the stronger test suite on paper: ranges, regex checks, compound uniqueness and
a careful calendar test. The DuckDB project's tests are simpler, but they actually run.

## 8. Gold data model

| | Glue project | DuckDB project |
|---|---|---|
| Trip fact | `fact_taxi_trips`: string key, partitioned **by day** (82 files for one month) | `fact_trips`: integer key, partitioned by month (12 files for the year) |
| Aggregates | `fact_daily_zone_metrics` (day × pickup zone × dropoff zone) and `fact_hourly_vendor_performance` (day × hour × vendor) | `fact_daily_zone_summary` (day × pickup zone, with card and cash counts) |
| Date dimension | fixed 2020–2030, about 30 columns: ISO week **and ISO year**, season, quarter/month/year boundaries, leap year | the years present in the data, 12 columns; ISO week without ISO year |
| Location dimension | derived from the data; wrong boroughs (Section 5) | official TLC lookup |
| Other dimensions | names hard-coded in `CASE` statements; only codes seen in silver | reference CSV files, plus "Unknown …" rows for any new code |
| Time of day | 24 rows: 12-hour label, period, rush-hour flag | 24 rows: hour label, day part, rush-hour flag |

The Glue project's gold layer is broader. It has an origin–destination grain, an hourly vendor
mart, and a richer, better-tested date dimension. These would be worth porting to the DuckDB
project.

## 9. Orchestration, monitoring and reliability

| | Glue project | DuckDB project |
|---|---|---|
| Orchestrator | Step Functions: visual run history, retries for Lambda and SageMaker, every failure goes to an SNS topic (1 subscriber) | none: one task; dbt runs the steps in order |
| Logs | Glue continuous logs, Spark UI event logs, Glue job insights and observability metrics; Step Functions logging is OFF | CloudWatch Logs (30 days) with a `run_summary` line |
| Lineage | OpenLineage to Amazon DataZone, column level | none |
| Alerts | SNS on failure | none |
| Observed runs | 13 executions: 1 succeeded, 1 aborted, 11 failed. Bronze job: 42 runs, 14 succeeded. Silver job: 12 of 12 succeeded. SageMaker profiling: 3 of 5 runs failed. Most failures happened while it was being built | 7 of 7 ECS tasks succeeded; development failures happened locally in Docker |

## 10. Security

| | Glue project | DuckDB project |
|---|---|---|
| Spark / compute role | Glue role shared with another project (`yt-data-pipeline-glue-role-dev`), including `datazone:*` on all resources | one task role: read `landing/`, write only the three `*-duck/` folders, change Glue tables only in `nyc_*_duck` |
| dbt role | Lambda role: Athena on all resources, read/write on the whole bucket | the same task role |
| Check jobs | SageMaker role with the AWS managed `SageMakerStudioAdminIAMPermissiveExecutionPolicy` (`s3:*`, `glue:*`, `athena:*`, `cloudformation:*`, `iam:CreateRole`, `iam:PassRole`) | – |
| Account details in the public repo | account ID, role, Lambda and SNS ARNs in `StepFunction/StateMachine.json`; local paths in `.vscode/mcp.json` | none in the code; ARNs come from the CloudFormation stack at run time |

## 11. Infrastructure, deployment and drift

| | Glue project | DuckDB project |
|---|---|---|
| How resources are created | AWS console; the state machine definition was exported into the repo | one CloudFormation template (`infra/ecs-stack.yaml`) and three scripts |
| Deploying a code change | upload each Glue script to S3, rebuild and push the Lambda image, update the state machine | `push_to_ecr.ps1` (26 s for a code change) |
| Deployed = repository? | **No** (Section 4): the GitHub fixes were never deployed | yes: the image is built from the repository |
| Version control | Git on GitHub, 6 commits | **not in Git yet** |

## 12. Development and portability

| | Glue project | DuckDB project |
|---|---|---|
| Run it locally | no: Spark jobs need AWS Glue (or Glue's Docker image), dbt-athena needs Athena | yes: the same image runs on a laptop (`--target dev`); a 100K-row sample builds in 78 s |
| Cost of a test run | Glue minimum billing for 3–4 workers, plus ≈2 min start-up per job | nothing on a laptop; ≈$0.004 on ECS |
| Lock-in | Glue, Athena, Lambda, Step Functions, SageMaker | AWS only for S3, the Glue catalog and Fargate; DuckDB and dbt run anywhere |

## 13. Scaling

| | Glue project | DuckDB project |
|---|---|---|
| How it grows | add Spark workers; Athena scales by itself | a bigger task, up to 16 vCPU / 120 GiB, on one machine |
| Headroom at today's size | large, but Spark's start-up dominates at this size | full year in 4 min 52 s, 10.2 GiB peak memory on a 16 GiB task |
| Limit | practically none for this data | hundreds of GB would need splitting the work across tasks, or Spark |

For terabytes of data, the Glue project's design scales more naturally. For this dataset
(0.6 GB of input per year), it pays Spark's overhead without needing Spark's scale.

## 14. Storage, code and documentation

| | Glue project | DuckDB project |
|---|---|---|
| S3 used | 4.48 GB for one month. Silver alone uses 3.36 GB for a 280 MB table: each of the 12 `MERGE` runs rewrote it, and old snapshots are never expired | 2.33 GB for twelve months |
| Lines of code | 589 Python (PySpark, Lambda, SageMaker) + 369 SQL + 420 YAML + 283 state-machine JSON | 144 Python/shell + 312 SQL models + 409 SQL macros + 284 YAML + 174 CloudFormation + 192 scripts and Dockerfile |
| Hard parts to maintain | three engines and dialects; a Lambda workaround for multiprocessing locks; JSONata state machine | custom Iceberg materializations that work around DuckDB/Glue limits (no rename, no purge on drop) |
| Dependency versions | ranges (`dbt-core>=1.8,<1.9`) | exact pins |
| Documentation | long README, but it describes the repo version, not what is deployed. 24 lines still contain citation markers from an AI tool (`cite…turn226499view2`). The dbt sub-project still has the dbt starter README | README, the laptop-vs-ECS report, an architecture diagram and this document |
| Leftovers | `nyc_gold_db.gold_daily_trip_summary` (not in the repo); empty `quarantine/` prefix | none |

## 15. Methodology and caveats

**Sources**

* **Glue project code:** GitHub, commit `9732857` (2026-09-09).
* **Deployed Glue project:**
  * the Glue scripts in `s3://aws-glue-assets-…/scripts/` (last changed 2026-08-25);
  * the live state machine definition.
* **Run history:**
  * `aws glue get-job-runs` (all runs of both jobs);
  * the Step Functions history of `execution_13`;
  * Lambda `REPORT` log lines and the dbt result returned to Step Functions;
  * `aws sagemaker list-processing-jobs`.
* **Table contents:** the current Iceberg `metadata.json` of every table, plus read-only Athena
  queries. The Athena results were written to a temporary prefix and deleted.
* **DuckDB project numbers:** [docs/laptop-vs-ecs.md](laptop-vs-ecs.md).
* **Borough comparison:** the Glue project's `CASE` logic, applied to the official TLC zone lookup
  file.

**Caveats**

* **Only one Glue execution succeeded.** Its timing comes from that single run. Because of the
  argument bug it re-loaded March, but it is still a normal one-file run.
* **Full-year Glue figures are estimates** (12 × the measured run). A single Glue run over all files
  would be cheaper, but the project processes one file per execution.
* **Some costs are approximations.** Athena cost is estimated from the bytes scanned and the 10 MB
  minimum per query. SageMaker cost uses each job's run time.
* **Snapshot totals overstate gold row counts.** The Glue project's gold tables use delete files, so
  their snapshot `total-records` includes deleted rows. Gold row counts in this document come from
  Athena `count(*)` instead.
* **Nothing in the Glue project was changed.**
