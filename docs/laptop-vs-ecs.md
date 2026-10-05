# Laptop vs AWS ECS Fargate: full comparison

The pipeline (dbt + DuckDB writing Iceberg tables to S3 through the AWS Glue Data Catalog) ran in
two places with the same image, code and data:

* **Laptop**: Docker Desktop on a Windows laptop. It reaches S3 and Glue in us-east-1 over a home
  internet connection.
* **ECS**: a one-off ECS Fargate task in us-east-1, the bucket's region. It runs the image stored in
  Amazon ECR.

The ECS runs repeat the laptop runs one for one, starting from empty tables. One extra ECS run loads
the whole year at once. All measurements were taken on 2026-10-05. [Section 14](#14-methodology-and-caveats)
explains where each number comes from and its limits.

## Verdict by aspect

| Aspect | Laptop | ECS | Better |
|---|---|---|---|
| Speed of real loads | 10-month batch: 60 min 50 s | 5 min 8 s | **ECS**, 10–12× faster |
| Speed of tiny runs | no-op run: ≈33 s from start to finish | ≈41 s, of which 23–26 s is task start-up | **Laptop** |
| Network to S3 and Glue | 139 ms round trip; ≈3 MB/s down, ≈0.8 MB/s up | 1.5 ms; 63 MB/s per stream, 352 MB/s with 12 streams | **ECS** |
| CPU | 8 threads and faster cores, mostly waiting on the network | 4 vCPU, about 60% busy | Laptop on paper, ECS in practice |
| Memory | 7.6 GiB Docker VM; the first full load crashed Docker Desktop | 16 GiB; peak 10.7 GiB | **ECS** |
| Cost per run | $0 compute. Downloads are free up to 100 GB a month, then $0.09/GB: ≈$0.26 for the 10-month batch | $0.023 for the 10-month batch, ≈$0.005 for a monthly run | Laptop inside the free tier, **ECS** beyond it |
| Fixed cost per month | S3 storage, ≈$0.05 | ≈$0.08–0.11 (adds image storage) | Laptop, by a few cents |
| Reliability | 1 of 5 attempts failed; the laptop must stay awake and online | 7 of 7 tasks succeeded; does not need the laptop once started | **ECS** |
| Security | admin IAM user with a long-lived access key | least-privilege task role with temporary credentials | **ECS** |
| Development and debugging | edit and re-run without rebuilding; shell into the container | logs only; every code change needs an image push | **Laptop** |
| Logs and monitoring | terminal output only | CloudWatch Logs (30 days), task timestamps, run summary | **ECS** |
| Room to grow | limited by the home upload speed and 15.7 GiB of RAM | up to 16 vCPU / 120 GiB per task by changing one parameter | **ECS** |
| Results | identical rows, all 48 tests pass | identical | Same |

What explains the gap:

* **The internet connection, not the CPU.** The laptop has the faster processor. But it uploads at
  ≈0.8 MB/s and waits ≈140 ms for every S3 or Glue request. On the 10-month batch, 39 of its 61
  minutes went to uploading results.
* **ECS has a fixed start-up cost.** Each task needs 23–26 s before dbt starts and 23–25 s to shut
  down. On a run with nothing to do, that makes ECS slower than the laptop.
* **Money is not the deciding factor.** All five ECS test runs together cost $0.057 of Fargate time.
  A monthly run costs about half a cent.

**Recommendation:** run real loads (monthly files, backfills, `--full-refresh`) on ECS. Develop and
debug on the laptop against the `dev` target with a small sample. Details are in
[Section 13](#13-recommendations).

## 1. Test setup

### Machines

| | Laptop | ECS |
|---|---|---|
| Hardware | ASUS TUF Dash F15: Intel Core i7-11370H (4 cores / 8 threads, 3.3 GHz, up to 4.8 GHz), 15.7 GiB RAM, 1 TB NVMe SSD, Windows 11 | Fargate platform 1.4.0, x86_64. AWS chooses the CPU model; the probe task ran on an Intel Xeon Platinum 8259CL @ 2.5 GHz |
| Limits for the container | Docker Desktop (WSL2) VM: 8 CPUs, 7.6 GiB | task: 4 vCPU, 16 GiB, 20 GiB disk |
| DuckDB memory cap (60% of the container, set by the entrypoint) | 4.6 GiB | 9.6 GiB |
| Image | built locally from this repository | the same build, pulled from ECR (240 MB compressed) |
| Software | Python 3.12, dbt-core 1.12.5, dbt-duckdb 1.11.0, DuckDB 1.5.6 | same |
| AWS credentials | an IAM user's access key from `~/.aws`, mounted read-only | the task role |
| Path to S3 and Glue in us-east-1 | home internet | inside the region |

### Runs

| # | Run | dbt command | New rows | dbt threads |
|---|---|---|---:|---:|
| 1 | January, first load, all models + 48 tests | `build` | 3,066,766 | 4 |
| 2 | February added, models only | `build --exclude resource_type:test` | 2,913,955 | 4 |
| 3 | Nothing new | `build --select` the 4 incremental models, no tests | 0 | 4 |
| 4 | March–December added, all models + 48 tests | `build` | 32,329,505 | 2 |
| 5 | Fresh load of the full year (ECS only) | `build` on empty tables | 38,310,226 | 4 |

* `LANDING_GLOB` limited runs 1 and 2 to the January file and the January–February files.
* Run 4 used 2 threads in both places to keep memory down on the laptop.
* Run 5 was not repeated on the laptop: run 4 already took an hour for 10 of the 12 months.

## 2. Speed

### dbt run time

Times in this section are dbt's own execution time ("Finished running ... in X seconds") unless
stated otherwise.

| # | Run | Laptop | ECS | ECS faster by |
|---|---|---:|---:|---:|
| 1 | January + tests | 340.4 s | 34.4 s | 9.9× |
| 2 | February | 390.2 s | 38.2 s | 10.2× |
| 3 | Nothing new | 26.9 s | 9.4 s | 2.9× |
| 4 | March–December + tests | 3,649.5 s (60 min 50 s) | 308.0 s (5 min 8 s) | 11.9× |
| 5 | Full year, fresh | – | 260.1 s (4 min 20 s) | – |

Throughput in run 4 was 8,860 rows per second on the laptop and 105,000 on ECS. Run 5 on ECS
reached 147,000 rows per second, tests included.

### From start to committed data

dbt's timer leaves out start-up:

* **Laptop:** about 6 s to start the container and for dbt to parse the project.
* **ECS:** Fargate must first find capacity and pull the image, which takes 23–26 s before the
  container starts. After the container exits, tearing the task down takes another 23–25 s.

The data is committed when the container exits, so the teardown does not delay the results.

| # | Laptop (dbt + ≈6 s) | ECS: task created → container exited | ECS faster by | ECS: until `run_on_ecs.ps1` returned |
|---|---:|---:|---:|---:|
| 1 | ≈346 s | 67.0 s | 5.2× | 127 s |
| 2 | ≈396 s | 68.2 s | 5.8× | 117 s |
| 3 | ≈33 s | 41.2 s | 0.8× (laptop faster) | 84 s |
| 4 | ≈3,656 s | 337.4 s | 10.8× | 382 s |
| 5 | – | 292.0 s | – | 339 s |

ECS task phases, range over the five runs:

| Phase | Time |
|---|---|
| Provisioning (task created → image pull starts) | 10.6–12.6 s |
| Image pull, 240 MB from ECR | 8.1–8.9 s |
| Container start | 4.3–4.8 s |
| **Total before dbt starts** | **23–26 s** |
| Teardown after the container exits | 23.3–24.8 s |

The script's time also covers the teardown and up to 10 s of polling delay. On the laptop,
`docker run` returns as soon as dbt finishes.

### Where the time goes (run 4)

Each incremental model works in two steps:

1. It reads and transforms the new rows into a local staging table. dbt logs "N new rows" when this
   finishes.
2. It writes those rows to S3 as Parquet and commits the Iceberg snapshot.

Splitting the three big models at that log line shows what slows the laptop down. The critical path
is the chain of steps that run one after another, so it sets the total run time.

| Step on the critical path | Laptop | ECS | ECS faster by |
|---|---:|---:|---:|
| Bronze: read 10 landing files (540 MB) and add metadata columns | 197 s | 34 s | 5.8× |
| Bronze: write ≈530 MB and commit | 636 s | 24 s | 26× |
| Silver: read bronze and clean | 484 s | 92 s | 5.3× |
| Silver: write ≈640 MB and commit | 769 s | 28 s | 27× |
| fact_trips: read silver and build | 126 s | 41 s | 3.1× |
| fact_trips: write ≈735 MB and commit | 953 s | 26 s | 37× |
| Everything else: start-up, dimensions, daily summary, tests | 484 s | 64 s | 7.6× |
| **Total** | **3,650 s** | **308 s** | **11.9×** |

* **Writing is where the laptop loses.** The three writes took 2,358 s (39 min, 65% of the run) on
  the laptop and 78 s on ECS, 30× faster. The laptop wrote at 0.77–0.83 MB/s, which is its internet
  upload speed.
* **Reading is 3–6× faster on ECS.** The laptop downloads at 2.2–3.3 MB/s. On ECS, reading is
  limited by DuckDB's CPU work, not by the network.
* `silver_yellow_trips_rejected` runs at the same time as silver, so it is not on the critical path.

### Per model, run 4 (10 months, 32.3M rows)

| Model | Laptop | ECS | ECS faster by |
|---|---:|---:|---:|
| `bronze_yellow_tripdata` | 833.3 s | 57.9 s | 14.4× |
| `silver_yellow_trips` | 1,252.6 s | 119.9 s | 10.5× |
| `silver_yellow_trips_rejected` | 504.5 s | 101.4 s | 5.0× |
| `fact_trips` | 1,079.2 s | 66.6 s | 16.2× |
| `fact_daily_zone_summary` | 153.3 s | 12.8 s | 12.0× |
| `dim_date` | 146.1 s | 6.4 s | 22.8× |
| `dim_payment_type` | 99.7 s | 8.4 s | 11.9× |
| `dim_rate_code` | 41.3 s | 5.4 s | 7.7× |
| `dim_vendor` | 21.7 s | 6.1 s | 3.6× |
| `dim_location` | 12.1 s | 1.1 s | 11.0× |
| `dim_time_of_day` | 9.6 s | 1.1 s | 8.8× |

### Per model, runs 1–3

| Model | Run 1 laptop | Run 1 ECS | Run 2 laptop | Run 2 ECS | Run 3 laptop | Run 3 ECS |
|---|---:|---:|---:|---:|---:|---:|
| `bronze_yellow_tripdata` | ≈85 s* | 8.5 s | 88.5 s | 10.7 s | 7.0 s | 2.5 s |
| `silver_yellow_trips` | ≈97 s* | 8.7 s | 118.7 s | 11.7 s | 7.2 s | 2.5 s |
| `silver_yellow_trips_rejected` | ≈30 s* | 5.6 s | 38.4 s | 7.9 s | 5.9 s | 2.4 s |
| `fact_trips` | 96.8 s | 4.9 s | 98.9 s | 7.4 s | 5.6 s | 2.1 s |
| `fact_daily_zone_summary` | 17.2 s | 2.0 s | 51.6 s | 4.1 s | – | – |
| each dimension | 6.7–17.6 s | 1.5–2.3 s | 6.4–27.8 s | 1.3–3.0 s | – | – |

\* Derived from the Iceberg commit timestamps, because only part of this run's log was kept.

Run 3 moves no data, yet every model is 2.5–2.8× slower on the laptop. Each model makes dozens of
small requests to S3 and Glue: it lists the landing files, reads the Iceberg metadata and checks the
table. Every one of those requests waits for the 140 ms round trip ([Section 3](#3-network)).

### Tests (run 4)

| | Laptop | ECS |
|---|---:|---:|
| All 48 tests, summed | 310.4 s | 66.8 s |
| Unique `trip_id` on `fact_trips` | 39.5 s | 5.7 s |
| Unique `trip_id` on silver | 36.8 s | 8.5 s |
| 8 relationship tests on `fact_trips` | 15.8–31.6 s each | 4.1–7.7 s each |
| 8 tests on the reference CSVs (inside the image, no network) | 0.03–0.12 s each | 0.02–0.05 s each |

Tests that use no network take about the same time in both places. This confirms the gap comes from
the connection, not the CPU.

## 3. Network

Both places ran the same read-only Python probe (boto3, medians of 10–15 requests), one right after
the other at about 12:50 UTC:

| Measurement | Laptop | ECS | ECS better by |
|---|---:|---:|---:|
| TCP connect to the S3 endpoint (≈ one round trip) | 138.9 ms | 1.5 ms | 93× |
| TCP connect to the Glue endpoint | 135.4 ms | 1.4 ms | 97× |
| S3 HEAD request | 146.1 ms | 11.4 ms | 13× |
| S3 GET of 1 KB | 160.6 ms | 25.8 ms | 6× |
| Glue GetTable | 189.7 ms | 49.7 ms | 4× |
| Download one 47.7 MB landing file, 1 stream | 3.5 MB/s | 62.9 MB/s | 18× |
| Download all 12 landing files (636 MB), 12 streams | not run; ≈2.7 MB/s during run 4's bronze step | 351.8 MB/s | ≈130× |
| Upload | 0.77–0.83 MB/s during run 4's writes | not the limit: DuckDB wrote 22–29 MB/s, held back by the CPU | ≈30× |

More parallel streams do not help the laptop: 16 streams downloaded at 2.8 MB/s, because the home
line is the limit.

Both places move the same amount of data per run; only the speed differs. The ECS numbers below
come from the container's network counters. Laptop run 4 matched them: ≈2.9 GB read, ≈2.1 GB
written.

| Run | Read from S3 | Written to S3 |
|---|---:|---:|
| 1: January + tests | 0.24 GB | 0.19 GB |
| 2: February | 0.27 GB | 0.18 GB |
| 3: no-op | ≈1 MB | ≈0 |
| 4: 10 months + tests | 2.9 GB | 2.0–2.1 GB |
| 5: full year | 3.6 GB | 2.4 GB |

## 4. CPU, memory and disk

| | Laptop | ECS |
|---|---|---|
| CPUs available | 8 (i7-11370H, up to 4.8 GHz) | 4 vCPU (the probe task had a Xeon 8259CL, 2.5 GHz) |
| CPU busy, run 4 | 1–2 CPUs in `docker stats` samples; about 1 CPU while uploading | 737 CPU-seconds in 314 s: 2.35 vCPUs on average (59%) |
| CPU busy, run 5 | – | 650 CPU-seconds in 267 s: 2.43 vCPUs (61%) |
| CPU busy, runs 1–2 | – | 1.7–1.8 vCPUs |
| Peak memory, run 4 | 5.3 GiB of 7.6 GiB (`docker stats` samples) | 10.7 GiB of 16 GiB (sampled every 5 s) |
| Peak memory, run 5 | – | 10.2 GiB |
| Peak memory, runs 1–2 / run 3 | – | 1.8–2.1 GiB / 0.15 GiB |
| Local disk | ≈0.9 GB DuckDB staging file during bronze; 1 TB SSD | 20 GiB task disk; the same ≈1 GB of staging fits easily |

* **CPU.** The laptop has the faster cores and twice the threads, but most of the time it waits for
  the network. ECS has fewer, slower vCPUs and keeps them about 60% busy.
* **Memory.** Memory use follows the DuckDB cap, not the data size. DuckDB fills the memory it is
  given (4.6 GiB on the laptop, 9.6 GiB on ECS) before spilling to disk.
* **Laptop memory failure.** The first attempt at run 4 used 6.8 of the VM's 7.6 GiB and wrote 3.5 GB
  of temporary files. Docker Desktop stopped responding (HTTP 500) and the run died. Two changes fixed
  it: DuckDB is now capped at 60% of the container's memory, and new rows are staged in a compressed
  table on disk. Every run in this document uses that fix. ECS has 16 GiB and never came close to the
  limit.

## 5. Cost

### Prices

From the AWS Price List API, us-east-1, 2026-10-05.

| Item | Price |
|---|---|
| Fargate (x86) | $0.04048 per vCPU-hour + $0.004445 per GB-hour, which is $0.233 per hour for 4 vCPU / 16 GiB |
| Fargate billing rule | per second, from the start of the image pull to the task stop; 1-minute minimum |
| Fargate ARM (Graviton) | $0.03238 per vCPU-hour + $0.00356 per GB-hour (20% less) |
| Fargate Spot | up to 70% off Fargate (AWS's figure; the price changes) |
| Public IPv4 address (every ECS task gets one) | $0.005 per hour |
| Data transfer from S3 to the internet (the laptop's downloads) | first 100 GB per month free across the account, then $0.09/GB |
| Data transfer from S3 to ECS in the same region | free |
| S3 storage | $0.023 per GB-month |
| S3 requests | $0.005 per 1,000 PUT/LIST, $0.0004 per 1,000 GET |
| ECR storage | $0.10 per GB-month |
| Glue Data Catalog requests | first million per month free, then $1 per million |
| CloudWatch Logs ingestion | $0.50 per GB |

### Per run

| Run | Laptop: compute | Laptop: download charge if the free tier is used up | ECS: Fargate (billed time) | ECS: public IPv4 |
|---|---:|---:|---:|---:|
| 1 | $0 | 0.24 GB → $0.022 | $0.0050 (78 s) | $0.0001 |
| 2 | $0 | 0.27 GB → $0.024 | $0.0054 (83 s) | $0.0001 |
| 3 | $0 | ≈$0 | $0.0039 (60 s minimum) | $0.0001 |
| 4 | $0 | 2.9 GB → $0.26 | $0.0227 (351 s) | $0.0005 |
| 5 | – | (would be 3.6 GB → $0.32) | $0.0197 (304 s) | $0.0004 |
| **Runs 1–4** | **$0** | **≈$0.31** | **$0.037** | **$0.001** |

Notes:

* All five ECS runs together cost $0.057 of Fargate time and $0.001 for public IPv4.
* All laptop testing that day, including the failed attempt and data checks, downloaded ≈5 GB. That
  is $0 inside the free tier and ≈$0.45 beyond it.
* S3 request charges are the same in both places. They are under $0.01 per full run (estimate).
* Laptop electricity for the hour-long run 4 is about 0.05 kWh, roughly one cent (estimate, at
  ≈50 W).
* Fargate Spot would cut the ECS column by up to 70%, to about $0.006 for a full-year load. A Spot
  test run (bronze and `fact_trips`, nothing new) succeeded.
* The two extra short ECS tasks, the Spot test and the network probe, cost under $0.01 together.

### Per month

| Item | Laptop | ECS |
|---|---:|---:|
| One monthly run (one new file + tests) | $0 (≈0.25 GB downloaded: free tier, else ≈$0.02) | ≈$0.005 on demand, ≈$0.0015 on Spot |
| Lakehouse storage in S3 (2.33 GB now) | $0.054 | $0.054 |
| ECR image storage (2 versions of ≈240 MB; a code-only change adds a few MB) | – | ≈$0.02–0.05 |
| CloudWatch Logs (≈14 KB per run, kept 30 days) | – | ≈$0 |
| Glue Data Catalog | free tier | free tier |
| **Total** | **≈$0.05** | **≈$0.08–0.11** |

The laptop is a few cents a month cheaper while its downloads stay inside the free tier. Once the
account's 100 GB of free egress (data downloaded from AWS to the internet) is used up, one 10-month
batch from the laptop costs ≈$0.26 in egress. That is more than a year of monthly ECS runs.

## 6. Reliability

| | Laptop | ECS |
|---|---|---|
| What happened in testing | the 4 measured runs succeeded; an earlier attempt at run 4 crashed Docker Desktop (memory) | all 7 tasks succeeded (runs 1–5, the Spot test, the network probe) |
| Needs the starting machine during the run | yes: sleep, closing the lid, a Wi-Fi drop, or a Docker Desktop update or reboot stops the run | no: the task runs in AWS, and the laptop can disconnect (`-NoWait` returns at once) |
| Time exposed to interruption, 10-month batch | 61 min | 5 min |
| Platform interruptions | – | only with `-Spot`: AWS can reclaim the capacity with 2 minutes' warning; the entrypoint passes the stop signal to dbt |
| After a failure | each model commits atomically, so every table stays at its last snapshot. A re-run loads only the files that were not committed | same |
| Automatic retry | none | none (no service or schedule, by design) |

## 7. Security

| | Laptop | ECS |
|---|---|---|
| Credentials | long-lived access key of an IAM user with `AdministratorAccess`, read from `~/.aws` | task role: ECS issues temporary credentials and rotates them; nothing is stored in the image |
| What the job is allowed to touch (IAM) | everything in the account | read `landing/`; read/write only `bronze-duck/`, `silver-duck/`, `gold-duck/`; change Glue tables only in `nyc_*_duck` |
| Protection against deleting the wrong files | the plugin's purge allow-list (code only) | the same allow-list, plus IAM |
| Network exposure | behind the home router | public IP in a public subnet; the security group allows no inbound traffic |
| Data in transit | HTTPS over the internet | HTTPS, staying on the AWS network inside the region |
| Audit trail (CloudTrail) | calls appear as the IAM user | calls appear as the task role, per task |
| Image vulnerability scanning | – | basic scan on push (free) ran on every push; the image in use has 25 findings: 3 critical, 14 high, 6 medium, 2 low |

* **Image scan findings.** All 25 findings are in Debian packages that come with the
  `python:3.12-slim-bookworm` base image:
  * perl: 12, including all 3 critical ones;
  * util-linux: 6;
  * glibc: 3;
  * gcc-12 runtime libraries: 2;
  * pcre2 and zlib: 1 each.

  The pipeline does not call perl or util-linux itself. Basic scanning checks only OS packages, not
  Python packages such as dbt or DuckDB. To pick up Debian's security fixes, rebuild on a fresh base
  image from time to time (`docker build --pull`).
* **Where to find the scan results.** `latest` points to a multi-platform image index, and ECR stores
  scan results under the digest of the platform image inside it. So
  `aws ecr describe-image-scan-findings --image-id imageTag=latest` answers "scan not found". Query
  by the platform image's digest instead, or open the image in the ECR console.
* **Laptop credentials.** For safer laptop runs, use a named profile that assumes a role with the
  task role's permissions (`-e AWS_PROFILE=<name>`) instead of the admin user's key.

## 8. Operations

| | Laptop | ECS |
|---|---|---|
| One-time setup | Docker Desktop, AWS credentials, `docker build` | `scripts/deploy_ecs.ps1` (a CloudFormation stack with ECR, the cluster, task definition, roles, security group and log group), then `scripts/push_to_ecr.ps1` |
| Start a run | `docker run --rm -v "$env:USERPROFILE\.aws:/home/dbt/.aws:ro" nyc-taxi-duckdb-dbt` | `.\scripts\run_on_ecs.ps1` |
| Choose what to run | dbt arguments after the image name | `-DbtArgs build, --select, gold` |
| Ship a code change | nothing if the project folder is mounted; otherwise a cached rebuild takes seconds | `push_to_ecr.ps1`: 26 s for a code-only change (rebuild + push of the changed layers). The first push of the full 240 MB image took 232 s over this connection |
| Change resources | Docker Desktop settings, up to the 15.7 GiB of the host | `deploy_ecs.ps1 -TaskCpu ... -TaskMemory ...`; Spot with `-Spot` |
| Scheduling | none; would need Windows Task Scheduler and a laptop that is switched on | none, by design; EventBridge Scheduler could start the task |
| Two runs at once | nothing prevents it, so avoid it: commits to the same table conflict | same |
| Leftovers | `--rm` removes the container | the task stops by itself; nothing keeps running or billing |

## 9. Development and debugging

| | Laptop | ECS |
|---|---|---|
| Edit → run loop | edit the SQL and run again; the project folder is mounted, so no rebuild | push the image (26 s), then wait 23–26 s before dbt starts |
| Small test runs | `--target dev` with a 100K-row sample: a full build with all tests took 78 s; the re-delivery test took 99 s | possible with the same image, not measured; start-up and teardown add ≈50 s |
| Look inside a running job | `docker exec` or `docker run ... sh`; inspect `/tmp/dbt`; `docker stats` | logs only; ECS Exec is not enabled |
| dbt artifacts (`target/`, `logs/`) | in the container's `/tmp/dbt`; lost with `--rm` unless you mount a folder | lost when the task stops |

## 10. Logs and monitoring

| | Laptop | ECS |
|---|---|---|
| Run log | in the terminal; gone unless redirected to a file | CloudWatch Logs `/ecs/nyc-taxi-duckdb-dbt`, one stream per run, kept 30 days (≈14 KB per full run) |
| Resource summary | the `run_summary` line (CPU, peak memory, network) | the same line, in CloudWatch |
| Timings and exit status | in the terminal | `aws ecs describe-tasks`: create, pull, start and stop times, exit code, stop reason; available for about an hour after the task stops |
| Alerts | none | none configured; without a schedule, whoever starts a run watches it |

## 11. Scaling and limits

| | Laptop | ECS |
|---|---|---|
| Largest CPU / memory | 8 threads; the Docker VM can grow toward the host's 15.7 GiB | 16 vCPU / 120 GiB per Fargate task |
| Local disk for DuckDB spill | 1 TB SSD, shared with everything else | 20 GiB by default, up to 200 GiB |
| Network | fixed by the home line: ≈3 MB/s down, ≈0.8 MB/s up | hundreds of MB/s |
| What a bigger load hits first | upload: each GB written takes ≈21 minutes | CPU and memory: resize the task |

As a rough projection (not measured): the full-year load writes 2.4 GB, which is about 50 minutes of
uploading alone on the laptop. ECS finished the whole run in 4 min 20 s.

## 12. Results: the output is the same

| Table | Laptop | ECS |
|---|---:|---:|
| `bronze_yellow_tripdata` | 38,310,226 | 38,310,226 |
| `silver_yellow_trips` | 37,038,677 | 37,038,677 |
| `silver_yellow_trips_rejected` | 1,271,549 | 1,271,549 |
| `fact_trips` | 37,038,677 | 37,038,677 |
| `fact_daily_zone_summary`, sum of `trip_count` | 37,038,677 | 37,038,677 |
| Data tests | 48 of 48 pass | 48 of 48 pass |

New-row counts also matched run by run. For example, run 4 added 31,212,868 silver rows and
1,116,637 rejected rows in both places. Athena (engine v3) read the tables from both builds.

Storage after runs 1–4 (the same sequence in both places):

| Table | Laptop: data files / MB | ECS: data files / MB | Difference |
|---|---:|---:|---:|
| bronze | 12 / 626.5 | 12 / 626.5 | 0.0% |
| silver | 12 / 759.0 | 12 / 770.8 | +1.6% |
| rejected | 12 / 33.4 | 12 / 34.0 | +1.9% |
| `fact_trips` | 12 / 873.3 | 12 / 886.5 | +1.5% |
| `fact_daily_zone_summary` | 3 / 2.57 | 3 / 2.51 | −2.0% |

The rows are identical, but file sizes differ by up to 2%. Rows land in a different order inside each
file, because DuckDB runs with `preserve_insertion_order = false`, and the order changes how well
Parquet compresses.

## 13. Recommendations

1. **Run real loads on ECS** with `.\scripts\run_on_ecs.ps1`. It is 10–12× faster, has no internet
   egress, uses a least-privilege role and keeps the logs.
2. **Use `-Spot` for routine runs.** It is up to 70% cheaper, and a re-run after an interruption is
   safe.
3. **Keep 4 vCPU / 16 GiB for full loads and `--full-refresh`** (peak 10.7 GiB). Monthly runs peaked
   at 2.1 GiB, so 2 vCPU / 8 GiB should be enough for them, but that size is not tested.
4. **Develop on the laptop** with `--target dev` and a small sample. Push to ECR when a change is
   ready.
5. **Stop using the admin access key for laptop runs.** Use a profile with the task role's
   permissions instead.
6. **Rebuild on a fresh base image now and then.** The current image has 25 known vulnerabilities in
   Debian base packages, 3 of them critical (in perl). Build with `docker build --pull`, push, and
   check the new scan.
7. **For tiny one-off runs, the laptop is just as good.** For a single dimension or a no-op check, the
   ECS start-up time (23–26 s) outweighs the work.

## 14. Methodology and caveats

**How each number was collected**

* **Run times:** dbt's own log in both places: the console output on the laptop, CloudWatch Logs on
  ECS.
* **ECS phases and billing:** `createdAt`, `pullStartedAt`, `pullStoppedAt`, `startedAt`,
  `executionStoppedAt` and `stoppedAt` from `aws ecs describe-tasks`.
* **ECS resources:** the `run_summary` line the container prints at the end:
  * CPU time from the cgroup;
  * process memory without page cache, sampled every 5 s;
  * network bytes from `/proc/net/dev`.
* **Laptop resources:** `docker stats` samples taken by hand during the runs, because the
  `run_summary` line did not exist yet. Its memory figure includes part of the page cache, so it is
  not exactly the same measure as on ECS.
* **Laptop start-to-finish times:** dbt time plus ≈6 s for container start and dbt parsing (the
  overhead seen inside the ECS container). They were not timed separately.
* **Phase split ([Section 2](#where-the-time-goes-run-4)):** log timestamps (1-second resolution) of
  the model start, the "N new rows" line and the model end. The size written per model is estimated
  from the final table sizes, using each run's share of the rows.
* **Network probe:** one read-only boto3 script, run on the laptop's host OS and as an ECS task with
  the task role, a few hours after the laptop runs. A boto3 `download_file` test (16 streams writing
  to local disk) measured only 4.5 MB/s on Fargate, far below the 63 MB/s of a single stream. Its
  cause was not investigated, so that number is not used. DuckDB reads into memory, like the
  12-stream test.

**Caveats**

* **Earlier build for laptop runs 1–2.** The reference CSVs were dbt seeds (4 extra nodes), new rows
  were staged in temporary tables, and DuckDB used its default memory cap.
* **Partial log for laptop run 1.** Only part of the log was kept, so its bronze, silver and rejected
  times come from the Iceberg commit timestamps.
* **ECS run 1 predates the Fargate memory fix.** Its DuckDB cap was 11.6 GiB instead of 9.6 GiB, but
  it used only 2.1 GiB. None of these caveats changes the comparison.
* **One run per scenario.** Nothing was repeated, so differences under about 20% are within normal
  variation. Home internet speed also changes over the day.
* **The Fargate CPU model is chosen by AWS** and can differ between tasks. One Spot task saw 8 CPUs
  instead of 4, and DuckDB sizes its thread pool from the CPUs it sees.
* **Costs are list prices.** Spot prices change. The free-tier figures assume nothing else in the
  account uses the free egress.

**Reproducing a run**

```powershell
# Laptop (run 4 also had -e DBT_THREADS=2)
docker run --rm -v "$env:USERPROFILE\.aws:/home/dbt/.aws:ro" `
  -e LANDING_GLOB="s3://nyc-test-iceberg/landing/yellow_taxi/yellow_tripdata_2023_01.parquet" `
  nyc-taxi-duckdb-dbt build

# ECS (run 4 also had DBT_THREADS = "2")
.\scripts\run_on_ecs.ps1 -Environment @{ LANDING_GLOB = "s3://nyc-test-iceberg/landing/yellow_taxi/yellow_tripdata_2023_01.parquet" }
```

**ECS task IDs**

The logs are in log group `/ecs/nyc-taxi-duckdb-dbt`, stream `run/dbt/<task id>`, until 2026-11-04.

| Run | Task ID |
|---|---|
| 1 | `70229490c15b40e5b73a3e6d27590281` |
| 2 | `491a1b96b7a4402e942d8a64018767ba` |
| 3 | `9ec7176dd6f74d649809933db590d3c6` |
| 4 | `3119d7fa115a4ee780ed87d66b2526af` |
| 5 | `48532692c3b8442abeee24ffe090d6f4` |
| Spot test | `5ad6d03023ed46b7920bbef54f9d40b2` |
| Network probe | `987dc20b50214929841976ffe2fbae26` |
