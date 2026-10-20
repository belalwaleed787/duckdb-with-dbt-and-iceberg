# Run the pipeline from the AWS console

This guide runs the whole pipeline with the AWS web console, without PowerShell or the AWS CLI.
It covers:

1. the one-time setup;
2. adding a new month of data;
3. starting a run, watching it and reading its result;
4. querying the tables;
5. everyday changes and clean-up.

Each step does the same thing as one of the scripts in `scripts/`; the [last section](#console-vs-scripts)
maps them. There is **no schedule**: the pipeline runs only when someone starts it.

A run costs about $0.005 for one new month and $0.02 for a full year (4 vCPU / 16 GiB on Fargate).
Nothing keeps running or billing between runs.

## Before you start

* **Region.** Everything is in **US East (N. Virginia), `us-east-1`**. Pick it in the region menu at
  the top right of the console. In any other region the resources look missing.
* **Access.** Your user needs access to CloudFormation, ECS, ECR, CloudWatch Logs, S3, Glue and
  Athena.
* **Already set up?** If CloudFormation shows the stack `nyc-taxi-duckdb-dbt` with status
  `CREATE_COMPLETE`, and ECR shows the image `nyc-taxi-duckdb-dbt:latest`, skip to [Add new data](#2-add-new-data-optional).

The values the forms ask for are in **CloudFormation → Stacks → `nyc-taxi-duckdb-dbt` → Outputs**:

| Output | Value | Used in |
|---|---|---|
| `ClusterName` | `nyc-taxi-duckdb-dbt` | ECS |
| `TaskDefinition` | `nyc-taxi-duckdb-dbt` (family) | ECS → Run new task |
| `ContainerName` | `dbt` | container overrides |
| `SecurityGroupId` | `sg-…` (the group named `nyc-taxi-duckdb-dbt-task`) | networking |
| `SubnetIds` | the default subnets in us-east-1a, 1b, 1c, 1d, 1f | networking |
| `LogGroupName` | `/ecs/nyc-taxi-duckdb-dbt` | CloudWatch |
| `RepositoryUri` | `<account-id>.dkr.ecr.us-east-1.amazonaws.com/nyc-taxi-duckdb-dbt` | ECR |

## 1. One-time setup

Skip this part if the stack already exists.

### 1.1 Create the stack (CloudFormation)

1. Open **CloudFormation → Stacks → Create stack → With new resources (standard)**.
2. Choose **Choose an existing template → Upload a template file**. Select `infra/ecs-stack.yaml`
   from this repository, then **Next**.
3. Fill in the stack details:
   * **Stack name:** `nyc-taxi-duckdb-dbt`.
   * Keep the defaults for `ProjectName`, `BucketName`, `ImageTag` (`latest`), `TaskCpu` (`4096`),
     `TaskMemory` (`16384`) and `LogRetentionDays` (`30`).
   * **VpcId:** the default VPC.
   * **SubnetIds:** the default VPC's subnets, **except** the one in Availability Zone ID `use1-az3`
     (the zone name it maps to, such as us-east-1e, differs between accounts). Fargate does not run
     there. The **VPC → Subnets** page shows the *Availability Zone ID* column.
4. **Next**. Leave the stack options as they are, then **Next**.
5. On the review page, tick **"I acknowledge that AWS CloudFormation might create IAM resources with
   custom names"**, then **Submit**.
6. Wait for `CREATE_COMPLETE`, which takes about 2–3 minutes. The **Outputs** tab then shows the
   values from the table above.

If creation fails with **"Unable to assume the service linked role"**, the account has never used
ECS before:

1. Open **CloudShell** (the terminal icon in the console's top bar) and run:
   `aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com`
2. Delete the failed stack. A stack in `ROLLBACK_COMPLETE` cannot be updated.
3. Create it again.

This account already has that role.

### 1.2 Put the image in ECR

The console cannot build a container image; that needs Docker on a computer.

* **From this repository on Windows:** run `.\scripts\push_to_ecr.ps1`.
* **Any other way:** open **ECR → Private repositories → `nyc-taxi-duckdb-dbt` → View push commands**,
  which shows the login, build, tag and push commands. Add `--platform linux/amd64` to the
  `docker build` command, because the task runs on x86.

To check the push, open **ECR → `nyc-taxi-duckdb-dbt`**: an image tagged `latest` should be listed
(about 240 MB). Every run pulls `latest` when it starts, so pushing a new image changes the next run.

## 2. Add new data (optional)

1. Open **S3 → `nyc-test-iceberg` → `landing/` → `yellow_taxi/`**.
2. Click **Upload**, add the Parquet file, then **Upload**.

The file name must **end with the year and month**, like `yellow_tripdata_2024_01.parquet` (an
underscore or a hyphen both work). Bronze reads the month from the name. With any other name, every
row is rejected with the reason `unknown_source_period`.

* **The next run picks the file up by itself.** It loads only files that are new or changed since
  the last run, so you don't need to tell the run which file to load.
* **Re-uploading a corrected file is safe.** It replaces that file's rows in every layer.

## 3. Run the pipeline

### 3.1 Start a run (ECS)

1. Open **ECS → Clusters → `nyc-taxi-duckdb-dbt` → Tasks tab → Run new task**.
2. Fill in the form as follows. Leave everything else at its default.

| Section | Field | Value |
|---|---|---|
| Compute configuration | Compute options | **Launch type** |
| | Launch type / Platform version | `FARGATE` / `LATEST` |
| Deployment configuration | Application type | **Task** (not *Service*, see below) |
| | Task definition: Family / Revision | `nyc-taxi-duckdb-dbt` / latest |
| | Desired tasks | **1** |
| Networking | VPC | the default VPC |
| | Subnets | the subnets from the stack's `SubnetIds` output (the default selection is fine if it excludes us-east-1e) |
| | Security group | **Use an existing security group** → `nyc-taxi-duckdb-dbt-task` (remove `default`) |
| | Public IP | **Turned on** |
| Container overrides (optional) | `dbt` → Command override, Environment variables | see [3.2](#32-choose-what-to-run) |

3. Click **Create**. The task appears in the Tasks tab.

**Three settings that matter:**

* **Application type must be Task.** A *Service* restarts the container every time it finishes. That
  reruns the pipeline in a loop and keeps billing. For the same reason, don't use the cluster's
  *Scheduled tasks* tab: the pipeline is meant to run only on demand.
* **Public IP must be turned on.** The task reaches S3, Glue, ECR and CloudWatch over the internet;
  there is no NAT gateway. Without a public IP it cannot even pull the image.
* **Desired tasks must be 1.** Two runs at the same time would try to commit to the same tables, and
  one of them would fail.

### 3.2 Choose what to run

Expand **Container overrides → `dbt`**.

* **Command override:** leave it **empty** to run the whole pipeline (`dbt build`: all models and all
  48 tests). Otherwise, type the dbt arguments **separated by commas, without spaces**.
* **Environment variables:** add key/value pairs to change settings for this run only.

| To do this | Command override | Environment variables |
|---|---|---|
| Normal run: load new files, rebuild what depends on them, run all tests | *(empty)* | – |
| Rebuild only the gold layer | `build,--select,gold` | – |
| Rebuild silver and everything after it | `build,--select,silver_yellow_trips+` | – |
| Run two specific models | `build,--select,bronze_yellow_tripdata,fact_trips` | – |
| Run only the tests | `test` | – |
| Rebuild every table from scratch | `build,--full-refresh` | – |
| Load only some landing files | *(empty)* | `LANDING_GLOB` = `s3://nyc-test-iceberg/landing/yellow_taxi/yellow_tripdata_2023_0[12].parquet` |
| Use less memory (2 models at a time) | *(empty)* | `DBT_THREADS` = `2` |
| Set DuckDB's memory cap yourself | *(empty)* | `DUCKDB_MEMORY_LIMIT` = `8GB` |

The `dev` target (`--target dev`) does **not** work on ECS. The task role may only write the three
production folders, so use `dev` from a laptop only.

**To run on Fargate Spot** (up to 70% cheaper, but AWS can interrupt it), change the compute options:

1. Under **Compute options**, choose **Capacity provider strategy → Use custom (Advanced)**.
2. Add the capacity provider **`FARGATE_SPOT`** with base `0` and weight `1`, and remove any other
   provider.

Re-running after an interruption is safe.

### 3.3 Watch the run

Click the task ID in the **Tasks** tab. The status moves through:

| Status | Meaning | Typical time |
|---|---|---|
| `PROVISIONING` → `PENDING` | Fargate finds capacity and pulls the 240 MB image | 20–25 s |
| `RUNNING` | dbt is working | 35 s for one month, 4–5 min for a full year |
| `DEPROVISIONING` → `STOPPED` | the container has exited; the data is already committed | 20–25 s |

* **Logs:** the task page's **Logs** tab shows the dbt output. **View in CloudWatch** opens the full
  log stream, `run/dbt/<task id>` in the log group `/ecs/nyc-taxi-duckdb-dbt`.
* **Result:** when the task is `STOPPED`, look at **Containers → `dbt` → Exit code**:

| Exit code | Meaning | What to do |
|---|---|---|
| `0` | success: every model built and every test passed | nothing |
| `1` | dbt reported an error: a model failed or a test found bad data | search the log for `ERROR` or `FAIL`; the log names the model or test |
| `137` | the container was killed, usually out of memory (stopped reason mentions `OutOfMemory`) | run with `DBT_THREADS` = `2`, or make the task bigger ([5.2](#52-change-the-task-size)) |

### 3.4 Read the run summary

The last log line of every run is a `run_summary` record:

```json
run_summary {"exit_code": 0, "elapsed_s": 267, "cpu_s": 650, "cpus": 4, "container_memory_mib": 16384,
"duckdb_memory_limit": "9830MiB", "peak_memory_mib": 10436, "network_in_mib": 3421,
"network_out_mib": 2300, "dbt_threads": "4"}
```

To compare runs:

1. Open **CloudWatch → Logs Insights** and select the log group `/ecs/nyc-taxi-duckdb-dbt`.
2. Run this query:

```
fields @timestamp, @logStream, @message
| filter @message like /run_summary/
| sort @timestamp desc
| limit 20
```

## 4. Look at the results

### Athena: query the tables

1. Open **Athena → Query editor**.
2. **First time only:** Athena asks for a result location. Open **Settings → Manage** and set
   `s3://nyc-test-iceberg/athena-results/console/`, then **Save**. The `primary` workgroup has no
   default.
3. Choose **Data source** `AwsDataCatalog` and **Database** `nyc_gold_duck`, then run a query:

```sql
select d.month_name, l.borough, count(*) as trips, round(sum(f.total_amount), 2) as revenue
from nyc_gold_duck.fact_trips f
join nyc_gold_duck.dim_date d on f.pickup_date_key = d.date_key
join nyc_gold_duck.dim_location l on f.pickup_location_id = l.location_id
group by 1, 2
order by revenue desc;
```

Athena charges $5 per TB scanned; queries on the gold tables scan well under 1 GB.

Other tables to query:

* **`nyc_silver_duck.silver_yellow_trips_rejected`:** why rows were dropped. Group by
  `_reject_reason`.
* **`nyc_bronze_duck.bronze_yellow_tripdata`:** the raw rows plus which file and run they came from.

### Glue and S3: see what was built

* **Glue → Data Catalog → Databases:** `nyc_bronze_duck`, `nyc_silver_duck` and `nyc_gold_duck`
  hold the tables, with their columns and the *Apache Iceberg* table format. View them, but **don't
  edit or delete tables here**. The pipeline manages them, and editing them by hand breaks the
  Iceberg metadata.
* **S3 → `nyc-test-iceberg`:** the files are under `bronze-duck/`, `silver-duck/` and `gold-duck/`.
  Each table has a `data/` folder of Parquet files and a `metadata/` folder.

Other things in the same bucket and catalog belong to other pipelines. **Don't change** `landing/`,
`warehouse/`, or the `nyc_*_db` Glue databases.

## 5. Everyday changes

### 5.1 Stop a run

Go to **ECS → cluster → Tasks**, tick the task, then **Stop → Stop selected**. This is safe:

* every table stays at its last committed version;
* the next run loads whatever was not committed.

### 5.2 Change the task size

1. Open **CloudFormation → `nyc-taxi-duckdb-dbt` → Update stack → Make a direct update → Use
   existing template**, then **Next**.
2. Change `TaskCpu` and `TaskMemory`. Fargate accepts only certain combinations; common ones:

| Size | `TaskCpu` | `TaskMemory` | Good for |
|---|---|---|---|
| 2 vCPU / 8 GiB | `2048` | `8192` | monthly runs (peak memory was about 2 GiB; not tested at this size) |
| 4 vCPU / 16 GiB | `4096` | `16384` | full loads and `--full-refresh` (default; full year peaked at 10.2 GiB) |
| 8 vCPU / 32 GiB | `8192` | `32768` | much more data |

3. **Next → Next**, tick the IAM acknowledgement, then **Submit**.

The update creates a new task-definition revision. **Run new task** uses the latest revision by
default.

### 5.3 Deploy new code

Push a new image ([1.2](#12-put-the-image-in-ecr)). Nothing else changes, because every run pulls
`latest` when it starts.

## 6. Remove everything

1. **CloudFormation → `nyc-taxi-duckdb-dbt` → Delete**. This removes the ECR repository and its
   images, the cluster, the task definition, the IAM roles, the security group and the log group.
2. The data is **not** part of the stack and stays. To delete it too:
   * **S3:** delete the folders `bronze-duck/`, `silver-duck/` and `gold-duck/` (and `dev/` if it
     exists).
   * **Glue:** delete the databases `nyc_bronze_duck`, `nyc_silver_duck` and `nyc_gold_duck`.

   Leave `landing/` alone: it is the raw input.

## Troubleshooting

| What you see | Cause | Fix |
|---|---|---|
| The stack, cluster or tables seem to be missing | the console is in another region | switch to US East (N. Virginia) |
| Task stops right away: `CannotPullContainerError` or `ResourceInitializationError` | no public IP, or no image tagged `latest` | turn **Public IP** on; check ECR for the `latest` image |
| Error about an Availability Zone or "no Fargate configuration" when starting a task | a subnet in `use1-az3` (us-east-1e) is selected | remove that subnet |
| Stack creation fails: "Unable to assume the service linked role" | the ECS service-linked role does not exist yet | see [1.1](#11-create-the-stack-cloudformation) |
| Exit code `1`, log says `Columns of ... changed` and names a command | an incremental model's columns changed | run with Command override `build,--full-refresh,--select,<model>` |
| Exit code `1` with `AccessDenied` | the run tried to write outside the three `*-duck/` folders (for example `--target dev`) | run `dev` from a laptop only |
| Exit code `137` / `OutOfMemory` | not enough memory for the run | `DBT_THREADS` = `2`, or a bigger task |
| Stopped reason mentions a Spot interruption | AWS took back Fargate Spot capacity | run again (safe), or use Launch type FARGATE |
| A new landing file is not loaded | its name does not end with `YYYY_MM.parquet` | rename it in S3 and run again |
| Athena: "No output location provided" | no query result location set | see [section 4](#athena-query-the-tables) |

## Console vs scripts

| Script | Same thing in the console |
|---|---|
| `scripts/deploy_ecs.ps1` | CloudFormation → Create stack / Update stack |
| `scripts/push_to_ecr.ps1` | no console equivalent (needs Docker); ECR → View push commands |
| `scripts/run_on_ecs.ps1` | ECS → cluster → Tasks → Run new task |
| `run_on_ecs.ps1 -DbtArgs build, --select, gold` | Container overrides → Command override `build,--select,gold` |
| `run_on_ecs.ps1 -Environment @{ DBT_THREADS = "2" }` | Container overrides → Environment variables |
| `run_on_ecs.ps1 -Spot` | Compute options → Capacity provider strategy → `FARGATE_SPOT` |
| the logs the script prints | task page → Logs tab, or CloudWatch → `/ecs/nyc-taxi-duckdb-dbt` |
| the exit code the script prints | task page → Containers → Exit code |
