# Batch MapReduce Clickstream Log Analysis

Mini project: store raw e-commerce clickstream logs on **HDFS**, run a **MapReduce** pipeline on **YARN** every night, and publish three reports on a **batch dashboard**:

| Report | Question it answers |
|---|---|
| **Peak-traffic hour** | Which hour of the day carries the most events? |
| **Top-10 pages** | Which pages (product pages) get the most hits? |
| **Error rate** | What share of log records is invalid, per day and per reason? |

```
 web/app exports            HDFS (raw zone)                    YARN queue "batch"                 local
 daily CSV  ──ingest──►  /clickstream/raw/dt=YYYY-MM-DD/  ──►  ┌ PeakHourJob  ┐
 (landing dir)           (128 MB blocks, never modified)       ├ TopPages 1+2 ├──► /clickstream/out/dt=…
                                                               └ ErrorRateJob ┘            │ getmerge
        cron 01:30 ── scripts/run_nightly.sh ─────────────────────────────────────────────┘
                                                                  reports/<date>/*.tsv ──► dashboard/index.html
```

## 1. Dataset

**Kaggle – "eCommerce behavior data from multi category store"** (REES46 / mkechinov):
<https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>

`2019-Oct.csv` (≈ 42 M events) and `2019-Nov.csv` (≈ 67 M events) – several GB each – 9 columns:

`event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`
(`event_type` ∈ `view | cart | remove_from_cart | purchase`; time format `2019-10-01 00:00:04 UTC`)

```bash
pip install kaggle                       # + put your API token in ~/.kaggle/kaggle.json
scripts/get_kaggle_dataset.sh 5000000    # downloads both files; optional arg = also write a 5M-line sample
python3 scripts/split_by_day.py data/kaggle/2019-Oct.csv data/daily   # simulate the nightly daily drops
```

**No internet / quick demo?** `python3 scripts/generate_sample_data.py --rows 500000` creates a synthetic file with the **same schema** (evening traffic peak, a few hot products, ~1.5 % injected bad rows).

### Important: how the problem statement maps onto this dataset
The Kaggle file is a *product-event* export. It has **no page URL and no HTTP status code**, so:

* **"Page"** = the product detail page. Top-10 pages = the 10 products with the most `view` events (change with `-Dclickstream.top.event=purchase` to rank by purchases).
* **"Error"** = a malformed/invalid log record (data-quality error rate of the logging pipeline). A record is bad if the *first* failing check is: wrong field count → bad timestamp → unknown event type → non-numeric product id → non-numeric user id → price missing/≤ 0. Each bad record is counted once, under one reason.
* For raw web-server logs with a status column you would only change the validation in `ClickEvent.java` to `status >= 400`; the job structure stays the same.

Mention this mapping in your viva/report – it is a deliberate, documented design decision.

## 2. Project layout

```
pom.xml                               Maven build -> target/clickstream-mr.jar
src/main/java/com/clickstream/
  ClickEvent.java                     CSV line parser + validation rules
  PeakHourJob.java                    map: hour -> 1 | combiner: sum | 1 reducer: totals + PEAK_HOUR
  TopPagesJob.java                    stage 1: count per product | stage 2: distributed Top-N
  ErrorRateJob.java                   per-day total/errors/rate + per-reason counts
  ClickstreamDriver.java              ToolRunner entry point; submits the jobs to YARN concurrently
scripts/
  get_kaggle_dataset.sh               download from Kaggle
  generate_sample_data.py             synthetic data, same schema
  split_by_day.py                     month file -> one file per day
  hdfs_ingest.sh                      local CSV -> /clickstream/raw/dt=<date>/
  run_nightly.sh                      the nightly batch (ingest -> YARN jobs -> reports -> dashboard)
  run_local.sh                        same pipeline on one machine, no cluster
  verify_results.py                   independent plain-Python recompute + comparison
  build_dashboard.py                  reports/*  ->  dashboard/index.html
config/
  pipeline.env                        paths, queue, reducers (env-overridable)
  capacity-scheduler-batch-queue.xml  YARN "batch" queue
  crontab.example                     01:30 nightly schedule
reports/<date>/                       peak_hour.tsv, top_pages.tsv, error_rate.tsv, run_info.json
dashboard/index.html                  self-contained dashboard (no server / internet needed)
docs/PROJECT_REPORT.md                report skeleton for submission
```

`reports/2019-10-01…03` and `dashboard/index.html` are **sample output on synthetic data** so you can see the result immediately – open `dashboard/index.html` in a browser.

## 3. Prerequisites

Java 8+ (JDK), Maven 3.6+, Python 3.8+, Hadoop 3.x (pseudo-distributed on one machine is enough: HDFS + YARN running; `jps` should list NameNode, DataNode, ResourceManager, NodeManager).

```bash
mvn -q -DskipTests package        # -> target/clickstream-mr.jar
```
If your lab cluster runs a different Hadoop 3.x, change `hadoop.version` in `pom.xml` (the Hadoop jars are `provided`, so the cluster's own are used at run time).

## 4. Run it

### A. One machine, no cluster (fastest check)
```bash
scripts/run_local.sh                               # generates a sample file, runs the 3 jobs in Hadoop local mode
scripts/run_local.sh data/kaggle_sample.csv 2019-10-01
```
It also runs `verify_results.py` and rebuilds the dashboard.

### B. HDFS + YARN (the real pipeline)
```bash
# 1. YARN batch queue (once): merge config/capacity-scheduler-batch-queue.xml, then
yarn rmadmin -refreshQueues

# 2. put one day of logs into HDFS
hdfs dfs -mkdir -p /clickstream/raw
scripts/hdfs_ingest.sh data/daily/2019-10-01.csv 2019-10-01     # -> /clickstream/raw/dt=2019-10-01/

# 3. run the batch for that day (jobs run on YARN, queue "batch")
scripts/run_nightly.sh 2019-10-01
#    watch: http://localhost:8088  or  yarn application -list

# 4. results
ls reports/2019-10-01/        # peak_hour.tsv  top_pages.tsv  error_rate.tsv
# open dashboard/index.html
```
Run it for several days (`2019-10-02`, …) and the dashboard's run selector and history table fill up.

### C. Nightly schedule
```bash
crontab -e      # paste config/crontab.example (fix the paths) -> every night 01:30 processes *yesterday's* partition
```
`run_nightly.sh` uses a lock (`flock`) so a slow run is never overlapped by the next one, and exits non-zero on failure (see `logs/cron.log`). Back-fill any missed day with `scripts/run_nightly.sh YYYY-MM-DD`.

### Running a job by hand
```bash
hadoop jar target/clickstream-mr.jar -Dmapreduce.job.queuename=batch -Dmapreduce.job.reduces=8 \
    all /clickstream/raw/dt=2019-10-01 /clickstream/out/dt=2019-10-01
# or a single report: peakhour | toppages | errorrate
```

### D. Hadoop in Docker (no Hadoop install) - recommended for a laptop
Needs only Docker (Docker Desktop on Windows/macOS, give it ≥ 6 GB RAM) and, to download the data, a Kaggle account. On Windows run the commands in WSL2 or Git Bash.
```bash
# 1. build the jar without installing Java/Maven
docker run --rm -v "$PWD":/app -w /app maven:3.9-eclipse-temurin-8 mvn -q -DskipTests package

# 2. data: download from Kaggle in the browser (Download -> unzip into data/kaggle/), then cut a laptop-sized piece
head -n 5000000 data/kaggle/2019-Oct.csv > data/kaggle_sample.csv        # ~3-4 days of events
python3 scripts/split_by_day.py data/kaggle_sample.csv data/daily         # one file per day
#    (no Kaggle account yet?  python3 scripts/generate_sample_data.py   then split the same way)

# 3. start HDFS + YARN (1 NameNode, 1 DataNode, ResourceManager, NodeManager)
docker compose up -d
#    UIs: HDFS http://localhost:9870   YARN http://localhost:8088

# 4. run the pipeline INSIDE the container (project folder is mounted at /project)
docker compose exec resourcemanager bash
  hdfs dfsadmin -safemode wait
  cd /project
  for d in 2019-10-01 2019-10-02 2019-10-03; do
      scripts/hdfs_ingest.sh data/daily/$d.csv $d
      scripts/run_nightly.sh $d
  done
  exit

# 5. dashboard (on your computer, python3) -> open dashboard/index.html
python3 scripts/build_dashboard.py --reports reports --out dashboard/index.html
python3 scripts/verify_results.py --input data/daily/2019-10-01.csv --reports reports/2019-10-01

docker compose down          # stop (add -v to delete the HDFS data)
```
Notes: the compose file defines the `batch` YARN queue from `docker/hadoop.env`, so `run_nightly.sh` works unchanged. If a job sits in *ACCEPTED* in the YARN UI, the NodeManager is out of memory - raise Docker's memory or lower the `4096` in `docker/hadoop.env`. On Linux, if writing to `reports/` is "permission denied", run `chmod -R a+w reports dashboard logs` on the host. These Docker files were written from the official `apache/hadoop` image conventions but **not run** in the build sandbox.

## 5. Design notes (good viva material)

* **Combiners** – all three jobs pre-aggregate in the map task (`LongSumReducer`, `VectorCombiner`), so only a few hundred bytes per key cross the network instead of one record per click.
* **Top-N pattern** – stage 1 counts per product with many reducers (scales out). Stage 2: every mapper keeps only its local top-10 in a bounded heap and emits it in `cleanup()`; one reducer merges those tiny lists. Correct because each product appears in exactly one stage-1 output line.
* **Peak hour / error rate** have ≤ 24 / a few dozen keys, so a single reducer is fine and can also compute the maximum / the rate.
* **Concurrent YARN jobs** – the driver `submit()`s the three independent jobs together; the CapacityScheduler shares the `batch` queue between them. Stage 2 of Top-N starts when stage 1 ends.
* **HDFS** – raw data is write-once in date partitions (`dt=…`), 128 MB blocks → one map task per block; replication 1 for a lab, 3 in production. Reruns are idempotent (output dirs are deleted first, `-Dclickstream.overwrite=false` to disable).
* **Bad data never kills a job** – invalid records are counted (Hadoop counters + error report), not thrown.

Tuning for big input: raise `REDUCERS`, set `mapreduce.map.memory.mb` / `mapreduce.reduce.memory.mb` to fit your NodeManager, and enable map-output compression (`-Dmapreduce.map.output.compress=true`).

## 6. Verification status (read this)

What was actually tested while building this project, and what was not:

* ✅ All Java sources compile (Java 8 target).
* ✅ The full pipeline logic (parser, mappers, combiners, reducers, Top-N, the driver and its concurrent submission order) ran end to end on **synthetic data** (≈ 300 k rows) and the three reports matched `verify_results.py`, an independent plain-Python implementation of the same rules (all checks PASS for the 3 simulated nightly runs).
* ✅ `run_nightly.sh`, `hdfs_ingest.sh`, `split_by_day.py`, `build_dashboard.py` ran against a local stand-in for the `hadoop`/`hdfs` commands; the dashboard script ran without errors against a fake DOM.
* ⚠️ **Not tested:** a real Hadoop/YARN cluster, the real Kaggle files, `mvn package` (the build sandbox had no internet access to download Maven dependencies), and `run_local.sh`. The jobs use only standard Hadoop 3 `mapreduce` APIs, so they should behave the same, but run **section 4A first** on your machine and fix any version/path issue before the demo.
* The sample numbers in `reports/` and `dashboard/` come from synthetic data, **not** from Kaggle. Re-run on the real dataset and use those numbers in your report.

## 7. Possible extensions
Sessions/funnel report (`view → cart → purchase` per `user_session`), top categories/brands, hourly report per event type, Oozie/Airflow instead of cron, Hive external tables over `/clickstream/out`, email alert when error rate > threshold.
