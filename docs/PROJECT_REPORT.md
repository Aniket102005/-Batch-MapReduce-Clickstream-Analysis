# Batch MapReduce Clickstream Log Analysis
**Mini Project Report** – (Your name, roll number, department, guide, college, date)

---

## Abstract
E-commerce platforms generate millions of clickstream events per day. These logs are too large for ad-hoc querying on a single machine, so insights such as the busiest hour, the most visited pages and the share of faulty records arrive days late. This project stores raw clickstream logs on the Hadoop Distributed File System (HDFS) and implements a MapReduce pipeline, scheduled every night on YARN, that produces three reports – peak-traffic hour, top-10 pages and error rate – which are published on a batch dashboard. The pipeline is demonstrated on the Kaggle "eCommerce behavior data from multi category store" dataset.

## 1. Introduction
1.1 Problem statement (see abstract)  
1.2 Motivation – volume of web logs, delay of manual analysis, need for scheduled batch analytics  
1.3 Scope – batch (not real-time) processing, daily granularity

## 2. Objectives
1. Store raw clickstream logs on HDFS in date partitions.
2. Implement MapReduce jobs for peak-traffic hour, top-10 pages and error rate.
3. Run the jobs automatically every night through YARN's scheduler.
4. Present the results on a dashboard.

## 3. Technology stack
| Component | Role |
|---|---|
| HDFS | Distributed, replicated storage of raw logs (128 MB blocks, `dt=YYYY-MM-DD` partitions) |
| MapReduce (Java, Hadoop 3 `mapreduce` API) | Parallel computation of the reports |
| YARN (CapacityScheduler, queue `batch`) | Resource management and scheduling of the jobs |
| cron + shell | Nightly trigger, ingest, report collection |
| Python + HTML/SVG | Report dashboard, data generation, independent verification |

## 4. Dataset
Kaggle: *eCommerce behavior data from multi category store* (Oct–Nov 2019, ≈ 110 M events).  
Columns: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`.  
Event types: `view`, `cart`, `remove_from_cart`, `purchase`.

Because the dataset has no URL and no HTTP status column, a **page** is defined as a product detail page (counted via `view` events) and an **error** as an invalid/malformed record (wrong field count, bad timestamp, unknown event type, non-numeric product/user id, price ≤ 0).

## 5. System design
5.1 Architecture (insert the diagram from README.md)  
5.2 HDFS layout – `/clickstream/raw/dt=<date>/`, `/clickstream/out/dt=<date>/`  
5.3 Nightly flow – cron 01:30 → ingest → 3 YARN jobs → `getmerge` → `reports/<date>/` → dashboard  
5.4 YARN queue configuration – `config/capacity-scheduler-batch-queue.xml` (70 % guaranteed, 90 % max)

## 6. MapReduce design
| Job | Map output | Combiner | Reduce | Reducers |
|---|---|---|---|---|
| Peak hour | (HH, 1) | sum | totals per hour + PEAK_HOUR | 1 |
| Top pages – stage 1 | ((product, category, brand), 1) | sum | count per product | many |
| Top pages – stage 2 | local top-10 per mapper (in `cleanup`) | – | merge into global top-10 | 1 |
| Error rate | (DAY\|date, "1,0/1") and (REASON\|type, "0,1") | vector sum | total, errors, rate % | 1 |

Key points to explain: combiner as a network optimisation, bounded-heap Top-N pattern, why a single reducer is acceptable for small key spaces, idempotent reruns.

## 7. Implementation
Files: see the project layout in README.md section 2. Describe `ClickEvent` (validation), the four job classes and `ClickstreamDriver` (concurrent submission to YARN).

## 8. Results

### 8.1 Real Kaggle Dataset Run (2019-11-01)
Executed on the genuine Kaggle eCommerce dataset (`data/2019-Nov.csv` partition `2019-11-01`):

* **Input Data Size**: 192.8 MB (183.9 MB on HDFS)
* **Total Events Processed**: 1,445,360
* **MapReduce Execution Time**: 72 seconds on YARN `batch` queue
* **Peak-Traffic Hour**: **16:00 UTC** with **104,679 events**
* **Error Rate**: **0.0856%** (1,237 invalid records out of 1,445,360, all categorized under `BAD_PRICE` due to price ≤ 0.0)

#### Top-10 Pages (Most Viewed Products on 2019-11-01):
| Rank | Product ID | Category | Brand | Views |
|---|---|---|---|---|
| 1 | 1004856 | electronics.smartphone | samsung | 13,626 |
| 2 | 1005115 | electronics.smartphone | apple | 12,937 |
| 3 | 1004767 | electronics.smartphone | samsung | 12,298 |
| 4 | 1004249 | electronics.smartphone | apple | 7,420 |
| 5 | 1005105 | electronics.smartphone | apple | 7,265 |
| 6 | 1004870 | electronics.smartphone | samsung | 6,678 |
| 7 | 1004833 | electronics.smartphone | samsung | 6,670 |
| 8 | 4804056 | electronics.audio.headphone | apple | 6,421 |
| 9 | 1002544 | electronics.smartphone | apple | 6,407 |
| 10 | 1005160 | electronics.smartphone | xiaomi | 6,331 |

### 8.2 Historical Development Runs (Synthetic Baseline)
| Night | Events | Peak hour (UTC) | Top page (product id) | Error rate |
|---|---|---|---|---|
| 2019-10-01 | 101,375 | 19:00 (8,552 events) | 1000000 | 1.226 % |
| 2019-10-02 | 100,011 | 19:00 (8,536 events) | 1000000 | 1.258 % |
| 2019-10-03 | 102,136 | 19:00 (8,579 events) | 1000000 | 1.239 % |

### 8.3 Independent Verification
`scripts/verify_results.py` recomputed all three reports from the raw CSV input using an independent Python algorithm and validated against the MapReduce outputs:
* Hourly counts (24 hours): **PASS**
* Peak hour match (16:00, 104,679 events): **PASS**
* Top-10 pages ordering and view counts: **PASS**
* Daily totals and error count (1,445,360 total, 1,237 errors): **PASS**
* Error reason breakdown (`BAD_PRICE`): **PASS**

## 9. Conclusion
Summarise: logs stored on HDFS, three reports produced by MapReduce, scheduled nightly on YARN, published on a dashboard; reports now available the next morning instead of after days.

## 10. Limitations and future work
Batch only (no real time); dataset has no HTTP status/URL so "error" = invalid record; single-node lab results. Future: sessions/funnel analysis, Hive/Spark, Oozie or Airflow scheduling, alerting.

## References
1. Kaggle – eCommerce behavior data from multi category store (M. Kechinov): https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
2. Apache Hadoop documentation – MapReduce Tutorial, HDFS Architecture, YARN Capacity Scheduler: https://hadoop.apache.org/docs/stable/
3. T. White, *Hadoop: The Definitive Guide*, O'Reilly.
