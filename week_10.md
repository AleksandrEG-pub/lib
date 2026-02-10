# ETL Solution Assessment Report

## 1. Overview

The company currently operates a custom Python-based ETL script that transfers data from multiple accounting systems into a centralized PostgreSQL storage. While the solution was initially sufficient, growth in the number of data sources and transferred tables has exposed architectural and operational limitations.

This document:

* analyzes the current solution and its issues,
* identifies root causes behind the observed problems,
* maps problems to technical solutions using the allowed toolset,
* evaluates implementation effort and timelines,
* prepares the ground for proposing a new ETL architecture.

---

## 2. Current Solution (As-Is)

### Description

* A standalone Python script performs data extraction and loading.
* Source and target details (server, schema, table) are passed as parameters.
* The script is reusable for different sources using the same logic.
* Logs are written to text files or standard output.
* Execution can be triggered manually or by an external system.

### Observed Characteristics

* Single-process execution
* No centralized orchestration
* No structured logging or execution metadata
* No explicit data quality validation
* No automatic failure notification
* No CI/CD process

---

## 3. Identified Problems and Root Causes

Perfect! Here’s the **full Problem–Cause–Solution reference section**, fully converted into **Markdown tables** for clarity and visual readability.

---

# ETL Problem–Cause–Solution Reference

This section provides a structured reference of **problems, root causes, possible solutions/tools, effort estimates, and related requirements**. It serves as a planning and justification tool for ETL architecture design.

---

## 1. Memory Failures (ETL-001)

**Cause 1:** Full dataset loaded into memory

| Solution / Tool | Effort    | Requirement addressed | Notes                                                        |
| --------------- | --------- | --------------------- | ------------------------------------------------------------ |
| pyarrow         | 1–2 weeks | ETL-001               | Stream data in batches to reduce memory usage                |
| Spark           | 3–5 weeks | ETL-001               | Distributed processing with spill-to-disk for large datasets |

**Cause 2:** Concurrent executions increase memory pressure

| Solution / Tool | Effort    | Requirement addressed | Notes                                               |
| --------------- | --------- | --------------------- | --------------------------------------------------- |
| Airflow         | 2–3 weeks | ETL-001               | Limit parallelism via scheduler to prevent overload |

**Cause 3:** Poor data partitioning

| Solution / Tool  | Effort    | Requirement addressed | Notes                                                     |
| ---------------- | --------- | --------------------- | --------------------------------------------------------- |
| pyarrow / pandas | 1–2 weeks | ETL-001               | Partition data by table/schema for incremental processing |

---

## 2. Slow Progress and Error Discovery (ETL-002)

**Cause 1:** Logs are unstructured

| Solution / Tool  | Effort  | Requirement addressed | Notes                                                            |
| ---------------- | ------- | --------------------- | ---------------------------------------------------------------- |
| Application code | <1 week | ETL-002               | Structured logging with task_id, source, table, timestamp, level |




**Cause 2:** Logs are file-based

| Solution / Tool                      | Effort    | Requirement addressed | Notes                                                                                                                                      |
| ------------------------------------ | --------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Direct DB insertion (PostgreSQL)** | 1–2 weeks | ETL-002               | Modify ETL tasks to write logs directly to structured tables via a lightweight connector or logging library (e.g., Python logging handler) |
| **Direct DB insertion (ClickHouse)** | 2–3 weeks | ETL-002               | Same as above, but optimized for high-volume logs                                                                                          |
| **Log shipping agent**               | 1–2 weeks | ETL-002               | Use a tool like **Fluentd** or **Filebeat** to monitor log files and push updates to Postgres/ClickHouse automatically                     |
| **Message queue**                    | 2–3 weeks | ETL-002               | ETL tasks send log events to **Kafka**, which then streams logs to Postgres/ClickHouse; adds resilience and decoupling                     |
| **Airflow task logging integration** | 1 week    | ETL-002               | Airflow can capture task stdout/stderr automatically and push it to database or monitoring system                                          |

---


**Cause 3:** Manual inspection of logs

| Solution / Tool          | Effort | Requirement addressed | Notes                                            |
| ------------------------ | ------ | --------------------- | ------------------------------------------------ |
| Grafana / SQL dashboards | 1 week | ETL-002               | Visualize progress and errors for faster insight |

**Cause 4:** No aggregation / analysis

| Solution / Tool | Effort | Requirement addressed | Notes                                                       |
| --------------- | ------ | --------------------- | ----------------------------------------------------------- |
| Airflow         | 1 week | ETL-002, ETL-003      | Scheduled log analysis for aggregation and automated checks |

---


## 3. Automatic Notifications (ETL-003)

**Cause 1:** No failure detection

| Solution / Tool        | Effort  | Requirement addressed | Notes                                                                   |
| ---------------------- | ------- | --------------------- | ----------------------------------------------------------------------- |
| Airflow task callbacks | <1 week | ETL-003               | Task-level notifications with context (source, table, timestamp, error) |

**Cause 2:** Log anomalies not monitored

| Solution / Tool          | Effort | Requirement addressed | Notes                                                  |
| ------------------------ | ------ | --------------------- | ------------------------------------------------------ |
| Airflow scheduled checks | 1 week | ETL-003               | Periodic log queries trigger alerts on detected errors |

---

## 4. Missing Data Quality Validation (Optional)

**Cause 1:** No row count / null checks

| Solution / Tool  | Effort | Requirement addressed | Notes                                           |
| ---------------- | ------ | --------------------- | ----------------------------------------------- |
| pandas / pyarrow | 1 week | Optional              | Simple validation of row counts and null values |

**Cause 2:** Schema mismatch

| Solution / Tool | Effort    | Requirement addressed | Notes                                               |
| --------------- | --------- | --------------------- | --------------------------------------------------- |
| Iceberg         | 1–2 weeks | Optional              | Enforce schema validation to prevent invalid writes |

**Cause 3:** Incorrect / missing records

| Solution / Tool    | Effort    | Requirement addressed | Notes                                               |
| ------------------ | --------- | --------------------- | --------------------------------------------------- |
| Comparison scripts | 1–2 weeks | Optional              | Compare source and target metadata for completeness |

---
