# 🚕 NYC Taxi Analytics Platform

An end-to-end analytics engineering project using **Airflow**, **Snowflake**, **dbt**, and **Metabase**.

## Architecture
```
[NYC TLC Public Data (S3)]
        ↓
[Airflow — Ingestion DAG]
        ↓
[Snowflake — RAW schema]
        ↓
[dbt — staging → marts]
        ↓
[Snowflake — ANALYTICS schema]
        ↓
[Metabase — Dashboard]
```

## Stack
| Tool | Purpose |
|---|---|
| Apache Airflow | Pipeline orchestration & scheduling |
| Snowflake | Cloud data warehouse |
| dbt | Data transformation & modeling |
| Metabase | BI dashboards & visualization |
| Docker | Local Airflow environment |

## Project Structure
```
nyc-taxi-analytics/
├── airflow/
│   ├── dags/
│   ├── plugins/
│   └── logs/
├── dbt/
│   └── nyc_taxi/
│       ├── models/
│       │   ├── staging/
│       │   └── marts/
│       ├── tests/
│       ├── macros/
│       └── seeds/
├── metabase/
├── docs/
├── .env.example
└── README.md
```

## Data Source
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Yellow Taxi, Jan–Feb 2024
