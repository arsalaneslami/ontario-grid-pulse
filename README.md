# Ontario Grid Pulse

> **A production-grade Azure Databricks Lakehouse** correlating Ontario's electricity demand with Toronto weather — built end-to-end across every layer of the modern data stack.

![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-FF3621?style=flat)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?style=flat)
![DABs](https://img.shields.io/badge/Asset_Bundles-FF3621?style=flat)

---

## What this project is

A complete Lakehouse implementation on Azure Databricks that ingests hourly electricity demand from IESO (Independent Electricity System Operator) and hourly weather data from ECCC (Environment and Climate Change Canada), reconciles them to UTC, applies data quality expectations, joins them into a feature-engineered Gold table, and surfaces the result through a governed AI/BI dashboard.

Built across **12 phases** covering infrastructure, ingestion, transformation, governance, orchestration, CI/CD, and analytics delivery.

```mermaid
flowchart LR
    classDef source fill:#E5E7EB,stroke:#6B7280,color:#1F2937,stroke-width:2px
    classDef bronze fill:#1F3864,stroke:#0F1E3C,color:#FFFFFF,stroke-width:2px
    classDef silver fill:#1C7293,stroke:#0F4F6B,color:#FFFFFF,stroke-width:2px
    classDef gold  fill:#D97706,stroke:#92400E,color:#FFFFFF,stroke-width:2px
    classDef cons  fill:#C2410C,stroke:#7C2D12,color:#FFFFFF,stroke-width:2px

    IESO[("IESO Demand<br/>CSV")]:::source
    ECCC[("ECCC Weather<br/>CSV")]:::source

    B1["BRONZE<br/>ieso_demand_raw<br/>8,784 rows"]:::bronze
    B2["BRONZE<br/>eccc_weather_raw<br/>744 rows"]:::bronze

    S1["SILVER<br/>demand_hourly<br/>4 expectations"]:::silver
    S2["SILVER<br/>weather_hourly<br/>5 expectations"]:::silver

    GOLD["GOLD<br/>demand_weather_hourly<br/>23 cols · Liquid Clustering"]:::gold

    DASH["AI/BI Dashboard<br/>7 pages · 10 datasets"]:::cons

    IESO --> B1
    ECCC --> B2
    B1 --> S1
    B2 --> S2
    S1 --> GOLD
    S2 --> GOLD
    GOLD --> DASH
```
## Key Analytical Findings

| Finding | Detail |
|---|---|
| **Thermal sensitivity** | Demand nearly doubles between mild and extreme-hot conditions (12,390 MW → 23,122 MW — a 1.87× multiplier). |
| **Peak window** | Daily peak concentrates **14:00–20:00 local**. Weekday-weekend gap of 1,500–2,500 MW during 8 AM–5 PM proves the commercial/industrial load signature. |
| **Net-import anomalies** | 16 hours in 2024 (0.18%) had Ontario importing more than exporting — all clustered in spring shoulder season when Quebec hydro surpluses flow south. Documented as findings, not data errors. |

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Cloud** | Azure (ADLS Gen2, Access Connector, Managed Identity, Premium workspace with SCC) |
| **Compute** | Databricks Serverless + Single-Node cluster |
| **Ingestion** | Auto Loader (`cloudFiles`, `availableNow` trigger) |
| **Transformation** | Lakeflow Spark Declarative Pipelines (`@dlt`) |
| **Governance** | Unity Catalog (RBAC + ABAC: row filters, column masks), System Tables |
| **Performance** | Liquid Clustering, Predictive Optimization, Change Data Feed |
| **Orchestration** | Lakeflow Jobs (DAG with retries, schedules, notifications) |
| **CI/CD** | Databricks Asset Bundles (`databricks bundle deploy --target {dev,prod}`) |
| **Analytics** | AI/BI Dashboard (7 pages, 10 datasets, fully bundle-managed `.lvdash.json`) |
| **Languages** | Python (PySpark), SQL, YAML, Bash |

```mermaid
graph TB
    classDef root   fill:#1F3864,color:#FFFFFF,stroke:#0F1E3C,stroke-width:2px
    classDef config fill:#D97706,color:#FFFFFF,stroke:#92400E,stroke-width:2px
    classDef folder fill:#1C7293,color:#FFFFFF,stroke:#0F4F6B,stroke-width:2px
    classDef code   fill:#E5E7EB,color:#1F2937,stroke:#9CA3AF,stroke-width:2px

    ROOT["ontario-grid-pulse/"]:::root

    CONFIG["databricks.yml<br/>Bundle config<br/>(dev / prod targets)"]:::config
    RES["resources/<br/>Resource YAML definitions"]:::folder
    SRC["src/<br/>Source code"]:::folder
    SCRIPTS["scripts/<br/>Helper scripts"]:::folder

    R1["silver_pipeline.yml"]:::code
    R2["gold_pipeline.yml"]:::code
    R3["daily_ingestion.job.yml"]:::code
    R4["dashboard.yml"]:::code

    BRONZE["bronze/<br/>Auto Loader notebooks"]:::code
    SILVER["silver/<br/>Lakeflow + DQ expectations"]:::code
    GOLD["gold/<br/>Materialized view"]:::code
    SQL["sql/<br/>Governance · observability"]:::code
    DASH["dashboards/<br/>.lvdash.json"]:::code

    ROOT --> CONFIG
    ROOT --> RES
    ROOT --> SRC
    ROOT --> SCRIPTS

    RES --> R1
    RES --> R2
    RES --> R3
    RES --> R4

    SRC --> BRONZE
    SRC --> SILVER
    SRC --> GOLD
    SRC --> SQL
    SRC --> DASH
```

## Engineering Practices Demonstrated

- **Medallion architecture** with strict layer separation
- **9 data quality expectations** (drop and warn) with documented anomaly handling
- **UTC normalization** across heterogeneous source timezone conventions (IESO EST hour-ending, ECCC LST)
- **Attribute-based access control** — row filters and column masks at Silver, propagating to Gold
- **Predictive Optimization + Liquid Clustering** for query performance
- **System table observability** — audit, lineage (table + column), billing usage
- **Multi-environment CI/CD** with environment-aware variables (`${bundle.target}`)
- **Cost discipline** — ~91 DBUs (~$50 CAD) for the entire project, monitored via `system.billing.usage`

---

## What's Out of Scope (Production Hardening Roadmap)

Honest scope tracking — these would be the next iteration:

- Backfill ECCC weather to full year (currently July 2024 sample)
- File-arrival triggers (replacing scheduled triggers)
- Multi-station weather coverage for province-wide modeling
- Service principal isolation for prod (currently runs as user)
- GitHub Actions CI for `bundle validate` on every PR
- Cost monitoring dashboard built on `system.billing.usage`
- Terraform/Bicep for Azure infrastructure provisioning (currently click-ops)

---

## About

Built by **Arsalan Eslami** — Senior Data Engineer in Newmarket, Ontario.

Currently exploring **Senior Azure Databricks Engineer** opportunities. Open to roles at energy & utility companies, Databricks consulting partners, and enterprises building lakehouse platforms.

📧 arsalaneslami@gmail.com  
🔗 [LinkedIn](https://www.linkedin.com/)

---

*Project documentation, deployment runbook, and architectural diagrams available on request.*
