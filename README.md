# Databricks Account Health Check

A comprehensive diagnostic tool that analyzes your Databricks account configuration, usage patterns, and best practice adherence across **171 checks in 13 categories**, producing a scored health report (1–100).

## Architecture

- **Frontend:** React + TypeScript (Vite) — premium Apple-inspired UI
- **Backend:** Python Flask with SSE streaming — real-time progress
- **Data Sources:** System tables (Unity Catalog) + REST APIs (Databricks SDK)
- **Deployment:** Databricks Apps (containerized, runs inside your workspace)

## Quick Start

### Deploy to Databricks Apps

```bash
databricks apps create health-check --manifest app.yaml
databricks apps deploy health-check
```

### Local Development

```bash
# Set environment variables
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-pat-token
export DATABRICKS_APP_PORT=8000

# Build frontend
cd frontend && npm install && npm run build && cd ..

# Run backend
cd backend && pip install -r requirements.txt && python app.py
```

## Scoring Framework

- **Overall Score: 1–100** — weighted average of active section scores
- Sections only scored if they have meaningful usage (>$100/mo or >10 resources)
- Each check scores **0** (fail), **50** (partial), or **100** (pass)
- **90–100:** Excellent | **70–89:** Good | **50–69:** Needs Attention | **0–49:** Critical

## Sections

| # | Section | Checks | Type |
|---|---------|--------|------|
| 1 | Data Engineering & Table Health | 27 | Core |
| 2 | Data Warehousing / SQL Analytics | 18 | Core |
| 3 | Compute Management | 16 | Core |
| 4 | Cost Optimization | 15 | Core |
| 5 | Security & Compliance | 18 | Core |
| 6 | Governance (Unity Catalog) | 14 | Core |
| 7 | AI & ML Workloads | 14 | Conditional |
| 8 | Databricks Apps | 9 | Conditional |
| 9 | OLTP / Lakebase | 8 | Conditional |
| 10 | Delta Sharing | 8 | Conditional |
| 11 | Data Ingestion | 11 | Conditional |
| 12 | Workspace Administration | 8 | Core |
| 13 | CI/CD & DevOps | 5 | Advisory |

## Key Features

- **Real-time progress** — SSE streaming shows each section completing live
- **Per-warehouse sizing scorecard** — individual sizing analysis per SQL warehouse
- **Dynamic recommendations** — specific to your findings with ready-to-run commands
- **Export** — full JSON report for offline review
- **Error isolation** — one failing check never crashes the entire run

## System Table Requirements

The app queries these system tables (must be enabled):
- `system.billing.usage` / `system.billing.list_prices`
- `system.compute.clusters` / `system.compute.node_timeline` / `system.compute.warehouses`
- `system.query.history`
- `system.access.audit` / `system.access.table_lineage`
- `system.lakeflow.jobs` / `system.lakeflow.job_run_timeline` / `system.lakeflow.pipelines`
- `system.serving.served_entities` / `system.serving.endpoint_usage`
- `system.storage.predictive_optimization_operations_history`
- `system.information_schema.tables` / `system.information_schema.table_privileges`

> **Note:** Only `billing` and `access.workspaces_latest` are global. All other tables are regional — the app only sees data from workspaces in the same region.
