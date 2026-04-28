# Databricks Account Health Check

A one-click diagnostic app that scores your Databricks account across **80+ checks in 13 categories** — covering cost optimization, security, governance, compute, AI/ML, and more. Get an overall health score (0–100), actionable recommendations, and achievement badges.

<!-- Add a screenshot of your dashboard here -->
<!-- ![Dashboard Screenshot](screenshot.png) -->

---

## What You Get

- **Overall Health Score (0–100)** — weighted average across all active sections
- **13 Section Scores** — Data Engineering, SQL Analytics, Compute, Cost, Security, Governance, AI/ML, Apps, Data Storage, Delta Sharing, Ingestion, Workspace Admin, Adoption
- **Actionable Recommendations** — prioritized fixes with ready-to-use prompts for Databricks Assistant
- **Cost Overview** — compute and storage cost analysis with savings opportunities
- **Achievement Badges** — Bronze/Silver/Gold/Perfect per section, plus 12 individual achievement badges
- **GenAI Insights** — AI-generated summary of your account posture (requires a Foundation Model endpoint)
- **Export** — full JSON report for offline review

---

## Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **System Tables** enabled ([docs](https://docs.databricks.com/en/administration-guide/system-tables/index.html))
3. **Databricks Apps** enabled on your workspace ([docs](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html))
4. **SQL Warehouse** — Serverless recommended (see [Warehouse Sizing](#warehouse-sizing) below)

---

## Deployment (3 Steps)

### Step 1: Import the source code

Upload this repository to your Databricks workspace. You can either:

**Option A — Git folder:**
```
Workspace → Repos → Add Repo → paste the Git URL
```

**Option B — Manual upload:**
```
Workspace → Home → Import → Upload this folder
```

The files should end up at a path like:
```
/Workspace/Users/<your-email>/databricks-health-check/
```

### Step 2: Create the Databricks App

Navigate to **Compute → Apps → Create App**, then:

| Setting | Value |
| --- | --- |
| **App name** | `account-health-check` (or any name you prefer) |
| **Source code path** | `/Workspace/Users/<your-email>/databricks-health-check` |

Or via CLI:
```bash
databricks apps create account-health-check \
  --source-code-path /Workspace/Users/<your-email>/databricks-health-check
```

### Step 2b: Configure User Authorization Scopes

After creating the app, go to the app's **Settings → User authorization** and add these four scopes:

| Scope | Description |
| --- | --- |
| `sql` | Allow the app to execute SQL and manage SQL related resources |
| `catalog.tables:read` | Allows the app to read tables in Unity Catalog |
| `catalog.catalogs:read` | Allows the app to read catalogs in Unity Catalog |
| `catalog.schemas:read` | Allows the app to read schemas in Unity Catalog |

**To add scopes:** Click **"+ Add scope"** and add each one. Users will be prompted to consent to these scopes the first time they open the app.

> **Without these scopes, the app will fail with SQL permission errors when running the health check.**

---

### Step 3: Deploy

```bash
databricks apps deploy account-health-check \
  --source-code-path /Workspace/Users/<your-email>/databricks-health-check
```

Or click **Deploy** in the Apps UI. The app installs dependencies and starts automatically (~2 minutes).

Once deployed, open the app URL, select a SQL Warehouse, and click **Run Health Check**.

---

## Warehouse Sizing

The health check runs **200+ SQL queries** against system tables in parallel. Warehouse sizing directly impacts runtime:

| Warehouse Size | Recommended Instances | Expected Runtime |
| --- | --- | --- |
| **Small** | 1 | 8–12 minutes |
| **Medium** | 1–2 | 4–6 minutes |
| **Large** | 2–4 | 2–4 minutes |

**Recommendations:**
- Use a **Serverless SQL Warehouse** (instant startup, auto-scaling, zero idle cost)
- **Medium with 2 instances** is the sweet spot for most accounts (~5 min runtime)
- Enable **auto-scaling** (Min: 1, Max: 2–4) so the warehouse scales during the burst of parallel queries
- The warehouse can be stopped after the health check completes — it only runs during the check
- A **Starter Warehouse** works but will be slower due to single-instance limits

---

## Permissions

The app runs as a **service principal** that is automatically created when you create the Databricks App. This service principal needs:

### Required (automatically granted in most setups)
- `SELECT` on `system.*` tables (system tables in Unity Catalog)
- Access to the SQL Warehouse you select

### Optional (for full coverage)
- **Workspace Admin** role on the service principal — enables the IP Access List and PAT Token checks. Without this, those checks show as N/A with guidance on how to enable them.
  - Go to **Admin Settings → Service Principals → [your app SP] → Grant Workspace Admin**

### System Tables That Must Be Enabled

| Table | Used For |
| --- | --- |
| `system.billing.usage` | Cost analysis, DBU tracking |
| `system.billing.list_prices` | Cost calculations |
| `system.query.history` | SQL analytics, query performance |
| `system.compute.clusters` | Cluster configuration checks |
| `system.compute.warehouses` | Warehouse sizing and usage |
| `system.compute.node_timeline` | Compute utilization |
| `system.access.audit` | Security and access pattern analysis |
| `system.access.table_lineage` | Data lineage checks |
| `system.lakeflow.jobs` | Job configuration analysis |
| `system.lakeflow.job_run_timeline` | Job performance and failure rates |
| `system.information_schema.tables` | Table metadata and governance |
| `system.serving.served_entities` | Model serving checks |
| `system.storage.table_metrics_history` | Storage optimization |

> **Note:** `system.billing.*` and `system.access.workspaces_latest` are account-level (global). All other system tables are **regional** — the app only analyzes workspaces in the same cloud region as the workspace where the app is deployed.

---

## Scoring Framework

- **Overall Score: 0–100** — weighted average of active section scores
- Sections are only scored if they have meaningful usage (prevents penalizing unused features)
- Each check scores **0** (fail), **50** (partial), or **100** (pass)

| Score Range | Rating |
| --- | --- |
| 90–100 | Excellent |
| 70–89 | Good |
| 50–69 | Needs Attention |
| 0–49 | Critical |

---

## Architecture

```
databricks-health-check/
├── app.yaml                    # Databricks App manifest
├── backend/
│   ├── app.py                  # Flask server with SSE streaming
│   ├── checks/                 # 15 check modules (80+ checks)
│   │   ├── base.py             # QueryExecutor, APIClient, base classes
│   │   ├── data_engineering.py
│   │   ├── sql_analytics.py
│   │   ├── compute.py
│   │   ├── cost.py
│   │   ├── security.py
│   │   ├── governance.py
│   │   ├── ai_ml.py
│   │   └── ...
│   ├── insights.py             # Trend analysis and anomaly detection
│   ├── genai_insights.py       # AI-generated insights (optional)
│   ├── scoring.py              # Overall score computation
│   └── recommendations.py      # Prioritized recommendation engine
└── frontend/
    └── dist/
        └── index.html          # Single-page app (self-contained)
```

---

## Troubleshooting

| Issue | Solution |
| --- | --- |
| **App shows "Initializing" for a long time** | The SQL Warehouse may be starting up. Serverless warehouses start in seconds; classic warehouses can take 2–5 minutes. |
| **Checks show "N/A"** | The required system table may not be enabled, or the app service principal lacks permissions. Check the drill-down for specific guidance. |
| **"Requires workspace admin permissions"** | Grant the app's service principal Workspace Admin role (Admin Settings → Service Principals). |
| **Score seems low** | Sections are only scored when active. Unused features (e.g., Delta Sharing) won't penalize your score. Click into each section to see which checks need attention. |
| **SQL permission errors when running health check** | Go to the app Settings → User authorization and add all four required scopes (`sql`, `catalog.tables:read`, `catalog.catalogs:read`, `catalog.schemas:read`). Users must consent on first use. |
| **App deployment fails** | Verify the source code path is correct and the workspace has Databricks Apps enabled. Check the deployment logs in the Apps UI. |
| **Queries time out** | Use a larger warehouse or enable auto-scaling. The health check runs 200+ queries in parallel; a Small warehouse may queue them. |

---

## FAQ

**Q: Does this modify anything in my workspace?**
A: No. The app is completely read-only. It only runs SELECT queries against system tables and read-only API calls.

**Q: How often should I run it?**
A: Monthly is a good cadence. You can also run it after major infrastructure changes (new clusters, warehouse migrations, policy updates).

**Q: Does it work on AWS, Azure, and GCP?**
A: Yes. The app uses Unity Catalog system tables which are available on all cloud providers.

**Q: What about multi-workspace accounts?**
A: System tables are regional. Deploy the app in each region to get full coverage, or deploy once and understand that only same-region workspaces are analyzed. Billing data (`system.billing.*`) is global and always complete.

---

## License

Internal use. Contact the author for redistribution.
