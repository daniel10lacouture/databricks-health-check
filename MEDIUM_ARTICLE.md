
# How to Score Your Databricks Account Health in 5 Minutes

*A step-by-step guide to deploying the Databricks Account Health Check app — 80+ automated checks across security, cost, governance, and more.*

---

You wouldn't drive a car without a dashboard. So why run a Databricks account without one?

The **Databricks Account Health Check** is a free, open-source app that analyzes your entire Databricks environment and gives you a single score from 0 to 100. It checks 80+ best practices across 13 categories — from cost optimization and security posture to governance compliance and compute efficiency — and tells you exactly what to fix.

It runs in about 5 minutes, is completely read-only (nothing gets modified), and works on AWS, Azure, and GCP.

---

## What You'll Get

Once the health check completes, you'll see:

- **An overall health score (0–100)** — like a credit score for your Databricks account
- **13 section scores** — Data Engineering, SQL Analytics, Compute, Cost, Security, Governance, AI/ML, Apps, Data Storage, Delta Sharing, Ingestion, Workspace Admin, and Adoption
- **Actionable recommendations** — prioritized fixes with ready-to-use prompts you can paste into Databricks Assistant
- **A cost overview** — with specific savings opportunities (idle clusters, oversized warehouses, unoptimized storage)
- **Achievement badges** — Bronze, Silver, Gold, and Perfect tiers per section
- **AI-generated insights** — trend analysis and anomaly detection across your account

<!-- [INSERT SCREENSHOT: Dashboard with overall score and section cards] -->

---

## Prerequisites

Before you start, make sure you have:

1. **A Databricks workspace with Unity Catalog enabled** — the app queries Unity Catalog system tables
2. **System tables enabled** — go to Admin Settings → System Tables and enable them ([docs](https://docs.databricks.com/en/administration-guide/system-tables/index.html))
3. **Databricks Apps enabled** — the app deploys as a Databricks App ([docs](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html))
4. **A Serverless SQL Warehouse** — Medium size recommended (see sizing guide below)

---

## Step 1: Import the Code

Download or clone the repository, then import it into your Databricks workspace.

**In the Databricks UI:**

1. Go to **Workspace** → **Home**
2. Click **Import**
3. Upload the `databricks-health-check` folder

The files should end up at a path like:
```
/Workspace/Users/your-email@company.com/databricks-health-check/
```

---

## Step 2: Create the App

Navigate to **Compute → Apps → Create App** and fill in:

| Setting | Value |
| --- | --- |
| **App name** | `account-health-check` |
| **Source code path** | `/Workspace/Users/your-email@company.com/databricks-health-check` |

Click **Create**. Databricks will provision a service principal for the app automatically.

> **Tip:** If you want the IP Access List and PAT Token checks to work (instead of showing N/A), grant the app's service principal **Workspace Admin** permissions: Admin Settings → Service Principals → select the app SP → grant Workspace Admin.

---

## Step 3: Deploy and Run

Click **Deploy** in the Apps UI (or run `databricks apps deploy account-health-check` from the CLI).

The app installs its dependencies and starts in about 2 minutes. Once the status shows **"Succeeded"**, click the app URL to open it.

You'll see the health check landing page. Select your SQL Warehouse from the dropdown and click **Run Health Check**.

<!-- [INSERT SCREENSHOT: Setup screen with warehouse selector] -->

---

## Understanding Your Results

### The Dashboard

The main dashboard shows your overall score prominently, with section cards below. Each card shows:

- The section score (0–100)
- A progress bar
- Number of checks needing attention

Click any section to drill into individual checks.

<!-- [INSERT SCREENSHOT: Section detail with checks] -->

### Check Statuses

Each check has one of four statuses:

| Status | Meaning |
| --- | --- |
| **Pass (100)** | Best practice achieved. Click to see conforming resources. |
| **Partial (50)** | Partially meeting the target. Specific gaps identified. |
| **Fail (0)** | Not meeting the target. Actionable fix provided. |
| **N/A** | Could not evaluate (usually a permissions issue — see the drill-down for guidance). |

### Improve My Score

Click **"Improve My Score"** in the left menu for a prioritized list of recommendations. Each one includes a ready-to-use prompt you can copy-paste into Databricks Assistant (Genie Code) to start fixing issues immediately.

### Cost Overview

The **Cost Overview** page breaks down your compute and storage spend, identifies idle resources, and shows a remediation roadmap with estimated savings.

### Badges

Earn **Bronze (55+), Silver (70+), Gold (85+),** and **Perfect (100)** badges for each section. Plus 12 individual achievement badges like:

- **Security Guardian** — Security score 80+
- **Cost Optimizer** — Compute & cost score 70+
- **Zero Waste** — No idle compute resources
- **Full Coverage** — All 13 sections active

---

## Warehouse Sizing Guide

The health check runs 200+ SQL queries in parallel against system tables. Your warehouse size determines how fast it completes:

| Warehouse | Instances | Runtime |
| --- | --- | --- |
| Small | 1 | 8–12 min |
| **Medium** | **1–2** | **4–6 min** |
| Large | 2–4 | 2–4 min |

**My recommendation:** Use a **Serverless Medium warehouse with auto-scaling (min 1, max 2)**. This gives you ~5 minute runtimes, instant startup, and zero idle cost when you're not running the check.

---

## What Gets Checked

Here's a sample of what the 80+ checks evaluate:

**Cost Optimization**
- Idle clusters and warehouses burning money
- Oversized warehouse auto-stop timeouts
- Jobs running on all-purpose compute instead of jobs compute
- Resources missing cost attribution tags

**Security & Compliance**
- SSO adoption and legacy authentication
- IP access lists and network security
- PAT token lifecycle management
- Data classification and column masking

**Governance**
- Tables without owners or documentation
- Unity Catalog adoption (external vs managed tables)
- Row-level security and column masking policies

**Data Engineering**
- Pipeline health and failure rates
- Delta table optimization (ZORDER, VACUUM, OPTIMIZE)
- Predictive Optimization enrollment
- Streaming vs batch workload patterns

**AI/ML & GenAI**
- Model Serving endpoint efficiency
- AI Gateway adoption
- Databricks Assistant and Genie usage
- Feature store and MLflow adoption

*...and many more across SQL Analytics, Compute, Apps, Delta Sharing, and Workspace Admin.*

---

## Frequently Asked Questions

**Does this modify anything in my account?**
No. The app is 100% read-only. It only runs SELECT queries on system tables and read-only API calls.

**How often should I run it?**
Monthly is a good cadence — or after major changes like new cluster policies, warehouse migrations, or security updates.

**Does it work across cloud providers?**
Yes. It uses Unity Catalog system tables, which are available on AWS, Azure, and GCP.

**What about multi-region accounts?**
Most system tables are regional. Deploy the app in each region for full coverage. Billing data is global and always complete regardless of region.

**Is the score comparable across accounts?**
Yes. The scoring framework is consistent. A 75 on one account means the same thing as a 75 on another.

---

## What's Next

After your first health check:

1. **Focus on the top 3 recommendations** — the "Improve My Score" page prioritizes them by impact
2. **Share your score** — use the LinkedIn share button on the badges page
3. **Run it monthly** — track your progress over time
4. **Contribute** — the app is open-source. Add checks, improve the UI, or extend the scoring framework

---

*The Databricks Account Health Check is an open-source project. Deploy it in your workspace today and find out what your score is.*
