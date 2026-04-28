"""
Flask API server for the Databricks Account Health Check app.
Serves frontend as static files and provides API endpoints with SSE streaming.
v2 fixes: auto-timeout reset, progress tracking, account-level consistency.
"""
import json
import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from pathlib import Path
from collections import OrderedDict

from flask import Flask, Response, jsonify, request, send_from_directory

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("health_check")

# Determine paths
BACKEND_DIR = Path(__file__).parent
PROJECT_DIR = BACKEND_DIR.parent
FRONTEND_DIST = PROJECT_DIR / "frontend" / "dist"

app = Flask(__name__, static_folder=str(FRONTEND_DIST), static_url_path="")

# ── Global state ──────────────────────────────────────────────────────
RUN_TIMEOUT_SECONDS = 600  # 10 minutes auto-reset

health_check_state = {
    "running": False,
    "started_at": 0,       # timestamp when run started
    "progress": [],
    "results": None,
    "error": None,
    "warehouse_id": None,
    "user_token": None,
}
state_lock = threading.Lock()


def is_stale_run() -> bool:
    """Check if a run has been going for too long and should be auto-reset."""
    with state_lock:
        if not health_check_state["running"]:
            return False
        elapsed = time.time() - health_check_state["started_at"]
        return elapsed > RUN_TIMEOUT_SECONDS


def reset_state():
    """Force-reset the health check state."""
    with state_lock:
        health_check_state["running"] = False
        health_check_state["started_at"] = 0
        health_check_state["progress"] = []
        health_check_state["error"] = None
    logger.info("Health check state reset")


# ── Helpers ───────────────────────────────────────────────────────────
def get_host():
    h = os.environ.get("DATABRICKS_HOST", "")
    if not h:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            h = w.config.host
        except Exception:
            pass
    if h and not h.startswith("http"):
        h = f"https://{h}"
    return h.rstrip("/")


def get_token():
    t = os.environ.get("DATABRICKS_TOKEN", "")
    if not t:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            t = w.config.token
        except Exception:
            pass
    return t or ""


def push_event(event: dict):
    with state_lock:
        health_check_state["progress"].append(event)


# ── Section registry ─────────────────────────────────────────────────
def get_runners(executor, api_client, include_table_analysis, prefetch_data=None):
    from checks.cost import CostCheckRunner
    from checks.sql_analytics import SQLAnalyticsCheckRunner
    from checks.security import SecurityCheckRunner
    from checks.data_engineering import DataEngineeringCheckRunner
    from checks.compute import ComputeCheckRunner
    from checks.governance import GovernanceCheckRunner
    from checks.ai_ml import AIMLCheckRunner
    from checks.data_storage import DataStorageCheckRunner
    from checks.apps import AppsCheckRunner
    from checks.lakebase import LakebaseCheckRunner
    from checks.delta_sharing import DeltaSharingCheckRunner
    from checks.workspace_admin import WorkspaceAdminCheckRunner
    from checks.bi_tooling import BIToolingCheckRunner
    from checks.adoption import AdoptionCheckRunner
    from checks.genie_code import GenieCodeCheckRunner

    return [
        DataEngineeringCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        SQLAnalyticsCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        ComputeCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        CostCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        SecurityCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        GovernanceCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        AIMLCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        BIToolingCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        AppsCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        LakebaseCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        DeltaSharingCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        WorkspaceAdminCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        AdoptionCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        GenieCodeCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
        DataStorageCheckRunner(executor, api_client, include_table_analysis, prefetch_data),
    ]


def _compute_score_booster(overall: dict, section_results: list) -> dict:
    """
    Compute the Score Booster: potential score if adoption opportunities are realized.
    
    Looks at the adoption section (advisory), extracts projected_score_boost from
    each failing check, and computes what the overall score WOULD be if those
    boosts were applied to the relevant health sections.
    """
    current_score = overall.get("overall_score")
    if current_score is None:
        return {"available": False}

    # Find the adoption section
    adoption_section = None
    for s in section_results:
        if s.get("section_id") == "adoption":
            adoption_section = s
            break

    if not adoption_section or not adoption_section.get("active"):
        return {"available": False, "current_score": current_score}

    # Extract opportunities (non-passing checks with projected_score_boost)
    opportunities = []
    total_boost = 0
    for check in adoption_section.get("checks", []):
        boost = (check.get("details") or {}).get("projected_score_boost", 0)
        if boost > 0 and check.get("status") in ("fail", "partial"):
            opportunities.append({
                "check_id": check.get("check_id"),
                "name": check.get("name"),
                "subsection": check.get("subsection"),
                "current_value": check.get("current_value"),
                "projected_boost": boost,
                "peer_benchmark": (check.get("details") or {}).get("peer_benchmark", ""),
                "recommendation": check.get("recommendation", {}),
            })
            total_boost += boost

    # Sort by projected boost descending
    opportunities.sort(key=lambda x: x["projected_boost"], reverse=True)

    # Cap potential score at 100
    potential_score = min(round(current_score + total_boost, 1), 100)

    return {
        "available": True,
        "current_score": current_score,
        "potential_score": potential_score,
        "total_boost": total_boost,
        "opportunity_count": len(opportunities),
        "top_opportunities": opportunities[:5],
        "all_opportunities": opportunities,
    }



def _compute_burn_rate(section_results: list) -> dict:
    """
    Compute the real-time burn rate — dollars being wasted per second.
    
    Extracts waste signals from specific checks:
    - 4.5.1 CSP infra cost (eliminable via serverless)
    - 4.5.2 Idle cluster burn (auto-termination/serverless)
    - 4.5.3 Warehouse idle time (serverless warehouses)
    - 4.1.2 All-purpose compute for jobs (job compute migration)
    - 4.2.1 Idle warehouses (auto-stop)
    - 4.2.2 Idle clusters (auto-termination)
    """
    import re as _re

    def _extract_dollar(text, patterns):
        """Extract dollar amount from check current_value or details."""
        if not text:
            return 0
        for pat in patterns:
            m = _re.search(pat, str(text))
            if m:
                val = m.group(1).replace(",", "")
                try:
                    return float(val)
                except (ValueError, TypeError):
                    pass
        return 0

    def _extract_hours(text):
        """Extract hours from text like '1,234 idle node-hours'."""
        m = _re.search(r"([\d,]+(?:\.\d+)?)\s*idle\s*(?:node-)?hours", str(text))
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except (ValueError, TypeError):
                pass
        return 0

    waste_sources = []
    checks_by_id = {}

    for sec in section_results:
        for check in sec.get("checks", []):
            checks_by_id[check.get("check_id", "")] = check

    # 1. CSP infrastructure cost (monthly, fully eliminable via serverless)
    c = checks_by_id.get("4.5.1", {})
    cv = c.get("current_value", "")
    csp_monthly = _extract_dollar(cv, [r"\$([\d,.]+)\s*in"])
    if csp_monthly > 0:
        waste_sources.append({
            "id": "csp_infra",
            "label": "Migrate to serverless compute",
            "description": "Eliminate CSP infrastructure charges (EC2/VMs) by using serverless — compute is fully managed and included in pricing.",
            "monthly_waste": csp_monthly,
            "per_second": csp_monthly / 30 / 24 / 3600,
            "category": "serverless_migration",
            "docs_url": "https://docs.databricks.com/en/compute/serverless.html"
        })

    # 2. Idle cluster burn (monthly estimate from check)
    c = checks_by_id.get("4.5.2", {})
    cv = c.get("current_value", "")
    idle_waste = _extract_dollar(cv, [r"~?\$([\d,.]+)\s*(?:estimated|waste)"])
    if idle_waste > 0:
        waste_sources.append({
            "id": "idle_clusters",
            "label": "Enable auto-termination on all clusters",
            "description": "Clusters running at <5% CPU are burning compute. Auto-termination shuts them down after idle periods.",
            "monthly_waste": idle_waste,
            "per_second": idle_waste / 30 / 24 / 3600,
            "category": "auto_termination",
            "docs_url": "https://docs.databricks.com/en/compute/configure.html#auto-termination"
        })

    # 3. Warehouse idle time (estimate: idle_hours * $2/hour average)
    c = checks_by_id.get("4.5.3", {})
    cv = c.get("current_value", "")
    idle_hours = _extract_hours(cv)
    if idle_hours > 0:
        wh_waste = idle_hours * 2.0  # conservative $2/hour estimate
        waste_sources.append({
            "id": "warehouse_idle",
            "label": "Switch to serverless SQL warehouses",
            "description": "Serverless warehouses have zero idle cost and start in under 2 seconds. Classic warehouses charge while idle.",
            "monthly_waste": wh_waste,
            "per_second": wh_waste / 30 / 24 / 3600,
            "category": "serverless_warehouses",
            "docs_url": "https://docs.databricks.com/en/compute/sql-warehouse/serverless.html"
        })

    # 4. All-purpose compute for jobs (check 4.1.2)
    c = checks_by_id.get("4.1.2", {})
    cv = c.get("current_value", "")
    # Look for percentage of all-purpose usage and estimate waste
    ap_pct_match = _re.search(r"(\d+(?:\.\d+)?)%\s*all-purpose", str(cv))
    if ap_pct_match:
        ap_pct = float(ap_pct_match.group(1))
        # Estimate: all-purpose is ~2x the cost of jobs compute for same workload
        # Get total DBUs from details if available
        details = c.get("details", {})
        nc = details.get("non_conforming", [])
        ap_dbus = 0
        for item in nc:
            val = str(item.get("all_purpose_dbus", item.get("total_dbus", "0")))
            val = val.replace(",", "")
            try:
                ap_dbus += float(val)
            except (ValueError, TypeError):
                pass
        if ap_dbus > 0:
            # ~30% premium for all-purpose vs jobs compute
            ap_waste = ap_dbus * 0.15 * 0.30  # 30% of list price delta, conservative
            if ap_waste > 100:
                waste_sources.append({
                    "id": "job_compute",
                    "label": "Move jobs to dedicated job clusters",
                    "description": "All-purpose clusters cost ~2x more than job clusters for the same workload. Switching saves 30-50% on job compute.",
                    "monthly_waste": ap_waste,
                    "per_second": ap_waste / 30 / 24 / 3600,
                    "category": "right_compute",
                    "docs_url": "https://docs.databricks.com/en/workflows/jobs/use-compute.html"
                })

    # 5. Check for idle warehouses (4.2.1) and idle clusters (4.2.2) as additional signals
    for check_id, label, desc, url in [
        ("4.2.1", "Resize or stop underused warehouses", "Warehouses with minimal query volume should be stopped or consolidated.", "https://docs.databricks.com/en/compute/sql-warehouse/index.html"),
        ("4.2.2", "Terminate unused interactive clusters", "Interactive clusters left running with no users attached waste compute.", "https://docs.databricks.com/en/compute/configure.html#auto-termination"),
    ]:
        c = checks_by_id.get(check_id, {})
        if c.get("status") in ("fail", "partial"):
            details = c.get("details", {})
            nc = details.get("non_conforming", [])
            count = len(nc) if nc else 0
            if count > 0:
                est = count * 200  # conservative $200/month per idle resource
                waste_sources.append({
                    "id": f"idle_{check_id.replace('.','_')}",
                    "label": label,
                    "description": desc,
                    "monthly_waste": est,
                    "per_second": est / 30 / 24 / 3600,
                    "category": "idle_resources",
                    "docs_url": url
                })

    # Sort by monthly waste descending
    waste_sources.sort(key=lambda x: x["monthly_waste"], reverse=True)

    total_monthly = sum(s["monthly_waste"] for s in waste_sources)
    total_per_second = sum(s["per_second"] for s in waste_sources)
    total_daily = total_per_second * 86400
    total_annual = total_monthly * 12

    return {
        "available": total_monthly > 0,
        "total_monthly_waste": round(total_monthly, 2),
        "total_daily_waste": round(total_daily, 2),
        "total_annual_waste": round(total_annual, 2),
        "per_second": round(total_per_second, 6),
        "source_count": len(waste_sources),
        "sources": waste_sources,
    }



# ── Background health-check runner ───────────────────────────────────
def run_health_check(warehouse_id: str, include_table_analysis: bool, user_token: str = ""):
    from checks.base import QueryExecutor, APIClient, DataPrefetcher
    from scoring import compute_overall_score
    from recommendations import get_top_recommendations
    from genai_insights import GenAIInsights

    try:
        token = user_token or get_token()
        executor = QueryExecutor(get_host().replace("https://", ""), token, warehouse_id)
        api_client = APIClient()
        # Prefetch disabled (check files use their own queries)
        prefetch_data = None
        push_event({"type": "progress", "message": "Prefetch complete, running checks..."})

        runners = get_runners(executor, api_client, include_table_analysis, prefetch_data)
        total = len(runners)
        section_results = [None] * total
        completed_count = 0

        # Send initial queued events for all sections
        for idx, runner in enumerate(runners):
            push_event({
                "type": "progress",
                "section": runner.section_id,
                "section_name": runner.section_name,
                "completed": 0,
                "total_sections": total,
                "status": "queued",
            })

        def run_section(idx_runner):
            idx, runner = idx_runner
            logger.info(f"Running section {idx+1}/{total}: {runner.section_name}")
            push_event({
                "type": "progress",
                "section": runner.section_id,
                "section_name": runner.section_name,
                "completed": 0,
                "total_sections": total,
                "status": "running",
            })
            t0 = time.time()
            result = runner.run()
            elapsed = round(time.time() - t0, 1)
            logger.info(f"  -> {runner.section_name} completed in {elapsed}s, score={result.score}")
            return idx, runner, result, elapsed

        # PERF: Fire wrapped queries + insights EARLY (overlap with section checks)
        # These are independent SQL queries that don't need section results
        # Pillars 2+6: Run insights and wrapped stats IN PARALLEL (perf fix)
        logger.info("Starting insights + wrapped stats in parallel...")
        push_event({"type": "progress", "section": "wrapped", "name": "Account Intelligence"})

        # Fire trend + anomaly queries early (no dependency on section_results)
        from concurrent.futures import ThreadPoolExecutor as TPE2, Future
        from insights import compute_trends, detect_anomalies
        _early_pool = TPE2(max_workers=2)
        _trends_future = _early_pool.submit(compute_trends, executor)
        _anomalies_future = _early_pool.submit(detect_anomalies, executor)

        wrapped_queries = {
            "query_volume": "SELECT COUNT(*) AS total_queries, COUNT(DISTINCT executed_by) AS unique_users, COUNT(DISTINCT DATE(start_time)) AS active_days FROM system.query.history WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())",
            "busiest_hour": "SELECT HOUR(start_time) AS hr, DAYOFWEEK(start_time) AS dow, COUNT(*) AS cnt FROM system.query.history WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE()) GROUP BY HOUR(start_time), DAYOFWEEK(start_time) ORDER BY cnt DESC LIMIT 1",
            "top_users": "SELECT executed_by, COUNT(*) AS queries FROM system.query.history WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE()) AND executed_by IS NOT NULL GROUP BY executed_by ORDER BY queries DESC LIMIT 5",
            "total_dbus": "SELECT ROUND(SUM(usage_quantity), 0) AS total_dbus, COUNT(DISTINCT usage_date) AS billing_days FROM system.billing.usage WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE()) AND usage_unit = 'DBU'",
            "workspace_count": "SELECT COUNT(*) AS total, COUNT(CASE WHEN status = 'RUNNING' THEN 1 END) AS active FROM system.access.workspaces_latest",
            "data_volume": "SELECT COUNT(DISTINCT CONCAT(table_catalog,'.',table_schema,'.',table_name)) AS total_tables, COUNT(DISTINCT table_catalog) AS catalogs, COUNT(DISTINCT CONCAT(table_catalog,'.',table_schema)) AS schemas FROM system.information_schema.tables WHERE table_schema != 'information_schema' AND table_catalog != 'system'",
            "job_stats": "SELECT COUNT(DISTINCT job_id) AS unique_jobs, COUNT(*) AS total_runs, ROUND(AVG(run_duration_seconds)/60.0, 1) AS avg_duration_min FROM system.lakeflow.job_run_timeline WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE()) AND run_type = 'JOB_RUN' AND result_state IS NOT NULL",
            "ai_usage": "SELECT COUNT(*) AS ai_queries FROM system.query.history WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE()) AND (statement_text LIKE '%%ai_query%%' OR statement_text LIKE '%%ai_classify%%' OR statement_text LIKE '%%ai_extract%%' OR statement_text LIKE '%%ai_forecast%%')",
        }
        wrapped_stats = {}
        def _run_wrapped(kv):
            wkey, wsql = kv
            try:
                wrows = executor.execute(wsql, timeout=180)
                return wkey, wrows[0] if wrows else {}
            except Exception as we:
                logger.warning(f"Wrapped query '{wkey}' failed: {we}")
                return wkey, {}
        with ThreadPoolExecutor(max_workers=8) as wpool:
            for wkey, wval in wpool.map(_run_wrapped, wrapped_queries.items()):
                wrapped_stats[wkey] = wval

        
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = [pool.submit(run_section, (i, r)) for i, r in enumerate(runners)]
            for future in as_completed(futures):
                try:
                    idx, runner, result, elapsed = future.result()
                except Exception as exc:
                    logger.error(f"Section failed with exception: {exc}", exc_info=True)
                    completed_count += 1
                    push_event({
                        "type": "section_complete",
                        "section": "unknown",
                        "section_name": "Error",
                        "completed": completed_count,
                        "total_sections": total,
                        "score": 0,
                        "active": False,
                        "issues_count": 0,
                        "checks": [],
                        "elapsed": 0,
                    })
                    continue

                result_dict = result.to_dict()
                section_results[idx] = result_dict
                completed_count += 1

                push_event({
                    "type": "section_complete",
                    "section": runner.section_id,
                    "section_name": runner.section_name,
                    "completed": completed_count,
                    "total_sections": total,
                    "score": result.score,
                    "active": result.active,
                    "issues_count": result_dict["issues_count"],
                    "checks": result_dict["checks"],
                    "elapsed": elapsed,
                })

        # Filter out None results (from failed sections)
        section_results = [s for s in section_results if s is not None]
        # ── Merge sections with same section_id (consolidation) ──────────
        merged = {}
        for sec in section_results:
            sid = sec["section_id"]
            if sid not in merged:
                merged[sid] = sec
            else:
                # Merge checks into existing section
                merged[sid]["checks"].extend(sec.get("checks", []))
                # Recalculate score as weighted average of active checks
                all_checks = [c for c in merged[sid]["checks"] if c.get("status") not in ("not_evaluated", "info")]
                if all_checks:
                    merged[sid]["score"] = round(sum(c.get("score", 0) or 0 for c in all_checks) / len(all_checks), 1)
                    merged[sid]["issues_count"] = sum(1 for c in all_checks if c.get("status") in ("fail", "partial"))
                    merged[sid]["active"] = True
                # Merge subsections
                existing_subs = set()
                for c in merged[sid]["checks"]:
                    if c.get("subsection"):
                        existing_subs.add(c["subsection"])
        section_results = list(merged.values())



        overall = compute_overall_score(section_results)
        top_recs = get_top_recommendations(section_results, limit=10)

        # Pillar 4: Compute Score Booster (pure Python, instant)
        score_booster = _compute_score_booster(overall, section_results)
        burn_rate = _compute_burn_rate(section_results)

        # Collect early-fired trend + anomaly results
        try:
            trends = _trends_future.result(timeout=120)
        except Exception as e:
            logger.warning(f"Trends failed: {e}")
            trends = {}
        try:
            anomalies = _anomalies_future.result(timeout=120)
        except Exception as e:
            logger.warning(f"Anomalies failed: {e}")
            anomalies = []
        _early_pool.shutdown(wait=False)

        # Compute remaining insights that NEED section_results
        from insights import compute_maturity, compute_whatif_scenarios
        maturity = compute_maturity(overall.get("overall_score"), section_results)
        whatif = compute_whatif_scenarios(section_results, executor)
        import time as _t
        insights_data = {"maturity": maturity, "trends": trends, "anomalies": anomalies,
                         "whatif_scenarios": whatif, "generated_at": _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime())}

        # Pillar 3: Generate GenAI insights (non-blocking)
        ai_insights = {}
        try:
            host = get_host()
            genai = GenAIInsights(host, token)
            ai_insights = genai.generate(
                {"overall": overall, "sections": section_results, "top_recommendations": top_recs},
                insights_data)
        except Exception as ge:
            logger.warning(f"GenAI insights failed (non-blocking): {ge}")
            ai_insights = {"error": str(ge)}

        dow_names = {1:'Sunday',2:'Monday',3:'Tuesday',4:'Wednesday',5:'Thursday',6:'Friday',7:'Saturday'}
        wqv = wrapped_stats.get("query_volume", {})
        wbh = wrapped_stats.get("busiest_hour", {})
        wtd = wrapped_stats.get("total_dbus", {})
        wwc = wrapped_stats.get("workspace_count", {})
        wdv = wrapped_stats.get("data_volume", {})
        wjs = wrapped_stats.get("job_stats", {})
        wai = wrapped_stats.get("ai_usage", {})

        def _safe_int(v):
            try:
                return int(float(v or 0))
            except (ValueError, TypeError):
                return 0

        wrapped_slides = [
            {"title": "Your account ran", "big": f"{_safe_int(wqv.get('total_queries')):,}", "subtitle": "queries in the last 30 days", "icon": "query"},
            {"title": "Powered by", "big": f"{_safe_int(wqv.get('unique_users')):,}", "subtitle": "active data practitioners", "icon": "users"},
            {"title": "Your busiest hour is", "big": f"{dow_names.get(_safe_int(wbh.get('dow')), 'N/A')} at {_safe_int(wbh.get('hr'))}:00", "subtitle": f"when {_safe_int(wbh.get('cnt')):,} queries typically run", "icon": "clock"},
            {"title": "Your data estate spans", "big": f"{_safe_int(wdv.get('total_tables')):,} tables", "subtitle": f"across {_safe_int(wdv.get('catalogs'))} catalogs and {_safe_int(wdv.get('schemas'))} schemas", "icon": "database"},
            {"title": "Your team ran", "big": f"{_safe_int(wjs.get('total_runs')):,} job runs", "subtitle": f"across {_safe_int(wjs.get('unique_jobs'))} unique jobs", "icon": "jobs"},
            {"title": "Total compute consumed", "big": f"{_safe_int(wtd.get('total_dbus')):,} DBUs", "subtitle": f"over {_safe_int(wtd.get('billing_days'))} billing days", "icon": "compute"},
            {"title": "Operating across", "big": f"{_safe_int(wwc.get('active'))} workspaces", "subtitle": f"out of {_safe_int(wwc.get('total'))} total provisioned", "icon": "workspaces"},
            {"title": "AI-powered queries", "big": f"{_safe_int(wai.get('ai_queries')):,}", "subtitle": "calls to ai_query, ai_classify, ai_extract, and ai_forecast", "icon": "ai"},
        ]
        wrapped_data = {"slides": wrapped_slides, "raw": wrapped_stats}
        logger.info(f"Wrapped stats: {_safe_int(wqv.get('total_queries'))} queries, {_safe_int(wqv.get('unique_users'))} users")

        final = {
            "overall": overall,
            "sections": section_results,
            "top_recommendations": top_recs,
            "insights": insights_data,
            "ai_insights": ai_insights,
            "score_booster": score_booster,
            "burn_rate": burn_rate,
            "wrapped": wrapped_data,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "diagnostics": {
                "query_stats": executor.stats,
                "warehouse_id": warehouse_id,
                "workspace_host": get_host(),
                "token_source": "user (on-behalf-of)" if user_token else "SP",
            },
        }

        with state_lock:
            health_check_state["results"] = final
            health_check_state["running"] = False

        push_event({
            "type": "complete",
            "overall_score": overall.get("overall_score"),
            "label": overall.get("label"),
        })

    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        with state_lock:
            health_check_state["running"] = False
            health_check_state["error"] = str(e)
        push_event({"type": "error", "message": str(e)})


# ── API Routes ────────────────────────────────────────────────────────



@app.route("/api/wrapped")
def wrapped_stats():
    """Generate Spotify-Wrapped-style account stats from system tables."""
    token = request.headers.get("x-forwarded-access-token") or get_token()
    wh = request.args.get("warehouse_id", "")
    if not wh:
        return jsonify({"error": "warehouse_id required"}), 400
    
    from checks.base import QueryExecutor
    host = get_host().replace("https://", "")
    executor = QueryExecutor(host, token, wh)
    
    stats = {}
    queries = {
        "query_volume": """
            SELECT COUNT(*) AS total_queries, 
                   COUNT(DISTINCT executed_by) AS unique_users,
                   COUNT(DISTINCT DATE(start_time)) AS active_days
            FROM system.query.history 
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())""",
        
        "busiest_hour": """
            SELECT HOUR(start_time) AS hr, DAYOFWEEK(start_time) AS dow, COUNT(*) AS cnt
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY HOUR(start_time), DAYOFWEEK(start_time)
            ORDER BY cnt DESC LIMIT 1""",
        
        "top_users": """
            SELECT executed_by, COUNT(*) AS queries
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE()) AND executed_by IS NOT NULL
            GROUP BY executed_by ORDER BY queries DESC LIMIT 5""",
        
        "total_dbus": """
            SELECT ROUND(SUM(usage_quantity), 0) AS total_dbus,
                   COUNT(DISTINCT usage_date) AS billing_days
            FROM system.billing.usage
            WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE()) AND usage_unit = 'DBU'""",
        
        "workspace_count": """
            SELECT COUNT(*) AS total, COUNT(CASE WHEN status = 'RUNNING' THEN 1 END) AS active
            FROM system.access.workspaces_latest""",
        
        "data_volume": """
            SELECT COUNT(DISTINCT CONCAT(table_catalog,'.',table_schema,'.',table_name)) AS total_tables,
                   COUNT(DISTINCT table_catalog) AS catalogs,
                   COUNT(DISTINCT CONCAT(table_catalog,'.',table_schema)) AS schemas
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema' AND table_catalog != 'system'""",
        
        "job_stats": """
            SELECT COUNT(DISTINCT job_id) AS unique_jobs,
                   COUNT(*) AS total_runs,
                   ROUND(AVG(run_duration_seconds)/60.0, 1) AS avg_duration_min
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
              AND run_type = 'JOB_RUN' AND result_state IS NOT NULL""",
        
        "ai_usage": """
            SELECT COUNT(*) AS ai_queries
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
              AND (statement_text LIKE '%ai_query%' OR statement_text LIKE '%ai_classify%'
                   OR statement_text LIKE '%ai_extract%' OR statement_text LIKE '%ai_forecast%')""",
    }
    
    for key, sql in queries.items():
        try:
            rows = executor.execute(sql)
            stats[key] = rows[0] if rows else {}
            logger.info(f"Wrapped query '{key}': {len(rows)} rows, result={stats[key]}")
        except Exception as e:
            logger.warning(f"Wrapped query '{key}' failed: {e}")
            logger.warning(f"  Token present: {bool(token)}, Token prefix: {(token or '')[:15]}..., WH: {wh}, Host: {host}")
            stats[key] = {"_error": str(e)[:200]}
    
    # Build narrative slides
    dow_names = {1:'Sunday',2:'Monday',3:'Tuesday',4:'Wednesday',5:'Thursday',6:'Friday',7:'Saturday'}
    
    qv = stats.get("query_volume", {})
    bh = stats.get("busiest_hour", {})
    tu = stats.get("top_users", {})
    td = stats.get("total_dbus", {})
    wc = stats.get("workspace_count", {})
    dv = stats.get("data_volume", {})
    js = stats.get("job_stats", {})
    ai = stats.get("ai_usage", {})
    
    slides = [
        {"title": "Your account ran", "big": f"{int(qv.get('total_queries', 0) or 0):,}", "subtitle": "queries in the last 30 days", "icon": "query"},
        {"title": "Powered by", "big": f"{int(qv.get('unique_users', 0) or 0):,}", "subtitle": "active data practitioners", "icon": "users"},
        {"title": "Your busiest hour is", "big": f"{dow_names.get(int(bh.get('dow', 0) or 0), 'N/A')} at {int(bh.get('hr', 0) or 0)}:00", "subtitle": f"when {int(bh.get('cnt', 0) or 0):,} queries typically run", "icon": "clock"},
        {"title": "Your data estate spans", "big": f"{int(dv.get('total_tables', 0) or 0):,} tables", "subtitle": f"across {int(dv.get('catalogs', 0) or 0)} catalogs and {int(dv.get('schemas', 0) or 0)} schemas", "icon": "database"},
        {"title": "Your team ran", "big": f"{int(js.get('total_runs', 0) or 0):,} job runs", "subtitle": f"across {int(js.get('unique_jobs', 0) or 0)} unique jobs (avg {js.get('avg_duration_min', 0)} min each)", "icon": "jobs"},
        {"title": "Total compute consumed", "big": f"{int(td.get('total_dbus', 0) or 0):,} DBUs", "subtitle": f"over {int(td.get('billing_days', 0) or 0)} billing days", "icon": "compute"},
        {"title": "Operating at scale across", "big": f"{int(wc.get('active', 0) or 0)} workspaces", "subtitle": f"out of {int(wc.get('total', 0) or 0)} total provisioned", "icon": "workspaces"},
        {"title": "AI-powered queries", "big": f"{int(ai.get('ai_queries', 0) or 0):,}", "subtitle": "calls to ai_query, ai_classify, ai_extract, and ai_forecast", "icon": "ai"},
    ]
    
    errors = {k: v.get("_error") for k, v in stats.items() if isinstance(v, dict) and "_error" in v}
    logger.info(f"Wrapped stats complete. Errors: {errors}")
    return jsonify({"slides": slides, "raw": stats, "errors": errors})


@app.route("/api/chat", methods=["POST"])
def chat():
    """AI Health Advisor — uses ai_query() via SQL warehouse for auth compatibility."""
    import requests as _req

    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        data = {}

    messages = data.get("messages", [])
    question = data.get("question", "")
    if not messages and not question:
        return jsonify({"answer": "Please ask a question."})

    # Get auth — user token + warehouse from stored state or request
    user_token = request.headers.get("x-forwarded-access-token")
    if not user_token:
        with state_lock:
            user_token = health_check_state.get("user_token")
    
    with state_lock:
        wh_id = health_check_state.get("warehouse_id")
    # Client can also send warehouse_id
    wh_id = wh_id or data.get("warehouse_id")

    ws_host = get_host()

    if not user_token:
        return jsonify({"answer": "Please run a health check first to establish authentication."})
    if not wh_id:
        return jsonify({"answer": "No warehouse available. Please run a health check first."})

    # Get results context
    try:
        with state_lock:
            raw = health_check_state.get("results")
            results = dict(raw) if raw else {}
    except Exception:
        results = {}
    if not results and data.get("context"):
        results = data["context"]
    if not results:
        return jsonify({"answer": "Please run a health check first so I have data to analyze."})

    # Build context
    try:
        ov = results.get("overall", {})
        secs = results.get("sections", [])
        br = results.get("burn_rate", {})
        ctx = [f"Score: {ov.get('overall_score','?')}/100, Maturity: {ov.get('maturity_label','?')}", ""]
        for sec in secs:
            if not sec.get("active"): continue
            checks = sec.get("checks", [])
            fails = [c for c in checks if c.get("status") in ("fail","partial")]
            ctx.append(f"{sec.get('section_name','')}: {sec.get('score','?')}/100 ({len(fails)} issues)")
            for c in fails[:4]:
                ctx.append(f"  - {c.get('name','')}: {str(c.get('current_value',''))[:60]}")
                r = c.get("recommendation") or {}
                if r.get("action"): ctx.append(f"    Fix: {str(r['action'])[:80]}")
        if br.get("available"):
            ctx.append(f"Monthly waste: ~${br.get('total_monthly_waste',0):,.0f}")
        context_str = "\n".join(ctx)[:5000]
    except Exception as e:
        logger.error(f"Chat context error: {e}")
        context_str = f"Score: {results.get('overall',{}).get('overall_score','?')}/100"

    # Build the user's latest message
    user_msg = ""
    if messages:
        # Get the last user message for ai_query (single-turn)
        for m in reversed(messages):
            if m.get("role") == "user":
                user_msg = str(m.get("content", ""))
                break
    elif question:
        user_msg = question

    if not user_msg:
        return jsonify({"answer": "Please ask a question."})

    # Build prompt for ai_query
    prompt = f"""You are the Databricks Account Health Advisor. Help admins understand and act on their health check results.
Be specific (cite scores, check names, numbers). Keep answers concise (2-3 paragraphs). Use markdown.

HEALTH CHECK DATA:
{context_str}

USER QUESTION: {user_msg}"""

    # Escape single quotes for SQL
    safe_prompt = prompt.replace("'", "''").replace("\\", "\\\\")

    # Use ai_query via Statement Execution API
    sql = f"SELECT ai_query('databricks-claude-sonnet-4', '{safe_prompt}') AS answer"

    try:
        logger.info(f"Chat: executing ai_query via warehouse {wh_id}")
        api_resp = _req.post(
            f"{ws_host}/api/2.0/sql/statements",
            headers={"Authorization": f"Bearer {user_token}"},
            json={
                "warehouse_id": wh_id,
                "statement": sql,
                "wait_timeout": "50s",
                "disposition": "INLINE",
                "format": "JSON_ARRAY",
            },
            timeout=60,
        )

        if api_resp.status_code != 200:
            logger.error(f"Chat SQL API error: {api_resp.status_code} {api_resp.text[:200]}")
            return jsonify({"answer": f"SQL API error ({api_resp.status_code}). Please try again."})

        resp_data = api_resp.json()
        state = resp_data.get("status", {}).get("state", "")

        if state == "FAILED":
            err = resp_data.get("status", {}).get("error", {}).get("message", "Unknown error")
            logger.error(f"Chat SQL failed: {err}")
            return jsonify({"answer": f"Query error: {err[:200]}"})

        if state == "SUCCEEDED":
            rows = resp_data.get("result", {}).get("data_array", [])
            if rows and rows[0]:
                answer = rows[0][0] or "No response generated."
                return jsonify({"answer": answer})
            return jsonify({"answer": "No response generated."})

        if state in ("PENDING", "RUNNING"):
            # Poll for result
            stmt_id = resp_data.get("statement_id")
            for _ in range(30):
                time.sleep(2)
                poll = _req.get(
                    f"{ws_host}/api/2.0/sql/statements/{stmt_id}",
                    headers={"Authorization": f"Bearer {user_token}"},
                    timeout=15,
                )
                pdata = poll.json()
                pstate = pdata.get("status", {}).get("state", "")
                if pstate == "SUCCEEDED":
                    rows = pdata.get("result", {}).get("data_array", [])
                    if rows and rows[0]:
                        return jsonify({"answer": rows[0][0] or "No response."})
                    return jsonify({"answer": "No response generated."})
                if pstate == "FAILED":
                    err = pdata.get("status", {}).get("error", {}).get("message", "")
                    return jsonify({"answer": f"Query error: {err[:200]}"})
            return jsonify({"answer": "Request timed out. Please try again."})

        return jsonify({"answer": f"Unexpected state: {state}"})

    except Exception as e:
        logger.error(f"Chat error: {e}")
        return jsonify({"answer": f"Error: {str(e)[:200]}"})


@app.route("/api/chat-test")
def chat_test():
    """Diagnostic endpoint for chat."""
    import requests as _req
    info = {}
    ut = request.headers.get("x-forwarded-access-token") or ""
    st = get_token()
    h = get_host()
    info["user_token"] = bool(ut)
    info["sp_token"] = bool(st)
    info["host"] = h[:50] if h else None
    info["has_results"] = health_check_state.get("results") is not None
    for label, tok in [("user", ut), ("sp", st)]:
        if not tok: info[f"fmapi_{label}"] = "no token"; continue
        try:
            r = _req.post(f"{h}/serving-endpoints/databricks-claude-sonnet-4/invocations",
                headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
                json={"messages":[{"role":"user","content":"say ok"}],"max_tokens":5}, timeout=15)
            info[f"fmapi_{label}"] = f"HTTP {r.status_code}" + ("" if r.status_code==200 else f": {r.text[:80]}")
        except Exception as e:
            info[f"fmapi_{label}"] = str(e)[:100]
    return jsonify(info)


@app.route("/api/workspace-host")
def workspace_host():
    host = get_host()
    return jsonify({"host": host})

@app.route("/api/warehouses")
def list_warehouses():
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        result = []
        for wh in warehouses:
            is_serverless = getattr(wh, "enable_serverless_compute", False)
            result.append({
                "id": wh.id,
                "name": wh.name,
                "state": str(wh.state).split(".")[-1] if wh.state else "UNKNOWN",
                "cluster_size": wh.cluster_size,
                "warehouse_type": str(wh.warehouse_type).split(".")[-1] if wh.warehouse_type else "UNKNOWN",
                "enable_serverless_compute": is_serverless,
                "auto_stop_mins": getattr(wh, "auto_stop_mins", None),
                "num_clusters": getattr(wh, "num_clusters", 1),
            })
        result.sort(key=lambda w: (not w.get("enable_serverless_compute", False), w.get("name", "")))
        logger.info(f"Found {len(result)} warehouses")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Failed to list warehouses: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/health-check/start", methods=["POST"])
def start_health_check():
    # Auto-reset stale runs
    if is_stale_run():
        logger.warning("Auto-resetting stale health check run")
        reset_state()

    with state_lock:
        if health_check_state["running"]:
            elapsed = time.time() - health_check_state["started_at"]
            return jsonify({
                "error": f"Health check already running ({elapsed:.0f}s elapsed). Use /api/health-check/reset to force-reset.",
                "running_since": health_check_state["started_at"],
            }), 409
        health_check_state["running"] = True
        health_check_state["started_at"] = time.time()
        health_check_state["progress"] = []
        health_check_state["results"] = None
        health_check_state["error"] = None

    body = request.json or {}
    warehouse_id = body.get("warehouse_id")
    include_table_analysis = body.get("include_table_analysis", False)

    if not warehouse_id:
        with state_lock:
            health_check_state["running"] = False
        return jsonify({"error": "warehouse_id is required"}), 400

    user_token = request.headers.get("x-forwarded-access-token")
    if not user_token:
        with state_lock:
            health_check_state["running"] = False
        return jsonify({"error": "No user authorization token. Please re-authorize the app."}), 401

    with state_lock:
        health_check_state["warehouse_id"] = warehouse_id
        health_check_state["user_token"] = user_token

    thread = threading.Thread(
        target=run_health_check,
        args=(warehouse_id, include_table_analysis, user_token),
        daemon=True,
    )
    thread.start()
    return jsonify({"status": "started"})


@app.route("/api/health-check/reset", methods=["POST"])
def force_reset():
    """Force-reset a stuck health check run."""
    reset_state()
    return jsonify({"status": "reset", "message": "Health check state has been reset."})


@app.route("/api/health-check/stream")
def stream_progress():
    def generate():
        sent = 0
        last_ping = time.time()
        while True:
            with state_lock:
                events = health_check_state["progress"][sent:]
                running = health_check_state["running"]
            for event in events:
                yield f"data: {json.dumps(event)}\n\n"
                sent += 1
                last_ping = time.time()
            if not running and sent >= len(health_check_state["progress"]):
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
                break
            if time.time() - last_ping > 10:
                yield f": keepalive\n\n"
                last_ping = time.time()
            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    })


@app.route("/api/health-check/results")
def get_results():
    # Auto-reset stale runs
    if is_stale_run():
        reset_state()

    with state_lock:
        results = health_check_state["results"]
        error = health_check_state["error"]
        running = health_check_state["running"]
    if running:
        return jsonify({"status": "running"}), 202
    if error:
        return jsonify({"error": error}), 500
    if results is None:
        return jsonify({"error": "No results available. Run a health check first."}), 404
    return jsonify(results)


@app.route("/api/health-check/results/<section_id>")
def get_section_results(section_id):
    with state_lock:
        results = health_check_state["results"]
    if results is None:
        return jsonify({"error": "No results available"}), 404
    for s in results.get("sections", []):
        if s["section_id"] == section_id:
            return jsonify(s)
    return jsonify({"error": f"Section {section_id} not found"}), 404


@app.route("/api/health-check/export")
def export_results():
    with state_lock:
        results = health_check_state["results"]
    if results is None:
        return jsonify({"error": "No results available"}), 404
    return Response(
        json.dumps(results, indent=2, default=str),
        mimetype="application/json",
        headers={"Content-Disposition": "attachment; filename=health_check_report.json"},
    )


# ── Export Checklist ──────────────────────────────────────────────────

@app.route("/api/health-check/export-checklist")
def export_checklist():
    """Export an actionable checklist of all non-passing checks as HTML."""
    with state_lock:
        results = health_check_state["results"]
    if results is None:
        return jsonify({"error": "No results available"}), 404

    ts = results.get("timestamp", "")
    overall = results.get("overall", {})
    sections = results.get("sections", [])

    items = []
    for sec in sections:
        if not sec.get("active"):
            continue
        for check in sec.get("checks", []):
            st = check.get("status", "")
            if st in ("fail", "partial"):
                rec = check.get("recommendation") or {}
                nc = (check.get("details") or {}).get("non_conforming", [])
                items.append({
                    "section": sec.get("section_name", ""),
                    "check": check.get("name", ""),
                    "status": st,
                    "current": check.get("current_value", ""),
                    "target": check.get("target_value", ""),
                    "action": rec.get("action", "Review and remediate"),
                    "priority": rec.get("priority", "medium"),
                    "impact": rec.get("impact", ""),
                    "docs": rec.get("docs_url", ""),
                    "objects": nc[:10],
                })

    html = [f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; margin: 40px; color: #1F2937; max-width: 900px; }}
        h1 {{ color: #111827; font-size: 24px; }}
        h2 {{ color: #374151; font-size: 18px; margin-top: 24px; border-bottom: 1px solid #E5E7EB; padding-bottom: 4px; }}
        .item {{ margin: 12px 0; padding: 12px; border: 1px solid #E5E7EB; border-radius: 8px; page-break-inside: avoid; }}
        .item-high {{ border-left: 4px solid #DC2626; }}
        .item-medium {{ border-left: 4px solid #D97706; }}
        .item-low {{ border-left: 4px solid #2563EB; }}
        .checkbox {{ font-size: 18px; margin-right: 8px; }}
        .action {{ font-weight: 600; font-size: 14px; }}
        .meta {{ font-size: 12px; color: #6B7280; margin-top: 4px; }}
        .objects {{ font-size: 11px; margin-top: 6px; padding: 6px; background: #F9FAFB; border-radius: 4px; }}
        .priority {{ font-size: 11px; font-weight: 600; padding: 2px 6px; border-radius: 8px; color: white; }}
        .p-high {{ background: #DC2626; }} .p-medium {{ background: #D97706; }} .p-low {{ background: #2563EB; }}
        @media print {{ .item {{ break-inside: avoid; }} }}
    </style></head><body>
    <h1>&#9745; Databricks Health Check — Action Checklist</h1>
    <p>Generated: {ts} &middot; Overall Score: {overall.get('overall_score', 'N/A')}</p>
    <p><strong>{len(items)} items</strong> need attention ({sum(1 for i in items if i['priority']=='high')} high,
       {sum(1 for i in items if i['priority']=='medium')} medium,
       {sum(1 for i in items if i['priority']=='low')} low priority)</p>
    """]

    grouped = OrderedDict()
    for item in items:
        sec = item["section"]
        if sec not in grouped:
            grouped[sec] = []
        grouped[sec].append(item)

    for sec_name, sec_items in grouped.items():
        html.append(f"<h2>{sec_name}</h2>")
        for item in sec_items:
            p = item["priority"]
            obj_html = ""
            if item["objects"]:
                cols = list(item["objects"][0].keys())
                hdr = "".join(f"<b>{c}</b> &nbsp; " for c in cols[:4])
                rows_html = ""
                for obj in item["objects"][:5]:
                    vals = " &middot; ".join(str(obj.get(c,""))[:40] for c in cols[:4])
                    rows_html += f"<div>{vals}</div>"
                obj_html = f'<div class="objects"><div>{hdr}</div>{rows_html}</div>'

            html.append(f"""<div class="item item-{p}">
                <span class="checkbox">&#9744;</span>
                <span class="priority p-{p}">{p.upper()}</span>
                <div class="action" style="display:inline; margin-left:8px;">{item['check']}</div>
                <div class="meta">{item['current']} &rarr; Target: {item['target']}</div>
                <div class="meta"><b>Action:</b> {item['action']}</div>
                {f'<div class="meta"><b>Impact:</b> {item["impact"]}</div>' if item["impact"] else ""}
                {f'<div class="meta"><a href="{item["docs"]}">Documentation &nearr;</a></div>' if item["docs"] else ""}
                {obj_html}
            </div>""")

    html.append("</body></html>")
    return Response("".join(html), mimetype="text/html",
        headers={"Content-Disposition": f"attachment; filename=health_check_checklist_{ts[:10]}.html"})


# ── Export PDF (HTML-based) ──────────────────────────────────────────

@app.route("/api/health-check/export-pdf")
def export_pdf():
    """Export full report as a downloadable HTML report."""
    with state_lock:
        results = health_check_state["results"]
    if results is None:
        return jsonify({"error": "No results available"}), 404

    overall = results.get("overall", {})
    sections = results.get("sections", [])
    top_recs = results.get("top_recommendations", [])
    ts = results.get("timestamp", "")

    html_parts = [f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; margin: 40px; color: #1F2937; }}
        h1 {{ color: #111827; border-bottom: 2px solid #E5E7EB; padding-bottom: 8px; }}
        h2 {{ color: #374151; margin-top: 24px; }}
        h3 {{ color: #4B5563; margin-top: 16px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 8px 0; }}
        th, td {{ border: 1px solid #D1D5DB; padding: 6px 10px; text-align: left; font-size: 12px; }}
        th {{ background: #F3F4F6; font-weight: 600; }}
        .pass {{ color: #059669; }} .fail {{ color: #DC2626; }} .partial {{ color: #D97706; }}
        .info {{ color: #6B7280; }} .score {{ font-size: 18px; font-weight: 700; }}
        .rec-box {{ border-left: 3px solid #D97706; padding: 8px; margin: 4px 0; background: #FFFBEB; }}
        .rec-high {{ border-left-color: #DC2626; background: #FEF2F2; }}
    </style></head><body>
    <h1>Databricks Account Health Check Report</h1>
    <p>Generated: {ts}</p>
    <p class="score">Overall Score: {overall.get('overall_score', 'N/A')} — {overall.get('label', '')}</p>
    <p>Active sections: {overall.get('active_sections', 0)}/{overall.get('total_sections', 0)}</p>
    """]

    if top_recs:
        html_parts.append("<h2>Top Recommendations</h2><table><tr><th>Priority</th><th>Check</th><th>Current</th><th>Action</th></tr>")
        for r in top_recs[:10]:
            p = r.get('priority','')
            html_parts.append(f"<tr><td><b class=\"{'fail' if p=='high' else 'partial'}\">{p.upper()}</b></td>"
                            f"<td>{r.get('check_name','')}</td><td>{r.get('current_value','')}</td>"
                            f"<td>{r.get('action','')}</td></tr>")
        html_parts.append("</table>")

    for sec in sections:
        if not sec.get("active"):
            continue
        score = sec.get("score")
        score_str = f"{score}" if score is not None else "N/A"
        html_parts.append(f"<h2>{sec.get('section_name','')} — Score: {score_str}</h2>")
        for check in sec.get("checks", []):
            status = check.get("status", "")
            status_icon = {"pass": "✅", "fail": "❌", "partial": "⚠️", "info": "ℹ️"}.get(status, "—")
            html_parts.append(f"<h3>{status_icon} {check.get('name','')}</h3>")
            html_parts.append(f"<p>{check.get('current_value','')} · Target: {check.get('target_value','')}</p>")
            rec = check.get("recommendation")
            if rec:
                p = rec.get("priority","")
                cls = "rec-high" if p == "high" else "rec-box"
                html_parts.append(f"<div class=\"{cls}\"><b>{rec.get('action','')}</b><br>"
                                f"<small>{rec.get('impact','')}</small></div>")
            det = check.get("details", {}) or {}
            nc = det.get("non_conforming", [])
            if nc and isinstance(nc[0], dict):
                cols = list(nc[0].keys())
                hdr = "".join(f"<th>{c}</th>" for c in cols)
                rows_html = ""
                for item in nc[:30]:
                    cells = "".join(f"<td>{item.get(c,'')}</td>" for c in cols)
                    rows_html += f"<tr>{cells}</tr>"
                html_parts.append(f"<table><tr>{hdr}</tr>{rows_html}</table>")

    html_parts.append("</body></html>")
    return Response("".join(html_parts), mimetype="text/html",
        headers={"Content-Disposition": f"attachment; filename=health_check_report_{ts[:10]}.html"})


# ── AI Insights (on-demand re-generation) ─────────────────────────────

@app.route("/api/ai-insights", methods=["POST"])
def regenerate_ai_insights():
    with state_lock:
        results = health_check_state["results"]
    if results is None:
        return jsonify({"error": "No results available. Run a health check first."}), 404
    user_token = request.headers.get("x-forwarded-access-token")
    if not user_token:
        return jsonify({"error": "No user authorization token."}), 401
    try:
        host = get_host()
        genai = GenAIInsights(host, user_token)
        insights_data = results.get("insights", {})
        ai_insights = genai.generate(results, insights_data)
        with state_lock:
            if health_check_state["results"]:
                health_check_state["results"]["ai_insights"] = ai_insights
        return jsonify(ai_insights)
    except Exception as e:
        logger.error(f"AI insights regeneration failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ── Debug / Health ─────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    import sys as _sys
    user_token = request.headers.get("x-forwarded-access-token")
    with state_lock:
        running = health_check_state["running"]
        started = health_check_state["started_at"]
    info = {
        "status": "ok",
        "has_user_token": bool(user_token),
        "python_version": _sys.version,
        "health_check_running": running,
        "running_elapsed_s": round(time.time() - started) if running and started else 0,
        "frontend_dist_exists": FRONTEND_DIST.exists(),
    }
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        info["sdk_host"] = w.config.host
        info["sdk_auth_type"] = str(w.config.auth_type)
    except Exception as e:
        info["sdk_error"] = str(e)
    return jsonify(info)


@app.route("/api/debug")
def debug_info():
    info = {}
    user_token = request.headers.get("x-forwarded-access-token")
    info["token"] = {"present": bool(user_token), "prefix": (user_token or "")[:20] + "..." if user_token else None}
    with state_lock:
        info["state"] = {
            "running": health_check_state["running"],
            "started_at": health_check_state["started_at"],
            "elapsed_s": round(time.time() - health_check_state["started_at"]) if health_check_state["running"] else 0,
            "has_results": health_check_state["results"] is not None,
            "has_error": health_check_state["error"],
            "progress_events": len(health_check_state["progress"]),
        }
        if health_check_state["results"]:
            r = health_check_state["results"]
            info["last_run"] = {
                "timestamp": r.get("timestamp"),
                "overall_score": r.get("overall", {}).get("overall_score"),
                "sections": len(r.get("sections", [])),
                "diagnostics": r.get("diagnostics"),
            }
    return jsonify(info)


# ── Static file serving ──────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(FRONTEND_DIST), "index.html")


@app.route("/<path:path>")
def static_files(path):
    # Don't serve static files for API routes
    if path.startswith("api/"):
        return jsonify({"error": "Not found"}), 404
    try:
        return send_from_directory(str(FRONTEND_DIST), path)
    except Exception:
        return send_from_directory(str(FRONTEND_DIST), "index.html")




# Mission Control routes removed (P0 perf fix)

@app.route("/api/smart-feed")
def smart_feed():
    """Generate smart feed cards based on user's catalog and usage."""
    import random
    
    # For now, return static cards that will be dynamically filtered on the frontend
    # In the future, this will scan Unity Catalog and generate AI-powered suggestions
    
    cards = [
        {
            "type": "challenge",
            "title": "\U0001F3C6 Weekly: Create a Dashboard",
            "description": "Build a dashboard from your top tables. Share insights with your team in one click.",
            "score": 40,
            "action": "Start Challenge",
            "primary": True
        },
        {
            "type": "usecase", 
            "title": "\U0001F9E0 Predictive Analytics",
            "description": "Your data has time-series patterns. Build a forecasting model with AutoML and deploy it to production.",
            "score": 120,
            "roi": "Est. 25% better forecasts",
            "action": "Build with AI",
            "primary": True
        },
        {
            "type": "tip",
            "title": "\u26A1 Enable Photon",
            "description": "Get 3-5x query speedup on your SQL workloads at the same cost. One-click activation.",
            "score": 30,
            "action": "Enable Now"
        },
        {
            "type": "suggested",
            "title": "\U0001F4CA Real-time Dashboard",
            "description": "Connect streaming data to a live dashboard. Update in seconds, not hours.",
            "score": 80,
            "roi": "10x faster insights",
            "action": "Try Streaming"
        },
        {
            "type": "optimize",
            "title": "\U0001F680 Right-size Your Clusters",
            "description": "We detected idle capacity. Consolidate and reinvest savings into new use cases.",
            "score": 25,
            "action": "Review Clusters"
        }
    ]
    
    # Shuffle to vary the experience
    random.shuffle(cards)
    
    return jsonify({
        "cards": cards[:4],  # Return 4 cards
        "score": 420,  # Placeholder - will be computed from usage data
        "unlocked": 5,
        "total_capabilities": 8
    })


@app.route("/api/cost-stream", methods=["GET"])
def cost_stream():
    """Cost stream disabled — Mission Control removed (P0 perf fix)."""
    return jsonify({
        "timeseries": [], "yesterday": [], "skus": [], "sku_ts": {},
        "total_cost": 0, "yesterday_total": 0, "burn_rate_per_min": 0,
        "projected_daily": 0, "range": "1d", "data_points": 0,
    })


if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", os.environ.get("PORT", 8000)))
    logger.info(f"Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
