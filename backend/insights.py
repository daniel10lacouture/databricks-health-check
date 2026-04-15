"""
Pillar 2: "Truly Amazing" insights layer.
Computes maturity scoring, trends, anomaly detection, and what-if scenarios
from existing health check results and system table queries.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

logger = logging.getLogger("health_check.insights")

MATURITY_LEVELS = {
    1: {"label": "Foundational", "description": "Basic Databricks adoption — core features in use, significant optimization opportunities."},
    2: {"label": "Managed", "description": "Growing adoption — some best practices in place, key gaps in governance and cost control."},
    3: {"label": "Optimized", "description": "Solid practices — most checks passing, proactive monitoring, room for advanced features."},
    4: {"label": "Advanced", "description": "Strong maturity — leveraging advanced features, good governance, data-driven operations."},
    5: {"label": "Elite", "description": "Best-in-class — comprehensive adoption, automated governance, continuous optimization."},
}

def compute_maturity(overall_score: Optional[float], section_results: list[dict]) -> dict:
    if overall_score is None:
        return {"level": 0, "label": "Unknown", "description": "Insufficient data.", "section_maturity": {}, "next_level_actions": []}
    if overall_score >= 90: level = 5
    elif overall_score >= 75: level = 4
    elif overall_score >= 60: level = 3
    elif overall_score >= 40: level = 2
    else: level = 1
    meta = MATURITY_LEVELS[level]
    section_maturity = {}
    for sec in section_results:
        if not sec.get("active") or sec.get("score") is None: continue
        s = sec["score"]
        sl = 5 if s >= 90 else 4 if s >= 75 else 3 if s >= 60 else 2 if s >= 40 else 1
        section_maturity[sec["section_id"]] = {"level": sl, "label": MATURITY_LEVELS[sl]["label"], "score": s, "section_name": sec["section_name"]}
    weakest = sorted(section_maturity.values(), key=lambda x: x["score"])[:3]
    next_actions = [f"Improve {w['section_name']} (Level {w['level']}) to match overall maturity." for w in weakest if w["level"] < level]
    return {"level": level, "label": meta["label"], "description": meta["description"], "section_maturity": section_maturity, "next_level_actions": next_actions}

def _trend_direction(values: list[float]) -> str:
    if len(values) < 6: return "insufficient_data"
    recent = sum(values[-3:]) / 3
    prior = sum(values[-6:-3]) / 3
    if prior == 0: return "stable"
    change_pct = (recent - prior) / abs(prior) * 100
    if change_pct > 10: return "increasing"
    elif change_pct < -10: return "decreasing"
    return "stable"

def compute_trends(executor) -> dict:
    trends = {}
    try:
        rows = executor.execute("SELECT date_trunc('week', usage_date) AS week, ROUND(SUM(usage_quantity), 0) AS weekly_dbus FROM system.billing.usage WHERE usage_date >= DATEADD(WEEK, -12, CURRENT_DATE()) GROUP BY 1 ORDER BY 1")
        if rows:
            values = [float(r.get("weekly_dbus", 0) or 0) for r in rows]
            trends["cost"] = {"data": [{"week": str(r.get("week",""))[:10], "value": float(r.get("weekly_dbus",0) or 0)} for r in rows], "direction": _trend_direction(values), "label": "Weekly DBU Spend", "unit": "DBUs"}
    except Exception as e: logger.warning(f"Cost trend failed: {e}")
    try:
        rows = executor.execute("SELECT date_trunc('week', start_time) AS week, ROUND(PERCENTILE_APPROX(total_duration_ms/1000.0, 0.5), 2) AS p50_sec, ROUND(PERCENTILE_APPROX(total_duration_ms/1000.0, 0.95), 2) AS p95_sec, COUNT(*) AS query_count FROM system.query.history WHERE start_time >= DATEADD(WEEK, -12, CURRENT_DATE()) AND statement_type IN ('SELECT','INSERT','MERGE','UPDATE','DELETE') GROUP BY 1 ORDER BY 1")
        if rows:
            values = [float(r.get("p95_sec", 0) or 0) for r in rows]
            trends["query_performance"] = {"data": [{"week": str(r.get("week",""))[:10], "p50_sec": float(r.get("p50_sec",0) or 0), "p95_sec": float(r.get("p95_sec",0) or 0), "query_count": int(r.get("query_count",0) or 0)} for r in rows], "direction": _trend_direction(values), "label": "Query Latency (P95)", "unit": "seconds"}
    except Exception as e: logger.warning(f"Query perf trend failed: {e}")
    try:
        rows = executor.execute("SELECT date_trunc('week', period_start_time) AS week, COUNT(*) AS total_runs, SUM(CASE WHEN result_state IN ('FAILED','TIMEDOUT') THEN 1 ELSE 0 END) AS failures, ROUND(SUM(CASE WHEN result_state IN ('FAILED','TIMEDOUT') THEN 1 ELSE 0 END)*100.0/COUNT(*), 1) AS failure_rate FROM system.lakeflow.job_run_timeline WHERE period_start_time >= DATEADD(WEEK, -12, CURRENT_DATE()) GROUP BY 1 ORDER BY 1")
        if rows:
            values = [float(r.get("failure_rate", 0) or 0) for r in rows]
            trends["job_failures"] = {"data": [{"week": str(r.get("week",""))[:10], "total_runs": int(r.get("total_runs",0) or 0), "failures": int(r.get("failures",0) or 0), "failure_rate": float(r.get("failure_rate",0) or 0)} for r in rows], "direction": _trend_direction(values), "label": "Job Failure Rate", "unit": "%"}
    except Exception as e: logger.warning(f"Job failure trend failed: {e}")
    return trends

def detect_anomalies(executor) -> list[dict]:
    anomalies = []
    try:
        rows = executor.execute("WITH recent AS (SELECT PERCENTILE_APPROX(total_duration_ms/1000.0, 0.95) AS p95 FROM system.query.history WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE()) AND statement_type IN ('SELECT','INSERT','MERGE')), baseline AS (SELECT PERCENTILE_APPROX(total_duration_ms/1000.0, 0.95) AS p95 FROM system.query.history WHERE start_time >= DATEADD(DAY, -37, CURRENT_DATE()) AND start_time < DATEADD(DAY, -7, CURRENT_DATE()) AND statement_type IN ('SELECT','INSERT','MERGE')) SELECT r.p95 AS recent_p95, b.p95 AS baseline_p95, ROUND((r.p95 - b.p95)/NULLIF(b.p95,0)*100, 1) AS change_pct FROM recent r, baseline b")
        if rows and rows[0].get("change_pct") is not None:
            change = float(rows[0]["change_pct"])
            if change > 20:
                anomalies.append({"type": "query_regression", "severity": "high" if change > 50 else "medium", "title": "Query Latency Regression", "message": f"P95 query latency increased {change:.0f}% vs prior 30d baseline ({rows[0].get('baseline_p95',0):.1f}s -> {rows[0].get('recent_p95',0):.1f}s).", "metric": {"recent": rows[0].get("recent_p95"), "baseline": rows[0].get("baseline_p95"), "change_pct": change}})
    except Exception as e: logger.warning(f"Query anomaly detection failed: {e}")
    try:
        rows = executor.execute("WITH recent AS (SELECT ROUND(SUM(usage_quantity), 0) AS dbus FROM system.billing.usage WHERE usage_date >= DATEADD(DAY, -7, CURRENT_DATE())), baseline AS (SELECT ROUND(SUM(usage_quantity)/4.0, 0) AS weekly_avg FROM system.billing.usage WHERE usage_date >= DATEADD(DAY, -35, CURRENT_DATE()) AND usage_date < DATEADD(DAY, -7, CURRENT_DATE())) SELECT r.dbus AS recent_dbus, b.weekly_avg AS baseline_weekly, ROUND((r.dbus - b.weekly_avg)/NULLIF(b.weekly_avg,0)*100, 1) AS change_pct FROM recent r, baseline b")
        if rows and rows[0].get("change_pct") is not None:
            change = float(rows[0]["change_pct"])
            if change > 30:
                anomalies.append({"type": "cost_spike", "severity": "high" if change > 60 else "medium", "title": "Cost Spike Detected", "message": f"Last 7d spend is {change:.0f}% above the prior 4-week weekly average ({rows[0].get('baseline_weekly',0):,.0f} -> {rows[0].get('recent_dbus',0):,.0f} DBUs).", "metric": {"recent": rows[0].get("recent_dbus"), "baseline": rows[0].get("baseline_weekly"), "change_pct": change}})
    except Exception as e: logger.warning(f"Cost anomaly detection failed: {e}")
    try:
        rows = executor.execute("WITH recent AS (SELECT COUNT(*) AS total, SUM(CASE WHEN result_state IN ('FAILED','TIMEDOUT') THEN 1 ELSE 0 END) AS fails FROM system.lakeflow.job_run_timeline WHERE period_start_time >= DATEADD(DAY, -7, CURRENT_DATE())), baseline AS (SELECT SUM(CASE WHEN result_state IN ('FAILED','TIMEDOUT') THEN 1 ELSE 0 END)*1.0/NULLIF(COUNT(*),0) AS fail_rate FROM system.lakeflow.job_run_timeline WHERE period_start_time >= DATEADD(DAY, -35, CURRENT_DATE()) AND period_start_time < DATEADD(DAY, -7, CURRENT_DATE())) SELECT r.total, r.fails, ROUND(r.fails*100.0/NULLIF(r.total,0), 1) AS recent_rate, ROUND(b.fail_rate*100, 1) AS baseline_rate FROM recent r, baseline b")
        if rows and rows[0].get("recent_rate") is not None and rows[0].get("baseline_rate") is not None:
            recent_rate = float(rows[0]["recent_rate"]); baseline_rate = float(rows[0]["baseline_rate"])
            if baseline_rate > 0 and recent_rate > baseline_rate * 2:
                anomalies.append({"type": "job_failure_spike", "severity": "high", "title": "Job Failure Spike", "message": f"Job failure rate doubled: {recent_rate:.1f}% (last 7d) vs {baseline_rate:.1f}% (prior 4 weeks).", "metric": {"recent_rate": recent_rate, "baseline_rate": baseline_rate}})
    except Exception as e: logger.warning(f"Job failure anomaly detection failed: {e}")
    return anomalies

def compute_whatif_scenarios(section_results: list[dict], executor) -> list[dict]:
    scenarios = []
    for sec in section_results:
        if sec.get("section_id") == "compute":
            for check in sec.get("checks", []):
                if "all-purpose" in check.get("name", "").lower() and check.get("status") in ("fail", "partial"):
                    nc = (check.get("details") or {}).get("non_conforming", [])
                    if nc: scenarios.append({"id": "migrate_allpurpose", "title": "Migrate All-Purpose -> Jobs Clusters", "description": f"Converting {len(nc)} all-purpose clusters used for jobs to dedicated jobs clusters reduces cost by ~40-60%.", "estimated_savings": "40-60% on affected cluster DBUs", "effort": "Medium", "affected_items": len(nc)})
                    break
        if sec.get("section_id") == "sql_analytics":
            for check in sec.get("checks", []):
                if "serverless" in check.get("name", "").lower() and check.get("status") in ("fail", "partial"):
                    nc = (check.get("details") or {}).get("non_conforming", [])
                    if nc: scenarios.append({"id": "serverless_warehouses", "title": "Switch Classic -> Serverless Warehouses", "description": f"{len(nc)} classic warehouses could benefit from serverless: instant startup, auto-scaling, zero idle cost.", "estimated_savings": "20-50% from eliminating idle time", "effort": "Low", "affected_items": len(nc)})
                    break
        if sec.get("section_id") == "cost_optimization":
            for check in sec.get("checks", []):
                if "idle" in check.get("name", "").lower() and check.get("status") in ("fail", "partial"):
                    nc = (check.get("details") or {}).get("non_conforming", [])
                    if nc: scenarios.append({"id": "eliminate_idle", "title": "Eliminate Idle Compute Resources", "description": f"{len(nc)} idle resources detected. Terminating or auto-stopping them recovers wasted spend.", "estimated_savings": "Direct savings from idle resource elimination", "effort": "Low", "affected_items": len(nc)})
                    break
    return scenarios

def generate_all_insights(overall_score: Optional[float], section_results: list[dict], executor) -> dict:
    t0 = time.time()
    logger.info("Generating insights...")
    maturity = compute_maturity(overall_score, section_results)
    trends = compute_trends(executor)
    anomalies = detect_anomalies(executor)
    whatif = compute_whatif_scenarios(section_results, executor)
    elapsed = round(time.time() - t0, 1)
    logger.info(f"Insights generated in {elapsed}s")
    return {"maturity": maturity, "trends": trends, "anomalies": anomalies, "whatif_scenarios": whatif, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
