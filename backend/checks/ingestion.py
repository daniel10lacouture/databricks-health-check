"""Section: Data Ingestion — checks."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class IngestionCheckRunner(BaseCheckRunner):
    section_id = "ingestion"
    section_name = "Data Ingestion"
    section_type = "conditional"
    icon = "download"

    def get_subsections(self):
        return ["Pipeline Inventory", "Pipeline Performance"]

    def is_active(self) -> bool:
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS cnt FROM system.lakeflow.pipelines""")
            return int(rows[0].get("cnt", 0)) > 0 if rows else False
        except Exception:
            return False

    def check_11_1_2_pipeline_status(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT CASE WHEN delete_time IS NULL THEN 'ACTIVE' ELSE 'DELETED' END AS state, COUNT(*) AS cnt FROM system.lakeflow.pipelines GROUP BY 1""")
        except Exception:
            return CheckResult("11.1.2", "DLT pipeline inventory and status",
                "Pipeline Inventory", 0, "not_evaluated",
                "Could not query pipelines", "All healthy")
        detail = {r.get("state","UNKNOWN"): int(r.get("cnt",0)) for r in rows}
        total = sum(detail.values()) or 1
        unhealthy = sum(v for k, v in detail.items() if k not in ("RUNNING", "IDLE"))
        pct = unhealthy / total * 100
        if pct == 0: score, status = 100, "pass"
        elif pct <= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        rec = None
        if unhealthy:
            rec = Recommendation(
                action=f"{unhealthy} pipeline(s) in unhealthy state.",
                impact="Unhealthy pipelines may not process data.",
                priority="high" if pct > 20 else "medium")
        return CheckResult("11.1.2", "DLT pipeline inventory and status",
            "Pipeline Inventory", score, status,
            f"{unhealthy}/{total} unhealthy ({pct:.0f}%)",
            "All pipelines healthy", details=detail, recommendation=rec)

    def check_11_2_1_pipeline_success_rate(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS total,
                    SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success
                FROM system.lakeflow.job_run_timeline
                WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("11.2.1", "Pipeline update success rate",
                "Pipeline Performance", 0, "not_evaluated",
                "Could not query", ">95% success")
        r = rows[0] if rows else {}
        total = int(r.get("total", 0)) or 1
        success = int(r.get("success", 0))
        pct = success / total * 100
        if pct >= 95: score, status = 100, "pass"
        elif pct >= 80: score, status = 50, "partial"
        else: score, status = 0, "fail"
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Pipeline success rate is {pct:.0f}%. Investigate failing pipelines.",
                impact="Failed pipelines delay data freshness.",
                priority="high" if pct < 80 else "medium")
        return CheckResult("11.2.1", "Pipeline update success rate",
            "Pipeline Performance", score, status,
            f"{pct:.0f}% success rate ({success}/{total})",
            ">95% success", recommendation=rec)

    # ── Tier 1: Lakeflow Connector Adoption ──────────────────────────

    def check_11_3_1_connector_adoption(self):
        """Tier 1: 72% managed connectors (1,239/1,730)."""
        try:
            rows = self.executor.execute("""
                SELECT connection_type, COUNT(*) AS cnt
                FROM system.information_schema.connections
                GROUP BY 1 ORDER BY cnt DESC
            """)
        except Exception:
            return CheckResult("11.3.1", "Lakeflow Connector adoption", "Connector Inventory",
                0, "not_evaluated", "system.information_schema.connections not available", "N/A")

        total = sum(r["cnt"] for r in rows)
        if total == 0:
            return CheckResult("11.3.1", "Lakeflow Connector adoption", "Connector Inventory",
                0, "not_evaluated", "No connections found", "N/A")

        managed_types = ['DATABRICKS', 'LAKEHOUSE_FEDERATION', 'ONLINE_TABLE', 'DELTA_SHARING']
        managed = sum(r["cnt"] for r in rows if any(mt in (r.get("connection_type") or "").upper() for mt in managed_types))
        rate = managed / max(total, 1) * 100

        if rate >= 70: score, status = 100, "pass"
        elif rate >= 40: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"connection_type": r["connection_type"], "count": r["cnt"],
               "pct": f"{r['cnt']/max(total,1)*100:.1f}%"} for r in rows]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Increase managed connector usage. Currently {managed}/{total} ({rate:.0f}%) use managed connections.",
                impact="Managed connectors provide centralized governance, credential management, and monitoring.",
                priority="medium")
        return CheckResult("11.3.1", "Lakeflow Connector adoption", "Connector Inventory",
            score, status, f"{rate:.0f}% managed ({managed:,}/{total:,})", "≥70% managed connectors",
            details={"connector_types": nc}, recommendation=rec)

    # ── Tier 2: Streaming Pipeline Inventory ─────────────────────────

    def check_11_3_2_streaming_pipeline_inventory(self):
        """Tier 2: 788 streaming pipelines (437 ST, 278 ingestion, 73 gateway)."""
        try:
            rows = self.executor.execute("""
                SELECT pipeline_type, state, COUNT(*) AS cnt
                FROM system.lakeflow.pipelines
                GROUP BY 1, 2 ORDER BY cnt DESC
            """)
        except Exception:
            return CheckResult("11.3.2", "Streaming pipeline inventory", "Streaming Pipelines",
                0, "not_evaluated", "system.lakeflow.pipelines not available", "N/A")

        total = sum(r["cnt"] for r in rows)
        by_type = {}
        for r in rows:
            t = r.get("pipeline_type", "UNKNOWN")
            by_type[t] = by_type.get(t, 0) + r["cnt"]

        type_detail = [{"pipeline_type": t, "count": c} for t, c in sorted(by_type.items(), key=lambda x: -x[1])]

        # Check for failed pipelines
        failed = sum(r["cnt"] for r in rows if r.get("state") in ("FAILED", "ERROR"))
        failed_rate = failed / max(total, 1) * 100

        return CheckResult("11.3.2", "Streaming pipeline inventory", "Streaming Pipelines",
            0, "info", f"{total} pipelines ({len(by_type)} types)", "N/A",
            details={"pipeline_types": type_detail,
                     "state_breakdown": [{"pipeline_type": r.get("pipeline_type"), "state": r.get("state"), "count": r["cnt"]} for r in rows[:20]]})

