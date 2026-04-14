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

