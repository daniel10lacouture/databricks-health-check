"""Section: CI/CD & DevOps — checks."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class CICDCheckRunner(BaseCheckRunner):
    section_id = "cicd"
    section_name = "CI/CD & DevOps"
    section_type = "advisory"
    icon = "git-branch"

    def get_subsections(self):
        return ["Deployment Practices"]

    def check_13_1_3_manual_runs(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT
                    SUM(CASE WHEN trigger_type = 'MANUAL' THEN 1 ELSE 0 END) AS manual,
                    COUNT(*) AS total
                FROM system.lakeflow.job_run_timeline
                WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("13.1.3", "Jobs triggered via CI/CD",
                "Deployment Practices", 0, "not_evaluated",
                "Could not query", "<10% manual")
        r = rows[0] if rows else {}
        total = int(r.get("total", 0)) or 1
        manual = int(r.get("manual", 0))
        pct = manual / total * 100
        if pct < 10: score, status = 100, "pass"
        elif pct < 30: score, status = 50, "partial"
        else: score, status = 0, "fail"
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{pct:.0f}% of runs are manual. Automate via scheduled triggers or CI/CD.",
                impact="Manual runs are error-prone and not reproducible.",
                priority="medium")
        return CheckResult("13.1.3", "Jobs triggered via CI/CD",
            "Deployment Practices", score, status,
            f"{pct:.0f}% manual runs ({manual}/{total})",
            "<10% manual", recommendation=rec)

