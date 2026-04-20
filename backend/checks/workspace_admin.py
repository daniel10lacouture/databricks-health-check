"""Section: Workspace Administration — checks for workspace health, user activity,
User activity and CI/CD practices. Merges former CI/CD section.
All checks include drill-down details with actual objects and recommendations."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class WorkspaceAdminCheckRunner(BaseCheckRunner):
    section_id = "governance"
    section_name = "Governance"
    section_type = "core"
    icon = "settings"

    def get_subsections(self):
        return ["User & Resource Activity", "Deployment Practices"]

    def check_12_2_1_active_users(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT user_identity.email AS user_email, COUNT(*) AS event_count
                FROM system.access.audit
                WHERE event_time >= DATEADD(DAY, -90, CURRENT_DATE())
                    AND user_identity.email IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 30""")
        except Exception:
            return CheckResult("12.2.1", "Active vs. inactive users",
                "User & Resource Activity", 0, "not_evaluated",
                "Could not query audit logs", "<20% inactive")
        active = len(rows)
        nc = [{"user": r.get("user_email",""), "events_90d": r.get("event_count",0)} for r in rows[:20]]
        return CheckResult("12.2.1", "Active users (last 90 days)",
            "User & Resource Activity", 0, "info",
            f"{active} active users in last 90 days", "Informational",
            details={"non_conforming": nc, "summary": f"Top {min(active, 20)} users by activity (90 days)."})

    def check_12_2_5_runtime_spread(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT dbr_version, COUNT(*) AS cluster_count
                FROM system.compute.clusters
                WHERE delete_time IS NULL AND cluster_source IN ('UI', 'API') AND dbr_version IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC""")
        except Exception:
            return CheckResult("12.2.5", "Runtime version health",
                "User & Resource Activity", 0, "not_evaluated",
                "Could not query", "All on supported versions")
        supported_prefixes = ["15.4", "16.", "17.", "18."]
        total = sum(int(r.get("cluster_count", 0)) for r in rows)
        eol = [r for r in rows if not any(str(r.get("dbr_version","")).startswith(p) for p in supported_prefixes)]
        eol_count = sum(int(r.get("cluster_count", 0)) for r in eol)
        eol_pct = (eol_count / max(total, 1)) * 100
        if eol_pct == 0: score, status = 100, "pass"
        elif eol_pct <= 10: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"dbr_version": r.get("dbr_version",""), "cluster_count": r.get("cluster_count",0),
               "status": "EOL" if r in eol else "OK"} for r in rows[:20]]
        rec = None
        if eol_count:
            rec = Recommendation(
                action=f"Upgrade {eol_count} cluster(s) from EOL runtimes.",
                impact="EOL runtimes miss security patches and performance improvements.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/release-notes/runtime/index.html")
        return CheckResult("12.2.5", "Runtime version health",
            "User & Resource Activity", score, status,
            f"{eol_count}/{total} interactive clusters on EOL runtimes",
            "All on supported versions (15.4 LTS+)",
            details={"non_conforming": nc}, recommendation=rec)

    # ── AI Assistant Adoption (Tier 1) ────────────────────────────────

    def check_12_4_1_manual_runs(self) -> CheckResult:
        """Merged from CI/CD section: Jobs triggered manually vs automated."""
        try:
            rows = self.executor.execute("""
                SELECT j.job_id, j.name AS job_name,
                    SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) AS manual_runs,
                    COUNT(*) AS total_runs,
                    ROUND(SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS manual_pct
                FROM system.lakeflow.job_run_timeline r
                JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
                WHERE r.period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND j.delete_time IS NULL
                GROUP BY 1, 2
                HAVING COUNT(*) >= 5 AND SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 50
                ORDER BY 3 DESC
                LIMIT 20""")
            stats = self.executor.execute("""
                SELECT SUM(CASE WHEN trigger_type = 'MANUAL' THEN 1 ELSE 0 END) AS manual,
                    COUNT(*) AS total
                FROM system.lakeflow.job_run_timeline
                WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("12.4.1", "Jobs triggered via CI/CD vs manual",
                "Deployment Practices", 0, "not_evaluated",
                "Could not query", "<10% manual")
        s = stats[0] if stats else {}
        total = int(s.get("total", 0)) or 1
        manual = int(s.get("manual", 0))
        pct = manual / total * 100
        if pct < 10: score, status = 100, "pass"
        elif pct < 30: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"job_name": r.get("job_name",""), "job_id": r.get("job_id",""),
               "manual_runs": r.get("manual_runs",0), "total_runs": r.get("total_runs",0),
               "manual_pct": r.get("manual_pct",0),
               "action": "Add a schedule, file-arrival, or CI/CD trigger"} for r in rows[:20]]
        if not nc and pct < 10:
            nc = [{"summary": f"Only {pct:.0f}% of runs are manual — well automated."}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{pct:.0f}% of runs are manual ({manual}/{total}). Automate via scheduled triggers or CI/CD.",
                impact="Manual runs are error-prone and not reproducible.",
                priority="medium")
        return CheckResult("12.4.1", "Jobs triggered via CI/CD vs manual",
            "Deployment Practices", score, status,
            f"{pct:.0f}% manual runs ({manual}/{total})",
            "<10% manual runs", details={"non_conforming": nc}, recommendation=rec)
