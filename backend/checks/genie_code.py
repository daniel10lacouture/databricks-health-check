"""
Genie Code Adoption check — moved from workspace_admin.py.
Reports under Gen AI & ML section.
"""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class GenieCodeCheckRunner(BaseCheckRunner):
    section_id = "genai_ml"
    section_name = "Gen AI & ML"
    section_type = "core"
    icon = "brain"

    def get_subsections(self):
        return ["Genie Code Adoption"]

    def check_12_3_1_genie_code_adoption(self) -> CheckResult:
        """Tier 1: Genie Code usage — measure Genie Code adoption across your workspace."""
        try:
            rows = self.executor.execute("""
                SELECT initiated_by, COUNT(*) AS events
                FROM system.access.assistant_events
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 30""")
            total_events = sum(int(r.get("events",0)) for r in rows)
            total_users = len(rows)
        except Exception:
            return CheckResult("12.3.1", "Genie Code adoption",
                "Genie Code Adoption", 0, "not_evaluated",
                "Could not query assistant events", "Active Assistant usage")

        if total_events == 0:
            return CheckResult("12.3.1", "Genie Code adoption",
                "Genie Code Adoption", 0, "fail",
                "No Assistant usage detected", "Active Assistant usage",
                details={"non_conforming": [{"summary": "No Genie Code events found in last 30 days."}]},
                recommendation=Recommendation(
                    action="Enable and promote Genie Code for code generation, debugging, and SQL authoring.",
                    impact="Genie Code can dramatically increase developer productivity.",
                    priority="low",
                    docs_url="https://docs.databricks.com/en/notebooks/use-databricks-assistant.html"))

        nc = [{"user": r.get("initiated_by",""), "events_30d": r.get("events",0)} for r in rows[:20]]

        # If >100 users, excellent; >20, good
        if total_users >= 100: score, status = 100, "pass"
        elif total_users >= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"

        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{total_users} users are using Genie Code ({total_events:,} events in 30d). Promote adoption to more team members.",
                impact="Genie Code accelerates development through code generation, debugging, and natural language querying.",
                priority="low",
                docs_url="https://docs.databricks.com/en/notebooks/use-databricks-assistant.html")

        return CheckResult("12.3.1", "Genie Code adoption",
            "Genie Code Adoption", score, status,
            f"{total_users} users, {total_events:,} events in 30 days",
            "Active Assistant usage across team",
            details={"non_conforming": nc}, recommendation=rec)

    # ── Deployment Practices (merged from CI/CD) ─────────────────────
