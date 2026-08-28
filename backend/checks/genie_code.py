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

    # Each Databricks user gets 150 free Genie DBUs per calendar month; usage beyond
    # that is billed. This surfaces per-user consumption against that allotment.
    GENIE_FREE_ALLOTMENT = 150

    def check_12_3_2_genie_free_dbu_usage(self) -> CheckResult:
        """Genie free-tier DBU consumption per user vs the 150 DBU/user/month allotment."""
        rows = self.executor.execute("""
            SELECT
                identity_metadata.run_as AS user_name,
                ROUND(SUM(usage_quantity), 1) AS genie_dbus
            FROM system.billing.usage
            WHERE billing_origin_product = 'GENIE'
              AND usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
              AND identity_metadata.run_as IS NOT NULL
            GROUP BY 1
            ORDER BY genie_dbus DESC
            LIMIT 100""")

        if not rows:
            # No Genie billing this month → nothing to surface (hidden by no-N/A policy).
            return CheckResult("12.3.2", "Genie free-tier DBU usage",
                "Genie Code Adoption", 0, "not_evaluated",
                "No Genie DBU usage this month", f"Within {self.GENIE_FREE_ALLOTMENT} DBU/user free tier")

        allot = self.GENIE_FREE_ALLOTMENT
        users = len(rows)
        total_dbus = sum(float(r.get("genie_dbus", 0) or 0) for r in rows)
        over = [r for r in rows if float(r.get("genie_dbus", 0) or 0) >= allot]
        approaching = [r for r in rows if allot * 0.8 <= float(r.get("genie_dbus", 0) or 0) < allot]

        nc = [{
            "user": r.get("user_name", ""),
            "genie_dbus_mtd": round(float(r.get("genie_dbus", 0) or 0), 1),
            "free_dbus_remaining": round(max(0.0, allot - float(r.get("genie_dbus", 0) or 0)), 1),
            "status": "Over free tier" if float(r.get("genie_dbus", 0) or 0) >= allot
                      else ("Approaching limit" if float(r.get("genie_dbus", 0) or 0) >= allot * 0.8
                            else "Within free tier"),
        } for r in rows[:30]]

        if over:
            score, status = 50, "partial"
            current = f"{users} Genie users this month · {len(over)} over the {allot} free DBU/user allotment (billed overage)"
            rec = Recommendation(
                action=f"{len(over)} user(s) exceeded the {allot} free Genie DBU/month allotment and are now billed. "
                       f"Review heavy users, and consider a per-user Budget alert in the Account Console to catch overages early.",
                impact="Avoids surprise Genie charges while keeping high-value power users productive.",
                priority="medium",
                docs_url="https://docs.databricks.com/aws/en/genie/monitor-cost")
        elif approaching:
            score, status = 100, "pass"
            current = f"{users} Genie users this month · {len(approaching)} approaching the {allot} free DBU allotment · all within free tier"
            rec = Recommendation(
                action=f"{len(approaching)} user(s) are at 80%+ of the {allot} free Genie DBU allotment. "
                       f"No action needed yet — monitor so they don't tip into billed usage.",
                impact="Proactive cost visibility for Genie adoption.",
                priority="low",
                docs_url="https://docs.databricks.com/aws/en/genie/monitor-cost")
        else:
            score, status = 100, "pass"
            current = f"{users} Genie users this month · all comfortably within the {allot} free DBU/user allotment"
            rec = None

        return CheckResult("12.3.2", "Genie free-tier DBU usage",
            "Genie Code Adoption", score, status,
            current,
            f"Users within {allot} free DBU/month allotment",
            details={"non_conforming": nc,
                     "summary_stat": {"genie_users": users, "total_genie_dbus_mtd": round(total_dbus, 1),
                                      "users_over_free_tier": len(over), "free_allotment_per_user": allot}},
            recommendation=rec)

    # ── Deployment Practices (merged from CI/CD) ─────────────────────
