"""Section: Delta Sharing — checks with drill-downs."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class DeltaSharingCheckRunner(BaseCheckRunner):
    section_id = "delta_sharing"
    section_name = "Delta Sharing"
    section_type = "conditional"
    icon = "share-2"

    def get_subsections(self):
        return ["Share Security", "Share Activity & Hygiene"]

    def is_active(self) -> bool:
        try:
            recipients = list(self.api.w.recipients.list())
            return len(recipients) > 0
        except Exception:
            return False

    def check_10_1_4_least_privilege_shares(self) -> CheckResult:
        try:
            shares = list(self.api.w.shares.list())
        except Exception:
            return CheckResult("10.1.4", "Shares follow least privilege",
                "Share Security", 0, "not_evaluated",
                "Could not query shares", "Scoped table shares")
        if not shares:
            return CheckResult("10.1.4", "Shares follow least privilege",
                "Share Security", 100, "pass", "No shares configured", "Scoped table shares",
                details={"non_conforming": [{"summary": "No Delta Sharing shares configured."}]})

        nc = [{"share_name": getattr(s, "name", ""), "owner": getattr(s, "owner", ""),
               "action": "Review share scope — prefer specific tables over full schemas"} for s in shares[:20]]

        return CheckResult("10.1.4", "Shares follow least privilege",
            "Share Security", 0, "info",
            f"{len(shares)} share(s) configured — review scope", "Specific tables, not full catalogs",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action=f"Review {len(shares)} Delta Sharing share(s) for least-privilege access.",
                impact="Over-broad shares expose more data than necessary to external recipients.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/data-sharing/index.html"))

    def check_10_2_1_recipient_activity(self) -> CheckResult:
        try:
            recipients = list(self.api.w.recipients.list())
        except Exception:
            return CheckResult("10.2.1", "Recipient activity",
                "Share Activity & Hygiene", 0, "not_evaluated",
                "Could not query recipients", "All recipients active")
        nc = [{"recipient": getattr(r, "name", ""), "auth_type": str(getattr(r, "authentication_type", "")),
               "owner": getattr(r, "owner", "")} for r in recipients[:20]]

        return CheckResult("10.2.1", "Recipient activity",
            "Share Activity & Hygiene", 0, "info",
            f"{len(recipients)} recipient(s) configured", "All recipients active",
            details={"non_conforming": nc, "summary": "Review recipients for stale or unused configurations."})
