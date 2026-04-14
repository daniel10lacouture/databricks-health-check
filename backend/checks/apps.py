"""Section: Databricks Apps — checks with drill-downs."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class AppsCheckRunner(BaseCheckRunner):
    section_id = "apps"
    section_name = "Databricks Apps"
    section_type = "conditional"
    icon = "app-window"

    def get_subsections(self):
        return ["App Configuration"]

    def is_active(self) -> bool:
        try:
            apps = list(self.api.w.apps.list())
            return len(apps) > 0
        except Exception:
            return False

    def check_8_1_1_app_inventory(self) -> CheckResult:
        try:
            apps = list(self.api.w.apps.list())
        except Exception:
            return CheckResult("8.1.1", "App inventory",
                "App Configuration", 0, "not_evaluated",
                "Could not list apps", "Informational")

        nc = [{"app_name": getattr(a, "name", ""), "status": str(getattr(a, "status", "")),
               "create_time": str(getattr(a, "create_time", ""))[:19]} for a in apps[:20]]

        return CheckResult("8.1.1", "App inventory",
            "App Configuration", 0, "info",
            f"{len(apps)} Databricks App(s) deployed", "Informational",
            details={"non_conforming": nc, "summary": "Review each app uses a dedicated service principal with minimal permissions."},
            recommendation=Recommendation(
                action="Ensure each app uses a dedicated service principal with least-privilege OAuth scopes.",
                impact="Shared service principals increase blast radius if compromised.",
                priority="low",
                docs_url="https://docs.databricks.com/en/dev-tools/databricks-apps/index.html"))
