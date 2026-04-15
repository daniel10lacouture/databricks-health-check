"""Section: Databricks Apps — checks with drill-downs."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class AppsCheckRunner(BaseCheckRunner):
    section_id = "apps"
    section_name = "Databricks Apps"
    section_type = "conditional"
    icon = "app-window"

    def get_subsections(self):
        return ["App Inventory", "App Security", "App Health"]

    def is_active(self) -> bool:
        try:
            apps = list(self.api.w.apps.list())
            return len(apps) > 0
        except Exception:
            return False

    def check_8_1_1_app_inventory(self) -> CheckResult:
        """Inventory of deployed Databricks Apps — informational."""
        try:
            apps = list(self.api.w.apps.list())
        except Exception:
            return CheckResult("8.1.1", "App inventory",
                "App Inventory", None, "info",
                "Could not list apps", "Informational")

        nc = []
        for a in apps[:20]:
            status = "Unknown"
            if hasattr(a, 'compute_status') and a.compute_status:
                status = str(a.compute_status.state.value) if a.compute_status.state else "Unknown"
            nc.append({
                "app_name": getattr(a, "name", ""),
                "url": getattr(a, "url", "")[:60],
                "status": status,
                "creator": getattr(a, "creator", "")[:30],
                "created": str(getattr(a, "create_time", ""))[:10]
            })

        return CheckResult("8.1.1", "App inventory",
            "App Inventory", None, "info",
            f"{len(apps)} Databricks App(s) deployed", "Track deployed apps",
            details={"apps": nc, "summary": f"{len(apps)} apps deployed in this workspace"},
            recommendation=None)

    def check_8_1_2_app_descriptions(self) -> CheckResult:
        """Check if apps have descriptions."""
        try:
            apps = list(self.api.w.apps.list())
        except Exception:
            return CheckResult("8.1.2", "App documentation",
                "App Inventory", 0, "not_evaluated",
                "Could not list apps", "All apps documented")

        if not apps:
            return CheckResult("8.1.2", "App documentation",
                "App Inventory", 100, "pass", "No apps to document", "All apps documented")

        documented = sum(1 for a in apps if getattr(a, 'description', None))
        pct = documented / len(apps) * 100
        
        if pct >= 80:
            score, status = 100, "pass"
        elif pct >= 50:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"
        
        undoc = [{"app_name": getattr(a, "name", ""), "action": "Add a description in app settings"}
                 for a in apps if not getattr(a, 'description', None)][:10]

        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Add descriptions to {len(apps) - documented} undocumented app(s).",
                impact="Descriptions help team members understand each app's purpose.",
                priority="low",
                docs_url="https://docs.databricks.com/en/dev-tools/databricks-apps/index.html")

        return CheckResult("8.1.2", "App documentation coverage",
            "App Inventory", score, status,
            f"{pct:.0f}% ({documented}/{len(apps)} apps have descriptions)",
            "≥80% apps documented",
            details={"undocumented_apps": undoc} if undoc else {},
            recommendation=rec)

    def check_8_1_3_app_service_principals(self) -> CheckResult:
        """Check if apps use dedicated service principals."""
        try:
            apps = list(self.api.w.apps.list())
        except Exception:
            return CheckResult("8.1.3", "Apps use service principals",
                "App Security", 0, "not_evaluated",
                "Could not list apps", "All apps use dedicated SPs")

        if not apps:
            return CheckResult("8.1.3", "Apps use service principals",
                "App Security", 100, "pass", "No apps deployed", "All apps use dedicated SPs")

        with_sp = sum(1 for a in apps if getattr(a, 'service_principal_id', None))
        pct = with_sp / len(apps) * 100

        if pct >= 100:
            score, status = 100, "pass"
        elif pct >= 80:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"
        
        apps_info = [{"app_name": getattr(a, "name", ""),
                      "service_principal": getattr(a, "service_principal_name", "N/A"),
                      "sp_id": getattr(a, "service_principal_id", "None")} for a in apps[:15]]

        rec = None
        if score < 100:
            rec = Recommendation(
                action="Configure dedicated service principals for all apps.",
                impact="Dedicated service principals limit blast radius and enable granular permissions.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/dev-tools/databricks-apps/app-development.html")

        return CheckResult("8.1.3", "Apps use dedicated service principals",
            "App Security", score, status,
            f"{with_sp}/{len(apps)} apps have service principals",
            "100% apps use dedicated SPs",
            details={"app_service_principals": apps_info},
            recommendation=rec)

    def check_8_1_4_app_compute_health(self) -> CheckResult:
        """Check app compute status — are apps running or stopped?"""
        try:
            apps = list(self.api.w.apps.list())
        except Exception:
            return CheckResult("8.1.4", "App compute health",
                "App Health", 0, "not_evaluated",
                "Could not list apps", "All apps healthy")

        if not apps:
            return CheckResult("8.1.4", "App compute health",
                "App Health", 100, "pass", "No apps deployed", "All apps healthy")

        statuses = {}
        for a in apps:
            state = "UNKNOWN"
            if hasattr(a, 'compute_status') and a.compute_status:
                state = str(a.compute_status.state.value) if a.compute_status.state else "UNKNOWN"
            statuses[state] = statuses.get(state, 0) + 1

        active = statuses.get("ACTIVE", 0)
        stopped = statuses.get("STOPPED", 0)
        error = statuses.get("ERROR", 0) + statuses.get("UNKNOWN", 0)
        
        if error == 0:
            score, status = 100, "pass"
        elif error <= 1:
            score, status = 50, "partial"
        else:
            score, status = 0, "fail"

        nc = [{"app_name": getattr(a, "name", ""),
               "compute_state": str(a.compute_status.state.value) if hasattr(a, 'compute_status') and a.compute_status and a.compute_status.state else "UNKNOWN",
               "message": str(a.compute_status.message)[:60] if hasattr(a, 'compute_status') and a.compute_status else ""}
              for a in apps[:15]]

        rec = None
        if error > 0:
            rec = Recommendation(
                action=f"Investigate {error} app(s) with compute errors.",
                impact="Apps with compute issues may be unavailable to users.",
                priority="high" if error > 1 else "medium",
                docs_url="https://docs.databricks.com/en/dev-tools/databricks-apps/troubleshoot.html")

        summary = f"{active} active, {stopped} stopped" + (f", {error} error" if error else "")
        return CheckResult("8.1.4", "App compute health",
            "App Health", score, status,
            summary, "0 apps in error state",
            details={"app_compute_status": nc, "summary": f"Compute status: {summary}"},
            recommendation=rec)

    def check_8_1_5_app_resources(self) -> CheckResult:
        """Check if apps have resources configured — informational."""
        try:
            apps = list(self.api.w.apps.list())
        except Exception:
            return CheckResult("8.1.5", "App resources",
                "App Health", None, "info",
                "Could not list apps", "N/A")

        if not apps:
            return CheckResult("8.1.5", "App resources",
                "App Health", None, "info", "No apps deployed", "N/A")

        resource_counts = {}
        apps_with_resources = 0
        for a in apps:
            resources = getattr(a, 'resources', []) or []
            if resources:
                apps_with_resources += 1
            for r in resources:
                rtype = getattr(r, 'name', 'unknown')
                resource_counts[rtype] = resource_counts.get(rtype, 0) + 1

        nc = [{"resource_type": k, "count": v} for k, v in sorted(resource_counts.items(), key=lambda x: -x[1])]

        return CheckResult("8.1.5", "App resource configuration",
            "App Health", None, "info",
            f"{apps_with_resources}/{len(apps)} apps have resources configured",
            "Review resource bindings",
            details={"resource_types": nc,
                     "summary": f"Apps reference {sum(resource_counts.values())} total resources"},
            recommendation=None)
