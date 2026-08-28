"""
Genie ONE Ready — a readiness checklist that tells a customer whether their
workspace is set up to roll AI/BI Genie out to non-technical business users,
and gives concrete enablement steps for anything that's missing.

Readiness signals are derived from system tables + the workspace REST API:
  - Unity Catalog governed data (Genie requires UC-registered tables)
  - A Pro or Serverless SQL Warehouse (Genie spaces need one to run)
  - Genie Spaces already in use (is Genie live at all?)
  - Databricks Assistant / partner-powered AI activity (AI features enabled?)
  - Breadth of business-user access (are non-admins actually querying?)

Every check returns pass/partial/fail (never not_evaluated) so the tab always
renders as a checklist, even on a brand-new workspace.
"""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class GenieOneReadyCheckRunner(BaseCheckRunner):
    section_id = "genie_one_ready"
    section_name = "Genie ONE Ready"
    section_type = "advisory"
    icon = "sparkle"

    def get_subsections(self):
        return ["Genie ONE Readiness"]

    def is_active(self) -> bool:
        return True

    # ── 1. Unity Catalog governed data ───────────────────────────────
    def check_g1_unity_catalog(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT table_catalog) AS catalogs,
                       COUNT(*) AS tables
                FROM system.information_schema.tables
                WHERE table_catalog NOT IN ('system', 'hive_metastore', 'samples', '__databricks_internal')
                  AND table_schema NOT IN ('information_schema')""")
            catalogs = int((rows[0] or {}).get("catalogs", 0) or 0) if rows else 0
            tables = int((rows[0] or {}).get("tables", 0) or 0) if rows else 0
        except Exception:
            catalogs, tables = 0, 0

        if catalogs >= 1 and tables >= 10:
            return CheckResult("g1", "Unity Catalog governed data", "Genie ONE Readiness",
                100, "pass",
                f"{tables:,} tables across {catalogs} Unity Catalog catalog(s)",
                "Data registered in Unity Catalog")
        return CheckResult("g1", "Unity Catalog governed data", "Genie ONE Readiness",
            0, "fail",
            f"Only {tables} UC tables in {catalogs} catalog(s)",
            "Data registered in Unity Catalog",
            details={"non_conforming": [{"step": "Register the datasets business users care about in Unity Catalog and add table/column comments — Genie reads UC metadata to answer questions."}]},
            recommendation=Recommendation(
                action="Register business-facing datasets in Unity Catalog and document them (table + column comments). Genie can only answer questions on UC-governed data.",
                impact="Foundation for Genie — without UC data there is nothing for non-technical users to ask about.",
                priority="high",
                docs_url="https://docs.databricks.com/aws/genie/set-up"))

    # ── 2. Pro / Serverless SQL Warehouse ─────────────────────────────
    def check_g2_sql_warehouse(self) -> CheckResult:
        pro_or_serverless = 0
        total = 0
        try:
            for w in self.api.list_warehouses():
                total += 1
                wtype = str(getattr(w, "warehouse_type", "") or "")
                serverless = bool(getattr(w, "enable_serverless_compute", False))
                if serverless or wtype.upper() == "PRO":
                    pro_or_serverless += 1
        except Exception:
            pass

        if pro_or_serverless >= 1:
            return CheckResult("g2", "Pro / Serverless SQL Warehouse", "Genie ONE Readiness",
                100, "pass",
                f"{pro_or_serverless} Pro/Serverless warehouse(s) available",
                "At least one Pro or Serverless SQL warehouse")
        return CheckResult("g2", "Pro / Serverless SQL Warehouse", "Genie ONE Readiness",
            0, "fail",
            f"No Pro/Serverless warehouse found ({total} total)",
            "At least one Pro or Serverless SQL warehouse",
            details={"non_conforming": [{"step": "Create a Serverless (recommended) or Pro SQL Warehouse and grant the business-user group CAN USE on it."}]},
            recommendation=Recommendation(
                action="Create a Serverless or Pro SQL Warehouse and grant your business-user group CAN USE. Genie spaces require a Pro/Serverless warehouse to run queries.",
                impact="Serverless starts instantly and is the smoothest experience for occasional business users.",
                priority="high",
                docs_url="https://docs.databricks.com/aws/genie/set-up"))

    # ── 3. Genie Spaces already live ──────────────────────────────────
    def check_g3_genie_spaces_live(self) -> CheckResult:
        users, queries = 0, 0
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT executed_by) AS users, COUNT(*) AS queries
                FROM system.query.history
                WHERE client_application = 'Databricks SQL Genie Space'
                  AND start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
            if rows:
                users = int(rows[0].get("users", 0) or 0)
                queries = int(rows[0].get("queries", 0) or 0)
        except Exception:
            pass

        if users >= 5:
            return CheckResult("g3", "Genie Spaces in use", "Genie ONE Readiness",
                100, "pass",
                f"{users} users ran {queries:,} Genie queries in 30 days",
                "Genie actively used by business users")
        if users >= 1:
            return CheckResult("g3", "Genie Spaces in use", "Genie ONE Readiness",
                50, "partial",
                f"Only {users} user(s), {queries:,} Genie queries in 30 days",
                "Genie actively used by business users",
                recommendation=Recommendation(
                    action="Genie is live but adoption is thin. Publish curated Genie spaces on your top datasets and share them with business teams with CAN RUN.",
                    impact="Turns a technical pilot into broad self-serve analytics.",
                    priority="medium",
                    docs_url="https://docs.databricks.com/aws/genie/set-up"))
        return CheckResult("g3", "Genie Spaces in use", "Genie ONE Readiness",
            0, "fail",
            "No Genie Space activity in the last 30 days",
            "Genie actively used by business users",
            details={"non_conforming": [{"step": "Create a Genie space on a well-documented dataset, then share it with a business-user group with CAN RUN."}]},
            recommendation=Recommendation(
                action="Create your first Genie space on a well-documented dataset and share it (CAN RUN) with a business-user group. Start with one high-value domain (e.g. sales or ops).",
                impact="First step to self-serve natural-language analytics for non-technical users.",
                priority="high",
                docs_url="https://docs.databricks.com/aws/genie/set-up"))

    # ── 4. Databricks Assistant / partner-powered AI active ───────────
    def check_g4_ai_features_enabled(self) -> CheckResult:
        events = 0
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS events
                FROM system.access.assistant_events
                WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
            if rows:
                events = int(rows[0].get("events", 0) or 0)
        except Exception:
            pass

        if events >= 1:
            return CheckResult("g4", "AI assistant features enabled", "Genie ONE Readiness",
                100, "pass",
                f"{events:,} assistant events in 30 days (AI features active)",
                "Partner-powered AI features enabled")
        return CheckResult("g4", "AI assistant features enabled", "Genie ONE Readiness",
            0, "fail",
            "No assistant activity detected",
            "Partner-powered AI features enabled",
            details={"non_conforming": [{"step": "Account Console → Settings → Feature enablement → turn on 'Enable partner-powered AI features' (required for the full Genie experience)."}]},
            recommendation=Recommendation(
                action="In the Account Console → Settings → Feature enablement, turn on 'Enable partner-powered AI features'. This is required for the full Genie/Assistant experience.",
                impact="Unlocks the AI capabilities Genie relies on.",
                priority="high",
                docs_url="https://docs.databricks.com/aws/en/databricks-ai/partner-powered"))

    # ── 5. Business-user access breadth ───────────────────────────────
    def check_g5_business_user_access(self) -> CheckResult:
        distinct_users = 0
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT executed_by) AS users
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND executed_by IS NOT NULL""")
            if rows:
                distinct_users = int(rows[0].get("users", 0) or 0)
        except Exception:
            pass

        if distinct_users >= 20:
            return CheckResult("g5", "Business-user access breadth", "Genie ONE Readiness",
                100, "pass",
                f"{distinct_users} distinct users querying in 30 days",
                "Broad user access (SQL entitlement + grants)")
        if distinct_users >= 5:
            return CheckResult("g5", "Business-user access breadth", "Genie ONE Readiness",
                50, "partial",
                f"{distinct_users} distinct users querying in 30 days",
                "Broad user access (SQL entitlement + grants)",
                recommendation=Recommendation(
                    action="Access is concentrated in a small group. Create a 'Genie-Users' group, grant it the Databricks SQL entitlement, SELECT on the relevant UC data, and CAN USE on the warehouse.",
                    impact="Expands self-serve analytics beyond the core technical team.",
                    priority="medium",
                    docs_url="https://docs.databricks.com/aws/genie/set-up"))
        return CheckResult("g5", "Business-user access breadth", "Genie ONE Readiness",
            0, "fail",
            f"Only {distinct_users} distinct users querying in 30 days",
            "Broad user access (SQL entitlement + grants)",
            details={"non_conforming": [{"step": "Create a 'Genie-Users' group; grant Databricks SQL entitlement + SELECT on UC data + CAN USE on a warehouse."}]},
            recommendation=Recommendation(
                action="Onboard business users: create a 'Genie-Users' group, grant the Databricks SQL entitlement, SELECT on the relevant UC schemas, and CAN USE on a Serverless warehouse.",
                impact="Removes the access blockers that keep non-technical users off Genie.",
                priority="high",
                docs_url="https://docs.databricks.com/aws/genie/set-up"))

    # ── 6. Automated Identity Management (Seamless Onboarding) ─────────
    # AIM can't be reliably detected from system tables, so this is an
    # informational readiness item (not scored) with concrete enablement steps.
    def check_g6_seamless_onboarding(self) -> CheckResult:
        return CheckResult("g6", "Automated Identity Management (Seamless Onboarding)",
            "Genie ONE Readiness", 0, "info",
            "Recommended for rolling Genie out to all business users at scale",
            "AIM enabled + custom workspace URL",
            details={"non_conforming": [
                {"step": "Enable Automated Identity Management (AIM) so business users are auto-provisioned via your IdP instead of being added by hand."},
                {"step": "Configure a custom (vanity) workspace URL, a prerequisite for seamless onboarding / unified login."},
                {"step": "Federate your identity groups so a 'Genie-Users' group syncs automatically from your IdP."},
            ]},
            recommendation=Recommendation(
                action="Enable Automated Identity Management (Seamless Onboarding) with a custom workspace URL so business users are auto-provisioned from your identity provider — the scalable way to give a large non-technical audience Genie access.",
                impact="Removes manual per-user onboarding, the biggest friction when rolling Genie out org-wide.",
                priority="medium",
                docs_url="https://databricks.atlassian.net/wiki/spaces/UN/pages/4667015512"))
