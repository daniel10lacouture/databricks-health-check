"""Section: BI & Tooling Adoption — detect BI tools, recommend AI/BI, measure platform adoption."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


# Known BI tools that appear in client_application
EXTERNAL_BI_TOOLS = {
    "Tableau": "tableau",
    "Power BI": "power_bi", 
    "Looker": "looker",
    "Qlik": "qlik",
    "Mode": "mode",
    "Sigma": "sigma",
    "ThoughtSpot": "thoughtspot",
    "Metabase": "metabase",
    "Redash": "redash",
    "Quest": "quest",
    "Hex": "hex",
    "dbt": "dbt",
}

DATABRICKS_BI_TOOLS = {
    "Databricks SQL Dashboard": "dashboards",
    "Databricks SQL Genie Space": "genie",
    "Databricks Catalog Explorer": "catalog_explorer",
    "Databricks SQL Editor": "sql_editor",
    "Databricks Notebooks": "notebooks",
}


class BIToolingCheckRunner(BaseCheckRunner):
    section_id = "bi_tooling"
    section_name = "BI & Tooling Adoption"
    section_type = "advisory"
    icon = "bar-chart-2"

    def get_subsections(self):
        return ["BI Tool Landscape", "AI/BI Adoption", "Developer Tooling"]

    def check_14_1_1_bi_tool_landscape(self) -> CheckResult:
        """Detect what BI tools are connecting to Databricks."""
        try:
            rows = self.executor.execute("""
                SELECT client_application, COUNT(*) AS query_count,
                    COUNT(DISTINCT executed_by) AS distinct_users
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND client_application IS NOT NULL
                  AND client_application != ''
                GROUP BY 1
                ORDER BY 2 DESC""")
        except Exception:
            return CheckResult("14.1.1", "BI tool landscape",
                "BI Tool Landscape", 0, "not_evaluated",
                "Could not query", "Informational")
        
        external = []
        databricks_native = []
        other = []
        for r in rows:
            app = r.get("client_application", "")
            queries = int(r.get("query_count", 0))
            users = int(r.get("distinct_users", 0))
            entry = {"tool": app, "queries": queries, "users": users}
            
            matched_ext = False
            for name, key in EXTERNAL_BI_TOOLS.items():
                if name.lower() in app.lower():
                    external.append(entry)
                    matched_ext = True
                    break
            if not matched_ext:
                matched_db = False
                for name, key in DATABRICKS_BI_TOOLS.items():
                    if name.lower() in app.lower():
                        databricks_native.append(entry)
                        matched_db = True
                        break
                if not matched_db and queries > 100:
                    other.append(entry)
        
        total_ext_queries = sum(e["queries"] for e in external)
        total_db_queries = sum(e["queries"] for e in databricks_native)
        
        details = {
            "external_bi_tools": external[:10],
            "databricks_native": databricks_native[:10],
            "other_tools": other[:10],
            "external_query_volume": total_ext_queries,
            "native_query_volume": total_db_queries,
        }
        
        rec = None
        if external:
            tool_names = ", ".join(e["tool"] for e in external[:3])
            rec = Recommendation(
                action=f"External BI tools detected: {tool_names}. Evaluate AI/BI Dashboards and Genie as native alternatives to reduce license costs.",
                impact=f"{total_ext_queries:,} queries/month from external tools could leverage built-in AI/BI capabilities.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/dashboards/index.html")
        
        return CheckResult("14.1.1", "BI tool landscape",
            "BI Tool Landscape", 0, "info",
            f"{len(external)} external BI tool(s), {len(databricks_native)} native tool(s) detected",
            "Informational", details=details, recommendation=rec)

    def check_14_1_2_aibi_dashboard_adoption(self) -> CheckResult:
        """Check AI/BI Dashboard usage vs total SQL queries."""
        try:
            rows = self.executor.execute("""
                SELECT 
                    SUM(CASE WHEN client_application = 'Databricks SQL Dashboard' THEN 1 ELSE 0 END) AS dashboard_queries,
                    SUM(CASE WHEN client_application = 'Databricks SQL Genie Space' THEN 1 ELSE 0 END) AS genie_queries,
                    COUNT(*) AS total_queries
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("14.1.2", "AI/BI Dashboard & Genie adoption",
                "AI/BI Adoption", 0, "not_evaluated",
                "Could not query", ">10% queries from AI/BI")
        
        r = rows[0] if rows else {}
        dash = int(r.get("dashboard_queries", 0))
        genie = int(r.get("genie_queries", 0))
        total = int(r.get("total_queries", 1)) or 1
        aibi_pct = (dash + genie) / total * 100
        
        if aibi_pct >= 10: score, status = 100, "pass"
        elif aibi_pct >= 2: score, status = 50, "partial"
        else: score, status = 0, "fail"
        
        details = {"dashboard_queries": dash, "genie_queries": genie, "total_queries": total}
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"AI/BI tools account for {aibi_pct:.1f}% of queries. Increase adoption of Dashboards and Genie.",
                impact="AI/BI Dashboards and Genie provide self-service analytics with AI assistance, reducing dependency on external tools.",
                priority="low",
                docs_url="https://docs.databricks.com/en/dashboards/index.html")
        
        return CheckResult("14.1.2", "AI/BI Dashboard & Genie adoption",
            "AI/BI Adoption", score, status,
            f"{aibi_pct:.1f}% of queries from AI/BI ({dash:,} dashboard + {genie:,} Genie)",
            ">10% queries from AI/BI tools", details=details, recommendation=rec)

    def check_14_1_3_developer_tooling(self) -> CheckResult:
        """Check developer tool usage patterns."""
        try:
            rows = self.executor.execute("""
                SELECT client_application, COUNT(*) AS queries, COUNT(DISTINCT executed_by) AS users
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND client_application IN ('Databricks CLI', 'Databricks Notebooks', 'SPARK_CONNECT',
                      'Databricks SQL Connector for Python', 'Python', 'mlflow')
                GROUP BY 1 ORDER BY 2 DESC""")
        except Exception:
            return CheckResult("14.1.3", "Developer tooling usage",
                "Developer Tooling", 0, "not_evaluated",
                "Could not query", "Informational")
        
        details = {"tools": [{"tool": r.get("client_application",""), "queries": r.get("queries",0), "users": r.get("users",0)} for r in rows]}
        
        return CheckResult("14.1.3", "Developer tooling usage",
            "Developer Tooling", 0, "info",
            f"{len(rows)} developer tools active in last 30 days",
            "Informational", details=details)
