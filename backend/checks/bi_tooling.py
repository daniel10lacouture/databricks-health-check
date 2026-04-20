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
    section_id = "aibi_dashboards"
    section_name = "AI/BI Dashboards"
    section_type = "conditional"
    icon = "bar-chart-2"

    def get_subsections(self):
        return ["BI Tool Landscape", "AI/BI Adoption", "Developer Tooling", "Dashboard Engagement"]

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
    def check_14_1_4_warehouse_bi_usage(self) -> CheckResult:
        """Show which warehouses power AI/BI Dashboards and Genie spaces."""
        try:
            rows = self.executor.execute("""
                WITH active AS (
                    SELECT compute.warehouse_id AS warehouse_id,
                           COUNT(CASE WHEN client_application = 'Databricks SQL Dashboard' THEN 1 END) AS dashboard_queries,
                           COUNT(CASE WHEN client_application = 'Databricks SQL Genie Space' THEN 1 END) AS genie_queries,
                           COUNT(*) AS total_queries
                    FROM system.query.history
                    WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                      AND compute.warehouse_id IS NOT NULL
                    GROUP BY 1
                )
                SELECT w.warehouse_name, w.warehouse_type, a.dashboard_queries, a.genie_queries, a.total_queries
                FROM active a
                JOIN system.compute.warehouses w ON a.warehouse_id = w.warehouse_id AND w.delete_time IS NULL
                WHERE a.dashboard_queries > 0 OR a.genie_queries > 0
                ORDER BY a.dashboard_queries + a.genie_queries DESC
                LIMIT 20""")
        except Exception as e:
            return CheckResult("14.1.4", "Warehouses powering AI/BI", "Dashboard & Genie Analytics",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "Warehouses serving dashboards")

        if not rows:
            return CheckResult("14.1.4", "Warehouses powering AI/BI", "Dashboard & Genie Analytics",
                0, "fail", "No warehouses serving AI/BI dashboards or Genie", "Warehouses serving dashboards")

        nc = [{"warehouse": r.get("warehouse_name",""), "type": r.get("warehouse_type",""),
               "dashboard_queries": r.get("dashboard_queries",0), "genie_queries": r.get("genie_queries",0)} for r in rows[:10]]
        total_dash = sum(int(r.get("dashboard_queries",0)) for r in rows)
        total_genie = sum(int(r.get("genie_queries",0)) for r in rows)

        return CheckResult("14.1.4", "Warehouses powering AI/BI", "Dashboard & Genie Analytics",
            100, "pass",
            f"{len(rows)} warehouses serving {total_dash:,} dashboard + {total_genie:,} Genie queries",
            "Warehouses serving dashboards",
            details={"non_conforming": nc})


    def check_14_1_5_genie_space_activity(self) -> CheckResult:
        """Track Genie space activity by user count — informational."""
        try:
            rows = self.executor.execute("""
                SELECT DATE(start_time) AS query_date,
                       COUNT(DISTINCT executed_by) AS genie_users,
                       COUNT(*) AS genie_queries
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND client_application = 'Databricks SQL Genie Space'
                GROUP BY 1 ORDER BY 1 DESC
            """)
        except Exception as e:
            return CheckResult("14.1.5", "Genie space activity trend",
                "AI/BI Adoption", None, "info",
                f"Could not query: {str(e)[:50]}", "N/A")

        if not rows:
            return CheckResult("14.1.5", "Genie space activity trend",
                "AI/BI Adoption", None, "info",
                "No Genie activity in last 30 days", "N/A")

        total_users = len(set(r["genie_users"] for r in rows))  # Approximation
        total_queries = sum(r["genie_queries"] for r in rows)
        avg_daily_users = sum(r["genie_users"] for r in rows) / max(len(rows), 1)

        trend = [{"date": str(r["query_date"]), "users": r["genie_users"], 
                  "queries": r["genie_queries"]} for r in rows[:14]]

        return CheckResult("14.1.5", "Genie space activity trend",
            "AI/BI Adoption", None, "info",
            f"~{avg_daily_users:.0f} daily users, {total_queries:,} queries in 30d",
            "Track Genie adoption",
            details={"daily_trend": trend,
                     "summary": f"Genie is averaging {avg_daily_users:.0f} users/day"},
            recommendation=None)

    # ── Dashboard Engagement ─────────────────────────────────────────

    def check_14_2_1_dashboard_sharing(self) -> CheckResult:
        """Check if AI/BI dashboards are shared or remain private-only."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS total_dash_queries,
                       COUNT(DISTINCT executed_by) AS unique_users
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND client_application = 'Databricks SQL Dashboard'""")
        except Exception as e:
            return CheckResult("14.2.1", "Dashboard Sharing & Collaboration", "Dashboard Engagement",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "Dashboards shared broadly")

        total = int(rows[0].get("total_dash_queries", 0)) if rows else 0
        users = int(rows[0].get("unique_users", 0)) if rows else 0

        if total == 0:
            return CheckResult("14.2.1", "Dashboard Sharing & Collaboration", "Dashboard Engagement",
                0, "fail", "No dashboard queries in last 30d", "Dashboards shared broadly")

        if users >= 10: score, status = 100, "pass"
        elif users >= 3: score, status = 60, "partial"
        else: score, status = 30, "fail"

        nc = [{"metric": "Dashboard queries", "value": f"{total:,}"},
              {"metric": "Unique users", "value": f"{users}"}]

        return CheckResult("14.2.1", "Dashboard Sharing & Collaboration", "Dashboard Engagement",
            score, status,
            f"{total:,} dashboard queries by {users} users (30d)",
            "Dashboards shared broadly",
            details={"non_conforming": nc})


    def check_14_2_2_dashboard_query_freshness(self) -> CheckResult:
        """Check if dashboard queries are running regularly (not stale)."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS recent_queries,
                       COUNT(CASE WHEN start_time >= DATEADD(DAY, -7, CURRENT_DATE()) THEN 1 END) AS last_7d,
                       ROUND(AVG(total_duration_ms), 0) AS avg_duration_ms
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND client_application = 'Databricks SQL Dashboard'""")
        except Exception as e:
            return CheckResult("14.2.2", "Dashboard Query Freshness", "Dashboard Engagement",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "Regular dashboard refreshes")

        total = int(rows[0].get("recent_queries", 0)) if rows else 0
        last_7d = int(rows[0].get("last_7d", 0)) if rows else 0
        avg_ms = float(rows[0].get("avg_duration_ms", 0) or 0) if rows else 0

        if total == 0:
            return CheckResult("14.2.2", "Dashboard Query Freshness", "Dashboard Engagement",
                0, "fail", "No dashboard queries in last 30d", "Regular dashboard refreshes")

        freshness = last_7d / total * 100 if total > 0 else 0
        if freshness >= 20: score, status = 100, "pass"
        elif freshness >= 5: score, status = 60, "partial"
        else: score, status = 30, "fail"

        return CheckResult("14.2.2", "Dashboard Query Freshness", "Dashboard Engagement",
            score, status,
            f"{total:,} queries (30d), {last_7d:,} in last 7d ({freshness:.0f}% recent), avg {avg_ms/1000:.1f}s",
            "Regular dashboard refreshes",
            details={"non_conforming": [{"total_30d": total, "last_7d": last_7d, "avg_duration_s": round(avg_ms/1000,1)}]})


