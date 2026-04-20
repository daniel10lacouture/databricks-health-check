"""
Data Storage health checks.
Section 11: Delta format, table maintenance, staleness, documentation, storage optimization.
"""
from __future__ import annotations
import logging
from checks.base import (BaseCheckRunner, CheckResult, Recommendation, Status, Priority)

logger = logging.getLogger("health_check")


class DataStorageCheckRunner(BaseCheckRunner):
    section_id = "data_storage"
    section_name = "Data Storage"
    section_type = "core"
    icon = "💾"

    def get_subsections(self):
        return ["Format & Layout", "Table Maintenance", "Storage Optimization", "Data Freshness & Lifecycle", "Lakehouse Federation"]

    def is_active(self):
        try:
            r = self.executor.execute("""
                SELECT COUNT(*) AS cnt FROM system.information_schema.tables
                WHERE table_type IN ('MANAGED', 'EXTERNAL', 'BASE TABLE')
                  AND table_schema != 'information_schema' AND table_catalog != 'system'
                LIMIT 1""")
            return r[0]["cnt"] > 0 if r else False
        except Exception:
            return True

    # ── Format & Layout ──────────────────────────────────────────

    def check_11_1_1_delta_format_adoption(self) -> CheckResult:
        """Percentage of tables using Delta format vs other formats."""
        try:
            rows = self.executor.execute("""
                SELECT data_source_format, COUNT(*) AS cnt
                FROM system.information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'default')
                  AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore', 'samples')
                  AND table_type IN ('MANAGED', 'EXTERNAL', 'BASE TABLE')
                GROUP BY data_source_format""")
        except Exception as e:
            return CheckResult("11.1.1", "Delta format adoption", "Format & Layout",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "100% Delta")
        total = sum(int(r.get("cnt", 0) or 0) for r in rows) or 1
        delta = sum(int(r.get("cnt", 0) or 0) for r in rows
                    if (r.get("data_source_format") or "").upper() in ("DELTA", ""))
        pct = round(delta / total * 100, 1)
        score = 100 if pct >= 95 else 50 if pct >= 70 else 0
        status = "pass" if score == 100 else "partial" if score == 50 else "fail"
        nc = [{"format": r.get("data_source_format", "UNKNOWN"), "count": r.get("cnt", 0)}
              for r in rows if (r.get("data_source_format") or "").upper() not in ("DELTA", "")]
        rec = Recommendation(
            action=f"{pct}% Delta adoption. Migrate remaining tables to Delta for ACID, time travel, and optimization.",
            impact="Non-Delta tables miss OPTIMIZE, Z-ORDER, Liquid Clustering, and Predictive Optimization.",
            priority="high" if pct < 70 else "medium",
            docs_url="https://docs.databricks.com/en/delta/index.html") if score < 100 else None
        return CheckResult("11.1.1", "Delta format adoption", "Format & Layout",
            score, status, f"{pct}% Delta ({delta}/{total} tables)", "100% Delta",
            details={"non_conforming": nc, "delta_count": delta, "total": total}, recommendation=rec)

    def check_11_1_2_liquid_clustering_adoption(self) -> CheckResult:
        """Tables using Liquid Clustering vs traditional partitioning."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS total_tables,
                       SUM(CASE WHEN comment LIKE '%liquid%' OR comment LIKE '%cluster_by%' THEN 1 ELSE 0 END) AS lc_tables
                FROM system.information_schema.tables
                WHERE table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore', 'samples')
                  AND table_schema != 'information_schema'
                  AND table_type IN ('MANAGED', 'EXTERNAL', 'BASE TABLE')""")
        except Exception:
            return CheckResult("11.1.2", "Liquid Clustering adoption", "Format & Layout",
                None, "info", "Could not determine clustering adoption", "Track adoption")
        r = rows[0] if rows else {}
        total = int(r.get("total_tables", 0) or 0) or 1
        lc = int(r.get("lc_tables", 0) or 0)
        return CheckResult("11.1.2", "Liquid Clustering adoption", "Format & Layout",
            None, "info", f"{lc}/{total} tables detected with Liquid Clustering hints",
            "Adopt Liquid Clustering for large tables",
            details={"lc_tables": lc, "total": total})

    # ── Table Maintenance ────────────────────────────────────────

    def check_11_2_1_predictive_optimization_coverage(self) -> CheckResult:
        """Percentage of tables with Predictive Optimization enabled."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN predictive_optimization_enabled = 'true' THEN 1 ELSE 0 END) AS po_enabled
                FROM system.storage.table_metrics_history
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM system.storage.table_metrics_history)""")
        except Exception as e:
            return CheckResult("11.2.1", "Predictive Optimization coverage", "Table Maintenance",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", ">80% tables PO-enabled")
        r = rows[0] if rows else {}
        total = int(r.get("total", 0) or 0) or 1
        enabled = int(r.get("po_enabled", 0) or 0)
        pct = round(enabled / total * 100, 1)
        score = 100 if pct >= 80 else 50 if pct >= 40 else 0
        status = "pass" if score == 100 else "partial" if score == 50 else "fail"
        rec = Recommendation(
            action=f"Only {pct}% of tables have Predictive Optimization enabled. Enable it on managed tables.",
            impact="PO automatically runs OPTIMIZE and VACUUM, reducing storage costs and improving query speed.",
            priority="high" if pct < 40 else "medium",
            docs_url="https://docs.databricks.com/en/optimizations/predictive-optimization.html") if score < 100 else None
        return CheckResult("11.2.1", "Predictive Optimization coverage", "Table Maintenance",
            score, status, f"{pct}% ({enabled}/{total} tables)", ">80% tables PO-enabled",
            details={"enabled": enabled, "total": total, "pct": pct}, recommendation=rec)

    def check_11_2_2_optimization_operations(self) -> CheckResult:
        """Recent OPTIMIZE/VACUUM operations from PO history."""
        try:
            rows = self.executor.execute("""
                SELECT operation_type, COUNT(*) AS ops,
                       SUM(CASE WHEN operation_status = 'SUCCESSFUL' THEN 1 ELSE 0 END) AS succeeded
                FROM system.storage.predictive_optimization_operations_history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY operation_type""")
        except Exception:
            return CheckResult("11.2.2", "Maintenance operations (30d)", "Table Maintenance",
                None, "info", "Could not query PO operations", "Regular OPTIMIZE & VACUUM")
        total_ops = sum(int(r.get("ops", 0) or 0) for r in rows)
        succeeded = sum(int(r.get("succeeded", 0) or 0) for r in rows)
        nc = [{"operation": r.get("operation_type", ""), "count": r.get("ops", 0),
               "succeeded": r.get("succeeded", 0)} for r in rows]
        score = 100 if total_ops > 100 else 50 if total_ops > 10 else 0
        status = "pass" if score == 100 else "partial" if score == 50 else "fail"
        return CheckResult("11.2.2", "Maintenance operations (30d)", "Table Maintenance",
            score, status, f"{total_ops} operations ({succeeded} succeeded)", "Regular OPTIMIZE & VACUUM",
            details={"non_conforming": nc, "total_ops": total_ops})

    # ── Storage Optimization ─────────────────────────────────────



    def check_11_4_1_stale_tables(self) -> CheckResult:
        """Identify tables not modified in 30/90 days."""
        try:
            rows = self.executor.execute("""
                SELECT table_catalog, table_schema, table_name,
                       last_altered, DATEDIFF(DAY, last_altered, CURRENT_TIMESTAMP()) AS days_stale
                FROM system.information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'default')
                  AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore', 'samples')
                  AND last_altered IS NOT NULL
                  AND DATEDIFF(DAY, last_altered, CURRENT_TIMESTAMP()) > 30
                ORDER BY last_altered ASC LIMIT 50""")
        except Exception as e:
            return CheckResult("11.4.1", "Stale tables (>30 days)", "Data Freshness & Lifecycle",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "<20% tables stale >30d")
        stale_30 = len(rows)
        stale_90 = sum(1 for r in rows if int(r.get("days_stale", 0) or 0) > 90)
        score = 100 if stale_30 == 0 else 50 if stale_30 < 20 else 0
        status = "pass" if score == 100 else "partial" if score == 50 else "fail"
        nc = [{"table": f"{r.get('table_catalog','')}.{r.get('table_schema','')}.{r.get('table_name','')}",
               "days_stale": r.get("days_stale", 0)} for r in rows[:15]]
        rec = Recommendation(
            action=f"{stale_30} tables not modified in 30+ days ({stale_90} over 90 days). Archive or drop unused tables.",
            impact="Stale tables waste storage costs and clutter the catalog for data consumers.",
            priority="high" if stale_90 > 10 else "medium",
            docs_url="https://docs.databricks.com/en/optimizations/predictive-optimization.html") if score < 100 else None
        return CheckResult("11.4.1", "Stale tables (>30 days)", "Data Freshness & Lifecycle",
            score, status, f"{stale_30} stale tables ({stale_90} over 90d)", "<20% tables stale >30d",
            details={"non_conforming": nc, "stale_30d": stale_30, "stale_90d": stale_90}, recommendation=rec)

    def check_11_4_2_table_documentation_coverage(self) -> CheckResult:
        """Percentage of tables with comments/descriptions."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN comment IS NOT NULL AND TRIM(comment) != '' THEN 1 ELSE 0 END) AS documented
                FROM system.information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'default')
                  AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore', 'samples')""")
        except Exception:
            return CheckResult("11.4.2", "Table documentation coverage", "Data Freshness & Lifecycle",
                0, "not_evaluated", "Could not query table comments", ">50% tables documented")
        r = rows[0] if rows else {}
        total = int(r.get("total", 0) or 0) or 1
        documented = int(r.get("documented", 0) or 0)
        pct = round(documented / total * 100, 1)
        score = 100 if pct >= 50 else 50 if pct >= 20 else 0
        status = "pass" if score == 100 else "partial" if score == 50 else "fail"
        rec = Recommendation(
            action=f"Only {pct}% of tables have descriptions. Add COMMENT ON TABLE to improve discoverability.",
            impact="Undocumented tables are harder to find, understand, and trust for downstream consumers.",
            priority="medium",
            docs_url="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-comment.html") if score < 100 else None
        return CheckResult("11.4.2", "Table documentation coverage", "Data Freshness & Lifecycle",
            score, status, f"{pct}% ({documented}/{total} tables)", ">50% tables documented",
            details={"documented": documented, "total": total, "pct": pct}, recommendation=rec)

    def check_11_4_3_column_documentation(self) -> CheckResult:
        """Percentage of columns with descriptions."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN comment IS NOT NULL AND TRIM(comment) != '' THEN 1 ELSE 0 END) AS documented
                FROM system.information_schema.columns
                WHERE table_schema NOT IN ('information_schema', 'default')
                  AND table_catalog NOT IN ('system', '__databricks_internal', 'hive_metastore', 'samples')""")
        except Exception:
            return CheckResult("11.4.3", "Column documentation coverage", "Data Freshness & Lifecycle",
                0, "not_evaluated", "Could not query column comments", ">30% columns documented")
        r = rows[0] if rows else {}
        total = int(r.get("total", 0) or 0) or 1
        documented = int(r.get("documented", 0) or 0)
        pct = round(documented / total * 100, 1)
        score = 100 if pct >= 30 else 50 if pct >= 10 else 0
        status = "pass" if score == 100 else "partial" if score == 50 else "fail"
        return CheckResult("11.4.3", "Column documentation coverage", "Data Freshness & Lifecycle",
            score, status, f"{pct}% ({documented}/{total} columns)", ">30% columns documented",
            details={"documented": documented, "total": total})

    # ── Lakehouse Federation ─────────────────────────────────────────

    def check_11_5_1_federation_connection_inventory(self) -> CheckResult:
        """Lakehouse Federation connection inventory and documentation."""
        try:
            rows = self.executor.execute("""
                SELECT
                    COUNT(*) AS total_connections,
                    COUNT(DISTINCT connection_type) AS distinct_types,
                    COUNT(CASE WHEN comment IS NOT NULL AND comment != '' THEN 1 END) AS documented,
                    COUNT(CASE WHEN connection_type NOT IN ('MANAGED_POSTGRESQL', 'HTTP', 'ONLINE_CATALOG') THEN 1 END) AS federation_connections
                FROM system.information_schema.connections""")
        except Exception:
            return CheckResult("11.5.1", "Lakehouse Federation Connections",
                "Lakehouse Federation", 0, "not_evaluated",
                "Could not query connections", "Federation connection inventory")

        r = rows[0] if rows else {}
        total = r.get("total_connections", 0) or 0
        fed = r.get("federation_connections", 0) or 0
        documented = r.get("documented", 0) or 0
        types = r.get("distinct_types", 0) or 0

        if fed == 0:
            return CheckResult("11.5.1", "Lakehouse Federation Connections",
                "Lakehouse Federation", 0, "info",
                "No federation connections configured", "Active federation connections",
                details={"summary": f"{total} total connections, 0 federation (database) connections"},
                recommendation=Recommendation(
                    action="Set up Lakehouse Federation to query external databases (PostgreSQL, Snowflake, SQL Server, etc.) without data movement.",
                    impact="Federation enables real-time cross-platform queries without ETL overhead.",
                    priority="low",
                    docs_url="https://docs.databricks.com/en/query-federation/index.html"))

        doc_pct = round(documented / total * 100) if total > 0 else 0
        score = 100 if doc_pct >= 80 else 50 if doc_pct >= 40 else 30
        status = "pass" if score == 100 else "partial" if score >= 50 else "fail"

        nc = [{"summary": f"{fed} federation connections across {types} types, {doc_pct}% documented"}]
        return CheckResult("11.5.1", "Lakehouse Federation Connections",
            "Lakehouse Federation", score, status,
            f"{fed} federation connections ({doc_pct}% documented)", "All connections documented",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action=f"Document {total - documented} undocumented connections with descriptions of their purpose and data sources.",
                impact="Documentation improves discoverability and governance of federated data sources.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/query-federation/index.html") if doc_pct < 80 else None)

    def check_11_5_2_federation_privilege_audit(self) -> CheckResult:
        """Audit access control on Lakehouse Federation connections."""
        try:
            rows = self.executor.execute("""
                SELECT
                    connection_name,
                    COUNT(DISTINCT grantee) AS grantee_count,
                    COUNT(CASE WHEN is_grantable = 'YES' THEN 1 END) AS grantable_count
                FROM system.information_schema.connection_privileges
                GROUP BY connection_name
                ORDER BY grantee_count DESC
                LIMIT 50""")
        except Exception:
            return CheckResult("11.5.2", "Federation Privilege Audit",
                "Lakehouse Federation", 0, "not_evaluated",
                "Could not query connection privileges", "Controlled federation access")

        if not rows:
            return CheckResult("11.5.2", "Federation Privilege Audit",
                "Lakehouse Federation", 100, "pass",
                "No explicit connection grants (default)", "Controlled federation access")

        total_conns = len(rows)
        wide_access = [r for r in rows if (r.get("grantee_count", 0) or 0) > 10]
        grantable = [r for r in rows if (r.get("grantable_count", 0) or 0) > 0]

        issues = []
        if wide_access:
            issues.append(f"{len(wide_access)} connections accessible by >10 principals")
        if grantable:
            issues.append(f"{len(grantable)} connections with re-grantable permissions")

        score = 100 if not issues else 50
        status = "pass" if score == 100 else "partial"

        nc = [{"connection": r.get("connection_name",""), "grantees": r.get("grantee_count",0)} for r in wide_access[:10]]
        return CheckResult("11.5.2", "Federation Privilege Audit",
            "Lakehouse Federation", score, status,
            f"{total_conns} connections with explicit grants" + (f" ({'; '.join(issues)})" if issues else ""),
            "Least-privilege access on all connections",
            details={"non_conforming": nc} if nc else {},
            recommendation=Recommendation(
                action="Review connections with broad access. Restrict federation connection grants to specific groups and avoid re-grantable permissions.",
                impact="Tighter access control prevents unauthorized queries against external databases.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/query-federation/index.html") if issues else None)

    def check_11_5_3_stale_federation_connections(self) -> CheckResult:
        """Detect stale Lakehouse Federation connections not altered in 90+ days."""
        try:
            rows = self.executor.execute("""
                SELECT
                    connection_name, connection_type, connection_owner,
                    last_altered, comment,
                    DATEDIFF(DAY, last_altered, CURRENT_DATE()) AS days_stale
                FROM system.information_schema.connections
                WHERE connection_type NOT IN ('MANAGED_POSTGRESQL', 'HTTP', 'ONLINE_CATALOG')
                  AND DATEDIFF(DAY, last_altered, CURRENT_DATE()) > 90
                ORDER BY days_stale DESC
                LIMIT 30""")
        except Exception:
            return CheckResult("11.5.3", "Stale Federation Connections",
                "Lakehouse Federation", 0, "not_evaluated",
                "Could not query connections", "No stale connections")

        if not rows:
            return CheckResult("11.5.3", "Stale Federation Connections",
                "Lakehouse Federation", 100, "pass",
                "No stale federation connections (>90d)", "All connections recently maintained")

        stale_count = len(rows)
        nc = [{"connection": r.get("connection_name",""), "type": r.get("connection_type",""),
               "days_stale": r.get("days_stale",0), "owner": r.get("connection_owner","")} for r in rows[:10]]

        score = 50 if stale_count <= 5 else 30
        return CheckResult("11.5.3", "Stale Federation Connections",
            "Lakehouse Federation", score, "partial" if score >= 50 else "fail",
            f"{stale_count} federation connections not updated in >90 days",
            "All connections recently maintained",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action=f"Review {stale_count} stale federation connections. Remove unused ones and verify credentials are still valid for active ones.",
                impact="Stale connections may have expired credentials or point to decommissioned databases.",
                priority="low",
                docs_url="https://docs.databricks.com/en/query-federation/index.html"))

