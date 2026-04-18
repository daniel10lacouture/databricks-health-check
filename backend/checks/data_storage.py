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
        return ["Format & Layout", "Table Maintenance", "Storage Optimization", "Data Freshness & Lifecycle"]

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

    def check_11_3_1_storage_volume(self) -> CheckResult:
        """Total managed storage volume and table count."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name)) AS total_tables,
                       ROUND(SUM(active_bytes) / (1024*1024*1024*1024.0), 2) AS total_tb,
                       ROUND(AVG(active_bytes) / (1024*1024.0), 1) AS avg_mb
                FROM system.storage.table_metrics_history
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM system.storage.table_metrics_history)""")
        except Exception:
            return CheckResult("11.3.1", "Storage volume overview", "Storage Optimization",
                None, "info", "Could not query storage metrics", "Monitor storage growth")
        r = rows[0] if rows else {}
        tables = int(r.get("total_tables", 0) or 0)
        tb = float(r.get("total_tb", 0) or 0)
        avg_mb = float(r.get("avg_mb", 0) or 0)
        return CheckResult("11.3.1", "Storage volume overview", "Storage Optimization",
            None, "info", f"{tables} tables, {tb:.1f} TB total, {avg_mb:.0f} MB avg",
            "Monitor storage growth", details={"tables": tables, "total_tb": tb, "avg_mb": avg_mb})

    def check_11_3_2_storage_growth(self) -> CheckResult:
        """Detect tables with rapid storage growth over last 30 days."""
        try:
            rows = self.executor.execute("""
                WITH bounds AS (
                    SELECT catalog_name, schema_name, table_name,
                           MIN(CASE WHEN snapshot_date <= DATEADD(DAY, -25, CURRENT_DATE()) THEN active_bytes END) AS early_bytes,
                           MAX(CASE WHEN snapshot_date >= DATEADD(DAY, -5, CURRENT_DATE()) THEN active_bytes END) AS late_bytes
                    FROM system.storage.table_metrics_history
                    WHERE snapshot_date >= DATEADD(DAY, -30, CURRENT_DATE())
                    GROUP BY 1, 2, 3
                    HAVING MIN(CASE WHEN snapshot_date <= DATEADD(DAY, -25, CURRENT_DATE()) THEN active_bytes END) > 0
                )
                SELECT catalog_name, schema_name, table_name,
                       ROUND(early_bytes / (1024*1024.0), 1) AS early_mb,
                       ROUND(late_bytes / (1024*1024.0), 1) AS late_mb,
                       ROUND((late_bytes - early_bytes) * 100.0 / early_bytes, 1) AS growth_pct
                FROM bounds
                WHERE late_bytes > early_bytes * 1.5 AND late_bytes > 1073741824
                ORDER BY (late_bytes - early_bytes) DESC LIMIT 15""")
        except Exception:
            return CheckResult("11.3.2", "Rapid storage growth detection", "Storage Optimization",
                None, "info", "Could not analyze storage growth", "Monitor table bloat")
        if not rows:
            return CheckResult("11.3.2", "Rapid storage growth detection", "Storage Optimization",
                100, "pass", "No tables with >50% growth exceeding 1GB", "No excessive growth")
        nc = [{"table": f"{r.get('catalog_name','')}.{r.get('schema_name','')}.{r.get('table_name','')}",
               "growth_pct": r.get("growth_pct", 0), "current_mb": r.get("late_mb", 0)} for r in rows]
        score = 50 if len(rows) <= 5 else 0
        return CheckResult("11.3.2", "Rapid storage growth detection", "Storage Optimization",
            score, "partial" if score > 0 else "fail",
            f"{len(rows)} tables with >50% storage growth", "No excessive growth",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action=f"{len(rows)} tables grew >50% in 30 days. Review for runaway ingestion or missing VACUUM.",
                impact="Uncontrolled growth inflates cloud storage costs and degrades query performance.",
                priority="high" if len(rows) > 5 else "medium",
                docs_url="https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html"))

    # ── Data Freshness & Lifecycle ───────────────────────────────

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
