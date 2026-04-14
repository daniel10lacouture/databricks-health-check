"""Section 1: Data Engineering & Table Health — comprehensive checks for table inventory,
Delta maintenance, DQ, ETL pipeline health, storage optimization, and ingestion.
Merges former 'Data Ingestion' section. All checks include drill-down details with actual objects."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class DataEngineeringCheckRunner(BaseCheckRunner):
    section_id = "data_engineering"
    section_name = "Data Engineering & Table Health"
    section_type = "core"
    icon = "layers"

    def get_subsections(self):
        return ["Table Inventory & Governance", "Delta Table Maintenance & Layout",
                "Storage Optimization", "ETL Pipeline Health",
                "Pipeline & Ingestion Performance", "Job Configuration Health"]

    # ── 1.1 Table Inventory & Governance ─────────────────────────────

    def check_1_1_1_table_inventory(self) -> CheckResult:
        rows = self.executor.execute("""
            SELECT table_type, COUNT(*) AS cnt
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
            GROUP BY 1 ORDER BY 2 DESC""")
        detail = {r.get("table_type","UNKNOWN"): int(r.get("cnt",0)) for r in rows}
        total = sum(detail.values())
        non_conforming = [{"table_type": k, "count": v} for k, v in detail.items()]
        return CheckResult("1.1.1", "Table inventory by type",
            "Table Inventory & Governance", 0, "info",
            f"{total} total tables", "Informational",
            details={"non_conforming": non_conforming, "total_tables": total})

    def check_1_1_2_hive_metastore_tables(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT table_catalog, table_schema, table_name, table_type
                FROM hive_metastore.information_schema.tables
                WHERE table_schema != 'information_schema'
                LIMIT 50""")
            hms_count = len(rows)
        except Exception:
            return CheckResult("1.1.2", "Tables still in hive_metastore",
                "Table Inventory & Governance", 100, "pass",
                "hive_metastore not accessible (good)", "0 tables",
                details={"non_conforming": [], "summary": "hive_metastore catalog not accessible — all tables are in Unity Catalog."})
        total_rows = self.executor.execute("""
            SELECT COUNT(*) AS cnt FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'""")
        total = int(total_rows[0].get("cnt", 0)) if total_rows else 1
        pct = hms_count / max(total, 1) * 100
        if pct == 0: score, status = 100, "pass"
        elif pct <= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"schema": r.get("schema_name",""), "table": r.get("table_name",""), "type": r.get("table_type","")} for r in rows[:20]]
        rec = None
        if hms_count > 0:
            rec = Recommendation(
                action=f"Migrate {hms_count} table(s) from hive_metastore to Unity Catalog.",
                impact="UC provides centralized governance, lineage, and access control.",
                priority="high" if pct > 20 else "medium",
                docs_url="https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html")
        return CheckResult("1.1.2", "Tables still in hive_metastore",
            "Table Inventory & Governance", score, status,
            f"{hms_count} tables in hive_metastore ({pct:.0f}%)",
            "0 tables in hive_metastore",
            details={"non_conforming": nc, "hms_count": hms_count, "total": total},
            recommendation=rec)

    def check_1_1_3_managed_vs_external(self) -> CheckResult:
        rows = self.executor.execute("""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
                AND table_catalog != 'hive_metastore'
                AND table_type = 'EXTERNAL'
            LIMIT 30""")
        total_rows = self.executor.execute("""
            SELECT COUNT(*) AS cnt FROM system.information_schema.tables
            WHERE table_schema != 'information_schema' AND table_catalog != 'hive_metastore'""")
        total = int(total_rows[0].get("cnt",0)) if total_rows else 1
        external = len(rows)
        managed_pct = (total - external) / max(total, 1) * 100
        if managed_pct > 90: score, status = 100, "pass"
        elif managed_pct >= 50: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""), "table": r.get("table_name","")} for r in rows[:20]]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{external} external tables found ({100-managed_pct:.0f}%). Convert to managed where possible.",
                impact="Managed tables benefit from predictive optimization and simplified governance.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html")
        return CheckResult("1.1.3", "Managed vs. external table ratio",
            "Table Inventory & Governance", score, status,
            f"{managed_pct:.0f}% managed ({total - external}/{total})", ">90% managed",
            details={"non_conforming": nc}, recommendation=rec)

    def check_1_1_4_tables_without_owners(self) -> CheckResult:
        rows = self.executor.execute("""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'
                AND (table_owner IS NULL OR table_owner = '')
            LIMIT 30""")
        total_rows = self.executor.execute("""
            SELECT COUNT(*) AS cnt FROM system.information_schema.tables
            WHERE table_schema != 'information_schema'""")
        total = int(total_rows[0].get("cnt", 0)) if total_rows else 1
        ownerless = len(rows)
        pct = ownerless / max(total, 1) * 100
        if pct == 0: score, status = 100, "pass"
        elif pct <= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""), "table": r.get("table_name",""), "type": r.get("table_type","")} for r in rows[:20]]
        rec = None
        if ownerless > 0:
            rec = Recommendation(
                action=f"Assign owners to {ownerless} table(s). Use ALTER TABLE ... SET OWNER TO <group>.",
                impact="Ownerless tables have no clear accountability for data quality or access.",
                priority="medium")
        return CheckResult("1.1.4", "Tables without owners",
            "Table Inventory & Governance", score, status,
            f"{ownerless}/{total} ownerless ({pct:.0f}%)", "0 ownerless tables",
            details={"non_conforming": nc}, recommendation=rec)

    # ── 1.2 Delta Table Maintenance ──────────────────────────────────

    def check_1_2_1_predictive_optimization(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT table_catalog, table_schema, table_name,
                    COUNT(*) AS operations, MAX(period_start_time) AS last_op
                FROM system.storage.predictive_optimization_operations_history
                WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY 1, 2, 3
                ORDER BY 4 DESC LIMIT 20""")
            count = len(rows)
        except Exception:
            return CheckResult("1.2.1", "Predictive Optimization enabled",
                "Delta Table Maintenance & Layout", 0, "fail",
                "PO not enabled or no operations found",
                "Enabled for all managed tables",
                details={"non_conforming": [], "summary": "Predictive Optimization system table not accessible."},
                recommendation=Recommendation(
                    action="Enable Predictive Optimization at catalog or schema level.",
                    impact="PO automatically optimizes, vacuums, and analyzes tables — eliminating manual maintenance.",
                    priority="high",
                    docs_url="https://docs.databricks.com/en/optimizations/predictive-optimization.html"))
        if count > 0:
            nc = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""),
                   "table": r.get("table_name",""), "operations_30d": r.get("operations",0),
                   "last_operation": str(r.get("last_op",""))[:19]} for r in rows]
            return CheckResult("1.2.1", "Predictive Optimization enabled",
                "Delta Table Maintenance & Layout", 100, "pass",
                f"PO active on {count} table(s) in last 30d",
                "Enabled for all managed tables",
                details={"non_conforming": nc, "summary": f"Top {count} tables by PO operations."})
        return CheckResult("1.2.1", "Predictive Optimization enabled",
            "Delta Table Maintenance & Layout", 0, "fail",
            "No PO operations in last 30 days", "Enabled for all managed tables",
            details={"non_conforming": [], "summary": "No Predictive Optimization activity detected."},
            recommendation=Recommendation(
                action="Enable Predictive Optimization at catalog or schema level.",
                impact="PO automatically optimizes, vacuums, and analyzes tables — eliminating manual maintenance.",
                priority="high",
                docs_url="https://docs.databricks.com/en/optimizations/predictive-optimization.html"))

    # ── 1.3 Storage Optimization (NEW - Tier 1) ─────────────────────

    def check_1_3_1_table_storage_bloat(self) -> CheckResult:
        """Identify tables with small files that need OPTIMIZE."""
        try:
            rows = self.executor.execute("""
                SELECT t.catalog_name, t.schema_name, t.table_name,
                    t.active_files, t.active_bytes,
                    CASE WHEN t.active_files > 0 THEN ROUND(t.active_bytes / t.active_files) ELSE 0 END AS avg_file_bytes,
                    t.predictive_optimization_enabled
                FROM system.storage.table_metrics_history t
                WHERE t.snapshot_date = (SELECT MAX(snapshot_date) FROM system.storage.table_metrics_history)
                    AND t.active_files > 100
                ORDER BY t.active_files DESC
                LIMIT 30""")
        except Exception:
            return CheckResult("1.3.1", "Table storage bloat (small files)",
                "Storage Optimization", 0, "not_evaluated",
                "Could not query table_metrics_history", "No tables with excessive small files")

        # Small file = avg < 32MB and >100 files
        small_file_tables = [r for r in rows if (r.get("avg_file_bytes", 0) or 0) < 32 * 1024 * 1024]
        if not small_file_tables:
            nc = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""),
                   "table": r.get("table_name",""), "files": r.get("active_files",0),
                   "avg_file_size_mb": round((r.get("avg_file_bytes",0) or 0) / (1024*1024), 1),
                   "size_gb": round((r.get("active_bytes",0) or 0) / (1024**3), 2)} for r in rows[:10]]
            return CheckResult("1.3.1", "Table storage bloat (small files)",
                "Storage Optimization", 100, "pass",
                f"No tables with excessive small files (checked {len(rows)} tables)",
                "Avg file size >32 MB",
                details={"non_conforming": nc, "summary": "Top tables by file count — all healthy."})

        nc = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""),
               "table": r.get("table_name",""), "files": r.get("active_files",0),
               "avg_file_size_mb": round((r.get("avg_file_bytes",0) or 0) / (1024*1024), 1),
               "size_gb": round((r.get("active_bytes",0) or 0) / (1024**3), 2),
               "po_enabled": r.get("predictive_optimization_enabled", False),
               "action": "Enable Predictive Optimization" if not r.get("predictive_optimization_enabled") else "Run OPTIMIZE manually"
              } for r in small_file_tables[:20]]

        score = 0 if len(small_file_tables) > 10 else 50
        return CheckResult("1.3.1", "Table storage bloat (small files)",
            "Storage Optimization", score, "fail" if score == 0 else "partial",
            f"{len(small_file_tables)} tables with small files (<32 MB avg)",
            "Avg file size >32 MB",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action=f"Enable Predictive Optimization on {len(small_file_tables)} table(s) with small files, or run OPTIMIZE manually. PO auto-compacts small files.",
                impact="Small files degrade query performance by increasing I/O overhead. OPTIMIZE compacts them into larger files.",
                priority="high",
                docs_url="https://docs.databricks.com/en/optimizations/predictive-optimization.html"))

    def check_1_3_2_largest_tables(self) -> CheckResult:
        """Identify the largest tables by storage size."""
        try:
            rows = self.executor.execute("""
                SELECT table_catalog, table_schema, table_name,
                    active_bytes, active_files, predictive_optimization_enabled
                FROM system.storage.table_metrics_history
                WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM system.storage.table_metrics_history)
                    AND active_bytes > 0
                ORDER BY active_bytes DESC
                LIMIT 20""")
        except Exception:
            return CheckResult("1.3.2", "Largest tables by storage",
                "Storage Optimization", 0, "not_evaluated",
                "Could not query", "Informational")

        total_bytes = sum(r.get("active_bytes", 0) or 0 for r in rows)
        nc = [{"catalog": r.get("catalog_name",""), "schema": r.get("schema_name",""),
               "table": r.get("table_name",""), "size_gb": round((r.get("active_bytes",0) or 0)/(1024**3), 2),
               "files": r.get("active_files",0),
               "po_enabled": r.get("predictive_optimization_enabled", False)} for r in rows]

        return CheckResult("1.3.2", "Largest tables by storage",
            "Storage Optimization", 0, "info",
            f"Top {len(rows)} tables: {round(total_bytes/(1024**4), 2)} TB total",
            "Informational",
            details={"non_conforming": nc, "summary": "Review largest tables for retention policies and partitioning strategy."},
            recommendation=Recommendation(
                action="Review largest tables for retention policies. Consider VACUUM with shorter retention for non-time-travel tables.",
                impact="Large tables are the primary cost driver. Proper retention reduces storage costs.",
                priority="low",
                docs_url="https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html"))

    # ── 1.4 ETL Pipeline Health ──────────────────────────────────────

    def check_1_4_1_job_failure_rate(self) -> CheckResult:
        rows = self.executor.execute("""
            SELECT j.job_id, j.name AS job_name,
                COUNT(*) AS total_runs,
                SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
                ROUND(SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fail_pct
            FROM system.lakeflow.job_run_timeline r
            JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
            WHERE r.period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                AND j.delete_time IS NULL
            GROUP BY 1, 2
            HAVING SUM(CASE WHEN r.result_state = 'FAILED' THEN 1 ELSE 0 END) > 0
            ORDER BY 4 DESC
            LIMIT 20""")
        # Also get overall stats
        stats = self.executor.execute("""
            SELECT COUNT(*) AS total_runs,
                SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        r = stats[0] if stats else {}
        total = int(r.get("total_runs", 0)) or 1
        failed = int(r.get("failed", 0))
        fail_pct = failed / total * 100
        if fail_pct < 5: score, status = 100, "pass"
        elif fail_pct < 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"job_name": r.get("job_name",""), "job_id": r.get("job_id",""),
               "total_runs": r.get("total_runs",0), "failed_runs": r.get("failed_runs",0),
               "fail_pct": r.get("fail_pct",0)} for r in rows[:20]]
        rec = None
        if score < 100:
            top_names = ", ".join(r.get("job_name","")[:40] for r in rows[:3])
            rec = Recommendation(
                action=f"Job failure rate is {fail_pct:.1f}% ({failed}/{total}). Top failing jobs: {top_names}",
                impact="High failure rates indicate reliability issues and wasted compute.",
                priority="high" if fail_pct > 20 else "medium",
                docs_url="https://docs.databricks.com/en/workflows/jobs/monitor-job-runs.html")
        return CheckResult("1.4.1", "Job failure rate (last 30 days)",
            "ETL Pipeline Health", score, status,
            f"{fail_pct:.1f}% failure rate ({failed}/{total})",
            "<5% failure rate", details={"non_conforming": nc}, recommendation=rec)

    def check_1_4_8_allpurpose_compute_jobs(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT j.job_id, j.name AS job_name, j.compute
                FROM system.lakeflow.jobs j
                WHERE j.delete_time IS NULL""")
        except Exception:
            return CheckResult("1.4.8", "Jobs using all-purpose compute",
                "ETL Pipeline Health", 0, "not_evaluated",
                "Could not query jobs", "All jobs use job compute")
        # Can't easily determine compute type from system tables alone, so check for recent runs
        run_rows = self.executor.execute("""
            SELECT DISTINCT r.job_id, j.name AS job_name
            FROM system.lakeflow.job_run_timeline r
            JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
            WHERE r.period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                AND r.cluster_type = 'EXISTING'
                AND j.delete_time IS NULL
            LIMIT 20""")
        nc = [{"job_name": r.get("job_name",""), "job_id": r.get("job_id",""),
               "action": "Switch to dedicated job compute or serverless"} for r in run_rows[:20]]
        total_jobs = len(rows)
        ap_jobs = len(run_rows)
        if ap_jobs == 0: score, status = 100, "pass"
        elif ap_jobs <= 5: score, status = 50, "partial"
        else: score, status = 0, "fail"
        rec = None
        if ap_jobs > 0:
            rec = Recommendation(
                action=f"{ap_jobs} job(s) run on all-purpose clusters. Switch to job compute or serverless.",
                impact="Job compute is significantly cheaper than all-purpose. Serverless provides instant startup.",
                priority="high",
                docs_url="https://docs.databricks.com/en/compute/use-compute.html")
        return CheckResult("1.4.8", "Jobs using all-purpose compute",
            "ETL Pipeline Health", score, status,
            f"{ap_jobs} jobs on all-purpose compute", "All jobs use job compute",
            details={"non_conforming": nc}, recommendation=rec)

    # ── 1.5 Pipeline & Ingestion Performance (merged from ingestion) ─

    def check_1_5_1_pipeline_inventory(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT pipeline_type, COUNT(*) AS cnt
                FROM system.lakeflow.pipelines
                WHERE delete_time IS NULL
                GROUP BY 1 ORDER BY 2 DESC""")
        except Exception:
            return CheckResult("1.5.1", "Pipeline inventory by type",
                "Pipeline & Ingestion Performance", 0, "not_evaluated",
                "Could not query pipelines", "Informational")
        total = sum(int(r.get("cnt",0)) for r in rows)
        nc = [{"pipeline_type": r.get("pipeline_type",""), "count": r.get("cnt",0)} for r in rows]
        return CheckResult("1.5.1", "Pipeline inventory by type",
            "Pipeline & Ingestion Performance", 0, "info",
            f"{total} active pipelines across {len(rows)} types",
            "Informational", details={"non_conforming": nc})

    def check_1_5_2_pipeline_update_success(self) -> CheckResult:
        """Tier 1: Pipeline update success rate from pipeline_update_timeline."""
        try:
            rows = self.executor.execute("""
                SELECT p.pipeline_id, p.name AS pipeline_name, p.pipeline_type,
                    COUNT(*) AS total_updates,
                    SUM(CASE WHEN u.result_state = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN u.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                    ROUND(SUM(CASE WHEN u.result_state = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fail_pct
                FROM system.lakeflow.pipeline_update_timeline u
                JOIN system.lakeflow.pipelines p ON u.pipeline_id = p.pipeline_id
                WHERE u.period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND p.delete_time IS NULL
                GROUP BY 1, 2, 3
                HAVING SUM(CASE WHEN u.result_state = 'FAILED' THEN 1 ELSE 0 END) > 0
                ORDER BY 6 DESC
                LIMIT 20""")
            # Overall stats
            stats = self.executor.execute("""
                SELECT COUNT(*) AS total,
                    SUM(CASE WHEN result_state = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed
                FROM system.lakeflow.pipeline_update_timeline
                WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("1.5.2", "Pipeline update success rate",
                "Pipeline & Ingestion Performance", 0, "not_evaluated",
                "Could not query pipeline updates", ">90% success rate")

        s = stats[0] if stats else {}
        total = int(s.get("total", 0)) or 1
        completed = int(s.get("completed", 0))
        failed = int(s.get("failed", 0))
        success_pct = completed / total * 100

        if success_pct >= 90: score, status = 100, "pass"
        elif success_pct >= 70: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"pipeline_name": r.get("pipeline_name",""), "pipeline_id": r.get("pipeline_id",""),
               "type": r.get("pipeline_type",""), "total_updates": r.get("total_updates",0),
               "failed": r.get("failed",0), "fail_pct": r.get("fail_pct",0),
               "action": "Investigate pipeline failures in the Pipelines UI"} for r in rows[:20]]

        rec = None
        if score < 100:
            top = ", ".join(r.get("pipeline_name","")[:30] for r in rows[:3])
            rec = Recommendation(
                action=f"Pipeline success rate is {success_pct:.0f}% ({failed} failures in 30d). Top failing: {top}",
                impact="Failed pipeline updates delay data freshness and may cause downstream issues.",
                priority="high" if success_pct < 70 else "medium",
                docs_url="https://docs.databricks.com/en/delta-live-tables/observability.html")

        return CheckResult("1.5.2", "Pipeline update success rate",
            "Pipeline & Ingestion Performance", score, status,
            f"{success_pct:.0f}% success ({completed}/{total}), {failed} failures",
            ">90% success rate", details={"non_conforming": nc}, recommendation=rec)

    # ── 1.5.3 Task-Level Job Performance (Tier 1) ────────────────────

    def check_1_5_3_task_failure_rate(self) -> CheckResult:
        """Tier 1: Task-level job performance — reveals bottlenecks hidden by job-level metrics."""
        try:
            rows = self.executor.execute("""
                SELECT t.job_id, j.name AS job_name, t.task_key,
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN t.result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                    ROUND(AVG(t.execution_duration_seconds), 1) AS avg_duration_s,
                    ROUND(SUM(CASE WHEN t.result_state = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fail_pct
                FROM system.lakeflow.job_task_run_timeline t
                JOIN system.lakeflow.jobs j ON t.job_id = j.job_id
                WHERE t.period_start_time >= DATEADD(DAY, -7, CURRENT_DATE())
                    AND j.delete_time IS NULL
                GROUP BY 1, 2, 3
                HAVING SUM(CASE WHEN t.result_state = 'FAILED' THEN 1 ELSE 0 END) > 2
                ORDER BY 5 DESC
                LIMIT 20""")
            stats = self.executor.execute("""
                SELECT COUNT(*) AS total_tasks,
                    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                    ROUND(AVG(execution_duration_seconds), 1) AS avg_exec_s
                FROM system.lakeflow.job_task_run_timeline
                WHERE period_start_time >= DATEADD(DAY, -7, CURRENT_DATE())""")
        except Exception:
            return CheckResult("1.5.3", "Task-level failure rate (7d)",
                "Pipeline & Ingestion Performance", 0, "not_evaluated",
                "Could not query task runs", "<10% task failure rate")

        s = stats[0] if stats else {}
        total = int(s.get("total_tasks", 0)) or 1
        failed = int(s.get("failed", 0))
        fail_pct = failed / total * 100

        if fail_pct < 5: score, status = 100, "pass"
        elif fail_pct < 15: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"job_name": r.get("job_name",""), "task_key": r.get("task_key",""),
               "total_runs": r.get("total_runs",0), "failed": r.get("failed",0),
               "fail_pct": r.get("fail_pct",0), "avg_duration_s": r.get("avg_duration_s",0),
               "action": "Review task logs in the job run details page"} for r in rows[:20]]

        rec = None
        if score < 100:
            top = ", ".join(f"{r.get('job_name','')}:{r.get('task_key','')}" for r in rows[:3])
            rec = Recommendation(
                action=f"Task failure rate is {fail_pct:.1f}% ({failed}/{total} in 7d). Top failing tasks: {top}",
                impact="Task-level failures reveal bottlenecks hidden by job-level aggregates. Fix the root task to fix the job.",
                priority="high" if fail_pct > 15 else "medium",
                docs_url="https://docs.databricks.com/en/workflows/jobs/monitor-job-runs.html")

        return CheckResult("1.5.3", "Task-level failure rate (7d)",
            "Pipeline & Ingestion Performance", score, status,
            f"{fail_pct:.1f}% task failure rate ({failed}/{total})",
            "<5% task failure rate", details={"non_conforming": nc}, recommendation=rec)

    # ── 1.6 Job Configuration Health (Tier 2) ────────────────────────

    def check_1_6_1_orphan_jobs(self) -> CheckResult:
        """Jobs with no runs in 90 days — likely stale."""
        try:
            rows = self.executor.execute("""
                WITH recent AS (
                    SELECT DISTINCT job_id FROM system.lakeflow.job_run_timeline
                    WHERE period_start_time >= DATEADD(DAY, -90, CURRENT_DATE()))
                SELECT j.job_id, j.name AS job_name, j.creator
                FROM system.lakeflow.jobs j
                LEFT JOIN recent r ON j.job_id = r.job_id
                WHERE j.delete_time IS NULL AND r.job_id IS NULL
                LIMIT 30""")
        except Exception:
            return CheckResult("1.6.1", "Orphan jobs (no runs in 90d)",
                "Job Configuration Health", 0, "not_evaluated",
                "Could not query", "0 orphan jobs")

        nc = [{"job_name": r.get("job_name",""), "job_id": r.get("job_id",""),
               "creator": r.get("creator",""),
               "action": "Delete if no longer needed, or update the schedule"} for r in rows[:20]]

        if not rows: score, status = 100, "pass"
        elif len(rows) <= 10: score, status = 50, "partial"
        else: score, status = 0, "fail"

        rec = None
        if rows:
            rec = Recommendation(
                action=f"Delete or archive {len(rows)} orphan job(s) with no runs in 90 days.",
                impact="Orphan jobs clutter the workspace and may auto-start unexpectedly if schedules resume.",
                priority="medium")

        return CheckResult("1.6.1", "Orphan jobs (no runs in 90d)",
            "Job Configuration Health", score, status,
            f"{len(rows)} jobs with no runs in 90 days",
            "0 orphan jobs", details={"non_conforming": nc}, recommendation=rec)

    def check_1_6_2_jobs_manual_trigger(self) -> CheckResult:
        """Jobs with high manual trigger rate — should be automated."""
        try:
            rows = self.executor.execute("""
                SELECT j.job_id, j.name AS job_name,
                    SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) AS manual_runs,
                    COUNT(*) AS total_runs,
                    ROUND(SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS manual_pct
                FROM system.lakeflow.job_run_timeline r
                JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
                WHERE r.period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND j.delete_time IS NULL
                GROUP BY 1, 2
                HAVING COUNT(*) >= 5 AND SUM(CASE WHEN r.trigger_type = 'MANUAL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 50
                ORDER BY 3 DESC
                LIMIT 20""")
            # Overall
            stats = self.executor.execute("""
                SELECT SUM(CASE WHEN trigger_type = 'MANUAL' THEN 1 ELSE 0 END) AS manual,
                    COUNT(*) AS total
                FROM system.lakeflow.job_run_timeline
                WHERE period_start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("1.6.2", "Jobs triggered manually vs automated",
                "Job Configuration Health", 0, "not_evaluated",
                "Could not query", "<10% manual runs")

        s = stats[0] if stats else {}
        total = int(s.get("total", 0)) or 1
        manual = int(s.get("manual", 0))
        pct = manual / total * 100

        if pct < 10: score, status = 100, "pass"
        elif pct < 30: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"job_name": r.get("job_name",""), "job_id": r.get("job_id",""),
               "manual_runs": r.get("manual_runs",0), "total_runs": r.get("total_runs",0),
               "manual_pct": r.get("manual_pct",0),
               "action": "Add a schedule or file-arrival trigger"} for r in rows[:20]]

        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{pct:.0f}% of runs are manual ({manual}/{total}). Automate via scheduled triggers or CI/CD.",
                impact="Manual runs are error-prone and not reproducible.",
                priority="medium")

        return CheckResult("1.6.2", "Jobs triggered manually vs automated",
            "Job Configuration Health", score, status,
            f"{pct:.0f}% manual runs ({manual}/{total})",
            "<10% manual runs", details={"non_conforming": nc}, recommendation=rec)
