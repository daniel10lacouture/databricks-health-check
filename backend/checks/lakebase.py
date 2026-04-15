"""Section: OLTP / Lakebase — streaming and real-time ingestion checks."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class LakebaseCheckRunner(BaseCheckRunner):
    section_id = "lakebase"
    section_name = "OLTP / Lakebase"
    section_type = "conditional"
    icon = "server"

    def get_subsections(self):
        return ["Streaming Ingestion Health", "Streaming Inventory"]

    def is_active(self) -> bool:
        """Active if there's any streaming ingestion activity."""
        try:
            r = self.executor.execute("""
                SELECT COUNT(*) AS cnt FROM system.lakeflow.zerobus_ingest
                WHERE commit_time >= DATEADD(DAY, -30, CURRENT_DATE())
            """)
            return r[0]["cnt"] > 0
        except Exception:
            return False

    def check_9_1_1_streaming_ingest_health(self) -> CheckResult:
        """Check streaming ingestion error rate."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN SIZE(errors) > 0 THEN 1 ELSE 0 END) AS errors
                FROM system.lakeflow.zerobus_ingest
                WHERE commit_time >= DATEADD(DAY, -7, CURRENT_DATE())
            """)
            total = rows[0]["total"] or 0
            errors = rows[0]["errors"] or 0
            
            if total == 0:
                return CheckResult("9.1.1", "Streaming ingestion error rate",
                    "Streaming Ingestion Health", 0, "not_evaluated",
                    "No streaming ingestion activity", "< 1% error rate")
            
            error_rate = errors / total * 100
            if error_rate < 1:
                score, status = 100, "pass"
            elif error_rate < 5:
                score, status = 50, "partial"
            else:
                score, status = 0, "fail"

            # Get top tables with errors
            err_tables = self.executor.execute("""
                SELECT table_name, COUNT(*) AS error_commits
                FROM system.lakeflow.zerobus_ingest
                WHERE commit_time >= DATEADD(DAY, -7, CURRENT_DATE())
                  AND SIZE(errors) > 0
                GROUP BY 1 ORDER BY error_commits DESC LIMIT 10
            """)
            nc = [{"table": r["table_name"], "error_commits": r["error_commits"]} 
                  for r in err_tables] if err_tables else []

            rec = None
            if score < 100:
                rec = Recommendation(
                    action=f"Investigate {errors:,} streaming ingestion errors ({error_rate:.1f}% error rate).",
                    impact="Streaming errors can cause data loss or delays in real-time analytics.",
                    priority="high" if error_rate >= 5 else "medium",
                    docs_url="https://docs.databricks.com/en/ingestion/index.html")

            return CheckResult("9.1.1", "Streaming ingestion error rate (7d)",
                "Streaming Ingestion Health", score, status,
                f"{error_rate:.2f}% ({errors:,}/{total:,} commits)", "< 1% error rate",
                details={"tables_with_errors": nc} if nc else {"healthy_commits": total},
                recommendation=rec)
        except Exception as e:
            return CheckResult("9.1.1", "Streaming ingestion error rate",
                "Streaming Ingestion Health", 0, "not_evaluated",
                f"Query failed: {str(e)[:80]}", "< 1% error rate")

    def check_9_1_2_streaming_volume(self) -> CheckResult:
        """Track streaming ingestion volume — informational."""
        try:
            rows = self.executor.execute("""
                SELECT COUNT(*) AS commits,
                       SUM(committed_records) AS records,
                       SUM(committed_bytes)/1024/1024/1024 AS gb,
                       COUNT(DISTINCT table_name) AS tables
                FROM system.lakeflow.zerobus_ingest
                WHERE commit_time >= DATEADD(DAY, -7, CURRENT_DATE())
            """)
            commits = rows[0]["commits"] or 0
            records = rows[0]["records"] or 0
            gb = rows[0]["gb"] or 0
            tables = rows[0]["tables"] or 0

            # Get top tables by volume
            top_tables = self.executor.execute("""
                SELECT table_name, 
                       COUNT(*) AS commits,
                       SUM(committed_records) AS records,
                       SUM(committed_bytes)/1024/1024 AS mb
                FROM system.lakeflow.zerobus_ingest
                WHERE commit_time >= DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY 1 ORDER BY mb DESC LIMIT 15
            """)
            nc = [{"table": r["table_name"], "commits": r["commits"], 
                   "records": f"{int(r['records'] or 0):,}", "mb": f"{r['mb']:.1f}"}
                  for r in top_tables] if top_tables else []

            return CheckResult("9.1.2", "Streaming ingestion volume (7d)",
                "Streaming Inventory", None, "info",
                f"{commits:,} commits, {int(records):,} records, {gb:.1f} GB across {tables} tables",
                "Track volume trends",
                details={"top_tables_by_volume": nc, 
                         "summary": f"Streaming data ingested to {tables} tables this week"},
                recommendation=None)
        except Exception as e:
            return CheckResult("9.1.2", "Streaming ingestion volume",
                "Streaming Inventory", None, "info",
                f"Query failed: {str(e)[:80]}", "N/A")

    def check_9_1_3_streaming_protocols(self) -> CheckResult:
        """Show streaming protocols in use — informational."""
        try:
            rows = self.executor.execute("""
                SELECT COALESCE(protocol, 'Native') AS protocol,
                       COALESCE(data_format, 'Default') AS format,
                       COUNT(DISTINCT stream_id) AS streams
                FROM system.lakeflow.zerobus_stream
                WHERE event_time >= DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY 1, 2 ORDER BY streams DESC
            """)
            nc = [{"protocol": r["protocol"], "format": r["format"], 
                   "active_streams": r["streams"]} for r in rows] if rows else []
            total_streams = sum(r["streams"] for r in rows) if rows else 0

            return CheckResult("9.1.3", "Streaming protocols in use",
                "Streaming Inventory", None, "info",
                f"{total_streams:,} active streams (7d)",
                "Track protocol diversity",
                details={"protocols": nc},
                recommendation=None)
        except Exception as e:
            return CheckResult("9.1.3", "Streaming protocols",
                "Streaming Inventory", None, "info",
                f"Query failed: {str(e)[:80]}", "N/A")
