"""Section 2: Data Warehousing / SQL Analytics — checks for warehouse config, sizing, query performance.
All checks include drill-down details with actual objects and recommendations."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class SQLAnalyticsCheckRunner(BaseCheckRunner):
    section_id = "sql_analytics"
    section_name = "Data Warehousing / SQL Analytics"
    section_type = "core"
    icon = "database"

    def get_subsections(self):
        return ["Warehouse Inventory & Configuration", "Warehouse Sizing Score",
                "Query Performance", "Warehouse Scaling Efficiency", "Semantic Layer & Data Modeling"]

    def check_2_1_1_warehouse_type_distribution(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT warehouse_id, warehouse_name, warehouse_type, warehouse_size,
                    auto_stop_minutes, min_clusters, max_clusters
                FROM system.compute.warehouses
                WHERE delete_time IS NULL""")
        except Exception:
            return CheckResult("2.1.1", "Warehouse type distribution",
                "Warehouse Inventory & Configuration", 0, "not_evaluated",
                "Could not query warehouses", ">80% serverless")
        total = len(rows) or 1
        serverless = [r for r in rows if r.get("warehouse_type") == "SERVERLESS"]
        non_sl = [r for r in rows if r.get("warehouse_type") != "SERVERLESS"]
        pct = len(serverless) / total * 100
        if pct >= 80: score, status = 100, "pass"
        elif pct >= 30: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"warehouse": r.get("warehouse_name",""), "id": r.get("warehouse_id",""),
               "type": r.get("warehouse_type",""), "size": r.get("warehouse_size",""),
               "action": "Migrate to serverless: edit warehouse > enable serverless"} for r in non_sl[:20]]
        if not nc:
            nc = [{"warehouse": r.get("warehouse_name",""), "id": r.get("warehouse_id",""),
                   "type": "SERVERLESS", "size": r.get("warehouse_size",""),
                   "status": "OK"} for r in serverless[:20]]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Migrate {len(non_sl)} warehouse(s) to serverless (currently {pct:.0f}%).",
                impact="Serverless eliminates idle costs and provides instant startup.",
                priority="high" if pct < 30 else "medium",
                docs_url="https://docs.databricks.com/en/compute/sql-warehouse/serverless.html")
        return CheckResult("2.1.1", "Warehouse type distribution",
            "Warehouse Inventory & Configuration", score, status,
            f"{len(serverless)}/{total} serverless ({pct:.0f}%)", ">80% serverless",
            details={"non_conforming": nc}, recommendation=rec)

    def check_2_1_2_auto_stop(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT warehouse_id, warehouse_name, warehouse_type, auto_stop_minutes
                FROM system.compute.warehouses
                WHERE delete_time IS NULL""")
        except Exception:
            return CheckResult("2.1.2", "Auto-stop configuration",
                "Warehouse Inventory & Configuration", 0, "not_evaluated",
                "Could not query warehouses", "<=10 min serverless, <=15 min classic")
        issues = []
        ok = []
        for r in rows:
            mins = r.get("auto_stop_minutes")
            is_sl = r.get("warehouse_type") == "SERVERLESS"
            threshold = 10 if is_sl else 15
            entry = {"warehouse": r.get("warehouse_name",""), "auto_stop_min": mins,
                     "threshold": threshold, "type": r.get("warehouse_type","")}
            if mins is None or mins == 0 or mins > threshold:
                entry["action"] = f"Set auto-stop to <={threshold} min in warehouse settings"
                issues.append(entry)
            else:
                entry["status"] = "OK"
                ok.append(entry)
        total = len(rows) or 1
        pct_ok = (total - len(issues)) / total * 100
        if pct_ok >= 100: score, status = 100, "pass"
        elif pct_ok >= 50: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = issues[:20] if issues else ok[:20]
        rec = None
        if issues:
            names = [i["warehouse"] for i in issues[:3]]
            rec = Recommendation(
                action=f"Set auto-stop <=10m serverless, <=15m classic. Fix: {', '.join(names)}",
                impact="Long auto-stop wastes compute during idle periods.",
                priority="high" if len(issues) > 2 else "medium")
        return CheckResult("2.1.2", "Auto-stop configuration",
            "Warehouse Inventory & Configuration", score, status,
            f"{len(issues)} warehouse(s) with excessive auto-stop",
            "All within thresholds", details={"non_conforming": nc}, recommendation=rec)

    def check_2_1_5_unused_warehouses(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                WITH active AS (
                    SELECT DISTINCT compute.warehouse_id AS warehouse_id FROM system.query.history
                    WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                      AND compute.warehouse_id IS NOT NULL)
                SELECT w.warehouse_id, w.warehouse_name, w.warehouse_type, w.warehouse_size
                FROM system.compute.warehouses w
                LEFT JOIN active a ON w.warehouse_id = a.warehouse_id
                WHERE a.warehouse_id IS NULL AND w.delete_time IS NULL""")
        except Exception:
            return CheckResult("2.1.5", "Unused warehouses (>30d no queries)",
                "Warehouse Inventory & Configuration", 0, "not_evaluated",
                "Could not query", "0 unused warehouses")
        nc = [{"warehouse": r.get("warehouse_name",""), "id": r.get("warehouse_id",""),
               "type": r.get("warehouse_type",""), "size": r.get("warehouse_size",""),
               "action": "Delete warehouse if no longer needed"} for r in rows[:20]]
        rec = None
        if rows:
            names = [r.get("warehouse_name","") for r in rows[:3]]
            rec = Recommendation(
                action=f"Delete {len(rows)} unused warehouse(s): {', '.join(names)}.",
                impact="Eliminates auto-start cost and management overhead.",
                priority="medium")
        return CheckResult("2.1.5", "Unused warehouses (>30d no queries)",
            "Warehouse Inventory & Configuration",
            100 if not rows else 0, "pass" if not rows else "fail",
            f"{len(rows)} unused", "0 unused warehouses",
            details={"non_conforming": nc if nc else [{"summary": "All warehouses have been used in the last 30 days."}]},
            recommendation=rec)

    def check_2_2_1_spill_frequency(self) -> CheckResult:
        rows = self.executor.execute("""
            SELECT compute.warehouse_id AS warehouse_id,
                COUNT(*) AS total_queries,
                SUM(CASE WHEN spilled_local_bytes > 0 THEN 1 ELSE 0 END) AS spill_queries,
                ROUND(SUM(spilled_local_bytes) / (1024*1024*1024), 2) AS total_spill_gb
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                AND compute.warehouse_id IS NOT NULL
                AND statement_type NOT IN ('SET','USE','SHOW')
            GROUP BY 1 ORDER BY 3 DESC""")
        total_q = sum(int(r.get("total_queries", 0)) for r in rows) or 1
        spill_q = sum(int(r.get("spill_queries", 0)) for r in rows)
        spill_pct = spill_q / total_q * 100
        if spill_pct < 5: score, status = 100, "pass"
        elif spill_pct < 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"warehouse_id": r.get("warehouse_id",""), "total_queries": r.get("total_queries",0),
               "spill_queries": r.get("spill_queries",0), "spill_gb": r.get("total_spill_gb",0),
               "action": "Upsize warehouse or optimize queries"} for r in rows if int(r.get("spill_queries",0)) > 0][:20]
        if not nc:
            nc = [{"summary": f"No spill detected across {total_q} queries."}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{spill_pct:.1f}% of queries spill to disk. Upsize affected warehouses.",
                impact="Spill-to-disk dramatically increases query latency.",
                priority="high" if spill_pct > 20 else "medium",
                docs_url="https://docs.databricks.com/en/compute/sql-warehouse/index.html")
        return CheckResult("2.2.1", "Spill to disk frequency",
            "Warehouse Sizing Score", score, status,
            f"{spill_pct:.1f}% queries spill ({spill_q}/{total_q})",
            "<5% of queries spill", details={"non_conforming": nc}, recommendation=rec)

    def check_2_2_2_queue_wait_time(self) -> CheckResult:
        rows = self.executor.execute("""
            SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY waiting_at_capacity_duration_ms) AS p95_queue_ms
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                AND compute.warehouse_id IS NOT NULL
                AND statement_type NOT IN ('SET','USE','SHOW')""")
        if not rows:
            return CheckResult("2.2.2", "Query queue wait time (p95)",
                "Warehouse Sizing Score", 100, "pass", "No query data", "p95 <30s",
                details={"non_conforming": [{"summary": "No warehouse queries found."}]})
        p95_ms = float(rows[0].get("p95_queue_ms", 0) or 0)
        p95_s = p95_ms / 1000
        if p95_s < 30: score, status = 100, "pass"
        elif p95_s < 300: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"metric": "p95 queue wait", "value_seconds": round(p95_s, 1),
               "threshold": "30s", "status": "PASS" if score == 100 else "FAIL"}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"p95 queue time is {p95_s:.0f}s. Increase max clusters or upsize the warehouse.",
                impact="Long queue times degrade interactive user experience.",
                priority="high" if p95_s > 300 else "medium")
        return CheckResult("2.2.2", "Query queue wait time (p95)",
            "Warehouse Sizing Score", score, status,
            f"p95 queue: {p95_s:.1f}s", "p95 <30 seconds",
            details={"non_conforming": nc}, recommendation=rec)

    def check_2_2_3_query_duration(self) -> CheckResult:
        rows = self.executor.execute("""
            SELECT
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_duration_ms) AS p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_duration_ms) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_duration_ms) AS p99
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                AND compute.warehouse_id IS NOT NULL
                AND statement_type NOT IN ('SET','USE','SHOW')""")
        if not rows:
            return CheckResult("2.2.3", "Query duration distribution",
                "Warehouse Sizing Score", 100, "pass", "No query data", "p95 <60s",
                details={"non_conforming": [{"summary": "No queries found."}]})
        r = rows[0]
        p50 = float(r.get("p50",0) or 0)/1000
        p95 = float(r.get("p95",0) or 0)/1000
        p99 = float(r.get("p99",0) or 0)/1000
        if p95 < 60: score, status = 100, "pass"
        elif p95 < 300: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"percentile": "p50", "seconds": round(p50, 1)},
              {"percentile": "p95", "seconds": round(p95, 1)},
              {"percentile": "p99", "seconds": round(p99, 1)}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Query p95 is {p95:.0f}s. Review slow queries and consider upsizing.",
                impact="Slow queries impact user productivity.",
                priority="medium")
        return CheckResult("2.2.3", "Query duration distribution",
            "Warehouse Sizing Score", score, status,
            f"p50={p50:.1f}s, p95={p95:.1f}s, p99={p99:.1f}s",
            "p95 <60 seconds", details={"non_conforming": nc}, recommendation=rec)

    def check_2_3_3_repeated_expensive_queries(self) -> CheckResult:
        """Show actual query text, execution count, avg duration, and estimated cost."""
        rows = self.executor.execute("""
            SELECT LEFT(statement_text, 200) AS query_preview,
                COUNT(*) AS exec_count,
                ROUND(AVG(total_duration_ms) / 1000, 1) AS avg_duration_s,
                ROUND(SUM(total_duration_ms) / 1000, 0) AS total_time_s,
                SUM(CASE WHEN from_result_cache THEN 1 ELSE 0 END) AS cache_hits,
                MAX(executed_by) AS last_user
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
                AND compute.warehouse_id IS NOT NULL AND statement_type = 'SELECT'
                AND total_duration_ms > 5000
            GROUP BY 1 HAVING COUNT(*) >= 5
            ORDER BY COUNT(*) * AVG(total_duration_ms) DESC LIMIT 15""")
        if not rows:
            return CheckResult("2.3.3", "Repeated expensive queries (no cache)",
                "Query Performance", 100, "pass",
                "No repeated expensive queries", "High-frequency queries should hit cache",
                details={"non_conforming": [{"summary": "No repeated expensive queries found in the last 7 days."}]})

        nc = [{"query_preview": r.get("query_preview","")[:150], "executions": r.get("exec_count",0),
               "avg_duration_s": r.get("avg_duration_s",0), "total_time_s": r.get("total_time_s",0),
               "cache_hits": r.get("cache_hits",0), "last_user": r.get("last_user",""),
               "action": "Create a materialized view or enable result caching"} for r in rows]

        return CheckResult("2.3.3", "Repeated expensive queries (no cache)",
            "Query Performance", 50, "partial",
            f"{len(rows)} expensive queries repeated >=5x without cache",
            "High-frequency queries should hit cache",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action="Review top repeated expensive queries. Consider materialized views or result caching.",
                impact="Caching repeated queries can reduce warehouse load by 50%+.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/sql/language-manual/sql-ref-materialized-views.html"))

    # ── 2.4 Warehouse Scaling Efficiency (Tier 2) ────────────────────

    def check_2_4_1_warehouse_scaling_thrash(self) -> CheckResult:
        """Tier 2: Detect warehouses with excessive scale-up/down events."""
        try:
            rows = self.executor.execute("""
                SELECT warehouse_id, event_type, COUNT(*) AS events
                FROM system.compute.warehouse_events
                WHERE event_time >= DATEADD(DAY, -7, CURRENT_DATE())
                    AND event_type IN ('SCALED_UP', 'SCALED_DOWN')
                GROUP BY 1, 2
                ORDER BY 3 DESC""")
        except Exception:
            return CheckResult("2.4.1", "Warehouse scaling efficiency",
                "Warehouse Scaling Efficiency", 0, "not_evaluated",
                "Could not query warehouse events", "No excessive scaling (>100 events/day)")

        # Aggregate by warehouse
        wh_events = {}
        for r in rows:
            wid = r.get("warehouse_id", "")
            if wid not in wh_events:
                wh_events[wid] = {"scale_up": 0, "scale_down": 0}
            if r.get("event_type") == "SCALED_UP":
                wh_events[wid]["scale_up"] = int(r.get("events", 0))
            else:
                wh_events[wid]["scale_down"] = int(r.get("events", 0))

        # Thrashing = >100 scale events per day (700/week)
        thrashing = [(wid, d) for wid, d in wh_events.items()
                     if d["scale_up"] + d["scale_down"] > 700]

        if not thrashing:
            nc = [{"warehouse_id": wid, "scale_ups_7d": d["scale_up"], "scale_downs_7d": d["scale_down"],
                   "status": "OK"} for wid, d in list(wh_events.items())[:20]]
            return CheckResult("2.4.1", "Warehouse scaling efficiency",
                "Warehouse Scaling Efficiency", 100, "pass",
                f"No warehouses with excessive scaling ({len(wh_events)} checked)",
                "No excessive scaling (>100 events/day)",
                details={"non_conforming": nc})

        nc = [{"warehouse_id": wid, "scale_ups_7d": d["scale_up"], "scale_downs_7d": d["scale_down"],
               "events_per_day": round((d["scale_up"] + d["scale_down"]) / 7),
               "action": "Increase min_clusters to reduce thrash, or review workload patterns"} for wid, d in thrashing[:20]]

        score = 0 if len(thrashing) > 3 else 50
        return CheckResult("2.4.1", "Warehouse scaling efficiency",
            "Warehouse Scaling Efficiency", score, "fail" if score == 0 else "partial",
            f"{len(thrashing)} warehouse(s) with excessive scaling (>100/day)",
            "No excessive scaling (>100 events/day)",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action=f"{len(thrashing)} warehouse(s) scale up/down excessively. Increase min_clusters to reduce cold-start overhead.",
                impact="Frequent scaling causes cold-start latency and cluster provisioning overhead. Increasing min_clusters keeps warm capacity available.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/compute/sql-warehouse/configure.html"))

    def check_2_5_1_metric_views(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT table_catalog, table_schema, table_name
                FROM system.information_schema.tables
                WHERE table_type = 'MATERIALIZED_VIEW'
                LIMIT 20""")
        except Exception:
            return CheckResult("2.5.1", "Materialized / Metric views",
                "Semantic Layer & Data Modeling", 0, "not_evaluated",
                "Could not query", "Metric views defined")
        count = len(rows)
        nc = [{"catalog": r.get("table_catalog",""), "schema": r.get("table_schema",""),
               "table": r.get("table_name","")} for r in rows[:20]]
        if not nc:
            nc = [{"summary": "No materialized views defined."}]
        if count > 10: score, status = 100, "pass"
        elif count > 0: score, status = 50, "partial"
        else: score, status = 0, "fail"
        rec = None
        if score < 100:
            rec = Recommendation(
                action="Define metric views for key business metrics in Unity Catalog.",
                impact="Metric views provide a governed semantic layer for consistent definitions.",
                priority="low",
                docs_url="https://docs.databricks.com/en/sql/language-manual/sql-ref-materialized-views.html")
        return CheckResult("2.5.1", "Materialized / Metric views",
            "Semantic Layer & Data Modeling", score, status,
            f"{count} materialized/metric views", "Business metrics defined as views",
            details={"non_conforming": nc}, recommendation=rec)

    # ── Tier 1: Query Error Rate ─────────────────────────────────────

    def check_2_6_1_query_error_rate(self):
        """Tier 1: 495K failed queries/week (4% failure rate)."""
        rows = self.executor.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS errors
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
        """)
        total = rows[0]["total"] or 0
        errors = rows[0]["errors"] or 0
        rate = errors / max(total, 1) * 100
        if rate <= 2: score, status = 100, "pass"
        elif rate <= 5: score, status = 50, "partial"
        else: score, status = 0, "fail"

        top_errors = self.executor.execute("""
            SELECT SUBSTRING(error_message, 1, 120) AS error_pattern,
                   COUNT(*) AS occurrences,
                   COUNT(DISTINCT user_name) AS affected_users,
                   COUNT(DISTINCT compute.warehouse_id) AS warehouses
            FROM system.query.history
            WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
              AND execution_status = 'FAILED' AND error_message IS NOT NULL
            GROUP BY 1 ORDER BY occurrences DESC LIMIT 15
        """)
        nc = top_errors if top_errors else [{"status": "Healthy", "error_rate": f"{rate:.1f}%"}]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Investigate {errors:,} query errors this week ({rate:.1f}% error rate). Focus on the top error patterns affecting the most users.",
                impact="Query errors indicate broken dashboards, stale views, or permission issues that degrade user trust.",
                priority="high" if rate > 5 else "medium",
                docs_url="https://docs.databricks.com/en/sql/admin/query-history.html")
        return CheckResult("2.6.1", "Query error rate (7d)", "Query Performance",
            score, status, f"{rate:.1f}% ({errors:,}/{total:,} queries)", "≤2% error rate",
            details={"top_error_patterns": nc}, recommendation=rec)

    # ── 2.7 Warehouse Scaling Efficiency ─────────────────────────────

    def check_2_7_1_warehouse_scaling_efficiency(self) -> CheckResult:
        """Analyze warehouse scaling patterns from warehouse_events."""
        try:
            rows = self.executor.execute("""
                SELECT warehouse_id,
                       SUM(CASE WHEN event_type = 'SCALED_UP' THEN 1 ELSE 0 END) AS scale_ups,
                       SUM(CASE WHEN event_type = 'SCALED_DOWN' THEN 1 ELSE 0 END) AS scale_downs,
                       SUM(CASE WHEN event_type = 'QUEUED' THEN 1 ELSE 0 END) AS queue_events,
                       SUM(CASE WHEN event_type = 'STARTING' THEN 1 ELSE 0 END) AS starts,
                       COUNT(*) AS total_events
                FROM system.compute.warehouse_events
                WHERE event_time >= DATEADD(DAY, -14, CURRENT_DATE())
                GROUP BY warehouse_id ORDER BY queue_events DESC
            """)
        except Exception as e:
            return CheckResult("2.7.1", "Warehouse scaling efficiency (14d)", "Warehouse Scaling Efficiency",
                0, "not_evaluated", f"Could not query warehouse_events: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("2.7.1", "Warehouse scaling efficiency (14d)", "Warehouse Scaling Efficiency",
                None, "info", "No warehouse events data available", "Minimal queueing events")

        total_queues = sum(int(r.get("queue_events", 0) or 0) for r in rows)
        total_scale_ups = sum(int(r.get("scale_ups", 0) or 0) for r in rows)
        heavy_queueing = [r for r in rows if int(r.get("queue_events", 0) or 0) > 50]

        if total_queues == 0: score, status = 100, "pass"
        elif len(heavy_queueing) == 0: score, status = 85, "pass"
        elif len(heavy_queueing) <= 2: score, status = 60, "partial"
        else: score, status = 35, "fail"

        nc = [{"warehouse_id": r.get("warehouse_id", ""), "scale_ups": r.get("scale_ups", 0),
               "scale_downs": r.get("scale_downs", 0), "queue_events": r.get("queue_events", 0),
               "starts": r.get("starts", 0)} for r in rows[:15]]

        rec = None
        if score < 85:
            rec = Recommendation(
                action=f"{total_queues:,} queueing events across {len(heavy_queueing)} warehouses. Increase max_num_clusters or consider serverless for elastic scaling.",
                impact="Query queueing degrades user experience and dashboard refresh times. Proper scaling eliminates wait times.",
                priority="high" if len(heavy_queueing) > 2 else "medium",
                docs_url="https://docs.databricks.com/en/sql/admin/warehouse-type.html")
        return CheckResult("2.7.1", "Warehouse scaling efficiency (14d)", "Warehouse Scaling Efficiency",
            score, status, f"{total_queues:,} queue events, {total_scale_ups:,} scale-ups across {len(rows)} warehouses",
            "Minimal queueing events", details={"non_conforming": nc}, recommendation=rec)

