"""
Section 4: Cost Optimization — 19 checks
Covers spend analysis, idle resource detection, governance & attribution.
All checks include drill-down details with actual objects and recommendations.
"""
from checks.base import BaseCheckRunner, CheckResult, Recommendation, Status


class CostCheckRunner(BaseCheckRunner):
    section_id = "cost_optimization"
    section_name = "Cost Optimization"
    section_type = "core"
    icon = "dollar-sign"

    def get_subsections(self):
        return ["Spend Analysis", "Idle Resource Detection", "Governance & Attribution", "Cloud Infrastructure Cost", "Resource Efficiency", "Cost Intelligence"]

    # ── 4.1 Spend Analysis ────────────────────────────────────────────

    def check_4_1_1_mom_spend_trend(self) -> CheckResult:
        """Month-over-month spend trend — flag >30% spikes."""
        rows = self.executor.execute("""
            SELECT
                date_trunc('month', usage_date) AS month,
                SUM(u.usage_quantity * lp.pricing.default) AS total_cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices lp
                ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                AND u.usage_date >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
            WHERE u.usage_date >= DATEADD(DAY, -180, CURRENT_DATE())
            GROUP BY 1 ORDER BY 1
        """)
        if len(rows) < 2:
            return CheckResult("4.1.1", "Month-over-month spend trend",
                "Spend Analysis", 100, "pass",
                "Insufficient data for trend analysis", "No unexpected spikes >30%",
                details={"non_conforming": [{"summary": "Less than 2 months of billing data available."}]})

        nc = []
        max_spike = 0
        spike_month = None
        for i in range(1, len(rows)):
            prev = float(rows[i-1].get("total_cost", 0) or 0)
            curr = float(rows[i].get("total_cost", 0) or 0)
            change = ((curr - prev) / prev * 100) if prev > 0 else 0
            month_str = str(rows[i].get("month", ""))[:7]
            nc.append({"month": month_str, "cost": round(curr, 2), "prev_cost": round(prev, 2),
                       "change_pct": round(change, 1)})
            if change > max_spike:
                max_spike = change
                spike_month = month_str

        if max_spike > 30:
            return CheckResult("4.1.1", "Month-over-month spend trend",
                "Spend Analysis", 0, "fail",
                f"{max_spike:.0f}% spike in {spike_month}", "No unexpected spikes >30%",
                details={"non_conforming": nc},
                recommendation=Recommendation(
                    action=f"Investigate the {max_spike:.0f}% spend spike in {spike_month}. Check for new workloads, misconfigured auto-scaling, or runaway jobs.",
                    impact="Unexpected spikes may indicate waste or misconfigurations.",
                    priority="high",
                    docs_url="https://docs.databricks.com/en/admin/system-tables/billing.html"))
        return CheckResult("4.1.1", "Month-over-month spend trend",
            "Spend Analysis", 100, "pass",
            f"Max MoM change: {max_spike:.0f}%", "No unexpected spikes >30%",
            details={"non_conforming": nc, "summary": "Monthly spend trend is stable."})

    def check_4_1_2_allpurpose_vs_job(self) -> CheckResult:
        """All-purpose vs. job compute spend ratio."""
        rows = self.executor.execute("""
            SELECT
                CASE WHEN u.sku_name LIKE '%ALL_PURPOSE%' OR u.sku_name LIKE '%ALL PURPOSE%' THEN 'all_purpose'
                     WHEN u.sku_name LIKE '%JOBS%' OR u.sku_name LIKE '%JOB%' THEN 'jobs'
                     ELSE 'other' END AS compute_type,
                SUM(u.usage_quantity * lp.pricing.default) AS cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices lp
                ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                AND u.usage_date >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
            WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1
        """)
        costs = {r["compute_type"]: float(r.get("cost", 0) or 0) for r in rows}
        total = sum(costs.values()) or 1
        ap_pct = costs.get("all_purpose", 0) / total * 100

        if ap_pct > 50: score, status = 0, "fail"
        elif ap_pct > 30: score, status = 50, "partial"
        else: score, status = 100, "pass"

        nc = [{"compute_type": k, "cost_30d": round(v, 2), "pct": round(v/total*100, 1)} for k, v in sorted(costs.items(), key=lambda x: -x[1])]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Migrate interactive workloads to job clusters. All-purpose compute is {ap_pct:.0f}% of spend.",
                impact=f"Job clusters are significantly cheaper. Potential savings: ${costs.get('all_purpose',0)*(1-0.3):.0f}/month if 70% migrated.",
                priority="high" if ap_pct > 50 else "medium",
                docs_url="https://docs.databricks.com/en/compute/use-compute.html")

        return CheckResult("4.1.2", "All-purpose vs. job compute spend ratio",
            "Spend Analysis", score, status,
            f"All-purpose: {ap_pct:.0f}% of compute spend", "All-purpose <30% of total compute",
            details={"non_conforming": nc}, recommendation=rec)

    def check_4_1_3_serverless_ratio(self) -> CheckResult:
        """Serverless vs. classic spend ratio."""
        rows = self.executor.execute("""
            SELECT
                CASE WHEN u.sku_name LIKE '%SERVERLESS%' THEN 'serverless' ELSE 'classic' END AS type,
                SUM(u.usage_quantity * lp.pricing.default) AS cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices lp
                ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                AND u.usage_date >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
            WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1
        """)
        costs = {r["type"]: float(r.get("cost", 0) or 0) for r in rows}
        total = sum(costs.values()) or 1
        sl_pct = costs.get("serverless", 0) / total * 100

        if sl_pct >= 50: score, status = 100, "pass"
        elif sl_pct >= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"type": k, "cost_30d": round(v, 2), "pct": round(v/total*100, 1)} for k, v in sorted(costs.items(), key=lambda x: -x[1])]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Increase serverless adoption (currently {sl_pct:.0f}%). Migrate SQL warehouses and jobs to serverless.",
                impact="Serverless eliminates idle costs and provides instant scaling.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/compute/serverless.html")

        return CheckResult("4.1.3", "Serverless vs. classic spend ratio",
            "Spend Analysis", score, status,
            f"Serverless: {sl_pct:.0f}% of spend", ">50% serverless",
            details={"non_conforming": nc}, recommendation=rec)

    def check_4_1_4_top_expensive_jobs(self) -> CheckResult:
        """Top 10 most expensive jobs — informational."""
        rows = self.executor.execute("""
            SELECT
                u.usage_metadata.job_id AS job_id,
                COALESCE(j.name, CONCAT('Job ', u.usage_metadata.job_id)) AS job_name,
                SUM(u.usage_quantity * lp.pricing.default) AS cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices lp
                ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                AND u.usage_date >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
            LEFT JOIN system.lakeflow.jobs j ON CAST(u.usage_metadata.job_id AS STRING) = CAST(j.job_id AS STRING)
            WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                AND u.usage_metadata.job_id IS NOT NULL
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
        """)
        nc = [{"job_name": r.get("job_name",""), "job_id": r.get("job_id",""),
               "cost_30d": round(float(r.get("cost",0) or 0), 2),
               "action": "Review job for optimization opportunities"} for r in rows]
        return CheckResult("4.1.4", "Top 10 most expensive jobs",
            "Spend Analysis", 0, "info",
            f"{len(rows)} jobs analyzed", "Informational",
            details={"non_conforming": nc, "summary": "Review top-cost jobs for optimization. Consider serverless compute, smaller clusters, or result caching."},
            recommendation=Recommendation(
                action="Review top expensive jobs for compute right-sizing, caching, and serverless migration.",
                impact="Top 10 jobs often represent 60-80% of total spend. Small optimizations yield big savings.",
                priority="low"))

    def check_4_1_5_top_expensive_warehouses(self) -> CheckResult:
        """Top 10 most expensive warehouses — informational."""
        try:
            rows = self.executor.execute("""
                SELECT u.usage_metadata.warehouse_id AS warehouse_id,
                    w.warehouse_name, w.warehouse_type,
                    SUM(u.usage_quantity * lp.pricing.default) AS cost
                FROM system.billing.usage u
                LEFT JOIN system.billing.list_prices lp
                    ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                    AND u.usage_date >= lp.price_start_time
                    AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
                LEFT JOIN system.compute.warehouses w ON u.usage_metadata.warehouse_id = w.warehouse_id
                WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND u.usage_metadata.warehouse_id IS NOT NULL
                GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 10
            """)
        except Exception:
            rows = self.executor.execute("""
                SELECT u.usage_metadata.warehouse_id AS warehouse_id,
                    SUM(u.usage_quantity * lp.pricing.default) AS cost
                FROM system.billing.usage u
                LEFT JOIN system.billing.list_prices lp
                    ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                    AND u.usage_date >= lp.price_start_time
                    AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
                WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND u.usage_metadata.warehouse_id IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10
            """)
        nc = [{"warehouse_name": r.get("warehouse_name", r.get("warehouse_id","")),
               "warehouse_id": r.get("warehouse_id",""),
               "type": r.get("warehouse_type",""),
               "cost_30d": round(float(r.get("cost",0) or 0), 2),
               "action": "Review sizing and auto-stop configuration"} for r in rows]
        return CheckResult("4.1.5", "Top 10 most expensive warehouses",
            "Spend Analysis", 0, "info",
            f"{len(rows)} warehouses analyzed", "Informational",
            details={"non_conforming": nc, "summary": "Review warehouse sizing — downsize or enable serverless to reduce costs."},
            recommendation=Recommendation(
                action="Review top warehouse costs. Consider serverless migration, auto-stop tuning, or downsizing.",
                impact="Warehouse costs are the #1 SQL Analytics cost driver.",
                priority="low"))

    def check_4_1_6_offhours_spend(self) -> CheckResult:
        """Weekend/off-hours spend analysis."""
        rows = self.executor.execute("""
            SELECT
                CASE WHEN dayofweek(usage_date) IN (1, 7) THEN 'weekend' ELSE 'weekday' END AS day_type,
                SUM(u.usage_quantity * lp.pricing.default) AS cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices lp
                ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                AND u.usage_date >= lp.price_start_time
                AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
            WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
            GROUP BY 1
        """)
        costs = {r["day_type"]: float(r.get("cost",0) or 0) for r in rows}
        total = sum(costs.values()) or 1
        weekend_pct = costs.get("weekend", 0) / total * 100

        nc = [{"day_type": k, "cost_30d": round(v, 2), "pct": round(v/total*100, 1)} for k, v in costs.items()]

        if weekend_pct > 20:
            score, status = 50, "partial"
            rec = Recommendation(
                action=f"Review weekend spend ({weekend_pct:.0f}% of total). Consider pausing non-critical workloads.",
                impact=f"Potential savings: ${costs.get('weekend',0)*0.5:.0f}/month by reducing weekend usage by 50%.",
                priority="medium")
        else:
            score, status = 100, "pass"
            rec = None

        return CheckResult("4.1.6", "Weekend/off-hours spend",
            "Spend Analysis", score, status,
            f"Weekend spend: {weekend_pct:.0f}% of total", "<10% off-hours unless justified",
            details={"non_conforming": nc}, recommendation=rec)

    # ── 4.2 Idle Resource Detection ───────────────────────────────────

    def check_4_2_1_idle_warehouses(self) -> CheckResult:
        """Idle warehouses — billing hours with 0 queries."""
        rows = self.executor.execute("""
            WITH warehouse_usage AS (
                SELECT u.usage_metadata.warehouse_id AS wh_id,
                    SUM(u.usage_quantity) AS total_dbus
                FROM system.billing.usage u
                WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND u.usage_metadata.warehouse_id IS NOT NULL
                GROUP BY 1),
            warehouse_queries AS (
                SELECT compute.warehouse_id AS wh_id, COUNT(*) AS query_count
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY 1)
            SELECT wu.wh_id, wu.total_dbus, COALESCE(wq.query_count, 0) AS query_count
            FROM warehouse_usage wu
            LEFT JOIN warehouse_queries wq ON wu.wh_id = wq.wh_id
        """)
        idle = [r for r in rows if (r.get("query_count") or 0) == 0 and (r.get("total_dbus") or 0) > 0]
        active = [r for r in rows if (r.get("query_count") or 0) > 0]
        total = len(rows) or 1

        if len(idle) > total * 0.1: score, status = 0, "fail"
        elif len(idle) > 0: score, status = 50, "partial"
        else: score, status = 100, "pass"

        nc = [{"warehouse_id": r.get("wh_id",""), "dbus_30d": round(float(r.get("total_dbus",0) or 0), 2),
               "queries": 0, "action": "Delete or stop this warehouse"} for r in idle[:20]]
        if not nc:
            nc = [{"warehouse_id": r.get("wh_id",""), "dbus_30d": round(float(r.get("total_dbus",0) or 0), 2),
                   "queries": r.get("query_count",0), "status": "OK - active"} for r in active[:20]]

        rec = None
        if idle:
            rec = Recommendation(
                action=f"Delete or stop {len(idle)} idle warehouse(s) that consumed DBUs with zero queries.",
                impact="Eliminating idle warehouses saves 100% of their cost.",
                priority="high",
                docs_url="https://docs.databricks.com/en/compute/sql-warehouse/index.html")

        return CheckResult("4.2.1", "Idle warehouses (billing with 0 queries)",
            "Idle Resource Detection", score, status,
            f"{len(idle)} idle warehouse(s) out of {len(rows)}", "0 idle warehouses",
            details={"non_conforming": nc}, recommendation=rec)

    def check_4_2_2_idle_clusters(self) -> CheckResult:
        """Idle interactive clusters — running with very low CPU utilization."""
        try:
            rows = self.executor.execute("""
                SELECT n.cluster_id, c.cluster_name,
                    ROUND(AVG(n.cpu_user_percent + n.cpu_system_percent), 1) AS avg_cpu
                FROM system.compute.node_timeline n
                JOIN system.compute.clusters c ON n.cluster_id = c.cluster_id
                WHERE n.start_time >= DATEADD(DAY, -7, CURRENT_DATE())
                  AND c.cluster_source IN ('UI', 'API')
                GROUP BY 1, 2
                HAVING AVG(n.cpu_user_percent + n.cpu_system_percent) < 5
            """)
        except Exception:
            return CheckResult("4.2.2", "Idle interactive clusters",
                "Idle Resource Detection", 0, "not_evaluated",
                "Could not query node_timeline", "0 idle interactive clusters")

        if len(rows) > 5: score, status = 0, "fail"
        elif len(rows) > 0: score, status = 50, "partial"
        else: score, status = 100, "pass"

        nc = [{"cluster_name": r.get("cluster_name",""), "cluster_id": r.get("cluster_id",""),
               "avg_cpu_pct": r.get("avg_cpu",0),
               "action": "Terminate or reduce auto-termination timeout"} for r in rows[:20]]
        if not nc:
            nc = [{"summary": "No idle interactive clusters found (all >5% avg CPU)."}]

        rec = None
        if rows:
            rec = Recommendation(
                action=f"Investigate {len(rows)} interactive cluster(s) with <5% avg CPU.",
                impact="Idle interactive clusters consume compute cost with no productive work.",
                priority="high" if len(rows) > 5 else "medium")

        return CheckResult("4.2.2", "Idle interactive clusters",
            "Idle Resource Detection", score, status,
            f"{len(rows)} interactive cluster(s) with <5% avg CPU",
            "0 idle interactive clusters", details={"non_conforming": nc}, recommendation=rec)

    def check_4_2_4_unused_serving_endpoints(self) -> CheckResult:
        """Unused model serving endpoints — 0 requests in 30 days."""
        try:
            rows = self.executor.execute("""
                SELECT se.endpoint_name, se.served_entity_name, se.entity_type,
                    COUNT(eu.client_request_id) AS request_count
                FROM system.serving.served_entities se
                LEFT JOIN system.serving.endpoint_usage eu
                    ON se.served_entity_id = eu.served_entity_id
                    AND eu.request_time >= DATEADD(DAY, -30, CURRENT_DATE())
                WHERE se.endpoint_delete_time IS NULL
                GROUP BY 1, 2, 3""")
        except Exception:
            return CheckResult("4.2.4", "Unused model serving endpoints",
                "Idle Resource Detection", 0, "not_evaluated",
                "Could not query serving tables", "No unused endpoints")

        unused = [r for r in rows if int(r.get("request_count", 0)) == 0]
        active = [r for r in rows if int(r.get("request_count", 0)) > 0]

        nc = [{"endpoint": r.get("endpoint_name",""), "entity": r.get("served_entity_name",""),
               "type": r.get("entity_type",""), "requests_30d": 0,
               "action": "Delete endpoint if no longer needed"} for r in unused[:20]]
        if not nc:
            nc = [{"endpoint": r.get("endpoint_name",""), "entity": r.get("served_entity_name",""),
                   "requests_30d": r.get("request_count",0), "status": "OK - active"} for r in active[:20]]

        rec = None
        if unused:
            names = ", ".join(r.get("endpoint_name","") for r in unused[:3])
            rec = Recommendation(
                action=f"Delete {len(unused)} unused serving endpoint(s): {names}. Zero requests in 30 days.",
                impact="Unused endpoints incur provisioned compute costs.",
                priority="high")

        return CheckResult("4.2.4", "Unused model serving endpoints",
            "Idle Resource Detection", 100 if not unused else 0, "pass" if not unused else "fail",
            f"{len(unused)} unused endpoint(s)", "0 unused endpoints",
            details={"non_conforming": nc}, recommendation=rec)

    def check_4_2_5_unused_warehouses(self) -> CheckResult:
        """Unused SQL warehouses — 0 queries in 30 days."""
        try:
            rows = self.executor.execute("""
                WITH active AS (
                    SELECT DISTINCT compute.warehouse_id AS warehouse_id
                    FROM system.query.history
                    WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND compute.warehouse_id IS NOT NULL)
                SELECT w.warehouse_id, w.warehouse_name, w.warehouse_type, w.warehouse_size
                FROM system.compute.warehouses w
                LEFT JOIN active a ON w.warehouse_id = a.warehouse_id
                WHERE a.warehouse_id IS NULL AND w.delete_time IS NULL""")
        except Exception:
            return CheckResult("4.2.5", "Unused SQL warehouses (0 queries in 30d)",
                "Idle Resource Detection", 0, "not_evaluated",
                "Could not query warehouse tables", "0 unused warehouses")

        nc = [{"warehouse": r.get("warehouse_name", r.get("warehouse_id","")),
               "warehouse_id": r.get("warehouse_id",""),
               "type": r.get("warehouse_type",""), "size": r.get("warehouse_size",""),
               "action": "Delete warehouse if no longer needed"} for r in rows[:20]]
        if not nc:
            nc = [{"summary": "All warehouses have been used in the last 30 days."}]

        rec = None
        if rows:
            names = [r.get("warehouse_name", r.get("warehouse_id","")) for r in rows[:3]]
            rec = Recommendation(
                action=f"Delete {len(rows)} unused warehouse(s): {', '.join(names)}. Zero queries in 30 days.",
                impact="Eliminates auto-start cost and reduces management overhead.",
                priority="medium")

        return CheckResult("4.2.5", "Unused SQL warehouses (0 queries in 30d)",
            "Idle Resource Detection", 100 if not rows else 0, "pass" if not rows else "fail",
            f"{len(rows)} unused warehouse(s)", "0 unused warehouses",
            details={"non_conforming": nc}, recommendation=rec)

    # ── 4.3 Governance & Attribution ──────────────────────────────────

    def check_4_3_1_tag_coverage(self) -> CheckResult:
        """Tag coverage on compute resources."""
        try:
            rows = self.executor.execute("""
                SELECT cluster_id, cluster_name, tags
                FROM system.compute.clusters
                WHERE delete_time IS NULL
            """)
        except Exception:
            return CheckResult("4.3.1", "Tag coverage on compute resources",
                "Governance & Attribution", 0, "not_evaluated",
                "Could not query cluster data", ">80% tagged")

        total = len(rows) or 1
        tagged = [r for r in rows if r.get("tags") and len(r.get("tags", {})) > 0]
        untagged = [r for r in rows if not r.get("tags") or len(r.get("tags", {})) == 0]
        tagged_pct = len(tagged) / total * 100

        if tagged_pct >= 80: score, status = 100, "pass"
        elif tagged_pct >= 30: score, status = 50, "partial"
        else: score, status = 0, "fail"

        nc = [{"cluster_name": r.get("cluster_name",""), "cluster_id": r.get("cluster_id",""),
               "action": "Add cost-attribution tags in cluster settings"} for r in untagged[:20]]
        if not nc:
            nc = [{"cluster_name": r.get("cluster_name",""), "status": "OK - tagged"} for r in tagged[:20]]

        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"Add tags to {len(untagged)} untagged cluster(s). Enforce via compute policies.",
                impact="Tags enable cost attribution by team/project/environment.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/compute/configure.html#cluster-tags")

        return CheckResult("4.3.1", "Tag coverage on compute resources",
            "Governance & Attribution", score, status,
            f"{tagged_pct:.0f}% of clusters tagged ({len(tagged)}/{total})", ">80% tagged",
            details={"non_conforming": nc}, recommendation=rec)

    def check_4_3_3_cost_per_query_trend(self) -> CheckResult:
        """Cost per query trend — advisory."""
        try:
            rows = self.executor.execute("""
                WITH monthly_cost AS (
                    SELECT date_trunc('month', usage_date) AS month,
                        SUM(usage_quantity * lp.pricing.default) AS total_cost
                    FROM system.billing.usage u
                    LEFT JOIN system.billing.list_prices lp
                        ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                        AND u.usage_date >= lp.price_start_time
                        AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
                    WHERE u.usage_date >= DATEADD(DAY, -90, CURRENT_DATE())
                        AND u.usage_metadata.warehouse_id IS NOT NULL
                    GROUP BY 1),
                monthly_queries AS (
                    SELECT date_trunc('month', start_time) AS month, COUNT(*) AS query_count
                    FROM system.query.history
                    WHERE start_time >= DATEADD(DAY, -90, CURRENT_DATE())
                    GROUP BY 1)
                SELECT mc.month, mc.total_cost, mq.query_count,
                    CASE WHEN mq.query_count > 0 THEN mc.total_cost / mq.query_count ELSE 0 END AS cost_per_query
                FROM monthly_cost mc
                LEFT JOIN monthly_queries mq ON mc.month = mq.month
                ORDER BY 1
            """)
        except Exception:
            return CheckResult("4.3.3", "Cost per query trend",
                "Governance & Attribution", 0, "not_evaluated",
                "Could not compute cost/query", "Stable or decreasing")

        nc = [{"month": str(r.get("month",""))[:7],
               "total_cost": round(float(r.get("total_cost",0) or 0), 2),
               "query_count": int(r.get("query_count",0) or 0),
               "cost_per_query": round(float(r.get("cost_per_query",0) or 0), 4)} for r in rows]

        return CheckResult("4.3.3", "Cost per query trend",
            "Governance & Attribution", 0, "info",
            f"{len(rows)} months analyzed", "Stable or decreasing",
            details={"non_conforming": nc, "summary": "Track cost-per-query over time to measure efficiency improvements."},
            recommendation=Recommendation(
                action="Monitor cost-per-query trend monthly. Rising costs may indicate inefficient queries or over-provisioned warehouses.",
                impact="Cost-per-query is the key efficiency metric for SQL workloads.",
                priority="low"))

    # ── 4.4 Cloud Infrastructure Cost ────────────────────────────────

    def check_4_4_1_total_cost_with_infra(self) -> CheckResult:
        """Compare DBU cost vs total cost including cloud infrastructure."""
        try:
            rows = self.executor.execute("""
                WITH dbu_cost AS (
                    SELECT ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)), 2) AS total_dbu_cost
                    FROM system.billing.usage u
                    LEFT JOIN system.billing.list_prices lp
                        ON u.sku_name = lp.sku_name
                        AND u.usage_date >= lp.price_start_time
                        AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
                    WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                ),
                infra_cost AS (
                    SELECT ROUND(SUM(cost), 2) AS total_infra_cost
                    FROM system.billing.cloud_infra_cost
                    WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                )
                SELECT d.total_dbu_cost, i.total_infra_cost,
                       ROUND(d.total_dbu_cost + COALESCE(i.total_infra_cost, 0), 2) AS true_total,
                       CASE WHEN (d.total_dbu_cost + COALESCE(i.total_infra_cost, 0)) > 0
                            THEN ROUND(COALESCE(i.total_infra_cost, 0) / (d.total_dbu_cost + COALESCE(i.total_infra_cost, 0)) * 100, 1)
                            ELSE 0 END AS infra_pct
                FROM dbu_cost d, infra_cost i
            """)
        except Exception as e:
            return CheckResult("4.4.1", "Total cost incl. cloud infra (30d)", "Cloud Infrastructure Cost",
                0, "not_evaluated", f"Could not query cloud_infra_cost: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("4.4.1", "Total cost incl. cloud infra (30d)", "Cloud Infrastructure Cost",
                None, "info", "No cloud infrastructure cost data available", "Track true total cost",
                recommendation=Recommendation(
                    action="Enable cloud_infra_cost system table to understand true total cost beyond DBUs.",
                    impact="Cloud VM/storage costs often add 30-60% on top of DBU spend — tracking both gives the full picture.",
                    priority="medium"))

        r = rows[0]
        dbu = float(r.get("total_dbu_cost", 0) or 0)
        infra = float(r.get("total_infra_cost", 0) or 0)
        total = float(r.get("true_total", 0) or 0)
        infra_pct = float(r.get("infra_pct", 0) or 0)

        score, status = (None, "info")
        nc = [{"dbu_cost": f"${dbu:,.2f}", "infra_cost": f"${infra:,.2f}",
               "true_total": f"${total:,.2f}", "infra_percent": f"{infra_pct:.1f}%"}]

        rec = Recommendation(
            action=f"Cloud infra is {infra_pct:.1f}% of total spend (${infra:,.0f}/${total:,.0f}). Review VM sizing and storage to optimize the non-DBU portion.",
            impact="Understanding true total cost enables more accurate budgeting and identifies cloud-layer optimization opportunities.",
            priority="medium" if infra_pct > 40 else "low",
            docs_url="https://docs.databricks.com/en/admin/system-tables/billing.html")
        return CheckResult("4.4.1", "Total cost incl. cloud infra (30d)", "Cloud Infrastructure Cost",
            score, status, f"${total:,.0f} total (${dbu:,.0f} DBU + ${infra:,.0f} infra, {infra_pct:.1f}% cloud)",
            "Track true total cost", details={"non_conforming": nc}, recommendation=rec)

    def check_4_4_2_cost_by_product(self) -> CheckResult:
        """Break down cost by Databricks product (billing_origin_product)."""
        try:
            rows = self.executor.execute("""
                SELECT billing_origin_product AS product,
                       ROUND(SUM(usage_quantity), 0) AS total_dbus,
                       COUNT(DISTINCT usage_date) AS active_days,
                       ROUND(SUM(usage_quantity) / COUNT(DISTINCT usage_date), 0) AS avg_daily_dbus
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND billing_origin_product IS NOT NULL
                GROUP BY billing_origin_product
                ORDER BY total_dbus DESC
            """)
        except Exception as e:
            return CheckResult("4.4.2", "Cost by product breakdown (30d)", "Cloud Infrastructure Cost",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("4.4.2", "Cost by product breakdown (30d)", "Cloud Infrastructure Cost",
                None, "info", "No billing data available", "Understand product cost distribution")

        total_dbus = sum(float(r.get("total_dbus", 0) or 0) for r in rows)
        nc = [{"product": r.get("product", "unknown"),
               "total_dbus": f"{float(r.get('total_dbus', 0) or 0):,.0f}",
               "pct_of_total": f"{float(r.get('total_dbus', 0) or 0) / total_dbus * 100:.1f}%" if total_dbus > 0 else "0%",
               "active_days": r.get("active_days", 0),
               "avg_daily_dbus": f"{float(r.get('avg_daily_dbus', 0) or 0):,.0f}"} for r in rows]

        top_product = rows[0].get("product", "unknown") if rows else "N/A"
        top_pct = float(rows[0].get("total_dbus", 0) or 0) / total_dbus * 100 if total_dbus > 0 else 0

        return CheckResult("4.4.2", "Cost by product breakdown (30d)", "Cloud Infrastructure Cost",
            None, "info", f"{len(rows)} products — top: {top_product} ({top_pct:.0f}%)",
            "Understand product cost distribution",
            details={"non_conforming": nc, "summary": f"Total: {total_dbus:,.0f} DBUs across {len(rows)} products"},
            recommendation=Recommendation(
                action=f"Review product-level spend. {top_product} consumes {top_pct:.0f}% of DBUs. Look for consolidation or optimization opportunities.",
                impact="Product-level cost visibility enables targeted optimization of the highest-spend areas.",
                priority="low"))

    # ── 4.5 Resource Efficiency ───────────────────────────────────────

    def check_4_5_1_csp_infra_cost_breakdown(self) -> CheckResult:
        """Break down cloud provider infrastructure costs (EC2/VMs/storage) by cluster and warehouse."""
        try:
            rows = self.executor.execute("""
                SELECT cloud,
                       usage_metadata.cluster_id AS resource_id,
                       CASE
                           WHEN usage_metadata.warehouse_id IS NOT NULL THEN 'warehouse'
                           WHEN usage_metadata.cluster_id IS NOT NULL THEN 'cluster'
                           ELSE 'other'
                       END AS resource_type,
                       ROUND(SUM(cost), 2) AS total_cost,
                       currency_code
                FROM system.billing.cloud_infra_cost
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY cloud, usage_metadata.cluster_id, 
                         CASE WHEN usage_metadata.warehouse_id IS NOT NULL THEN 'warehouse'
                              WHEN usage_metadata.cluster_id IS NOT NULL THEN 'cluster'
                              ELSE 'other' END,
                         currency_code
                ORDER BY total_cost DESC
            """)
        except Exception as e:
            return CheckResult("4.5.1", "CSP infrastructure cost breakdown (30d)", "Resource Efficiency",
                0, "not_evaluated", f"Could not query cloud_infra_cost: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("4.5.1", "CSP infrastructure cost breakdown (30d)", "Resource Efficiency",
                None, "info",
                "No cloud infrastructure cost data available. This table may not be enabled — contact your Databricks account team to activate system.billing.cloud_infra_cost.",
                "Track CSP costs alongside DBU spend",
                recommendation=Recommendation(
                    action="Enable cloud_infra_cost system table to get visibility into your AWS/Azure/GCP compute and storage spend that runs underneath Databricks clusters.",
                    impact="CSP infrastructure can be 30-50% of total Databricks-related spend. Migrating to serverless eliminates this cost entirely.",
                    priority="medium",
                    docs_url="https://docs.databricks.com/en/admin/system-tables/billing.html"))

        total = sum(float(r.get("total_cost", 0) or 0) for r in rows)
        cluster_cost = sum(float(r.get("total_cost", 0) or 0) for r in rows if r.get("resource_type") == "cluster")
        wh_cost = sum(float(r.get("total_cost", 0) or 0) for r in rows if r.get("resource_type") == "warehouse")
        other_cost = total - cluster_cost - wh_cost
        currency = rows[0].get("currency_code", "USD") if rows else "USD"
        cloud = rows[0].get("cloud", "unknown") if rows else "unknown"

        # Score: lower CSP cost relative to total is better; serverless eliminates CSP cost
        # If CSP cost > 0 there's a serverless migration opportunity  
        score = 50 if total > 1000 else 80
        status = "partial" if total > 1000 else "pass"

        top_resources = rows[:15]
        nc = [{"resource_id": r.get("resource_id", "N/A") or "unattributed",
               "resource_type": r.get("resource_type", "unknown"),
               "cost": f"${float(r.get('total_cost', 0) or 0):,.2f}",
               "cloud": r.get("cloud", "N/A")} for r in top_resources]

        rec = Recommendation(
            action=f"Your {cloud.upper()} infrastructure costs {currency} {total:,.0f}/mo on Databricks-related resources. "
                   f"Clusters account for ${cluster_cost:,.0f}, warehouses ${wh_cost:,.0f}. "
                   f"Migrating to serverless compute eliminates CSP infrastructure charges entirely.",
            impact=f"Potential savings of up to ${total:,.0f}/month by migrating all workloads to serverless — "
                   f"the CSP compute layer is fully managed and included in serverless pricing.",
            priority="high" if total > 10000 else "medium",
            docs_url="https://docs.databricks.com/en/compute/serverless.html")

        return CheckResult("4.5.1", "CSP infrastructure cost breakdown (30d)", "Resource Efficiency",
            score, status,
            f"${total:,.0f} in {cloud.upper()} infra costs (clusters: ${cluster_cost:,.0f}, warehouses: ${wh_cost:,.0f}, other: ${other_cost:,.0f})",
            "Minimize or eliminate CSP infrastructure spend via serverless migration",
            details={"non_conforming": nc, "summary": f"Total CSP spend: ${total:,.0f} {currency} across {len(rows)} resources"},
            recommendation=rec)

    def check_4_5_2_idle_cluster_burn_rate(self) -> CheckResult:
        """Calculate cost being burned on idle/near-idle clusters (CPU < 5%)."""
        try:
            rows = self.executor.execute("""
                WITH idle AS (
                    SELECT cluster_id,
                           COUNT(*) AS idle_intervals,
                           COUNT(DISTINCT instance_id) AS idle_nodes,
                           ROUND(SUM(
                               TIMESTAMPDIFF(SECOND, start_time, end_time)
                           ) / 3600.0, 1) AS idle_node_hours
                    FROM system.compute.node_timeline
                    WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                      AND cpu_user_percent < 5
                      AND cpu_system_percent < 5
                    GROUP BY cluster_id
                ),
                total AS (
                    SELECT cluster_id,
                           ROUND(SUM(
                               TIMESTAMPDIFF(SECOND, start_time, end_time)
                           ) / 3600.0, 1) AS total_node_hours
                    FROM system.compute.node_timeline
                    WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                    GROUP BY cluster_id
                )
                SELECT i.cluster_id,
                       i.idle_node_hours,
                       t.total_node_hours,
                       ROUND(i.idle_node_hours / NULLIF(t.total_node_hours, 0) * 100, 1) AS idle_pct,
                       i.idle_nodes
                FROM idle i
                JOIN total t ON i.cluster_id = t.cluster_id
                WHERE i.idle_node_hours > 10
                ORDER BY i.idle_node_hours DESC
                LIMIT 20
            """)
        except Exception as e:
            return CheckResult("4.5.2", "Idle cluster burn rate (30d)", "Resource Efficiency",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        total_idle_hours = sum(float(r.get("idle_node_hours", 0) or 0) for r in rows)
        total_hours = sum(float(r.get("total_node_hours", 0) or 0) for r in rows)
        idle_pct_overall = (total_idle_hours / total_hours * 100) if total_hours > 0 else 0
        # Conservative estimate: $0.50/node-hour average (mix of instance types)
        est_waste = total_idle_hours * 0.50
        cluster_count = len(rows)

        score = max(0, 100 - int(idle_pct_overall))
        status = "pass" if idle_pct_overall < 15 else "partial" if idle_pct_overall < 35 else "fail"

        nc = [{"cluster_id": r.get("cluster_id", "N/A"),
               "idle_node_hours": f"{float(r.get('idle_node_hours', 0) or 0):,.0f}",
               "total_node_hours": f"{float(r.get('total_node_hours', 0) or 0):,.0f}",
               "idle_pct": f"{float(r.get('idle_pct', 0) or 0):.1f}%",
               "idle_nodes": r.get("idle_nodes", 0)} for r in rows[:15]]

        rec = Recommendation(
            action=f"{cluster_count} clusters burned {total_idle_hours:,.0f} idle node-hours (est. ${est_waste:,.0f} wasted). "
                   f"Enable auto-termination, right-size clusters, or migrate to serverless to eliminate idle costs.",
            impact=f"Estimated ${est_waste:,.0f}/month in idle compute waste. Serverless compute scales to zero automatically.",
            priority="high" if est_waste > 5000 else "medium" if est_waste > 1000 else "low",
            docs_url="https://docs.databricks.com/en/compute/configure.html#auto-termination")

        return CheckResult("4.5.2", "Idle cluster burn rate (30d)", "Resource Efficiency",
            score, status,
            f"{total_idle_hours:,.0f} idle node-hours across {cluster_count} clusters (~${est_waste:,.0f} estimated waste)",
            "< 15% idle time across clusters",
            details={"non_conforming": nc, "summary": f"{idle_pct_overall:.1f}% of compute node-hours were idle"},
            recommendation=rec)

    def check_4_5_3_warehouse_idle_cost(self) -> CheckResult:
        """Estimate warehouse idle time from start/stop event patterns."""
        try:
            rows = self.executor.execute("""
                WITH events_ranked AS (
                    SELECT warehouse_id, event_type, event_time,
                           LEAD(event_type) OVER (PARTITION BY warehouse_id ORDER BY event_time) AS next_event,
                           LEAD(event_time) OVER (PARTITION BY warehouse_id ORDER BY event_time) AS next_time
                    FROM system.compute.warehouse_events
                    WHERE event_time >= DATEADD(DAY, -30, CURRENT_DATE())
                      AND event_type IN ('RUNNING', 'STOPPING', 'STOPPED', 'SCALED_UP', 'SCALED_DOWN')
                ),
                idle_windows AS (
                    SELECT warehouse_id,
                           TIMESTAMPDIFF(SECOND, event_time, next_time) / 3600.0 AS hours_in_state,
                           cluster_count
                    FROM (
                        SELECT e.warehouse_id, e.event_time, e.next_time,
                               we.cluster_count
                        FROM events_ranked e
                        JOIN system.compute.warehouse_events we
                            ON e.warehouse_id = we.warehouse_id AND e.event_time = we.event_time
                        WHERE e.event_type = 'RUNNING'
                          AND e.next_event IN ('STOPPING', 'STOPPED')
                          AND TIMESTAMPDIFF(SECOND, e.event_time, e.next_time) > 300
                    )
                )
                SELECT warehouse_id,
                       ROUND(SUM(hours_in_state), 1) AS idle_hours,
                       COUNT(*) AS idle_sessions,
                       MAX(cluster_count) AS max_clusters
                FROM idle_windows
                GROUP BY warehouse_id
                ORDER BY idle_hours DESC
                LIMIT 15
            """)
        except Exception as e:
            return CheckResult("4.5.3", "Warehouse idle time (30d)", "Resource Efficiency",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        total_idle = sum(float(r.get("idle_hours", 0) or 0) for r in rows)
        wh_count = len(rows)

        if not rows or total_idle < 1:
            return CheckResult("4.5.3", "Warehouse idle time (30d)", "Resource Efficiency",
                100, "pass", "Minimal warehouse idle time detected — warehouses are shutting down efficiently.",
                "Minimize idle warehouse hours",
                recommendation=Recommendation(
                    action="Warehouse auto-stop is working well. Consider serverless warehouses for even faster cold starts.",
                    impact="Serverless warehouses start in under 2 seconds and have zero idle cost.",
                    priority="low"))

        score = max(0, 100 - min(60, int(total_idle / 10)))
        status = "pass" if total_idle < 50 else "partial" if total_idle < 200 else "fail"

        nc = [{"warehouse_id": r.get("warehouse_id", "N/A"),
               "idle_hours": f"{float(r.get('idle_hours', 0) or 0):,.1f}",
               "idle_sessions": r.get("idle_sessions", 0),
               "max_clusters": r.get("max_clusters", 0)} for r in rows]

        rec = Recommendation(
            action=f"{wh_count} warehouses had {total_idle:,.0f} hours of idle running time. "
                   f"Reduce auto-stop timeout or migrate to serverless warehouses (zero idle cost, sub-2s startup).",
            impact=f"Serverless warehouses eliminate idle costs entirely — no compute charges when not processing queries.",
            priority="high" if total_idle > 200 else "medium",
            docs_url="https://docs.databricks.com/en/compute/sql-warehouse/serverless.html")

        return CheckResult("4.5.3", "Warehouse idle time (30d)", "Resource Efficiency",
            score, status,
            f"{total_idle:,.0f} idle hours across {wh_count} warehouses",
            "< 50 total idle warehouse-hours/month",
            details={"non_conforming": nc},
            recommendation=rec)

    # ── 4.6 Cost Intelligence ─────────────────────────────────────────

    def check_4_6_1_cost_per_active_user(self) -> CheckResult:
        """Calculate cost per active user to benchmark efficiency."""
        try:
            rows = self.executor.execute("""
                WITH spend AS (
                    SELECT ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)), 2) AS total_cost
                    FROM system.billing.usage u
                    LEFT JOIN system.billing.list_prices lp
                        ON u.cloud = lp.cloud AND u.sku_name = lp.sku_name
                        AND u.usage_date >= lp.price_start_time
                        AND (lp.price_end_time IS NULL OR u.usage_date < lp.price_end_time)
                    WHERE u.usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                      AND u.usage_unit = 'DBU'
                ),
                users AS (
                    SELECT COUNT(DISTINCT executed_by) AS active_users
                    FROM system.query.history
                    WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                      AND executed_by IS NOT NULL
                )
                SELECT s.total_cost, u.active_users,
                       ROUND(s.total_cost / NULLIF(u.active_users, 0), 2) AS cost_per_user
                FROM spend s CROSS JOIN users u
            """)
        except Exception as e:
            return CheckResult("4.6.1", "Cost per active user (30d)", "Cost Intelligence",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("4.6.1", "Cost per active user (30d)", "Cost Intelligence",
                None, "info", "No billing or user data available", "Track per-user cost efficiency")

        r = rows[0]
        total = float(r.get("total_cost", 0) or 0)
        users = int(r.get("active_users", 0) or 0)
        cpu = float(r.get("cost_per_user", 0) or 0)

        # Benchmark: < $500/user/mo is efficient, > $2000 is high
        score = 100 if cpu < 500 else 80 if cpu < 1000 else 60 if cpu < 2000 else 40
        status = "pass" if cpu < 500 else "partial" if cpu < 2000 else "fail"

        nc = [{"total_cost": f"${total:,.0f}", "active_users": f"{users:,}",
               "cost_per_user": f"${cpu:,.0f}"}]

        rec = Recommendation(
            action=f"Cost per active user is ${cpu:,.0f}/month ({users:,} users, ${total:,.0f} total). "
                   + ("This is efficient — focus on increasing adoption to drive more value." if cpu < 500
                      else "Review whether high-cost users are running inefficient queries or using oversized compute."),
            impact="Per-user cost benchmarking reveals whether platform investment is well-distributed across the organization.",
            priority="low" if cpu < 500 else "medium" if cpu < 2000 else "high",
            docs_url="https://docs.databricks.com/en/admin/system-tables/billing.html")

        return CheckResult("4.6.1", "Cost per active user (30d)", "Cost Intelligence",
            score, status,
            f"${cpu:,.0f}/user/month ({users:,} active users, ${total:,.0f} total)",
            "< $500/user/month",
            details={"non_conforming": nc},
            recommendation=rec)

    def check_4_6_2_top_expensive_queries(self) -> CheckResult:
        """Identify the most expensive queries by total task duration."""
        try:
            rows = self.executor.execute("""
                SELECT executed_by,
                       statement_type,
                       ROUND(total_task_duration_ms / 1000.0 / 3600.0, 2) AS task_hours,
                       ROUND(execution_duration_ms / 1000.0 / 60.0, 1) AS wall_clock_min,
                       ROUND(read_bytes / (1024.0 * 1024 * 1024), 2) AS read_gb,
                       SUBSTRING(statement_text, 1, 120) AS query_preview,
                       start_time
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND total_task_duration_ms > 0
                  AND execution_status = 'FINISHED'
                ORDER BY total_task_duration_ms DESC
                LIMIT 10
            """)
        except Exception as e:
            return CheckResult("4.6.2", "Top expensive queries (30d)", "Cost Intelligence",
                0, "not_evaluated", f"Could not query: {str(e)[:80]}", "N/A")

        if not rows:
            return CheckResult("4.6.2", "Top expensive queries (30d)", "Cost Intelligence",
                None, "info", "No query history available", "Identify expensive query patterns")

        total_task_hours = sum(float(r.get("task_hours", 0) or 0) for r in rows)
        top_user = rows[0].get("executed_by", "unknown") if rows else "N/A"
        top_hours = float(rows[0].get("task_hours", 0) or 0) if rows else 0

        nc = [{"executed_by": r.get("executed_by", "N/A"),
               "statement_type": r.get("statement_type", "N/A"),
               "task_hours": f"{float(r.get('task_hours', 0) or 0):,.1f}h",
               "wall_clock_min": f"{float(r.get('wall_clock_min', 0) or 0):,.0f}m",
               "read_gb": f"{float(r.get('read_gb', 0) or 0):,.1f} GB",
               "query_preview": (r.get("query_preview", "")[:100] or "N/A")} for r in rows]

        # Score: if top query used > 100 task-hours, there's optimization opportunity
        score = 100 if top_hours < 10 else 70 if top_hours < 50 else 50 if top_hours < 200 else 30
        status = "pass" if top_hours < 10 else "partial" if top_hours < 200 else "fail"

        rec = Recommendation(
            action=f"Top 10 queries consumed {total_task_hours:,.0f} task-hours in 30 days. "
                   f"Heaviest: {top_user} ({top_hours:,.0f} task-hours). "
                   f"Review for missing filters, unnecessary full scans, or opportunities to use materialized views.",
            impact="Optimizing the top 10 most expensive queries often reduces total compute spend by 15-30%.",
            priority="high" if total_task_hours > 500 else "medium",
            docs_url="https://docs.databricks.com/en/optimizations/index.html")

        return CheckResult("4.6.2", "Top expensive queries (30d)", "Cost Intelligence",
            score, status,
            f"Top 10 queries consumed {total_task_hours:,.0f} task-hours — heaviest by {top_user}",
            "< 10 task-hours per individual query",
            details={"non_conforming": nc, "summary": f"Top 10 queries: {total_task_hours:,.0f} total task-hours"},
            recommendation=rec)

