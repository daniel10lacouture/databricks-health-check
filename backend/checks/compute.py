"""Section 3: Compute Management — checks for cluster config, policies, utilization, right-sizing.
Only evaluates interactive clusters (cluster_source IN ('UI', 'API')).
All checks include drill-down details with actual objects and recommendations."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


INTERACTIVE_FILTER = "cluster_source IN ('UI', 'API')"


class ComputeCheckRunner(BaseCheckRunner):
    section_id = "compute"
    section_name = "Compute Management"
    section_type = "core"
    icon = "cpu"

    def get_subsections(self):
        return ["Cluster Configuration", "Compute Policies", "Compute Utilization", "Compute Right-Sizing"]

    # ── 3.1 Cluster Configuration ────────────────────────────────────

    def check_3_1_1_auto_termination(self) -> CheckResult:
        try:
            rows = self.executor.execute(f"""
                SELECT cluster_id, cluster_name, auto_termination_minutes, cluster_source
                FROM system.compute.clusters
                WHERE delete_time IS NULL AND {INTERACTIVE_FILTER}""")
        except Exception:
            return CheckResult("3.1.1", "Auto-termination enabled",
                "Cluster Configuration", 0, "not_evaluated",
                "Could not query clusters", "All interactive clusters have auto-termination")
        total = len(rows) or 1
        disabled = [r for r in rows if not r.get("auto_termination_minutes") or r.get("auto_termination_minutes", 0) == 0]
        enabled = [r for r in rows if r.get("auto_termination_minutes") and r.get("auto_termination_minutes", 0) > 0]
        pct = len(disabled) / total * 100
        if pct == 0: score, status = 100, "pass"
        elif pct <= 20: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"cluster_name": r.get("cluster_name",""), "cluster_id": r.get("cluster_id",""),
               "auto_termination_minutes": r.get("auto_termination_minutes", 0),
               "action": "Set auto-termination in cluster settings"} for r in disabled[:20]]
        if not nc:
            nc = [{"cluster_name": r.get("cluster_name",""), "auto_termination_minutes": r.get("auto_termination_minutes", 0),
                   "status": "OK"} for r in enabled[:20]]
        rec = None
        if disabled:
            rec = Recommendation(
                action=f"Enable auto-termination on {len(disabled)} interactive cluster(s).",
                impact="Clusters without auto-termination run indefinitely, wasting compute.",
                priority="high",
                docs_url="https://docs.databricks.com/en/compute/clusters-manage.html#automatic-termination")
        return CheckResult("3.1.1", "Auto-termination enabled",
            "Cluster Configuration", score, status,
            f"{len(disabled)}/{total} interactive clusters without auto-termination",
            "All interactive clusters have auto-termination",
            details={"non_conforming": nc, "total_interactive": total}, recommendation=rec)

    def check_3_1_2_auto_termination_value(self) -> CheckResult:
        try:
            rows = self.executor.execute(f"""
                SELECT cluster_id, cluster_name, auto_termination_minutes
                FROM system.compute.clusters
                WHERE delete_time IS NULL AND {INTERACTIVE_FILTER}
                  AND auto_termination_minutes > 0""")
        except Exception:
            return CheckResult("3.1.2", "Auto-termination value reasonable",
                "Cluster Configuration", 0, "not_evaluated", "Could not query", "<=60 min")
        long = [r for r in rows if (r.get("auto_termination_minutes", 0) or 0) > 60]
        ok = [r for r in rows if (r.get("auto_termination_minutes", 0) or 0) <= 60]
        total = len(rows) or 1
        if not long: score, status = 100, "pass"
        elif len(long) / total <= 0.3: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"cluster_name": r.get("cluster_name",""), "auto_termination_minutes": r.get("auto_termination_minutes",0),
               "action": "Reduce auto-termination to <=60 minutes"} for r in sorted(long, key=lambda x: -(x.get("auto_termination_minutes",0) or 0))[:20]]
        if not nc:
            nc = [{"cluster_name": r.get("cluster_name",""), "auto_termination_minutes": r.get("auto_termination_minutes",0),
                   "status": "OK"} for r in ok[:20]]
        rec = None
        if long:
            rec = Recommendation(
                action=f"Reduce auto-termination on {len(long)} cluster(s) to <=60 minutes.",
                impact="Databricks recommends auto-termination of 1 hour or less to avoid paying for idle time.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/lakehouse-architecture/cost-optimization/best-practices.html")
        return CheckResult("3.1.2", "Auto-termination value reasonable",
            "Cluster Configuration", score, status,
            f"{len(long)} cluster(s) with auto-term >60 min", "<=60 min",
            details={"non_conforming": nc}, recommendation=rec)

    def check_3_1_3_autoscaling(self) -> CheckResult:
        try:
            rows = self.executor.execute(f"""
                SELECT cluster_id, cluster_name, min_autoscale_workers, max_autoscale_workers, worker_count
                FROM system.compute.clusters
                WHERE delete_time IS NULL AND {INTERACTIVE_FILTER}""")
        except Exception:
            return CheckResult("3.1.3", "Autoscaling enabled",
                "Cluster Configuration", 0, "not_evaluated", "Could not query", ">50% autoscaling")
        total = len(rows) or 1
        multi_node = [r for r in rows if (r.get("worker_count") or 0) > 0 or r.get("max_autoscale_workers") is not None]
        if not multi_node:
            return CheckResult("3.1.3", "Autoscaling enabled",
                "Cluster Configuration", 100, "pass", "All clusters are single-node", ">50% autoscaling",
                details={"non_conforming": [{"summary": "All clusters are single-node — autoscaling not applicable."}]})
        autoscale = [r for r in multi_node if r.get("max_autoscale_workers") is not None]
        fixed = [r for r in multi_node if r.get("max_autoscale_workers") is None]
        pct = len(autoscale) / len(multi_node) * 100 if multi_node else 0
        if pct >= 50: score, status = 100, "pass"
        elif pct >= 30: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"cluster_name": r.get("cluster_name",""), "worker_count": r.get("worker_count",0),
               "action": "Enable autoscaling in cluster settings"} for r in fixed[:20]]
        if not nc:
            nc = [{"cluster_name": r.get("cluster_name",""), "min_workers": r.get("min_autoscale_workers",0),
                   "max_workers": r.get("max_autoscale_workers",0), "status": "OK"} for r in autoscale[:20]]
        rec = None
        if score < 100:
            rec = Recommendation(
                action=f"{len(fixed)} multi-node interactive cluster(s) use fixed sizing. Enable autoscaling.",
                impact="Autoscaling reduces cost by scaling down during low utilization.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/compute/configure.html#autoscaling")
        return CheckResult("3.1.3", "Autoscaling enabled",
            "Cluster Configuration", score, status,
            f"{pct:.0f}% of multi-node interactive clusters use autoscaling",
            ">50% autoscaling", details={"non_conforming": nc}, recommendation=rec)

    def check_3_1_6_lts_runtime(self) -> CheckResult:
        try:
            rows = self.executor.execute(f"""
                SELECT cluster_id, cluster_name, dbr_version,
                  REGEXP_EXTRACT(dbr_version, '^(\\\\d+\\\\.\\\\d+)', 1) AS major_ver
                FROM system.compute.clusters
                WHERE delete_time IS NULL AND {INTERACTIVE_FILTER}
                  AND dbr_version IS NOT NULL""")
        except Exception:
            return CheckResult("3.1.6", "Supported runtime versions",
                "Cluster Configuration", 0, "not_evaluated", "Could not query", "All on supported versions")
        total = len(rows) or 1
        supported_prefixes = ["15.4", "16.", "17.", "18."]
        unsupported = [r for r in rows if not any(str(r.get("dbr_version","")).startswith(p) for p in supported_prefixes)]
        supported = [r for r in rows if any(str(r.get("dbr_version","")).startswith(p) for p in supported_prefixes)]
        eol_pct = len(unsupported) / total * 100
        if eol_pct == 0: score, status = 100, "pass"
        elif eol_pct <= 10: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"cluster_name": r.get("cluster_name",""), "dbr_version": r.get("dbr_version",""),
               "action": "Upgrade to 15.4 LTS or 16.4 LTS"} for r in unsupported[:20]]
        if not nc:
            nc = [{"cluster_name": r.get("cluster_name",""), "dbr_version": r.get("dbr_version",""),
                   "status": "OK - supported"} for r in supported[:20]]
        rec = None
        if unsupported:
            versions = list(set(r.get("dbr_version","")[:10] for r in unsupported))[:5]
            rec = Recommendation(
                action=f"Upgrade {len(unsupported)} cluster(s) from EOL runtimes: {', '.join(versions)}",
                impact="EOL runtimes miss security patches, performance improvements, and bug fixes.",
                priority="high" if eol_pct > 20 else "medium",
                docs_url="https://docs.databricks.com/en/release-notes/runtime/index.html")
        return CheckResult("3.1.6", "Supported runtime versions",
            "Cluster Configuration", score, status,
            f"{len(unsupported)}/{total} interactive clusters on EOL runtimes",
            "All on supported versions (15.4 LTS+)",
            details={"non_conforming": nc}, recommendation=rec)

    # ── 3.2 Compute Policies ────────────────────────────────────────

    def check_3_2_1_policy_coverage(self) -> CheckResult:
        try:
            rows = self.executor.execute(f"""
                SELECT cluster_id, cluster_name, policy_id
                FROM system.compute.clusters
                WHERE delete_time IS NULL AND {INTERACTIVE_FILTER}""")
        except Exception:
            return CheckResult("3.2.1", "Cluster policy coverage",
                "Compute Policies", 0, "not_evaluated", "Could not query", ">80% policy-governed")
        total = len(rows) or 1
        no_policy = [r for r in rows if not r.get("policy_id")]
        with_policy = [r for r in rows if r.get("policy_id")]
        pct = len(with_policy) / total * 100
        if pct >= 80: score, status = 100, "pass"
        elif pct >= 50: score, status = 50, "partial"
        else: score, status = 0, "fail"
        nc = [{"cluster_name": r.get("cluster_name",""), "cluster_id": r.get("cluster_id",""),
               "action": "Assign a cluster policy to enforce standards"} for r in no_policy[:20]]
        if not nc:
            nc = [{"cluster_name": r.get("cluster_name",""), "policy_id": r.get("policy_id",""),
                   "status": "OK - policy assigned"} for r in with_policy[:20]]
        rec = None
        if no_policy:
            rec = Recommendation(
                action=f"Assign policies to {len(no_policy)} cluster(s). Policies enforce instance types, auto-term, and runtime constraints.",
                impact="Unpolicied clusters can use any instance type or configuration, leading to cost overruns.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/admin/clusters/policy-definition.html")
        return CheckResult("3.2.1", "Cluster policy coverage",
            "Compute Policies", score, status,
            f"{pct:.0f}% interactive clusters use policies ({len(with_policy)}/{total})",
            ">80% policy-governed", details={"non_conforming": nc}, recommendation=rec)

    # ── 3.3 Compute Utilization ──────────────────────────────────────

    def check_3_3_1_idle_clusters(self) -> CheckResult:
        try:
            rows = self.executor.execute(f"""
                WITH usage AS (
                    SELECT cluster_id,
                        ROUND(AVG(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu
                    FROM system.compute.node_timeline
                    WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
                    GROUP BY 1)
                SELECT c.cluster_id, c.cluster_name, u.avg_cpu
                FROM system.compute.clusters c
                JOIN usage u ON c.cluster_id = u.cluster_id
                WHERE c.delete_time IS NULL AND c.{INTERACTIVE_FILTER.replace('cluster_source', 'cluster_source')}
                ORDER BY u.avg_cpu ASC LIMIT 30""")
        except Exception:
            return CheckResult("3.3.1", "Idle interactive clusters (<10% CPU)",
                "Compute Utilization", 0, "not_evaluated", "Could not query", "No idle clusters")
        idle = [r for r in rows if (r.get("avg_cpu", 0) or 0) < 10]
        active = [r for r in rows if (r.get("avg_cpu", 0) or 0) >= 10]
        if not idle:
            nc = [{"cluster_name": r.get("cluster_name",""), "avg_cpu_pct": r.get("avg_cpu",0),
                   "status": "OK - active"} for r in active[:20]]
            return CheckResult("3.3.1", "Idle interactive clusters (<10% CPU)",
                "Compute Utilization", 100, "pass",
                f"No idle interactive clusters (checked {len(rows)})", "No idle clusters",
                details={"non_conforming": nc})
        nc = [{"cluster_name": r.get("cluster_name",""), "cluster_id": r.get("cluster_id",""),
               "avg_cpu_pct": r.get("avg_cpu",0),
               "action": "Terminate if unused, or reduce auto-termination timeout"} for r in idle[:20]]
        score = 0 if len(idle) > 5 else 50
        rec = Recommendation(
            action=f"{len(idle)} interactive cluster(s) are idle (<10% CPU in 7d). Terminate or reduce auto-termination.",
            impact="Idle clusters waste compute. Consider shorter auto-termination or serverless alternatives.",
            priority="high" if len(idle) > 5 else "medium")
        return CheckResult("3.3.1", "Idle interactive clusters (<10% CPU)",
            "Compute Utilization", score, "fail" if score == 0 else "partial",
            f"{len(idle)} idle interactive clusters (<10% avg CPU)",
            "No idle clusters", details={"non_conforming": nc}, recommendation=rec)

    # ── 3.4 Compute Right-Sizing (Tier 2) ────────────────────────────

    def check_3_4_1_oversized_clusters(self) -> CheckResult:
        """Tier 2: Identify clusters with high core counts but low CPU usage."""
        try:
            rows = self.executor.execute(f"""
                WITH usage AS (
                    SELECT cluster_id,
                        ROUND(AVG(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu,
                        ROUND(AVG(mem_used_percent), 1) AS avg_mem
                    FROM system.compute.node_timeline
                    WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
                    GROUP BY 1)
                SELECT c.cluster_id, c.cluster_name, c.driver_node_type, c.worker_node_type,
                    c.worker_count, c.max_autoscale_workers,
                    u.avg_cpu, u.avg_mem
                FROM system.compute.clusters c
                JOIN usage u ON c.cluster_id = u.cluster_id
                WHERE c.delete_time IS NULL AND c.{INTERACTIVE_FILTER.replace('cluster_source', 'cluster_source')}
                    AND u.avg_cpu < 30
                ORDER BY u.avg_cpu ASC LIMIT 20""")
        except Exception:
            return CheckResult("3.4.1", "Compute right-sizing opportunities",
                "Compute Right-Sizing", 0, "not_evaluated",
                "Could not query", "All clusters properly sized")

        oversized = [r for r in rows if (r.get("avg_cpu", 0) or 0) < 20]
        if not oversized:
            nc = [{"cluster_name": r.get("cluster_name",""), "avg_cpu": r.get("avg_cpu",0),
                   "avg_mem": r.get("avg_mem",0), "worker_type": r.get("worker_node_type",""),
                   "status": "OK - reasonably utilized"} for r in rows[:20]]
            return CheckResult("3.4.1", "Compute right-sizing opportunities",
                "Compute Right-Sizing", 100, "pass",
                f"All interactive clusters reasonably utilized (checked {len(rows)})",
                "All clusters properly sized",
                details={"non_conforming": nc})

        nc = [{"cluster_name": r.get("cluster_name",""), "worker_type": r.get("worker_node_type",""),
               "workers": r.get("worker_count") or r.get("max_autoscale_workers", "autoscale"),
               "avg_cpu_pct": r.get("avg_cpu",0), "avg_mem_pct": r.get("avg_mem",0),
               "action": "Downsize to a smaller instance type or reduce worker count"} for r in oversized[:20]]

        score = 0 if len(oversized) > 5 else 50
        return CheckResult("3.4.1", "Compute right-sizing opportunities",
            "Compute Right-Sizing", score, "fail" if score == 0 else "partial",
            f"{len(oversized)} cluster(s) with <20% avg CPU — likely oversized",
            "All clusters properly sized",
            details={"non_conforming": nc},
            recommendation=Recommendation(
                action=f"{len(oversized)} cluster(s) average <20% CPU. Downsize instance types or reduce worker count to match actual usage.",
                impact="Right-sizing eliminates wasted compute. A cluster at 15% CPU could use a 50% smaller instance type.",
                priority="high" if len(oversized) > 5 else "medium",
                docs_url="https://docs.databricks.com/en/compute/configure.html"))
