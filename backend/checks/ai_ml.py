"""Section: AI & ML Workloads — checks with full drill-downs for all statuses."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class AIMLCheckRunner(BaseCheckRunner):
    section_id = "ai_ml"
    section_name = "AI & ML Workloads"
    section_type = "conditional"
    icon = "brain"

    def get_subsections(self):
        return ["Model Lifecycle & Registry", "Model Serving", "AI Gateway"]

    def is_active(self) -> bool:
        try:
            rows = self.executor.execute("""
                SELECT SUM(usage_quantity) AS total
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                    AND (sku_name LIKE '%MODEL_SERVING%' OR sku_name LIKE '%FOUNDATION_MODEL%'
                         OR sku_name LIKE '%VECTOR_SEARCH%')""")
            return float(rows[0].get("total", 0) or 0) > 0 if rows else False
        except Exception:
            return False

    def check_7_1_4_experiment_tracking(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT experiment_id, name AS experiment_name
                FROM system.mlflow.experiments_latest
                LIMIT 20""")
        except Exception:
            return CheckResult("7.1.4", "MLflow experiment tracking",
                "Model Lifecycle & Registry", 0, "not_evaluated",
                "Could not query experiments", "Active experiments exist")
        count = len(rows)
        nc = [{"experiment_id": r.get("experiment_id",""), "name": r.get("experiment_name","")} for r in rows[:20]]
        if count > 0:
            return CheckResult("7.1.4", "MLflow experiment tracking",
                "Model Lifecycle & Registry", 100, "pass",
                f"{count} experiments found", "Active experiments exist",
                details={"non_conforming": nc, "summary": f"Top {count} experiments."})
        return CheckResult("7.1.4", "MLflow experiment tracking",
            "Model Lifecycle & Registry", 0, "fail",
            "No MLflow experiments", "Active experiments exist",
            details={"non_conforming": [{"summary": "No experiments found. Set up MLflow experiment tracking."}]},
            recommendation=Recommendation(
                action="Set up MLflow experiment tracking for ML workloads.",
                impact="Experiment tracking enables reproducibility and model comparison.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/mlflow/tracking.html"))

    def check_7_2_3_endpoint_utilization(self) -> CheckResult:
        try:
            rows = self.executor.execute("""
                SELECT se.endpoint_name, se.served_entity_name, se.entity_type,
                    COUNT(eu.client_request_id) AS requests
                FROM system.serving.served_entities se
                LEFT JOIN system.serving.endpoint_usage eu
                    ON se.served_entity_id = eu.served_entity_id
                    AND eu.request_time >= DATEADD(DAY, -7, CURRENT_DATE())
                WHERE se.endpoint_delete_time IS NULL
                GROUP BY 1, 2, 3""")
        except Exception:
            return CheckResult("7.2.3", "Serving endpoint utilization",
                "Model Serving", 0, "not_evaluated",
                "Could not query serving data", "No unused endpoints")
        unused = [r for r in rows if int(r.get("requests", 0)) == 0]
        active = [r for r in rows if int(r.get("requests", 0)) > 0]
        nc_unused = [{"endpoint_name": r.get("endpoint_name",""), "entity_name": r.get("served_entity_name",""),
                      "entity_type": r.get("entity_type",""), "requests_7d": 0,
                      "action": "Delete if no longer needed"} for r in unused[:20]]
        nc_active = [{"endpoint_name": r.get("endpoint_name",""), "requests_7d": r.get("requests",0),
                      "status": "OK - active"} for r in sorted(active, key=lambda x: -int(x.get("requests",0)))[:10]]
        
        if not unused:
            return CheckResult("7.2.3", "Serving endpoint utilization",
                "Model Serving", 100, "pass",
                f"All {len(active)} endpoint(s) actively serving", "No unused endpoints in 7d",
                details={"non_conforming": nc_active})
        score = 0 if len(unused) > 3 else 50
        names = ", ".join(r.get("endpoint_name","") for r in unused[:3])
        return CheckResult("7.2.3", "Serving endpoint utilization",
            "Model Serving", score, "fail" if score == 0 else "partial",
            f"{len(unused)} endpoint(s) with 0 requests in 7d", "No unused endpoints",
            details={"non_conforming": nc_unused, "active_endpoints": nc_active},
            recommendation=Recommendation(
                action=f"Review {len(unused)} idle endpoint(s): {names}",
                impact="Idle endpoints incur provisioned compute costs.",
                priority="high"))

    def check_7_2_4_ai_gateway_usage(self) -> CheckResult:
        """Check AI Gateway usage and routing."""
        try:
            rows = self.executor.execute("""
                SELECT endpoint_name, destination_model, COUNT(*) AS requests,
                    COUNT(DISTINCT requester) AS users,
                    ROUND(AVG(latency_ms)) AS avg_latency_ms
                FROM system.ai_gateway.usage
                WHERE event_time >= DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY 1, 2 ORDER BY 3 DESC""")
        except Exception:
            return CheckResult("7.2.4", "AI Gateway usage",
                "AI Gateway", 0, "not_evaluated",
                "Could not query AI Gateway", "Active gateway endpoints")

        total_requests = sum(int(r.get("requests", 0)) for r in rows)
        endpoints = len(set(r.get("endpoint_name","") for r in rows))
        nc = [{"endpoint": r.get("endpoint_name",""), "model": r.get("destination_model",""),
               "requests_7d": r.get("requests",0), "users": r.get("users",0),
               "avg_latency_ms": int(r.get("avg_latency_ms",0) or 0)} for r in rows[:20]]

        return CheckResult("7.2.4", "AI Gateway usage",
            "AI Gateway", 0, "info",
            f"{endpoints} gateway endpoint(s), {total_requests:,} requests in 7 days",
            "Informational", details={"non_conforming": nc},
            recommendation=Recommendation(
                action="Route all LLM traffic through AI Gateway for centralized monitoring, rate limiting, and cost control.",
                impact="AI Gateway provides guardrails, logging, and fallback routing for all model calls.",
                priority="low",
                docs_url="https://docs.databricks.com/en/ai-gateway/index.html"))

    def check_7_1_5_mlflow_experiment_activity(self) -> CheckResult:
        """Check MLflow experiment and run activity."""
        try:
            rows = self.executor.execute("""
                SELECT experiment_id, COUNT(*) AS total_runs,
                    SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed
                FROM system.mlflow.runs_latest
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY 1 ORDER BY 2 DESC LIMIT 20""")
            stats = self.executor.execute("""
                SELECT COUNT(DISTINCT experiment_id) AS experiments,
                    COUNT(*) AS total_runs,
                    SUM(CASE WHEN status = 'FINISHED' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed
                FROM system.mlflow.runs_latest
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())""")
        except Exception:
            return CheckResult("7.1.5", "MLflow experiment activity",
                "Model Lifecycle & Registry", 0, "not_evaluated",
                "Could not query MLflow", "Active experiments")
        
        s = stats[0] if stats else {}
        experiments = int(s.get("experiments", 0))
        total_runs = int(s.get("total_runs", 0))
        completed = int(s.get("completed", 0))
        failed = int(s.get("failed", 0))
        fail_pct = (failed / max(total_runs, 1)) * 100
        
        nc = [{"experiment_id": r.get("experiment_id",""), "total_runs": r.get("total_runs",0),
               "completed": r.get("completed",0), "failed": r.get("failed",0)} for r in rows[:20]]
        
        if fail_pct < 10: score, status = 100, "pass"
        elif fail_pct < 30: score, status = 50, "partial"
        else: score, status = 0, "fail"
        
        rec = None
        if fail_pct >= 10:
            rec = Recommendation(
                action=f"{fail_pct:.0f}% of MLflow runs failed ({failed}/{total_runs}). Investigate failing experiments.",
                impact="High failure rates waste compute and delay model development.",
                priority="medium")
        
        return CheckResult("7.1.5", "MLflow experiment activity",
            "Model Lifecycle & Registry", score, status,
            f"{experiments} experiments, {total_runs:,} runs (30d), {fail_pct:.0f}% failure rate",
            "<10% run failure rate", details={"non_conforming": nc}, recommendation=rec)
