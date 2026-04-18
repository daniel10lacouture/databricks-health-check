"""Section: AI & ML Workloads — checks with full drill-downs for all statuses."""
from checks.base import BaseCheckRunner, CheckResult, Recommendation


class AIMLCheckRunner(BaseCheckRunner):
    section_id = "genai_ml"
    section_name = "Gen AI & ML"
    section_type = "conditional"
    icon = "brain"

    def get_subsections(self):
        return ["Model Lifecycle & Registry", "Model Serving", "AI Gateway", "GenAI Serving", "GenAI Adoption"]

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
    def check_7_2_5_foundation_model_usage(self) -> CheckResult:
        """Track Foundation Model API usage — GenAI workloads."""
        try:
            rows = self.executor.execute("""
                SELECT se.endpoint_name, se.served_entity_name,
                       se.foundation_model_config.min_provisioned_throughput AS min_pt,
                       se.foundation_model_config.max_provisioned_throughput AS max_pt,
                       COUNT(eu.client_request_id) AS requests_7d,
                       SUM(eu.input_token_count) AS input_tokens,
                       SUM(eu.output_token_count) AS output_tokens
                FROM system.serving.served_entities se
                LEFT JOIN system.serving.endpoint_usage eu
                    ON se.served_entity_id = eu.served_entity_id
                    AND eu.request_time >= DATEADD(DAY, -7, CURRENT_DATE())
                WHERE se.entity_type = 'FOUNDATION_MODEL'
                  AND se.endpoint_delete_time IS NULL
                GROUP BY 1, 2, 3, 4
                ORDER BY requests_7d DESC
            """)
        except Exception as e:
            return CheckResult("7.2.5", "Foundation Model API usage",
                "Model Serving", None, "info",
                f"Could not query: {str(e)[:60]}", "N/A")

        if not rows:
            return CheckResult("7.2.5", "Foundation Model API usage",
                "Model Serving", None, "info",
                "No Foundation Model endpoints configured", "N/A",
                recommendation=Recommendation(
                    action="Consider using Databricks Foundation Model APIs for GenAI workloads.",
                    impact="Foundation Model APIs provide access to state-of-the-art LLMs with pay-per-token pricing.",
                    priority="low",
                    docs_url="https://docs.databricks.com/en/machine-learning/foundation-models/index.html"))

        total_requests = sum(int(r.get("requests_7d", 0) or 0) for r in rows)
        total_input = sum(int(r.get("input_tokens", 0) or 0) for r in rows)
        total_output = sum(int(r.get("output_tokens", 0) or 0) for r in rows)

        nc = [{"endpoint": r.get("endpoint_name", ""),
               "model": r.get("served_entity_name", ""),
               "requests_7d": int(r.get("requests_7d", 0) or 0),
               "input_tokens": f"{int(r.get('input_tokens', 0) or 0):,}",
               "output_tokens": f"{int(r.get('output_tokens', 0) or 0):,}"} for r in rows[:15]]

        return CheckResult("7.2.5", "Foundation Model API usage (7d)",
            "Model Serving", None, "info",
            f"{len(rows)} FM endpoint(s), {total_requests:,} requests, {total_input + total_output:,} tokens",
            "Track GenAI usage",
            details={"foundation_model_endpoints": nc,
                     "summary": f"Processing {total_input:,} input + {total_output:,} output tokens this week"},
            recommendation=None)

    def check_7_2_6_genai_workload_types(self) -> CheckResult:
        """Breakdown of GenAI/serving workload types — informational."""
        try:
            rows = self.executor.execute("""
                SELECT entity_type, COUNT(DISTINCT endpoint_name) AS endpoints,
                       COUNT(*) AS served_entities
                FROM system.serving.served_entities
                WHERE endpoint_delete_time IS NULL
                GROUP BY 1 ORDER BY endpoints DESC
            """)
        except Exception as e:
            return CheckResult("7.2.6", "Serving workload types",
                "Model Serving", None, "info",
                f"Could not query: {str(e)[:60]}", "N/A")

        if not rows:
            return CheckResult("7.2.6", "Serving workload types",
                "Model Serving", None, "info",
                "No serving endpoints configured", "N/A")

        nc = [{"workload_type": r.get("entity_type", "UNKNOWN"),
               "endpoints": r.get("endpoints", 0),
               "served_entities": r.get("served_entities", 0)} for r in rows]

        total_endpoints = sum(r.get("endpoints", 0) for r in rows)
        fm_count = next((r["endpoints"] for r in rows if r.get("entity_type") == "FOUNDATION_MODEL"), 0)
        custom_count = next((r["endpoints"] for r in rows if r.get("entity_type") == "CUSTOM_MODEL"), 0)
        external_count = next((r["endpoints"] for r in rows if r.get("entity_type") == "EXTERNAL_MODEL"), 0)

        summary_parts = []
        if fm_count: summary_parts.append(f"{fm_count} Foundation Model")
        if custom_count: summary_parts.append(f"{custom_count} Custom Model")
        if external_count: summary_parts.append(f"{external_count} External Model")

        return CheckResult("7.2.6", "Serving workload types",
            "Model Serving", None, "info",
            f"{total_endpoints} endpoint(s): {', '.join(summary_parts) or 'N/A'}",
            "Track workload mix",
            details={"workload_breakdown": nc},
            recommendation=None)

    def check_7_2_7_ai_gateway_genai_routing(self) -> CheckResult:
        """Track which GenAI models are being routed through AI Gateway."""
        try:
            rows = self.executor.execute("""
                SELECT destination_model, destination_type,
                       COUNT(*) AS requests,
                       COUNT(DISTINCT requester) AS users,
                       SUM(input_tokens) AS input_tokens,
                       SUM(output_tokens) AS output_tokens,
                       ROUND(AVG(latency_ms)) AS avg_latency_ms,
                       SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS errors
                FROM system.ai_gateway.usage
                WHERE event_time >= DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY 1, 2 ORDER BY requests DESC
            """)
        except Exception as e:
            return CheckResult("7.2.7", "AI Gateway GenAI model routing",
                "AI Gateway", None, "info",
                f"Could not query: {str(e)[:60]}", "N/A")

        if not rows:
            return CheckResult("7.2.7", "AI Gateway GenAI model routing",
                "AI Gateway", None, "info",
                "No AI Gateway traffic in last 7 days", "N/A",
                recommendation=Recommendation(
                    action="Route LLM traffic through AI Gateway for centralized monitoring and cost control.",
                    impact="AI Gateway provides guardrails, rate limiting, and usage analytics for all model calls.",
                    priority="low",
                    docs_url="https://docs.databricks.com/en/ai-gateway/index.html"))

        total_requests = sum(int(r.get("requests", 0) or 0) for r in rows)
        total_tokens = sum(int(r.get("input_tokens", 0) or 0) + int(r.get("output_tokens", 0) or 0) for r in rows)
        total_errors = sum(int(r.get("errors", 0) or 0) for r in rows)
        error_rate = total_errors / max(total_requests, 1) * 100

        nc = [{"model": r.get("destination_model", "unknown"),
               "type": r.get("destination_type", ""),
               "requests": int(r.get("requests", 0) or 0),
               "users": int(r.get("users", 0) or 0),
               "tokens": f"{int(r.get('input_tokens', 0) or 0) + int(r.get('output_tokens', 0) or 0):,}",
               "avg_latency_ms": int(r.get("avg_latency_ms", 0) or 0),
               "error_rate": f"{int(r.get('errors', 0) or 0) / max(int(r.get('requests', 0) or 1), 1) * 100:.1f}%"} 
              for r in rows[:15]]

        rec = None
        if error_rate > 5:
            rec = Recommendation(
                action=f"AI Gateway error rate is {error_rate:.1f}%. Investigate failing model calls.",
                impact="High error rates may indicate quota issues, model unavailability, or malformed requests.",
                priority="medium",
                docs_url="https://docs.databricks.com/en/ai-gateway/index.html")

        return CheckResult("7.2.7", "AI Gateway model routing (7d)",
            "AI Gateway", None, "info",
            f"{len(rows)} model(s), {total_requests:,} requests, {total_tokens:,} tokens, {error_rate:.1f}% errors",
            "Track GenAI routing",
            details={"model_routing": nc,
                     "summary": f"AI Gateway processed {total_requests:,} requests across {len(rows)} models"},
            recommendation=rec)
    # ── GenAI Serving & Adoption ─────────────────────────────────

    def check_7_3_1_endpoint_inventory(self) -> CheckResult:
        """Model serving endpoint inventory by type."""
        try:
            rows = self.executor.execute("""
                SELECT entity_type, COUNT(DISTINCT endpoint_name) AS endpoints,
                       COUNT(*) AS served_entities
                FROM system.serving.served_entities
                GROUP BY entity_type""")
        except Exception as e:
            return CheckResult("7.3.1", "Serving endpoint inventory", "GenAI Serving",
                None, "info", f"Could not query: {str(e)[:80]}", "Track endpoints")
        total_endpoints = sum(int(r.get("endpoints", 0) or 0) for r in rows)
        nc = [{"type": r.get("entity_type", ""), "endpoints": r.get("endpoints", 0),
               "entities": r.get("served_entities", 0)} for r in rows]
        score = 100 if total_endpoints > 0 else 0
        return CheckResult("7.3.1", "Serving endpoint inventory", "GenAI Serving",
            score, "pass" if score == 100 else "fail",
            f"{total_endpoints} endpoints across {len(rows)} types", "Active model serving",
            details={"non_conforming": nc, "total": total_endpoints})

    def check_7_3_2_endpoint_error_rate(self) -> CheckResult:
        """Error rate across model serving endpoints (4xx/5xx)."""
        try:
            rows = self.executor.execute("""
                SELECT se.endpoint_name,
                       COUNT(*) AS total_requests,
                       SUM(CASE WHEN CAST(eu.status_code AS INT) >= 400 THEN 1 ELSE 0 END) AS errors,
                       SUM(CASE WHEN eu.status_code = '429' THEN 1 ELSE 0 END) AS rate_limited
                FROM system.serving.endpoint_usage eu
                JOIN system.serving.served_entities se ON eu.served_entity_id = se.served_entity_id
                WHERE eu.request_time >= DATEADD(DAY, -7, CURRENT_DATE())
                GROUP BY se.endpoint_name
                HAVING COUNT(*) >= 100
                ORDER BY errors DESC LIMIT 15""")
        except Exception:
            return CheckResult("7.3.2", "Endpoint error rate (7d)", "GenAI Serving",
                None, "info", "Could not query endpoint usage", "<5% error rate")
        if not rows:
            return CheckResult("7.3.2", "Endpoint error rate (7d)", "GenAI Serving",
                100, "pass", "No endpoints with significant traffic", "<5% error rate")
        total_req = sum(int(r.get("total_requests", 0) or 0) for r in rows) or 1
        total_err = sum(int(r.get("errors", 0) or 0) for r in rows)
        total_rl = sum(int(r.get("rate_limited", 0) or 0) for r in rows)
        err_pct = round(total_err / total_req * 100, 2)
        score = 100 if err_pct < 1 else 50 if err_pct < 5 else 0
        nc = [{"endpoint": r.get("endpoint_name", ""), "requests": r.get("total_requests", 0),
               "errors": r.get("errors", 0), "rate_limited": r.get("rate_limited", 0)}
              for r in rows if int(r.get("errors", 0) or 0) > 0]
        rec = Recommendation(
            action=f"{err_pct}% error rate ({total_err:,} errors, {total_rl:,} rate-limited). Review capacity and retry logic.",
            impact="High error rates degrade user experience and waste tokens on retries.",
            priority="high" if err_pct > 5 else "medium",
            docs_url="https://docs.databricks.com/en/machine-learning/model-serving/index.html") if score < 100 else None
        return CheckResult("7.3.2", "Endpoint error rate (7d)", "GenAI Serving",
            score, "pass" if score == 100 else "partial" if score == 50 else "fail",
            f"{err_pct}% error rate ({total_rl:,} rate-limited)", "<5% error rate",
            details={"non_conforming": nc}, recommendation=rec)

    def check_7_3_3_token_throughput(self) -> CheckResult:
        """Token throughput and cost efficiency across endpoints."""
        try:
            rows = self.executor.execute("""
                SELECT se.endpoint_name, se.entity_type,
                       COUNT(*) AS requests,
                       SUM(CAST(eu.input_token_count AS BIGINT)) AS input_tokens,
                       SUM(CAST(eu.output_token_count AS BIGINT)) AS output_tokens,
                       COUNT(DISTINCT eu.requester) AS unique_users
                FROM system.serving.endpoint_usage eu
                JOIN system.serving.served_entities se ON eu.served_entity_id = se.served_entity_id
                WHERE eu.request_time >= DATEADD(DAY, -30, CURRENT_DATE())
                GROUP BY se.endpoint_name, se.entity_type
                ORDER BY input_tokens DESC LIMIT 10""")
        except Exception:
            return CheckResult("7.3.3", "Token throughput (30d)", "GenAI Serving",
                None, "info", "Could not query token metrics", "Monitor token usage")
        total_in = sum(int(r.get("input_tokens", 0) or 0) for r in rows)
        total_out = sum(int(r.get("output_tokens", 0) or 0) for r in rows)
        total_users = max(int(r.get("unique_users", 0) or 0) for r in rows) if rows else 0
        nc = [{"endpoint": r.get("endpoint_name", ""), "type": r.get("entity_type", ""),
               "requests": f"{int(r.get('requests', 0) or 0):,}",
               "input_tokens": f"{int(r.get('input_tokens', 0) or 0):,}",
               "output_tokens": f"{int(r.get('output_tokens', 0) or 0):,}",
               "users": r.get("unique_users", 0)} for r in rows]
        return CheckResult("7.3.3", "Token throughput (30d)", "GenAI Serving",
            None, "info",
            f"{total_in/1e9:.1f}B input tokens, {total_out/1e6:.0f}M output tokens",
            "Monitor token usage", details={"non_conforming": nc, "total_input": total_in, "total_output": total_out})

    def check_7_3_4_agentbricks_usage(self) -> CheckResult:
        """AgentBricks adoption — Knowledge Assistants and Supervisor Agents."""
        try:
            rows = self.executor.execute("""
                SELECT product_features.agent_bricks AS agent_config,
                       COUNT(*) AS records, ROUND(SUM(usage_quantity), 1) AS total_dbu
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND product_features.agent_bricks IS NOT NULL
                GROUP BY 1""")
        except Exception:
            return CheckResult("7.3.4", "AgentBricks usage (30d)", "GenAI Serving",
                None, "info", "Could not query AgentBricks usage", "Track agent adoption")
        total_dbu = sum(float(r.get("total_dbu", 0) or 0) for r in rows)
        if not rows or total_dbu == 0:
            return CheckResult("7.3.4", "AgentBricks usage (30d)", "GenAI Serving",
                None, "info", "No AgentBricks usage detected", "Explore Databricks Agents",
                recommendation=Recommendation(
                    action="No AgentBricks usage detected. Explore building AI agents with Databricks Agent Framework.",
                    impact="Agents automate complex workflows and enable conversational data access.",
                    priority="low",
                    docs_url="https://docs.databricks.com/en/generative-ai/agent-framework/index.html"))
        nc = [{"config": r.get("agent_config", ""), "dbu": r.get("total_dbu", 0)} for r in rows]
        return CheckResult("7.3.4", "AgentBricks usage (30d)", "GenAI Serving",
            100, "pass", f"{total_dbu:,.0f} DBUs across {len(rows)} agent type(s)",
            "Active agent adoption", details={"non_conforming": nc, "total_dbu": total_dbu})

    def check_7_3_5_ai_sql_function_adoption(self) -> CheckResult:
        """Usage of AI SQL functions (ai_query, ai_classify, ai_extract, ai_forecast, ai_parse)."""
        try:
            rows = self.executor.execute("""
                SELECT 
                    SUM(CASE WHEN statement_text LIKE '%ai_query%' THEN 1 ELSE 0 END) AS ai_query_cnt,
                    SUM(CASE WHEN statement_text LIKE '%ai_classify%' THEN 1 ELSE 0 END) AS ai_classify_cnt,
                    SUM(CASE WHEN statement_text LIKE '%ai_extract%' THEN 1 ELSE 0 END) AS ai_extract_cnt,
                    SUM(CASE WHEN statement_text LIKE '%ai_forecast%' THEN 1 ELSE 0 END) AS ai_forecast_cnt,
                    SUM(CASE WHEN statement_text LIKE '%ai_generate%' THEN 1 ELSE 0 END) AS ai_generate_cnt,
                    SUM(CASE WHEN statement_text LIKE '%ai_similarity%' THEN 1 ELSE 0 END) AS ai_similarity_cnt,
                    SUM(CASE WHEN statement_text LIKE '%ai_parse%' THEN 1 ELSE 0 END) AS ai_parse_cnt,
                    COUNT(DISTINCT executed_by) AS unique_users
                FROM system.query.history
                WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND (statement_text LIKE '%ai_query%' OR statement_text LIKE '%ai_classify%'
                       OR statement_text LIKE '%ai_extract%' OR statement_text LIKE '%ai_forecast%'
                       OR statement_text LIKE '%ai_generate%' OR statement_text LIKE '%ai_similarity%'
                       OR statement_text LIKE '%ai_parse%')""")
        except Exception:
            return CheckResult("7.3.5", "AI SQL function adoption (30d)", "GenAI Adoption",
                None, "info", "Could not query AI function usage", "Track AI function adoption")
        r = rows[0] if rows else {}
        users = int(r.get("unique_users", 0) or 0)
        funcs = {k.replace("_cnt", ""): int(r.get(k, 0) or 0) for k in r if k.endswith("_cnt")}
        total = sum(funcs.values())
        nc = [{"function": k, "calls": v} for k, v in sorted(funcs.items(), key=lambda x: -x[1]) if v > 0]
        score = 100 if total > 100 else 50 if total > 0 else 0
        return CheckResult("7.3.5", "AI SQL function adoption (30d)", "GenAI Adoption",
            score, "pass" if score == 100 else "partial" if score == 50 else "fail",
            f"{total:,} AI function calls by {users} users", "Active AI SQL function usage",
            details={"non_conforming": nc, "total": total, "users": users})

    def check_7_3_6_genai_cost_breakdown(self) -> CheckResult:
        """GenAI cost breakdown by provider (Anthropic, OpenAI, Gemini, OSS)."""
        try:
            rows = self.executor.execute("""
                SELECT
                    CASE
                        WHEN sku_name LIKE '%ANTHROPIC%' THEN 'Anthropic'
                        WHEN sku_name LIKE '%OPENAI%' OR sku_name LIKE '%GPT%' THEN 'OpenAI'
                        WHEN sku_name LIKE '%GEMINI%' THEN 'Google Gemini'
                        WHEN sku_name LIKE '%MODEL_TRAINING%' THEN 'Model Training'
                        ELSE 'Other Serving'
                    END AS provider,
                    ROUND(SUM(usage_quantity), 0) AS total_dbu,
                    COUNT(DISTINCT workspace_id) AS workspaces
                FROM system.billing.usage
                WHERE usage_date >= DATEADD(DAY, -30, CURRENT_DATE())
                  AND (sku_name LIKE '%SERVING%' OR sku_name LIKE '%MODEL%' OR sku_name LIKE '%ANTHROPIC%'
                       OR sku_name LIKE '%OPENAI%' OR sku_name LIKE '%GEMINI%' OR sku_name LIKE '%GPT%')
                GROUP BY 1 ORDER BY total_dbu DESC""")
        except Exception:
            return CheckResult("7.3.6", "GenAI cost breakdown (30d)", "GenAI Adoption",
                None, "info", "Could not query GenAI billing", "Monitor GenAI costs")
        total_dbu = sum(float(r.get("total_dbu", 0) or 0) for r in rows)
        nc = [{"provider": r.get("provider", ""), "dbu": r.get("total_dbu", 0),
               "workspaces": r.get("workspaces", 0)} for r in rows]
        return CheckResult("7.3.6", "GenAI cost breakdown (30d)", "GenAI Adoption",
            None, "info", f"{total_dbu:,.0f} DBUs on GenAI serving/training",
            "Monitor GenAI costs", details={"non_conforming": nc, "total_dbu": total_dbu})

    def check_7_3_7_fm_vs_custom_ratio(self) -> CheckResult:
        """Foundation Model API vs custom-deployed model ratio."""
        try:
            rows = self.executor.execute("""
                SELECT entity_type, COUNT(DISTINCT endpoint_name) AS endpoints
                FROM system.serving.served_entities
                GROUP BY entity_type""")
        except Exception:
            return CheckResult("7.3.7", "Foundation vs Custom model ratio", "GenAI Adoption",
                None, "info", "Could not query model types", "Track model deployment strategy")
        fm = sum(int(r.get("endpoints", 0) or 0) for r in rows if r.get("entity_type") == "FOUNDATION_MODEL")
        custom = sum(int(r.get("endpoints", 0) or 0) for r in rows if r.get("entity_type") == "CUSTOM_MODEL")
        ext = sum(int(r.get("endpoints", 0) or 0) for r in rows if r.get("entity_type") == "EXTERNAL_MODEL")
        total = fm + custom + ext or 1
        nc = [{"type": r.get("entity_type", ""), "endpoints": r.get("endpoints", 0)} for r in rows]
        return CheckResult("7.3.7", "Foundation vs Custom model ratio", "GenAI Adoption",
            None, "info", f"{fm} Foundation, {custom} Custom, {ext} External endpoints",
            "Balanced model deployment", details={"non_conforming": nc})

