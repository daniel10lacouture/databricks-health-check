"""
Pillar 3: GenAI-powered insights using Databricks Foundation Model API.
Generates executive summary, action plan, and cross-section correlations.
"""
from __future__ import annotations
import json
import logging
import time
import requests
from typing import Optional

logger = logging.getLogger("health_check.genai")


class GenAIInsights:
    def __init__(self, host: str, token: str, model: str = "databricks-meta-llama-3-3-70b-instruct"):
        self.host = host.rstrip("/")
        self.token = token
        self.model = model
        self.endpoint = f"{self.host}/serving-endpoints/{self.model}/invocations"

    def generate(self, results: dict, insights: dict) -> dict:
        t0 = time.time()
        try:
            prompt = self._build_prompt(results, insights)
            raw = self._call_model(prompt)
            parsed = self._parse_response(raw)
            elapsed = round(time.time() - t0, 1)
            logger.info(f"GenAI insights generated in {elapsed}s")
            parsed["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            parsed["model"] = self.model
            return parsed
        except Exception as e:
            logger.warning(f"GenAI insights failed (non-blocking): {e}")
            return {"error": str(e), "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    def _build_prompt(self, results: dict, insights: dict) -> str:
        overall = results.get("overall", {})
        sections = results.get("sections", [])
        top_recs = results.get("top_recommendations", [])
        maturity = insights.get("maturity", {})
        anomalies = insights.get("anomalies", [])
        whatif = insights.get("whatif_scenarios", [])

        section_summary = []
        for sec in sections:
            if not sec.get("active"): continue
            failing = [c["name"] for c in sec.get("checks", []) if c.get("status") in ("fail", "partial")]
            section_summary.append(f"- {sec['section_name']}: Score {sec.get('score', 'N/A')}, {len(failing)} issues" + (f" ({', '.join(failing[:3])})" if failing else ""))

        rec_summary = [f"- [{r.get('priority','medium').upper()}] {r.get('check_name','')}: {r.get('action','')[:120]}" for r in top_recs[:8]]

        anomaly_text = ""
        if anomalies:
            anomaly_text = "\nANOMALIES DETECTED:\n" + "\n".join(f"- {a['title']}: {a['message']}" for a in anomalies)

        whatif_text = ""
        if whatif:
            whatif_text = "\nWHAT-IF SCENARIOS:\n" + "\n".join(f"- {s['title']}: {s['description']} (Savings: {s['estimated_savings']})" for s in whatif)

        return f"""You are a Databricks solutions architect analyzing an account health check.

ACCOUNT HEALTH CHECK RESULTS:
Overall Score: {overall.get('overall_score', 'N/A')} ({overall.get('label', '')})
Maturity Level: {maturity.get('level', 'N/A')} - {maturity.get('label', '')}
Active Sections: {overall.get('active_sections', 0)}/{overall.get('total_sections', 0)}

SECTION SCORES:
{chr(10).join(section_summary)}

TOP RECOMMENDATIONS:
{chr(10).join(rec_summary)}
{anomaly_text}
{whatif_text}

Respond with valid JSON only (no markdown, no code fences):
{{
  "executive_summary": "2-3 paragraph narrative connecting the dots across sections. Be specific with numbers.",
  "action_plan": [
    {{"priority": 1, "action": "specific action", "impact": "expected outcome", "effort": "low/medium/high", "dependencies": "what needs to happen first"}},
    ...up to 5 items
  ],
  "cross_section_insights": [
    {{"sections": ["section1", "section2"], "insight": "how findings are connected"}},
    ...up to 3 correlations
  ],
  "whatif_narrative": "2-3 sentence projection of implementing top 3 recommendations"
}}"""

    def _call_model(self, prompt: str) -> str:
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"messages": [{"role": "user", "content": prompt}], "max_tokens": 2000, "temperature": 0.3}
        resp = requests.post(self.endpoint, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")

    def _parse_response(self, raw: str) -> dict:
        text = raw.strip()
        if text.startswith("```"): text = text.split("\n", 1)[-1]
        if text.endswith("```"): text = text.rsplit("\n", 1)[0]
        if text.startswith("```json"): text = text[7:]
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                try: return json.loads(text[start:end])
                except json.JSONDecodeError: pass
            return {"executive_summary": text[:1000] if text else "AI analysis unavailable.", "action_plan": [], "cross_section_insights": [], "whatif_narrative": ""}
