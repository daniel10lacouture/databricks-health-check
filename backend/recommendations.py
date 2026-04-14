"""
Recommendation engine: aggregates and prioritizes recommendations across sections.
"""
from __future__ import annotations


PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}


def get_top_recommendations(section_results: list[dict], limit: int = 10) -> list[dict]:
    """Extract top N recommendations across all sections, sorted by priority and score gap."""
    recs = []
    for section in section_results:
        if not section.get("active"):
            continue
        for check in section.get("checks", []):
            rec = check.get("recommendation")
            if rec and check.get("status") in ("fail", "partial"):
                recs.append({
                    "section_id": section["section_id"],
                    "section_name": section["section_name"],
                    "check_id": check["check_id"],
                    "check_name": check["name"],
                    "score": check["score"],
                    "current_value": check["current_value"],
                    "target_value": check["target_value"],
                    **rec,
                })

    # Sort by priority (high first), then by score (0 before 50)
    recs.sort(key=lambda r: (PRIORITY_ORDER.get(r.get("priority", "medium"), 1), r.get("score", 50)))
    return recs[:limit]


def estimate_impact(check_id: str, current_value: str, details: dict) -> str:
    """Generate a human-readable impact estimate based on check findings."""
    # This can be expanded with more sophisticated logic per check
    return "Improves account health score and aligns with Databricks best practices."
