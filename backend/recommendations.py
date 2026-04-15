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
                    "dollar_impact": _estimate_dollar_impact(check["check_id"]),
                    **rec,
                })

    # Sort by priority (high first), then by score (0 before 50)
    recs.sort(key=lambda r: (PRIORITY_ORDER.get(r.get("priority", "medium"), 1), r.get("score", 50)))
    return recs[:limit]


def estimate_impact(check_id: str, current_value: str, details: dict) -> str:
    """Generate a human-readable impact estimate based on check findings."""
    return _estimate_dollar_impact(check_id)


IMPACT_ESTIMATES = {
    "3.1": "$500-$5,000/mo", "3.2": "$1,000-$10,000/mo", "3.3": "$2,000-$15,000/mo",
    "3.5": "$1,000-$8,000/mo", "3.6": "$1,000-$10,000/mo", "4.1": "$2,000-$20,000/mo",
    "4.2": "$500-$10,000/mo", "4.3": "$1,000-$5,000/mo", "4.4": "$2,000-$15,000/mo",
    "2.1": "$500-$5,000/mo", "2.2": "$1,000-$8,000/mo", "2.3": "$500-$3,000/mo",
    "2.6": "$200-$2,000/mo", "2.7": "$500-$5,000/mo", "1.2": "$500-$5,000/mo",
    "1.3": "$1,000-$10,000/mo", "1.5": "$500-$5,000/mo", "1.8": "$1,000-$8,000/mo",
    "5.1": "Risk mitigation", "5.2": "Risk mitigation", "5.4": "Compliance risk mitigation",
    "5.5": "Risk mitigation", "5.6": "Compliance risk mitigation",
}

def _estimate_dollar_impact(check_id: str) -> str:
    prefix = check_id[:3] if len(check_id) >= 3 else check_id
    return IMPACT_ESTIMATES.get(prefix, "Improves operational efficiency")

