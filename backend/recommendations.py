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
                    "dollar_impact": _personalized_impact(check["check_id"], check.get("current_value", "")),
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


import re

# Fraction of an observed spend figure we estimate is recoverable per check family.
# Cost checks (prefix "3"/"4") tend to have higher recoverable waste than others.
_RECOVERY_RATE = {"3": 0.30, "4": 0.25, "2": 0.15, "1": 0.10}


def _personalized_impact(check_id: str, current_value: str) -> str:
    """
    If the check's measured value contains a real dollar figure, estimate a
    personalized monthly savings from it. Otherwise fall back to the static
    range. Always returns a string, so the UI renders exactly as before.
    """
    static = _estimate_dollar_impact(check_id)
    try:
        cv = str(current_value or "")
        # Match figures like $12,345 or $1.2K/$3M, optionally with a magnitude suffix.
        m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)\s*([KMB])?", cv, re.IGNORECASE)
        if not m:
            return static
        amount = float(m.group(1).replace(",", ""))
        suffix = (m.group(2) or "").upper()
        amount *= {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
        rate = _RECOVERY_RATE.get(check_id[:1], 0.10)
        est = amount * rate
        if est < 100:
            return static
        return f"~${est:,.0f}/mo potential savings"
    except Exception:
        return static

