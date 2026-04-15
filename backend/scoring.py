"""
Scoring engine: computes section scores and overall account health score.
"""
from __future__ import annotations
from typing import Optional


def score_label(score: float) -> str:
    if score >= 90:
        return "Excellent"
    elif score >= 70:
        return "Good"
    elif score >= 50:
        return "Needs Attention"
    else:
        return "Critical"


def score_color(score: float) -> str:
    if score >= 90:
        return "#059669"
    elif score >= 70:
        return "#2563EB"
    elif score >= 50:
        return "#D97706"
    else:
        return "#DC2626"


def compute_overall_score(section_results: list[dict]) -> dict:
    """
    Compute overall score as weighted average of active section scores.
    Only active, non-advisory sections contribute to the score.
    All active sections carry equal weight.
    """
    active_sections = [
        s for s in section_results
        if s.get("active") and s.get("score") is not None and s.get("section_type") != "advisory"
    ]

    if not active_sections:
        return {
            "overall_score": None,
            "label": "No Data",
            "color": "#6B7280",
            "active_sections": 0,
            "total_sections": len(section_results),
        }

    overall = round(sum(s["score"] for s in active_sections) / len(active_sections), 1)

    if overall >= 90: maturity_level, maturity_label = 5, "Elite"
    elif overall >= 75: maturity_level, maturity_label = 4, "Advanced"
    elif overall >= 60: maturity_level, maturity_label = 3, "Optimized"
    elif overall >= 40: maturity_level, maturity_label = 2, "Managed"
    else: maturity_level, maturity_label = 1, "Foundational"

    return {
        "overall_score": overall,
        "label": score_label(overall),
        "color": score_color(overall),
        "active_sections": len(active_sections),
        "total_sections": len(section_results),
        "maturity_level": maturity_level,
        "maturity_label": maturity_label,
    }


def compute_section_score(check_results: list[dict]) -> Optional[float]:
    scored = [c for c in check_results if c.get("status") not in ("not_evaluated", "info")]
    if not scored:
        return None
    return round(sum(c["score"] for c in scored) / len(scored), 1)
