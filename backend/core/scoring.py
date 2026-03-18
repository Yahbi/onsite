"""
Onsite Lead Scoring Engine v3.0
Rebalanced for real-world permit data distributions.

Key changes from v2.0:
- Removed Project Value category (96.5% of leads have $0 valuation)
- Removed Owner Type category (folded into contact scoring)
- Added Project Indicators from work_description keywords
- Added Property Data bonus (market_value, square_feet)
- Implemented Enrichment Status bonus
- Rebalanced weights to reflect actual data availability

Scoring Factors:
- Recency (0-35 points): How recent the permit was filed
- Permit Type (0-20 points): High-value vs low-value work types
- Geographic Quality (0-15 points): Location completeness + market
- Project Indicators (0-15 points): Description keyword analysis
- Contact Availability (0-10 points): Owner info completeness
- Property Data Bonus (0-5 points): market_value / square_feet present
- Enrichment Status (0-5 points): Data completeness bonus
- Insurance Signals (0-20 points): Fire/water/storm damage indicators (bonus)
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Canonical temperature thresholds (single source of truth) ──
# Recalibrated for v3.0 scoring distribution.
# Hot ≥30: Recent + strong signals → act now
# Warm ≥25: Above-average signals → pursue
# Med ≥20: Standard permits
# Cold <20: Minimal signals
TEMP_THRESHOLDS = {"hot": 30, "warm": 22, "med": 15, "cold": 0}


# ── Project indicator keyword tiers ──
PROJECT_TIERS = (
    (15, ("new construction", "commercial")),
    (12, ("addition", "remodel", "renovation")),
    (10, ("solar", "roof replacement")),
    (8,  ("hvac", "plumbing", "electrical")),
    (3,  ("fence", "shed", "deck")),
)


def classify_temperature(score: int) -> str:
    """Classify a score into a 4-tier temperature label (lowercase).
    Hot >=30, Warm >=25, Med >=20, Cold <20.
    """
    if score >= TEMP_THRESHOLDS["hot"]:
        return "hot"
    elif score >= TEMP_THRESHOLDS["warm"]:
        return "warm"
    elif score >= TEMP_THRESHOLDS["med"]:
        return "med"
    return "cold"


def _safe_int(value, default: int = 0) -> int:
    """Safely convert a value to int."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _extract_lead_fields(lead: dict) -> dict:
    """Extract and normalize all fields from a lead dict.
    Returns a new dict with cleaned values (immutable pattern).
    """
    return {
        "days_old": _safe_int(lead.get("days_old"), 999),
        "valuation": _safe_float(lead.get("valuation")),
        "permit_type": (lead.get("permit_type") or "").lower(),
        "work_desc": (lead.get("work_description") or "").lower(),
        "desc_full": (lead.get("description_full") or "").lower(),
        "owner_name": lead.get("owner_name") or "",
        "owner_phone": lead.get("owner_phone") or "",
        "owner_email": lead.get("owner_email") or "",
        "city": (lead.get("city") or "").lower(),
        "state": (lead.get("state") or "").upper(),
        "zip_code": lead.get("zip_code") or lead.get("zip") or "",
        "market_value": _safe_float(lead.get("market_value")),
        "square_feet": _safe_float(lead.get("square_feet")),
        "enrichment_status": (lead.get("enrichment_status") or "").lower(),
    }


def _score_recency(days_old: int) -> Tuple[int, str]:
    """Score recency (0-35 points). Most reliable signal."""
    tiers = (
        (1, 35), (3, 32), (5, 28), (7, 24), (10, 19),
        (14, 14), (21, 10), (30, 6), (45, 3), (60, 2), (90, 1),
    )
    for threshold, pts in tiers:
        if days_old <= threshold:
            detail = f"{days_old} day{'s' if days_old != 1 else ''} old"
            return pts, detail
    return 0, f"{days_old} days old"


def _score_permit_type(permit_type: str, work_desc: str) -> Tuple[int, str]:
    """Score permit type (0-20 points). Expanded keyword matching."""
    combined = f"{permit_type} {work_desc}"

    ultra_high = [
        "new construction", "new building", "new single family",
        "new commercial", "new multi", "ground up",
    ]
    high_value = [
        "addition", "remodel", "renovation", "tenant improvement",
        "solar", "adu", "accessory dwelling", "second story",
        "conversion", "seismic retrofit", "foundation",
    ]
    medium_value = [
        "alteration", "repair", "electrical", "plumbing", "mechanical",
        "hvac", "roof", "reroof", "window", "siding", "insulation",
        "fire sprinkler", "elevator",
    ]
    low_value = [
        "demolition", "sign", "fence", "grading", "pool", "shed",
        "deck", "patio", "driveway", "retaining wall",
    ]

    if any(t in combined for t in ultra_high):
        return 20, "New Construction"
    if any(t in combined for t in high_value):
        return 16, "Major Renovation"
    if any(t in combined for t in medium_value):
        return 10, "Standard Repair"
    if any(t in combined for t in low_value):
        return 4, "Minor Work"
    # Fallback: any permit_type text gets partial credit
    if permit_type.strip():
        return 7, permit_type[:30]
    return 5, "Unspecified"


def _score_geography(city: str, state: str, zip_code: str) -> Tuple[int, str]:
    """Score geographic quality (0-15 points). Rewards completeness."""
    premium_cities = frozenset([
        "los angeles", "san francisco", "san jose", "seattle", "portland",
        "san diego", "denver", "austin", "miami", "atlanta", "boston",
        "new york", "chicago", "washington", "houston", "dallas",
        "phoenix", "nashville", "charlotte", "raleigh", "tampa",
    ])
    strong_states = frozenset([
        "CA", "NY", "WA", "TX", "FL", "CO", "MA", "GA", "NC", "AZ",
        "TN", "OR", "VA", "IL", "NJ",
    ])

    score = 0
    detail_parts = []

    # Completeness bonus: city + state + zip = full location
    if city:
        score += 2
    if state:
        score += 2
    if zip_code:
        score += 2

    # Market quality bonus
    if any(city.startswith(metro) or metro in city for metro in premium_cities):
        score += 9
        detail_parts.append(f"{city.title()} (Premium market)")
    elif state in strong_states:
        score += 6
        detail_parts.append(f"{state} (Strong market)")
    elif state:
        score += 3
        detail_parts.append(state)
    else:
        detail_parts.append("Unknown location")

    return min(15, score), ", ".join(detail_parts)


def _score_project_indicators(work_desc: str, desc_full: str) -> Tuple[int, str]:
    """Score project indicators from description keywords (0-15 points)."""
    combined = f"{work_desc} {desc_full}"
    if not combined.strip():
        return 0, "No description"

    best_score = 0
    matched_keywords = []

    for pts, keywords in PROJECT_TIERS:
        for kw in keywords:
            if kw in combined:
                matched_keywords.append(kw)
                if pts > best_score:
                    best_score = pts

    if not matched_keywords:
        return 0, "No project indicators"

    return min(15, best_score), f"Matched: {', '.join(matched_keywords[:3])}"


def _score_contact(owner_name: str, owner_phone: str, owner_email: str) -> Tuple[int, str]:
    """Score contact availability (0-10 points)."""
    score = 0
    details = []

    if owner_phone:
        score += 5
        details.append("Phone")
    if owner_email:
        score += 3
        details.append("Email")
    if owner_name and owner_name.strip():
        score += 2
        details.append("Name")

    return min(10, score), ", ".join(details) if details else "No contact info"


def _score_property_data(market_value: float, square_feet: float) -> Tuple[int, str]:
    """Score property data bonus (0-5 points)."""
    score = 0
    details = []

    if market_value > 0:
        score += 3
        details.append(f"Value: ${market_value:,.0f}")
    if square_feet > 0:
        score += 2
        details.append(f"SqFt: {square_feet:,.0f}")

    return score, ", ".join(details) if details else "No property data"


def _score_enrichment(enrichment_status: str) -> Tuple[int, str]:
    """Score enrichment status bonus (0-5 points)."""
    status_scores = {
        "fully_enriched": 5,
        "enriched": 5,
        "partially_enriched": 3,
        "partial": 3,
        "basic": 1,
        "pending": 0,
        "failed": 0,
    }
    score = status_scores.get(enrichment_status, 0)
    detail = enrichment_status if enrichment_status else "Not enriched"
    return score, detail


def _score_insurance(permit_type: str, work_desc: str, desc_full: str) -> Tuple[int, str]:
    """Score insurance/emergency signals (bonus: 0-20 points)."""
    insurance_keywords = [
        "fire", "water damage", "flood", "storm", "wind damage",
        "insurance", "restoration", "remediation", "mold", "smoke",
        "emergency", "casualty", "disaster", "hail", "vandalism",
    ]
    combined = f"{permit_type} {work_desc} {desc_full}"
    detected = [kw for kw in insurance_keywords if kw in combined]

    if not detected:
        return 0, "None detected"
    return min(20, len(detected) * 5), f"Detected: {', '.join(detected[:3])}"


def calculate_score_v2(lead: dict) -> Tuple[int, str, dict]:
    """
    Enhanced multi-factor lead scoring algorithm (v3.0).
    Returns (score, temperature, breakdown_dict).

    Rebalanced for real permit data where most leads lack
    valuation, phone, and email data.

    Scoring breakdown:
    - Recency: 0-35 points
    - Permit Type: 0-20 points
    - Geographic Quality: 0-15 points
    - Project Indicators: 0-15 points
    - Contact Info: 0-10 points
    - Property Data: 0-5 points
    - Enrichment Status: 0-5 points
    - Insurance Signals: 0-20 points (bonus, can exceed 100)
    """
    fields = _extract_lead_fields(lead)

    recency_pts, recency_detail = _score_recency(fields["days_old"])
    permit_pts, permit_detail = _score_permit_type(
        fields["permit_type"], fields["work_desc"],
    )
    geo_pts, geo_detail = _score_geography(
        fields["city"], fields["state"], fields["zip_code"],
    )
    project_pts, project_detail = _score_project_indicators(
        fields["work_desc"], fields["desc_full"],
    )
    contact_pts, contact_detail = _score_contact(
        fields["owner_name"], fields["owner_phone"], fields["owner_email"],
    )
    property_pts, property_detail = _score_property_data(
        fields["market_value"], fields["square_feet"],
    )
    enrichment_pts, enrichment_detail = _score_enrichment(
        fields["enrichment_status"],
    )
    insurance_pts, insurance_detail = _score_insurance(
        fields["permit_type"], fields["work_desc"], fields["desc_full"],
    )

    total = (
        recency_pts + permit_pts + geo_pts + project_pts
        + contact_pts + property_pts + enrichment_pts + insurance_pts
    )
    total = min(100, total)

    breakdown = {
        "recency": {"score": recency_pts, "max": 35, "detail": recency_detail},
        "permit_type": {"score": permit_pts, "max": 20, "detail": permit_detail},
        "geography": {"score": geo_pts, "max": 15, "detail": geo_detail},
        "project_indicators": {"score": project_pts, "max": 15, "detail": project_detail},
        "contact": {"score": contact_pts, "max": 10, "detail": contact_detail},
        "property_data": {"score": property_pts, "max": 5, "detail": property_detail},
        "enrichment": {"score": enrichment_pts, "max": 5, "detail": enrichment_detail},
        "insurance": {"score": insurance_pts, "max": 20, "detail": insurance_detail},
        "total": total,
    }

    temperature = classify_temperature(total)
    return total, temperature, breakdown


def calculate_score(days_old: int, valuation: float, permit_type: str) -> Tuple[int, str, str]:
    """
    Convenience wrapper for calculate_score_v2.
    Returns (score, temperature, urgency) — same 3-tuple that callers expect.
    """
    lead = {
        "days_old": days_old,
        "valuation": valuation,
        "permit_type": permit_type,
    }
    score, temp, _ = calculate_score_v2(lead)

    if days_old <= 3 and score >= 80:
        urgency = "CRITICAL"
    elif days_old <= 7 and score >= 70:
        urgency = "HIGH"
    elif days_old <= 14 or score >= 65:
        urgency = "MEDIUM"
    else:
        urgency = "LOW"

    return score, temp, urgency


def compute_readiness(lead: dict) -> dict:
    """
    Compute readiness_score and action hints for a lead.
    Returns dict with readiness_score, recommended_action, urgency_level,
    contact_window_days, budget_range, competition_level.
    """
    score = 0
    signals = []

    days_old = _safe_int(lead.get("days_old"), 999)
    valuation = _safe_float(lead.get("valuation"))
    permit_type = (lead.get("permit_type") or "").lower()
    owner_name = lead.get("owner_name", "")
    owner_phone = lead.get("owner_phone", "")
    owner_email = lead.get("owner_email", "")

    # Timing signals
    if days_old <= 3:
        score += 25
        signals.append("Filed in last 3 days — act fast")
    elif days_old <= 7:
        score += 18
        signals.append("Filed this week — good timing")
    elif days_old <= 14:
        score += 10
        signals.append("Filed within 2 weeks")

    # Value signals
    if valuation >= 250000:
        score += 20
        signals.append(f"High value project (${valuation:,.0f})")
    elif valuation >= 100000:
        score += 12
        signals.append(f"Medium-high value (${valuation:,.0f})")
    elif valuation >= 50000:
        score += 8

    # Contact availability
    if owner_phone or owner_email:
        score += 15
        signals.append("Direct contact available")
    elif owner_name:
        score += 5
        signals.append("Owner identified — needs lookup")

    # Permit type signals
    if any(t in permit_type for t in ["new construction", "addition", "remodel"]):
        score += 15
        signals.append("High-intent permit type")
    elif any(t in permit_type for t in ["fire", "water", "insurance", "damage"]):
        score += 20
        signals.append("Insurance/damage — urgent need")

    # Corporate owner penalty
    if owner_name and any(t in owner_name.upper() for t in ["LLC", "INC", "CORP", "TRUST", "LP"]):
        score -= 5
        signals.append("Corporate owner — skip trace recommended")

    readiness = min(100, score)

    if readiness >= 80:
        action = "Call immediately — high readiness lead"
        urgency = "critical"
        window = max(1, 7 - days_old)
    elif readiness >= 60:
        action = "Contact within 48 hours"
        urgency = "high"
        window = max(3, 14 - days_old)
    elif readiness >= 40:
        action = "Add to outreach queue"
        urgency = "medium"
        window = 14
    else:
        action = "Monitor — low priority"
        urgency = "low"
        window = 30

    budget_low = valuation * 0.10
    budget_high = valuation * 0.15

    if days_old <= 3 and valuation >= 100000:
        competition = "high"
    elif days_old <= 7:
        competition = "medium"
    else:
        competition = "low"

    return {
        "readiness_score": readiness,
        "recommended_action": action,
        "urgency_level": urgency,
        "contact_window_days": window,
        "budget_range": [budget_low, budget_high],
        "competition_level": competition,
        "signals": signals,
    }


# ============================================================================
# INSURANCE CLAIM DETECTION
# ============================================================================

_INSURANCE_RE = re.compile(
    r"\b(insurance|fire.?damage|water.?damage|storm.?damage|flood.?damage|"
    r"mold|remediat|restorat|casualty|catastroph|wind.?damage|hail|"
    r"smoke.?damage|vandalism|disaster|emergency.?repair|claim|loss|adjuster)\b",
    re.IGNORECASE,
)


def is_insurance_claim(lead: dict) -> bool:
    """Detect if a lead is likely an insurance claim."""
    desc = (
        f"{lead.get('work_description', '')} "
        f"{lead.get('description_full', '')} "
        f"{lead.get('permit_type', '')}"
    )
    return bool(_INSURANCE_RE.search(desc))


def score_breakdown(lead: dict) -> dict:
    """Generate a detailed score breakdown for UI display.
    Delegates to calculate_score_v2 for consistency."""
    _score, _temp, raw = calculate_score_v2(lead)

    label_map = [
        ("recency", "Recency"),
        ("permit_type", "Permit Type"),
        ("geography", "Geographic Quality"),
        ("project_indicators", "Project Indicators"),
        ("contact", "Contact Info"),
        ("property_data", "Property Data"),
        ("enrichment", "Enrichment Status"),
        ("insurance", "Insurance Signal"),
    ]

    components = []
    for key, label in label_map:
        entry = raw.get(key, {})
        components.append({
            "label": label,
            "value": entry.get("score", 0),
            "max": entry.get("max", 0),
            "detail": entry.get("detail", ""),
        })

    return {
        "total": raw.get("total", lead.get("score", 0)),
        "components": components,
    }
