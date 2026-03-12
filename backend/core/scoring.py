"""
Onsite Lead Scoring Engine v2.0
Multi-factor scoring with advanced heuristics and ML-ready structure

Scoring Factors:
- Recency (0-30 points): How recent the permit was filed
- Project Value (0-20 points): Valuation amount
- Permit Type (0-15 points): High-value vs low-value work types
- Contact Availability (0-15 points): Owner info completeness
- Geographic Quality (0-10 points): Property location signals
- Owner Type (0-10 points): Individual vs corporate/trust
- Enrichment Status (0-5 points): Data completeness bonus
- Insurance Signals (0-20 points): Fire/water/storm damage indicators
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


def calculate_score_v2(lead: dict) -> Tuple[int, str, dict]:
    """
    Enhanced multi-factor lead scoring algorithm (v2.0).
    Returns (score, temperature, breakdown_dict).

    Scoring breakdown:
    - Recency: 0-30 points
    - Project Value: 0-20 points
    - Permit Type: 0-15 points
    - Contact Info: 0-15 points
    - Geographic Quality: 0-10 points
    - Owner Type: 0-10 points
    - Insurance Signals: 0-20 points (bonus, can exceed 100)
    """
    score = 0
    breakdown = {}

    days_old = lead.get("days_old", 999)
    valuation = lead.get("valuation", 0)
    permit_type = (lead.get("permit_type") or "").lower()
    work_desc = (lead.get("work_description") or "").lower()
    desc_full = (lead.get("description_full") or "").lower()
    owner_name = lead.get("owner_name", "")
    owner_phone = lead.get("owner_phone", "")
    owner_email = lead.get("owner_email", "")
    city = (lead.get("city") or "").lower()
    state = (lead.get("state") or "").upper()

    # 1. RECENCY SCORE (0-30 points) - Most critical factor
    recency_score = 0
    if days_old <= 1:
        recency_score = 30
    elif days_old <= 3:
        recency_score = 27
    elif days_old <= 5:
        recency_score = 24
    elif days_old <= 7:
        recency_score = 20
    elif days_old <= 10:
        recency_score = 16
    elif days_old <= 14:
        recency_score = 12
    elif days_old <= 21:
        recency_score = 8
    elif days_old <= 30:
        recency_score = 4
    elif days_old <= 45:
        recency_score = 2
    else:
        recency_score = 0

    score += recency_score
    breakdown["recency"] = {
        "score": recency_score,
        "max": 30,
        "detail": f"{days_old} day{'s' if days_old != 1 else ''} old"
    }

    # 2. PROJECT VALUE SCORE (0-20 points)
    value_score = 0
    if valuation >= 1000000:
        value_score = 20
    elif valuation >= 500000:
        value_score = 18
    elif valuation >= 250000:
        value_score = 15
    elif valuation >= 100000:
        value_score = 12
    elif valuation >= 50000:
        value_score = 8
    elif valuation >= 25000:
        value_score = 5
    elif valuation >= 10000:
        value_score = 3
    else:
        value_score = 0

    score += value_score
    breakdown["value"] = {
        "score": value_score,
        "max": 20,
        "detail": f"${valuation:,.0f}" if valuation > 0 else "Not specified"
    }

    # 3. PERMIT TYPE SCORE (0-15 points)
    type_score = 0
    type_detail = "Unknown"

    # Ultra-high value types
    ultra_high = ["new construction", "new building", "new single family", "new commercial"]
    # High value types
    high_value = ["addition", "remodel", "renovation", "tenant improvement",
                  "solar", "adu", "accessory dwelling", "second story"]
    # Medium value types
    medium_value = ["alteration", "repair", "electrical", "plumbing", "mechanical",
                    "hvac", "roof", "reroof", "window", "siding"]
    # Low value types
    low_value = ["demolition", "sign", "fence", "grading", "pool", "shed"]

    combined_text = f"{permit_type} {work_desc}"

    if any(t in combined_text for t in ultra_high):
        type_score = 15
        type_detail = "New Construction"
    elif any(t in combined_text for t in high_value):
        type_score = 12
        type_detail = "Major Renovation"
    elif any(t in combined_text for t in medium_value):
        type_score = 7
        type_detail = "Standard Repair"
    elif any(t in combined_text for t in low_value):
        type_score = 2
        type_detail = "Minor Work"
    else:
        type_score = 5
        type_detail = permit_type[:30] if permit_type else "Unspecified"

    score += type_score
    breakdown["permit_type"] = {
        "score": type_score,
        "max": 15,
        "detail": type_detail
    }

    # 4. CONTACT AVAILABILITY SCORE (0-15 points)
    contact_score = 0
    contact_detail = []

    if owner_phone:
        contact_score += 8
        contact_detail.append("Phone")
    if owner_email:
        contact_score += 5
        contact_detail.append("Email")
    if owner_name and owner_name.strip():
        contact_score += 2
        contact_detail.append("Name")

    score += contact_score
    breakdown["contact"] = {
        "score": contact_score,
        "max": 15,
        "detail": ", ".join(contact_detail) if contact_detail else "No contact info"
    }

    # 5. GEOGRAPHIC QUALITY SCORE (0-10 points)
    geo_score = 0
    geo_detail = "Unknown location"

    # High-value metro areas
    premium_cities = ["los angeles", "san francisco", "san jose", "seattle", "portland",
                      "san diego", "denver", "austin", "miami", "atlanta", "boston",
                      "new york", "chicago", "washington"]

    if any(city.startswith(metro) or metro in city for metro in premium_cities):
        geo_score = 10
        geo_detail = f"{city.title()} (Premium market)"
    elif state in ["CA", "NY", "WA", "TX", "FL", "CO", "MA"]:
        geo_score = 7
        geo_detail = f"{state} (Strong market)"
    else:
        geo_score = 5
        geo_detail = f"{state or 'Unknown'}"

    score += geo_score
    breakdown["geography"] = {
        "score": geo_score,
        "max": 10,
        "detail": geo_detail
    }

    # 6. OWNER TYPE SCORE (0-10 points)
    owner_score = 0
    owner_detail = "Unknown"

    if owner_name:
        name_upper = owner_name.upper()
        if any(indicator in name_upper for indicator in ["LLC", "INC", "CORP", "LP", "LTD"]):
            owner_score = 5
            owner_detail = "Corporate (harder reach)"
        elif "TRUST" in name_upper:
            owner_score = 6
            owner_detail = "Trust (moderate)"
        else:
            owner_score = 10
            owner_detail = "Individual (best)"

    score += owner_score
    breakdown["owner_type"] = {
        "score": owner_score,
        "max": 10,
        "detail": owner_detail
    }

    # 7. INSURANCE/EMERGENCY SIGNALS (BONUS: 0-20 points)
    insurance_score = 0
    insurance_detail = "None detected"

    insurance_keywords = [
        "fire", "water damage", "flood", "storm", "wind damage",
        "insurance", "restoration", "remediation", "mold", "smoke",
        "emergency", "casualty", "disaster", "hail", "vandalism"
    ]

    combined_desc = f"{permit_type} {work_desc} {desc_full}"
    detected_signals = [kw for kw in insurance_keywords if kw in combined_desc]

    if detected_signals:
        insurance_score = min(20, len(detected_signals) * 5)
        insurance_detail = f"Detected: {', '.join(detected_signals[:3])}"

    score += insurance_score
    breakdown["insurance"] = {
        "score": insurance_score,
        "max": 20,
        "detail": insurance_detail
    }

    # TOTAL SCORE (can exceed 100 with insurance bonus)
    score = min(125, score)  # Cap at 125
    breakdown["total"] = score

    # TEMPERATURE CLASSIFICATION
    if score >= 90:
        temperature = "Hot"
    elif score >= 75:
        temperature = "Hot"
    elif score >= 50:
        temperature = "Warm"
    else:
        temperature = "Cold"

    return score, temperature, breakdown


def calculate_score(days_old: int, valuation: float, permit_type: str) -> Tuple[int, str]:
    """
    Legacy scoring function for backwards compatibility.
    Use calculate_score_v2() for enhanced multi-factor scoring.
    """
    # Convert to lead dict and use v2
    lead = {
        "days_old": days_old,
        "valuation": valuation,
        "permit_type": permit_type
    }
    score, temp, _ = calculate_score_v2(lead)
    return score, temp


def compute_readiness(lead: dict) -> dict:
    """
    Compute readiness_score and action hints for a lead.
    Returns dict with readiness_score, recommended_action, urgency_level,
    contact_window_days, budget_range, competition_level.
    """
    score = 0
    signals = []
    
    days_old = lead.get("days_old", 999)
    valuation = lead.get("valuation", 0)
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
    
    # Check for LLC (skip trace opportunity)
    if owner_name and any(t in owner_name.upper() for t in ["LLC", "INC", "CORP", "TRUST", "LP"]):
        score -= 5  # Slightly harder to reach
        signals.append("Corporate owner — skip trace recommended")
    
    readiness = min(100, score)
    
    # Determine action + urgency
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
    
    # Budget estimate (10-15% of valuation for GC services)
    budget_low = valuation * 0.10
    budget_high = valuation * 0.15
    
    # Competition estimate
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
    desc = f"{lead.get('work_description', '')} {lead.get('description_full', '')} {lead.get('permit_type', '')}"
    return bool(_INSURANCE_RE.search(desc))


def score_breakdown(lead: dict) -> dict:
    """Generate a detailed score breakdown for UI display."""
    days_old = lead.get("days_old", 999)
    valuation = lead.get("valuation", 0)
    permit_type = lead.get("permit_type", "")
    
    breakdown = {
        "total": lead.get("score", 0),
        "components": [
            {
                "label": "Recency",
                "value": max(0, 30 - days_old),
                "max": 30,
                "detail": f"{days_old} days old"
            },
            {
                "label": "Project Value",
                "value": min(20, int(valuation / 50000) * 2),
                "max": 20,
                "detail": f"${valuation:,.0f}"
            },
            {
                "label": "Permit Type",
                "value": 10 if any(t in (permit_type or "").lower() for t in ["construction", "remodel", "addition"]) else 5,
                "max": 15,
                "detail": permit_type or "Unknown"
            },
            {
                "label": "Contact Info",
                "value": (10 if lead.get("owner_phone") else 0) + (5 if lead.get("owner_email") else 0),
                "max": 15,
                "detail": "Available" if lead.get("owner_phone") or lead.get("owner_email") else "Missing"
            },
            {
                "label": "Insurance Signal",
                "value": 15 if is_insurance_claim(lead) else 0,
                "max": 15,
                "detail": "Detected" if is_insurance_claim(lead) else "None"
            },
        ]
    }
    return breakdown
