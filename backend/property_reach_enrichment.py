"""
PropertyReach API Integration for Owner Contact Enrichment
Free tier: 100 skip traces/month
Returns: Owner name, phone number, email address
"""

import os
import logging
import requests
import json
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Configuration
PROPERTYREACH_API_KEY = os.getenv("PROPERTYREACH_API_KEY", "")
PROPERTYREACH_API_KEY_ALT = os.getenv("PROPERTYREACH_API_KEY_ALT", "")
PROPERTYREACH_API_KEY_ALT2 = os.getenv("PROPERTYREACH_API_KEY_ALT2", "")
PROPERTYREACH_API_KEY_ALT3 = os.getenv("PROPERTYREACH_API_KEY_ALT3", "")
PROPERTYREACH_API_KEY_ALT4 = os.getenv("PROPERTYREACH_API_KEY_ALT4", "")
PROPERTYREACH_API_KEY_ALT5 = os.getenv("PROPERTYREACH_API_KEY_ALT5", "")
PROPERTYREACH_API_KEY_ALT6 = os.getenv("PROPERTYREACH_API_KEY_ALT6", "")
PROPERTYREACH_API_KEY_ALT7 = os.getenv("PROPERTYREACH_API_KEY_ALT7", "")
PROPERTYREACH_API_KEY_ALT8 = os.getenv("PROPERTYREACH_API_KEY_ALT8", "")
PROPERTYREACH_API_KEY_ALT9 = os.getenv("PROPERTYREACH_API_KEY_ALT9", "")

# Collect all keys (filter out empty strings)
PROPERTYREACH_KEYS = [k for k in (
    PROPERTYREACH_API_KEY, PROPERTYREACH_API_KEY_ALT, PROPERTYREACH_API_KEY_ALT2,
    PROPERTYREACH_API_KEY_ALT3, PROPERTYREACH_API_KEY_ALT4, PROPERTYREACH_API_KEY_ALT5,
    PROPERTYREACH_API_KEY_ALT6, PROPERTYREACH_API_KEY_ALT7, PROPERTYREACH_API_KEY_ALT8,
    PROPERTYREACH_API_KEY_ALT9
) if k]

PROPERTYREACH_ENABLED = os.getenv("PROPERTYREACH_ENABLED", "false").lower() in ("true", "1", "yes", "y")
PROPERTYREACH_MONTHLY_LIMIT = int(os.getenv("PROPERTYREACH_MONTHLY_LIMIT", "500"))
PROPERTYREACH_BASE_URL = "https://api.propertyreach.com/v1"

# Cache file for API usage tracking
USAGE_CACHE_FILE = Path(__file__).parent / "propertyreach_usage.json"


def load_usage_tracker():
    """Load API usage tracker from file"""
    if not USAGE_CACHE_FILE.exists():
        return {"month": datetime.now().strftime("%Y-%m"), "count": 0}
    
    try:
        with open(USAGE_CACHE_FILE, "r") as f:
            data = json.load(f)
            current_month = datetime.now().strftime("%Y-%m")
            # Reset counter if new month
            if data.get("month") != current_month:
                data = {"month": current_month, "count": 0}
            return data
    except Exception as e:
        logger.warning(f"Failed to load usage tracker: {e}")
        return {"month": datetime.now().strftime("%Y-%m"), "count": 0}


def save_usage_tracker(tracker):
    """Save API usage tracker to file"""
    try:
        with open(USAGE_CACHE_FILE, "w") as f:
            json.dump(tracker, f)
    except Exception as e:
        logger.warning(f"Failed to save usage tracker: {e}")


def can_make_api_call():
    """Check if we're within monthly API limit"""
    if not PROPERTYREACH_ENABLED:
        return False

    if not PROPERTYREACH_KEYS:
        logger.warning("PropertyReach API key not configured")
        return False
    
    tracker = load_usage_tracker()
    return tracker["count"] < PROPERTYREACH_MONTHLY_LIMIT


def increment_api_usage():
    """Increment API usage counter"""
    tracker = load_usage_tracker()
    tracker["count"] += 1
    save_usage_tracker(tracker)
    
    # Log warning if approaching limit
    if tracker["count"] >= PROPERTYREACH_MONTHLY_LIMIT * 0.9:
        logger.warning(
            f"PropertyReach API usage: {tracker['count']}/{PROPERTYREACH_MONTHLY_LIMIT} "
            f"(90%+ of monthly limit)"
        )


def skip_trace_property(
    address: str = None,
    city: str = None,
    state: str = None,
    zip_code: str = None,
    apn: str = None,
    county: str = None
) -> dict:
    """
    Call PropertyReach Skip Trace API to get owner contact information.
    
    Returns owner name/phone/email if available.
    """
    # Check if API is enabled and within limits
    if not can_make_api_call():
        if PROPERTYREACH_ENABLED:
            logger.debug("PropertyReach API limit reached for this month")
        return {}
    
    # Build request payload
    payload = {}
    if address:
        payload["streetAddress"] = address
    if city:
        payload["city"] = city
    if state:
        payload["state"] = state
    if zip_code:
        payload["zip"] = zip_code
    if apn:
        payload["apn"] = apn
    if county:
        payload["county"] = county
    
    # Need at least address or APN
    if not address and not apn:
        logger.warning("Skip trace requires address or APN")
        return {}
    
    url = f"{PROPERTYREACH_BASE_URL}/skip-trace"

    for key in PROPERTYREACH_KEYS:
        headers = {
            "x-api-key": key,
            "Content-Type": "application/json"
        }
        try:
            logger.info(f"PropertyReach skip trace: {address or apn} | Payload: {payload}")
            resp = requests.post(url, json=payload, headers=headers, timeout=30)

            # Log full response details for debugging
            logger.info(f"PropertyReach API status: {resp.status_code} | Key: ...{key[-4:]} | Response length: {len(resp.text)} bytes")

            # Increment usage counter on successful request (even if no data)
            increment_api_usage()

            if resp.status_code == 401:
                logger.error(f"PropertyReach API authentication failed - check API key ending in ...{key[-4:]}")
                continue
            if resp.status_code == 429:
                logger.warning(f"PropertyReach API rate limit exceeded for key ...{key[-4:]}")
                continue
            if resp.status_code != 200:
                logger.warning(f"PropertyReach API returned {resp.status_code} for key ...{key[-4:]}: {resp.text[:500]}")
                continue

            data = resp.json()
            logger.info(f"PropertyReach API response data: {data}")

            persons = data.get("persons") or []
            phones = []
            emails = []
            names = []
            for p in persons:
                fn = p.get("firstName") or ""
                ln = p.get("lastName") or ""
                nm = (fn + " " + ln).strip()
                if nm:
                    names.append(nm)
                for ph in p.get("phones") or []:
                    if ph.get("phone"):
                        phones.append(ph["phone"])
                for em in p.get("emails") or []:
                    if em.get("email"):
                        emails.append(em["email"])

            # legacy top-level fields
            if data.get("ownerName"):
                names.append(data["ownerName"])
            if data.get("phone"):
                phones.append(data["phone"])
            if data.get("email"):
                emails.append(data["email"])

            result = {
                "names": list(dict.fromkeys([n for n in names if n])),
                "phones": list(dict.fromkeys([p for p in phones if p])),
                "emails": list(dict.fromkeys([e for e in emails if e])),
                "success": bool(names or phones or emails)
            }

            if result["success"]:
                logger.info(f"✅ Skip trace: {result['names'][:1]} / {result['phones'][:1]} / {result['emails'][:1]} (key tail …{key[-4:]})")
                return result

        except requests.exceptions.Timeout:
            logger.warning(f"PropertyReach API timeout for {address or apn} (key tail …{key[-4:]})")
            continue
        except Exception as e:
            logger.error(f"PropertyReach API error: {e}")
            continue

    return {}


def get_current_usage():
    """Get current API usage stats"""
    tracker = load_usage_tracker()
    return {
        "month": tracker["month"],
        "used": tracker["count"],
        "limit": PROPERTYREACH_MONTHLY_LIMIT,
        "remaining": max(0, PROPERTYREACH_MONTHLY_LIMIT - tracker["count"]),
        "enabled": PROPERTYREACH_ENABLED
    }


def fetch_property_detail(
    address: str = None,
    city: str = None,
    state: str = None,
    zip_code: str = None,
    apn: str = None,
    county: str = None,
    fips: str = None,
) -> dict:
    """
    Call PropertyReach /v1/property to retrieve parcel/owner details.
    Returns owner names + mailing address where available.
    """
    if not PROPERTYREACH_ENABLED or not PROPERTYREACH_API_KEY:
        return {}

    params = {}
    if apn:
        params["apn"] = apn
    if address:
        params["streetAddress"] = address
    if city:
        params["city"] = city
    if state:
        params["state"] = state
    if zip_code:
        params["zip"] = zip_code
    if county:
        params["county"] = county
    if fips:
        params["fips"] = fips

    if not params:
        return {}

    headers = {"x-api-key": PROPERTYREACH_API_KEY}
    url = f"{PROPERTYREACH_BASE_URL}/property"
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
        if resp.status_code != 200:
            return {}
        data = resp.json().get("property") or {}
        names = []
        for k in ("owner1Name", "owner2Name", "ownerNames"):
            v = data.get(k)
            if v:
                if isinstance(v, str):
                    names.extend([n.strip() for n in v.split(";") if n.strip()])
        names = list(dict.fromkeys(names))
        mailing = data.get("mailingAddress")
        return {"names": names, "mailing": mailing, "raw": data}
    except Exception as e:
        logger.warning(f"PropertyReach property lookup failed: {e}")
        return {}


if __name__ == "__main__":
    # Test the integration
    print("PropertyReach API Integration Test")
    print("=" * 50)
    
    # Check configuration
    print(f"API Key configured: {'Yes' if PROPERTYREACH_API_KEY else 'No'}")
    print(f"Enabled: {PROPERTYREACH_ENABLED}")
    
    # Check usage
    usage = get_current_usage()
    print(f"Usage: {usage['used']}/{usage['limit']} ({usage['remaining']} remaining)")
    
    # Test skip trace (only if enabled)
    if PROPERTYREACH_ENABLED and PROPERTYREACH_API_KEY:
        print("\nTesting skip trace API...")
        result = skip_trace_property(
            address="1600 Amphitheatre Parkway",
            city="Mountain View",
            state="CA",
            zip_code="94043"
        )
        print(f"Result: {result}")
    else:
        print("\n⚠️  API not enabled or key missing - skipping test")
