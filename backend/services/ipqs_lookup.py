"""
IPQualityScore lookup — free API for phone/email validation + owner info.

Sign up at https://www.ipqualityscore.com/ for a free API key (5,000 lookups/month).
Uses httpx async client. Rate-limited. Results cached 7 days.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Sign up at https://www.ipqualityscore.com/ for a free key
API_KEY = ""  # user must set this

# ── In-memory cache ──
_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400

_last_request: float = 0.0
RATE_LIMIT_SECS = 1.0

BASE_URL = "https://ipqualityscore.com/api/json"


def _normalize_phone(phone: str) -> str:
    """Strip to 10-digit US number, return with +1 prefix."""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return f"1{digits}"


def _cache_key(lookup_type: str, value: str) -> str:
    raw = f"ipqs|{lookup_type}|{value.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()


async def _rate_wait():
    global _last_request
    now = time.time()
    wait = RATE_LIMIT_SECS - (now - _last_request)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_request = time.time()


async def _fetch(url: str) -> Optional[dict]:
    """Make async GET request, return JSON or None."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("IPQS returned %d for %s", resp.status_code, url)
                return None
            data = resp.json()
            if not data.get("success", False):
                logger.warning("IPQS lookup failed: %s", data.get("message", "unknown"))
                return None
            return data
    except Exception as exc:
        logger.warning("IPQS request failed: %s", exc)
        return None


async def phone_lookup(phone: str) -> Optional[dict]:
    """Look up a phone number via IPQualityScore. Returns dict or None."""
    if not API_KEY:
        logger.debug("IPQS API key not configured — skipping phone lookup")
        return None

    normalized = _normalize_phone(phone)
    if not normalized:
        logger.warning("Invalid phone number: %s", phone)
        return None

    key = _cache_key("phone", normalized)
    now = time.time()
    if key in _cache and (now - _cache_ts.get(key, 0)) < CACHE_TTL:
        return _cache[key]

    await _rate_wait()
    url = f"{BASE_URL}/phone/{API_KEY}/{normalized}"
    data = await _fetch(url)
    if data is None:
        return None

    result = {
        "source": "IPQualityScore",
        "phone": phone,
        "name": data.get("name") or data.get("first_name", ""),
        "carrier": data.get("carrier", ""),
        "line_type": data.get("line_type", ""),
        "city": data.get("city", ""),
        "state": data.get("state", ""),
        "country": data.get("country", ""),
        "fraud_score": data.get("fraud_score"),
        "valid": data.get("valid", False),
        "active": data.get("active", False),
        "risky": data.get("risky", False),
    }

    _cache[key] = result
    _cache_ts[key] = time.time()
    return result


async def email_lookup(email: str) -> Optional[dict]:
    """Validate an email and get owner info via IPQualityScore. Returns dict or None."""
    if not API_KEY:
        logger.debug("IPQS API key not configured — skipping email lookup")
        return None

    email = email.strip().lower()
    if not email or "@" not in email:
        logger.warning("Invalid email: %s", email)
        return None

    key = _cache_key("email", email)
    now = time.time()
    if key in _cache and (now - _cache_ts.get(key, 0)) < CACHE_TTL:
        return _cache[key]

    await _rate_wait()
    url = f"{BASE_URL}/email/{API_KEY}/{email}"
    data = await _fetch(url)
    if data is None:
        return None

    result = {
        "source": "IPQualityScore",
        "email": email,
        "name": data.get("first_name", ""),
        "valid": data.get("valid", False),
        "disposable": data.get("disposable", False),
        "deliverability": data.get("deliverability", ""),
        "fraud_score": data.get("fraud_score"),
        "spam_trap": data.get("spam_trap_score", 0),
        "recent_abuse": data.get("recent_abuse", False),
        "leaked": data.get("leaked", False),
        "domain_age_days": data.get("domain_age", {}).get("human", ""),
    }

    _cache[key] = result
    _cache_ts[key] = time.time()
    return result
