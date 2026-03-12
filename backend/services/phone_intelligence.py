"""
Abstract API Phone Intelligence — validates and enriches phone numbers.

Returns carrier info, line type, validation status, risk level,
and SMS email gateway for found phone numbers.

Rate: 100 lookups/month on current plan.
Cached 7 days per phone to conserve quota.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

API_KEY = "f5577680db464a06ae7772590e28116f"
BASE_URL = "https://phoneintelligence.abstractapi.com/v1/"

_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400
_last_call = 0.0
RATE_LIMIT = 1.0  # 1s between calls


def _digits(phone: str) -> str:
    """Extract 10-digit US phone number."""
    d = re.sub(r'\D', '', phone)
    if len(d) == 11 and d.startswith('1'):
        d = d[1:]
    return d if len(d) == 10 else ""


def _cache_key(phone: str) -> str:
    return hashlib.md5(f"phoneint|{phone}".encode()).hexdigest()


async def _rate_wait():
    global _last_call
    now = time.time()
    wait = RATE_LIMIT - (now - _last_call)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_call = time.time()


async def lookup(phone: str) -> Optional[dict]:
    """
    Validate and enrich a phone number via Abstract API.

    Args:
        phone: Phone number (any format — digits extracted automatically)

    Returns:
        Dict with validation, carrier, line_type, risk, sms_email fields,
        or None if lookup fails.
    """
    digits = _digits(phone)
    if not digits or not API_KEY:
        return None

    ck = _cache_key(digits)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    await _rate_wait()

    try:
        params = {"api_key": API_KEY, "phone": f"1{digits}"}
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(BASE_URL, params=params)

        if resp.status_code == 401:
            logger.warning("Abstract Phone API key invalid or expired")
            return None
        if resp.status_code == 429:
            logger.warning("Abstract Phone API rate limit hit")
            return None
        if resp.status_code != 200:
            logger.debug(f"Abstract Phone API {resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()

        validation = data.get("phone_validation", {})
        carrier = data.get("phone_carrier", {})
        risk = data.get("phone_risk", {})
        messaging = data.get("phone_messaging", {})
        fmt = data.get("phone_format", {})

        out = {
            "phone_formatted": fmt.get("national", phone),
            "is_valid": validation.get("is_valid", False),
            "line_status": validation.get("line_status", "unknown"),
            "line_type": carrier.get("line_type", "unknown"),
            "carrier": carrier.get("name", ""),
            "is_voip": validation.get("is_voip", False),
            "risk_level": risk.get("risk_level", "unknown"),
            "is_disposable": risk.get("is_disposable", False),
            "sms_email": messaging.get("sms_email", ""),
            "sms_domain": messaging.get("sms_domain", ""),
            "source": "AbstractPhoneIntel",
        }

        _cache[ck] = out
        _cache_ts[ck] = time.time()
        return out

    except Exception as e:
        logger.debug(f"Abstract Phone API error: {e}")
        return None


async def validate_phones(phones: list[str]) -> list[dict]:
    """
    Validate a list of phones, return enriched results.
    Skips duplicates and caches aggressively.
    """
    seen = set()
    results = []
    for phone in phones:
        digits = _digits(phone)
        if not digits or digits in seen:
            continue
        seen.add(digits)
        result = await lookup(phone)
        if result:
            results.append(result)
    return results
