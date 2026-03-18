"""
Veriphone API — phone number validation and carrier lookup.

Returns validity, phone type (mobile/landline/voip), carrier, region.
Free tier: 1000 verifications/month.
Cached 7 days per phone to conserve quota.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional

import os

import httpx

logger = logging.getLogger(__name__)

API_KEY = os.getenv("VERIPHONE_API_KEY", "")
BASE_URL = "https://api.veriphone.io/v2/verify"

_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400
_last_call = 0.0
RATE_LIMIT = 0.5  # 500ms between calls


def _digits(phone: str) -> str:
    """Extract 10-digit US phone number."""
    d = re.sub(r'\D', '', phone)
    if len(d) == 11 and d.startswith('1'):
        d = d[1:]
    return d if len(d) == 10 else ""


def _cache_key(phone: str) -> str:
    return hashlib.md5(f"veriphone|{phone}".encode()).hexdigest()


async def _rate_wait():
    global _last_call
    now = time.time()
    wait = RATE_LIMIT - (now - _last_call)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_call = time.time()


async def verify(phone: str) -> Optional[dict]:
    """
    Verify a phone number via Veriphone API.

    Returns dict with: valid, phone_type, carrier, region, e164,
    international_number, local_number.
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
        params = {"phone": f"1{digits}", "key": API_KEY}
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(BASE_URL, params=params)

        if resp.status_code == 401:
            logger.warning("Veriphone API key invalid or expired")
            return None
        if resp.status_code == 429:
            logger.warning("Veriphone rate limit hit")
            return None
        if resp.status_code != 200:
            logger.debug(f"Veriphone {resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()

        if data.get("status") != "success":
            return None

        out = {
            "valid": data.get("phone_valid", False),
            "phone_type": data.get("phone_type", "unknown"),
            "carrier": data.get("carrier", ""),
            "region": data.get("phone_region", ""),
            "e164": data.get("e164", ""),
            "international_number": data.get("international_number", ""),
            "local_number": data.get("local_number", ""),
            "country": data.get("country", ""),
            "source": "Veriphone",
        }

        _cache[ck] = out
        _cache_ts[ck] = time.time()
        return out

    except Exception as e:
        logger.debug(f"Veriphone error: {e}")
        return None
