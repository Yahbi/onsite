"""
NumLookup reverse phone lookup — free carrier + owner data from numlookup.com.

Scrapes the public results page for owner name, carrier, and line type.
Uses cloudscraper to bypass Cloudflare. Rate-limited. Results cached 7 days.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── In-memory cache ──
_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400

_last_request: float = 0.0
RATE_LIMIT_SECS = 2.0


def _get_scraper():
    import cloudscraper
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False},
    )


def _normalize_phone(phone: str) -> str:
    """Strip to 10-digit US number."""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return digits


def _cache_key(phone: str) -> str:
    return hashlib.md5(f"numlookup|{phone}".encode()).hexdigest()


async def _rate_wait():
    global _last_request
    now = time.time()
    wait = RATE_LIMIT_SECS - (now - _last_request)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_request = time.time()


def _scrape_sync(phone: str) -> Optional[dict]:
    """Synchronous scrape — run via asyncio.to_thread."""
    url = f"https://www.numlookup.com/us/{phone}"
    scraper = _get_scraper()
    try:
        resp = scraper.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning("NumLookup returned %d for %s", resp.status_code, phone)
            return None
    except Exception as exc:
        logger.warning("NumLookup request failed for %s: %s", phone, exc)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    result: dict = {"source": "NumLookup", "phone": phone}

    # Owner name — typically in an h2 or span with class containing "name"
    for selector in ["h2.owner-name", ".owner-name", "h2", ".name"]:
        tag = soup.select_one(selector)
        if tag:
            name = re.sub(r"\s+", " ", tag.get_text()).strip()
            if len(name) >= 3 and name.upper() not in ("N/A", "UNKNOWN", "NOT FOUND"):
                result["name"] = name.title()
                break

    # Carrier and line type — look for labeled fields
    text = soup.get_text(" ", strip=True)
    carrier_match = re.search(r"(?:Carrier|Network)[:\s]+([A-Za-z0-9&\s\-\.]+?)(?:\s{2,}|\||$)", text)
    if carrier_match:
        result["carrier"] = carrier_match.group(1).strip()

    line_match = re.search(r"(?:Line\s*Type|Type)[:\s]+(Mobile|Landline|VoIP|Wireless|Fixed)", text, re.IGNORECASE)
    if line_match:
        result["line_type"] = line_match.group(1).capitalize()

    if "name" not in result and "carrier" not in result:
        return None

    return result


async def reverse_phone(phone: str) -> Optional[dict]:
    """Look up a phone number on NumLookup. Returns dict or None."""
    normalized = _normalize_phone(phone)
    if not normalized:
        logger.warning("Invalid phone number: %s", phone)
        return None

    key = _cache_key(normalized)
    now = time.time()
    if key in _cache and (now - _cache_ts.get(key, 0)) < CACHE_TTL:
        return _cache[key]

    await _rate_wait()
    result = await asyncio.to_thread(_scrape_sync, normalized)

    if result is not None:
        _cache[key] = result
        _cache_ts[key] = time.time()

    return result
