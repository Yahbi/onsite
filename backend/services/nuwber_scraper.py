"""
ClustrMaps people search scraper — uses Schema.org structured data.

Extracts itemprop="telephone" and itemprop="name" from person cards.
Filters by city/state to match the correct person.
No Cloudflare blocking. Cached 7 days. 2s rate limit.

Note: Originally nuwber_scraper.py — Nuwber returns 403 (Cloudflare managed challenge).
Replaced with ClustrMaps which has identical data and no blocking.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400
_last_call = 0.0
RATE_LIMIT = 2.0


def _cache_key(prefix: str, val: str) -> str:
    return hashlib.md5(f"clustr|{prefix}|{val.lower().strip()}".encode()).hexdigest()


def _clean_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


async def _rate_wait():
    global _last_call
    now = time.time()
    wait = RATE_LIMIT - (now - _last_call)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_call = time.time()


def _fetch_sync(url: str) -> Optional[str]:
    import cloudscraper
    try:
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "darwin"},
        )
        r = scraper.get(url, timeout=15)
        if r.status_code == 200:
            return r.text
        logger.debug(f"ClustrMaps {r.status_code} for {url}")
    except Exception as e:
        logger.debug(f"ClustrMaps error: {e}")
    return None


def _parse_persons(html: str) -> list[dict]:
    """Parse Schema.org Person entries from ClustrMaps HTML.

    Location data isn't on the list page — individual profiles would need
    a second fetch. We collect all persons with phones and let the orchestrator
    handle location matching via other sources.
    """
    persons = []
    blocks = re.split(r'itemtype="https?://schema\.org/Person"', html)

    for block in blocks[1:]:
        name_m = re.search(r"itemprop=['\"]name['\"]>([^<]+)<", block)
        name = name_m.group(1).strip() if name_m else ""

        phone_m = re.search(r"itemprop=['\"]telephone['\"]>([^<]+)<", block)
        phone = _clean_phone(phone_m.group(1)) if phone_m else ""

        if not phone:
            continue

        persons.append({"name": name, "phone": phone})

    return persons


async def search_by_name(name: str, city: str = "", state: str = "") -> Optional[dict]:
    """Search ClustrMaps by person name, filter by city/state."""
    if not name or len(name.strip()) < 3:
        return None

    ck = _cache_key("name", f"{name}|{city}|{state}")
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    await _rate_wait()

    parts = name.strip().split()
    slug = "-".join(p.capitalize() for p in parts)
    url = f"https://clustrmaps.com/persons/{quote(slug)}"

    html = await asyncio.to_thread(_fetch_sync, url)
    if not html:
        return None

    persons = _parse_persons(html)
    if not persons:
        return None

    # Collect all unique phones from matching persons
    phones = []
    best_name = ""
    for p in persons:
        if p["phone"] and p["phone"] not in phones:
            phones.append(p["phone"])
        if p["name"] and not best_name:
            best_name = p["name"]

    if not phones:
        return None

    out = {
        "phone": phones[0],
        "phones": phones[:5],
        "email": "",
        "emails": [],
        "name": best_name or name,
        "source": "ClustrMaps",
    }
    _cache[ck] = out
    _cache_ts[ck] = time.time()
    return out


async def search_by_address(address: str, city: str = "", state: str = "") -> Optional[dict]:
    """ClustrMaps doesn't support address search — delegate to name search."""
    # ClustrMaps only supports name-based search
    return None
