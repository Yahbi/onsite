"""
TruePeopleSearch.com scraper — JSON-LD structured data for phone + email.

High-quality data source:
- Phone numbers (multiple per person)
- Email addresses (NOT obfuscated)
- Aliases, relatives, full addresses

Heavy Cloudflare protection — uses cloudscraper with retry logic.
Rate-limited at 2s between calls. Results cached 7 days.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400
_last_call = 0.0
RATE_LIMIT = 2.0


def _get_scraper():
    import cloudscraper
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False},
        delay=3,
    )


def _cache_key(name: str, city: str, state: str) -> str:
    raw = f"tps|{name.lower().strip()}|{city.lower().strip()}|{state.upper().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()


async def _rate_wait():
    global _last_call
    now = time.time()
    wait = RATE_LIMIT - (now - _last_call)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_call = time.time()


def _clean_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


def _scrape_sync(name: str, city: str, state: str) -> Optional[dict]:
    """Scrape TruePeopleSearch for JSON-LD Person data."""
    import json as _json
    from bs4 import BeautifulSoup

    name_slug = re.sub(r'[^a-z\s]', '', name.lower()).strip().replace(' ', '-')
    if not name_slug:
        return None

    city_slug = re.sub(r'[^a-z\s]', '', city.lower()).strip().replace(' ', '-') if city else ""
    state_slug = state.upper().strip() if state else ""

    url = f"https://www.truepeoplesearch.com/find/{name_slug}"
    if city_slug and state_slug:
        url = f"https://www.truepeoplesearch.com/find/{name_slug}/{city_slug}-{state_slug}"

    try:
        scraper = _get_scraper()
        resp = scraper.get(url, timeout=20, headers={
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

        if resp.status_code == 403:
            logger.debug(f"TruePeopleSearch 403 (Cloudflare) for {name}")
            return None
        if resp.status_code != 200:
            logger.debug(f"TruePeopleSearch {resp.status_code} for {name}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Parse JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = _json.loads(script.string or "")
            except (_json.JSONDecodeError, TypeError):
                continue

            if data.get("@type") != "Person":
                continue

            phones_raw = data.get("telephone", [])
            if isinstance(phones_raw, str):
                phones_raw = [phones_raw]
            emails_raw = data.get("email", [])
            if isinstance(emails_raw, str):
                emails_raw = [emails_raw]

            phones = [_clean_phone(p) for p in phones_raw if _clean_phone(p)]
            emails = [e.lower() for e in emails_raw
                      if '@' in e and 'xxx' not in e.lower()]

            if phones or emails:
                return {
                    "phone": phones[0] if phones else "",
                    "phones": phones[:5],
                    "email": emails[0] if emails else "",
                    "emails": emails[:5],
                    "name": data.get("name", ""),
                    "source": "TruePeopleSearch",
                }

        # Fallback: parse HTML directly for phone/email patterns
        phones = []
        emails = []

        for a_tag in soup.find_all("a", href=re.compile(r"^tel:")):
            raw = a_tag.get("href", "").replace("tel:", "")
            cleaned = _clean_phone(raw)
            if cleaned and cleaned not in phones:
                phones.append(cleaned)

        email_pat = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        for match in email_pat.findall(soup.get_text()):
            em = match.lower()
            if em not in emails and 'xxx' not in em and 'example' not in em:
                emails.append(em)

        if phones or emails:
            return {
                "phone": phones[0] if phones else "",
                "phones": phones[:5],
                "email": emails[0] if emails else "",
                "emails": emails[:5],
                "name": "",
                "source": "TruePeopleSearch-html",
            }

        return None

    except Exception as e:
        logger.debug(f"TruePeopleSearch error for {name}: {e}")
        return None


async def search(name: str, city: str = "", state: str = "") -> Optional[dict]:
    """Async name search on TruePeopleSearch."""
    ck = _cache_key(name, city, state)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    await _rate_wait()
    result = await asyncio.to_thread(_scrape_sync, name, city, state)
    if result:
        _cache[ck] = result
        _cache_ts[ck] = time.time()
    return result
