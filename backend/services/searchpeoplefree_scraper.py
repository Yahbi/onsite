"""
SearchPeopleFree.com scraper — phone, email, and name lookup.

Uses cloudscraper for Cloudflare bypass. Results cached 7 days in-memory.
Rate-limited to 1 request per 2 seconds.

URL patterns:
  Name:    https://www.searchpeoplefree.com/find/{first-last}/{state}/{city}
  Address: https://www.searchpeoplefree.com/address/{address}/{city}/{state}
"""

import asyncio
import hashlib
import json
import logging
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

# ── In-memory cache (TTL = 7 days) ──
_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
_CACHE_TTL = 7 * 86400

_last_request: float = 0.0
_RATE_LIMIT = 2.0

_PHONE_RE = re.compile(
    r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
)
_EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
)
_JUNK_EMAILS = frozenset({
    "support@searchpeoplefree.com",
    "info@searchpeoplefree.com",
    "privacy@searchpeoplefree.com",
    "abuse@searchpeoplefree.com",
    "noreply@searchpeoplefree.com",
})


def _get_scraper():
    import cloudscraper
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False},
    )


def _cache_key(*parts: str) -> str:
    raw = "|".join(p.lower().strip() for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


def _check_cache(key: str) -> Optional[dict]:
    ts = _cache_ts.get(key)
    if ts and (time.time() - ts) < _CACHE_TTL:
        return _cache[key]
    return None


def _store_cache(key: str, value: Optional[dict]) -> None:
    _cache[key] = value
    _cache_ts[key] = time.time()


async def _rate_wait():
    global _last_request
    now = time.time()
    wait = _RATE_LIMIT - (now - _last_request)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_request = time.time()


def _clean_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


def _slug(text: str) -> str:
    return re.sub(r'[^a-z0-9-]', '', text.lower().strip().replace(' ', '-'))


def _extract_jsonld(html: str) -> Optional[dict]:
    """Extract Person data from JSON-LD structured data."""
    for match in re.finditer(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    ):
        try:
            data = json.loads(match.group(1))
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict) and item.get("@type") == "Person":
                    return item
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _parse_result(html: str) -> Optional[dict]:
    """Extract phone, email, and name from page HTML."""
    phones: list[str] = []
    emails: list[str] = []
    name = ""

    # Try JSON-LD first
    person = _extract_jsonld(html)
    if person:
        name = person.get("name", "")
        for tel in (person.get("telephone") or []):
            cleaned = _clean_phone(str(tel))
            if cleaned:
                phones.append(cleaned)
        for em in (person.get("email") or []):
            if em.lower() not in _JUNK_EMAILS:
                emails.append(em.lower())

    # Regex fallback / supplement
    for raw_phone in _PHONE_RE.findall(html):
        cleaned = _clean_phone(raw_phone)
        if cleaned and cleaned not in phones:
            phones.append(cleaned)

    for raw_email in _EMAIL_RE.findall(html):
        em = raw_email.lower()
        if em not in _JUNK_EMAILS and em not in emails and "searchpeoplefree" not in em:
            emails.append(em)

    if not phones and not emails:
        return None

    return {
        "phone": phones[0] if phones else None,
        "phones": phones,
        "email": emails[0] if emails else None,
        "emails": emails,
        "name": name or None,
        "source": "SearchPeopleFree",
    }


def _fetch_sync(url: str) -> Optional[str]:
    """Fetch a URL with cloudscraper. Returns HTML or None."""
    try:
        scraper = _get_scraper()
        resp = scraper.get(url, timeout=15)
        if resp.status_code != 200:
            logger.debug("SearchPeopleFree %d for %s", resp.status_code, url)
            return None
        return resp.text
    except Exception as exc:
        logger.warning("SearchPeopleFree fetch error: %s", exc)
        return None


async def search(name: str, city: str, state: str) -> Optional[dict]:
    """Search by person name + city/state. Returns contact dict or None."""
    if not name or not state:
        return None

    key = _cache_key(name, city, state)
    cached = _check_cache(key)
    if cached is not None:
        return cached if cached else None

    name_slug = _slug(name)
    state_slug = _slug(state)
    city_slug = _slug(city) if city else ""
    if not name_slug:
        return None

    url = f"https://www.searchpeoplefree.com/find/{name_slug}/{state_slug}/{city_slug}"

    await _rate_wait()
    html = await asyncio.to_thread(_fetch_sync, url)
    if not html:
        _store_cache(key, {})
        return None

    result = _parse_result(html)
    _store_cache(key, result or {})
    return result


async def reverse_address(address: str, city: str, state: str) -> Optional[dict]:
    """Reverse address lookup. Returns contact dict or None."""
    if not address or not city or not state:
        return None

    key = _cache_key("addr", address, city, state)
    cached = _check_cache(key)
    if cached is not None:
        return cached if cached else None

    addr_slug = _slug(address)
    city_slug = _slug(city)
    state_slug = _slug(state)
    if not addr_slug:
        return None

    url = f"https://www.searchpeoplefree.com/address/{addr_slug}/{city_slug}/{state_slug}"

    await _rate_wait()
    html = await asyncio.to_thread(_fetch_sync, url)
    if not html:
        _store_cache(key, {})
        return None

    result = _parse_result(html)
    _store_cache(key, result or {})
    return result
