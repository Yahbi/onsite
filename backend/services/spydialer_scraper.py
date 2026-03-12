"""
SpyDialer scraper — free reverse phone/name/address lookup.

Uses cloudscraper for Cloudflare bypass. Rate-limited. Results cached 7 days.
Supports name search and reverse address lookup.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

# ── In-memory cache with TTL ──
_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400

_last_request: float = 0.0
RATE_LIMIT_SECS = 2.0

_BASE = "https://www.spydialer.com"

# ── Regex patterns ──
_PHONE_RE = re.compile(
    r'(?<!\d)'
    r'[\(]?(\d{3})[\)\s.\-]*(\d{3})[\s.\-]*(\d{4})'
    r'(?!\d)'
)
_EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)
_JUNK_EMAILS = frozenset({
    "support@spydialer.com", "info@spydialer.com",
    "privacy@spydialer.com", "abuse@spydialer.com",
    "noreply@spydialer.com", "contact@spydialer.com",
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
    ts = _cache_ts.get(key, 0)
    if time.time() - ts < CACHE_TTL and key in _cache:
        return _cache[key]
    return None


def _store_cache(key: str, result: dict) -> None:
    _cache[key] = result
    _cache_ts[key] = time.time()


async def _rate_wait():
    global _last_request
    now = time.time()
    wait = RATE_LIMIT_SECS - (now - _last_request)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_request = time.time()


def _clean_phone(digits_tuple: tuple[str, str, str]) -> str:
    area, mid, last = digits_tuple
    return f"({area}) {mid}-{last}"


def _extract_phones(html: str) -> list[str]:
    seen = set()
    phones = []
    for match in _PHONE_RE.finditer(html):
        formatted = _clean_phone(match.groups())
        digits = re.sub(r'\D', '', formatted)
        # skip toll-free / 555 / obviously fake
        if digits[:3] in ("800", "888", "877", "866", "855", "844", "833"):
            continue
        if digits[3:6] == "555":
            continue
        if digits not in seen:
            seen.add(digits)
            phones.append(formatted)
    return phones


def _extract_emails(html: str) -> list[str]:
    seen = set()
    emails = []
    for match in _EMAIL_RE.finditer(html):
        email = match.group(0).lower()
        if email in _JUNK_EMAILS:
            continue
        if "spydialer" in email:
            continue
        if email not in seen:
            seen.add(email)
            emails.append(email)
    return emails


def _extract_name(html: str) -> str:
    """Try to pull a full name from the results page."""
    # Look for common heading patterns
    name_pattern = re.compile(
        r'<h[12][^>]*class="[^"]*name[^"]*"[^>]*>([^<]+)</h[12]>',
        re.IGNORECASE,
    )
    m = name_pattern.search(html)
    if m:
        name = re.sub(r'\s+', ' ', m.group(1).strip())
        if 3 <= len(name) <= 80:
            return name.title()
    return ""


def _build_result(html: str) -> Optional[dict]:
    phones = _extract_phones(html)
    emails = _extract_emails(html)
    name = _extract_name(html)

    if not phones and not emails and not name:
        return None

    return {
        "phone": phones[0] if phones else None,
        "phones": phones,
        "email": emails[0] if emails else None,
        "emails": emails,
        "name": name or None,
        "source": "SpyDialer",
    }


def _sync_get(url: str) -> Optional[str]:
    scraper = _get_scraper()
    try:
        resp = scraper.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning("SpyDialer returned %s for %s", resp.status_code, url)
            return None
        return resp.text
    except Exception as exc:
        logger.error("SpyDialer request failed: %s", exc)
        return None


async def search_by_name(
    name: str, city: str, state: str
) -> Optional[dict]:
    """Search SpyDialer by person name + location."""
    if not name or not state:
        return None

    key = _cache_key("name", name, city, state)
    cached = _check_cache(key)
    if cached is not None:
        return cached

    await _rate_wait()

    url = (
        f"{_BASE}/results.aspx"
        f"?name={quote_plus(name)}"
        f"&city={quote_plus(city)}"
        f"&state={quote_plus(state)}"
    )
    html = await asyncio.to_thread(_sync_get, url)
    if html is None:
        return None

    result = _build_result(html)
    if result is not None:
        _store_cache(key, result)
    return result


async def reverse_address(
    address: str, city: str, state: str
) -> Optional[dict]:
    """Reverse address lookup on SpyDialer."""
    if not address or not state:
        return None

    key = _cache_key("addr", address, city, state)
    cached = _check_cache(key)
    if cached is not None:
        return cached

    await _rate_wait()

    url = (
        f"{_BASE}/results.aspx"
        f"?address={quote_plus(address)}"
        f"&city={quote_plus(city)}"
        f"&state={quote_plus(state)}"
    )
    html = await asyncio.to_thread(_sync_get, url)
    if html is None:
        return None

    result = _build_result(html)
    if result is not None:
        _store_cache(key, result)
    return result
