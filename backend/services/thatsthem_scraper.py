"""
ThatsThem.com scraper — reverse address + name lookup for phone & email.

HIGHEST-VALUE free source:
- Reverse address lookup: property address → owner phone + email
- Name search: owner name + city/state → phone + email
- Emails are NOT obfuscated (unlike USSearch)
- 2.2B records, 1B phones, 1.7B emails

Uses cloudscraper for Cloudflare bypass. Rate-limited at 1.5s between calls.
Results cached 7 days in memory.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400
_last_call = 0.0
RATE_LIMIT = 1.5


def _get_scraper():
    import cloudscraper
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False},
    )


def _cache_key(prefix: str, *parts: str) -> str:
    raw = f"{prefix}|{'|'.join(p.lower().strip() for p in parts)}"
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


def _extract_contact_from_html(html: str) -> dict:
    """Parse ThatsThem result page for phone numbers and emails."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    phones = []
    emails = []
    name = ""

    # Name from h2.person-name or similar
    name_el = soup.find("h2", class_=re.compile(r"name|person", re.I))
    if not name_el:
        name_el = soup.find("span", class_=re.compile(r"name|person", re.I))
    if name_el:
        name = name_el.get_text(strip=True)

    # Phone numbers — look for tel: links and phone patterns
    for a_tag in soup.find_all("a", href=re.compile(r"^tel:")):
        raw = a_tag.get("href", "").replace("tel:", "").strip()
        cleaned = _clean_phone(raw)
        if cleaned and cleaned not in phones:
            phones.append(cleaned)

    # Also scan for phone-number spans/divs
    for el in soup.find_all(["span", "div", "a"], class_=re.compile(r"phone|tel", re.I)):
        raw = el.get_text(strip=True)
        cleaned = _clean_phone(raw)
        if cleaned and cleaned not in phones:
            phones.append(cleaned)

    # Broader phone regex scan in text
    phone_pattern = re.compile(r'[\(\d][\d\s\(\)\-\.]{8,14}\d')
    for match in phone_pattern.findall(soup.get_text()):
        cleaned = _clean_phone(match)
        if cleaned and cleaned not in phones:
            phones.append(cleaned)

    # Email addresses — mailto: links
    for a_tag in soup.find_all("a", href=re.compile(r"^mailto:")):
        raw = a_tag.get("href", "").replace("mailto:", "").strip().lower()
        if '@' in raw and 'xxx' not in raw and raw not in emails:
            emails.append(raw)

    # Email from text content
    email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    for match in email_pattern.findall(soup.get_text()):
        email = match.lower()
        if email not in emails and 'xxx' not in email and 'example' not in email:
            emails.append(email)

    return {
        "name": name,
        "phones": phones[:5],
        "emails": emails[:5],
        "phone": phones[0] if phones else "",
        "email": emails[0] if emails else "",
    }


def _reverse_address_sync(address: str, city: str, state: str) -> Optional[dict]:
    """Reverse address lookup on ThatsThem — property address → owner contact."""
    addr_parts = []
    if address:
        addr_parts.append(address.strip())
    if city:
        addr_parts.append(city.strip())
    if state:
        addr_parts.append(state.strip())
    if not addr_parts:
        return None

    full_addr = ", ".join(addr_parts)
    slug = re.sub(r'[^a-z0-9]+', '-', full_addr.lower()).strip('-')
    url = f"https://thatsthem.com/address/{slug}"

    try:
        scraper = _get_scraper()
        resp = scraper.get(url, timeout=15, headers={
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://thatsthem.com/",
        })
        if resp.status_code != 200:
            logger.debug(f"ThatsThem address {resp.status_code} for {full_addr}")
            return None

        result = _extract_contact_from_html(resp.text)
        if result.get("phone") or result.get("email"):
            result["source"] = "ThatsThem-address"
            return result
        return None

    except Exception as e:
        logger.debug(f"ThatsThem address error: {e}")
        return None


def _name_search_sync(name: str, city: str, state: str) -> Optional[dict]:
    """Name search on ThatsThem — owner name + location → contact info."""
    if not name or len(name.strip()) < 3:
        return None

    name_slug = re.sub(r'[^a-z\s-]', '', name.lower()).strip().replace(' ', '-')
    city_slug = re.sub(r'[^a-z\s-]', '', city.lower()).strip().replace(' ', '-') if city else ""
    state_slug = state.lower().strip() if state else ""

    url = f"https://thatsthem.com/name/{quote_plus(name)}"
    if city_slug and state_slug:
        url = f"https://thatsthem.com/name/{name_slug}/{city_slug}-{state_slug}"

    try:
        scraper = _get_scraper()
        resp = scraper.get(url, timeout=15, headers={
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://thatsthem.com/",
        })
        if resp.status_code != 200:
            logger.debug(f"ThatsThem name {resp.status_code} for {name}")
            return None

        result = _extract_contact_from_html(resp.text)
        if result.get("phone") or result.get("email"):
            result["source"] = "ThatsThem-name"
            return result
        return None

    except Exception as e:
        logger.debug(f"ThatsThem name error: {e}")
        return None


async def reverse_address_lookup(
    address: str, city: str = "", state: str = ""
) -> Optional[dict]:
    """Async reverse address lookup — best for permit leads (always have address)."""
    ck = _cache_key("addr", address, city, state)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    await _rate_wait()
    result = await asyncio.to_thread(_reverse_address_sync, address, city, state)
    if result:
        _cache[ck] = result
        _cache_ts[ck] = time.time()
    return result


async def name_search(
    name: str, city: str = "", state: str = ""
) -> Optional[dict]:
    """Async name search — fallback when address lookup fails."""
    ck = _cache_key("name", name, city, state)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    await _rate_wait()
    result = await asyncio.to_thread(_name_search_sync, name, city, state)
    if result:
        _cache[ck] = result
        _cache_ts[ck] = time.time()
    return result


async def lookup(
    address: str = "",
    name: str = "",
    city: str = "",
    state: str = "",
) -> Optional[dict]:
    """Combined lookup: tries reverse address first, then name search."""
    if address:
        result = await reverse_address_lookup(address, city, state)
        if result and (result.get("phone") or result.get("email")):
            return result

    if name:
        result = await name_search(name, city, state)
        if result and (result.get("phone") or result.get("email")):
            return result

    return None
