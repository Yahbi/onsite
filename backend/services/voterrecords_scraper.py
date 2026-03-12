"""
VoterRecords.com scraper — public voter registration data with phone + email.

Hidden gem for contact enrichment:
- 100M+ voter records across the US
- Best states for phone+email: FL, CO, WA, NC, OH, OR, PA
- Data: name, address, phone, email, party, DOB
- 100% free, no login required

Rate-limited at 2s. Cached 14 days (voter data changes slowly).
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
CACHE_TTL = 14 * 86400
_last_call = 0.0
RATE_LIMIT = 2.0

# States known to have phone/email in voter files
RICH_STATES = {"FL", "CO", "WA", "NC", "OH", "OR", "PA", "OK", "AR", "CT"}


def _get_scraper():
    import cloudscraper
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False},
    )


def _cache_key(name: str, city: str, state: str) -> str:
    raw = f"voter|{name.lower().strip()}|{city.lower().strip()}|{state.upper().strip()}"
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
    """Scrape VoterRecords.com for contact data."""
    from bs4 import BeautifulSoup

    parts = name.strip().split()
    if len(parts) < 2:
        return None

    first = parts[0].lower()
    last = parts[-1].lower()

    # Build search URL
    url = f"https://voterrecords.com/voters/{first}-{last}"
    if state:
        url += f"/{state.lower()}"

    try:
        scraper = _get_scraper()
        resp = scraper.get(url, timeout=15, headers={
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

        if resp.status_code != 200:
            logger.debug(f"VoterRecords {resp.status_code} for {name}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        phones = []
        emails = []
        matched_name = ""

        # Look for voter cards matching city
        city_lower = city.lower().strip() if city else ""
        cards = soup.find_all(["div", "tr", "li"], class_=re.compile(r"voter|result|record", re.I))
        if not cards:
            cards = soup.find_all("div", class_=re.compile(r"card|item|entry", re.I))

        target_card = None
        for card in cards:
            text = card.get_text().lower()
            if city_lower and city_lower in text:
                target_card = card
                break
        if not target_card and cards:
            target_card = cards[0]

        search_area = target_card or soup

        # Extract phone numbers
        for a_tag in search_area.find_all("a", href=re.compile(r"^tel:")):
            raw = a_tag.get("href", "").replace("tel:", "")
            cleaned = _clean_phone(raw)
            if cleaned and cleaned not in phones:
                phones.append(cleaned)

        phone_pattern = re.compile(r'[\(\d][\d\s\(\)\-\.]{8,14}\d')
        for match in phone_pattern.findall(search_area.get_text()):
            cleaned = _clean_phone(match)
            if cleaned and cleaned not in phones:
                phones.append(cleaned)

        # Extract emails
        email_pat = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        for match in email_pat.findall(search_area.get_text()):
            em = match.lower()
            if em not in emails and 'xxx' not in em and 'voterrecords' not in em:
                emails.append(em)

        for a_tag in search_area.find_all("a", href=re.compile(r"^mailto:")):
            raw = a_tag.get("href", "").replace("mailto:", "").strip().lower()
            if '@' in raw and raw not in emails and 'voterrecords' not in raw:
                emails.append(raw)

        if phones or emails:
            return {
                "phone": phones[0] if phones else "",
                "phones": phones[:3],
                "email": emails[0] if emails else "",
                "emails": emails[:3],
                "name": matched_name,
                "source": "VoterRecords",
            }

        return None

    except Exception as e:
        logger.debug(f"VoterRecords error for {name}: {e}")
        return None


async def search(name: str, city: str = "", state: str = "") -> Optional[dict]:
    """Async voter records lookup. Best for FL, CO, WA, NC, OH, OR, PA."""
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
