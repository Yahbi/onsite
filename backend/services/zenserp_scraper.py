"""
Zenserp Google SERP API — searches owner names to find phone + email from results.

Uses Google search to find contact info from people-search sites, social profiles,
business directories, etc. Extracts phone numbers and emails from:
- Search result snippets (descriptions)
- Page titles
- Rich result structured data

Rate: 100 searches/month on current plan.
Cached 7 days per lookup to conserve quota.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

API_KEY = "39db36b0-1cd4-11f1-9d1d-930ea230f5a5"
BASE_URL = "https://app.zenserp.com/api/v2/search"

_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400
_last_call = 0.0
RATE_LIMIT = 1.0  # 1s between calls to be safe


def _cache_key(query: str) -> str:
    return hashlib.md5(f"zenserp|{query.lower().strip()}".encode()).hexdigest()


def _clean_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


def _extract_contacts_from_text(text: str) -> dict:
    """Extract phone numbers and emails from a block of text."""
    phones = []
    emails = []

    # Phone patterns
    phone_pat = re.compile(
        r'[\(]?\d{3}[\)\s.\-]*\d{3}[\s.\-]*\d{4}'
    )
    for match in phone_pat.findall(text):
        cleaned = _clean_phone(match)
        if cleaned and cleaned not in phones:
            phones.append(cleaned)

    # Email patterns
    email_pat = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    for match in email_pat.findall(text):
        em = match.lower()
        # Skip junk emails
        if any(skip in em for skip in [
            'example.com', 'xxx', 'noreply', 'support@', 'info@',
            'privacy@', 'abuse@', 'admin@', 'webmaster@',
            'zenserp', 'google', 'facebook', 'twitter',
        ]):
            continue
        if em not in emails:
            emails.append(em)

    return {"phones": phones, "emails": emails}


async def _rate_wait():
    global _last_call
    now = time.time()
    wait = RATE_LIMIT - (now - _last_call)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_call = time.time()


async def search_owner(
    name: str,
    city: str = "",
    state: str = "",
    address: str = "",
) -> Optional[dict]:
    """
    Google search for owner contact info via Zenserp API.

    Tries two queries:
    1. "{name} {city} {state} phone email" — broad contact search
    2. "{name} {address} {city} {state}" — address-specific search

    Returns dict with phone, phones, email, emails, source fields.
    """
    if not name or not API_KEY:
        return None

    # Build primary query
    parts = [name.strip()]
    if city:
        parts.append(city.strip())
    if state:
        parts.append(state.strip())
    query = " ".join(parts) + " phone email"

    ck = _cache_key(query)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    await _rate_wait()

    all_phones = []
    all_emails = []

    # Query 1: Name + location + "phone email"
    try:
        result = await _do_search(query)
        if result:
            for p in result.get("phones", []):
                if p not in all_phones:
                    all_phones.append(p)
            for e in result.get("emails", []):
                if e not in all_emails:
                    all_emails.append(e)
    except Exception as e:
        logger.debug(f"Zenserp query 1 error: {e}")

    # Query 2: Name + address (if we have it and query 1 didn't find both)
    if address and (not all_phones or not all_emails):
        addr_query = f"{name.strip()} {address.strip()}"
        if city:
            addr_query += f" {city.strip()}"
        if state:
            addr_query += f" {state.strip()}"

        await _rate_wait()
        try:
            result2 = await _do_search(addr_query)
            if result2:
                for p in result2.get("phones", []):
                    if p not in all_phones:
                        all_phones.append(p)
                for e in result2.get("emails", []):
                    if e not in all_emails:
                        all_emails.append(e)
        except Exception as e:
            logger.debug(f"Zenserp query 2 error: {e}")

    if not all_phones and not all_emails:
        return None

    out = {
        "phone": all_phones[0] if all_phones else "",
        "phones": all_phones[:5],
        "email": all_emails[0] if all_emails else "",
        "emails": all_emails[:5],
        "name": name,
        "source": "Zenserp-Google",
    }

    _cache[ck] = out
    _cache_ts[ck] = time.time()
    return out


async def _do_search(query: str) -> Optional[dict]:
    """Execute a single Zenserp search and extract contacts from results."""
    params = {
        "q": query,
        "location": "United States",
        "search_engine": "google.com",
        "num": "10",
    }
    headers = {"apikey": API_KEY}

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(BASE_URL, params=params, headers=headers)

    if resp.status_code == 401:
        logger.warning("Zenserp API key invalid or expired")
        return None
    if resp.status_code == 429:
        logger.warning("Zenserp rate limit hit")
        return None
    if resp.status_code != 200:
        logger.debug(f"Zenserp {resp.status_code}: {resp.text[:200]}")
        return None

    data = resp.json()

    # Combine all text from search results
    all_text = ""
    for result in data.get("organic", []):
        title = result.get("title", "")
        desc = result.get("description", "")
        url = result.get("url", "")
        all_text += f" {title} {desc} {url}"

    # Also check knowledge graph if present
    kg = data.get("knowledge_graph", {})
    if kg:
        all_text += f" {kg.get('description', '')} {kg.get('phone', '')} {kg.get('email', '')}"
        # Direct phone from knowledge graph
        if kg.get("phone"):
            all_text += f" {kg['phone']}"

    # Check answer box
    ab = data.get("answer_box", {})
    if ab:
        all_text += f" {ab.get('answer', '')} {ab.get('description', '')}"

    return _extract_contacts_from_text(all_text)


async def check_quota() -> dict:
    """Check remaining Zenserp API quota."""
    headers = {"apikey": API_KEY}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            "https://app.zenserp.com/api/v2/status",
            headers=headers,
        )
    if resp.status_code == 200:
        return resp.json()
    return {"error": resp.status_code}
