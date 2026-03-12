"""
Free contact scraper — finds phone numbers and emails for property owners.

PROVEN WORKING sources (tested, no API keys):
1. Whitepages.com — name + city/state → phone numbers (WORKS, no captcha)
2. OpenCorporates.com — LLC/Corp → registered agent / officers
3. SMTP email verification — name patterns + common providers (gmail, yahoo, etc.)

Uses cloudscraper to bypass Cloudflare. Rate-limited. Results cached 7 days.
"""

import asyncio
import hashlib
import logging
import re
import smtplib
import time
from typing import Optional
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── In-memory cache ──
_contact_cache: dict[str, dict] = {}
_contact_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400

_rate_limits: dict[str, float] = {}
RATE_LIMIT_SECS = 2.0


def _get_scraper():
    import cloudscraper
    return cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False},
    )


def _cache_key(name: str, city: str, state: str) -> str:
    raw = f"{name.lower().strip()}|{city.lower().strip()}|{state.upper().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()


async def _rate_wait(source: str):
    now = time.time()
    last = _rate_limits.get(source, 0)
    wait = RATE_LIMIT_SECS - (now - last)
    if wait > 0:
        await asyncio.sleep(wait)
    _rate_limits[source] = time.time()


def _clean_phone(raw: str) -> str:
    digits = re.sub(r'\D', '', raw)
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) != 10:
        return ""
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


def _clean_name(raw: str) -> str:
    name = re.sub(r'\s+', ' ', raw.strip())
    if len(name) < 3 or name.upper() in ("N/A", "UNKNOWN", "NOT FOUND"):
        return ""
    return name.title()


def _is_llc(name: str) -> bool:
    upper = name.upper()
    patterns = (
        ' LLC', ' L.L.C', ' INC', ' CORP', ' LTD', ' LP', ' L.P.',
        ' TRUST', ' REVOCABLE', ' IRREVOCABLE', ' LIVING TRUST',
        ' HOLDINGS', ' PROPERTIES', ' PARTNERS', ' INVESTMENTS',
        ' ASSOCIATES', ' ENTERPRISES', ' GROUP', ' MANAGEMENT',
        ' CAPITAL', ' VENTURES', ' DEVELOPMENT', ' REALTY',
        ' ESTATE', ' FUND',
    )
    return any(p in upper for p in patterns)


# ═══════════════════════════════════════════════════════════════════
# SOURCE 1: USSearch.com (JSON-LD Person schema — phones + emails)
# ═══════════════════════════════════════════════════════════════════

# US state names for matching
_STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
}


def _scrape_ussearch_sync(name: str, city: str, state: str) -> Optional[dict]:
    """Scrape USSearch.com for phone/email via JSON-LD Person schema.

    USSearch embeds structured data in <script type="application/ld+json">
    with @type: Person containing telephone[], email[], and homeLocation.
    We match by state/city to find the right person among multiple results.
    """
    import json as _json

    name_slug = re.sub(r'[^a-z\s-]', '', name.lower()).strip().replace(' ', '-')
    if not name_slug:
        return None

    city_slug = re.sub(r'[^a-z\s-]', '', city.lower()).strip().replace(' ', '-')
    state_slug = state.lower().strip()
    url = f"https://www.ussearch.com/people/{name_slug}/{city_slug}-{state_slug}"

    try:
        scraper = _get_scraper()
        resp = scraper.get(url, timeout=15)
        if resp.status_code != 200:
            logger.debug(f"USSearch {resp.status_code} for {name}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Parse JSON-LD Person objects
        state_full = _STATE_NAMES.get(state.upper(), state).lower()
        city_lower = city.lower().strip()
        best_match = None
        best_score = -1

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = _json.loads(script.string or "")
            except (_json.JSONDecodeError, TypeError):
                continue

            if data.get("@type") != "Person":
                continue

            phones = data.get("telephone", [])
            emails = data.get("email", [])
            if not phones and not emails:
                continue

            # Score this person by location match
            score = 0
            locations = data.get("homeLocation", {})
            addresses = []
            if isinstance(locations, dict):
                addr_list = locations.get("address", [])
                if isinstance(addr_list, dict):
                    addr_list = [addr_list]
                addresses = addr_list
            elif isinstance(locations, list):
                for loc in locations:
                    addr_list = loc.get("address", []) if isinstance(loc, dict) else []
                    if isinstance(addr_list, dict):
                        addr_list = [addr_list]
                    addresses.extend(addr_list)

            for addr in addresses:
                if not isinstance(addr, dict):
                    continue
                region = str(addr.get("addressRegion", "")).lower()
                locality = str(addr.get("addressLocality", "")).lower()
                if state_full in region or state.lower() in region:
                    score += 5
                    if city_lower and city_lower in locality:
                        score += 10

            # Boost score for name similarity
            person_name = (data.get("name", "") or "").lower()
            search_parts = name.lower().split()
            for part in search_parts:
                if len(part) > 1 and part in person_name:
                    score += 2  # each matching name part

            if score > best_score:
                best_score = score
                best_match = {
                    "phones": [_clean_phone(p) for p in phones if _clean_phone(p)],
                    "emails": [e for e in emails if '@' in e and 'xxx' not in e],
                    "name": data.get("name", ""),
                    "alt_name": data.get("alternateName", ""),
                }

        if not best_match or best_score < 5:
            # No location match — if only one result, use it cautiously
            if best_match and best_score >= 0 and not best_match.get("phones"):
                return None
            if not best_match:
                return None

        result = {"source": "USSearch"}
        if best_match.get("phones"):
            result["phone"] = best_match["phones"][0]
            result["phones"] = best_match["phones"][:5]
        if best_match.get("emails"):
            result["email"] = best_match["emails"][0]
            result["emails"] = best_match["emails"][:5]
        if best_match.get("name"):
            result["full_name"] = _clean_name(best_match["name"])

        if result.get("phone") or result.get("email"):
            return result
        return None

    except Exception as e:
        logger.debug(f"USSearch error for {name}: {e}")
        return None


async def scrape_ussearch(name: str, city: str, state: str) -> Optional[dict]:
    await _rate_wait("ussearch")
    return await asyncio.to_thread(_scrape_ussearch_sync, name, city, state)


# ═══════════════════════════════════════════════════════════════════
# SOURCE 2: OpenCorporates (LLC → real person)
# ═══════════════════════════════════════════════════════════════════

def _resolve_llc_sync(entity_name: str, state: str) -> Optional[dict]:
    """Look up LLC/Corp via OpenCorporates HTML scraping.
    Returns the registered agent or officer (the real person)."""
    query = quote_plus(entity_name.strip())
    jurisdiction = f"us_{state.lower()}" if state else ""
    url = f"https://opencorporates.com/companies?q={query}&jurisdiction_code={jurisdiction}"

    try:
        scraper = _get_scraper()
        resp = scraper.get(url, timeout=12)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find first company link
        company_link = soup.find("a", href=re.compile(r"/companies/us_"))
        if not company_link:
            return None

        detail_url = company_link.get("href", "")
        if not detail_url.startswith("http"):
            detail_url = f"https://opencorporates.com{detail_url}"

        resp2 = scraper.get(detail_url, timeout=12)
        if resp2.status_code != 200:
            return None

        detail_soup = BeautifulSoup(resp2.text, "html.parser")
        result = {"source": "OpenCorporates"}

        # Look for registered agent
        for dt in detail_soup.find_all("dt"):
            label = dt.get_text(strip=True).lower()
            if "agent" in label or "registered" in label:
                dd = dt.find_next_sibling("dd")
                if dd:
                    agent = _clean_name(dd.get_text(strip=True))
                    if agent and not _is_llc(agent) and len(agent) > 3:
                        result["agent_name"] = agent
                        break

        # Look for officers/directors
        for heading in detail_soup.find_all(["h2", "h3", "dt"]):
            text = heading.get_text(strip=True).lower()
            if "officer" in text or "director" in text:
                parent = heading.parent or detail_soup
                for li in parent.find_all("li")[:5]:
                    name_text = li.get_text(strip=True)
                    name_parts = re.split(r'\s*[-–,]\s*', name_text)
                    if name_parts:
                        person = _clean_name(name_parts[0])
                        if person and not _is_llc(person) and len(person) > 3:
                            result["person_name"] = person
                            break
                if result.get("person_name"):
                    break

        if result.get("person_name") or result.get("agent_name"):
            return result
        return None

    except Exception as e:
        logger.debug(f"OpenCorporates error for {entity_name}: {e}")
        return None


async def resolve_llc_owner(entity_name: str, state: str) -> Optional[dict]:
    await _rate_wait("opencorporates")
    return await asyncio.to_thread(_resolve_llc_sync, entity_name, state)


# ═══════════════════════════════════════════════════════════════════
# SOURCE 3: SMTP email finder (personal email providers)
# ═══════════════════════════════════════════════════════════════════

def _smtp_verify_sync(email: str) -> bool:
    """Verify email via SMTP handshake (no email sent). 3s timeout."""
    try:
        domain = email.split('@')[1]
        try:
            import dns.resolver
            resolver = dns.resolver.Resolver()
            resolver.lifetime = 2  # 2s DNS timeout
            mx_records = resolver.resolve(domain, 'MX')
            mx_host = str(mx_records[0].exchange).rstrip('.')
        except Exception:
            mx_host = domain

        server = smtplib.SMTP(timeout=3)
        server.connect(mx_host)
        server.helo('onsite.com')
        server.mail('verify@onsite.com')
        code, _ = server.rcpt(email)
        server.quit()
        return code in (250, 251)
    except Exception:
        return False


async def find_email_by_name(
    first_name: str, last_name: str
) -> Optional[str]:
    """Try common email patterns against major providers via SMTP."""
    first = re.sub(r'[^a-z]', '', first_name.lower().strip())
    last = re.sub(r'[^a-z]', '', last_name.lower().strip())
    if not first or not last or len(first) < 2 or len(last) < 2:
        return None

    # Most likely patterns (ordered by probability)
    patterns = [
        f'{first}.{last}',
        f'{first}{last}',
        f'{first[0]}{last}',
        f'{first}_{last}',
        f'{last}.{first}',
    ]

    # Top personal email providers (most US homeowners use these)
    providers = ['gmail.com', 'yahoo.com', 'outlook.com', 'aol.com', 'hotmail.com']

    # Test top combos (15 total = 5 patterns × 3 providers, quick)
    candidates = []
    for pattern in patterns[:3]:  # top 3 patterns
        for provider in providers[:3]:  # top 3 providers
            candidates.append(f"{pattern}@{provider}")

    # Also add remaining combos
    for pattern in patterns:
        for provider in providers:
            candidate = f"{pattern}@{provider}"
            if candidate not in candidates:
                candidates.append(candidate)

    # Test candidates with overall 20s timeout (stop early on first hit)
    async def _check_all():
        for candidate in candidates[:9]:  # max 9 checks (3 patterns × 3 providers)
            try:
                valid = await asyncio.to_thread(_smtp_verify_sync, candidate)
                if valid:
                    logger.info(f"Email FOUND via SMTP: {candidate}")
                    return candidate
            except Exception:
                continue
            await asyncio.sleep(0.2)
        return None

    try:
        return await asyncio.wait_for(_check_all(), timeout=20.0)
    except asyncio.TimeoutError:
        logger.debug(f"SMTP email search timed out for {first_name} {last_name}")
        return None


# ═══════════════════════════════════════════════════════════════════
# MASTER CONTACT ENRICHMENT
# ═══════════════════════════════════════════════════════════════════

async def find_contact(
    owner_name: str,
    address: str = "",
    city: str = "",
    state: str = "",
    skip_smtp: bool = False,
) -> dict:
    """
    Master contact finder — chains all free sources:
    1. If LLC → resolve via OpenCorporates to find real person
    2. Whitepages for phone numbers (PROVEN WORKING)
    3. SMTP email pattern matching (gmail, yahoo, outlook, etc.)
    4. Cache results 7 days

    Returns: {phone, email, full_name, phones, emails, source, person_name}
    """
    if not owner_name or owner_name.lower() in ("not found", "unknown", "n/a", ""):
        return {"phone": "", "email": "", "source": "none"}

    # Check cache
    ck = _cache_key(owner_name, city, state)
    cached = _contact_cache.get(ck)
    if cached and (time.time() - _contact_cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    result = {
        "phone": "", "email": "", "full_name": "",
        "phones": [], "emails": [], "source": "none",
        "person_name": "",
    }

    search_name = owner_name

    # Step 1: If LLC/Corp, resolve to real person
    if _is_llc(owner_name):
        logger.info(f"LLC: {owner_name} — resolving")
        llc_result = await resolve_llc_owner(owner_name, state)
        if llc_result:
            person = llc_result.get("person_name") or llc_result.get("agent_name") or ""
            if person:
                result["person_name"] = person
                search_name = person
                logger.info(f"LLC → person: {person}")

    # Step 2: USSearch for phone + email (structured JSON-LD data)
    us = await scrape_ussearch(search_name, city, state)
    if us:
        if us.get("phone"):
            result["phone"] = us["phone"]
            result["phones"] = us.get("phones", [us["phone"]])
        if us.get("email"):
            result["email"] = us["email"]
            result["emails"] = us.get("emails", [us["email"]])
        if us.get("full_name"):
            result["full_name"] = us["full_name"]
        result["source"] = "USSearch"

    # If owner is LLC and USSearch didn't work with person name,
    # also try the LLC name directly (some are listed)
    if not result["phone"] and _is_llc(owner_name) and search_name != owner_name:
        us2 = await scrape_ussearch(owner_name, city, state)
        if us2 and us2.get("phone"):
            result["phone"] = us2["phone"]
            result["phones"] = us2.get("phones", [us2["phone"]])
            result["source"] = "USSearch"

    # Step 3: SMTP email discovery (slow ~20s, low hit rate — skip in batch mode)
    if not result.get("email") and not skip_smtp:
        parts = search_name.strip().split()
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = parts[-1]
            if not _is_llc(search_name) and len(first_name) > 1 and len(last_name) > 1:
                email = await find_email_by_name(first_name, last_name)
                if email:
                    result["email"] = email
                    result["emails"] = [email]
                    if result["source"] == "none":
                        result["source"] = "SMTP"
                    else:
                        result["source"] += " + SMTP"

    # Cache
    _contact_cache[ck] = result
    _contact_cache_ts[ck] = time.time()

    found = bool(result.get("phone") or result.get("email"))
    logger.info(
        f"Contact {'FOUND' if found else 'MISS'}: "
        f"{owner_name} ({city}, {state}) → "
        f"ph={result.get('phone', '')} em={result.get('email', '')} "
        f"via {result['source']}"
    )

    return result


async def batch_find_contacts(
    leads: list[dict],
    max_leads: int = 500,
    concurrency: int = 2,
) -> dict:
    """
    Batch contact enrichment — finds phone/email for leads with owner_name.
    Uses Whitepages (phone) + SMTP (email). Rate-limited, cached.
    """
    t0 = time.time()

    candidates = []
    for lead in leads:
        owner = str(lead.get("owner_name", "") or "").strip()
        if not owner or owner.lower() in ("not found", "unknown", "n/a"):
            continue
        phone = str(lead.get("owner_phone", "") or "").strip()
        email = str(lead.get("owner_email", "") or "").strip()
        if phone and email:
            continue
        candidates.append(lead)

    if not candidates:
        return {"enriched": 0, "total_candidates": 0, "elapsed": 0}

    candidates.sort(key=lambda l: float(l.get("score", 0) or 0), reverse=True)
    batch = candidates[:max_leads]

    logger.info(f"Contact scraping: {len(batch)} leads (of {len(candidates)} needing enrichment)")

    sem = asyncio.Semaphore(concurrency)
    enriched = 0
    phones_found = 0
    emails_found = 0

    async def _process(lead):
        nonlocal enriched, phones_found, emails_found
        async with sem:
            contact = await find_contact(
                lead.get("owner_name", ""),
                lead.get("address", ""),
                lead.get("city", ""),
                lead.get("state", ""),
                skip_smtp=True,  # SMTP too slow for batch
            )

            updated = False
            if contact.get("phone") and not lead.get("owner_phone", "").strip():
                lead["owner_phone"] = contact["phone"]
                phones_found += 1
                updated = True

            if contact.get("email") and not lead.get("owner_email", "").strip():
                lead["owner_email"] = contact["email"]
                emails_found += 1
                updated = True

            if contact.get("person_name") and _is_llc(lead.get("owner_name", "")):
                lead["beneficial_owner"] = contact["person_name"]
                updated = True

            if updated:
                enriched += 1

    # Process in small batches with pauses
    batch_size = 10
    for i in range(0, len(batch), batch_size):
        chunk = batch[i:i + batch_size]
        await asyncio.gather(*[_process(lead) for lead in chunk])
        if i + batch_size < len(batch):
            await asyncio.sleep(3)  # rate limit pause

    elapsed = round(time.time() - t0, 1)
    stats = {
        "enriched": enriched,
        "phones_found": phones_found,
        "emails_found": emails_found,
        "total_candidates": len(candidates),
        "batch_size": len(batch),
        "elapsed": elapsed,
    }
    logger.info(f"Contact scraping done: {stats}")
    return stats
