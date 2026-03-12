"""
Universal Socrata API fetcher.
Handles all 2,694 Socrata data sources with automatic field discovery.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

from backend.services.fetchers.field_mapper import (
    clean_valuation,
    map_record,
    parse_date,
)

logger = logging.getLogger(__name__)


def parse_socrata_url(url: str) -> tuple:
    """
    Extract domain and resource_id from a Socrata URL.
    Patterns:
      https://data.lacity.org/resource/pi9x-tg5x.json
      https://data.lacity.org/resource/pi9x-tg5x
      https://data.lacity.org/api/views/pi9x-tg5x
    Returns (domain, resource_id) or ("", "").
    """
    m = re.match(r"https?://([^/]+)/resource/([^/.?]+)", url)
    if m:
        return (m.group(1), m.group(2))
    m = re.match(r"https?://([^/]+)/api/views/([^/.?]+)", url)
    if m:
        return (m.group(1), m.group(2))
    return ("", "")


async def fetch_socrata(
    source: dict,
    days: int = 90,
    limit: int = 500,
    timeout_sec: int = 30,
) -> List[dict]:
    """
    Fetch records from a Socrata source.
    source can be:
      - Registry format: {api_url, domain, resource_id, ...}
      - Legacy format: {domain, rid, date_field, ...}
    Returns list of raw records.
    """
    # Extract domain + resource_id
    domain = source.get("domain", "")
    rid = source.get("resource_id") or source.get("rid", "")

    if not domain or not rid:
        url = source.get("api_url", "")
        if url:
            domain, rid = parse_socrata_url(url)
    if not domain or not rid:
        logger.warning(f"Socrata: cannot parse URL for {source.get('location', 'unknown')}")
        return []

    # Build the API URL
    api_url = f"https://{domain}/resource/{rid}.json"

    # Try to find a date field for filtering
    date_field = source.get("date_field", "")
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    params: Dict[str, str] = {"$limit": str(limit)}

    if date_field:
        params["$order"] = f"{date_field} DESC"
        params["$where"] = f"{date_field} >= '{since}'"
    else:
        # No date field known — just get latest records
        params["$order"] = ":id DESC"

    label = source.get("location") or source.get("city") or domain

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                api_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_sec),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, list):
                        logger.info(f"Socrata {label}: {len(data)} records")
                        return data
                    return []
                elif resp.status == 403:
                    raise PermissionError(f"Socrata {label} HTTP 403 Forbidden")
                elif resp.status == 404:
                    raise FileNotFoundError(f"Socrata {label} HTTP 404 Not Found")
                else:
                    raise RuntimeError(f"Socrata {label} HTTP {resp.status}")
    except (PermissionError, FileNotFoundError, RuntimeError):
        raise
    except Exception as e:
        raise RuntimeError(f"Socrata {label}: {e}") from e


async def probe_socrata_fields(domain: str, resource_id: str) -> List[str]:
    """
    Probe a Socrata dataset to discover field names.
    Fetches 1 record with $limit=1 and returns the field names.
    """
    url = f"https://{domain}/resource/{resource_id}.json"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params={"$limit": "1"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and isinstance(data, list) and len(data) > 0:
                        return list(data[0].keys())
                return []
    except Exception:
        return []


def normalize_socrata_record(
    raw: dict,
    source: dict,
    calculate_score_fn=None,
) -> Optional[dict]:
    """
    Normalize a raw Socrata record into the standard lead format.
    Uses the universal field mapper.
    source: registry row with location, state, etc.
    calculate_score_fn: scoring function (days_old, valuation, permit_type) -> (score, temp, urgency)
    """
    mapped = map_record(raw, source_type="socrata")

    addr = str(mapped.get("address", "")).strip()
    if not addr:
        return None

    city = str(mapped.get("city", "")).strip()
    state = str(mapped.get("state") or source.get("state", "")).strip()

    # Fall back to source location if city is missing or dirty
    if not city or len(city) <= 1 or city.lower() in ("unknown", "n/a", "none"):
        city = (source.get("location") or source.get("city") or "").split(" (")[0]

    zipcode = str(mapped.get("zip", ""))[:5]

    valuation = clean_valuation(mapped.get("valuation"))

    permit_number = str(mapped.get("permit_number", ""))
    permit_type = str(mapped.get("permit_type", ""))
    work_desc = str(mapped.get("work_description", ""))

    # Parse date
    date_field_name = source.get("date_field", "")
    raw_date = raw.get(date_field_name) if date_field_name else mapped.get("issue_date")
    issue_date, days_old = parse_date(raw_date)

    # Lat/lng
    lat = _safe_float(mapped.get("lat"))
    lng = _safe_float(mapped.get("lng"))

    # Fallback geocode: hash-based scatter around source center
    if not lat or not lng or lat == 0 or lng == 0:
        import hashlib
        src_lat = _safe_float(source.get("lat")) or 39.5
        src_lng = _safe_float(source.get("lng")) or -98.5
        h = int(hashlib.md5((addr + permit_number).encode()).hexdigest()[:8], 16)
        lat = src_lat + (h % 1000 - 500) * 0.0001
        lng = src_lng + ((h >> 10) % 1000 - 500) * 0.0001

    # Score
    score, temp, urgency = 50, "Warm", "Medium"
    if calculate_score_fn:
        score, temp, urgency = calculate_score_fn(days_old, valuation, permit_type or work_desc)
    pri = "hot" if score >= 70 else "warm" if score >= 50 else "med" if score >= 30 else "cold"

    owner_name = str(mapped.get("owner_name", ""))
    src_label = source.get("location") or source.get("city") or ""

    return {
        "permit_number": permit_number,
        "address": addr,
        "city": city,
        "state": state,
        "zip": zipcode,
        "lat": lat,
        "lng": lng,
        "work_description": work_desc,
        "permit_type": permit_type or (work_desc[:40] if work_desc else "Building Permit"),
        "valuation": valuation,
        "issue_date": issue_date,
        "days_old": days_old,
        "score": score,
        "temperature": temp,
        "source": src_label,
        "contractor_name": str(mapped.get("contractor_name", "")),
        "contractor_phone": str(mapped.get("contractor_phone", "")),
        "owner_name": owner_name or "Pending lookup",
        "owner_phone": str(mapped.get("owner_phone", "")),
        "owner_email": str(mapped.get("owner_email", "")),
        "apn": str(mapped.get("apn", "")),
        "stage": "new",
        "enrichment_status": "pending",
    }


def _safe_float(val: Any) -> Optional[float]:
    """Safely convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0 else None
    except (ValueError, TypeError):
        return None
