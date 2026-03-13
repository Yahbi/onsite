"""
Universal CKAN API fetcher.
Handles 580 CKAN data sources (datastore_search and direct CSV/JSON).
"""

import logging
import re
from typing import Any, Dict, List, Optional

import aiohttp

from services.fetchers.field_mapper import (
    clean_valuation,
    map_record,
    parse_date,
    split_combined_address,
)

logger = logging.getLogger(__name__)


def parse_ckan_url(url: str) -> tuple:
    """
    Parse a CKAN URL to extract base_url and resource_id.
    Patterns:
      https://data.gov/api/3/action/datastore_search?resource_id=abc123
      https://data.gov/dataset/xyz/resource/abc123
      https://data.gov/datastore/dump/abc123
    Returns (base_url, resource_id) or ("", "").
    """
    # Direct datastore_search URL
    m = re.match(r"(https?://[^/]+).*[?&]resource_id=([a-f0-9-]+)", url)
    if m:
        return (m.group(1), m.group(2))

    # Resource page URL
    m = re.match(r"(https?://[^/]+)/dataset/[^/]+/resource/([a-f0-9-]+)", url)
    if m:
        return (m.group(1), m.group(2))

    # Datastore dump
    m = re.match(r"(https?://[^/]+)/datastore/dump/([a-f0-9-]+)", url)
    if m:
        return (m.group(1), m.group(2))

    # Just the base domain + anything that looks like a UUID
    m = re.match(r"(https?://[^/]+).*?([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", url)
    if m:
        return (m.group(1), m.group(2))

    return ("", "")


async def fetch_ckan(
    source: dict,
    limit: int = 500,
    timeout_sec: int = 30,
) -> List[dict]:
    """
    Fetch records from a CKAN data source.
    source: {api_url, location, state, ...}
    Returns list of raw record dicts.
    """
    url = source.get("api_url", "")
    if not url:
        return []

    base_url, resource_id = parse_ckan_url(url)
    label = source.get("location") or url[:60]

    if not base_url or not resource_id:
        # Try direct URL fetch (might be a raw CSV/JSON endpoint)
        return await _fetch_direct(url, label, limit, timeout_sec)

    # Use CKAN datastore_search API
    api_url = f"{base_url}/api/3/action/datastore_search"
    params = {
        "resource_id": resource_id,
        "limit": str(limit),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                api_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_sec),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if data.get("success"):
                        records = data.get("result", {}).get("records", [])
                        logger.info(f"CKAN {label}: {len(records)} records")
                        return records
                    else:
                        err = data.get("error", "unknown")
                        raise RuntimeError(f"CKAN {label} API error: {err}")
                elif resp.status == 403:
                    raise PermissionError(f"CKAN {label} HTTP 403 Forbidden")
                elif resp.status == 404:
                    raise FileNotFoundError(f"CKAN {label} HTTP 404 Not Found")
                else:
                    # Fallback to direct URL before giving up
                    fallback = await _fetch_direct(url, label, limit, timeout_sec)
                    if fallback:
                        return fallback
                    raise RuntimeError(f"CKAN {label} HTTP {resp.status}")
    except (PermissionError, FileNotFoundError, RuntimeError):
        raise
    except Exception as e:
        raise RuntimeError(f"CKAN {label}: {e}") from e


async def _fetch_direct(
    url: str,
    label: str,
    limit: int,
    timeout_sec: int,
) -> List[dict]:
    """Fetch from a direct JSON/CSV URL as fallback."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout_sec),
            ) as resp:
                if resp.status != 200:
                    return []

                content_type = resp.content_type or ""
                if "json" in content_type:
                    data = await resp.json(content_type=None)
                    if isinstance(data, list):
                        return data[:limit]
                    if isinstance(data, dict):
                        # Try common CKAN result structures
                        for key in ("records", "results", "data", "rows", "features"):
                            if key in data and isinstance(data[key], list):
                                return data[key][:limit]
                    return []
                else:
                    # CSV — parse inline
                    import csv
                    import io
                    csv.field_size_limit(10 * 1024 * 1024)  # 10MB per field
                    text = await resp.text()
                    reader = csv.DictReader(io.StringIO(text))
                    records = []
                    for i, row in enumerate(reader):
                        if i >= limit:
                            break
                        records.append(dict(row))
                    logger.info(f"CKAN CSV {label}: {len(records)} records")
                    return records
    except Exception as e:
        logger.warning(f"CKAN direct fetch {label} error: {e}")
        return []


def normalize_ckan_record(
    raw: dict,
    source: dict,
    calculate_score_fn=None,
) -> Optional[dict]:
    """
    Normalize a CKAN record into the standard lead format.
    CKAN sources are highly heterogeneous — the field mapper handles most of it.
    """
    # Remove CKAN internal fields
    clean = {k: v for k, v in raw.items() if not k.startswith("_")}

    mapped = map_record(clean, source_type="ckan")

    addr = str(mapped.get("address", "")).strip()

    # Some CKAN sources have combined addresses
    if not addr:
        for key in ("full_address", "site_address", "property_address", "location"):
            val = clean.get(key, "")
            if val and isinstance(val, str) and len(val) > 5:
                parts = split_combined_address(val)
                addr = parts["address"]
                if parts["city"] and "city" not in mapped:
                    mapped["city"] = parts["city"]
                if parts["state"] and "state" not in mapped:
                    mapped["state"] = parts["state"]
                if parts["zip"] and "zip" not in mapped:
                    mapped["zip"] = parts["zip"]
                break

    if not addr:
        return None

    city = str(mapped.get("city", "")).strip()
    state = str(mapped.get("state") or source.get("state", "")).strip()
    if not city:
        city = (source.get("location") or "").split(" (")[0]

    zipcode = str(mapped.get("zip", ""))[:5]
    valuation = clean_valuation(mapped.get("valuation"))
    permit_number = str(mapped.get("permit_number", ""))
    permit_type = str(mapped.get("permit_type", ""))
    work_desc = str(mapped.get("work_description", ""))

    issue_date, days_old = parse_date(mapped.get("issue_date"))

    lat = _safe_float(mapped.get("lat"))
    lng = _safe_float(mapped.get("lng"))

    if not lat or not lng:
        import hashlib
        h = int(hashlib.md5((addr + permit_number).encode()).hexdigest()[:8], 16)
        lat = 39.5 + (h % 1000 - 500) * 0.0001
        lng = -98.5 + ((h >> 10) % 1000 - 500) * 0.0001

    score, temp, urgency = 50, "Warm", "Medium"
    if calculate_score_fn:
        score, temp, urgency = calculate_score_fn(days_old, valuation, permit_type or work_desc)

    return {
        "permit_number": permit_number,
        "address": addr,
        "city": city,
        "state": state,
        "zip": zipcode,
        "lat": lat,
        "lng": lng,
        "work_description": work_desc,
        "permit_type": permit_type or (work_desc[:40] if work_desc else "Permit"),
        "valuation": valuation,
        "issue_date": issue_date,
        "days_old": days_old,
        "score": score,
        "temperature": temp,
        "source": source.get("location") or "",
        "contractor_name": str(mapped.get("contractor_name", "")),
        "owner_name": str(mapped.get("owner_name", "")) or "Pending lookup",
        "owner_phone": str(mapped.get("owner_phone", "")),
        "owner_email": str(mapped.get("owner_email", "")),
        "apn": str(mapped.get("apn", "")),
        "stage": "new",
        "enrichment_status": "pending",
    }


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0 else None
    except (ValueError, TypeError):
        return None
