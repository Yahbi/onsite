"""
Universal ArcGIS FeatureServer fetcher.
Handles all 662 ArcGIS data sources with automatic pagination.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

from services.fetchers.field_mapper import (
    clean_valuation,
    map_record,
    parse_date,
)

logger = logging.getLogger(__name__)


def ensure_query_url(url: str) -> str:
    """Ensure an ArcGIS URL ends with /LayerIndex/query for the REST API.

    Many ArcGIS URLs point to FeatureServer or MapServer without a layer index.
    Without a layer index, ArcGIS returns service metadata, not features.
    Default to layer 0 when no index is specified.
    """
    url = url.strip().rstrip("/")
    # Already has /query — leave as-is
    if url.endswith("/query"):
        return url
    # Strip existing query params that got baked into the URL
    if "?" in url:
        url = url.split("?")[0].rstrip("/")
    # Has FeatureServer/MapServer with layer index already (e.g., /FeatureServer/0)
    if re.search(r"/(Feature|Map)Server/\d+$", url):
        return url + "/query"
    # Has FeatureServer/MapServer but NO layer index — default to layer 0
    if re.search(r"/(Feature|Map)Server$", url):
        return url + "/0/query"
    # Unknown format — try appending /query
    return url + "/query"


async def fetch_arcgis(
    source: dict,
    limit: int = 1000,
    days: int = 90,
    timeout_sec: int = 30,
) -> List[dict]:
    """
    Fetch features from an ArcGIS FeatureServer/MapServer source.
    source: {api_url or url or query_url, location, state, ...}
    Returns list of raw feature dicts: {attributes: {...}, geometry: {...}}
    """
    url = source.get("query_url") or source.get("url") or source.get("api_url", "")
    if not url:
        return []
    url = ensure_query_url(url)

    label = source.get("location") or source.get("city") or url[:60]

    # Build query params
    params: Dict[str, str] = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "resultRecordCount": str(limit),
        "orderByFields": "OBJECTID DESC",
    }

    # Try date filtering if available
    date_field = source.get("date_field")
    if date_field:
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        params["where"] = f"{date_field} >= DATE '{since}'"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_sec),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    # ArcGIS sometimes returns error JSON with 200 status
                    if "error" in data:
                        err_msg = data["error"].get("message", "Unknown ArcGIS error")
                        raise RuntimeError(f"ArcGIS {label}: {err_msg}")
                    features = data.get("features", [])
                    logger.info(f"ArcGIS {label}: {len(features)} features")
                    # Flatten features: merge attributes + geometry into flat dicts
                    # so extract_address/extract_permit_number in main.py can find fields
                    return [_flatten_feature(f) for f in features]
                elif resp.status == 403:
                    raise PermissionError(f"ArcGIS {label} HTTP 403 Forbidden")
                else:
                    raise RuntimeError(f"ArcGIS {label} HTTP {resp.status}")
    except (PermissionError, RuntimeError):
        raise
    except Exception as e:
        raise RuntimeError(f"ArcGIS {label}: {e}") from e


async def fetch_arcgis_paginated(
    source: dict,
    max_records: int = 5000,
    page_size: int = 1000,
    timeout_sec: int = 30,
) -> List[dict]:
    """
    Fetch all features with pagination using resultOffset.
    Use for initial bulk loads.
    """
    url = source.get("query_url") or source.get("url") or source.get("api_url", "")
    if not url:
        return []
    url = ensure_query_url(url)

    all_features = []
    offset = 0

    async with aiohttp.ClientSession() as session:
        while offset < max_records:
            params = {
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": str(page_size),
                "resultOffset": str(offset),
                "orderByFields": "OBJECTID DESC",
            }
            try:
                async with session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout_sec),
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json(content_type=None)
                    features = data.get("features", [])
                    if not features:
                        break
                    all_features.extend(_flatten_feature(f) for f in features)
                    offset += len(features)
                    if len(features) < page_size:
                        break
            except Exception as e:
                logger.warning(f"ArcGIS pagination error at offset {offset}: {e}")
                break

    return all_features


def normalize_arcgis_record(
    feature: dict,
    source: dict,
    calculate_score_fn=None,
) -> Optional[dict]:
    """
    Normalize an ArcGIS feature into the standard lead format.
    feature: {attributes: {...}, geometry: {x, y}}
    """
    attrs = feature.get("attributes", {})
    if not attrs:
        return None

    # Use field mapper on attributes, but also pass geometry
    combined = dict(attrs)
    combined["geometry"] = feature.get("geometry", {})
    mapped = map_record(combined, source_type="arcgis")

    addr = str(mapped.get("address", "")).strip()
    if not addr:
        return None

    city = str(mapped.get("city", "")).strip() or source.get("location") or source.get("city") or ""
    state = str(mapped.get("state", "")).strip() or source.get("state") or source.get("st") or ""
    zipcode = str(mapped.get("zip", ""))[:5]

    valuation = clean_valuation(mapped.get("valuation"))
    permit_number = str(mapped.get("permit_number", ""))
    permit_type = str(mapped.get("permit_type", "")) or "Building Permit"
    work_desc = str(mapped.get("work_description", ""))

    # Date
    raw_date = mapped.get("issue_date", 0)
    issue_date, days_old = parse_date(raw_date)

    # Geometry
    lat = _safe_float(mapped.get("lat"))
    lng = _safe_float(mapped.get("lng"))

    # Fallback
    if not lat or not lng:
        src_lat = _safe_float(source.get("lat")) or 39.5
        src_lng = _safe_float(source.get("lng")) or -98.5
        import hashlib
        h = int(hashlib.md5((addr + permit_number).encode()).hexdigest()[:8], 16)
        lat = src_lat + (h % 1000 - 500) * 0.0001
        lng = src_lng + ((h >> 10) % 1000 - 500) * 0.0001

    # Score
    score, temp, urgency = 50, "Warm", "Medium"
    if calculate_score_fn:
        score, temp, urgency = calculate_score_fn(days_old, valuation, permit_type)

    return {
        "permit_number": permit_number,
        "address": addr,
        "city": city,
        "state": state,
        "zip": zipcode,
        "lat": lat,
        "lng": lng,
        "work_description": work_desc,
        "permit_type": permit_type,
        "valuation": valuation,
        "issue_date": issue_date,
        "days_old": days_old,
        "score": score,
        "temperature": temp,
        "source": source.get("location") or source.get("city") or "",
        "contractor_name": str(mapped.get("contractor_name", "")),
        "owner_name": str(mapped.get("owner_name", "")) or "Pending lookup",
        "apn": str(mapped.get("apn", "")),
        "zoning": str(mapped.get("zoning", "")),
        "stage": "new",
        "enrichment_status": "pending",
    }


def _flatten_feature(feature: dict) -> dict:
    """Flatten an ArcGIS feature {attributes: {...}, geometry: {x, y}} into a flat dict.

    This allows extract_address() and other field extractors in main.py
    to find fields like 'address', 'permit_number' etc. directly.
    """
    attrs = feature.get("attributes") or {}
    geo = feature.get("geometry") or {}
    flat = dict(attrs)
    # Map ArcGIS geometry x/y to lat/lng
    if "y" in geo and "x" in geo:
        flat.setdefault("lat", geo["y"])
        flat.setdefault("lng", geo["x"])
        flat.setdefault("longitude", geo["x"])
        flat.setdefault("latitude", geo["y"])
    return flat


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0 else None
    except (ValueError, TypeError):
        return None
