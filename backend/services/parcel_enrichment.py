"""
Batch ownership enrichment using free Regrid MVT parcel tiles.

Regrid serves nationwide US parcel boundary tiles at:
  https://tiles.regrid.com/api/v1/parcels/{z}/{x}/{y}.mvt
No API key needed. Each tile contains parcel polygons with:
  owner, address, parcelnumb (APN), path

Strategy:
  1. Group leads without owner_name by z15 tile
  2. Download tile (cached on disk)
  3. Parse MVT, extract parcel centroids + owner data
  4. Match each lead to nearest parcel by distance
  5. Write owner_name back to lead
"""

import asyncio
import aiohttp
import math
import os
import json
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

TILE_CACHE_DIR = Path(__file__).parent.parent / ".tile_cache"
TILE_ZOOM = 15
MAX_CONCURRENT = 10
MATCH_RADIUS_DEG = 0.002  # ~200m — max distance to match a parcel


def _lat_lng_to_tile(lat: float, lng: float, z: int) -> tuple[int, int]:
    """Convert lat/lng to tile x,y at zoom level z."""
    n = 2 ** z
    x = int((lng + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(math.radians(lat)) + 1 / math.cos(math.radians(lat))) / math.pi) / 2.0 * n)
    return x, y


def _tile_to_bbox(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    """Get bounding box (west, south, east, north) for a tile."""
    n = 2 ** z
    w = x / n * 360.0 - 180.0
    e = (x + 1) / n * 360.0 - 180.0
    n_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    s_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return w, s_lat, e, n_lat


def _parse_mvt_parcels(data: bytes) -> list[dict]:
    """Parse MVT tile and extract parcel data with approximate centroids."""
    try:
        import mapbox_vector_tile
    except ImportError:
        logger.warning("mapbox_vector_tile not installed — run: pip install mapbox-vector-tile")
        return []

    try:
        decoded = mapbox_vector_tile.decode(data)
    except Exception as e:
        logger.debug(f"MVT decode error: {e}")
        return []

    layer = decoded.get("parcels")
    if not layer:
        return []

    parcels = []
    extent = layer.get("extent", 4096)
    for feat in layer.get("features", []):
        props = feat.get("properties", {})
        owner = props.get("owner", "")
        if not owner:
            continue

        # Get centroid from geometry (approximate — tile coords)
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates", [])
        if not coords:
            continue

        # Flatten nested coord arrays to find centroid
        all_pts = []
        def _flatten(c):
            if isinstance(c, (list, tuple)):
                if len(c) == 2 and isinstance(c[0], (int, float)):
                    all_pts.append(c)
                else:
                    for sub in c:
                        _flatten(sub)
        _flatten(coords)

        if not all_pts:
            continue

        # Average to get centroid (in tile pixel coords 0..extent)
        cx = sum(p[0] for p in all_pts) / len(all_pts)
        cy = sum(p[1] for p in all_pts) / len(all_pts)

        parcels.append({
            "owner": owner,
            "address": props.get("address", ""),
            "apn": props.get("parcelnumb", ""),
            "tx": cx / extent,  # normalized 0..1 within tile
            "ty": cy / extent,
        })

    return parcels


async def _download_tile(session: aiohttp.ClientSession, x: int, y: int, z: int) -> Optional[bytes]:
    """Download a single MVT tile, with disk caching."""
    TILE_CACHE_DIR.mkdir(exist_ok=True)
    cache_path = TILE_CACHE_DIR / f"{z}_{x}_{y}.mvt"

    # Check disk cache (tiles don't change often)
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path.read_bytes()

    url = f"https://tiles.regrid.com/api/v1/parcels/{z}/{x}/{y}.mvt"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.read()
                if len(data) > 50:  # Skip empty tiles
                    cache_path.write_bytes(data)
                    return data
            return None
    except Exception as e:
        logger.debug(f"Tile download error {z}/{x}/{y}: {e}")
        return None


async def enrich_leads_from_tiles(leads: list[dict], max_tiles: int = 2000) -> dict:
    """
    Enrich leads that lack owner_name using Regrid parcel tiles.

    Args:
        leads: List of lead dicts (modified in-place)
        max_tiles: Max number of tiles to download per run

    Returns:
        Stats dict with enrichment results
    """
    t0 = time.time()

    # Find leads needing enrichment (have coords but no owner)
    needs_enrichment = []
    for lead in leads:
        owner = str(lead.get("owner_name", "") or "").strip()
        if owner and owner != "Not found":
            continue
        lat = lead.get("lat") or lead.get("latitude")
        lng = lead.get("lng") or lead.get("longitude") or lead.get("lon")
        if not lat or not lng:
            continue
        try:
            flat, flng = float(lat), float(lng)
            if 17 < flat < 72 and -180 < flng < -65:
                needs_enrichment.append((lead, flat, flng))
        except (ValueError, TypeError):
            continue

    if not needs_enrichment:
        return {"enriched": 0, "skipped": 0, "message": "All leads already have owners"}

    # Group by tile
    tile_groups: dict[tuple[int, int], list] = {}
    for lead, lat, lng in needs_enrichment:
        tx, ty = _lat_lng_to_tile(lat, lng, TILE_ZOOM)
        key = (tx, ty)
        if key not in tile_groups:
            tile_groups[key] = []
        tile_groups[key].append((lead, lat, lng))

    logger.info(f"Ownership enrichment: {len(needs_enrichment):,} leads across {len(tile_groups):,} tiles")

    # Limit tiles per run
    tile_keys = list(tile_groups.keys())[:max_tiles]
    enriched_count = 0
    tiles_processed = 0
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def process_tile(session, tx, ty):
        nonlocal enriched_count, tiles_processed
        async with sem:
            data = await _download_tile(session, tx, ty, TILE_ZOOM)
            if not data:
                return

            parcels = _parse_mvt_parcels(data)
            if not parcels:
                return

            # Get tile bbox for converting tile-local coords to lat/lng
            w, s, e, n = _tile_to_bbox(tx, ty, TILE_ZOOM)
            tile_w = e - w
            tile_h = n - s

            # Convert parcel centroids to lat/lng
            for p in parcels:
                p["lat"] = n - p["ty"] * tile_h  # y is inverted in MVT
                p["lng"] = w + p["tx"] * tile_w

            # Match each lead in this tile to nearest parcel
            for lead, lead_lat, lead_lng in tile_groups.get((tx, ty), []):
                best_dist = MATCH_RADIUS_DEG
                best_parcel = None
                for p in parcels:
                    dist = abs(p["lat"] - lead_lat) + abs(p["lng"] - lead_lng)
                    if dist < best_dist:
                        best_dist = dist
                        best_parcel = p

                if best_parcel:
                    lead["owner_name"] = best_parcel["owner"]
                    if best_parcel.get("apn"):
                        lead["apn"] = best_parcel["apn"]
                    enriched_count += 1

            tiles_processed += 1
            if tiles_processed % 100 == 0:
                logger.info(f"  Tiles: {tiles_processed}/{len(tile_keys)}, enriched: {enriched_count:,}")

    async with aiohttp.ClientSession() as session:
        tasks = [process_tile(session, tx, ty) for tx, ty in tile_keys]
        await asyncio.gather(*tasks)

    elapsed = time.time() - t0
    stats = {
        "enriched": enriched_count,
        "tiles_processed": tiles_processed,
        "total_tiles": len(tile_groups),
        "leads_checked": len(needs_enrichment),
        "elapsed_seconds": round(elapsed, 1),
    }
    logger.info(f"Ownership enrichment complete: {enriched_count:,} enriched in {elapsed:.1f}s")
    return stats
