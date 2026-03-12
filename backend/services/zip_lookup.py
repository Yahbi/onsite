"""
Zip Code Lookup from Coordinates
Assigns zip codes to leads missing them using nearest-neighbor from leads that have zips.
Uses a spatial grid for O(1) lookups — no external API calls needed.
"""

import math
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Grid resolution: ~0.05 degrees ≈ 3.5 miles at mid-latitudes
_GRID_RES = 0.05


def _grid_key(lat: float, lng: float) -> tuple:
    """Convert lat/lng to grid cell key."""
    return (round(lat / _GRID_RES), round(lng / _GRID_RES))


def _haversine_approx(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Fast approximate distance in km (good enough for nearest-neighbor)."""
    dlat = lat2 - lat1
    dlng = (lng2 - lng1) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.sqrt(dlat * dlat + dlng * dlng) * 111.0  # degrees to km


def fill_missing_zips(leads: List[Dict], max_distance_km: float = 10.0) -> int:
    """
    Fill missing zip codes by finding the nearest lead with a known zip.

    Uses a spatial grid index for fast lookups. Only assigns a zip if the
    nearest known-zip lead is within max_distance_km.

    Args:
        leads: List of lead dicts (mutated in place for performance on 2M+ leads)
        max_distance_km: Maximum distance to borrow a zip from

    Returns:
        Number of leads that had zips filled in
    """
    # Phase 1: Build spatial grid from leads WITH zips
    zip_grid: Dict[tuple, List[tuple]] = {}  # grid_key -> [(lat, lng, zip)]

    for lead in leads:
        z = lead.get("zip") or ""
        if not z:
            continue
        lat = lead.get("lat")
        lng = lead.get("lng")
        if not lat or not lng:
            continue
        try:
            lat_f = float(lat)
            lng_f = float(lng)
        except (ValueError, TypeError):
            continue
        key = _grid_key(lat_f, lng_f)
        if key not in zip_grid:
            zip_grid[key] = []
        # Only store first few per cell to keep memory bounded
        if len(zip_grid[key]) < 5:
            zip_grid[key].append((lat_f, lng_f, str(z)))

    if not zip_grid:
        logger.warning("No leads with zip codes found — cannot fill missing zips")
        return 0

    logger.info(f"Zip grid built: {len(zip_grid)} cells from leads with zips")

    # Phase 2: For each lead missing zip, find nearest known zip
    filled = 0
    for lead in leads:
        z = lead.get("zip") or ""
        if z:
            continue  # Already has zip
        lat = lead.get("lat")
        lng = lead.get("lng")
        if not lat or not lng:
            continue
        try:
            lat_f = float(lat)
            lng_f = float(lng)
        except (ValueError, TypeError):
            continue

        # Search this cell and 8 neighbors
        cx, cy = _grid_key(lat_f, lng_f)
        best_zip = None
        best_dist = max_distance_km

        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                cell = zip_grid.get((cx + dx, cy + dy))
                if not cell:
                    continue
                for (nlat, nlng, nzip) in cell:
                    dist = _haversine_approx(lat_f, lng_f, nlat, nlng)
                    if dist < best_dist:
                        best_dist = dist
                        best_zip = nzip

        if best_zip:
            lead["zip"] = best_zip
            filled += 1

    logger.info(f"Zip fill complete: {filled} leads got zips from nearby neighbors")
    return filled
