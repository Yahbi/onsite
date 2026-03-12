"""
Onsite - Discovered Sources API Routes
Query and manage the 2,300+ discovered permit API endpoints
"""

from fastapi import APIRouter, Query
from typing import Optional
import logging

router = APIRouter(prefix="/api/discovered", tags=["discovered-sources"])
logger = logging.getLogger(__name__)


@router.get("/sources")
async def list_discovered_sources(
    city: str = Query("", description="Filter by city name (partial match)"),
    state: str = Query("", description="Filter by state code (e.g. CA, TX)"),
    location_type: str = Query("", description="Filter by type: city, county, state"),
    new_only: bool = Query(False, description="Only show sources not already in config"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
):
    """
    Search discovered permit API endpoints.
    Returns sources from the master discovery CSV (2,300+ validated endpoints).
    """
    from backend.services.discovered_sources import get_discovered_registry

    registry = get_discovered_registry()
    results = registry.search(city=city, state=state, location_type=location_type)

    if new_only:
        results = [r for r in results if not r.get("already_configured")]

    total = len(results)
    results = results[offset:offset + limit]

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "sources": results,
    }


@router.get("/stats")
async def discovered_sources_stats():
    """Get summary statistics for discovered sources."""
    from backend.services.discovered_sources import get_discovered_registry

    registry = get_discovered_registry()
    return registry.get_stats()


@router.get("/coverage")
async def coverage_by_state(
    state: str = Query("", description="Filter to specific state"),
):
    """
    Get permit API coverage by state.
    Shows how many cities/counties in each state have discovered APIs.
    """
    from backend.services.discovered_sources import get_discovered_registry

    registry = get_discovered_registry()

    coverage = {}
    for st, apis in registry.by_state.items():
        if state and st.upper() != state.upper():
            continue
        locations = set()
        for a in apis:
            locations.add(f"{a.location}|{a.location_type}")
        coverage[st] = {
            "total_apis": len(apis),
            "locations_with_apis": len(locations),
            "locations": [
                {"name": loc.split("|")[0], "type": loc.split("|")[1]}
                for loc in sorted(locations)
            ],
        }

    return {
        "states": len(coverage),
        "coverage": dict(sorted(coverage.items())),
    }


@router.get("/sync-ready")
async def get_sync_ready_sources():
    """
    Get sources that are ready to be integrated into the sync pipeline.
    Returns counts and sample of new Socrata/ArcGIS sources not yet in config.
    """
    from backend.services.discovered_sources import get_discovered_registry

    registry = get_discovered_registry()

    new_socrata = registry.get_new_socrata_configs()
    new_arcgis = registry.get_new_arcgis_configs()

    return {
        "new_socrata_count": len(new_socrata),
        "new_arcgis_count": len(new_arcgis),
        "total_new": len(new_socrata) + len(new_arcgis),
        "socrata_sample": dict(list(new_socrata.items())[:10]),
        "arcgis_sample": dict(list(new_arcgis.items())[:10]),
    }
