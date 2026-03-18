"""
Fetcher dispatcher -- routes API sources to the appropriate fetcher
based on source_type.
"""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy-load fetchers to avoid circular imports
_fetcher_cache: dict = {}


def _get_fetcher(source_type: str) -> Optional[callable]:
    """Lazy-load and cache fetcher modules by source_type."""
    # Normalize to handle case variants (e.g. "socrata" -> "Socrata")
    normalized = source_type.lower()
    if normalized not in _fetcher_cache:
        try:
            if normalized in ("socrata",):
                from services.fetchers.socrata_fetcher import fetch_socrata
                _fetcher_cache[normalized] = fetch_socrata
            elif normalized in ("arcgis", "arcgis_hub"):
                from services.fetchers.arcgis_fetcher import fetch_arcgis
                _fetcher_cache[normalized] = fetch_arcgis
            elif normalized in ("ckan",):
                from services.fetchers.ckan_fetcher import fetch_ckan
                _fetcher_cache[normalized] = fetch_ckan
            elif normalized in ("federal", "fema", "noaa", "usgs"):
                from services.fetchers.federal_fetcher import fetch_federal
                _fetcher_cache[normalized] = fetch_federal
            elif normalized in ("direct", "state", "accela", "energov", "carto"):
                # These use generic HTTP fetch — treat as Socrata-like JSON endpoints
                from services.fetchers.socrata_fetcher import fetch_socrata
                _fetcher_cache[normalized] = fetch_socrata
            else:
                _fetcher_cache[normalized] = None
        except ImportError as exc:
            logger.warning(
                "Failed to load fetcher for %s: %s", source_type, exc
            )
            _fetcher_cache[normalized] = None
    return _fetcher_cache.get(normalized)


async def fetch_from_source(
    source: dict,
    timeout_seconds: int = 30,
) -> list:
    """
    Dispatch to the correct fetcher based on source_type.

    Returns a list of raw record dicts, or an empty list on error.
    The source dict is never mutated.
    """
    source_type = source.get("source_type", "")
    fetcher = _get_fetcher(source_type)

    if fetcher is None:
        logger.debug(
            "No fetcher for source_type=%s (source=%s)",
            source_type,
            source.get("api_name", ""),
        )
        return []

    try:
        result = await asyncio.wait_for(
            fetcher(source),
            timeout=timeout_seconds,
        )
        return result if isinstance(result, list) else []
    except asyncio.TimeoutError:
        logger.warning(
            "Timeout fetching %s (%s)",
            source.get("api_name", ""),
            source_type,
        )
        return []
    except Exception as exc:
        logger.warning(
            "Error fetching %s (%s): %s",
            source.get("api_name", ""),
            source_type,
            exc,
        )
        return []
