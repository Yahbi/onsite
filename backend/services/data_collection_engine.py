"""
Onsite - Data Collection Engine
Bulk data collection from multiple sources:
- OpenAddresses (free bulk download)
- Census TIGER/Line (free geographic data)
- Regrid (free parcel data samples)
- USGS Address Point data
"""

import aiohttp
import asyncio
import logging
import os
import zipfile
import json
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import csv

logger = logging.getLogger(__name__)


@dataclass
class BulkDataSource:
    """Configuration for a bulk data download source"""
    name: str
    url: str
    format: str  # csv, geojson, shapefile
    description: str
    update_frequency: str  # daily, weekly, monthly, quarterly
    coverage: str  # state, county, nationwide
    size_mb: Optional[int] = None
    last_updated: Optional[str] = None


class OpenAddressesCollector:
    """
    OpenAddresses - Free nationwide address data
    Source: https://openaddresses.io/
    Coverage: 50+ countries, 400+ million addresses
    Format: CSV files by region
    """

    def __init__(self, cache_dir: str = "./data_cache/openaddresses"):
        self.base_url = "https://results.openaddresses.io/latest/run.json"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {"downloaded": 0, "errors": 0, "cached": 0}

    async def get_available_sources(self) -> List[Dict]:
        """Get list of all available OpenAddresses data sources."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.error(f"OpenAddresses API returned {resp.status}")
                        return []

                    data = await resp.json()
                    sources = []

                    for source in data.get("runs", []):
                        if source.get("processed") and source.get("output"):
                            sources.append({
                                "source_id": source.get("source"),
                                "state": source.get("state"),
                                "county": source.get("county"),
                                "city": source.get("city"),
                                "csv_url": source["output"].get("csv"),
                                "geojson_url": source["output"].get("geojson"),
                                "address_count": source.get("address_count", 0),
                                "sample": source.get("sample", {})
                            })

                    logger.info(f"Found {len(sources)} OpenAddresses sources")
                    return sources

        except Exception as e:
            logger.error(f"Failed to fetch OpenAddresses sources: {e}")
            return []

    async def download_state_data(self, state_abbr: str) -> Optional[Path]:
        """
        Download address data for an entire state.

        Args:
            state_abbr: State abbreviation (e.g., "CA", "TX")

        Returns:
            Path to downloaded file or None
        """
        sources = await self.get_available_sources()
        state_sources = [s for s in sources if s.get("state") == state_abbr.upper()]

        if not state_sources:
            logger.warning(f"No OpenAddresses data found for state: {state_abbr}")
            return None

        # Download all county files for the state
        state_dir = self.cache_dir / state_abbr.lower()
        state_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = []
        for source in state_sources:
            csv_url = source.get("csv_url")
            if not csv_url:
                continue

            county = source.get("county", "unknown")
            filename = f"{state_abbr.lower()}_{county.lower().replace(' ', '_')}.csv"
            filepath = state_dir / filename

            # Check if already cached
            if filepath.exists():
                logger.info(f"Using cached file: {filename}")
                downloaded_files.append(filepath)
                self.stats["cached"] += 1
                continue

            # Download
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(csv_url, timeout=aiohttp.ClientTimeout(total=300)) as resp:
                        if resp.status == 200:
                            content = await resp.read()
                            filepath.write_bytes(content)
                            downloaded_files.append(filepath)
                            self.stats["downloaded"] += 1
                            logger.info(f"Downloaded: {filename} ({len(content) / 1024 / 1024:.1f} MB)")
                        else:
                            logger.error(f"Failed to download {filename}: {resp.status}")
                            self.stats["errors"] += 1
            except Exception as e:
                logger.error(f"Error downloading {filename}: {e}")
                self.stats["errors"] += 1

        logger.info(f"State {state_abbr}: Downloaded {len(downloaded_files)} files")
        return state_dir if downloaded_files else None


class CensusDataCollector:
    """
    US Census Bureau - TIGER/Line Shapefiles
    Source: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
    Coverage: Nationwide address ranges, roads, parcels
    Format: Shapefiles (can be converted to GeoJSON)
    """

    def __init__(self, api_key: Optional[str] = None, cache_dir: str = "./data_cache/census"):
        self.api_key = api_key or os.getenv("CENSUS_API_KEY", "")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {"downloaded": 0, "errors": 0}

    async def download_tiger_line(self, year: int = 2023, layer: str = "ADDR") -> Optional[Path]:
        """
        Download Census TIGER/Line shapefiles.

        Args:
            year: Census year (2020, 2021, 2022, 2023)
            layer: Layer type (ADDR=Address Ranges, ROADS, PARCELS)

        Returns:
            Path to downloaded shapefile directory
        """
        # TIGER/Line download URLs are structured by year and layer
        base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}"

        logger.warning("Census TIGER/Line download requires specific state/county FIPs codes")
        logger.info(f"Manual download available at: {base_url}")

        # This is a placeholder - full implementation would require:
        # 1. State/county FIPS code mapping
        # 2. Shapefile parsing library (e.g., pyshp, geopandas)
        # 3. Conversion to standardized format

        return None


class RegridDataCollector:
    """
    Regrid - Nationwide Parcel Data
    Source: https://regrid.com/
    Coverage: 150+ million parcels nationwide
    Format: GeoJSON, Shapefile
    Note: Requires API key for bulk access, has free tier
    """

    def __init__(self, api_key: Optional[str] = None, cache_dir: str = "./data_cache/regrid"):
        self.api_key = api_key or os.getenv("REGRID_API_KEY", "")
        self.base_url = "https://api.regrid.com/api/v1"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.stats = {"fetched": 0, "errors": 0}

    async def search_parcels(self, lat: float, lng: float, radius_meters: int = 1000) -> List[Dict]:
        """
        Search for parcels near a point.

        Args:
            lat: Latitude
            lng: Longitude
            radius_meters: Search radius in meters

        Returns:
            List of parcel records
        """
        if not self.api_key:
            logger.warning("Regrid API key not configured")
            return []

        url = f"{self.base_url}/search/nearby"
        params = {
            "lat": lat,
            "lng": lng,
            "radius": radius_meters,
            "token": self.api_key
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        parcels = data.get("results", [])
                        self.stats["fetched"] += len(parcels)
                        return parcels
                    else:
                        logger.error(f"Regrid API returned {resp.status}")
                        self.stats["errors"] += 1
                        return []
        except Exception as e:
            logger.error(f"Regrid API error: {e}")
            self.stats["errors"] += 1
            return []


class BulkDataOrchestrator:
    """
    Orchestrates bulk data collection from multiple sources.
    Manages downloads, caching, and data merging.
    """

    def __init__(self, cache_dir: str = "./data_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.openaddresses = OpenAddressesCollector(cache_dir=str(self.cache_dir / "openaddresses"))
        self.census = CensusDataCollector(cache_dir=str(self.cache_dir / "census"))
        self.regrid = RegridDataCollector(cache_dir=str(self.cache_dir / "regrid"))

    async def collect_state_addresses(self, state_abbr: str) -> Dict:
        """
        Collect all available address data for a state.

        Args:
            state_abbr: State abbreviation (e.g., "CA")

        Returns:
            Dict with download status and file paths
        """
        logger.info(f"Starting bulk data collection for {state_abbr}")

        # Download OpenAddresses data
        oa_path = await self.openaddresses.download_state_data(state_abbr)

        return {
            "state": state_abbr,
            "openaddresses": {
                "path": str(oa_path) if oa_path else None,
                "stats": self.openaddresses.stats
            },
            "timestamp": datetime.now().isoformat()
        }

    async def collect_multiple_states(self, states: List[str]) -> Dict[str, Dict]:
        """
        Collect data for multiple states in parallel.

        Args:
            states: List of state abbreviations

        Returns:
            Dict mapping state to collection results
        """
        tasks = {
            state: self.collect_state_addresses(state)
            for state in states
        }

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        state_data = {}
        for state, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"{state} collection failed: {result}")
                state_data[state] = {"error": str(result)}
            else:
                state_data[state] = result

        return state_data

    def get_stats(self) -> Dict:
        """Get collection statistics from all sources."""
        return {
            "openaddresses": self.openaddresses.stats,
            "census": self.census.stats,
            "regrid": self.regrid.stats
        }


# Data source catalog
BULK_DATA_SOURCES = [
    BulkDataSource(
        name="OpenAddresses",
        url="https://openaddresses.io/",
        format="csv",
        description="400M+ addresses worldwide, free CSV downloads by region",
        update_frequency="daily",
        coverage="nationwide",
        size_mb=None  # Varies by region
    ),
    BulkDataSource(
        name="Census TIGER/Line",
        url="https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html",
        format="shapefile",
        description="Address ranges, roads, parcels from US Census",
        update_frequency="annually",
        coverage="nationwide",
        size_mb=5000  # ~5GB for full dataset
    ),
    BulkDataSource(
        name="Regrid Parcels",
        url="https://regrid.com/",
        format="geojson",
        description="150M+ property parcels with ownership data",
        update_frequency="monthly",
        coverage="nationwide",
        size_mb=None  # API-based
    ),
    BulkDataSource(
        name="USGS National Map",
        url="https://www.usgs.gov/programs/national-geospatial-program/national-map",
        format="geojson",
        description="Structures and address points from USGS",
        update_frequency="quarterly",
        coverage="nationwide",
        size_mb=2000
    ),
]


# Singleton instance
bulk_collector = BulkDataOrchestrator()
