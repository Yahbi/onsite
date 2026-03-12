"""
Onsite - County Assessor Scrapers
Property data from county assessor offices
Supports: Harris County (Houston), Dallas County, Tarrant County (Fort Worth)
"""

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


@dataclass
class PropertyRecord:
    """Standardized property record from county assessor"""
    parcel_id: str
    address: str
    city: str
    state: str
    zip_code: str
    owner_name: str
    owner_address: str = ""
    owner_city: str = ""
    owner_state: str = ""
    owner_zip: str = ""
    property_type: str = ""
    year_built: Optional[int] = None
    square_feet: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    assessed_value: float = 0.0
    market_value: float = 0.0
    last_sale_date: str = ""
    last_sale_price: float = 0.0
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    county: str = ""
    source: str = ""
    scraped_at: str = ""


class HarrisCountyScraper:
    """
    Harris County, TX (Houston) Property Records
    Source: Harris County Appraisal District API
    API: https://public.hcad.org/records/
    """

    def __init__(self):
        self.base_url = "https://publicapi.hcad.org/api"
        self.county = "Harris"
        self.state = "TX"
        self.stats = {"fetched": 0, "errors": 0}

    async def search_by_address(self, street_name: str, limit: int = 100) -> List[PropertyRecord]:
        """
        Search Harris County properties by street name.

        Args:
            street_name: Street name to search (e.g., "Main St")
            limit: Max properties to return

        Returns:
            List of PropertyRecord objects
        """
        search_url = f"{self.base_url}/PropertySearch/StreetName"
        params = {"streetName": street_name}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(search_url, params=params,
                                      timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.error(f"Harris County API returned {resp.status}")
                        self.stats["errors"] += 1
                        return []

                    data = await resp.json()
                    properties = []

                    # Harris County API returns list of account numbers
                    # Need to fetch details for each
                    accounts = data[:limit] if isinstance(data, list) else []

                    for account in accounts:
                        try:
                            account_id = account.get("account") or account.get("accountNumber")
                            if not account_id:
                                continue

                            # Fetch property details
                            detail = await self._fetch_property_detail(session, account_id)
                            if detail:
                                properties.append(detail)
                                self.stats["fetched"] += 1

                        except Exception as e:
                            logger.debug(f"Error parsing Harris County property: {e}")
                            self.stats["errors"] += 1
                            continue

                    logger.info(f"Harris County: Fetched {len(properties)} properties for '{street_name}'")
                    return properties

        except Exception as e:
            logger.error(f"Harris County scraper failed: {e}")
            self.stats["errors"] += 1
            return []

    async def _fetch_property_detail(self, session: aiohttp.ClientSession, account_id: str) -> Optional[PropertyRecord]:
        """Fetch detailed property information by account ID."""
        detail_url = f"{self.base_url}/PropertyData/Account/{account_id}"

        try:
            async with session.get(detail_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None

                data = await resp.json()

                # Parse property data
                property_data = data.get("property", {})
                owner_data = data.get("owner", {})
                building_data = data.get("building", {})
                value_data = data.get("value", {})

                return PropertyRecord(
                    parcel_id=account_id,
                    address=property_data.get("situs_address", ""),
                    city=property_data.get("situs_city", ""),
                    state=self.state,
                    zip_code=property_data.get("situs_zip", ""),
                    owner_name=owner_data.get("owner_name", ""),
                    owner_address=owner_data.get("mail_address", ""),
                    owner_city=owner_data.get("mail_city", ""),
                    owner_state=owner_data.get("mail_state", ""),
                    owner_zip=owner_data.get("mail_zip", ""),
                    property_type=property_data.get("state_class", ""),
                    year_built=int(building_data.get("year_built", 0)) if building_data.get("year_built") else None,
                    square_feet=int(building_data.get("total_area", 0)) if building_data.get("total_area") else None,
                    assessed_value=float(value_data.get("appraised_value", 0) or 0),
                    market_value=float(value_data.get("market_value", 0) or 0),
                    latitude=float(property_data.get("latitude")) if property_data.get("latitude") else None,
                    longitude=float(property_data.get("longitude")) if property_data.get("longitude") else None,
                    county=self.county,
                    source="Harris County Appraisal District",
                    scraped_at=datetime.now().isoformat()
                )

        except Exception as e:
            logger.debug(f"Error fetching Harris County property detail: {e}")
            return None


class DallasCountyScraper:
    """
    Dallas County, TX Property Records
    Source: Dallas Central Appraisal District
    API: https://www.dallascad.org/
    """

    def __init__(self):
        # Dallas CAD uses a different API structure
        self.base_url = "https://www.dallascad.org"
        self.county = "Dallas"
        self.state = "TX"
        self.stats = {"fetched": 0, "errors": 0}

    async def search_by_address(self, street_name: str, limit: int = 100) -> List[PropertyRecord]:
        """
        Search Dallas County properties by street name.

        Note: Dallas CAD doesn't have a public REST API like Harris County.
        This is a placeholder for web scraping implementation.
        """
        logger.warning("Dallas County scraper not fully implemented - requires web scraping")
        self.stats["errors"] += 1
        return []

    async def search_by_owner(self, owner_name: str, limit: int = 100) -> List[PropertyRecord]:
        """
        Search Dallas County properties by owner name.

        Note: Placeholder for future implementation.
        """
        logger.warning("Dallas County owner search not implemented")
        self.stats["errors"] += 1
        return []


class TarrantCountyScraper:
    """
    Tarrant County, TX (Fort Worth) Property Records
    Source: Tarrant Appraisal District
    API: https://www.tad.org/
    """

    def __init__(self):
        self.base_url = "https://propaccess.tarrantcounty.com"
        self.county = "Tarrant"
        self.state = "TX"
        self.stats = {"fetched": 0, "errors": 0}

    async def search_by_address(self, street_name: str, limit: int = 100) -> List[PropertyRecord]:
        """
        Search Tarrant County properties by street name.

        Note: Tarrant County requires web scraping or proprietary API access.
        This is a placeholder.
        """
        logger.warning("Tarrant County scraper not fully implemented - requires web scraping")
        self.stats["errors"] += 1
        return []

    async def search_by_owner(self, owner_name: str, limit: int = 100) -> List[PropertyRecord]:
        """
        Search Tarrant County properties by owner name.

        Note: Placeholder for future implementation.
        """
        logger.warning("Tarrant County owner search not implemented")
        self.stats["errors"] += 1
        return []


class PhoenixAssessorScraper:
    """
    Maricopa County, AZ (Phoenix) Property Records
    Source: Maricopa County Assessor ArcGIS REST API
    API: https://gis.maricopa.gov/arcgis/rest/services/
    """

    def __init__(self):
        self.base_url = "https://gis.maricopa.gov/arcgis/rest/services/Assessor/Assessor/MapServer/0/query"
        self.county = "Maricopa"
        self.state = "AZ"
        self.stats = {"fetched": 0, "errors": 0}

    async def search_by_city(self, city: str = "Phoenix", limit: int = 1000) -> List[PropertyRecord]:
        """
        Search Maricopa County properties by city.

        Args:
            city: City name (default: Phoenix)
            limit: Max properties to return

        Returns:
            List of PropertyRecord objects
        """
        params = {
            "where": f"SITUS_CITY = '{city.upper()}'",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "json",
            "resultRecordCount": limit
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params,
                                      timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.error(f"Maricopa County API returned {resp.status}")
                        self.stats["errors"] += 1
                        return []

                    data = await resp.json()
                    properties = []

                    features = data.get("features", [])
                    for feature in features:
                        try:
                            attrs = feature.get("attributes", {})
                            geom = feature.get("geometry", {})

                            property_record = PropertyRecord(
                                parcel_id=attrs.get("APN", ""),
                                address=attrs.get("SITUS_ADDRESS", ""),
                                city=attrs.get("SITUS_CITY", ""),
                                state=self.state,
                                zip_code=attrs.get("SITUS_ZIP", ""),
                                owner_name=attrs.get("OWNER_NAME", ""),
                                owner_address=attrs.get("MAIL_ADDRESS", ""),
                                owner_city=attrs.get("MAIL_CITY", ""),
                                owner_state=attrs.get("MAIL_STATE", ""),
                                owner_zip=attrs.get("MAIL_ZIP", ""),
                                property_type=attrs.get("PROPERTY_USE", ""),
                                year_built=int(attrs.get("YEAR_BUILT", 0)) if attrs.get("YEAR_BUILT") else None,
                                square_feet=int(attrs.get("BUILDING_SQFT", 0)) if attrs.get("BUILDING_SQFT") else None,
                                assessed_value=float(attrs.get("ASSESSED_VALUE", 0) or 0),
                                market_value=float(attrs.get("MARKET_VALUE", 0) or 0),
                                latitude=float(geom.get("y")) if geom.get("y") else None,
                                longitude=float(geom.get("x")) if geom.get("x") else None,
                                county=self.county,
                                source="Maricopa County Assessor",
                                scraped_at=datetime.now().isoformat()
                            )
                            properties.append(property_record)
                            self.stats["fetched"] += 1

                        except Exception as e:
                            logger.debug(f"Error parsing Maricopa County property: {e}")
                            self.stats["errors"] += 1
                            continue

                    logger.info(f"Maricopa County: Fetched {len(properties)} properties in {city}")
                    return properties

        except Exception as e:
            logger.error(f"Maricopa County scraper failed: {e}")
            self.stats["errors"] += 1
            return []


class CountyScraperOrchestrator:
    """
    Orchestrates all county assessor scrapers.
    Provides unified interface for property data collection.
    """

    def __init__(self):
        self.scrapers = {
            "harris": HarrisCountyScraper(),
            "dallas": DallasCountyScraper(),
            "tarrant": TarrantCountyScraper(),
            "maricopa": PhoenixAssessorScraper()
        }

    async def search_county_by_address(self, county: str, street_name: str, limit: int = 100) -> List[PropertyRecord]:
        """
        Search a specific county by street name.

        Args:
            county: County name (harris, dallas, tarrant, maricopa)
            street_name: Street name to search
            limit: Max properties to return

        Returns:
            List of PropertyRecord objects
        """
        county_lower = county.lower()
        if county_lower not in self.scrapers:
            logger.error(f"Unknown county: {county}. Available: {list(self.scrapers.keys())}")
            return []

        return await self.scrapers[county_lower].search_by_address(street_name, limit=limit)

    async def search_all_counties(self, search_term: str, limit_per_county: int = 100) -> Dict[str, List[PropertyRecord]]:
        """
        Search all counties in parallel.

        Args:
            search_term: Street name or search term
            limit_per_county: Max properties per county

        Returns:
            Dict mapping county name to list of properties
        """
        tasks = {
            county: scraper.search_by_address(search_term, limit=limit_per_county)
            for county, scraper in self.scrapers.items()
        }

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        county_data = {}
        for county, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"{county} scraper failed: {result}")
                county_data[county] = []
            else:
                county_data[county] = result

        total_properties = sum(len(props) for props in county_data.values())
        logger.info(f"Total properties scraped: {total_properties:,}")

        return county_data

    def get_stats(self) -> Dict[str, Dict]:
        """Get scraping statistics for all counties."""
        return {
            county: scraper.stats
            for county, scraper in self.scrapers.items()
        }


# Singleton instance
county_scraper = CountyScraperOrchestrator()
