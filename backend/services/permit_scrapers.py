"""
Onsite - Permit Scrapers
Direct scraping for cities without Socrata APIs
Supports: Austin, Seattle, San Francisco
"""

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)


@dataclass
class PermitRecord:
    """Standardized permit record from any scraper"""
    permit_number: str
    issue_date: str
    address: str
    city: str
    state: str
    zip_code: str
    permit_type: str
    work_description: str
    valuation: float
    contractor_name: str = ""
    contractor_phone: str = ""
    owner_name: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    status: str = "Issued"
    source: str = ""
    scraped_at: str = ""


class AustinPermitScraper:
    """
    Austin, TX Building Permits
    Source: Austin Open Data Portal
    API: https://data.austintexas.gov/resource/3syk-w9eu.json
    """

    def __init__(self):
        self.base_url = "https://data.austintexas.gov/resource/3syk-w9eu.json"
        self.city = "Austin"
        self.state = "TX"
        self.stats = {"fetched": 0, "errors": 0}

    async def scrape(self, days_back: int = 30, limit: int = 5000) -> List[PermitRecord]:
        """
        Scrape Austin permits from the last N days.

        Args:
            days_back: How many days to look back
            limit: Max permits to fetch

        Returns:
            List of PermitRecord objects
        """
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        params = {
            "$where": f"issued_date >= '{cutoff_date}'",
            "$limit": limit,
            "$order": "issued_date DESC"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params,
                                      timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.error(f"Austin API returned {resp.status}")
                        self.stats["errors"] += 1
                        return []

                    data = await resp.json()
                    permits = []

                    for record in data:
                        try:
                            permit = PermitRecord(
                                permit_number=record.get("permit_number", ""),
                                issue_date=record.get("issued_date", "")[:10] if record.get("issued_date") else "",
                                address=record.get("original_address1", ""),
                                city=self.city,
                                state=self.state,
                                zip_code=record.get("original_zip", ""),
                                permit_type=record.get("permit_type_desc", "Building"),
                                work_description=record.get("work_description", ""),
                                valuation=float(record.get("total_valuation_remodel", 0) or 0),
                                contractor_name=record.get("contractor_trade_name", ""),
                                contractor_phone=record.get("contractor_phone", ""),
                                owner_name=record.get("applicant_full_name", ""),
                                latitude=float(record["latitude"]) if record.get("latitude") else None,
                                longitude=float(record["longitude"]) if record.get("longitude") else None,
                                status=record.get("status_current", "Issued"),
                                source="Austin Open Data",
                                scraped_at=datetime.now().isoformat()
                            )
                            permits.append(permit)
                            self.stats["fetched"] += 1
                        except Exception as e:
                            logger.debug(f"Error parsing Austin permit: {e}")
                            self.stats["errors"] += 1
                            continue

                    logger.info(f"Austin: Scraped {len(permits)} permits from last {days_back} days")
                    return permits

        except Exception as e:
            logger.error(f"Austin scraper failed: {e}")
            self.stats["errors"] += 1
            return []


class SeattlePermitScraper:
    """
    Seattle, WA Building Permits
    Source: Seattle Open Data Portal
    API: https://data.seattle.gov/resource/76t5-zqzr.json
    """

    def __init__(self):
        self.base_url = "https://data.seattle.gov/resource/76t5-zqzr.json"
        self.city = "Seattle"
        self.state = "WA"
        self.stats = {"fetched": 0, "errors": 0}

    async def scrape(self, days_back: int = 30, limit: int = 5000) -> List[PermitRecord]:
        """Scrape Seattle permits from the last N days."""
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        params = {
            "$where": f"application_date_str >= '{cutoff_date}'",
            "$limit": limit,
            "$order": "application_date_str DESC"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params,
                                      timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.error(f"Seattle API returned {resp.status}")
                        self.stats["errors"] += 1
                        return []

                    data = await resp.json()
                    permits = []

                    for record in data:
                        try:
                            # Extract address components
                            address = record.get("original_address1", "")
                            zip_code = record.get("original_zip", "")

                            permit = PermitRecord(
                                permit_number=record.get("permit_and_complaint_status_url", "").split("/")[-1] if record.get("permit_and_complaint_status_url") else "",
                                issue_date=record.get("application_date_str", "")[:10] if record.get("application_date_str") else "",
                                address=address,
                                city=self.city,
                                state=self.state,
                                zip_code=zip_code,
                                permit_type=record.get("permit_type", "Building"),
                                work_description=record.get("description", ""),
                                valuation=float(record.get("value", 0) or 0),
                                contractor_name=record.get("contractor", ""),
                                contractor_phone="",
                                owner_name=record.get("applicant_name", ""),
                                latitude=float(record["latitude"]) if record.get("latitude") else None,
                                longitude=float(record["longitude"]) if record.get("longitude") else None,
                                status=record.get("status", "Issued"),
                                source="Seattle Open Data",
                                scraped_at=datetime.now().isoformat()
                            )
                            permits.append(permit)
                            self.stats["fetched"] += 1
                        except Exception as e:
                            logger.debug(f"Error parsing Seattle permit: {e}")
                            self.stats["errors"] += 1
                            continue

                    logger.info(f"Seattle: Scraped {len(permits)} permits from last {days_back} days")
                    return permits

        except Exception as e:
            logger.error(f"Seattle scraper failed: {e}")
            self.stats["errors"] += 1
            return []


class SanFranciscoPermitScraper:
    """
    San Francisco, CA Building Permits
    Source: SF Open Data Portal
    API: https://data.sfgov.org/resource/i98e-djp9.json
    """

    def __init__(self):
        self.base_url = "https://data.sfgov.org/resource/i98e-djp9.json"
        self.city = "San Francisco"
        self.state = "CA"
        self.stats = {"fetched": 0, "errors": 0}

    async def scrape(self, days_back: int = 30, limit: int = 5000) -> List[PermitRecord]:
        """Scrape San Francisco permits from the last N days."""
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        params = {
            "$where": f"issued_date >= '{cutoff_date}'",
            "$limit": limit,
            "$order": "issued_date DESC"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params,
                                      timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.error(f"San Francisco API returned {resp.status}")
                        self.stats["errors"] += 1
                        return []

                    data = await resp.json()
                    permits = []

                    for record in data:
                        try:
                            # Parse address
                            address = record.get("street_number", "") + " " + record.get("street_name", "")
                            address = address.strip()

                            permit = PermitRecord(
                                permit_number=record.get("permit_number", ""),
                                issue_date=record.get("issued_date", "")[:10] if record.get("issued_date") else "",
                                address=address,
                                city=self.city,
                                state=self.state,
                                zip_code=record.get("zipcode", ""),
                                permit_type=record.get("permit_type_definition", "Building"),
                                work_description=record.get("description", ""),
                                valuation=float(record.get("estimated_cost", 0) or 0),
                                contractor_name="",
                                contractor_phone="",
                                owner_name="",
                                latitude=float(record["location"]["latitude"]) if record.get("location") and isinstance(record["location"], dict) and "latitude" in record["location"] else None,
                                longitude=float(record["location"]["longitude"]) if record.get("location") and isinstance(record["location"], dict) and "longitude" in record["location"] else None,
                                status=record.get("status", "Issued"),
                                source="SF Open Data",
                                scraped_at=datetime.now().isoformat()
                            )
                            permits.append(permit)
                            self.stats["fetched"] += 1
                        except Exception as e:
                            logger.debug(f"Error parsing SF permit: {e}")
                            self.stats["errors"] += 1
                            continue

                    logger.info(f"San Francisco: Scraped {len(permits)} permits from last {days_back} days")
                    return permits

        except Exception as e:
            logger.error(f"San Francisco scraper failed: {e}")
            self.stats["errors"] += 1
            return []


class PermitScraperOrchestrator:
    """
    Orchestrates all permit scrapers.
    Run all scrapers in parallel and combine results.
    """

    def __init__(self):
        self.scrapers = {
            "austin": AustinPermitScraper(),
            "seattle": SeattlePermitScraper(),
            "san_francisco": SanFranciscoPermitScraper()
        }

    async def scrape_all(self, days_back: int = 30, limit_per_city: int = 5000) -> Dict[str, List[PermitRecord]]:
        """
        Run all scrapers in parallel.

        Args:
            days_back: How many days to look back
            limit_per_city: Max permits per city

        Returns:
            Dict mapping city name to list of permits
        """
        tasks = {
            city: scraper.scrape(days_back=days_back, limit=limit_per_city)
            for city, scraper in self.scrapers.items()
        }

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        scraped_data = {}
        for city, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"{city} scraper failed: {result}")
                scraped_data[city] = []
            else:
                scraped_data[city] = result

        total_permits = sum(len(permits) for permits in scraped_data.values())
        logger.info(f"Total permits scraped: {total_permits:,}")

        return scraped_data

    async def scrape_city(self, city: str, days_back: int = 30, limit: int = 5000) -> List[PermitRecord]:
        """Scrape a specific city."""
        if city not in self.scrapers:
            logger.error(f"Unknown city: {city}. Available: {list(self.scrapers.keys())}")
            return []

        return await self.scrapers[city].scrape(days_back=days_back, limit=limit)

    def get_stats(self) -> Dict[str, Dict]:
        """Get scraping statistics for all cities."""
        return {
            city: scraper.stats
            for city, scraper in self.scrapers.items()
        }


# Singleton instance
permit_scraper = PermitScraperOrchestrator()
