"""
Shovels API Integration
https://docs.shovels.ai/
https://shovels.redoc.ly/

Provides building permit records, contractor profiles, and property details
from 2,000+ jurisdictions nationwide across the United States.
"""

import os
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import aiohttp
import asyncio

logger = logging.getLogger(__name__)


class ShovelsAPI:
    """Client for Shovels API v2"""

    BASE_URL = "https://api.shovels.ai/v2"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Shovels API client.

        Args:
            api_key: Shovels API key. If not provided, reads from SHOVELS_API_KEY env var.
        """
        self.api_key = api_key or os.getenv("SHOVELS_API_KEY", "")
        self.enabled = os.getenv("SHOVELS_ENABLED", "1") == "1"

        if not self.api_key:
            logger.warning("⚠️  Shovels API key not configured (SHOVELS_API_KEY)")
            self.enabled = False

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with API key."""
        return {
            "X-API-Key": self.api_key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    async def search_permits(
        self,
        city: Optional[str] = None,
        state: Optional[str] = None,
        county: Optional[str] = None,
        zip_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        page: int = 1
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Search permits using Shovels API.

        Args:
            city: City name
            state: State abbreviation (e.g., "CA")
            county: County name
            zip_code: ZIP code
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            tags: List of permit tags (e.g., ["solar", "heat_pump"])
            limit: Max results per page (default 100)
            page: Page number (default 1)

        Returns:
            Tuple of (permits list, error message if any)
        """
        if not self.enabled:
            return [], "Shovels API disabled"

        url = f"{self.BASE_URL}/permits/search"

        # Build query parameters
        params = {
            "page": page,
            "size": min(limit, 1000)  # Shovels max is 1000 per request
        }

        if city:
            params["city"] = city
        if state:
            params["state"] = state.upper()
        if county:
            params["county"] = county
        if zip_code:
            params["zip_code"] = zip_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if tags:
            params["tags"] = ",".join(tags)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        permits = data.get("results", [])
                        logger.info(f"✅ Shovels API: Retrieved {len(permits)} permits for {city or county or state or 'query'}")
                        return permits, None
                    elif response.status == 401:
                        error = "Shovels API: Invalid API key"
                        logger.error(error)
                        return [], error
                    elif response.status == 429:
                        error = "Shovels API: Rate limit exceeded"
                        logger.warning(error)
                        return [], error
                    else:
                        error = f"Shovels API error: HTTP {response.status}"
                        error_text = await response.text()
                        logger.error(f"{error} - {error_text}")
                        return [], error

        except asyncio.TimeoutError:
            error = "Shovels API: Request timeout"
            logger.error(error)
            return [], error
        except Exception as e:
            error = f"Shovels API error: {str(e)}"
            logger.error(error)
            return [], error

    async def get_permit_by_id(self, permit_id: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Get a single permit by ID.

        Args:
            permit_id: Shovels permit ID

        Returns:
            Tuple of (permit dict or None, error message if any)
        """
        if not self.enabled:
            return None, "Shovels API disabled"

        url = f"{self.BASE_URL}/permits/{permit_id}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=self._get_headers(),
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        permit = await response.json()
                        return permit, None
                    else:
                        error = f"Shovels API error: HTTP {response.status}"
                        logger.error(error)
                        return None, error

        except Exception as e:
            error = f"Shovels API error: {str(e)}"
            logger.error(error)
            return None, error

    async def search_contractors(
        self,
        city: Optional[str] = None,
        state: Optional[str] = None,
        name: Optional[str] = None,
        limit: int = 50
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Search contractors.

        Args:
            city: City name
            state: State abbreviation
            name: Contractor name
            limit: Max results

        Returns:
            Tuple of (contractors list, error message if any)
        """
        if not self.enabled:
            return [], "Shovels API disabled"

        url = f"{self.BASE_URL}/contractors/search"

        params = {"size": min(limit, 1000)}
        if city:
            params["city"] = city
        if state:
            params["state"] = state.upper()
        if name:
            params["name"] = name

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        contractors = data.get("results", [])
                        logger.info(f"✅ Shovels API: Retrieved {len(contractors)} contractors")
                        return contractors, None
                    else:
                        error = f"Shovels API error: HTTP {response.status}"
                        logger.error(error)
                        return [], error

        except Exception as e:
            error = f"Shovels API error: {str(e)}"
            logger.error(error)
            return [], error


def transform_shovels_permit_to_lead(permit: Dict) -> Dict:
    """
    Transform Shovels API permit into Onsite lead format.

    Args:
        permit: Shovels permit dict

    Returns:
        Onsite lead dict
    """
    # Extract basic fields
    permit_number = permit.get("permit_number", "")
    address = permit.get("address", "")
    city = permit.get("city", "")
    state = permit.get("state", "")
    zip_code = permit.get("zip_code", "")

    # Parse coordinates
    lat = float(permit.get("latitude", 0) or 0)
    lng = float(permit.get("longitude", 0) or 0)

    # Parse valuation
    valuation = float(permit.get("valuation", 0) or 0)

    # Parse dates
    issue_date_str = permit.get("issue_date") or permit.get("filed_date") or ""
    issue_date = ""
    days_old = 0

    if issue_date_str:
        try:
            issue_dt = datetime.fromisoformat(issue_date_str.replace("Z", "+00:00"))
            issue_date = issue_dt.strftime("%Y-%m-%d")
            days_old = (datetime.utcnow() - issue_dt.replace(tzinfo=None)).days
        except Exception as e:
            logger.warning("Failed to parse issue date '%s': %s", issue_date_str, e)

    # Calculate score
    score = 50  # Base score
    if valuation > 100000:
        score += 20
    if days_old < 30:
        score += 15
    if days_old < 7:
        score += 15

    # Determine temperature
    if score >= 70:
        temperature = "hot"
    elif score >= 50:
        temperature = "warm"
    else:
        temperature = "cold"

    # Extract permit type
    permit_type = permit.get("permit_type") or permit.get("work_type") or "Building"
    description = permit.get("description") or permit.get("work_description") or ""

    # Extract contractor info
    contractor_name = permit.get("contractor_name", "")
    contractor_phone = permit.get("contractor_phone", "")

    # Build lead object
    lead = {
        "permit_number": permit_number,
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "lat": lat,
        "lng": lng,
        "work_description": permit_type,
        "description_full": description,
        "permit_type": permit_type,
        "valuation": valuation,
        "issue_date": issue_date,
        "days_old": days_old,
        "score": score,
        "temperature": temperature,
        "urgency": "HIGH" if score >= 70 else "MEDIUM" if score >= 50 else "LOW",
        "source": f"Shovels ({city}, {state})",
        "contractor_name": contractor_name,
        "contractor_phone": contractor_phone,
        "contractor_email": "",
        "owner_name": permit.get("property_owner", ""),
        "owner_phone": "",
        "owner_email": "",
        "apn": permit.get("apn", ""),
        "permit_url": permit.get("url", ""),
        "enriched_attempted": False,
        "enriched_success": False,
    }

    return lead


# Global instance
shovels_api = ShovelsAPI()
