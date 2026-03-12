"""
Onsite - Parcel Data Service
Nationwide parcel boundary and property data via Regrid API
"""

import os
import aiohttp
from typing import Dict, Optional, List
from datetime import datetime, timedelta

class RegridParcelService:
    """
    Regrid API for nationwide parcel data
    Pricing: $0.02 per parcel lookup
    100K lookups = $2,000/month
    """

    def __init__(self):
        self.api_key = os.getenv("REGRID_API_KEY", "")
        self.base_url = "https://app.regrid.com/api/v1"
        self.cache = {}
        self.cache_ttl = timedelta(days=90)  # Parcel data changes slowly

    async def get_parcel_by_coordinates(self, lat: float, lng: float) -> Dict:
        """Get parcel data by lat/lng coordinates"""
        if not self.api_key:
            return {
                "success": False,
                "error": "Regrid API key not configured",
                "message": "Set REGRID_API_KEY environment variable"
            }

        # Check cache first
        cache_key = f"{lat:.6f},{lng:.6f}"
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            if datetime.now() - cached_data["cached_at"] < self.cache_ttl:
                cached_data["cached"] = True
                return cached_data["data"]

        url = f"{self.base_url}/parcel"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        params = {
            "lat": lat,
            "lon": lng,
            "return_geometry": "true"  # Include GeoJSON boundary
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = self._parse_regrid_response(data)

                    # Cache the result
                    self.cache[cache_key] = {
                        "data": result,
                        "cached_at": datetime.now()
                    }

                    return result
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }

    async def get_parcel_by_address(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: str = ""
    ) -> Dict:
        """Get parcel data by street address"""
        if not self.api_key:
            return {
                "success": False,
                "error": "Regrid API key not configured"
            }

        # Check cache first
        cache_key = f"{address}:{city}:{state}"
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            if datetime.now() - cached_data["cached_at"] < self.cache_ttl:
                cached_data["cached"] = True
                return cached_data["data"]

        url = f"{self.base_url}/parcel"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        params = {
            "address": f"{address}, {city}, {state} {zip_code}",
            "return_geometry": "true"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = self._parse_regrid_response(data)

                    # Cache the result
                    self.cache[cache_key] = {
                        "data": result,
                        "cached_at": datetime.now()
                    }

                    return result
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }

    async def get_parcel_by_apn(self, apn: str, state: str, county: str = "") -> Dict:
        """Get parcel data by Assessor Parcel Number (APN)"""
        if not self.api_key:
            return {
                "success": False,
                "error": "Regrid API key not configured"
            }

        url = f"{self.base_url}/parcel"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        params = {
            "apn": apn,
            "state": state,
            "return_geometry": "true"
        }

        if county:
            params["county"] = county

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return self._parse_regrid_response(data)
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }

    def _parse_regrid_response(self, data: Dict) -> Dict:
        """Parse Regrid API response into standardized format"""
        if not data or "results" not in data or len(data["results"]) == 0:
            return {
                "success": False,
                "error": "No parcel found"
            }

        parcel = data["results"][0]  # Get first result
        properties = parcel.get("properties", {})
        geometry = parcel.get("geometry", {})

        return {
            "success": True,
            "cached": False,
            "apn": properties.get("apn"),
            "address": properties.get("address"),
            "city": properties.get("city"),
            "state": properties.get("state"),
            "zip": properties.get("zip"),
            "county": properties.get("county"),

            # Property details
            "owner_name": properties.get("owner"),
            "owner_address": properties.get("owner_address"),
            "land_use": properties.get("land_use"),
            "zoning": properties.get("zoning"),

            # Measurements
            "lot_size_sqft": properties.get("lot_size_sq_ft"),
            "lot_size_acres": properties.get("lot_size_acres"),
            "building_sqft": properties.get("building_area"),

            # Valuation
            "assessed_value": properties.get("assessed_value"),
            "market_value": properties.get("market_value"),
            "land_value": properties.get("land_value"),
            "improvement_value": properties.get("improvement_value"),

            # Tax information
            "tax_amount": properties.get("tax_amount"),
            "tax_year": properties.get("tax_year"),

            # Physical attributes
            "year_built": properties.get("year_built"),
            "bedrooms": properties.get("bedrooms"),
            "bathrooms": properties.get("bathrooms"),
            "stories": properties.get("stories"),

            # Geospatial data
            "geometry": geometry,  # GeoJSON polygon
            "centroid_lat": properties.get("latitude"),
            "centroid_lng": properties.get("longitude"),

            # Metadata
            "data_source": "Regrid",
            "last_updated": properties.get("updated_at")
        }

    async def get_bulk_parcels(self, coordinates: List[Dict]) -> Dict:
        """Get multiple parcels in one request (batch operation)"""
        results = []
        success_count = 0
        fail_count = 0

        for coord in coordinates:
            result = await self.get_parcel_by_coordinates(
                lat=coord["lat"],
                lng=coord["lng"]
            )
            results.append(result)

            if result["success"]:
                success_count += 1
            else:
                fail_count += 1

        return {
            "success": fail_count == 0,
            "total": len(coordinates),
            "found": success_count,
            "not_found": fail_count,
            "results": results
        }

    async def get_neighboring_parcels(
        self,
        center_lat: float,
        center_lng: float,
        radius_meters: int = 100
    ) -> Dict:
        """Get all parcels within radius of a point"""
        if not self.api_key:
            return {
                "success": False,
                "error": "Regrid API key not configured"
            }

        url = f"{self.base_url}/parcels"  # Note: plural endpoint
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        params = {
            "lat": center_lat,
            "lon": center_lng,
            "radius": radius_meters,
            "return_geometry": "true"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "success": True,
                        "count": len(data.get("results", [])),
                        "parcels": [self._parse_regrid_response({"results": [p]}) for p in data.get("results", [])]
                    }
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }


# ═══════════════════════════════════════════════════════════════════════════
# COUNTY ASSESSOR FALLBACK SERVICE
# ═══════════════════════════════════════════════════════════════════════════

class CountyAssessorService:
    """
    Free county assessor APIs as fallback to Regrid
    Covers major counties with public APIs
    """

    async def get_parcel_la_county(self, apn: str = "", lat: float = None, lng: float = None) -> Dict:
        """LA County Assessor parcel data (FREE)"""
        base_url = "https://maps.assessment.lacounty.gov/GVH_2_2/GVH/wsLegacyService"

        if lat and lng:
            url = f"{base_url}/getParcelByLocation"
            params = {"type": "json", "lat": lat, "lng": lng}
        elif apn:
            url = f"{base_url}/getParcelByAin"
            params = {"type": "json", "ain": apn}
        else:
            return {"success": False, "error": "Either APN or lat/lng required"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "success": True,
                        "data_source": "LA County Assessor (FREE)",
                        **data
                    }
                else:
                    return {"success": False, "error": await resp.text()}

    # Add more county assessor APIs here as needed
    # - Orange County, CA
    # - San Diego County, CA
    # - Cook County, IL (Chicago)
    # - Maricopa County, AZ (Phoenix)
    # etc.
