"""
Discovered Sources Service
Loads the 2,300+ validated permit API endpoints discovered by openclaw_master_discovery
and makes them available for the sync pipeline and API queries.
"""

import csv
import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict

logger = logging.getLogger(__name__)

# Paths to discovery output files
_DATA_DIR = Path(__file__).parent.parent / "data"
DISCOVERY_CSV = _DATA_DIR / "MASTER_US_BUILDING_PERMIT_APIS.csv"
DISCOVERY_CACHE = _DATA_DIR / "openclaw_validated_datasets.json"


@dataclass
class DiscoveredAPI:
    """A single discovered permit API endpoint."""
    location: str
    state: str
    location_type: str  # city, county, state
    url: str
    api_type: str  # Socrata, ArcGIS, CKAN/csv, CKAN/json, CKAN/geojson
    dataset_name: str
    # Parsed fields for Socrata
    domain: str = ""
    resource_id: str = ""
    # Parsed fields for ArcGIS
    query_url: str = ""
    # Sample fields from validation
    sample_fields: List[str] = field(default_factory=list)
    # Whether already present in hardcoded config
    already_configured: bool = False


class DiscoveredSourcesRegistry:
    """
    Registry of all discovered permit API endpoints.
    Loads from the master CSV and provides lookup by location/state.
    """

    def __init__(self):
        self.apis: List[DiscoveredAPI] = []
        self.by_location: Dict[str, List[DiscoveredAPI]] = {}
        self.by_state: Dict[str, List[DiscoveredAPI]] = {}
        self.socrata_sources: List[DiscoveredAPI] = []
        self.arcgis_sources: List[DiscoveredAPI] = []
        self.ckan_sources: List[DiscoveredAPI] = []
        self._loaded = False

    def load(self, existing_socrata_ids: set = None, existing_arcgis_urls: set = None, force: bool = False):
        """Load discovered sources from the master CSV."""
        if self._loaded and not force:
            return

        if force:
            # Reset state for reload
            self.apis = []
            self.by_location = {}
            self.by_state = {}
            self.socrata_sources = []
            self.arcgis_sources = []
            self.ckan_sources = []

        existing_socrata_ids = existing_socrata_ids or set()
        existing_arcgis_urls = existing_arcgis_urls or set()

        if not DISCOVERY_CSV.exists():
            logger.warning(f"Discovery CSV not found at {DISCOVERY_CSV}")
            self._loaded = True
            return

        try:
            with open(DISCOVERY_CSV, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("has_permit_api", "").upper() != "YES":
                        continue

                    location = row.get("location_name", "").strip()
                    state = row.get("state", "").strip()
                    loc_type = row.get("location_type", "").strip()
                    portal = row.get("portal_domain", "").strip()

                    # Fix bad city names (dataset names used as location)
                    if _is_bad_city_name(location):
                        fixed = _city_from_domain(portal) if portal else ""
                        if not fixed:
                            # Try domain from first URL
                            first_url = row.get("api_url_1", "")
                            m = re.match(r"https?://([^/]+)", first_url)
                            if m:
                                fixed = _city_from_domain(m.group(1))
                        if fixed:
                            location = fixed

                    for i in range(1, 4):
                        url = row.get(f"api_url_{i}", "").strip()
                        api_type = row.get(f"api_type_{i}", "").strip()
                        dataset_name = row.get(f"dataset_name_{i}", "").strip()
                        validated = row.get(f"validated_{i}", "").strip()

                        if not url or validated.upper() != "YES":
                            continue

                        api = DiscoveredAPI(
                            location=location,
                            state=state,
                            location_type=loc_type,
                            url=url,
                            api_type=api_type,
                            dataset_name=dataset_name,
                        )

                        # Parse Socrata URLs
                        if api_type == "Socrata":
                            m = re.match(r"https?://([^/]+)/resource/([^/.]+)", url)
                            if m:
                                api.domain = m.group(1)
                                api.resource_id = m.group(2)
                                if api.resource_id in existing_socrata_ids:
                                    api.already_configured = True

                        # Parse ArcGIS URLs
                        elif api_type == "ArcGIS":
                            # Ensure URL ends with /query for FeatureServer/MapServer
                            query_url = url
                            if "/query" not in query_url:
                                if query_url.rstrip("/").endswith(("FeatureServer/0", "MapServer/0",
                                                                    "FeatureServer", "MapServer")):
                                    query_url = query_url.rstrip("/") + "/query"
                                elif re.search(r"/(Feature|Map)Server/\d+$", query_url):
                                    query_url = query_url.rstrip("/") + "/query"
                            api.query_url = query_url

                            # Check if already configured
                            normalized = url.split("?")[0].rstrip("/")
                            if normalized in existing_arcgis_urls:
                                api.already_configured = True

                        # Parse sample fields from CSV
                        fields_str = row.get("sample_fields_1", "")
                        if fields_str and i == 1:
                            api.sample_fields = [f.strip() for f in fields_str.split(",") if f.strip()]

                        self.apis.append(api)

                        # Index by location (skip unmatched datasets with no state)
                        if state:
                            key = f"{location}|{state}".lower()
                            self.by_location.setdefault(key, []).append(api)
                            self.by_state.setdefault(state.upper(), []).append(api)

                        # Categorize
                        if api_type == "Socrata" and not api.already_configured:
                            self.socrata_sources.append(api)
                        elif api_type == "ArcGIS" and not api.already_configured:
                            self.arcgis_sources.append(api)
                        elif "CKAN" in api_type:
                            self.ckan_sources.append(api)

            self._loaded = True
            new_count = sum(1 for a in self.apis if not a.already_configured)
            logger.info(
                f"Loaded {len(self.apis)} discovered APIs "
                f"({new_count} new, {len(self.apis) - new_count} already configured)"
            )
            logger.info(
                f"  Socrata: {len(self.socrata_sources)} new | "
                f"ArcGIS: {len(self.arcgis_sources)} new | "
                f"CKAN: {len(self.ckan_sources)}"
            )

        except Exception as e:
            logger.error(f"Failed to load discovery CSV: {e}")
            self._loaded = True

    def get_new_socrata_configs(self) -> Dict[str, dict]:
        """
        Return new Socrata sources formatted for the existing sync pipeline.
        Returns dict matching SOC_DATASETS format.
        Only includes datasets whose name indicates permit/construction/building data.
        """
        configs = {}
        seen = set()
        for api in self.socrata_sources:
            if api.already_configured or not api.domain or not api.resource_id:
                continue
            # Deduplicate by resource_id
            if api.resource_id in seen:
                continue
            # Filter: only permit/construction related datasets
            if not _is_permit_dataset(api.dataset_name):
                continue
            seen.add(api.resource_id)

            # Try to infer state from domain if missing
            state = api.state or _guess_state_from_domain(api.domain)

            # Detect date field from sample_fields
            date_field = _guess_date_field(api.sample_fields)

            safe_key = re.sub(r"[^a-z0-9_]", "_", f"disc_{api.location}_{state}_{api.resource_id[:8]}".lower())
            configs[safe_key] = {
                "label": f"{api.location} ({api.dataset_name[:30]})",
                "city": api.location,
                "state": state,
                "domain": api.domain,
                "resource_id": api.resource_id,
                "date_field": date_field,
                "lookback_days": 60,
                "discovered": True,
            }
        return configs

    def get_new_arcgis_configs(self) -> Dict[str, dict]:
        """
        Return new ArcGIS sources formatted for the existing sync pipeline.
        Returns dict matching ARCGIS_DATASETS format.
        Only includes datasets whose name indicates permit/construction/building data.
        """
        configs = {}
        seen = set()
        for api in self.arcgis_sources:
            if api.already_configured or not api.query_url or not api.state:
                continue
            # Filter: only permit/construction related datasets
            if not _is_permit_dataset(api.dataset_name):
                continue
            normalized = api.query_url.split("?")[0].rstrip("/")
            if normalized in seen:
                continue
            seen.add(normalized)

            safe_key = re.sub(r"[^a-z0-9_]", "_", f"disc_{api.location}_{api.state}".lower())
            # Avoid key collisions
            if safe_key in configs:
                safe_key += f"_{len(configs)}"

            configs[safe_key] = {
                "label": f"{api.location} ({api.dataset_name[:30]})",
                "city": api.location,
                "state": api.state,
                "url": api.query_url,
                "date_field": None,
                "order_by": "",
                "lookback_days": 90,
                "discovered": True,
            }
        return configs

    def search(self, city: str = "", state: str = "", location_type: str = "") -> List[dict]:
        """Search discovered sources by city/state/type."""
        results = []
        for api in self.apis:
            if city and city.lower() not in api.location.lower():
                continue
            if state and state.upper() != api.state.upper():
                continue
            if location_type and location_type.lower() != api.location_type.lower():
                continue
            results.append(asdict(api))
        return results

    def get_stats(self) -> dict:
        """Get summary statistics."""
        return {
            "total_apis": len(self.apis),
            "new_apis": sum(1 for a in self.apis if not a.already_configured),
            "already_configured": sum(1 for a in self.apis if a.already_configured),
            "new_socrata": len(self.socrata_sources),
            "new_arcgis": len(self.arcgis_sources),
            "new_ckan": len(self.ckan_sources),
            "unique_locations": len(self.by_location),
            "states_covered": len(self.by_state),
            "csv_path": str(DISCOVERY_CSV),
            "csv_exists": DISCOVERY_CSV.exists(),
        }


def _is_permit_dataset(name: str) -> bool:
    """Check if a dataset name indicates permit/construction/building data."""
    if not name:
        return False
    n = name.lower()
    # Must contain at least one permit/construction keyword
    permit_keywords = [
        "permit", "building", "construction", "inspection", "demolition",
        "certificate of occupancy", "code enforcement", "code violation",
        "zoning", "planning", "development", "renovation", "remodel",
        "electrical permit", "plumbing permit", "mechanical permit",
        "roofing", "hvac permit", "fire permit", "sign permit",
        "grading permit", "excavation", "site plan",
        "housing", "property maintenance", "unsafe building",
        "insurance claim", "property damage", "flood claim",
        "property sale", "deed transfer", "real estate",
        "assessed value", "property assessment",
    ]
    # Exclude known junk datasets
    junk_keywords = [
        "311", "pot hole", "pothole", "abandoned vehicle", "sanitation",
        "tobacco", "food service", "food inspection", "restaurant",
        "grass cutting", "tree removal", "graffiti", "noise complaint",
        "animal", "rat ", "rodent", "mosquito", "parking ticket",
        "traffic", "bus route", "transit", "bike", "library",
        "election", "voter", "census", "crime", "police incident",
        "fire incident", "accident", "taxi", "tow",
        "adjudication", "business license", "liquor license",
        "lobbyist", "campaign", "budget", "payroll", "employee",
        "school", "student", "hospital", "health clinic",
        "wifi", "internet", "energy benchmark", "greenhouse",
        "beach", "pool", "recreation", "parcel address",
        "tax parcel", "micro-market", "rental property registration",
        "retail tobacco", "city-owned land", "environmental complaint",
        "environmental inspection", "fire department occupancy",
        "property database", "explorer", "code complaint",
        "suites (completed", "applicant", "occupancy inspection",
        "lot abatement", "sidewalk", "street light", "water main",
        "sewer", "hydrant", "park ", "playground",
    ]
    if any(junk in n for junk in junk_keywords):
        return False
    return any(kw in n for kw in permit_keywords)


def _guess_state_from_domain(domain: str) -> str:
    """Infer US state from a Socrata domain name."""
    d = domain.lower()
    # Direct state data portals
    state_map = {
        "data.ct.gov": "CT", "data.ny.gov": "NY", "data.ca.gov": "CA",
        "data.texas.gov": "TX", "data.colorado.gov": "CO", "data.iowa.gov": "IA",
        "data.illinois.gov": "IL", "data.maryland.gov": "MD", "data.virginia.gov": "VA",
        "data.pa.gov": "PA", "data.wa.gov": "WA", "data.oregon.gov": "OR",
        "data.michigan.gov": "MI", "data.mo.gov": "MO", "data.nj.gov": "NJ",
        "data.mass.gov": "MA", "data.fl.gov": "FL", "data.georgia.gov": "GA",
        "data.ohio.gov": "OH", "data.in.gov": "IN", "data.wi.gov": "WI",
        "data.mn.gov": "MN", "data.ky.gov": "KY", "data.de.gov": "DE",
        "data.ri.gov": "RI", "data.arkansas.gov": "AR", "data.alabama.gov": "AL",
        "data.tennessee.gov": "TN", "data.ok.gov": "OK", "data.ks.gov": "KS",
        "data.nebraska.gov": "NE", "data.nd.gov": "ND", "data.sd.gov": "SD",
        "data.idaho.gov": "ID", "data.mt.gov": "MT", "data.wyo.gov": "WY",
        "data.nv.gov": "NV", "data.nm.gov": "NM", "data.utah.gov": "UT",
        "data.az.gov": "AZ", "data.hawaii.gov": "HI", "data.alaska.gov": "AK",
        "data.wv.gov": "WV", "data.sc.gov": "SC", "data.nc.gov": "NC",
        "data.ms.gov": "MS", "data.la.gov": "LA", "data.vermont.gov": "VT",
        "data.nh.gov": "NH", "data.maine.gov": "ME",
    }
    if d in state_map:
        return state_map[d]
    # City domains with state hints
    city_state = {
        "cityofnewyork.us": "NY", "cityofchicago.us": "IL", "lacity.org": "CA",
        "sfgov.org": "CA", "seattle.gov": "WA", "austintexas.gov": "TX",
        "boston.gov": "MA", "detroitmi.gov": "MI", "nashville.gov": "TN",
        "louisvilleky.gov": "KY", "raleighnc.gov": "NC", "sandiego.gov": "CA",
        "houstontx.gov": "TX", "fortworthtexas.gov": "TX", "providenceri.gov": "RI",
        "cincinnati-oh.gov": "OH", "nola.gov": "LA", "stlouis-mo.gov": "MO",
        "buffalony.gov": "NY", "oaklandca.gov": "CA", "honolulu.gov": "HI",
        "kcmo.org": "MO", "sanjoseca.gov": "CA", "sanantonio.gov": "TX",
        "mesaaz.gov": "AZ", "tucsonaz.gov": "AZ", "lacounty.gov": "CA",
        "miamidade.gov": "FL", "cookcountyil.gov": "IL", "kingcounty.gov": "WA",
        "princegeorgescountymd.gov": "MD", "montgomerycountymd.gov": "MD",
        "acgov.org": "CA", "dallasopendata.com": "TX", "gainesville.org": "FL",
        "cambridgema.gov": "MA", "cityoforlando.net": "FL", "memphis.gov": "TN",
        "jacksonms.gov": "MS", "cityoftacoma.org": "WA", "oaklandnet.com": "CA",
        "wichita.gov": "KS", "elpasotexas.gov": "TX", "chattanooga.gov": "TN",
        "knoxvilletn.gov": "TN", "norfolk.gov": "VA", "richmondgov.com": "VA",
        "virginia-beach.gov": "VA", "tallahassee.com": "FL", "smgov.net": "CA",
        "longbeach.gov": "CA", "elkgrovecity.org": "CA", "costamesaca.gov": "CA",
        "cityofpasadena.net": "CA", "culvercity.org": "CA", "burbank.com": "CA",
        "beverlyhills.org": "CA", "santamonicaca.gov": "CA",
    }
    for suffix, state in city_state.items():
        if suffix in d:
            return state
    return ""


def _is_bad_city_name(name: str) -> bool:
    """Detect location names that are actually dataset names, not real cities."""
    n = name.lower()
    bad_patterns = [
        "building and safety", "code enforcement", "permits submitted",
        "permits issued", "planning permits", "permit data",
        "construction permit", "residential permit", "commercial permit",
        "certificate of", "inspection", "violation",
        " - ", " (", "dataset", "no longer updat",
    ]
    if any(p in n for p in bad_patterns):
        return True
    # Pure state names used as city
    states = {
        "alabama","alaska","arizona","arkansas","california","colorado",
        "connecticut","delaware","florida","georgia","hawaii","idaho",
        "illinois","indiana","iowa","kansas","kentucky","louisiana","maine",
        "maryland","massachusetts","michigan","minnesota","mississippi",
        "missouri","montana","nebraska","nevada","new hampshire","new jersey",
        "new mexico","new york state","north carolina","north dakota","ohio",
        "oklahoma","oregon","pennsylvania","rhode island","south carolina",
        "south dakota","tennessee","texas","utah","vermont","virginia",
        "washington","west virginia","wisconsin","wyoming",
    }
    if n in states:
        return True
    # Names > 40 chars are likely dataset names
    if len(name) > 40:
        return True
    return False


def _city_from_domain(domain: str) -> str:
    """Extract a real city name from a Socrata/ArcGIS domain."""
    d = domain.lower().replace("www.", "")
    # Known domain → city mappings
    domain_city = {
        "data.lacity.org": "Los Angeles",
        "data.cityofnewyork.us": "New York",
        "data.cityofchicago.us": "Chicago",
        "data.sfgov.org": "San Francisco",
        "data.seattle.gov": "Seattle",
        "data.austintexas.gov": "Austin",
        "data.boston.gov": "Boston",
        "data.detroitmi.gov": "Detroit",
        "data.nashville.gov": "Nashville",
        "data.louisvilleky.gov": "Louisville",
        "data.raleighnc.gov": "Raleigh",
        "data.sandiego.gov": "San Diego",
        "data.houstontx.gov": "Houston",
        "data.fortworthtexas.gov": "Fort Worth",
        "data.providenceri.gov": "Providence",
        "data.cincinnati-oh.gov": "Cincinnati",
        "data.nola.gov": "New Orleans",
        "data.stlouis-mo.gov": "St. Louis",
        "data.buffalony.gov": "Buffalo",
        "data.oaklandca.gov": "Oakland",
        "data.honolulu.gov": "Honolulu",
        "data.kcmo.org": "Kansas City",
        "data.sanjoseca.gov": "San Jose",
        "data.sanantonio.gov": "San Antonio",
        "data.mesaaz.gov": "Mesa",
        "data.tucsonaz.gov": "Tucson",
        "data.lacounty.gov": "Los Angeles County",
        "data.miamidade.gov": "Miami",
        "data.cookcountyil.gov": "Cook County",
        "data.kingcounty.gov": "King County",
        "data.princegeorgescountymd.gov": "Prince George's County",
        "data.montgomerycountymd.gov": "Montgomery County",
        "data.dallasopendata.com": "Dallas",
        "data.gainesville.org": "Gainesville",
        "data.cambridgema.gov": "Cambridge",
        "data.cityoforlando.net": "Orlando",
        "data.memphis.gov": "Memphis",
        "data.chattanooga.gov": "Chattanooga",
        "data.knoxvilletn.gov": "Knoxville",
        "data.norfolk.gov": "Norfolk",
        "data.richmondgov.com": "Richmond",
        "data.tallahassee.com": "Tallahassee",
        "data.smgov.net": "Santa Monica",
        "data.longbeach.gov": "Long Beach",
        "data.littlerock.gov": "Little Rock",
        "usc.data.socrata.com": "Los Angeles",
    }
    if d in domain_city:
        return domain_city[d]
    # Try extracting city from domain pattern: data.{cityname}.gov/org
    m = re.match(r"data\.([a-z]+(?:city|town|village)?)\.", d)
    if m:
        raw = m.group(1)
        # Remove common suffixes
        for suffix in ("gov", "org", "net", "com", "city", "data"):
            raw = raw.replace(suffix, "")
        if len(raw) >= 3:
            return raw.replace("_", " ").replace("-", " ").title()
    # Try: {cityname}opendata.com or {cityname}.gov
    m = re.match(r"(?:data\.)?([a-z]+?)(?:opendata|data)\.", d)
    if m and len(m.group(1)) >= 3:
        return m.group(1).replace("_", " ").replace("-", " ").title()
    return ""


def _guess_date_field(fields: List[str]) -> Optional[str]:
    """Guess the date field from a list of field names."""
    date_keywords = [
        "issue_date", "issued_date", "issueddate", "issuedate",
        "permit_date", "filing_date", "filed_date",
        "application_date", "applied_date", "applieddate",
        "created_date", "date_issued", "permit_issue_date",
        "permitissuedate", "issuance_date", "date_entered",
        "application_start_date", "statusdate",
    ]
    lower_fields = {f.lower(): f for f in fields}
    for kw in date_keywords:
        if kw in lower_fields:
            return lower_fields[kw]
    # Partial match
    for f_lower, f_orig in lower_fields.items():
        if "date" in f_lower and ("issue" in f_lower or "filed" in f_lower or "permit" in f_lower):
            return f_orig
    return None


# Singleton
_registry: Optional[DiscoveredSourcesRegistry] = None


def get_discovered_registry() -> DiscoveredSourcesRegistry:
    """Get or create the singleton registry. Auto-loads from CSV on first access."""
    global _registry
    if _registry is None:
        _registry = DiscoveredSourcesRegistry()
        _registry.load()
    return _registry
