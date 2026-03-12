"""
Onsite - Production Backend
Real API integrations, data syncing, geocoding
"""

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel
from typing import List, Optional, Dict
import aiohttp
import asyncio
from datetime import datetime, timedelta
import os
import json
import logging
import csv
from pathlib import Path
import time
import threading
import secrets
import re
import hmac
import requests
from collections import Counter
try:
    import orjson
    _HAS_ORJSON = True
except ImportError:
    _HAS_ORJSON = False
from urllib.parse import urlencode
from requests import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Source registry and sync orchestrator for 4,000+ API sources
try:
    from backend.models.source_registry import (
        init_source_tables,
        import_from_csv,
        get_source_count,
        get_sources_paginated,
        get_registry_stats,
        toggle_source,
    )
    from backend.services.sync_orchestrator import SyncOrchestrator
    _HAS_SOURCE_REGISTRY = True
except Exception as _sr_err:
    logger.warning(f"Source registry not available: {_sr_err}")
    _HAS_SOURCE_REGISTRY = False

try:
    from backend.services.zip_lookup import fill_missing_zips as _fill_missing_zips
    _HAS_ZIP_LOOKUP = True
except Exception as _zl_err:
    logger.warning(f"Zip lookup not available: {_zl_err}")
    _HAS_ZIP_LOOKUP = False

app = FastAPI(title="Onsite API", version="1.0.0")

# GZip middleware — compresses responses >1KB, critical for 28MB+ JSON payloads
from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Session middleware required by authlib OAuth state management
from starlette.middleware.sessions import SessionMiddleware
app.add_middleware(SessionMiddleware, secret_key=os.getenv("JWT_SECRET", "onsite-dev-secret"))

# Mount auth & OAuth routers from backend
try:
    from backend.routes.auth import router as auth_router
    from backend.routes.oauth import router as oauth_router
    app.include_router(auth_router)
    app.include_router(oauth_router)
    logger.info("Auth & OAuth routers mounted at /api/auth/*")
except Exception as _router_err:
    logger.warning(f"Could not mount auth routers: {_router_err}")

# Mount additional routers (billing, leads, enhancements, discovered_sources, websocket)
_optional_routers = [
    ("backend.routes.billing", "router", "/api/billing"),
    ("backend.routes.leads", "router", "/api"),
    ("backend.routes.enhancements", "router", "/api/enhancements"),
    ("backend.routes.discovered_sources", "router", "/api/discovered"),
    ("backend.routes.websocket", "router", "/ws"),
    ("backend.routes.campaigns", "router", "/api"),
]
for _mod_path, _attr, _desc in _optional_routers:
    try:
        _mod = __import__(_mod_path, fromlist=[_attr])
        _r = getattr(_mod, _attr)
        app.include_router(_r)
        logger.info(f"Router mounted: {_desc} ({_mod_path})")
    except Exception as _err:
        logger.warning(f"Could not mount {_mod_path}: {_err}")

# CORS - allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000", "http://127.0.0.1:8000",
        "http://localhost:18000", "http://127.0.0.1:18000",
        *([os.getenv("APP_ORIGIN")] if os.getenv("APP_ORIGIN") else []),
        *[o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()],
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# CONFIGURATION
# ============================================================================

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")  # Get from env
_BASE_DIR = Path(__file__).parent
CACHE_FILE = str(_BASE_DIR / "data_cache.json")
BACKEND_CACHE_FILE = str(_BASE_DIR / "backend" / "data_cache.json")
BACKEND_LEADS_DB = str(_BASE_DIR / "backend" / "leads.db")
CACHE_DURATION = timedelta(hours=6)  # Refresh every 6 hours
# US nationwide permit sources/ingest (free/open only)
# Use backend/data/ for runtime data — works in Docker and locally
US_DATA_DIR = _BASE_DIR / "backend" / "data"
US_DATA_DIR.mkdir(parents=True, exist_ok=True)
US_SOURCES_JSON = US_DATA_DIR / "permit_sources.json"
US_PERMITS_CSV = US_DATA_DIR / "permits_ingested.csv"
US_DISCOVERY_LOG = US_DATA_DIR / "permit_sources_discovery.log"
SOURCES_PATH = US_SOURCES_JSON


def load_csv_dicts(path):
    """Load a CSV file as a list of dicts. Returns [] if file missing."""
    if not Path(path).exists():
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f))


def save_csv_dicts(path, rows):
    """Save a list of dicts to a CSV file."""
    if not rows:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# Real LA County addresses with verified coordinates
VERIFIED_ADDRESSES = [
    {"addr": "6777 Hollywood Blvd", "city": "Los Angeles", "zip": "90028", "lat": 34.1017, "lng": -118.3389},
    {"addr": "100 W Broadway", "city": "Long Beach", "zip": "90802", "lat": 33.7693, "lng": -118.1938},
    {"addr": "300 E Colorado Blvd", "city": "Pasadena", "zip": "91101", "lat": 34.1458, "lng": -118.1430},
    {"addr": "9560 Wilshire Blvd", "city": "Beverly Hills", "zip": "90212", "lat": 34.0672, "lng": -118.4044},
    {"addr": "1920 Santa Monica Blvd", "city": "Santa Monica", "zip": "90404", "lat": 34.0293, "lng": -118.4847},
    {"addr": "500 N Brand Blvd", "city": "Glendale", "zip": "91203", "lat": 34.1514, "lng": -118.2548},
    {"addr": "201 E Magnolia Blvd", "city": "Burbank", "zip": "91502", "lat": 34.1676, "lng": -118.3072},
    {"addr": "9777 Culver Blvd", "city": "Culver City", "zip": "90232", "lat": 34.0110, "lng": -118.3997},
    {"addr": "21540 Hawthorne Blvd", "city": "Torrance", "zip": "90503", "lat": 33.8337, "lng": -118.3521},
    {"addr": "1815 Hawthorne Blvd", "city": "Redondo Beach", "zip": "90278", "lat": 33.8630, "lng": -118.3528},
    {"addr": "1200 Highland Ave", "city": "Manhattan Beach", "zip": "90266", "lat": 33.8893, "lng": -118.4092},
    {"addr": "90 Pier Ave", "city": "Hermosa Beach", "zip": "90254", "lat": 33.8625, "lng": -118.3984},
    {"addr": "340 Main St", "city": "El Segundo", "zip": "90245", "lat": 33.9182, "lng": -118.4160},
    {"addr": "111 N Market St", "city": "Inglewood", "zip": "90301", "lat": 33.9616, "lng": -118.3527},
    {"addr": "12501 Hawthorne Blvd", "city": "Hawthorne", "zip": "90250", "lat": 33.9160, "lng": -118.3526},
    {"addr": "1651 W Redondo Beach Blvd", "city": "Gardena", "zip": "90247", "lat": 33.8901, "lng": -118.3091},
]

# ============================================================================
# DATA MODELS
# ============================================================================

class Lead(BaseModel):
    id: int
    permit_number: str
    address: str
    city: str
    zip: str
    lat: float
    lng: float
    work_description: str
    permit_type: str
    valuation: float
    issue_date: str
    days_old: int
    score: int
    temperature: str
    urgency: str
    source: str
    apn: Optional[str] = None
    owner_name: Optional[str] = None
    owner_phone: Optional[str] = None
    owner_email: Optional[str] = None

class APIStatus(BaseModel):
    name: str
    status: str
    response_time_ms: Optional[int] = None
    last_check: str
    details: Optional[str] = None
    contacts_count: Optional[int] = None


def ensure_dirs():
    US_DATA_DIR.mkdir(parents=True, exist_ok=True)

ensure_dirs()

# ============================================================================
# Discovery seeds
# ============================================================================
SOCRATA_DOMAINS = [
    "data.cityofchicago.org",
    "data.austintexas.gov",
    "data.sfgov.org",
    "data.seattle.gov",
    "data.cityofnewyork.us",
    "data.cityofboston.gov",
    "data.kcmo.org",
    "data.fortworthtexas.gov",
    "opendata.dc.gov",
    "data.cincinnati-oh.gov",
    "data.lacity.org",
    "data.cityofmadison.com",
    "data.milwaukee.gov",
    "data.cityofsalem.net",
    "data.denvergov.org",
    "data.phoenix.gov",
    "data.dallasopendata.com",
    "data.okc.gov",
    "data.sanjoseca.gov",
    "data.tampagov.net",
    "data.minneapolismn.gov",
    "data.oregon.gov",
    "data.ny.gov",
    "data.louisvilleky.gov",
    "data.smcgov.org",
    "data.montgomerycountymd.gov",
    "data.raleighnc.gov",
    "data.cityofhenderson.com"
]

CKAN_PORTALS = [
    "https://data.austintexas.gov",
    "https://data.sfgov.org",
]

# ============================================================================
# VERIFIED SOCRATA PERMIT SOURCES (direct resource IDs — no discovery needed)
# Each entry: city, domain, resource_id, date_field, state, lat/lng for fallback
# ============================================================================
VERIFIED_PERMIT_SOURCES = [
    # All fields=None means fetch all fields (no $select restriction)
    {"city": "Los Angeles", "st": "CA", "domain": "data.lacity.org", "rid": "pi9x-tg5x", "date_field": "issue_date",
     "fields": None, "lat": 34.0522, "lng": -118.2437},
    {"city": "Los Angeles (Electrical)", "st": "CA", "domain": "data.lacity.org", "rid": "ysqd-apz7", "date_field": "issue_date",
     "fields": None, "lat": 34.0522, "lng": -118.2437},
    {"city": "San Francisco", "st": "CA", "domain": "data.sfgov.org", "rid": "i98e-djp9", "date_field": "filed_date",
     "fields": None, "lat": 37.7749, "lng": -122.4194},
    {"city": "San Diego County", "st": "CA", "domain": "data.sandiegocounty.gov", "rid": "dyzh-7eat", "date_field": "issued_date",
     "fields": None, "lat": 32.7157, "lng": -117.1611},
    {"city": "New York City", "st": "NY", "domain": "data.cityofnewyork.us", "rid": "ipu4-2q9a", "date_field": "issuance_date",
     "fields": None, "lat": 40.7128, "lng": -74.006},
    {"city": "Chicago", "st": "IL", "domain": "data.cityofchicago.org", "rid": "ydr8-5enu", "date_field": "issue_date",
     "fields": None, "lat": 41.8781, "lng": -87.6298},
    {"city": "Seattle", "st": "WA", "domain": "data.seattle.gov", "rid": "76t5-zqzr", "date_field": "issueddate",
     "fields": None, "lat": 47.6062, "lng": -122.3321},
    {"city": "Austin", "st": "TX", "domain": "data.austintexas.gov", "rid": "3syk-w9eu", "date_field": "issue_date",
     "fields": None, "lat": 30.2672, "lng": -97.7431},
    # Nashville migrated off Socrata — resource 3h5w-q8b7 returns HTML. Disabled until new endpoint found.
    # {"city": "Nashville", "st": "TN", "domain": "data.nashville.gov", "rid": "3h5w-q8b7", "date_field": "date_issued",
    #  "fields": None, "lat": 36.1627, "lng": -86.7816},
    {"city": "Marin County", "st": "CA", "domain": "data.marincounty.gov", "rid": "mkbn-caye", "date_field": "issued_date",
     "fields": None, "lat": 37.9735, "lng": -122.5311},
    # LA Plumbing/HVAC — permanently 403 as of 2026-03. Disabled.
    # {"city": "Los Angeles (Plumbing/HVAC)", "st": "CA", "domain": "data.lacity.org", "rid": "nbyu-2ha9", "date_field": "issue_date",
    #  "fields": None, "lat": 34.0522, "lng": -118.2437},
    {"city": "Los Angeles (Pipeline)", "st": "CA", "domain": "data.lacity.org", "rid": "gwh9-jnip", "date_field": "submitted_date",
     "fields": None, "lat": 34.0522, "lng": -118.2437},
    {"city": "Los Angeles (New Housing)", "st": "CA", "domain": "data.lacity.org", "rid": "cpkv-aajs", "date_field": "issue_date",
     "fields": None, "lat": 34.0522, "lng": -118.2437},
    # Additional SF datasets
    {"city": "San Francisco (Electrical)", "st": "CA", "domain": "data.sfgov.org", "rid": "ftty-kx6y", "date_field": "application_creation_date",
     "fields": None, "lat": 37.7749, "lng": -122.4194},
    {"city": "San Francisco (Plumbing)", "st": "CA", "domain": "data.sfgov.org", "rid": "a6aw-rudh", "date_field": "application_creation_date",
     "fields": None, "lat": 37.7749, "lng": -122.4194},
    # Additional cities from master data
    {"city": "Honolulu", "st": "HI", "domain": "data.honolulu.gov", "rid": "4vab-c87q", "date_field": "issuedate",
     "fields": None, "lat": 21.3069, "lng": -157.8583},
    {"city": "Kansas City", "st": "MO", "domain": "data.kcmo.org", "rid": "jnga-5v37", "date_field": None,
     "fields": None, "lat": 39.0997, "lng": -94.5786},
    {"city": "New Orleans", "st": "LA", "domain": "data.nola.gov", "rid": "72f9-bi28", "date_field": "issuedate",
     "fields": None, "lat": 29.9511, "lng": -90.0715},
    {"city": "Dallas", "st": "TX", "domain": "www.dallasopendata.com", "rid": "e7gq-4sah", "date_field": "issued_date",
     "fields": None, "lat": 32.7767, "lng": -96.797},
    {"city": "Delaware", "st": "DE", "domain": "data.delaware.gov", "rid": "2655-qn8j", "date_field": "estconstructdate",
     "fields": None, "lat": 39.1582, "lng": -75.5244},
    {"city": "Somerville", "st": "MA", "domain": "data.somervillema.gov", "rid": "vxgw-vmky", "date_field": "issue_date",
     "fields": None, "lat": 42.3876, "lng": -71.0995},
    {"city": "Connecticut", "st": "CT", "domain": "data.ct.gov", "rid": "5mzw-sjtu", "date_field": "daterecorded",
     "fields": None, "lat": 41.6032, "lng": -73.0877},
    {"city": "Cook County", "st": "IL", "domain": "datacatalog.cookcountyil.gov", "rid": "csik-bsws", "date_field": None,
     "fields": None, "lat": 41.8119, "lng": -87.7453},
    {"city": "Montgomery County", "st": "MD", "domain": "data.montgomerycountymd.gov", "rid": "m88u-pqki", "date_field": "addeddate",
     "fields": None, "lat": 39.1547, "lng": -77.2405},
    {"city": "Prince George's County", "st": "MD", "domain": "data.princegeorgescountymd.gov", "rid": "dqvr-xqvv", "date_field": "permit_issuance_date",
     "fields": None, "lat": 38.8296, "lng": -76.8453},
]

# ============================================================================
# CITY NAME CLEANER — fixes garbage city fields at serve time, keeps ALL leads
# ============================================================================
_STREET_SUFFIXES_SET = frozenset([
    # Abbreviations
    'St', 'Ave', 'Blvd', 'Dr', 'Rd', 'Way', 'Pkwy', 'Ln', 'Ct', 'Pl', 'Hwy',
    'Apt', 'Ste', 'Bldg', 'Cir', 'Trl', 'Ter', 'Av', 'Xing', 'Pk', 'Loop',
    'Sq', 'Run', 'Wy', 'Cv', 'Pt', 'Pass', 'Crk', 'Holw', 'Mdw', 'Spg',
    # Full words
    'Avenue', 'Street', 'Drive', 'Road', 'Lane', 'Court', 'Place', 'Boulevard',
    'Circle', 'Trail', 'Terrace', 'Highway', 'Parkway', 'Square', 'Crossing',
    'Park', 'Pike', 'Alley', 'Route', 'Row', 'Path', 'Walk', 'Landing',
    'Bridge', 'Glen', 'Creek', 'Ridge', 'Hill', 'Hollow', 'Meadow', 'Spring',
    'View', 'Cove', 'Point', 'Bend', 'Gate', 'Trace', 'Dale', 'Fork',
    'Etj',  # Austin "2 Mile Etj" jurisdictional boundary
])

# Build source→city lookup from all configured sources
_SOURCE_TO_CITY = {}
for _src in VERIFIED_PERMIT_SOURCES:
    _SOURCE_TO_CITY[_src["city"]] = _src["city"].split(" (")[0]
    _SOURCE_TO_CITY[_src["domain"]] = _src["city"].split(" (")[0]

# Extra domain→city mappings for backend cache sources
_SOURCE_TO_CITY.update({
    "data.cityoforlando.net": "Orlando",
    "data.norfolk.gov": "Norfolk",
    "data.cityofchicago.org": "Chicago",
    "cos-data.seattle.gov": "Seattle",
    "data.montgomerycountymd.gov": "Montgomery County",
    "data.brla.gov": "Baton Rouge",
    "datacatalog.cookcountyil.gov": "Cook County",
    "data.cambridgema.gov": "Cambridge",
    "data.sccgov.org": "Santa Clara County",
    "data.cityofnewyork.us": "New York City",
    "data.kcmo.org": "Kansas City",
    "data.honolulu.gov": "Honolulu",
    "data.delaware.gov": "Delaware",
    "data.nola.gov": "New Orleans",
    "www.dallasopendata.com": "Dallas",
    "data.seattle.gov": "Seattle",
    "data.somervillema.gov": "Somerville",
    "data.ct.gov": "Connecticut",
    "data.princegeorgescountymd.gov": "Prince George's County",
    "data.wa.gov": "Washington State",
    "socrata_data.wa.gov_sefr-g784": "Washington State",
    "data.nashville.gov": "Nashville",
    "data.littlerock.gov": "Little Rock",
    "datahub.austintexas.gov": "Austin",
    "data.marincounty.gov": "Marin County",
    "data.austintexas.gov": "Austin",
    "data.sfgov.org": "San Francisco",
    "data.lacity.org": "Los Angeles",
    "data.sandiegocounty.gov": "San Diego County",
    "data.cityofnewyork.us": "New York City",
})

def _is_address_like(name: str) -> bool:
    """Detect if a string looks like a street address rather than a city name."""
    if not name or len(name) <= 1:
        return True
    if name[0].isdigit():
        return True
    if '  ' in name:
        return True
    if name.startswith("socrata_") or name.startswith("arcgis_"):
        return True
    if "_" in name and "." in name:
        return True
    lower = name.lower()
    if lower in ("unknown", "n/a", "none", "null", "unincorporated",
                 "please enter zip code", "center", "adu", "combo permits"):
        return True
    # Source domain leaked as city name
    if "." in name and ("data." in lower or "gov" in lower or "org" in lower
                        or "datahub" in lower or "socrata" in lower):
        return True
    words = name.split()
    # Check if any word is a street suffix (catches "Adams Av", "Alpine Ter")
    if len(words) >= 2 and words[-1] in _STREET_SUFFIXES_SET:
        return True
    if "Apt" in name or "Suite" in name or "#" in name:
        return True
    if len(name) > 2 and name[-2:] in (" N", " S", " E", " W"):
        return True
    if len(name) > 35:
        return True
    # Compound Cook County style: "StreetName SuburbName" (3+ words, not a known pattern)
    _city_prefixes = ("new ", "north ", "south ", "east ", "west ", "san ", "los ", "las ",
        "el ", "la ", "st ", "fort ", "cape ", "palm ", "mount ", "salt ",
        "coral ", "jersey ", "virginia ", "long ", "grand ", "cherry ",
        "cardiff ", "hot ", "red ", "point ", "bay ", "ocean ", "lake ",
        "key ", "royal ", "isle ", "spring ", "winter ")
    if len(words) >= 3 and not any(lower.startswith(p) for p in _city_prefixes):
        return True
    return False

# Known Cook County IL suburbs for extraction from compound names
_COOK_SUBURBS = frozenset([
    "Franklin Park", "Schiller Park", "Rolling Meadows", "Des Plaines",
    "Elk Grove Village", "Morton Grove", "Mount Prospect", "Hoffman Estates",
    "Hanover Park", "Wheeling", "Skokie", "Northbrook", "Schaumburg",
    "Arlington Heights", "Park Ridge", "Niles", "Glenview", "Palatine",
    "Buffalo Grove", "Streamwood", "Bartlett", "Carol Stream", "Roselle",
    "Bloomingdale", "Addison", "Bensenville", "Wood Dale", "Itasca",
    "Medinah", "Elgin", "Barrington", "Prospect Heights", "Lincolnwood",
    "Norridge", "Harwood Heights", "River Grove", "Melrose Park",
    "Maywood", "Bellwood", "Broadview", "Westchester", "La Grange",
    "Berwyn", "Cicero", "Oak Park", "Evanston", "Wilmette", "Winnetka",
    "Glencoe", "Highland Park", "Lake Forest", "Deerfield", "Libertyville",
    "Alsip", "Blue Island", "Calumet City", "Chicago Heights", "Country Club Hills",
    "Crestwood", "Dolton", "Evergreen Park", "Harvey", "Homewood",
    "Lansing", "Matteson", "Oak Forest", "Oak Lawn", "Orland Park",
    "Palos Heights", "Palos Hills", "Park Forest", "Richton Park",
    "South Holland", "Tinley Park", "Worth", "Burbank", "Bedford Park",
    "Bridgeview", "Chicago Ridge", "Countryside", "Hickory Hills",
    "Justice", "Lyons", "McCook", "Riverside", "Stickney", "Summit",
    "Willow Springs", "Lemont", "Lockport", "Homer Glen", "Mokena",
    "New Lenox", "Frankfort", "Orland Hills", "Flossmoor", "Olympia Fields",
    "Hazel Crest", "Markham", "Midlothian", "Posen", "Robbins",
])

_CINCINNATI_SOURCES = frozenset([
    "Cincinnati", "Cincinnati Combo Permits", "Cincinnati Licenses/Use Permits",
])

from functools import lru_cache

@lru_cache(maxsize=50000)
def _cached_clean_city(city_raw: str, source: str) -> str:
    """Cached wrapper for city cleaning — avoids re-computing on every request."""
    return _clean_city_name(city_raw, source)

def _clean_city_name(city_raw: str, source: str) -> str:
    """
    Fix a city name using all available signals. Never drops a lead.
    Source-specific rules run FIRST, then generic cleaning.
    """
    city = (city_raw or "").strip()
    source_clean = (source or "").split(" (")[0].strip()

    # ── Source-specific rules (run BEFORE generic "already clean" check) ──

    # Cook County: city field = "StreetName SuburbName" compound
    if "Cook County" in source_clean or "cookcounty" in source_clean.lower():
        city_title = city.title() if city else city
        # Try extracting known suburb from end of compound
        for suburb in sorted(_COOK_SUBURBS, key=len, reverse=True):
            if city_title.endswith(suburb):
                return suburb
        return "Cook County"

    # Cincinnati: city field = street name (e.g. "Academy Av")
    if source_clean in _CINCINNATI_SOURCES or "cincinnati" in source_clean.lower():
        return "Cincinnati"

    # Charlotte: city field = "StreetAddr   Charlotte" pattern
    if "charlotte" in source_clean.lower():
        if '  ' in city:
            parts = [p.strip() for p in city.split('  ') if p.strip()]
            for part in reversed(parts):
                if not _is_address_like(part) and len(part) > 2:
                    return part.title() if part == part.upper() else part
        if _is_address_like(city):
            return "Charlotte"

    # ── Generic cleaning ──

    # Title-case lowercase cities (baton rouge → Baton Rouge)
    if city and city == city.lower() and len(city) > 2:
        city = city.title()

    # Already clean — return as-is
    if city and not _is_address_like(city):
        if city == city.upper() and len(city) > 2:
            city = city.title()
        return re.sub(r'\s+', ' ', city).strip()

    # Double-space pattern: "Something   RealCity"
    if '  ' in city:
        parts = [p.strip() for p in city.split('  ') if p.strip()]
        for part in reversed(parts):
            clean_part = part.title() if part == part.upper() else part
            if not _is_address_like(clean_part) and len(clean_part) > 2:
                return re.sub(r'\s+', ' ', clean_part).strip()

    # Look up source → known city
    if source_clean in _SOURCE_TO_CITY:
        return _SOURCE_TO_CITY[source_clean]

    # Fuzzy match source against known domains/cities
    for key, city_name in _SOURCE_TO_CITY.items():
        if key in source_clean or source_clean in key:
            return city_name

    # Use source name if it looks like a city
    if source_clean and len(source_clean) < 30 and not _is_address_like(source_clean):
        return source_clean

    return city_raw.strip() if city_raw else "Unknown"


# ArcGIS FeatureServer sources (free, no auth)
# All 9 original ArcGIS endpoints retired (Invalid URL / Service not found as of 2026-03).
# ArcGIS sources are now served by the 662-entry registry in api_sources table.
ARCGIS_PERMIT_SOURCES = []


# ============================================================================
# US permit sources: load registry and ingest (free APIs: Socrata/CKAN)
# ============================================================================

def discover_socrata(domains):
    with Session() as session:
        session.headers.update({"User-Agent": "Onsite/1.0"})
        found = []
        for dom in domains:
            try:
                url = f"https://api.us.socrata.com/api/catalog/v1?{urlencode({'q':'permit','domains':dom,'limit':5})}"
                r = session.get(url, timeout=15)
                r.raise_for_status()
                for res in r.json().get("results", []):
                    meta = res.get("resource", {})
                    if meta.get("type") != "dataset":
                        continue
                    found.append({
                        "name": meta.get("name", f"{dom} permits"),
                        "type": "socrata",
                        "url": f"https://{dom}",
                        "dataset": meta.get("id")
                    })
            except Exception as e:
                logger.warning(f"Socrata discover failed for {dom}: {e}")
        return found

def discover_ckan(portals):
    with Session() as session:
        session.headers.update({"User-Agent": "Onsite/1.0"})
        found = []
        for base in portals:
            try:
                url = f"{base}/api/3/action/package_search?q=permit&rows=5"
                r = session.get(url, timeout=15)
                r.raise_for_status()
                data = r.json()
                if not data.get("success"):
                    continue
                for pkg in data.get("result", {}).get("results", []):
                    for res in pkg.get("resources", []):
                        if res.get("format", "").lower() == "json" or "api" in res.get("url", "").lower():
                            found.append({
                                "name": pkg.get("title", "permit dataset"),
                                "type": "ckan",
                                "url": base,
                                "resource_id": res.get("id"),
                                "resource_url": res.get("url")
                            })
                            break
            except Exception as e:
                logger.warning(f"CKAN discover failed for {base}: {e}")
        return found

def discover_all_sources():
    soc = discover_socrata(SOCRATA_DOMAINS)
    ckan = discover_ckan(CKAN_PORTALS)
    sources = soc + ckan
    with SOURCES_PATH.open("w") as f:
        json.dump(sources, f, indent=2)
    with US_DISCOVERY_LOG.open("a") as f:
        f.write(f"{datetime.utcnow().isoformat()} discovered {len(sources)} sources\\n")
    return sources

def load_us_sources():
    if not US_SOURCES_JSON.exists():
        return []
    with US_SOURCES_JSON.open() as f:
        return json.load(f)

def normalize_permit(rec: dict, source: dict):
    """Map raw record to a common shape."""
    return {
        "source_name": source.get("name",""),
        "source_type": source.get("type",""),
        "source_url": source.get("url",""),
        "permit_number": rec.get("permit_number") or rec.get("permit_num") or rec.get("permit_nbr") or rec.get("application_number") or "",
        "issue_date": rec.get("issue_date") or rec.get("issued_date") or rec.get("permit_issued_date") or rec.get("file_date") or "",
        "status": rec.get("status") or rec.get("permit_status") or "",
        "work_description": rec.get("work_description") or rec.get("description") or rec.get("worktype") or "",
        "valuation": rec.get("estimated_cost") or rec.get("valuation") or rec.get("declared_valuation") or "",
        "address": rec.get("address") or rec.get("street_address") or rec.get("site_address") or "",
        "city": rec.get("city") or rec.get("jurisdiction") or "",
        "zip": rec.get("zip") or rec.get("zipcode") or rec.get("zip_code") or "",
        "contractor": rec.get("contractor_name") or rec.get("contractor") or "",
        "owner": rec.get("owner_name") or rec.get("owner") or "",
        "parcel": rec.get("parcel_number") or rec.get("apn") or rec.get("parcelid") or "",
        "lat": rec.get("location", {}).get("latitude") if isinstance(rec.get("location"), dict) else rec.get("latitude") or "",
        "lon": rec.get("location", {}).get("longitude") if isinstance(rec.get("location"), dict) else rec.get("longitude") or ""
    }

def fetch_socrata(source, days=30, limit=500):
    base = source["url"]  # e.g. https://data.cityofchicago.org
    dataset = source["dataset"]
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    params = {
        "$limit": limit,
        "$order": "issue_date DESC",
        "$where": f"issue_date >= '{since}'"
    }
    url = f"{base}/resource/{dataset}.json?{urlencode(params)}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_ckan(source, days=30, limit=500):
    base = source["url"]  # e.g. https://data.austintexas.gov
    rid = source["resource_id"]
    url = f"{base}/api/3/action/datastore_search"
    params = {"resource_id": rid, "limit": limit}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        return []
    return data.get("result", {}).get("records", [])

def ingest_us_permits(days=30, limit=500):
    sources = load_us_sources()
    out = []
    for src in sources:
        try:
            if src["type"] == "socrata":
                recs = fetch_socrata(src, days=days, limit=limit)
            elif src["type"] == "ckan":
                recs = fetch_ckan(src, days=days, limit=limit)
            else:
                continue
            normalized = [normalize_permit(rec, src) for rec in recs]
            out.extend(normalized)
            logger.info(f"Ingested {len(recs)} from {src['name']}")
        except Exception as e:
            logger.warning(f"Source {src.get('name')} failed: {e}")
    if out:
        save_csv_dicts(US_PERMITS_CSV, out)
    return out


# ============================================================================
# API CLIENTS
# ============================================================================

class LADBSClient:
    """Real LADBS Socrata API client"""
    BASE_URL = "https://data.lacity.org/resource/yv23-pmwf.json"
    
    async def get_recent_permits(self, days=7, limit=100):
        """Fetch recent permits from LADBS"""
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        params = {
            '$where': f"issue_date >= '{cutoff}'",
            '$limit': limit,
            '$order': 'issue_date DESC',
            '$select': 'permit_nbr,issue_date,address,city,zip_code,work_description,permit_type,valuation,contractor_business_name,contractor_phone'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"LADBS API: Retrieved {len(data)} permits")
                        return data
                    else:
                        logger.error(f"LADBS API error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"LADBS API exception: {e}")
            return []

class LongBeachClient:
    """Real Long Beach OpenGov API client"""
    BASE_URL = "https://data.longbeach.gov/api/records/1.0/search/"
    
    async def get_recent_permits(self, days=7, limit=50):
        """Fetch recent permits from Long Beach"""
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        params = {
            'dataset': 'building-permits',
            'q': f'issue_date>={cutoff}',
            'rows': limit,
            'sort': '-issue_date'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        records = data.get('records', [])
                        logger.info(f"Long Beach API: Retrieved {len(records)} permits")
                        return records
                    else:
                        logger.error(f"Long Beach API error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Long Beach API exception: {e}")
            return []

class GeocodingClient:
    """Free geocoding using Nominatim"""
    BASE_URL = "https://nominatim.openstreetmap.org/search"
    
    async def geocode(self, address: str, city: str, state="CA"):
        """Geocode address to lat/lng"""
        query = f"{address}, {city}, {state}"
        
        params = {
            'q': query,
            'format': 'json',
            'limit': 1
        }
        
        headers = {
            'User-Agent': 'Onsite/1.0'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                await asyncio.sleep(1.1)  # Nominatim rate limit: 1 req/sec
                async with session.get(self.BASE_URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data:
                            return {
                                'lat': float(data[0]['lat']),
                                'lng': float(data[0]['lon'])
                            }
        except Exception as e:
            logger.error(f"Geocoding error for {address}: {e}")
        
        return None

# ============================================================================
# DATA PROCESSING
# ============================================================================

def calculate_score(days_old: int, valuation: float, permit_type: str) -> tuple:
    """Calculate lead score and classification"""
    score = 0
    
    # Urgency points (max 38)
    if days_old == 0:
        score += 38
    elif days_old <= 1:
        score += 33
    elif days_old <= 2:
        score += 27
    elif days_old <= 5:
        score += 20
    elif days_old <= 7:
        score += 14
    elif days_old <= 14:
        score += 8
    else:
        score += 3
    
    # Value points (max 32)
    if valuation >= 500000:
        score += 32
    elif valuation >= 200000:
        score += 25
    elif valuation >= 100000:
        score += 17
    elif valuation >= 50000:
        score += 11
    else:
        score += 4
    
    # Type points (max 22)
    if 'new' in permit_type.lower() or 'addition' in permit_type.lower():
        score += 22
    elif 'remodel' in permit_type.lower() or 'alter' in permit_type.lower():
        score += 16
    else:
        score += 9
    
    score = min(99, score)
    
    # Temperature
    if score >= 70:
        temp = 'hot'
    elif score >= 43:
        temp = 'warm'
    else:
        temp = 'cold'
    
    # Urgency
    if days_old <= 1:
        urgency = 'CRITICAL'
    elif days_old <= 3:
        urgency = 'HIGH'
    elif days_old <= 7:
        urgency = 'MEDIUM'
    else:
        urgency = 'LOW'
    
    return score, temp, urgency

async def process_ladbs_permit(permit_data: dict, geocoder: GeocodingClient) -> Optional[Lead]:
    """Process LADBS permit into Lead"""
    try:
        address = permit_data.get('address', '').strip()
        city = permit_data.get('city', 'Los Angeles').strip()
        zip_code = permit_data.get('zip_code', '')
        
        if not address:
            return None
        
        # Try geocoding
        coords = await geocoder.geocode(address, city)
        if not coords:
            logger.warning(f"Could not geocode: {address}")
            return None
        
        # Parse dates
        issue_date_str = permit_data.get('issue_date', '')[:10]
        if issue_date_str:
            issue_date = datetime.fromisoformat(issue_date_str)
            days_old = (datetime.now() - issue_date).days
        else:
            days_old = 0
        
        # Parse valuation
        val_str = permit_data.get('valuation', '0')
        try:
            valuation = float(str(val_str).replace('$', '').replace(',', ''))
        except (ValueError, TypeError):
            valuation = 0
        
        # Calculate score
        score, temp, urgency = calculate_score(days_old, valuation, permit_data.get('permit_type', ''))
        
        return Lead(
            id=hash(permit_data.get('permit_nbr', '')),
            permit_number=permit_data.get('permit_nbr', ''),
            address=address,
            city=city,
            zip=zip_code,
            lat=coords['lat'],
            lng=coords['lng'],
            work_description=permit_data.get('work_description', ''),
            permit_type=permit_data.get('permit_type', ''),
            valuation=valuation,
            issue_date=issue_date_str,
            days_old=days_old,
            score=score,
            temperature=temp,
            urgency=urgency,
            source='LADBS',
            owner_name=permit_data.get('contractor_business_name'),
            owner_phone=permit_data.get('contractor_phone')
        )
    except Exception as e:
        logger.error(f"Error processing LADBS permit: {e}")
        return None

# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

class DataCache:
    """In-memory + disk cache for leads"""
    _memory_cache = None  # In-memory store for merged 3.4M+ leads
    _lock = threading.RLock()

    @classmethod
    def load(cls):
        """Load from memory first, then disk"""
        with cls._lock:
            if cls._memory_cache is not None:
                return cls._memory_cache
            if os.path.exists(CACHE_FILE):
                try:
                    with open(CACHE_FILE, 'r') as f:
                        data = json.load(f)
                        cache_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
                        if datetime.now() - cache_time < CACHE_DURATION:
                            logger.info(f"Using cached data from {cache_time}")
                            leads = data.get('leads', [])
                            cls._memory_cache = leads
                            return leads
                except Exception as e:
                    logger.error(f"Error loading cache: {e}")
            return None

    @classmethod
    def set_memory(cls, leads):
        """Store merged leads in memory (no disk write for large datasets)"""
        with cls._lock:
            cls._memory_cache = leads
        logger.info(f"Memory cache set: {len(leads):,} leads")
    
    @classmethod
    def save(cls, leads: List[dict]):
        """Save cache to disk (thread-safe)"""
        with cls._lock:
            try:
                data = {
                    'timestamp': datetime.now().isoformat(),
                    'leads': leads
                }
                with open(CACHE_FILE, 'w') as f:
                    json.dump(data, f, indent=2)
                logger.info(f"Saved {len(leads)} leads to cache")
            except Exception as e:
                logger.error(f"Error saving cache: {e}")

# ============================================================================
# BACKEND DATA LOADER — loads 3.4M+ leads from existing caches
# ============================================================================

def load_backend_cache():
    """Load the massive 3.4M lead cache from the backend directory."""
    leads = []
    if not os.path.exists(BACKEND_CACHE_FILE):
        logger.warning(f"Backend cache not found: {BACKEND_CACHE_FILE}")
        return leads

    try:
        logger.info(f"Loading backend cache from {BACKEND_CACHE_FILE}...")
        with open(BACKEND_CACHE_FILE, 'r') as f:
            data = json.load(f)
        raw_leads = data.get('leads', [])
        logger.info(f"Backend cache loaded: {len(raw_leads):,} raw leads")

        for rec in raw_leads:
            addr = rec.get("address", "").strip()
            if not addr:
                continue

            city_raw = rec.get("city", "").strip()
            _sfx = ("St", "Ave", "Blvd", "Dr", "Rd", "Way", "Pkwy", "Ln", "Ct", "Pl", "Hwy", "Apt", "Ste", "Bldg")
            _domain_city = {
                "data.cityoforlando.net": "Orlando",
                "data.norfolk.gov": "Norfolk",
                "data.cityofchicago.org": "Chicago",
                "cos-data.seattle.gov": "Seattle",
                "data.montgomerycountymd.gov": "Montgomery County",
                "data.brla.gov": "Baton Rouge",
                "datacatalog.cookcountyil.gov": "Cook County",
                "data.cambridgema.gov": "Cambridge",
                "data.sccgov.org": "Santa Clara County",
                "data.cityofnewyork.us": "New York City",
            }
            _is_street = (
                not city_raw
                or any(city_raw.endswith(s) or city_raw.endswith(s + ".") for s in _sfx)
                or (city_raw and city_raw[0].isdigit())
                or city_raw.startswith("socrata_")
                or "Apt" in city_raw or "Suite" in city_raw
                or (len(city_raw) > 2 and city_raw[-2:] in (" N", " S", " E", " W"))
            )
            if _is_street:
                src = rec.get("source", "").split(" (")[0]
                city = _domain_city.get(src, "")
                if not city:
                    # Try matching socrata_ prefix to known cities
                    if "kcmo.org" in src: city = "Kansas City"
                    elif "honolulu" in src: city = "Honolulu"
                    elif "delaware" in src: city = "Delaware"
                    elif "nola.gov" in src: city = "New Orleans"
                    elif "dallas" in src: city = "Dallas"
                    elif "seattle" in src: city = "Seattle"
                    elif "DC_Permit" in src: city = "Washington"
                    elif "Philadelphia" in src: city = "Philadelphia"
                    else: city = src if src and len(src) < 30 else "Unknown"
            elif city_raw == city_raw.upper() and len(city_raw) > 2:
                city = city_raw.title()
            else:
                city = city_raw.split(" (")[0]
            state = rec.get("state", "")
            zipcode = str(rec.get("zip", ""))[:5]
            lat = rec.get("lat")
            lng = rec.get("lng")

            val_raw = rec.get("valuation", 0)
            try:
                valuation = float(str(val_raw).replace("$", "").replace(",", ""))
            except (ValueError, TypeError):
                valuation = 0

            perm = str(rec.get("permit_number", ""))
            ptype = rec.get("permit_type", "")
            issue_date = str(rec.get("issue_date", ""))[:10]
            owner = rec.get("owner_name", "")
            source_name = rec.get("source", "backend_cache")

            days_old = 0
            if issue_date and len(issue_date) >= 10:
                try:
                    dt = datetime.fromisoformat(issue_date)
                    days_old = max(0, (datetime.now() - dt).days)
                except (ValueError, TypeError):
                    pass

            score, temp, urgency = calculate_score(days_old, valuation, ptype)
            pri = "hot" if score >= 70 else "warm" if score >= 50 else "med" if score >= 30 else "cold"

            leads.append({
                "id": perm or str(rec.get("id", hash(addr + city))),
                "addr": addr,
                "city": city,
                "st": state,
                "zip": zipcode,
                "lat": lat if lat else 0,
                "lng": lng if lng else 0,
                "type": ptype or "Building Permit",
                "desc": rec.get("work_description", "") or ptype,
                "score": score,
                "pri": pri,
                "val": valuation,
                "sqft": 0,
                "perm": perm,
                "owner": owner or "Pending lookup",
                "phone": "Pending enrichment",
                "email": "Pending enrichment",
                "daysAgo": days_old,
                "filed": issue_date,
                "sold": False,
                "gc": None,
                "stage": "new",
                "history": [],
                "apn": "",
                "zone": "",
                "occ": "",
                "source": source_name,
            })

        logger.info(f"Backend cache normalized: {len(leads):,} leads with addresses")
    except Exception as e:
        logger.error(f"Error loading backend cache: {e}")

    return leads


def load_leads_db():
    """Load enriched leads from the SQLite database."""
    leads = []
    if not os.path.exists(BACKEND_LEADS_DB):
        return leads

    try:
        import sqlite3
        with sqlite3.connect(BACKEND_LEADS_DB) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM leads WHERE is_active = 1 OR is_active IS NULL")
            rows = cur.fetchall()

        for row in rows:
            r = dict(row)
            addr = r.get("address", "").strip()
            if not addr:
                continue

            valuation = float(r.get("valuation", 0) or 0)
            days_old = int(r.get("days_old", 0) or 0)
            score = int(r.get("score", 0) or 0)
            temp = r.get("temperature", "")
            pri = "hot" if score >= 70 else "warm" if score >= 50 else "med" if score >= 30 else "cold"

            leads.append({
                "id": str(r.get("permit_number", "")) or str(r.get("id", "")),
                "addr": addr,
                "city": r.get("city", ""),
                "st": r.get("state", ""),
                "zip": str(r.get("zip", ""))[:5],
                "lat": float(r.get("lat", 0) or 0),
                "lng": float(r.get("lng", 0) or 0),
                "type": r.get("permit_type", "") or "Building Permit",
                "desc": r.get("work_description", "") or r.get("description_full", ""),
                "score": score,
                "pri": pri,
                "val": valuation,
                "sqft": int(r.get("square_feet", 0) or 0),
                "perm": str(r.get("permit_number", "")),
                "owner": r.get("owner_name", "") or "Pending lookup",
                "phone": r.get("owner_phone", "") or r.get("contractor_phone", "") or "Pending enrichment",
                "email": r.get("owner_email", "") or "Pending enrichment",
                "daysAgo": days_old,
                "filed": r.get("issue_date", ""),
                "sold": False,
                "gc": r.get("contractor_name"),
                "stage": r.get("stage", "new"),
                "history": [],
                "apn": r.get("apn", ""),
                "zone": r.get("zoning", ""),
                "occ": "",
                "source": r.get("source", "leads_db"),
            })

        logger.info(f"Leads DB loaded: {len(leads):,} enriched leads")
    except Exception as e:
        logger.error(f"Error loading leads DB: {e}")

    return leads


# ============================================================================
# BACKGROUND SYNC
# ============================================================================

async def fetch_socrata_async(source, days=90, limit=500):
    """Fetch permits from a verified Socrata source (async).

    Handles date_field=None (no date filtering) and auto-retries without
    date filter on HTTP 400 (bad column name).
    """
    domain = source["domain"]
    rid = source["rid"]
    date_field = source.get("date_field")
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    fields = source.get("fields")
    url = f"https://{domain}/resource/{rid}.json"

    # Build params — with date filter if date_field is set
    params = {"$limit": str(limit)}
    if date_field:
        params["$order"] = f"{date_field} DESC"
        params["$where"] = f"{date_field} >= '{since}'"
    if fields:
        params["$select"] = fields

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.info(f"Socrata {source['city']}: {len(data)} permits")
                    return data

                # Fallback: if 400 and we used a date filter, retry without it
                if resp.status == 400 and date_field:
                    logger.warning(f"Socrata {source['city']} HTTP 400 with date_field={date_field}, retrying without date filter")
                    fallback_params = {"$limit": str(limit)}
                    if fields:
                        fallback_params["$select"] = fields
                    async with session.get(url, params=fallback_params, timeout=aiohttp.ClientTimeout(total=30)) as resp2:
                        if resp2.status == 200:
                            data = await resp2.json()
                            logger.info(f"Socrata {source['city']} (no date filter): {len(data)} permits")
                            return data
                        logger.warning(f"Socrata {source['city']} fallback also failed: HTTP {resp2.status}")
                        return []

                logger.warning(f"Socrata {source['city']} HTTP {resp.status}")
                return []
    except Exception as e:
        logger.warning(f"Socrata {source['city']} error: {e}")
        return []


async def fetch_arcgis_async(source, limit=200):
    """Fetch permits from an ArcGIS FeatureServer source (async)."""
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "json",
        "resultRecordCount": str(limit),
        "orderByFields": "OBJECTID DESC"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(source["url"], params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    features = data.get("features", [])
                    logger.info(f"ArcGIS {source['city']}: {len(features)} permits")
                    return features
                else:
                    logger.warning(f"ArcGIS {source['city']} HTTP {resp.status}")
                    return []
    except Exception as e:
        logger.warning(f"ArcGIS {source['city']} error: {e}")
        return []


def normalize_socrata_lead(rec, source):
    """Normalize a Socrata record into the frontend lead format."""
    # Extract address from various field naming conventions
    addr = (rec.get("address") or rec.get("primary_address") or rec.get("original_address1") or rec.get("originaladdress1") or rec.get("originaladdress") or "").strip()
    if not addr:
        # SF style: street_number + street_name + street_suffix
        sn = rec.get("street_number", "")
        sname = rec.get("street_name", "")
        ssuf = rec.get("street_suffix", "")
        if sn and sname:
            addr = f"{sn} {sname} {ssuf}".strip()
    if not addr:
        # NYC style: house__ + street_name
        house = rec.get("house__", "")
        sname = rec.get("street_name", "")
        if house and sname:
            addr = f"{house} {sname}".strip()
    if not addr:
        snum = rec.get("street_number", "")
        sdir = rec.get("street_direction", "")
        sname = rec.get("street_name", "")
        ssuf = rec.get("suffix", "")
        addr = f"{snum} {sdir} {sname} {ssuf}".strip()

    city_raw = rec.get("city") or rec.get("original_city") or rec.get("originalcity") or rec.get("jurisdiction") or ""
    city_raw = city_raw.strip()
    # Detect dirty city names — fall back to source city
    _street_suffixes = ("St", "Ave", "Blvd", "Dr", "Rd", "Way", "Pkwy", "Ln", "Ct", "Pl", "Hwy", "Apt", "Ste", "Bldg")
    _is_dirty = (
        not city_raw
        or len(city_raw) <= 1
        or city_raw.startswith("socrata_")
        or city_raw.startswith("arcgis_")
        or "_" in city_raw and "." in city_raw
        or any(city_raw.endswith(s) or city_raw.endswith(s + ".") for s in _street_suffixes)
        or (len(city_raw) > 0 and city_raw[0].isdigit())
        or city_raw.lower() in ("unknown", "n/a", "none", "null", "unincorporated")
    )
    if _is_dirty:
        city = source["city"].split(" (")[0]
    else:
        city = city_raw.title() if city_raw.isupper() and len(city_raw) > 2 else city_raw
        city = city.split(" (")[0]
        city = re.sub(r'\s+', ' ', city).strip()
    zipcode = rec.get("zip_code") or rec.get("zipcode") or rec.get("original_zip") or rec.get("originalzip") or rec.get("zip") or ""
    if isinstance(zipcode, str):
        zipcode = zipcode[:5]

    # Valuation
    val_raw = rec.get("valuation") or rec.get("estimated_cost") or rec.get("reported_cost") or rec.get("total_job_valuation") or rec.get("value") or rec.get("estimated_job_cost_") or rec.get("const_cost") or "0"
    try:
        valuation = float(str(val_raw).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        valuation = 0

    # Permit number
    perm = rec.get("permit_nbr") or rec.get("permit_number") or rec.get("permit_num") or rec.get("permit_") or rec.get("application_permit_number") or rec.get("job__") or rec.get("id") or ""

    # Date
    date_field = source["date_field"]
    raw_date = rec.get(date_field, "") or ""
    issue_date = str(raw_date)[:10]
    days_old = 0
    if issue_date and len(issue_date) >= 10:
        try:
            dt = datetime.fromisoformat(issue_date)
            days_old = max(0, (datetime.now() - dt).days)
        except (ValueError, TypeError):
            pass

    # Lat/lng
    lat = None
    lng = None
    for lat_key in ["latitude", "lat"]:
        if rec.get(lat_key):
            try:
                lat = float(rec[lat_key])
            except (ValueError, TypeError):
                pass
            break
    for lng_key in ["longitude", "lng", "lon"]:
        if rec.get(lng_key):
            try:
                lng = float(rec[lng_key])
            except (ValueError, TypeError):
                pass
            break
    # Location object (Socrata style)
    if not lat and isinstance(rec.get("location"), dict):
        try:
            lat = float(rec["location"].get("latitude", 0))
            lng = float(rec["location"].get("longitude", 0))
        except (ValueError, TypeError):
            pass
    # mapped_location
    if not lat and isinstance(rec.get("mapped_location"), dict):
        try:
            lat = float(rec["mapped_location"].get("latitude", 0))
            lng = float(rec["mapped_location"].get("longitude", 0))
        except (ValueError, TypeError):
            pass
    # Fallback to city center
    if not lat or not lng or lat == 0 or lng == 0:
        import hashlib
        _h = int(hashlib.md5((addr + str(perm)).encode()).hexdigest()[:8], 16)
        lat = source["lat"] + (_h % 1000 - 500) * 0.0001
        lng = source["lng"] + ((_h >> 10) % 1000 - 500) * 0.0001

    # Work description / type
    work_desc = rec.get("work_description") or rec.get("work_desc") or rec.get("description") or rec.get("permit_type_definition") or rec.get("work_type") or rec.get("permit_type_desc") or rec.get("purpose") or rec.get("action_type") or ""
    permit_type = rec.get("permit_type") or rec.get("permit_type_desc") or rec.get("work_class") or rec.get("category") or rec.get("type") or ""

    # Owner info (NYC has direct owner fields)
    owner_first = rec.get("owner_s_first_name") or rec.get("owner_first_name") or ""
    owner_last = rec.get("owner_s_last_name") or rec.get("owner_last_name") or ""
    owner_biz = rec.get("owner_s_business_name") or rec.get("owner_business_name") or rec.get("contact_1_name") or ""
    owner_name = f"{owner_first} {owner_last}".strip() or owner_biz or ""
    owner_phone = rec.get("owner_s_phone__") or rec.get("owner_phone") or rec.get("contractor_phone") or ""

    # Contractor
    gc = rec.get("contractor_business_name") or rec.get("contractorcompanyname") or rec.get("contractor_company") or rec.get("contractor") or rec.get("permittee_s_first_name", "")
    if not gc and rec.get("contact_1_type") == "CONTRACTOR":
        gc = rec.get("contact_1_name", "")

    # APN / parcel
    apn = rec.get("pin1") or rec.get("parcel_number") or rec.get("apn") or rec.get("parcelid") or rec.get("block_lot") or ""

    # Score
    score, temp, urgency = calculate_score(days_old, valuation, permit_type or work_desc)
    pri = "hot" if score >= 70 else "warm" if score >= 50 else "med" if score >= 30 else "cold"

    return {
        "id": perm or f"{source['city']}-{abs(hash(addr + issue_date)) % 999999}",
        "addr": addr,
        "city": city,
        "st": source["st"],
        "zip": str(zipcode),
        "lat": lat,
        "lng": lng,
        "type": permit_type or (work_desc[:40] if work_desc else "Building Permit"),
        "desc": work_desc,
        "score": score,
        "pri": pri,
        "val": valuation,
        "sqft": 0,
        "perm": str(perm),
        "owner": owner_name or "Pending lookup",
        "phone": str(owner_phone) if owner_phone else "Pending enrichment",
        "email": "Pending enrichment",
        "daysAgo": days_old,
        "filed": issue_date,
        "sold": False,
        "gc": gc or None,
        "stage": "new",
        "history": [],
        "apn": str(apn),
        "zone": "",
        "occ": "",
        "stories": max(1, int(float(rec.get("stories") or rec.get("floors") or 0))) if str(rec.get("stories") or rec.get("floors") or "0").replace(".","",1).isdigit() else 1,
        "units": max(1, int(float(rec.get("units") or rec.get("housing_units") or 0))) if str(rec.get("units") or rec.get("housing_units") or "0").replace(".","",1).isdigit() else 1,
        "lotSz": 0,
        "inspDone": 0,
        "inspTotal": 8,
        "source": source["city"],
        "temperature": temp,
        "urgency": urgency,
    }


def normalize_arcgis_lead(feature, source):
    """Normalize an ArcGIS feature into the frontend lead format."""
    attrs = feature.get("attributes", {})
    geom = feature.get("geometry", {})

    addr = attrs.get("Address") or attrs.get("ADDRESS") or attrs.get("FULL_ADDRESS") or attrs.get("FullAddress") or ""
    perm = attrs.get("PermitNumber") or attrs.get("PERMIT_NUMBER") or attrs.get("PERMITNUMBER") or attrs.get("OBJECTID") or ""
    val_raw = attrs.get("Valuation") or attrs.get("VALUATION") or attrs.get("EstProjectCost") or attrs.get("JobValue") or 0
    try:
        valuation = float(str(val_raw).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        valuation = 0

    permit_type = attrs.get("PermitType") or attrs.get("PERMIT_TYPE") or attrs.get("Type") or "Building Permit"
    work_desc = attrs.get("Description") or attrs.get("DESCRIPTION") or attrs.get("WorkDescription") or ""

    lat = geom.get("y") or geom.get("lat") or source["lat"]
    lng = geom.get("x") or geom.get("lng") or source["lng"]

    # Date handling from epoch ms
    date_raw = attrs.get("IssuedDate") or attrs.get("ISSUED_DATE") or attrs.get("IssueDate") or 0
    if isinstance(date_raw, (int, float)) and date_raw > 1e10:
        issue_date = datetime.fromtimestamp(date_raw / 1000).strftime("%Y-%m-%d")
    else:
        issue_date = str(date_raw)[:10] if date_raw else datetime.now().strftime("%Y-%m-%d")

    days_old = 0
    try:
        dt = datetime.fromisoformat(issue_date)
        days_old = max(0, (datetime.now() - dt).days)
    except (ValueError, TypeError):
        pass

    score, temp, urgency = calculate_score(days_old, valuation, permit_type)
    pri = "hot" if score >= 70 else "warm" if score >= 50 else "med" if score >= 30 else "cold"

    return {
        "id": str(perm) if perm else f"ARC-{source['city']}-{attrs.get('OBJECTID', 0)}",
        "addr": addr,
        "city": source["city"],
        "st": source["st"],
        "zip": str(attrs.get("ZipCode") or attrs.get("ZIP") or attrs.get("Zip") or ""),
        "lat": float(lat),
        "lng": float(lng),
        "type": permit_type,
        "desc": work_desc,
        "score": score,
        "pri": pri,
        "val": valuation,
        "sqft": 0,
        "perm": str(perm),
        "owner": str(attrs.get("OwnerName") or attrs.get("OWNER") or "Pending lookup"),
        "phone": "Pending enrichment",
        "email": "Pending enrichment",
        "daysAgo": days_old,
        "filed": issue_date,
        "sold": False,
        "gc": attrs.get("ContractorName") or attrs.get("CONTRACTOR") or None,
        "stage": "new",
        "history": [],
        "apn": str(attrs.get("APN") or attrs.get("ParcelNumber") or ""),
        "zone": str(attrs.get("Zoning") or attrs.get("ZONING") or ""),
        "occ": "",
        "stories": 1,
        "units": 1,
        "lotSz": 0,
        "inspDone": 0,
        "inspTotal": 8,
        "source": source["city"],
        "temperature": temp,
        "urgency": urgency,
    }


_sync_lock = None

async def _get_sync_lock():
    global _sync_lock
    if _sync_lock is None:
        _sync_lock = asyncio.Lock()
    return _sync_lock

async def sync_data():
    """Background task to sync permit data from all verified sources + backend cache."""
    lock = await _get_sync_lock()
    if lock.locked():
        logger.info("Sync already in progress, skipping")
        return
    async with lock:
        return await _sync_data_inner()

async def _sync_data_inner():
    logger.info("Starting multi-city data sync...")

    # ── Phase 1: Instant load — backend cache + DB → memory (serves 1.5M+ immediately) ──
    backend_leads = load_backend_cache()
    db_leads = load_leads_db()

    if backend_leads or db_leads:
        seen_keys = set()
        phase1_leads = []
        # DB leads first (enriched), then backend cache
        for lead in db_leads:
            key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
            if key not in seen_keys:
                seen_keys.add(key)
                phase1_leads.append(lead)
        for lead in backend_leads:
            key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
            if key not in seen_keys:
                seen_keys.add(key)
                phase1_leads.append(lead)
        phase1_leads.sort(key=lambda x: x.get("score", 0), reverse=True)
        # Fill missing zip codes from nearby leads with known zips
        if _HAS_ZIP_LOOKUP:
            try:
                zip_filled = _fill_missing_zips(phase1_leads)
                if zip_filled:
                    logger.info(f"Phase 1 zip fill: {zip_filled:,} leads got zips from neighbors")
            except Exception as e:
                logger.warning(f"Zip fill failed: {e}")
        DataCache.set_memory(phase1_leads)
        logger.info(f"Phase 1 loaded: {len(phase1_leads):,} leads available immediately ({len(db_leads):,} DB + {len(backend_leads):,} backend)")

    # ── Phase 2: Fresh API fetch from hardcoded Socrata sources ──
    fresh_leads = []
    socrata_tasks = [fetch_socrata_async(src, days=90, limit=5000) for src in VERIFIED_PERMIT_SOURCES]
    socrata_results = await asyncio.gather(*socrata_tasks, return_exceptions=True)

    for src, result in zip(VERIFIED_PERMIT_SOURCES, socrata_results):
        if isinstance(result, Exception):
            logger.warning(f"Socrata {src['city']} failed: {result}")
            continue
        if not isinstance(result, list):
            continue
        for rec in result:
            try:
                lead = normalize_socrata_lead(rec, src)
                if lead["addr"]:
                    fresh_leads.append(lead)
            except Exception as e:
                logger.debug(f"Skip record from {src['city']}: {e}")

    # Fetch from ArcGIS sources in parallel
    arcgis_tasks = [fetch_arcgis_async(src, limit=1000) for src in ARCGIS_PERMIT_SOURCES]
    arcgis_results = await asyncio.gather(*arcgis_tasks, return_exceptions=True)

    for src, result in zip(ARCGIS_PERMIT_SOURCES, arcgis_results):
        if isinstance(result, Exception):
            logger.warning(f"ArcGIS {src['city']} failed: {result}")
            continue
        if not isinstance(result, list):
            continue
        for feature in result:
            try:
                lead = normalize_arcgis_lead(feature, src)
                if lead["addr"]:
                    fresh_leads.append(lead)
            except Exception as e:
                logger.debug(f"Skip ArcGIS record from {src['city']}: {e}")

    logger.info(f"Fresh API fetch: {len(fresh_leads):,} leads from hardcoded sources")

    # ── Phase 3: Merge fresh leads into memory cache ──
    seen_keys = set()
    all_leads = []

    # Fresh API leads take highest priority
    for lead in fresh_leads:
        key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
        if key not in seen_keys:
            seen_keys.add(key)
            all_leads.append(lead)

    # Then DB leads
    for lead in db_leads:
        key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
        if key not in seen_keys:
            seen_keys.add(key)
            all_leads.append(lead)

    # Then backend cache
    for lead in backend_leads:
        key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
        if key not in seen_keys:
            seen_keys.add(key)
            all_leads.append(lead)

    all_leads.sort(key=lambda x: x.get("score", 0), reverse=True)
    # Fill missing zip codes from nearby leads
    if _HAS_ZIP_LOOKUP:
        try:
            zip_filled = _fill_missing_zips(all_leads)
            if zip_filled:
                logger.info(f"Phase 3 zip fill: {zip_filled:,} leads got zips from neighbors")
        except Exception as e:
            logger.warning(f"Zip fill failed: {e}")
    DataCache.set_memory(all_leads)
    DataCache.save(fresh_leads)

    logger.info(f"Phase 2+3 merged: {len(all_leads):,} leads ({len(fresh_leads):,} fresh + {len(db_leads):,} DB + {len(backend_leads):,} backend)")

    # ── Phase 4: Orchestrator sync (4,000+ sources) — runs after API is serving ──
    if _HAS_SOURCE_REGISTRY:
        try:
            orchestrator = SyncOrchestrator(calculate_score_fn=calculate_score)
            orch_stats = await orchestrator.run_batch()
            logger.info(
                f"Orchestrator: {orch_stats['total_records']} records from "
                f"{orch_stats['total_sources']} sources, "
                f"{orch_stats['new_leads']} new leads"
            )
            # Reload DB leads to pick up orchestrator's new records
            if orch_stats.get("new_leads", 0) > 0:
                db_leads_updated = load_leads_db()
                seen_keys_final = set()
                final_leads = []
                for lead in fresh_leads:
                    key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
                    if key not in seen_keys_final:
                        seen_keys_final.add(key)
                        final_leads.append(lead)
                for lead in db_leads_updated:
                    key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
                    if key not in seen_keys_final:
                        seen_keys_final.add(key)
                        final_leads.append(lead)
                for lead in backend_leads:
                    key = (str(lead.get("addr", "")).upper().strip(), str(lead.get("city", "")).upper().strip())
                    if key not in seen_keys_final:
                        seen_keys_final.add(key)
                        final_leads.append(lead)
                final_leads.sort(key=lambda x: x.get("score", 0), reverse=True)
                DataCache.set_memory(final_leads)
                logger.info(f"Phase 4 final: {len(final_leads):,} leads (after orchestrator)")
        except Exception as e:
            logger.error(f"Orchestrator sync failed: {e}")

    logger.info(f"Multi-city sync complete: {len(DataCache.load() or []):,} total leads in memory")
    return DataCache.load()

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Run data sync on startup"""
    logger.info("Application starting up...")

    # Initialize source registry (4,000+ APIs from CSV)
    if _HAS_SOURCE_REGISTRY:
        try:
            init_source_tables()
            count = get_source_count()
            if count == 0:
                logger.info("Source registry empty — importing from CSV...")
                existing_socrata_ids = {s["rid"] for s in VERIFIED_PERMIT_SOURCES}
                existing_arcgis_urls = {s["url"].split("?")[0].rstrip("/") for s in ARCGIS_PERMIT_SOURCES}
                stats = import_from_csv(
                    existing_socrata_ids=existing_socrata_ids,
                    existing_arcgis_urls=existing_arcgis_urls,
                )
                logger.info(f"Imported {stats['imported']} API sources from CSV")
            else:
                logger.info(f"Source registry has {count} API sources")
        except Exception as e:
            logger.error(f"Source registry init failed: {e}")

    # Don't block startup, run sync in background
    asyncio.create_task(sync_data())

# Mount static files (frontend) — backend/static/ has all HTML/CSS/JS
_BACKEND_STATIC = _BASE_DIR / "backend" / "static"
if _BACKEND_STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_BACKEND_STATIC)), name="static")
else:
    logger.warning(f"Static directory not found: {_BACKEND_STATIC}")

_STATIC_DIR = _BASE_DIR / "backend" / "static"

@app.get("/")
async def root():
    """Serve the SPA (React handles all routing internally)"""
    return FileResponse(str(_STATIC_DIR / "index.html"))

@app.get("/landing")
async def landing():
    return FileResponse(str(_STATIC_DIR / "index.html"))

@app.get("/login")
async def login_page():
    return FileResponse(str(_STATIC_DIR / "login.html"))

@app.get("/app")
async def app_page():
    return FileResponse(str(_STATIC_DIR / "index.html"))

@app.get("/admin")
async def admin_page():
    return FileResponse(str(_STATIC_DIR / "admin.html"))

# ============================================================================
# US permit endpoints (registry + ingest)
# ============================================================================

@app.get("/us/permits/summary")
async def us_permits_summary():
    return {
        "sources": len(load_us_sources()),
        "ingested_rows": len(load_csv_dicts(US_PERMITS_CSV)),
    }

@app.post("/us/permits/discover")
async def us_permits_discover(request: Request, background_tasks: BackgroundTasks = None):
    """
    Discover free permit APIs (Socrata/CKAN) and update permit_sources.json.
    """
    token = _extract_admin_token(request)
    with _session_lock:
        if token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    if background_tasks:
        background_tasks.add_task(discover_all_sources)
        return {"status": "started"}
    sources = discover_all_sources()
    return {"status": "done", "sources": len(sources)}

@app.post("/us/permits/sync")
async def us_permits_sync(request: Request, days: int = 30, limit: int = 500, background_tasks: BackgroundTasks = None):
    """
    Ingest free/open permit APIs defined in permit_sources.json.
    """
    token = _extract_admin_token(request)
    with _session_lock:
        if token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    limit = max(1, min(limit, 1000))
    if background_tasks:
        background_tasks.add_task(ingest_us_permits, days, limit)
        return {"status": "started", "days": days, "limit": limit}
    else:
        data = ingest_us_permits(days, limit)
        return {"status": "done", "rows": len(data)}

@app.get("/us/permits/export")
async def us_permits_export(request: Request):
    token = _extract_admin_token(request)
    with _session_lock:
        if token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    if not US_PERMITS_CSV.exists():
        raise HTTPException(status_code=404, detail="No ingested permits yet")
    return FileResponse(str(US_PERMITS_CSV), filename="us_permits_ingested.csv")

_leads_rate_limit = {}  # ip -> list of timestamps
_LEADS_RATE_MAX = 30
_LEADS_RATE_WINDOW = 60  # seconds

@app.get("/api/leads")
async def get_leads(request: Request, page: int = 1, limit: int = 200000, offset: int = 0, city: str = None, state: str = None, min_score: int = 0):
    """Get leads with pagination. Defaults to ALL leads."""
    # Rate limiting: 30 requests per minute per IP
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    with _session_lock:
        timestamps = _leads_rate_limit.get(client_ip, [])
        timestamps = [t for t in timestamps if now - t < _LEADS_RATE_WINDOW]
        if len(timestamps) >= _LEADS_RATE_MAX:
            return JSONResponse(status_code=429, content={"ok": False, "error": "Rate limit exceeded. Max 30 requests per minute."})
        timestamps.append(now)
        _leads_rate_limit[client_ip] = timestamps
    # Input validation
    limit = min(limit, 500000)
    page = max(1, min(page, 10000))
    city = (city or "")[:100] or None
    state = (state or "")[:50] or None
    cached = DataCache.load()
    if not cached:
        return {"leads": [], "source": "syncing", "total": 0, "cities": [], "sources_active": 0, "states": []}

    # Clean city names — use LRU cache to avoid re-cleaning on every request
    cleaned = []
    for lead in cached:
        city_raw = lead.get("city", "")
        source = lead.get("source", "")
        clean = _cached_clean_city(city_raw, source)
        if clean != city_raw:
            lead = {**lead, "city": clean}  # immutable — new dict
        cleaned.append(lead)
    cached = cleaned

    # Optional filters
    if city and city != "any":
        cached = [l for l in cached if l.get("city", "").lower() == city.lower()]
    if state and state != "any":
        cached = [l for l in cached if l.get("st", "").upper() == state.upper()]
    if min_score > 0:
        cached = [l for l in cached if l.get("score", 0) >= min_score]

    total_count = len(cached)
    # Build city list from cleaned names
    _raw_cities = Counter(l.get("city", "") for l in cached if l.get("city"))
    all_cities = sorted(
        c for c, cnt in _raw_cities.items()
        if len(c) > 2
        and cnt >= 5
        and c != 'Unknown'
    )
    all_states = sorted(s for s in set(l.get("st", "") for l in cached if l.get("st")) if len(s) == 2)
    sources_active = len(set(l.get("source", "") for l in cached))

    # Paginate — support both page-based and offset-based
    limit = max(1, min(limit, 500000))
    offset = max(0, offset)
    if offset > 0:
        start_idx = offset
    else:
        page = max(1, page)
        start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    page_leads = cached[start_idx:end_idx]

    return {
        "leads": page_leads,
        "source": "cache",
        "total": total_count,
        "count": len(page_leads),
        "cities": all_cities,
        "states": all_states,
        "sources_active": sources_active,
        "pagination": {
            "page": page, "limit": limit, "total": total_count,
            "total_pages": (total_count + limit - 1) // limit if limit > 0 else 1,
            "has_next": end_idx < total_count, "has_prev": page > 1
        }
    }


@app.get("/api/leads/map")
async def get_leads_map(request: Request, limit: int = 500000):
    """
    Lightweight map endpoint — returns ONLY lat/lng/pri/score/city for the map.
    Uses compact arrays for minimal JSON size (~40MB vs ~300MB for full leads).
    Also returns pre-computed clusters for instant zoomed-out rendering.
    """
    # Rate limiting: same pool as /api/leads
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    with _session_lock:
        timestamps = _leads_rate_limit.get(client_ip, [])
        timestamps = [t for t in timestamps if now - t < _LEADS_RATE_WINDOW]
        if len(timestamps) >= _LEADS_RATE_MAX:
            return JSONResponse(status_code=429, content={"ok": False, "error": "Rate limit exceeded."})
        timestamps.append(now)
        _leads_rate_limit[client_ip] = timestamps

    cached = DataCache.load()
    if not cached:
        return {"points": [], "clusters": [], "total": 0}

    limit = min(limit, 500000)  # Cap at 500K

    # Build clusters from ALL leads (fast, O(n) single pass)
    CLUSTER_RES = 0.5  # degrees per cluster cell
    cluster_grid = {}
    _PRI_MAP = {"hot": 0, "warm": 1, "med": 2, "cold": 3}

    # Build minimal point list — top `limit` leads only, sorted by score already
    points = []
    for lead in cached:
        lat = lead.get("lat")
        lng = lead.get("lng")
        if not lat or not lng:
            continue
        try:
            lat_f = float(lat)
            lng_f = float(lng)
        except (ValueError, TypeError):
            continue
        if lat_f == 0 or lng_f == 0:
            continue

        pri_str = lead.get("pri", "med")

        # Always cluster (from all leads with coords)
        cx = int(lat_f / CLUSTER_RES)
        cy = int(lng_f / CLUSTER_RES)
        key = (cx, cy)
        if key not in cluster_grid:
            cluster_grid[key] = [0.0, 0.0, 0, 0, 0]  # lat_sum, lng_sum, count, hot, warm
        c = cluster_grid[key]
        c[0] += lat_f
        c[1] += lng_f
        c[2] += 1
        if pri_str == "hot":
            c[3] += 1
        elif pri_str == "warm":
            c[4] += 1

        # Only add to points array if under limit — minimal fields for map rendering
        if len(points) < limit:
            points.append([
                lead.get("id", ""),   # [0] id
                round(lat_f, 5),      # [1] lat
                round(lng_f, 5),      # [2] lng
                _PRI_MAP.get(pri_str, 2),  # [3] pri as int (0=hot,1=warm,2=med,3=cold)
                lead.get("score", 50),     # [4] score
                lead.get("city", ""),      # [5] city
            ])

    clusters = []
    for cell in cluster_grid.values():
        n = cell[2]
        clusters.append({
            "lat": round(cell[0] / n, 4),
            "lng": round(cell[1] / n, 4),
            "n": n,
            "hot": cell[3],
            "warm": cell[4],
        })

    result = {"points": points, "clusters": clusters, "total": len(points), "total_all": len(cached)}
    # Use orjson for 3-5x faster serialization on large payloads
    if _HAS_ORJSON:
        return Response(
            content=orjson.dumps(result),
            media_type="application/json"
        )
    return result


# ============================================================================
# GOOGLE OAUTH — handled by backend/routes/oauth.py (mounted below)
# ============================================================================


# ============================================================================
# STRIPE INTEGRATION
# ============================================================================
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

STRIPE_PRICE_IDS = {
    "starter": os.getenv("STRIPE_PRICE_STARTER", ""),
    "pro": os.getenv("STRIPE_PRICE_PRO", ""),
    "enterprise": os.getenv("STRIPE_PRICE_ENTERPRISE", ""),
}

@app.post("/api/stripe/create-checkout")
async def stripe_create_checkout(request: Request):
    """Create a Stripe Checkout session for subscription"""
    if not STRIPE_SECRET_KEY:
        return JSONResponse(status_code=503, content={"ok": False, "error": "Stripe not configured. Set STRIPE_SECRET_KEY env var."})
    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY
        body = await request.json()
        plan_id = body.get("plan_id", "pro")
        email = body.get("email", "")
        annual = body.get("annual", False)
        price_id = STRIPE_PRICE_IDS.get(plan_id, STRIPE_PRICE_IDS.get("pro", ""))
        if not price_id:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"No Stripe price configured for plan: {plan_id}"})
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            customer_email=email or None,
            success_url=f"{os.getenv('APP_ORIGIN', 'http://localhost:18000')}/app?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{os.getenv('APP_ORIGIN', 'http://localhost:18000')}/",
            metadata={"plan": plan_id, "annual": str(annual)}
        )
        return {"ok": True, "url": session.url, "session_id": session.id}
    except ImportError:
        return JSONResponse(status_code=503, content={"ok": False, "error": "stripe package not installed. Run: pip install stripe"})
    except Exception as e:
        logger.error(f"Stripe checkout error: {e}")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    """Handle Stripe webhooks for subscription events"""
    if not STRIPE_SECRET_KEY or not STRIPE_WEBHOOK_SECRET:
        return JSONResponse(status_code=503, content={"ok": False})
    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY
        payload = await request.body()
        sig = request.headers.get("stripe-signature", "")
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        if event["type"] == "checkout.session.completed":
            session = event["data"]["object"]
            logger.info(f"Stripe checkout completed: {session.get('customer_email')} - plan: {session.get('metadata', {}).get('plan')}")
        elif event["type"] == "customer.subscription.deleted":
            logger.info(f"Subscription cancelled: {event['data']['object'].get('customer')}")
        return {"ok": True}
    except Exception as e:
        logger.error(f"Stripe webhook error: {e}")
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

@app.get("/api/stripe/config")
async def stripe_config():
    """Return Stripe publishable key for frontend"""
    return {"publishable_key": STRIPE_PUBLISHABLE_KEY or "", "configured": bool(STRIPE_SECRET_KEY)}


# ============================================================================
# ADMIN AUTH
# ============================================================================

ADMIN_USERNAME = os.getenv("ADMIN_USER", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASS", "")
_admin_disabled = not ADMIN_USERNAME or not ADMIN_PASSWORD
if _admin_disabled:
    logger.critical("ADMIN_USER and ADMIN_PASS env vars not set — admin login disabled")
_admin_sessions = {}  # token -> expiry
_oauth_states = {}    # state -> expiry timestamp
_session_lock = threading.Lock()
_login_attempts = {}  # ip -> (count, first_attempt_time)
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 300

@app.post("/api/admin/login")
async def admin_login(request: Request):
    """Admin login — returns session token."""
    if _admin_disabled:
        return JSONResponse(status_code=503, content={"ok": False, "error": "Admin login not configured"})
    # Rate limiting (protected by _session_lock to prevent race conditions)
    client_ip = request.client.host if request.client else "unknown"
    now = datetime.now()
    with _session_lock:
        attempts = _login_attempts.get(client_ip)
        if attempts:
            count, first_time = attempts
            if (now - first_time).total_seconds() > _LOGIN_WINDOW_SECONDS:
                _login_attempts[client_ip] = (0, now)
            elif count >= _LOGIN_MAX_ATTEMPTS:
                return JSONResponse(status_code=429, content={"ok": False, "error": "Too many attempts. Try again later."})
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid JSON body"})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"ok": False, "error": "Expected JSON object"})
    username = str(body.get("username", ""))
    password = str(body.get("password", ""))
    # Clean expired sessions and validate credentials (thread-safe)
    with _session_lock:
        expired = [k for k, v in _admin_sessions.items() if datetime.now() > v]
        for k in expired:
            del _admin_sessions[k]
        if hmac.compare_digest(username, ADMIN_USERNAME) and hmac.compare_digest(password, ADMIN_PASSWORD):
            token = secrets.token_urlsafe(32)
            _admin_sessions[token] = datetime.now() + timedelta(hours=8)
            # Cap max sessions at 10 — remove oldest expired if over limit
            if len(_admin_sessions) > 10:
                oldest_key = min(_admin_sessions, key=_admin_sessions.get)
                del _admin_sessions[oldest_key]
            logger.info(f"Admin login successful for {username}")
            _login_attempts.pop(client_ip, None)  # Reset on success
            return {"ok": True, "token": token, "expires_in": 28800}
    # Increment failed attempt counter (protected by lock)
    with _session_lock:
        prev = _login_attempts.get(client_ip, (0, now))
        _login_attempts[client_ip] = (prev[0] + 1, prev[1])
    logger.warning(f"Admin login failed for {username} from {client_ip}")
    return JSONResponse(status_code=401, content={"ok": False, "error": "Invalid credentials"})


def _extract_admin_token(request: Request):
    """Extract admin token from Authorization header only."""
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return ""

@app.post("/api/admin/logout")
async def admin_logout(request: Request):
    token = _extract_admin_token(request)
    with _session_lock:
        _admin_sessions.pop(token, None)
    return {"ok": True}

@app.get("/api/admin/leads")
async def admin_get_leads(request: Request, page: int = 1, limit: int = 50000):
    """Admin endpoint — returns ALL leads without filters, higher limits."""
    effective_token = _extract_admin_token(request)
    with _session_lock:
        session_expiry = _admin_sessions.get(effective_token)
    if not session_expiry or datetime.now() > session_expiry:
        return JSONResponse(status_code=401, content={"ok": False, "error": "Invalid or expired token"})

    cached = DataCache.load()
    if not cached:
        return {"leads": [], "total": 0, "cities": [], "states": [], "sources_active": 0}

    total_count = len(cached)
    all_cities = sorted(set(str(l.get("city", "")) for l in cached if l.get("city")))
    all_states = sorted(set(str(l.get("st", "")) for l in cached if l.get("st")))
    sources_active = len(set(str(l.get("source", "")) for l in cached))

    # Admin has no limit — return all leads
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    page_leads = cached[start_idx:end_idx]

    return {
        "leads": page_leads,
        "total": total_count,
        "count": len(page_leads),
        "cities": all_cities,
        "states": all_states,
        "sources_active": sources_active,
        "admin": True,
        "pagination": {
            "page": page, "limit": limit, "total": total_count,
            "total_pages": (total_count + limit - 1) // limit if limit > 0 else 1,
            "has_next": end_idx < total_count, "has_prev": page > 1
        }
    }


@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    """Admin dashboard stats."""
    effective_token = _extract_admin_token(request)
    with _session_lock:
        session_expiry = _admin_sessions.get(effective_token)
    if not session_expiry or datetime.now() > session_expiry:
        return JSONResponse(status_code=401, content={"ok": False, "error": "Invalid or expired token"})

    cached = DataCache.load()
    if not cached:
        return {"total": 0}

    states = Counter(str(l.get("st", "")) for l in cached if l.get("st"))
    cities = Counter(str(l.get("city", "")) for l in cached if l.get("city"))
    sources = Counter(str(l.get("source", "")) for l in cached)
    scores = {"hot": 0, "warm": 0, "med": 0, "cold": 0}
    total_val = 0
    for l in cached:
        s = l.get("score", 0)
        if s >= 70: scores["hot"] += 1
        elif s >= 50: scores["warm"] += 1
        elif s >= 30: scores["med"] += 1
        else: scores["cold"] += 1
        total_val += l.get("val", 0)

    return {
        "total_leads": len(cached),
        "total_valuation": total_val,
        "states": len(states),
        "cities": len(cities),
        "sources": len(sources),
        "score_breakdown": scores,
        "top_states": dict(states.most_common(20)),
        "top_cities": dict(cities.most_common(30)),
        "top_sources": dict(sources.most_common(20)),
    }


@app.get("/api/sources")
async def get_sources():
    """Return list of all configured permit data sources and their status."""
    sources = []
    for src in VERIFIED_PERMIT_SOURCES:
        sources.append({
            "city": src["city"], "state": src["st"], "type": "socrata",
            "domain": src["domain"], "resource_id": src["rid"],
        })
    for src in ARCGIS_PERMIT_SOURCES:
        sources.append({
            "city": src["city"], "state": src["st"], "type": "arcgis",
            "url": src["url"],
        })
    return {"sources": sources, "total": len(sources)}


# ============================================================================
# ADMIN: Source Registry (4,000+ API sources)
# ============================================================================

@app.get("/api/admin/sources")
async def admin_list_sources(
    request: Request,
    page: int = 1,
    per_page: int = 50,
    source_type: str = "",
    state: str = "",
    status: str = "",
    tier: Optional[int] = None,
    search: str = "",
):
    """List all API sources with filtering and pagination."""
    token = _extract_admin_token(request)
    with _session_lock:
        if not token or token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    if not _HAS_SOURCE_REGISTRY:
        return JSONResponse(status_code=501, content={"error": "Source registry not available"})
    sources, total = get_sources_paginated(
        page=page, per_page=per_page, source_type=source_type,
        state=state, status=status, tier=tier, search=search,
    )
    return {
        "sources": sources,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    }


@app.post("/api/admin/sources/{source_id}/toggle")
async def admin_toggle_source(source_id: int, request: Request):
    """Enable or disable a specific API source."""
    token = _extract_admin_token(request)
    with _session_lock:
        if not token or token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    if not _HAS_SOURCE_REGISTRY:
        return JSONResponse(status_code=501, content={"error": "Source registry not available"})
    try:
        body = await request.json()
        enabled = body.get("enabled", True)
    except Exception:
        enabled = True
    toggle_source(source_id, enabled)
    return {"ok": True, "source_id": source_id, "enabled": enabled}


@app.get("/api/admin/sources/stats")
async def admin_source_stats(request: Request):
    """Get source registry summary statistics."""
    token = _extract_admin_token(request)
    with _session_lock:
        if not token or token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    if not _HAS_SOURCE_REGISTRY:
        return JSONResponse(status_code=501, content={"error": "Source registry not available"})
    return get_registry_stats()


@app.get("/api/admin/sync-status")
async def admin_sync_status(request: Request):
    """Get current sync status and recent sync history."""
    token = _extract_admin_token(request)
    with _session_lock:
        if not token or token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    try:
        from backend.models.database import get_sync_history
        history = get_sync_history(limit=20)
    except Exception as e:
        logger.error(f"Failed to get sync history: {e}")
        history = []
    registry_stats = get_registry_stats() if _HAS_SOURCE_REGISTRY else {}
    return {
        "recent_syncs": history,
        "registry": registry_stats,
        "hardcoded_sources": {
            "socrata": len(VERIFIED_PERMIT_SOURCES),
            "arcgis": len(ARCGIS_PERMIT_SOURCES),
        },
    }


@app.post("/api/lead/{lead_id}/stage")
async def update_lead_stage(lead_id: str, request: Request):
    """Update a lead's pipeline stage."""
    # Accept stage from JSON body or query param for backwards compatibility
    stage = "new"
    try:
        body = await request.json()
        if isinstance(body, dict) and "stage" in body:
            stage = str(body["stage"])
    except Exception:
        pass
    # Fallback to query param
    stage = request.query_params.get("stage", stage)
    token = _extract_admin_token(request)
    with _session_lock:
        if token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    cached = DataCache.load()
    if not cached:
        return JSONResponse(status_code=404, content={"ok": False, "error": "No leads data"})
    VALID_STAGES = {"new", "contacted", "quoted", "proposed", "negotiating", "won", "lost"}
    if stage not in VALID_STAGES:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"Invalid stage. Must be one of: {', '.join(VALID_STAGES)}"})
    # M13 — Update in-place within lock instead of copying entire dataset
    updated = False
    with DataCache._lock:
        if DataCache._memory_cache is not None:
            for lead in DataCache._memory_cache:
                if str(lead.get("id")) == str(lead_id) or lead.get("perm") == lead_id:
                    lead["stage"] = stage
                    updated = True
                    break
    return {"ok": updated, "lead_id": lead_id, "stage": stage}


@app.post("/api/lead/{lead_id}/note")
async def add_lead_note(lead_id: str, request: Request):
    """Add a note to a lead (persisted to lead_notes table)."""
    body = await request.json()
    note_text = body.get("note", "") or body.get("notes", "")
    if not note_text:
        return JSONResponse(status_code=400, content={"error": "Note text required"})
    try:
        import sqlite3
        conn = sqlite3.connect(BACKEND_LEADS_DB)
        conn.execute("CREATE TABLE IF NOT EXISTS lead_notes (id INTEGER PRIMARY KEY AUTOINCREMENT, lead_id TEXT, note TEXT, created_at TEXT)")
        conn.execute("INSERT INTO lead_notes (lead_id, note, created_at) VALUES (?, ?, ?)", (lead_id, note_text, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Note save error: {e}")
    return {"ok": True, "lead_id": lead_id, "note": note_text}


@app.get("/api/analytics")
async def get_analytics():
    """Dashboard analytics — pipeline summary, top cities, permit types, recent activity."""
    leads = DataCache.load() or []
    total = len(leads)
    if total == 0:
        return {"total": 0, "pipeline": {}, "top_cities": [], "permit_types": [], "avg_score": 0, "avg_value": 0}

    # Pipeline stage counts
    pipeline = {}
    city_counter = Counter()
    type_counter = Counter()
    scores = []
    values = []
    hot_count = 0
    for lead in leads:
        stage = lead.get("stage", "new")
        pipeline[stage] = pipeline.get(stage, 0) + 1
        city = lead.get("city", "Unknown")
        if city:
            city_counter[city] += 1
        ptype = lead.get("type", "") or lead.get("permit_type", "")
        if ptype:
            type_counter[ptype] += 1
        s = lead.get("score", 0)
        if s:
            scores.append(s)
        v = lead.get("val", 0) or lead.get("valuation", 0) or 0
        if v:
            values.append(v)
        pri = lead.get("pri", "")
        if pri == "hot":
            hot_count += 1

    avg_score = round(sum(scores) / len(scores), 1) if scores else 0
    avg_value = round(sum(values) / len(values), 0) if values else 0
    total_value = sum(values)

    return {
        "total": total,
        "hot_leads": hot_count,
        "pipeline": pipeline,
        "top_cities": [{"city": c, "count": n} for c, n in city_counter.most_common(20)],
        "permit_types": [{"type": t, "count": n} for t, n in type_counter.most_common(15)],
        "avg_score": avg_score,
        "avg_value": avg_value,
        "total_value": total_value,
        "states_covered": len(set(l.get("st", "") or l.get("state", "") for l in leads if l.get("st") or l.get("state"))),
    }


@app.post("/api/sync")
async def trigger_sync(request: Request, background_tasks: BackgroundTasks):
    """Manually trigger data sync"""
    token = _extract_admin_token(request)
    with _session_lock:
        if token not in _admin_sessions or _admin_sessions[token] < datetime.now():
            return JSONResponse(status_code=401, content={"error": "Unauthorized"})
    background_tasks.add_task(sync_data)
    return {"message": "Sync started", "status": "processing"}

@app.get("/api/health")
async def health_check():
    """System health check — verifies core infrastructure, not external API availability."""
    checks = []

    # Check leads database
    try:
        import sqlite3
        conn = sqlite3.connect(BACKEND_LEADS_DB)
        row = conn.execute("SELECT COUNT(*) FROM leads").fetchone()
        conn.close()
        lead_count = row[0] if row else 0
        checks.append(APIStatus(
            name="Leads Database",
            status="online",
            last_check=datetime.now().isoformat(),
            details=f"{lead_count:,} leads",
        ))
    except Exception as e:
        checks.append(APIStatus(
            name="Leads Database",
            status="offline",
            last_check=datetime.now().isoformat(),
            details=str(e)[:100],
        ))

    # Check data cache
    try:
        cache_path = _BASE_DIR / "data_cache.json"
        if cache_path.exists():
            size_mb = cache_path.stat().st_size / (1024 * 1024)
            checks.append(APIStatus(
                name="Data Cache",
                status="online",
                last_check=datetime.now().isoformat(),
                details=f"{size_mb:.1f} MB",
            ))
        else:
            checks.append(APIStatus(
                name="Data Cache",
                status="degraded",
                last_check=datetime.now().isoformat(),
                details="Cache file missing",
            ))
    except Exception as e:
        checks.append(APIStatus(
            name="Data Cache",
            status="offline",
            last_check=datetime.now().isoformat(),
            details=str(e)[:100],
        ))

    # Check source registry
    if _HAS_SOURCE_REGISTRY:
        try:
            from backend.models.source_registry import get_registry_stats
            stats = get_registry_stats()
            checks.append(APIStatus(
                name="Source Registry",
                status="online",
                last_check=datetime.now().isoformat(),
                details=f"{stats.get('total', 0)} sources ({stats.get('active', 0)} active)",
            ))
        except Exception as e:
            checks.append(APIStatus(
                name="Source Registry",
                status="degraded",
                last_check=datetime.now().isoformat(),
                details=str(e)[:100],
            ))

    online_count = sum(1 for c in checks if c.status == "online")
    overall_status = "healthy" if online_count == len(checks) else "degraded" if online_count > 0 else "down"

    return {
        "status": overall_status,
        "checks": checks,
        "timestamp": datetime.now().isoformat(),
    }

@app.get("/api/parcel/{apn}/owner")
async def get_parcel_owner(apn: str):
    """Get property owner information from LA County Assessor by APN"""
    # Clean and validate APN format
    clean_apn = apn.replace('-', '').replace(' ', '')
    if not clean_apn or len(clean_apn) > 20 or not clean_apn.replace('.', '').isalnum():
        raise HTTPException(status_code=400, detail="Invalid APN format")
    
    # LA County Assessor API endpoint for property details
    url = "https://portal.assessor.lacounty.gov/api/parceldetail"
    params = {'ain': clean_apn}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Extract owner information
                    owner_info = {
                        'apn': apn,
                        'owner_name': data.get('OwnerName', ''),
                        'mailing_address': data.get('MailingAddress', ''),
                        'mailing_city': data.get('MailingCity', ''),
                        'mailing_state': data.get('MailingState', ''),
                        'mailing_zip': data.get('MailingZip', ''),
                        'property_address': data.get('SitusAddress', ''),
                        'assessed_value': data.get('AssessedValue', 0),
                        'year_built': data.get('YearBuilt', ''),
                        'square_feet': data.get('SquareFeet', ''),
                        'bedrooms': data.get('Bedrooms', ''),
                        'bathrooms': data.get('Bathrooms', ''),
                        'source': 'LA County Assessor'
                    }
                    return owner_info
                else:
                    raise HTTPException(status_code=response.status, detail="Assessor API error")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Parcel owner lookup failed for APN {apn}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/lead/{lead_id}/enrich-owner")
async def enrich_lead_owner(lead_id: str):
    """Enrich a lead with property owner information"""
    # Load cached leads
    cached = DataCache.load()
    if not cached:
        raise HTTPException(status_code=404, detail="No leads data available")
    
    # Find the lead
    lead = next((l for l in cached if str(l.get('id')) == str(lead_id)), None)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    apn = lead.get('apn')
    if not apn:
        return {
            'success': False,
            'error': 'No APN available for this lead',
            'lead_id': lead_id
        }
    
    try:
        # Get owner info from assessor
        url = f"https://portal.assessor.lacounty.gov/api/parceldetail"
        params = {'ain': apn.replace('-', '').replace(' ', '')}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Format response for frontend
                    result = {
                        'success': True,
                        'lead_id': lead_id,
                        'owner_name': data.get('OwnerName', ''),
                        'phone': '',  # Assessor doesn't provide phone
                        'email': '',  # Assessor doesn't provide email
                        'mailing_address': f"{data.get('MailingAddress', '')} {data.get('MailingCity', '')} {data.get('MailingState', '')} {data.get('MailingZip', '')}".strip(),
                        'property_type': data.get('UseType', ''),
                        'assessed_value': data.get('AssessedValue', 0),
                        'year_built': data.get('YearBuilt', ''),
                        'square_feet': data.get('SquareFeet', 0),
                        'bedrooms': data.get('Bedrooms', 0),
                        'bathrooms': data.get('Bathrooms', 0),
                        'confidence': 95,  # High confidence from official source
                        'sources_used': ['LA County Assessor'],
                        'cost': 0  # Free public API
                    }
                    return result
                else:
                    return {
                        'success': False,
                        'error': f'Assessor API returned status {response.status}',
                        'lead_id': lead_id
                    }
    except Exception as e:
        logger.error(f"Owner enrichment failed for lead {lead_id}: {e}")
        return {
            'success': False,
            'error': 'Owner enrichment failed',
            'lead_id': lead_id
        }

@app.get("/api/parcel/{lat}/{lng}")
async def get_parcel(lat: float, lng: float):
    """Get parcel boundary from LA County Assessor"""
    if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        raise HTTPException(status_code=400, detail="Invalid coordinates")
    url = f"https://maps.assessment.lacounty.gov/GVH_2_2/GVH/wsLegacyService/getParcelByLocation"
    params = {'type': 'json', 'lat': lat, 'lng': lng}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    raise HTTPException(status_code=response.status, detail="Parcel API error")
    except HTTPException:
        raise
    except aiohttp.ClientConnectorError as e:
        logger.warning(f"Parcel service unreachable for ({lat}, {lng}): {e}")
        raise HTTPException(status_code=503, detail="LA County parcel service temporarily unavailable")
    except Exception as e:
        logger.error(f"Parcel lookup failed for ({lat}, {lng}): {e}")
        raise HTTPException(status_code=502, detail="Parcel lookup failed — upstream service error")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 18000))
    uvicorn.run(app, host="0.0.0.0", port=port)
