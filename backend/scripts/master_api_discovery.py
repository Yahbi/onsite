#!/usr/bin/env python3
"""
MASTER API DISCOVERY SCRIPT
============================
Discovers ALL free government APIs for:
- Building/construction permits
- Remodel permits
- New construction permits
- ADU (Accessory Dwelling Unit) permits
- Insurance claims (building/construction related)
- Property sales / transfers
- Code enforcement
- Demolition permits
- All other permit types

Covers every US state, county, city, and ZIP code.
Sources: Socrata, ArcGIS Open Data, Federal APIs, CKAN portals.
Output: ~/Desktop/MASTER_US_FREE_APIS.csv
"""

import asyncio
import aiohttp
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# ── Output ──
OUTPUT_CSV = Path.home() / "Desktop" / "MASTER_US_FREE_APIS.csv"
OUTPUT_JSON = Path.home() / "Desktop" / "MASTER_US_FREE_APIS.json"

# ── US States ──
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "PR": "Puerto Rico", "GU": "Guam", "VI": "Virgin Islands",
}

STATE_NAME_TO_ABBR = {v.lower(): k for k, v in US_STATES.items()}

# ── Socrata domain → state mapping ──
DOMAIN_STATE_MAP = {
    "data.lacity.org": "CA", "data.sfgov.org": "CA", "data.oaklandca.gov": "CA",
    "data.sanjoseca.gov": "CA", "data.cityofsacramento.org": "CA",
    "data.sandiegocounty.gov": "CA", "data.sonomacounty.ca.gov": "CA",
    "data.marincounty.gov": "CA", "corstat.coronaca.gov": "CA",
    "data.roseville.ca.us": "CA", "data.oxnard.org": "CA",
    "data.cityofnewyork.us": "NY", "data.ny.gov": "NY", "data.buffalony.gov": "NY",
    "data.cityofchicago.org": "IL", "datacatalog.cookcountyil.gov": "IL",
    "data.urbanaillinois.us": "IL",
    "data.texas.gov": "TX", "datahub.austintexas.gov": "TX",
    "www.dallasopendata.com": "TX", "data.fortworthtexas.gov": "TX",
    "data.sanantonio.gov": "TX", "data.wcad.org": "TX",
    "data.wa.gov": "WA", "cos-data.seattle.gov": "WA",
    "performance.seattle.gov": "WA", "data.spokanecity.org": "WA",
    "internal.open.piercecountywa.gov": "WA", "open.piercecountywa.gov": "WA",
    "data.colorado.gov": "CO", "opendata.fcgov.com": "CO",
    "data.oregon.gov": "OR", "data.portlandoregon.gov": "OR",
    "data.ct.gov": "CT",
    "data.nola.gov": "LA", "data.brla.gov": "LA",
    "data.nj.gov": "NJ",
    "opendata.utah.gov": "UT",
    "data.maryland.gov": "MD", "opendata.maryland.gov": "MD",
    "data.montgomerycountymd.gov": "MD",
    "data.princegeorgescountymd.gov": "MD",
    "data.kcmo.org": "MO",
    "data.honolulu.gov": "HI", "highways.hidot.hawaii.gov": "HI",
    "mydata.iowa.gov": "IA",
    "data.michigan.gov": "MI", "data.detroitmi.gov": "MI",
    "data.cincinnati-oh.gov": "OH", "insights.cincinnati-oh.gov": "OH",
    "data.ramseycountymn.gov": "MN",
    "data.providenceri.gov": "RI",
    "data.cambridgema.gov": "MA", "data.somervillema.gov": "MA",
    "data.framinghamma.gov": "MA", "data.boston.gov": "MA",
    "data.bloomington.in.gov": "IN",
    "data.norfolk.gov": "VA", "data.virginia.gov": "VA",
    "data.louisvilleky.gov": "KY",
    "data.nashville.gov": "TN", "data.memphistn.gov": "TN",
    "data.cityofgainesville.org": "FL", "data.miami.gov": "FL",
    "data.tallahassee.com": "FL",
    "data.chattanooga.gov": "TN",
    "data.littlerock.gov": "AR",
    "data.cityofdavenportiowa.com": "IA",
    "performance.cityofdavenportiowa.com": "IA",
    "dashboard.plano.gov": "TX",
    "data.pa.gov": "PA", "data.phila.gov": "PA",
    "data.pittsburgh.gov": "PA",
    "janesville.data.socrata.com": "WI",
    "parkeronline.data.socrata.com": "CO",
}


@dataclass
class DiscoveredAPI:
    """A single discovered API endpoint."""
    source_type: str          # Socrata, ArcGIS, Federal, CKAN, Direct
    location: str             # City/County/State name
    location_type: str        # city, county, state, federal, national
    state: str                # State abbreviation
    data_category: str        # permit, property_sales, insurance_claims, etc.
    data_subcategory: str     # building, construction, remodel, adu, demolition, etc.
    api_name: str             # Dataset name
    api_url: str              # Full API URL
    api_id: str = ""          # Resource ID (Socrata) or layer number (ArcGIS)
    domain: str = ""          # Socrata domain
    format: str = ""          # json, csv, geojson, api
    description: str = ""
    last_updated: str = ""
    record_count: int = 0
    sample_fields: str = ""   # Comma-separated field names
    zip_codes_covered: str = ""
    counties_covered: str = ""
    cities_covered: str = ""
    geographic_scope: str = ""  # city, county, state, national


def guess_state_from_domain(domain: str) -> str:
    """Guess state from Socrata domain name."""
    if domain in DOMAIN_STATE_MAP:
        return DOMAIN_STATE_MAP[domain]
    # Try matching state abbreviation in domain
    domain_lower = domain.lower()
    for abbr, name in US_STATES.items():
        if f".{abbr.lower()}." in domain_lower or domain_lower.endswith(f".{abbr.lower()}.gov"):
            return abbr
        name_parts = name.lower().split()
        for part in name_parts:
            if part in domain_lower and len(part) > 3:
                return abbr
    return ""


def guess_location_from_domain(domain: str) -> str:
    """Extract city/county name from Socrata domain."""
    parts = domain.replace("data.", "").replace("opendata.", "").split(".")
    if parts:
        loc = parts[0].replace("-", " ").replace("_", " ")
        # Clean up common suffixes
        for suffix in ["gov", "org", "com", "us", "ca", "socrata"]:
            loc = loc.replace(suffix, "").strip()
        return loc.title()
    return domain


def categorize_dataset(name: str, description: str = "") -> Tuple[str, str]:
    """Categorize a dataset by its name into (category, subcategory)."""
    text = f"{name} {description}".lower()

    # Insurance claims
    if any(kw in text for kw in ["insurance claim", "flood claim", "nfip", "disaster claim",
                                   "hazard mitigation", "fema claim"]):
        return "insurance_claims", "flood_insurance" if "flood" in text else "general"

    # Property sales
    if any(kw in text for kw in ["property sale", "property transfer", "real estate sale",
                                   "deed transfer", "deed record", "property sold",
                                   "home sale", "housing sale"]):
        return "property_sales", "residential" if "residential" in text else "general"

    # ADU
    if any(kw in text for kw in ["accessory dwelling", "adu ", "adu-", "granny flat",
                                   "in-law unit", "secondary dwelling"]):
        return "permit", "adu"

    # Remodel
    if any(kw in text for kw in ["remodel", "renovation", "alteration", "addition",
                                   "improvement", "rehab"]):
        return "permit", "remodel"

    # New construction
    if any(kw in text for kw in ["new construction", "new build", "new residential",
                                   "new commercial", "new dwelling"]):
        return "permit", "new_construction"

    # Demolition
    if "demolit" in text:
        return "permit", "demolition"

    # Code enforcement
    if "code enforcement" in text or "code violation" in text or "code compliance" in text:
        return "code_enforcement", "general"

    # Specific permit types
    for ptype in ["electrical", "plumbing", "mechanical", "fire", "roofing",
                  "solar", "swimming pool", "pool", "sign", "grading",
                  "foundation", "occupancy", "land use", "zoning"]:
        if ptype in text:
            return "permit", ptype.replace(" ", "_")

    # General building/construction permits
    if any(kw in text for kw in ["building permit", "construction permit", "bldg permit"]):
        return "permit", "building"

    # General permits
    if "permit" in text:
        return "permit", "general"

    # Property/parcel data
    if any(kw in text for kw in ["parcel", "assessment", "property tax", "appraiser"]):
        return "property_data", "assessment"

    # Inspections
    if "inspection" in text:
        return "inspection", "general"

    # Contractor/license
    if any(kw in text for kw in ["contractor", "license"]):
        return "contractor", "license"

    return "other", "general"


def is_relevant_dataset(name: str) -> bool:
    """Filter out irrelevant datasets."""
    name_lower = name.lower()
    # Exclude junk
    exclude = [
        "311", "tobacco", "parking meter", "dog license", "marriage",
        "birth", "death certificate", "voting", "election", "ballot",
        "library", "park reservation", "food permit", "health permit",
        "liquor", "taxi", "sidewalk cafe", "street vendor", "film permit",
        "noise", "special event", "parade", "block party", "cannabis",
        "marijuana", "gun", "firearm", "pawn", "gambling", "amusement",
        "massage", "tattoo", "body art",
    ]
    if any(ex in name_lower for ex in exclude):
        return False

    # Must have at least one relevant keyword
    include = [
        "permit", "construction", "building", "remodel", "renovation",
        "adu", "accessory dwelling", "demolition", "property sale",
        "property transfer", "real estate", "insurance claim", "fema",
        "nfip", "flood", "code enforcement", "inspection", "plumbing",
        "electrical", "mechanical", "roofing", "solar", "zoning",
        "land use", "grading", "occupancy", "contractor", "parcel",
        "assessment", "deed",
    ]
    return any(kw in name_lower for kw in include)


# ══════════════════════════════════════════════════════════════
# SOCRATA DISCOVERY
# ══════════════════════════════════════════════════════════════

SOCRATA_SEARCH_QUERIES = [
    "building+permit",
    "construction+permit",
    "remodel+permit",
    "renovation+permit",
    "residential+permit",
    "commercial+permit",
    "accessory+dwelling+unit",
    "ADU+permit",
    "demolition+permit",
    "property+sales",
    "property+transfer",
    "real+estate+sales",
    "deed+transfer",
    "insurance+claims",
    "flood+insurance+claims",
    "FEMA+claims",
    "code+enforcement",
    "zoning+permit",
    "plumbing+permit",
    "electrical+permit",
    "mechanical+permit",
    "fire+permit",
    "roofing+permit",
    "solar+permit",
    "grading+permit",
    "occupancy+permit",
    "certificate+of+occupancy",
    "land+use+permit",
    "building+inspection",
    "new+construction",
    "home+improvement",
    "contractor+license",
    "parcel+data",
    "property+assessment",
    "housing+permit",
    "development+permit",
    "foundation+permit",
    "swimming+pool+permit",
    "sign+permit",
    "tenant+improvement",
    "hazard+mitigation",
    "disaster+housing",
    "housing+assistance",
]

# Additional state-specific searches
STATE_SEARCHES = []
for abbr, name in US_STATES.items():
    name_encoded = name.replace(" ", "+")
    STATE_SEARCHES.extend([
        (f"building+permit+{name_encoded}", abbr),
        (f"construction+permit+{name_encoded}", abbr),
        (f"property+sales+{name_encoded}", abbr),
    ])

# Major city searches
MAJOR_CITIES = [
    ("New+York", "NY"), ("Los+Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San+Antonio", "TX"), ("San+Diego", "CA"), ("Dallas", "TX"),
    ("San+Jose", "CA"), ("Austin", "TX"), ("Jacksonville", "FL"),
    ("Fort+Worth", "TX"), ("Columbus", "OH"), ("Indianapolis", "IN"),
    ("Charlotte", "NC"), ("San+Francisco", "CA"), ("Seattle", "WA"),
    ("Denver", "CO"), ("Nashville", "TN"), ("Oklahoma+City", "OK"),
    ("El+Paso", "TX"), ("Washington+DC", "DC"), ("Boston", "MA"),
    ("Portland+Oregon", "OR"), ("Las+Vegas", "NV"), ("Memphis", "TN"),
    ("Louisville", "KY"), ("Baltimore", "MD"), ("Milwaukee", "WI"),
    ("Albuquerque", "NM"), ("Tucson", "AZ"), ("Fresno", "CA"),
    ("Sacramento", "CA"), ("Mesa", "AZ"), ("Kansas+City", "MO"),
    ("Atlanta", "GA"), ("Omaha", "NE"), ("Colorado+Springs", "CO"),
    ("Raleigh", "NC"), ("Long+Beach", "CA"), ("Virginia+Beach", "VA"),
    ("Miami", "FL"), ("Oakland", "CA"), ("Minneapolis", "MN"),
    ("Tampa", "FL"), ("Tulsa", "OK"), ("Arlington", "TX"),
    ("New+Orleans", "LA"), ("Wichita", "KS"), ("Cleveland", "OH"),
    ("Bakersfield", "CA"), ("Aurora", "CO"), ("Anaheim", "CA"),
    ("Honolulu", "HI"), ("Santa+Ana", "CA"), ("Riverside", "CA"),
    ("Corpus+Christi", "TX"), ("Lexington", "KY"), ("Pittsburgh", "PA"),
    ("Anchorage", "AK"), ("Stockton", "CA"), ("Cincinnati", "OH"),
    ("Saint+Paul", "MN"), ("Greensboro", "NC"), ("Toledo", "OH"),
    ("Newark", "NJ"), ("Plano", "TX"), ("Henderson", "NV"),
    ("Lincoln", "NE"), ("Buffalo", "NY"), ("Fort+Wayne", "IN"),
    ("Jersey+City", "NJ"), ("Chula+Vista", "CA"), ("Norfolk", "VA"),
    ("Orlando", "FL"), ("Chandler", "AZ"), ("St+Petersburg", "FL"),
    ("Laredo", "TX"), ("Madison", "WI"), ("Durham", "NC"),
    ("Lubbock", "TX"), ("Winston+Salem", "NC"), ("Garland", "TX"),
    ("Glendale", "AZ"), ("Hialeah", "FL"), ("Reno", "NV"),
    ("Baton+Rouge", "LA"), ("Irvine", "CA"), ("Chesapeake", "VA"),
    ("Irving", "TX"), ("Scottsdale", "AZ"), ("North+Las+Vegas", "NV"),
    ("Fremont", "CA"), ("Gilbert", "AZ"), ("San+Bernardino", "CA"),
    ("Boise", "ID"), ("Birmingham", "AL"), ("Rochester", "NY"),
    ("Richmond", "VA"), ("Spokane", "WA"), ("Des+Moines", "IA"),
    ("Montgomery", "AL"), ("Modesto", "CA"), ("Fayetteville", "NC"),
    ("Tacoma", "WA"), ("Shreveport", "LA"), ("Fontana", "CA"),
    ("Moreno+Valley", "CA"), ("Akron", "OH"), ("Yonkers", "NY"),
    ("Columbus", "GA"), ("Aurora", "IL"), ("Little+Rock", "AR"),
    ("Amarillo", "TX"), ("Huntington+Beach", "CA"), ("Grand+Rapids", "MI"),
    ("Mobile", "AL"), ("Salt+Lake+City", "UT"), ("Tallahassee", "FL"),
    ("Huntsville", "AL"), ("Worcester", "MA"), ("Knoxville", "TN"),
    ("Newport+News", "VA"), ("Brownsville", "TX"), ("Providence", "RI"),
    ("Santa+Clarita", "CA"), ("Garden+Grove", "CA"), ("Oceanside", "CA"),
    ("Chattanooga", "TN"), ("Fort+Lauderdale", "FL"), ("Rancho+Cucamonga", "CA"),
    ("Santa+Rosa", "CA"), ("Port+Arthur", "TX"), ("Tempe", "AZ"),
    ("Ontario", "CA"), ("Vancouver", "WA"), ("Cape+Coral", "FL"),
    ("Sioux+Falls", "SD"), ("Springfield", "MO"), ("Peoria", "AZ"),
    ("Pembroke+Pines", "FL"), ("Elk+Grove", "CA"), ("Salem", "OR"),
    ("Corona", "CA"), ("Eugene", "OR"), ("McKinney", "TX"),
    ("Fort+Collins", "CO"), ("Cary", "NC"), ("Hayward", "CA"),
    ("Frisco", "TX"), ("Pasadena", "TX"), ("Joliet", "IL"),
    ("Paterson", "NJ"), ("Macon", "GA"), ("Savannah", "GA"),
    ("Charleston", "SC"), ("Greenville", "SC"), ("Columbia", "SC"),
    ("Jackson", "MS"), ("Billings", "MT"), ("Manchester", "NH"),
    ("Burlington", "VT"), ("Wilmington", "DE"), ("Fargo", "ND"),
    ("Sioux+Falls", "SD"), ("Cheyenne", "WY"), ("Bangor", "ME"),
]

for city, state in MAJOR_CITIES:
    STATE_SEARCHES.extend([
        (f"building+permit+{city}", state),
        (f"property+sales+{city}", state),
    ])


async def discover_socrata(session: aiohttp.ClientSession) -> List[DiscoveredAPI]:
    """Discover all relevant datasets from Socrata Discovery API."""
    print("\n══════════════════════════════════════")
    print("  PHASE 1: SOCRATA DISCOVERY")
    print("══════════════════════════════════════")

    all_datasets: Dict[str, DiscoveredAPI] = {}
    sem = asyncio.Semaphore(5)  # Rate limit

    async def search_query(query: str, expected_state: str = ""):
        async with sem:
            offset = 0
            while offset < 1000:
                url = (
                    f"https://api.us.socrata.com/api/catalog/v1"
                    f"?q={query}&limit=100&offset={offset}&only=datasets"
                )
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                        results = data.get("results", [])
                        if not results:
                            break

                        for r in results:
                            res = r.get("resource", {})
                            rid = res.get("id", "")
                            domain = r.get("metadata", {}).get("domain", "")
                            name = res.get("name", "")
                            key = f"{domain}/{rid}"

                            if key in all_datasets:
                                continue

                            if not is_relevant_dataset(name):
                                continue

                            # Filter out non-US domains
                            if any(x in domain for x in [
                                ".ca.", "calgary", "edmonton", "winnipeg",
                                "novascotia", "thedatazone", ".gov.co",
                                "datos.gov", "celebratingcities",
                            ]):
                                continue

                            desc = (res.get("description", "") or "")[:300]
                            category, subcategory = categorize_dataset(name, desc)
                            state = expected_state or guess_state_from_domain(domain)
                            location = guess_location_from_domain(domain)
                            columns = res.get("columns_field_name", [])
                            updated = res.get("updatedAt", "")

                            api = DiscoveredAPI(
                                source_type="Socrata",
                                location=location,
                                location_type="city",
                                state=state,
                                data_category=category,
                                data_subcategory=subcategory,
                                api_name=name,
                                api_url=f"https://{domain}/resource/{rid}.json",
                                api_id=rid,
                                domain=domain,
                                format="json",
                                description=desc,
                                last_updated=updated[:10] if updated else "",
                                sample_fields=",".join(columns[:15]),
                                geographic_scope="city",
                            )
                            all_datasets[key] = api

                        offset += 100
                        if len(results) < 100:
                            break
                except Exception:
                    break
            await asyncio.sleep(0.05)

    # Run general searches
    print(f"  Searching {len(SOCRATA_SEARCH_QUERIES)} general queries...")
    tasks = [search_query(q) for q in SOCRATA_SEARCH_QUERIES]
    await asyncio.gather(*tasks)
    print(f"  Found {len(all_datasets)} datasets after general queries")

    # Run state-specific searches in batches
    print(f"  Searching {len(STATE_SEARCHES)} state/city queries...")
    batch_size = 20
    for i in range(0, len(STATE_SEARCHES), batch_size):
        batch = STATE_SEARCHES[i:i + batch_size]
        tasks = [search_query(q, st) for q, st in batch]
        await asyncio.gather(*tasks)
        if (i + batch_size) % 100 == 0:
            print(f"    ...{i + batch_size}/{len(STATE_SEARCHES)} queries done, {len(all_datasets)} total")

    print(f"  ✓ Socrata: {len(all_datasets)} datasets across {len(set(d.domain for d in all_datasets.values()))} domains")
    return list(all_datasets.values())


# ══════════════════════════════════════════════════════════════
# ARCGIS DISCOVERY
# ══════════════════════════════════════════════════════════════

ARCGIS_HUB_SEARCH_URL = "https://opendata.arcgis.com/api/v3/search"

# Known ArcGIS open data portals with permit data
KNOWN_ARCGIS_ENDPOINTS = [
    # DC
    ("maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer", "DC", "DC DCRA Permits", [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]),
    # Portland OR
    ("www.portlandmaps.com/arcgis/rest/services/Public/BDS_Permit/FeatureServer", "OR", "Portland BDS Permits", [22]),
    # Minneapolis
    ("services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/CCS_Permits/FeatureServer", "MN", "Minneapolis Permits", [0]),
    # MN Metro
    ("arcgis.metc.state.mn.us/server/rest/services/VWResidentialPermits/FeatureServer", "MN", "MN Metro Residential Permits", [0]),
    # Forsyth County GA
    ("geo.forsythco.com/gis3/rest/services/Building_Permits/FeatureServer", "GA", "Forsyth County GA Permits", [0]),
    # Denver
    ("services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_RESIDENTIALCONSTPERMIT_P/FeatureServer", "CO", "Denver Residential Permits", [316]),
    ("services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_COMMCONSTPERMIT_P/FeatureServer", "CO", "Denver Commercial Permits", [0]),
    # Bozeman MT
    ("services3.arcgis.com/f4hk1qcfxRJ0L2BU/arcgis/rest/services/Building_Permits/FeatureServer", "MT", "Bozeman MT Permits", [0]),
    # San Francisco
    ("data.sfgov.org/resource/i98e-djp9.json", "CA", "SF Building Permits", []),
    # LA County
    ("public.gis.lacounty.gov/public/rest/services/LACounty_Dynamic/LMS_Data_Public/MapServer", "CA", "LA County Permits", [14]),
    # Maricopa County AZ
    ("geo.maricopa.gov/arcgis/rest/services/OpenData/Planning/FeatureServer", "AZ", "Maricopa County Planning", [0]),
    # Clark County NV (Las Vegas)
    ("services1.arcgis.com/JkIiLupAqlOmTpqR/arcgis/rest/services/Building_Permits/FeatureServer", "NV", "Clark County Permits", [0]),
    # Hillsborough County FL (Tampa)
    ("services.arcgis.com/apTfC6SUmnNfnxuF/arcgis/rest/services/BuildingPermits/FeatureServer", "FL", "Hillsborough County Permits", [0]),
    # Orange County FL (Orlando)
    ("services2.arcgis.com/LNSfMVWOFPaJgiHE/arcgis/rest/services/Building_Permits/FeatureServer", "FL", "Orange County FL Permits", [0]),
    # Mecklenburg County NC (Charlotte)
    ("services.arcgis.com/5vnLOHCzN2vc6f3z/arcgis/rest/services/Building_Permits/FeatureServer", "NC", "Mecklenburg County Permits", [0]),
    # Wake County NC (Raleigh)
    ("services.wakegov.com/arcgis/rest/services/Property/BuildingPermits/FeatureServer", "NC", "Wake County Permits", [0]),
    # Hamilton County OH (Cincinnati)
    ("services2.arcgis.com/RuWfnw8Wpm0gKapm/arcgis/rest/services/Building_Permits/FeatureServer", "OH", "Hamilton County Permits", [0]),
    # Fairfax County VA
    ("services1.arcgis.com/wnXTZFcAnMbD2WSv/arcgis/rest/services/Building_Permits/FeatureServer", "VA", "Fairfax County Permits", [0]),
    # Montgomery County MD
    ("services.arcgis.com/tf0dBEGsI4tKNiK9/arcgis/rest/services/Building_Permits/FeatureServer", "MD", "Montgomery County MD Permits", [0]),
    # Dallas County TX
    ("services.arcgis.com/BYMo6dC1FSzBawEH/arcgis/rest/services/Building_Permits/FeatureServer", "TX", "Dallas County Permits", [0]),
    # Bexar County TX (San Antonio)
    ("services.arcgis.com/g1fRTDLeMgspWrYp/arcgis/rest/services/Building_Permits/FeatureServer", "TX", "Bexar County Permits", [0]),
    # Tarrant County TX (Fort Worth)
    ("services.arcgis.com/ZzrwjXRi6tA5WLYb/arcgis/rest/services/Building_Permits/FeatureServer", "TX", "Tarrant County Permits", [0]),
]

ARCGIS_SEARCH_QUERIES = [
    "building permits",
    "construction permits",
    "remodel permits",
    "demolition permits",
    "property sales",
    "insurance claims",
    "code enforcement",
    "ADU permits",
    "accessory dwelling",
    "zoning permits",
    "electrical permits",
    "plumbing permits",
    "building inspections",
    "new construction",
]


async def discover_arcgis(session: aiohttp.ClientSession) -> List[DiscoveredAPI]:
    """Discover ArcGIS open data hub datasets."""
    print("\n══════════════════════════════════════")
    print("  PHASE 2: ARCGIS DISCOVERY")
    print("══════════════════════════════════════")

    apis: List[DiscoveredAPI] = []

    # Add known endpoints
    for base_url, state, name, layers in KNOWN_ARCGIS_ENDPOINTS:
        if layers:
            for layer in layers:
                full_url = f"https://{base_url}/{layer}/query?where=1%3D1&outFields=*&f=json&resultOffset=0&resultRecordCount=1"
                category, subcategory = categorize_dataset(name)
                apis.append(DiscoveredAPI(
                    source_type="ArcGIS",
                    location=name.split(" ")[0],
                    location_type="county" if "County" in name else "city",
                    state=state,
                    data_category=category,
                    data_subcategory=subcategory,
                    api_name=f"{name} (Layer {layer})",
                    api_url=f"https://{base_url}/{layer}/query?where=1%3D1&outFields=*&f=json",
                    api_id=str(layer),
                    format="json",
                    geographic_scope="county" if "County" in name else "city",
                ))
        else:
            category, subcategory = categorize_dataset(name)
            apis.append(DiscoveredAPI(
                source_type="ArcGIS",
                location=name.split(" ")[0],
                location_type="city",
                state=state,
                data_category=category,
                data_subcategory=subcategory,
                api_name=name,
                api_url=f"https://{base_url}" if base_url.startswith("http") else f"https://{base_url}",
                format="json",
                geographic_scope="city",
            ))

    print(f"  Added {len(apis)} known ArcGIS endpoints")

    # Search ArcGIS Hub
    sem = asyncio.Semaphore(3)

    async def search_hub(query: str):
        async with sem:
            found = []
            try:
                params = urllib.parse.urlencode({
                    "q": query,
                    "filter[type]": "Feature Service",
                    "page[size]": 100,
                })
                url = f"https://opendata.arcgis.com/api/v3/search?{params}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data.get("data", []):
                            attrs = item.get("attributes", {})
                            name = attrs.get("name", "")
                            source_url = attrs.get("url", "") or attrs.get("source", "")
                            if not source_url or not is_relevant_dataset(name):
                                continue
                            desc = (attrs.get("description", "") or "")[:300]
                            category, subcategory = categorize_dataset(name, desc)
                            org = attrs.get("organization", "") or ""

                            # Try to guess state
                            state = ""
                            for abbr, sname in US_STATES.items():
                                if sname.lower() in org.lower() or sname.lower() in name.lower():
                                    state = abbr
                                    break

                            found.append(DiscoveredAPI(
                                source_type="ArcGIS",
                                location=org or "Unknown",
                                location_type="city",
                                state=state,
                                data_category=category,
                                data_subcategory=subcategory,
                                api_name=name,
                                api_url=source_url,
                                format="json",
                                description=desc,
                                geographic_scope="city",
                            ))
            except Exception:
                pass
            return found

    print(f"  Searching ArcGIS Hub with {len(ARCGIS_SEARCH_QUERIES)} queries...")
    tasks = [search_hub(q) for q in ARCGIS_SEARCH_QUERIES]
    results = await asyncio.gather(*tasks)
    for batch in results:
        apis.extend(batch)

    # Deduplicate by URL
    seen_urls: Set[str] = set()
    unique_apis = []
    for api in apis:
        url_key = api.api_url.split("?")[0].lower()
        if url_key not in seen_urls:
            seen_urls.add(url_key)
            unique_apis.append(api)

    print(f"  ✓ ArcGIS: {len(unique_apis)} unique endpoints")
    return unique_apis


# ══════════════════════════════════════════════════════════════
# FEDERAL APIs
# ══════════════════════════════════════════════════════════════

FEDERAL_APIS = [
    # FEMA
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="flood_insurance",
        api_name="FEMA NFIP Flood Insurance Claims",
        api_url="https://www.fema.gov/api/open/v2/FimaNfipClaims",
        format="json", geographic_scope="national",
        description="National Flood Insurance Program claims data, all states",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="housing_assistance",
        api_name="FEMA Housing Assistance Owners",
        api_url="https://www.fema.gov/api/open/v2/HousingAssistanceOwners",
        format="json", geographic_scope="national",
        description="FEMA housing assistance for property owners after disasters",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="housing_assistance",
        api_name="FEMA Housing Assistance Renters",
        api_url="https://www.fema.gov/api/open/v2/HousingAssistanceRenters",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="disaster",
        api_name="FEMA Individual Assistance Housing Large Disasters",
        api_url="https://www.fema.gov/api/open/v2/IndividualAssistanceHousingRegistrantsLargeDisasters",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="disaster",
        api_name="FEMA Registration Intake - Individual Household Programs",
        api_url="https://www.fema.gov/api/open/v2/RegistrationIntakeIndividualsHouseholdPrograms",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="disaster",
        api_name="FEMA Disaster Declarations Summaries",
        api_url="https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="hazard_mitigation",
        api_name="FEMA Hazard Mitigation Grant Property Acquisitions",
        api_url="https://www.fema.gov/api/open/v2/HazardMitigationGrantProgramPropertyAcquisitions",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="flood_insurance",
        api_name="FEMA NFIP Policies",
        api_url="https://www.fema.gov/api/open/v2/FimaNfipPolicies",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="disaster",
        api_name="FEMA Public Assistance Funded Projects Details",
        api_url="https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="disaster",
        api_name="FEMA Hazard Mitigation Assistance Projects",
        api_url="https://www.fema.gov/api/open/v2/HazardMitigationAssistanceProjects",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="insurance_claims", data_subcategory="sba_disaster",
        api_name="SBA Disaster Loan Data",
        api_url="https://www.fema.gov/api/open/v2/SbaDeclarations",
        format="json", geographic_scope="national",
    ),
    # Census Bureau
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="permit", data_subcategory="building",
        api_name="Census Bureau Building Permits Survey",
        api_url="https://api.census.gov/data/timeseries/bps/",
        format="json", geographic_scope="national",
        description="Monthly new residential construction permits by place/county/state",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="permit", data_subcategory="building",
        api_name="Census Bureau Annual Building Permits",
        api_url="https://api.census.gov/data/timeseries/bps/",
        format="json", geographic_scope="national",
    ),
    # HUD
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="property_data", data_subcategory="fha",
        api_name="HUD FHA Single Family Loan Performance",
        api_url="https://api.hud.gov/v1/fha_sf_loan_performance",
        format="json", geographic_scope="national",
    ),
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="property_sales", data_subcategory="fhfa",
        api_name="FHFA House Price Index",
        api_url="https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index-Datasets.aspx",
        format="csv", geographic_scope="national",
        description="Federal Housing Finance Agency house price index by MSA, state, ZIP",
    ),
    # EPA
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="permit", data_subcategory="environmental",
        api_name="EPA ECHO Facility Search",
        api_url="https://echodata.epa.gov/echo/facilities?output=JSON",
        format="json", geographic_scope="national",
        description="EPA facility permits and compliance data",
    ),
    # OSHA
    DiscoveredAPI(
        source_type="Federal", location="National", location_type="federal",
        state="ALL", data_category="inspection", data_subcategory="construction",
        api_name="OSHA Construction Inspections",
        api_url="https://enforcedata.dol.gov/views/data_catalogs.php",
        format="csv", geographic_scope="national",
    ),
]


# ══════════════════════════════════════════════════════════════
# GEOGRAPHY DATA
# ══════════════════════════════════════════════════════════════

def load_geography() -> dict:
    """Load complete US geography from Census data."""
    geo = {
        "states": [],
        "counties": [],
        "places": [],
        "zip_codes": [],
    }

    # Load states
    states_file = Path("/tmp/census_states.json")
    if states_file.exists():
        with open(states_file) as f:
            data = json.load(f)
            for row in data[1:]:
                geo["states"].append({
                    "name": row[0],
                    "fips": row[1],
                })

    # Load counties
    counties_file = Path("/tmp/census_counties.json")
    if counties_file.exists():
        with open(counties_file) as f:
            data = json.load(f)
            for row in data[1:]:
                geo["counties"].append({
                    "name": row[0],
                    "state_fips": row[1],
                    "county_fips": row[2],
                })

    # Load places
    places_file = Path("/tmp/census_places.json")
    if places_file.exists():
        with open(places_file) as f:
            data = json.load(f)
            for row in data[1:]:
                geo["places"].append({
                    "name": row[0],
                    "state_fips": row[1],
                    "place_fips": row[2],
                })

    # Load ZIP codes
    zip_file = Path("/tmp/us_geo_data.csv")
    if zip_file.exists():
        with open(zip_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                geo["zip_codes"].append({
                    "zipcode": row.get("zipcode", ""),
                    "city": row.get("city", ""),
                    "state": row.get("state_abbr", ""),
                    "county": row.get("county", ""),
                })

    return geo


# ══════════════════════════════════════════════════════════════
# DATA.GOV CKAN SEARCH
# ══════════════════════════════════════════════════════════════

async def discover_datagov(session: aiohttp.ClientSession) -> List[DiscoveredAPI]:
    """Search data.gov CKAN catalog for relevant datasets."""
    print("\n══════════════════════════════════════")
    print("  PHASE 3: DATA.GOV / CKAN DISCOVERY")
    print("══════════════════════════════════════")

    apis: List[DiscoveredAPI] = []
    sem = asyncio.Semaphore(3)

    queries = [
        "building permits", "construction permits", "property sales",
        "insurance claims construction", "remodel permits", "demolition permits",
        "ADU permits", "accessory dwelling unit", "code enforcement",
        "zoning permits", "new construction", "residential permits",
    ]

    async def search_ckan(query: str):
        async with sem:
            found = []
            try:
                params = urllib.parse.urlencode({
                    "q": query,
                    "rows": 100,
                    "fq": "organization_type:City OR organization_type:County OR organization_type:State",
                })
                url = f"https://catalog.data.gov/api/3/action/package_search?{params}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        return found
                    data = await resp.json()
                    results = data.get("result", {}).get("results", [])

                    for pkg in results:
                        name = pkg.get("title", "")
                        if not is_relevant_dataset(name):
                            continue

                        desc = (pkg.get("notes", "") or "")[:300]
                        org = pkg.get("organization", {}).get("title", "") if pkg.get("organization") else ""
                        category, subcategory = categorize_dataset(name, desc)

                        # Find best resource URL
                        best_url = ""
                        best_format = ""
                        for resource in pkg.get("resources", []):
                            fmt = (resource.get("format", "") or "").lower()
                            rurl = resource.get("url", "")
                            if fmt in ("api", "json", "geojson", "csv"):
                                best_url = rurl
                                best_format = fmt
                                break
                            elif rurl and not best_url:
                                best_url = rurl
                                best_format = fmt

                        if not best_url:
                            continue

                        # Guess state
                        state = ""
                        for abbr, sname in US_STATES.items():
                            if sname.lower() in org.lower() or sname.lower() in name.lower():
                                state = abbr
                                break

                        found.append(DiscoveredAPI(
                            source_type="CKAN",
                            location=org or "Unknown",
                            location_type="city",
                            state=state,
                            data_category=category,
                            data_subcategory=subcategory,
                            api_name=name,
                            api_url=best_url,
                            format=best_format,
                            description=desc,
                            geographic_scope="city",
                        ))
            except Exception:
                pass
            return found

    print(f"  Searching data.gov with {len(queries)} queries...")
    tasks = [search_ckan(q) for q in queries]
    results = await asyncio.gather(*tasks)
    for batch in results:
        apis.extend(batch)

    # Deduplicate
    seen: Set[str] = set()
    unique = []
    for api in apis:
        key = api.api_url.lower()
        if key not in seen:
            seen.add(key)
            unique.append(api)

    print(f"  ✓ data.gov/CKAN: {len(unique)} datasets")
    return unique


# ══════════════════════════════════════════════════════════════
# OPENDATASOFT DISCOVERY
# ══════════════════════════════════════════════════════════════

async def discover_opendatasoft(session: aiohttp.ClientSession) -> List[DiscoveredAPI]:
    """Search OpenDataSoft portals."""
    print("\n══════════════════════════════════════")
    print("  PHASE 4: OPENDATASOFT DISCOVERY")
    print("══════════════════════════════════════")

    apis: List[DiscoveredAPI] = []

    queries = [
        "building permits", "construction permits", "property sales",
        "demolition permits", "code enforcement",
    ]

    for query in queries:
        try:
            params = urllib.parse.urlencode({
                "q": query,
                "rows": 50,
            })
            url = f"https://data.opendatasoft.com/api/v2/catalog/datasets?{params}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for ds in data.get("datasets", []):
                        meta = ds.get("metas", {}).get("default", {})
                        name = meta.get("title", "") or ds.get("dataset_id", "")
                        if not is_relevant_dataset(name):
                            continue

                        desc = (meta.get("description", "") or "")[:300]
                        category, subcategory = categorize_dataset(name, desc)
                        dsid = ds.get("dataset_id", "")

                        apis.append(DiscoveredAPI(
                            source_type="OpenDataSoft",
                            location=meta.get("publisher", "Unknown"),
                            location_type="city",
                            state="",
                            data_category=category,
                            data_subcategory=subcategory,
                            api_name=name,
                            api_url=f"https://data.opendatasoft.com/api/v2/catalog/datasets/{dsid}/records",
                            api_id=dsid,
                            format="json",
                            description=desc,
                            geographic_scope="city",
                        ))
        except Exception:
            pass
        await asyncio.sleep(0.1)

    print(f"  ✓ OpenDataSoft: {len(apis)} datasets")
    return apis


# ══════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════

async def main():
    start = time.time()
    print("=" * 60)
    print("  MASTER US FREE API DISCOVERY")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load geography
    print("\nLoading US geography data...")
    geo = load_geography()
    print(f"  States: {len(geo['states'])}")
    print(f"  Counties: {len(geo['counties'])}")
    print(f"  Places (cities/CDPs): {len(geo['places'])}")
    print(f"  ZIP codes: {len(geo['zip_codes'])}")

    # Run all discoveries
    all_apis: List[DiscoveredAPI] = []

    connector = aiohttp.TCPConnector(limit=20, limit_per_host=5)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": "Onsite-Discovery/1.0"},
    ) as session:
        # Run Socrata, ArcGIS, data.gov, and OpenDataSoft in parallel
        socrata_task = asyncio.create_task(discover_socrata(session))
        arcgis_task = asyncio.create_task(discover_arcgis(session))
        datagov_task = asyncio.create_task(discover_datagov(session))
        ods_task = asyncio.create_task(discover_opendatasoft(session))

        socrata_apis = await socrata_task
        arcgis_apis = await arcgis_task
        datagov_apis = await datagov_task
        ods_apis = await ods_task

        all_apis.extend(socrata_apis)
        all_apis.extend(arcgis_apis)
        all_apis.extend(FEDERAL_APIS)
        all_apis.extend(datagov_apis)
        all_apis.extend(ods_apis)

    # Global dedup by URL
    seen_urls: Set[str] = set()
    unique_apis: List[DiscoveredAPI] = []
    for api in all_apis:
        url_key = api.api_url.split("?")[0].lower().rstrip("/")
        if url_key not in seen_urls:
            seen_urls.add(url_key)
            unique_apis.append(api)

    # ── Write CSV ──
    print(f"\n{'=' * 60}")
    print(f"  WRITING RESULTS")
    print(f"{'=' * 60}")

    fieldnames = [
        "source_type", "location", "location_type", "state",
        "data_category", "data_subcategory", "api_name", "api_url",
        "api_id", "domain", "format", "description",
        "last_updated", "record_count", "sample_fields",
        "zip_codes_covered", "counties_covered", "cities_covered",
        "geographic_scope",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for api in sorted(unique_apis, key=lambda x: (x.state, x.source_type, x.data_category)):
            writer.writerow(asdict(api))

    # ── Write JSON ──
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated": datetime.now().isoformat(),
                "total_apis": len(unique_apis),
                "geography": {
                    "states": len(geo["states"]),
                    "counties": len(geo["counties"]),
                    "places": len(geo["places"]),
                    "zip_codes": len(geo["zip_codes"]),
                },
                "apis": [asdict(api) for api in unique_apis],
            },
            f,
            indent=2,
        )

    # ── Summary ──
    elapsed = time.time() - start
    by_source = Counter(a.source_type for a in unique_apis)
    by_category = Counter(a.data_category for a in unique_apis)
    by_state = Counter(a.state for a in unique_apis)
    states_covered = {a.state for a in unique_apis if a.state and a.state != "ALL"}

    print(f"\n  Total unique APIs: {len(unique_apis)}")
    print(f"\n  By source:")
    for src, cnt in by_source.most_common():
        print(f"    {src}: {cnt}")
    print(f"\n  By category:")
    for cat, cnt in by_category.most_common():
        print(f"    {cat}: {cnt}")
    print(f"\n  States covered: {len(states_covered)}/{len(US_STATES)}")
    missing = set(US_STATES.keys()) - states_covered
    if missing:
        print(f"  Missing states: {', '.join(sorted(missing))}")
    print(f"\n  Output CSV: {OUTPUT_CSV}")
    print(f"  Output JSON: {OUTPUT_JSON}")
    print(f"\n  Completed in {elapsed:.1f}s")

    # ── Append geography summary rows ──
    print(f"\n  Appending geography reference data...")
    geo_csv = Path.home() / "Desktop" / "US_COMPLETE_GEOGRAPHY.csv"
    with open(geo_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["type", "name", "state", "fips_code", "zipcode", "county", "city"])

        # States
        fips_to_state = {}
        for s in geo["states"]:
            abbr = STATE_NAME_TO_ABBR.get(s["name"].lower(), "")
            fips_to_state[s["fips"]] = abbr
            writer.writerow(["state", s["name"], abbr, s["fips"], "", "", ""])

        # Counties
        for c in geo["counties"]:
            state_abbr = fips_to_state.get(c["state_fips"], "")
            writer.writerow(["county", c["name"], state_abbr, f"{c['state_fips']}{c['county_fips']}", "", "", ""])

        # Places
        for p in geo["places"]:
            state_abbr = fips_to_state.get(p["state_fips"], "")
            writer.writerow(["place", p["name"], state_abbr, f"{p['state_fips']}{p['place_fips']}", "", "", ""])

        # ZIP codes
        for z in geo["zip_codes"]:
            writer.writerow(["zipcode", "", z.get("state", ""), "", z.get("zipcode", ""), z.get("county", ""), z.get("city", "")])

    total_geo = len(geo["states"]) + len(geo["counties"]) + len(geo["places"]) + len(geo["zip_codes"])
    print(f"  Geography CSV: {geo_csv} ({total_geo} records)")
    print(f"\n{'=' * 60}")
    print(f"  ALL DONE!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
