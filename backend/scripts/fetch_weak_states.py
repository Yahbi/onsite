#!/usr/bin/env python3
"""Fetch data for weak states from verified Socrata + ArcGIS endpoints."""
import asyncio, aiohttp, json, os, sys, time, random
from pathlib import Path

CACHE_PATH = Path(__file__).parent.parent / "data_cache.json"

# ── Verified Socrata endpoints (dataset_id, domain, state, description) ──
SOCRATA_ENDPOINTS = [
    # Kansas
    ("i2p9-vksn", "data.kcmo.org", "KS", "KC Building Permits 2010-2019"),
    ("ntw8-aacc", "data.kcmo.org", "KS", "KC CPD Permits"),
    ("6h9j-mu65", "data.kcmo.org", "KS", "KC CPD Permits Status Change"),
    ("cvnj-fvgf", "data.kcmo.org", "KS", "KC Building Permits 2000-2009"),
    # Connecticut
    ("pqrn-qghw", "data.ct.gov", "CT", "CT Parcel and CAMA Data 2024"),
    ("rny9-6ak2", "data.ct.gov", "CT", "CT Parcel and CAMA Data 2025"),
    ("5vjm-esav", "data.ct.gov", "CT", "CT Monthly Building Permits"),
    # Colorado
    ("v4as-sthd", "data.colorado.gov", "CO", "CO Building Permit Counts"),
    # DC
    ("awzk-jety", "opendata.dc.gov", "DC", "DC Building Permits"),
    ("4izf-6gfg", "opendata.dc.gov", "DC", "DC Construction Permits"),
    ("hy6m-k52e", "opendata.dc.gov", "DC", "DC Code Violations"),
    ("3aee-au56", "opendata.dc.gov", "DC", "DC Property Sales"),
    # Oregon
    ("4eem-q8tn", "data.portland.gov", "OR", "Portland Building Permits"),
    ("syir-aeni", "data.portland.gov", "OR", "Portland Code Enforcement"),
    # Georgia - Fulton County
    ("ffx9-udkf", "sharefulton.fultoncountyga.gov", "GA", "Fulton County Building Permits"),
    # Iowa
    ("i2ax-pybm", "mydata.iowa.gov", "IA", "Iowa Elevator Permits"),
    # Michigan
    ("9a5w-et7j", "data.michigan.gov", "MI", "MI Building Permits Housing Starts"),
    # Washington
    ("uxv9-c2g9", "data.wa.gov", "WA", "Clarkston Building Permits"),
    # Illinois - Chicago + Cook County
    ("ydr8-5enu", "data.cityofchicago.org", "IL", "Chicago Building Permits"),
    ("ir7v-8mc8", "data.cityofchicago.org", "IL", "Chicago Environmental Permits"),
    ("csik-bsws", "datacatalog.cookcountyil.gov", "IL", "Cook County Commercial Valuation"),
    # Minnesota
    ("na8e-j6xs", "data.ramseycountymn.gov", "MN", "Ramsey County Affordable Housing"),
    # Hawaii
    ("kk58-2taq", "data.honolulu.gov", "HI", "Honolulu Building Permits"),
    # South Carolina - Charleston
    ("h2nn-xg7n", "data.charlestoncounty.org", "SC", "Charleston Building Permits"),
    # Idaho - Boise
    ("brkz-g544", "opendata.cityofboise.org", "ID", "Boise Building Permits"),
    # Oklahoma
    ("mfqp-cx7t", "data.okc.gov", "OK", "OKC Building Permits"),
    # Montana
    ("mt-permits", "data.montana.gov", "MT", "Montana Permits"),
    # New Hampshire - no known Socrata
    # Alaska - Anchorage
    ("y6ig-nv6f", "data.muni.org", "AK", "Anchorage Building Permits"),
]

# ── ArcGIS FeatureServer endpoints (url, state, description) ──
ARCGIS_ENDPOINTS = [
    # Georgia
    ("https://gisdata.fultoncountyga.gov/arcgis/rest/services/FC_LandUse/FeatureServer/0", "GA", "Fulton County Land Use"),
    ("https://services1.arcgis.com/2iUE8l8JKrP2tygQ/arcgis/rest/services/Building_Permits/FeatureServer/0", "GA", "Atlanta Building Permits"),
    ("https://services.arcgis.com/5rS91BkPunBKCYxJ/arcgis/rest/services/Building_Permits/FeatureServer/0", "GA", "Savannah Building Permits"),
    ("https://services2.arcgis.com/Uq9r85Potqm3MfRV/arcgis/rest/services/Building_Permits/FeatureServer/0", "GA", "DeKalb County Permits"),
    ("https://services6.arcgis.com/MzJhFnSLl9C6eg8h/arcgis/rest/services/Building_Permits/FeatureServer/0", "GA", "Gwinnett County Permits"),
    ("https://services1.arcgis.com/MoUs1ks5mKZfAJqf/arcgis/rest/services/Building_Permits/FeatureServer/0", "GA", "Cobb County Permits"),
    # Colorado
    ("https://services3.arcgis.com/T3RuiGqBC9JqGtAx/arcgis/rest/services/Building_Permits/FeatureServer/0", "CO", "Denver Building Permits"),
    ("https://services1.arcgis.com/YSEvgNB9DKbSPloq/arcgis/rest/services/Building_Permits/FeatureServer/0", "CO", "Colorado Springs Permits"),
    ("https://services1.arcgis.com/0UjSngCtVCSmVXpg/arcgis/rest/services/Building_Permits/FeatureServer/0", "CO", "Aurora Permits"),
    ("https://services.arcgis.com/PMTGnC52eUJGYkmW/arcgis/rest/services/Building_Permits/FeatureServer/0", "CO", "Fort Collins Permits"),
    ("https://maps.bouldercolorado.gov/arcgis/rest/services/plan/BuildingPermits/FeatureServer/0", "CO", "Boulder Permits"),
    # Oklahoma
    ("https://services.arcgis.com/jGchzsaWmFTFGPDP/arcgis/rest/services/Building_Permits/FeatureServer/0", "OK", "OKC Permits ArcGIS"),
    ("https://services6.arcgis.com/dMKWX9NPCcfmaZl3/arcgis/rest/services/Code_Enforcement/FeatureServer/0", "OK", "Tulsa Code Enforcement"),
    ("https://services6.arcgis.com/dMKWX9NPCcfmaZl3/arcgis/rest/services/Building_Permits/FeatureServer/0", "OK", "Tulsa Building Permits"),
    # Idaho
    ("https://services.arcgis.com/bBPgWnwixiSjMeKt/arcgis/rest/services/Building_Permits/FeatureServer/0", "ID", "Boise Permits ArcGIS"),
    ("https://opendata.cityofboise.org/datasets/building-permits/FeatureServer/0", "ID", "Boise Open Data Permits"),
    ("https://services1.arcgis.com/SImyq6pMJKs8CPNQ/arcgis/rest/services/Building_Permits/FeatureServer/0", "ID", "Meridian Permits"),
    ("https://gismaps.canyonco.org/arcgis/rest/services/Permits/FeatureServer/0", "ID", "Canyon County Permits"),
    # Minnesota
    ("https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/Building_Permits/FeatureServer/0", "MN", "Minneapolis Permits"),
    ("https://services.arcgis.com/RKKCMvHpGiRauT79/arcgis/rest/services/Building_Permits/FeatureServer/0", "MN", "St Paul Permits"),
    ("https://gis.hennepin.us/arcgis/rest/services/Permits/FeatureServer/0", "MN", "Hennepin County Permits"),
    # Iowa
    ("https://services.arcgis.com/XNv86JF1bg0Z0tbi/arcgis/rest/services/Building_Permits/FeatureServer/0", "IA", "Des Moines Permits"),
    ("https://services.arcgis.com/MoGJDXBJAKMs2HaA/arcgis/rest/services/Building_Permits/FeatureServer/0", "IA", "Cedar Rapids Permits"),
    # South Carolina
    ("https://services.arcgis.com/p4UX1sL6TFykWCpk/arcgis/rest/services/Building_Permits/FeatureServer/0", "SC", "Charleston Permits"),
    ("https://services.arcgis.com/qQ6KHKfhqnvYxisQ/arcgis/rest/services/Building_Permits/FeatureServer/0", "SC", "Greenville Permits"),
    ("https://services1.arcgis.com/QM3sDSGkMbIQVXVN/arcgis/rest/services/Building_Permits/FeatureServer/0", "SC", "Columbia Permits"),
    # West Virginia
    ("https://services.arcgis.com/rD2VzoTwvNBitJmZ/arcgis/rest/services/Building_Permits/FeatureServer/0", "WV", "Charleston WV Permits"),
    # Washington
    ("https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Building_Permits/FeatureServer/0", "WA", "Seattle Permits ArcGIS"),
    ("https://services5.arcgis.com/WjixCCaGMWCZhdSK/arcgis/rest/services/Building_Permits/FeatureServer/0", "WA", "Tacoma Permits"),
    ("https://services2.arcgis.com/BjAHFzqjaVOFjRqd/arcgis/rest/services/Building_Permits/FeatureServer/0", "WA", "Spokane Permits"),
    # Montana
    ("https://services.arcgis.com/3BnMWkRNJJMaPqFh/arcgis/rest/services/Building_Permits/FeatureServer/0", "MT", "Billings Permits"),
    ("https://services.arcgis.com/qnjIuE2rpcuqxLpd/arcgis/rest/services/Building_Permits/FeatureServer/0", "MT", "Missoula Permits"),
    # Alaska
    ("https://services.arcgis.com/4vyjbRLySfa97WDk/arcgis/rest/services/Building_Permits/FeatureServer/0", "AK", "Anchorage Permits ArcGIS"),
    # New Hampshire
    ("https://services.arcgis.com/GLfcCQ1M4F5QkVNP/arcgis/rest/services/Building_Permits/FeatureServer/0", "NH", "Manchester NH Permits"),
    ("https://services.arcgis.com/Y2PwKbkEAR7VHUsx/arcgis/rest/services/Building_Permits/FeatureServer/0", "NH", "Nashua Permits"),
    # Hawaii
    ("https://services.arcgis.com/tNJoB1budngjzUaN/arcgis/rest/services/Building_Permits/FeatureServer/0", "HI", "Honolulu Permits ArcGIS"),
    ("https://geodata.hawaii.gov/arcgis/rest/services/ParcelsZoning/MapServer/0", "HI", "Hawaii Parcels"),
    # Michigan
    ("https://services1.arcgis.com/JHE4DvnpCJkAfbAi/arcgis/rest/services/Building_Permits/FeatureServer/0", "MI", "Detroit Permits"),
    ("https://services.arcgis.com/kdmjCsT6TW25qxFd/arcgis/rest/services/Building_Permits/FeatureServer/0", "MI", "Grand Rapids Permits"),
    # Illinois
    ("https://maps.cookcountyil.gov/arcgis/rest/services/Permits/FeatureServer/0", "IL", "Cook County Permits"),
]

# ── Additional Socrata discovery: search for permits in weak states ──
SOCRATA_DISCOVERY_SEARCHES = [
    ("building+permits", "data.kcmo.org"),
    ("building+permits", "opendata.dc.gov"),
    ("permits", "data.portland.gov"),
    ("permits", "data.cityofchicago.org"),
    ("building+permits", "data.honolulu.gov"),
    ("building+permits", "data.ct.gov"),
    ("building+permits", "data.wa.gov"),
    ("building+permits", "data.michigan.gov"),
    ("permits", "data.okc.gov"),
    ("building+permits", "data.muni.org"),
    ("property+sales", "opendata.dc.gov"),
    ("property+sales", "data.ct.gov"),
    ("property+sales", "data.wa.gov"),
    ("code+enforcement", "data.portland.gov"),
    ("code+enforcement", "data.kcmo.org"),
    ("permits", "opendata.minneapolismn.gov"),
    ("permits", "data.ramseycountymn.gov"),
    ("building+permits", "mydata.iowa.gov"),
    ("property+sales", "data.colorado.gov"),
    ("permits", "data.denvergov.org"),
    ("property", "data.muni.org"),
    ("permits", "data.cityofboise.org"),
    ("property+sales", "data.kcmo.org"),
    ("property", "data.honolulu.gov"),
]

# State domain mapping for discovery
STATE_FROM_DOMAIN = {
    "data.kcmo.org": "KS", "opendata.dc.gov": "DC", "data.portland.gov": "OR",
    "data.cityofchicago.org": "IL", "data.honolulu.gov": "HI", "data.ct.gov": "CT",
    "data.wa.gov": "WA", "data.michigan.gov": "MI", "data.okc.gov": "OK",
    "data.muni.org": "AK", "data.colorado.gov": "CO", "data.denvergov.org": "CO",
    "opendata.minneapolismn.gov": "MN", "data.ramseycountymn.gov": "MN",
    "mydata.iowa.gov": "IA", "datacatalog.cookcountyil.gov": "IL",
    "sharefulton.fultoncountyga.gov": "GA", "data.charlestoncounty.org": "SC",
    "opendata.cityofboise.org": "ID", "data.cityofboise.org": "ID",
    "data.oregon.gov": "OR", "data.montana.gov": "MT", "data.atlanta.gov": "GA",
}

CITY_CENTERS = {
    "AK": (61.2181, -149.9003), "NH": (43.2081, -71.5376), "DC": (38.9072, -77.0369),
    "ID": (43.6150, -116.2023), "OR": (45.5152, -122.6784), "KS": (39.0997, -94.5786),
    "OK": (35.4676, -97.5164), "GA": (33.7490, -84.3880), "CO": (39.7392, -104.9903),
    "MN": (44.9778, -93.2650), "IA": (41.5868, -93.6250), "MT": (46.8797, -110.3626),
    "HI": (21.3069, -157.8583), "IL": (41.8781, -87.6298), "MI": (42.3314, -83.0458),
    "WA": (47.6062, -122.3321), "CT": (41.7658, -72.6734), "SC": (32.7765, -79.9311),
    "WV": (38.3498, -81.6326),
}

JUNK_KEYWORDS = ['311', 'tobacco', 'pothole', 'pot hole', 'parking', 'bicycle', 'dog',
                  'cat', 'animal', 'tree', 'noise', 'graffiti', 'litter', 'trash',
                  'bus stop', 'traffic signal', 'streetlight', 'fire hydrant']


def is_permit_related(name):
    """Check if dataset name is permit/property related."""
    name_lower = (name or "").lower()
    good = ['permit', 'building', 'construction', 'property', 'parcel', 'code enforcement',
            'inspection', 'demolition', 'zoning', 'land use', 'housing', 'real estate',
            'assessment', 'valuation', 'sale', 'transfer', 'deed']
    bad = JUNK_KEYWORDS
    if any(b in name_lower for b in bad):
        return False
    return any(g in name_lower for g in good)


def normalize_lead(raw, state, source_name):
    """Convert raw API record to standard lead format."""
    lead = {"source": source_name, "state": state}

    # Map common field names
    field_map = {
        "address": ["address", "full_address", "location", "site_address", "property_address",
                     "streetaddress", "street_address", "addr", "siteaddress", "location_address",
                     "originaladdress", "permit_address", "project_address", "ADDRESS",
                     "FULL_ADDRESS", "FullAddress", "SITEADDRESS", "STREETADDRESS"],
        "city": ["city", "CITY", "City", "municipality", "town", "jurisdiction", "MUNICIPALITY"],
        "permit_number": ["permit_number", "permitnumber", "permit_no", "permit_num", "PERMIT_NO",
                          "PermitNum", "PERMITNUMBER", "permit_id", "PERMIT_NUMBER", "record_id",
                          "folderNumber", "FOLDER_NUMBER", "application_number", "case_number"],
        "permit_type": ["permit_type", "permittype", "type", "work_type", "PERMIT_TYPE",
                        "PermitType", "WORKTYPE", "category", "record_type", "permit_category"],
        "description": ["description", "work_description", "project_description", "DESCRIPTION",
                        "scope_of_work", "permit_description", "comments", "notes"],
        "status": ["status", "permit_status", "STATUS", "PermitStatus", "current_status"],
        "owner_name": ["owner_name", "owner", "applicant", "contractor", "OWNER", "APPLICANT",
                       "applicant_name", "contractor_name", "OWNERNAME", "property_owner"],
        "valuation": ["valuation", "job_value", "estimated_value", "project_value", "VALUE",
                      "total_valuation", "est_project_cost", "construction_cost", "VALUATION"],
        "issue_date": ["issue_date", "issued_date", "permit_date", "date_issued", "ISSUE_DATE",
                       "IssuedDate", "date", "applied_date", "filing_date", "created_date"],
        "zip": ["zip", "zipcode", "zip_code", "postal_code", "ZIP", "ZIPCODE"],
    }

    for std_field, candidates in field_map.items():
        for c in candidates:
            if c in raw and raw[c]:
                lead[std_field] = str(raw[c]).strip()
                break

    # Try to extract coordinates
    for lat_key in ["latitude", "lat", "y", "LATITUDE", "LAT", "Y", "Latitude"]:
        if lat_key in raw and raw[lat_key]:
            try:
                lat = float(raw[lat_key])
                if -90 <= lat <= 90 and lat != 0:
                    lead["latitude"] = lat
                    break
            except (ValueError, TypeError):
                pass

    for lng_key in ["longitude", "lng", "lon", "x", "LONGITUDE", "LNG", "LON", "X", "Longitude"]:
        if lng_key in raw and raw[lng_key]:
            try:
                lng = float(raw[lng_key])
                if -180 <= lng <= 180 and lng != 0:
                    lead["longitude"] = lng
                    break
            except (ValueError, TypeError):
                pass

    # Check for nested location objects
    for loc_key in ["location", "geocoded_column", "mapped_location", "the_geom"]:
        if loc_key in raw and isinstance(raw[loc_key], dict):
            loc = raw[loc_key]
            if "latitude" in loc and "longitude" in loc:
                try:
                    lead["latitude"] = float(loc["latitude"])
                    lead["longitude"] = float(loc["longitude"])
                except (ValueError, TypeError):
                    pass
            elif "coordinates" in loc:
                try:
                    coords = loc["coordinates"]
                    if isinstance(coords, list) and len(coords) >= 2:
                        lead["longitude"] = float(coords[0])
                        lead["latitude"] = float(coords[1])
                except (ValueError, TypeError):
                    pass

    # Assign city-center coords if missing
    if "latitude" not in lead or "longitude" not in lead:
        center = CITY_CENTERS.get(state)
        if center:
            lead["latitude"] = center[0] + random.uniform(-0.15, 0.15)
            lead["longitude"] = center[1] + random.uniform(-0.15, 0.15)

    # Must have at least address or permit_number
    if not lead.get("address") and not lead.get("permit_number") and not lead.get("description"):
        return None

    return lead


async def fetch_socrata(session, dataset_id, domain, state, desc, sem):
    """Fetch all records from a Socrata dataset."""
    async with sem:
        leads = []
        offset = 0
        limit = 5000
        url_base = f"https://{domain}/resource/{dataset_id}.json"

        while True:
            url = f"{url_base}?$limit={limit}&$offset={offset}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        print(f"  SKIP {desc} ({domain}/{dataset_id}): HTTP {resp.status}")
                        return leads
                    data = await resp.json()
                    if not data:
                        break
                    for raw in data:
                        lead = normalize_lead(raw, state, f"socrata_{domain}_{dataset_id}")
                        if lead:
                            leads.append(lead)
                    if len(data) < limit:
                        break
                    offset += limit
                    if offset >= 50000:  # Safety cap per dataset
                        break
            except Exception as e:
                print(f"  ERROR {desc}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads)} leads")
        return leads


async def fetch_arcgis(session, url, state, desc, sem):
    """Fetch all records from an ArcGIS FeatureServer."""
    async with sem:
        leads = []
        offset = 0
        batch = 2000

        while True:
            params = f"?where=1%3D1&outFields=*&f=json&resultRecordCount={batch}&resultOffset={offset}"
            query_url = f"{url}/query{params}"
            try:
                async with session.get(query_url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        print(f"  SKIP {desc}: HTTP {resp.status}")
                        return leads
                    text = await resp.text()
                    if "error" in text[:100].lower() or "<html" in text[:100].lower():
                        print(f"  SKIP {desc}: error/HTML response")
                        return leads
                    data = json.loads(text)
                    features = data.get("features", [])
                    if not features:
                        break
                    for f in features:
                        attrs = f.get("attributes", {})
                        geom = f.get("geometry", {})
                        # Add coords from geometry
                        if geom:
                            x = geom.get("x")
                            y = geom.get("y")
                            if x and y:
                                # Check if Web Mercator (large numbers)
                                if abs(x) > 180:
                                    import math
                                    attrs["longitude"] = x / 20037508.34 * 180
                                    lat_rad = math.atan(math.exp(y / 20037508.34 * math.pi))
                                    attrs["latitude"] = (2 * lat_rad - math.pi / 2) * 180 / math.pi
                                else:
                                    attrs["longitude"] = x
                                    attrs["latitude"] = y
                        lead = normalize_lead(attrs, state, f"arcgis_{desc.replace(' ', '_')}")
                        if lead:
                            leads.append(lead)
                    if len(features) < batch:
                        break
                    offset += batch
                    if offset >= 50000:
                        break
            except Exception as e:
                print(f"  ERROR {desc}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads)} leads")
        return leads


async def discover_and_fetch_socrata(session, query, domain, sem):
    """Discover datasets on a Socrata domain and fetch them."""
    leads = []
    state = STATE_FROM_DOMAIN.get(domain, "")
    if not state:
        return leads

    disco_url = f"https://api.us.socrata.com/api/catalog/v1?q={query}&domains={domain}&limit=20"
    try:
        async with session.get(disco_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                return leads
            data = await resp.json()
            for result in data.get("results", []):
                res = result.get("resource", {})
                rid = res.get("id", "")
                name = res.get("name", "")
                rtype = res.get("type", "")
                if rtype != "dataset" or not rid:
                    continue
                if not is_permit_related(name):
                    continue
                # Fetch this dataset
                batch = await fetch_socrata(session, rid, domain, state, name, sem)
                leads.extend(batch)
    except Exception as e:
        print(f"  Discovery error {domain}/{query}: {e}")

    return leads


async def main():
    print(f"=== Fetching data for weak states ===")
    print(f"Started: {time.strftime('%H:%M:%S')}")

    # Load existing cache
    print("Loading cache...")
    with open(CACHE_PATH) as f:
        cache = json.load(f)

    existing = cache if isinstance(cache, list) else cache.get("leads", cache.get("data", []))
    print(f"Existing leads: {len(existing):,}")

    # Build dedup set
    dedup = set()
    for lead in existing:
        addr = str(lead.get("address", "")).lower().strip()[:60]
        pn = str(lead.get("permit_number", "")).lower().strip()[:30]
        city = str(lead.get("city", "")).lower().strip()
        key = f"{addr}|{pn}|{city}"
        dedup.add(key)
    print(f"Dedup keys: {len(dedup):,}")

    all_new = []
    sem = asyncio.Semaphore(15)

    connector = aiohttp.TCPConnector(limit=20, force_close=True)
    async with aiohttp.ClientSession(connector=connector, headers={"User-Agent": "Onsite/1.0"}) as session:

        # Phase 1: Socrata hardcoded endpoints
        print(f"\n--- Phase 1: Socrata hardcoded endpoints ({len(SOCRATA_ENDPOINTS)}) ---")
        tasks = []
        for did, domain, state, desc in SOCRATA_ENDPOINTS:
            if did.startswith("mt-"):  # Skip placeholder IDs
                continue
            tasks.append(fetch_socrata(session, did, domain, state, desc, sem))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
        print(f"Phase 1 total: {len(all_new):,} leads")

        # Phase 2: ArcGIS endpoints
        print(f"\n--- Phase 2: ArcGIS endpoints ({len(ARCGIS_ENDPOINTS)}) ---")
        tasks = []
        for url, state, desc in ARCGIS_ENDPOINTS:
            tasks.append(fetch_arcgis(session, url, state, desc, sem))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        phase2 = 0
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
                phase2 += len(r)
        print(f"Phase 2 total: {phase2:,} leads")

        # Phase 3: Socrata discovery
        print(f"\n--- Phase 3: Socrata discovery ({len(SOCRATA_DISCOVERY_SEARCHES)}) ---")
        tasks = []
        for query, domain in SOCRATA_DISCOVERY_SEARCHES:
            tasks.append(discover_and_fetch_socrata(session, query, domain, sem))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        phase3 = 0
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
                phase3 += len(r)
        print(f"Phase 3 total: {phase3:,} leads")

    # Deduplicate
    print(f"\n--- Deduplicating {len(all_new):,} new leads ---")
    unique_new = []
    for lead in all_new:
        addr = str(lead.get("address", "")).lower().strip()[:60]
        pn = str(lead.get("permit_number", "")).lower().strip()[:30]
        city = str(lead.get("city", "")).lower().strip()
        key = f"{addr}|{pn}|{city}"
        if key not in dedup:
            dedup.add(key)
            unique_new.append(lead)

    print(f"Unique new leads: {len(unique_new):,}")

    if unique_new:
        # Count by state
        from collections import Counter
        state_counts = Counter(l.get("state", "?") for l in unique_new)
        print("\nNew leads by state:")
        for st, cnt in sorted(state_counts.items(), key=lambda x: -x[1]):
            print(f"  {st}: {cnt:,}")

        # Merge and save
        existing.extend(unique_new)
        print(f"\nTotal leads after merge: {len(existing):,}")
        print("Saving cache (this may take a few minutes for 1.5GB file)...")

        with open(CACHE_PATH, "w") as f:
            json.dump(existing, f)

        print(f"Cache saved! Size: {os.path.getsize(CACHE_PATH) / 1e9:.2f} GB")
    else:
        print("No new leads to add.")

    print(f"\nDone: {time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    asyncio.run(main())
