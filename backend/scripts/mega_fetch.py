#!/usr/bin/env python3
"""
Mega fetch script - rebuilds data from ALL known sources.
Combines FEMA, property sales, verified state endpoints, ArcGIS, and Socrata discovery.
"""
import asyncio, aiohttp, json, os, sys, time, random, math
from pathlib import Path
from collections import Counter

CACHE_PATH = Path(__file__).parent.parent / "data_cache.json"

# ── FEMA endpoints ──
FEMA_ENDPOINTS = [
    ("https://www.fema.gov/api/open/v2/FimaNfipClaims", "FEMA NFIP Claims"),
    ("https://www.fema.gov/api/open/v2/HousingAssistanceOwners", "FEMA Housing Assistance Owners"),
    ("https://www.fema.gov/api/open/v2/HousingAssistanceRenters", "FEMA Housing Assistance Renters"),
    ("https://www.fema.gov/api/open/v2/IndividualAssistanceHousingRegistrantsLargeDisasters", "FEMA IA Housing"),
    ("https://www.fema.gov/api/open/v2/RegistrationIntakeIndividualsHouseholdPrograms", "FEMA Registration Intake"),
    ("https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries", "FEMA Disaster Declarations"),
    ("https://www.fema.gov/api/open/v2/HazardMitigationGrantProgramPropertyAcquisitions", "FEMA Hazard Mitigation"),
]

# ── Verified Socrata endpoints ──
SOCRATA_ENDPOINTS = [
    # Kansas
    ("i2p9-vksn", "data.kcmo.org", "KS", "KC Building Permits 2010-2019"),
    ("ntw8-aacc", "data.kcmo.org", "KS", "KC CPD Permits"),
    ("6h9j-mu65", "data.kcmo.org", "KS", "KC CPD Permits Status Change"),
    ("cvnj-fvgf", "data.kcmo.org", "KS", "KC Building Permits 2000-2009"),
    ("4hr8-fgbf", "data.kcmo.org", "KS", "KC Building Permits 1990-1999"),
    ("hqe6-tuji", "data.kcmo.org", "KS", "KC Building Permits 1980-1989"),
    ("akhc-zaz6", "data.kcmo.org", "KS", "KC Building Permits 1970-1979"),
    ("qt6m-65bz", "data.kcmo.org", "KS", "KC 2014 Building Permits"),
    ("cwrz-29jm", "data.kcmo.org", "KS", "KC 2020 Building Permits"),
    ("vcj3-ncb3", "data.kcmo.org", "KS", "KC 2010-2013 Building Permits"),
    # Connecticut
    ("pqrn-qghw", "data.ct.gov", "CT", "CT Parcel CAMA 2024"),
    ("rny9-6ak2", "data.ct.gov", "CT", "CT Parcel CAMA 2025"),
    ("5vjm-esav", "data.ct.gov", "CT", "CT Monthly Building Permits"),
    ("5mzw-sjtu", "data.ct.gov", "CT", "CT Real Estate Sales"),
    # Washington - Seattle
    ("76t5-zqzr", "cos-data.seattle.gov", "WA", "Seattle Building Permits"),
    ("c4tj-daue", "cos-data.seattle.gov", "WA", "Seattle Electrical Permits"),
    ("c87v-5hwh", "cos-data.seattle.gov", "WA", "Seattle Trade Permits"),
    ("ht3q-kdvx", "cos-data.seattle.gov", "WA", "Seattle Land Use Permits"),
    # Washington State
    ("uxv9-c2g9", "data.wa.gov", "WA", "Clarkston Building Permits"),
    # Louisiana
    ("nbcf-m6c2", "data.nola.gov", "LA", "NOLA Building Permits 2018+"),
    ("f7tt-z5vu", "data.nola.gov", "LA", "NOLA Historical Permits"),
    ("72f9-bi28", "data.nola.gov", "LA", "NOLA BLDS Permits"),
    ("rcm3-fn58", "data.nola.gov", "LA", "NOLA Permits"),
    ("7fq7-8j7r", "data.brla.gov", "LA", "Baton Rouge Building Permits"),
    # Hawaii
    ("4vab-c87q", "data.honolulu.gov", "HI", "Honolulu Building Permits"),
    # Maryland
    ("weik-ttee", "data.princegeorgescountymd.gov", "MD", "PG County Permits"),
    ("m88u-pqki", "data.montgomerycountymd.gov", "MD", "Montgomery Residential Permits"),
    ("b6ht-fw3x", "data.montgomerycountymd.gov", "MD", "Montgomery Demo Permits"),
    # Illinois
    ("ydr8-5enu", "data.cityofchicago.org", "IL", "Chicago Building Permits"),
    ("ir7v-8mc8", "data.cityofchicago.org", "IL", "Chicago Environmental Permits"),
    ("csik-bsws", "datacatalog.cookcountyil.gov", "IL", "Cook County Commercial Valuation"),
    # California
    ("mkbn-caye", "data.marincounty.gov", "CA", "Marin Building Permits"),
    # Rhode Island
    ("ufmm-rbej", "data.providenceri.gov", "RI", "Providence Permits"),
    # Massachusetts
    ("vxgw-vmky", "data.somervillema.gov", "MA", "Somerville Permits"),
    ("kcfi-ackv", "data.cambridgema.gov", "MA", "Cambridge Demo Permits"),
    # Indiana
    ("9q6j-a8rc", "data.bloomington.in.gov", "IN", "Bloomington Rental Permits"),
    # Iowa
    ("i2ax-pybm", "mydata.iowa.gov", "IA", "Iowa Elevator Permits"),
    # New Jersey
    ("w9se-dmra", "data.nj.gov", "NJ", "NJ Construction Permit Data"),
    # New York
    ("ipu4-2q9a", "data.cityofnewyork.us", "NY", "NYC DOB Permit Issuance"),
    ("rbx6-tga4", "data.cityofnewyork.us", "NY", "NYC DOB NOW Approved Permits"),
    # Colorado
    ("v4as-sthd", "data.colorado.gov", "CO", "CO Building Permit Counts"),
    # Arizona
    ("2gkz-7z4f", "citydata.mesaaz.gov", "AZ", "Mesa Building Permits"),
]

# ── Verified ArcGIS endpoints ──
ARCGIS_ENDPOINTS = [
    ("https://services1.arcgis.com/PybKzo48QGbyiuCh/arcgis/rest/services/Denver_Building_Permits/FeatureServer/0", "CO", "Denver Building Permits"),
    ("https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_building_permits/FeatureServer/0", "MI", "Detroit Building Permits"),
    ("https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_trades_permits/FeatureServer/0", "MI", "Detroit Trades Permits"),
    ("https://services3.arcgis.com/SCwJH1pD8WSn5T5y/arcgis/rest/services/accela_permit_data/FeatureServer/0", "WA", "Tacoma Permits"),
]

# ── Socrata global discovery searches ──
DISCOVERY_SEARCHES = [
    "building+permits", "construction+permits", "property+sales",
    "code+enforcement", "building+inspections", "demolition+permits",
    "residential+permits", "commercial+permits", "zoning+permits",
    "property+transfers", "real+estate+sales", "building+violations",
]

STATE_FROM_DOMAIN = {
    "data.kcmo.org": "KS", "data.ct.gov": "CT", "cos-data.seattle.gov": "WA",
    "data.wa.gov": "WA", "data.nola.gov": "LA", "data.brla.gov": "LA",
    "data.honolulu.gov": "HI", "data.princegeorgescountymd.gov": "MD",
    "data.montgomerycountymd.gov": "MD", "data.cityofchicago.org": "IL",
    "datacatalog.cookcountyil.gov": "IL", "data.marincounty.gov": "CA",
    "data.providenceri.gov": "RI", "data.somervillema.gov": "MA",
    "data.cambridgema.gov": "MA", "data.bloomington.in.gov": "IN",
    "mydata.iowa.gov": "IA", "data.nj.gov": "NJ",
    "data.cityofnewyork.us": "NY", "data.colorado.gov": "CO",
    "citydata.mesaaz.gov": "AZ", "data.sfgov.org": "CA",
    "data.lacity.org": "CA", "datahub.austintexas.gov": "TX",
    "data.detroitmi.gov": "MI", "data.michigan.gov": "MI",
    "opendata.dc.gov": "DC", "data.nashville.gov": "TN",
    "data.raleighnc.gov": "NC", "data.wprdc.org": "PA",
    "data.chattanooga.gov": "TN", "opendata.minneapolismn.gov": "MN",
    "data.ramseycountymn.gov": "MN", "data.okc.gov": "OK",
    "sharefulton.fultoncountyga.gov": "GA", "data.muni.org": "AK",
    "data.oregon.gov": "OR", "data.portland.gov": "OR",
    "data.denvergov.org": "CO", "data.virginia.gov": "VA",
    "data.fortworthtexas.gov": "TX", "data.ny.gov": "NY",
    "data.pa.gov": "PA", "data.texas.gov": "TX",
    "opendata.maryland.gov": "MD", "data.baltimorecity.gov": "MD",
    "data.cityofboise.org": "ID", "data.montana.gov": "MT",
}

CITY_CENTERS = {
    "AL": (33.5207, -86.8025), "AK": (61.2181, -149.9003), "AZ": (33.4484, -112.0740),
    "AR": (34.7465, -92.2896), "CA": (34.0522, -118.2437), "CO": (39.7392, -104.9903),
    "CT": (41.7658, -72.6734), "DE": (39.7391, -75.5398), "FL": (25.7617, -80.1918),
    "GA": (33.7490, -84.3880), "HI": (21.3069, -157.8583), "ID": (43.6150, -116.2023),
    "IL": (41.8781, -87.6298), "IN": (39.7684, -86.1581), "IA": (41.5868, -93.6250),
    "KS": (39.0997, -94.5786), "KY": (38.2527, -85.7585), "LA": (29.9511, -90.0715),
    "ME": (43.6591, -70.2568), "MD": (39.2904, -76.6122), "MA": (42.3601, -71.0589),
    "MI": (42.3314, -83.0458), "MN": (44.9778, -93.2650), "MS": (32.2988, -90.1848),
    "MO": (38.6270, -90.1994), "MT": (46.8797, -110.3626), "NE": (41.2565, -95.9345),
    "NV": (36.1699, -115.1398), "NH": (43.2081, -71.5376), "NJ": (40.0583, -74.4057),
    "NM": (35.0844, -106.6504), "NY": (40.7128, -74.0060), "NC": (35.7796, -78.6382),
    "ND": (46.8772, -96.7898), "OH": (39.9612, -82.9988), "OK": (35.4676, -97.5164),
    "OR": (45.5152, -122.6784), "PA": (39.9526, -75.1652), "RI": (41.8240, -71.4128),
    "SC": (32.7765, -79.9311), "SD": (43.5460, -96.7313), "TN": (36.1627, -86.7816),
    "TX": (29.7604, -95.3698), "UT": (40.7608, -111.8910), "VT": (44.4759, -73.2121),
    "VA": (37.5407, -77.4360), "WA": (47.6062, -122.3321), "WV": (38.3498, -81.6326),
    "WI": (43.0389, -87.9065), "WY": (41.1400, -104.8197), "DC": (38.9072, -77.0369),
}

US_STATES = set(CITY_CENTERS.keys()) | {"PR", "VI", "GU", "AS", "MP"}

JUNK_KEYWORDS = ['311', 'tobacco', 'pothole', 'pot hole', 'parking meter', 'bicycle',
                  'dog license', 'animal control', 'tree removal', 'noise complaint',
                  'graffiti', 'litter', 'trash', 'bus stop', 'traffic signal',
                  'streetlight', 'fire hydrant', 'food truck', 'special event',
                  'sidewalk cafe', 'block party', 'film permit', 'entertainment',
                  'festival', 'parade', 'food establishment', 'restaurant inspection',
                  'energy benchmark', 'micro-market recovery']


def is_permit_related(name):
    name_lower = (name or "").lower()
    if any(b in name_lower for b in JUNK_KEYWORDS):
        return False
    good = ['permit', 'building', 'construction', 'property', 'parcel', 'code enforcement',
            'inspection', 'demolition', 'zoning', 'land use', 'housing', 'real estate',
            'assessment', 'valuation', 'sale', 'transfer', 'deed', 'violation']
    return any(g in name_lower for g in good)


def normalize_lead(raw, state, source_name):
    lead = {"source": source_name, "state": state}

    field_map = {
        "address": ["address", "full_address", "location", "site_address", "property_address",
                     "streetaddress", "street_address", "originaladdress1", "originaladdress",
                     "permit_address", "project_address", "ADDRESS", "FULL_ADDRESS",
                     "FullAddress", "SITEADDRESS", "STREETADDRESS", "PropertyAddress",
                     "PROPERTY_ADDRESS", "Location", "LOCATION", "damaged_address",
                     "damagestreetaddress", "propertyaddress"],
        "city": ["city", "CITY", "City", "municipality", "town", "jurisdiction",
                 "originalcity", "city1", "city_town", "damagedcity",
                 "damaged_city", "MUNICIPALITY"],
        "state": ["state_override"],  # Only use if explicitly set
        "permit_number": ["permit_number", "permitnumber", "permit_no", "permitnum",
                          "PERMIT_NO", "PERMITNUMBER", "permit_id", "PERMIT_NUMBER",
                          "record_id", "permitno", "buildingpermitno", "permit_case_id",
                          "permit_tracking_id", "folderNumber", "case_number",
                          "applicationnumber", "id"],
        "permit_type": ["permit_type", "permittype", "type", "work_type", "PERMIT_TYPE",
                        "permitclassmapped", "permittypemapped", "permit_category",
                        "applicationtype", "type_permit", "worktype", "incidenttype"],
        "description": ["description", "work_description", "project_description",
                        "DESCRIPTION", "scope_of_work", "permit_description",
                        "permittypedesc", "projectdescription", "case_name",
                        "comments", "notes"],
        "status": ["status", "permit_status", "STATUS", "statuscurrent"],
        "owner_name": ["owner_name", "owner", "applicant", "contractor",
                       "contractorcompanyname", "applicant_name", "contractor_name",
                       "property_owner", "dba", "APPLICANT", "OWNER", "CONTRACTOR"],
        "valuation": ["valuation", "job_value", "estimated_value", "project_value",
                      "total_valuation", "construction_cost", "estimatedvalueofwork",
                      "expected_construction_cost", "construction_value",
                      "projectvalue", "declaredvaluation", "acceptedvalue",
                      "amountpaidonbuildingclaim", "amountpaidoncontentsclaim"],
        "issue_date": ["issue_date", "issued_date", "permit_date", "date_issued",
                       "issueddate", "date", "applied_date", "filing_date",
                       "created_date", "permit_issuance_date", "addeddate",
                       "received_date", "creationdate", "dateofloss",
                       "declarationdate"],
        "zip": ["zip", "zipcode", "zip_code", "postal_code", "ZIP", "ZIPCODE",
                "originalzip", "reportedzipcode", "damaged_zip_code"],
    }

    for std_field, candidates in field_map.items():
        for c in candidates:
            if c in raw and raw[c] is not None:
                val = str(raw[c]).strip()
                if val and val.lower() not in ('none', 'null', 'nan', '', '0'):
                    lead[std_field] = val
                    break

    # Handle stno+stname combo
    if "address" not in lead:
        stno = str(raw.get("stno", "")).strip()
        stname = str(raw.get("stname", "")).strip()
        suffix = str(raw.get("suffix", "")).strip()
        if stno and stname:
            lead["address"] = f"{stno} {stname} {suffix}".strip()

    # FEMA state extraction
    if not lead.get("state") or lead["state"] == state:
        for sk in ["state", "damagedstatecode", "damaged_state_abbreviation"]:
            if sk in raw and raw[sk]:
                s = str(raw[sk]).strip().upper()
                if len(s) == 2 and s in US_STATES:
                    lead["state"] = s
                    break

    # Extract coordinates
    for lat_key in ["latitude", "lat", "y", "LATITUDE", "LAT", "Y", "Latitude"]:
        if lat_key in raw and raw[lat_key] is not None:
            try:
                lat = float(raw[lat_key])
                if -90 <= lat <= 90 and lat != 0:
                    lead["latitude"] = lat
                    break
            except (ValueError, TypeError):
                pass

    for lng_key in ["longitude", "lng", "lon", "x", "LONGITUDE", "LNG", "LON", "X", "Longitude"]:
        if lng_key in raw and raw[lng_key] is not None:
            try:
                lng = float(raw[lng_key])
                if -180 <= lng <= 180 and lng != 0:
                    lead["longitude"] = lng
                    break
            except (ValueError, TypeError):
                pass

    # Nested location
    for loc_key in ["location", "geocoded_column", "mapped_location", "the_geom"]:
        if loc_key in raw and isinstance(raw[loc_key], dict):
            loc = raw[loc_key]
            if "latitude" in loc and "longitude" in loc:
                try:
                    lead.setdefault("latitude", float(loc["latitude"]))
                    lead.setdefault("longitude", float(loc["longitude"]))
                except (ValueError, TypeError):
                    pass
            elif "coordinates" in loc:
                try:
                    coords = loc["coordinates"]
                    if isinstance(coords, list) and len(coords) >= 2:
                        lead.setdefault("longitude", float(coords[0]))
                        lead.setdefault("latitude", float(coords[1]))
                except (ValueError, TypeError):
                    pass

    # Assign city-center coords if missing
    st = lead.get("state", "")
    if ("latitude" not in lead or "longitude" not in lead) and st in CITY_CENTERS:
        center = CITY_CENTERS[st]
        lead["latitude"] = center[0] + random.uniform(-0.2, 0.2)
        lead["longitude"] = center[1] + random.uniform(-0.2, 0.2)

    if not lead.get("address") and not lead.get("permit_number") and not lead.get("description"):
        return None

    return lead


async def fetch_fema(session, url, desc, sem, max_records=100000):
    """Fetch from FEMA with pagination."""
    async with sem:
        leads = []
        skip = 0
        top = 10000

        while skip < max_records:
            full_url = f"{url}?$skip={skip}&$top={top}&$format=json"
            try:
                async with session.get(full_url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status != 200:
                        print(f"  SKIP {desc}: HTTP {resp.status}")
                        return leads
                    data = await resp.json()
                    records = data.get("FimaNfipClaims", data.get("HousingAssistanceOwners",
                              data.get("HousingAssistanceRenters", data.get("IndividualAssistanceHousingRegistrantsLargeDisasters",
                              data.get("RegistrationIntakeIndividualsHouseholdPrograms",
                              data.get("DisasterDeclarationsSummaries",
                              data.get("HazardMitigationGrantProgramPropertyAcquisitions", [])))))))
                    if not records:
                        break
                    for raw in records:
                        lead = normalize_lead(raw, "", f"fema_{desc.replace(' ', '_')}")
                        if lead:
                            leads.append(lead)
                    if len(records) < top:
                        break
                    skip += top
            except Exception as e:
                print(f"  ERROR {desc} at skip={skip}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads):,} leads")
        return leads


async def fetch_socrata(session, dataset_id, domain, state, desc, sem, max_records=50000):
    async with sem:
        leads = []
        offset = 0
        limit = 5000

        while offset < max_records:
            url = f"https://{domain}/resource/{dataset_id}.json?$limit={limit}&$offset={offset}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status != 200:
                        print(f"  SKIP {desc}: HTTP {resp.status}")
                        return leads
                    ct = resp.headers.get("content-type", "")
                    if "json" not in ct:
                        print(f"  SKIP {desc}: not JSON")
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
            except Exception as e:
                print(f"  ERROR {desc}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads):,} leads")
        return leads


def web_mercator_to_wgs84(x, y):
    lng = x / 20037508.34 * 180
    lat_rad = math.atan(math.exp(y / 20037508.34 * math.pi))
    lat = (2 * lat_rad - math.pi / 2) * 180 / math.pi
    return lat, lng


async def fetch_arcgis(session, url, state, desc, sem, max_records=50000):
    async with sem:
        leads = []
        offset = 0
        batch = 2000

        while offset < max_records:
            params = f"?where=1%3D1&outFields=*&f=json&resultRecordCount={batch}&resultOffset={offset}"
            try:
                async with session.get(f"{url}/query{params}", timeout=aiohttp.ClientTimeout(total=90)) as resp:
                    if resp.status != 200:
                        print(f"  SKIP {desc}: HTTP {resp.status}")
                        return leads
                    text = await resp.text()
                    if "<html" in text[:200].lower() or "error" in text[:100].lower():
                        print(f"  SKIP {desc}: error/HTML")
                        return leads
                    data = json.loads(text)
                    if "error" in data:
                        print(f"  SKIP {desc}: {data['error'].get('message', '')[:50]}")
                        return leads
                    features = data.get("features", [])
                    if not features:
                        break
                    for f in features:
                        attrs = f.get("attributes", {})
                        geom = f.get("geometry", {})
                        if geom:
                            x, y = geom.get("x"), geom.get("y")
                            if x is not None and y is not None:
                                try:
                                    x, y = float(x), float(y)
                                    if abs(x) > 180:
                                        lat, lng = web_mercator_to_wgs84(x, y)
                                    else:
                                        lat, lng = y, x
                                    if -90 <= lat <= 90 and -180 <= lng <= 180:
                                        attrs["latitude"] = lat
                                        attrs["longitude"] = lng
                                except:
                                    pass
                        lead = normalize_lead(attrs, state, f"arcgis_{desc.replace(' ', '_')}")
                        if lead:
                            leads.append(lead)
                    if len(features) < batch:
                        break
                    offset += batch
            except Exception as e:
                print(f"  ERROR {desc}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads):,} leads")
        return leads


async def discover_socrata(session, query, sem, fetched):
    """Global Socrata discovery."""
    leads = []
    url = f"https://api.us.socrata.com/api/catalog/v1?q={query}&limit=30"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                return leads
            data = await resp.json()
            for result in data.get("results", []):
                res = result.get("resource", {})
                rid = res.get("id", "")
                name = res.get("name", "")
                rtype = res.get("type", "")
                domain = result.get("metadata", {}).get("domain", "")
                if rtype != "dataset" or not rid:
                    continue
                if not is_permit_related(name):
                    continue
                key = f"{domain}/{rid}"
                if key in fetched:
                    continue
                fetched.add(key)
                state = STATE_FROM_DOMAIN.get(domain, "")
                if not state:
                    continue
                batch = await fetch_socrata(session, rid, domain, state, name, sem)
                leads.extend(batch)
    except:
        pass
    return leads


async def main():
    print(f"=== MEGA FETCH - Rebuilding all data ===")
    print(f"Started: {time.strftime('%H:%M:%S')}")

    print("Loading cache...")
    with open(CACHE_PATH) as f:
        cache = json.load(f)
    existing = cache if isinstance(cache, list) else cache.get("leads", cache.get("data", []))
    print(f"Existing leads: {len(existing):,}")

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
    fetched_datasets = set()

    connector = aiohttp.TCPConnector(limit=25, force_close=True)
    async with aiohttp.ClientSession(connector=connector, headers={"User-Agent": "Onsite/2.0"}) as session:

        # Phase 1: FEMA
        print(f"\n{'='*50}")
        print(f"Phase 1: FEMA ({len(FEMA_ENDPOINTS)} endpoints)")
        print(f"{'='*50}")
        tasks = [fetch_fema(session, url, desc, sem) for url, desc in FEMA_ENDPOINTS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        p1 = 0
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
                p1 += len(r)
        print(f"Phase 1 total: {p1:,}")

        # Phase 2: Socrata
        print(f"\n{'='*50}")
        print(f"Phase 2: Socrata ({len(SOCRATA_ENDPOINTS)} endpoints)")
        print(f"{'='*50}")
        tasks = []
        for did, domain, state, desc in SOCRATA_ENDPOINTS:
            fetched_datasets.add(f"{domain}/{did}")
            tasks.append(fetch_socrata(session, did, domain, state, desc, sem))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        p2 = 0
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
                p2 += len(r)
        print(f"Phase 2 total: {p2:,}")

        # Phase 3: ArcGIS
        print(f"\n{'='*50}")
        print(f"Phase 3: ArcGIS ({len(ARCGIS_ENDPOINTS)} endpoints)")
        print(f"{'='*50}")
        tasks = [fetch_arcgis(session, url, state, desc, sem) for url, state, desc in ARCGIS_ENDPOINTS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        p3 = 0
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
                p3 += len(r)
        print(f"Phase 3 total: {p3:,}")

        # Phase 4: Socrata Discovery
        print(f"\n{'='*50}")
        print(f"Phase 4: Socrata Discovery ({len(DISCOVERY_SEARCHES)} searches)")
        print(f"{'='*50}")
        for q in DISCOVERY_SEARCHES:
            batch = await discover_socrata(session, q, sem, fetched_datasets)
            all_new.extend(batch)
        print(f"Phase 4 done")

    # Deduplicate
    print(f"\n{'='*50}")
    print(f"Deduplicating {len(all_new):,} new leads")
    print(f"{'='*50}")
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
        state_counts = Counter(l.get("state", "?") for l in unique_new)
        print("\nNew leads by state:")
        for st, cnt in sorted(state_counts.items(), key=lambda x: -x[1])[:30]:
            print(f"  {st}: {cnt:,}")

        existing.extend(unique_new)
        print(f"\nTotal leads: {len(existing):,}")
        print("Saving cache...")
        with open(CACHE_PATH, "w") as f:
            json.dump(existing, f)
        print(f"Saved! {os.path.getsize(CACHE_PATH)/1e9:.2f} GB")

    # Final stats
    print(f"\n{'='*50}")
    print("FINAL STATS")
    print(f"{'='*50}")
    all_leads = existing
    state_counts = Counter(l.get("state", "?") for l in all_leads)
    print(f"Total leads: {len(all_leads):,}")
    print(f"\nAll states:")
    all_50 = sorted(CITY_CENTERS.keys())
    for st in all_50:
        cnt = state_counts.get(st, 0)
        marker = " *** LOW ***" if cnt < 5000 else ""
        print(f"  {st}: {cnt:,}{marker}")

    with_coords = sum(1 for l in all_leads if l.get("latitude") and l.get("longitude"))
    with_city = sum(1 for l in all_leads if (l.get("city") or "").strip())
    with_addr = sum(1 for l in all_leads if (l.get("address") or "").strip())
    print(f"\nWith coordinates: {with_coords:,} ({100*with_coords/len(all_leads):.1f}%)")
    print(f"With city: {with_city:,} ({100*with_city/len(all_leads):.1f}%)")
    print(f"With address: {with_addr:,} ({100*with_addr/len(all_leads):.1f}%)")

    print(f"\nDone: {time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    asyncio.run(main())
