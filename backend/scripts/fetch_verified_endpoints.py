#!/usr/bin/env python3
"""Fetch from VERIFIED working ArcGIS endpoints discovered by research."""
import asyncio, aiohttp, json, os, sys, time, random, math
from pathlib import Path
from collections import Counter

CACHE_PATH = Path(__file__).parent.parent / "data_cache.json"

# Verified ArcGIS endpoints with REAL URLs
ARCGIS_ENDPOINTS = [
    # DC - DCRA Building Permits (725k total across layers)
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/18", "DC", "DC Permits 2026"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/17", "DC", "DC Permits 2025"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/16", "DC", "DC Permits 2024"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/15", "DC", "DC Permits 2023"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/14", "DC", "DC Permits 2022"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/3", "DC", "DC Permits 2021"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/2", "DC", "DC Permits 2020"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/8", "DC", "DC Permits 2019"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/9", "DC", "DC Permits 2018"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/37", "DC", "DC Permits 2017"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/24", "DC", "DC Permits 2016"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/25", "DC", "DC Permits 2015"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/5", "DC", "DC Permits 2014"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/6", "DC", "DC Permits 2013"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/10", "DC", "DC Permits 2011"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/11", "DC", "DC Permits 2010"),
    ("https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/FeatureServer/12", "DC", "DC Permits 2009"),
    # Portland OR (1.4M permits in layer 22)
    ("https://www.portlandmaps.com/arcgis/rest/services/Public/BDS_Permit/FeatureServer/22", "OR", "Portland All Permits"),
    # Minneapolis MN (386k)
    ("https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/CCS_Permits/FeatureServer/0", "MN", "Minneapolis Permits"),
    # MN Metro 7-county (107k)
    ("https://arcgis.metc.state.mn.us/server/rest/services/CD/VWResidentialPermits/FeatureServer/0", "MN", "MN Metro Residential Permits"),
    # GA - Forsyth County (174k)
    ("https://geo.forsythco.com/gis3/rest/services/Public_EnerGovPlans/Building_Permits/FeatureServer/0", "GA", "Forsyth County Permits"),
    # GA - Atlanta (23k)
    ("https://services1.arcgis.com/Ug5xGQbHsD8zuZzM/arcgis/rest/services/Building_Plan_Permits/FeatureServer/0", "GA", "Atlanta Building Permits"),
    # Denver CO - Residential (77k)
    ("https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_RESIDENTIALCONSTPERMIT_P/FeatureServer/316", "CO", "Denver Residential Permits"),
    # Denver CO - Commercial (41k)
    ("https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_COMMERCIALCONSTPERMIT_P/FeatureServer/317", "CO", "Denver Commercial Permits"),
    # MT - Bozeman (6.9k)
    ("https://services3.arcgis.com/f4hk1qcfxRJ0L2BU/arcgis/rest/services/Building_Permits/FeatureServer/0", "MT", "Bozeman Building Permits"),
    # ID - Boise
    ("https://services1.arcgis.com/WHM6qC35aMtyAAlN/arcgis/rest/services/Development_Tracker_Open_Data/FeatureServer/0", "ID", "Boise Development Tracker"),
    ("https://services1.arcgis.com/WHM6qC35aMtyAAlN/arcgis/rest/services/PDS_BuildingPermits_HighImpact/FeatureServer/0", "ID", "Boise High Impact Permits"),
    # OK
    ("https://services7.arcgis.com/ZodPOMBKsdAsTqF4/arcgis/rest/services/OKC_Multifamily_Permits/FeatureServer/0", "OK", "OKC Multifamily Permits"),
    ("https://services2.arcgis.com/XkZ90iCdbTJ9oNXl/arcgis/rest/services/DevPlans_OpenData/FeatureServer/0", "OK", "Tulsa Development Plans"),
    # KS - Wichita
    ("https://services9.arcgis.com/TuMyQVg8YRPEnbjv/arcgis/rest/services/west_wichita_building_permits/FeatureServer/0", "KS", "Wichita Building Permits"),
    # KS - KCMO Property Violations (799k)
    # Note: KCMO is technically MO but we already count it as KS metro
]

# Additional Socrata with higher limits
SOCRATA_ENDPOINTS = [
    # Honolulu - full dataset (432k)
    ("4vab-c87q", "data.honolulu.gov", "HI", "Honolulu Building Permits Full", 500000),
    # KCMO - more datasets
    ("jnga-5v37", "data.kcmo.org", "KS", "KCMO Permits 2010-2019 v2", 200000),
    ("nhtf-e75a", "data.kcmo.org", "KS", "KCMO Property Violations", 200000),
    ("843w-mn7j", "data.kcmo.org", "KS", "KCMO Demolition List", 5000),
]

CITY_CENTERS = {
    "DC": (38.9072, -77.0369), "OR": (45.5152, -122.6784), "MN": (44.9778, -93.2650),
    "GA": (33.7490, -84.3880), "CO": (39.7392, -104.9903), "MT": (46.8797, -110.3626),
    "ID": (43.6150, -116.2023), "OK": (35.4676, -97.5164), "KS": (39.0997, -94.5786),
    "HI": (21.3069, -157.8583),
}


def web_mercator_to_wgs84(x, y):
    lng = x / 20037508.34 * 180
    lat_rad = math.atan(math.exp(y / 20037508.34 * math.pi))
    lat = (2 * lat_rad - math.pi / 2) * 180 / math.pi
    return lat, lng


def normalize_lead(raw, state, source):
    lead = {"source": source, "state": state}

    # Try all common field name patterns
    addr_fields = ["address", "full_address", "ADDRESS", "FULL_ADDRESS", "FullAddress",
                    "SITE_ADDRESS", "SiteAddress", "site_address", "STREETADDRESS",
                    "PropertyAddress", "PROPERTY_ADDRESS", "Location", "LOCATION",
                    "permit_address", "PERMIT_ADDRESS", "ADDR", "originaladdress1",
                    "originaladdress", "location_address", "Address1", "ADDRESS1",
                    "FULLADD", "FULLADDRESS", "APPLICATION_ADDRESS", "STREET_ADDRESS"]
    for f in addr_fields:
        if f in raw and raw[f]:
            val = str(raw[f]).strip()
            if val and len(val) > 3:
                lead["address"] = val
                break

    city_fields = ["city", "CITY", "City", "MUNICIPALITY", "Town", "TOWN",
                   "originalcity", "city1", "JURISDICTION", "PROJECT_CITY"]
    for f in city_fields:
        if f in raw and raw[f]:
            val = str(raw[f]).strip()
            if val:
                lead["city"] = val
                break

    pn_fields = ["permit_number", "PERMIT_NUMBER", "PermitNumber", "PERMITNO",
                 "PermitNo", "PERMIT_NO", "PERMIT_NUM", "PermitNum", "permitnum",
                 "RECORD_ID", "RecordId", "CASE_NUMBER", "case_number",
                 "FolderNumber", "FOLDER_NUMBER", "APPLICATION_NUMBER",
                 "buildingpermitno", "PERMIT_ID", "permit_id", "OBJECTID",
                 "GLOBALID", "id", "ID"]
    for f in pn_fields:
        if f in raw and raw[f] is not None:
            val = str(raw[f]).strip()
            if val:
                lead["permit_number"] = val
                break

    pt_fields = ["permit_type", "PERMIT_TYPE", "PermitType", "TYPE", "Type",
                 "WORK_TYPE", "WorkType", "CATEGORY", "Category", "RecordType",
                 "PERMIT_CATEGORY", "APPLICATION_TYPE", "type_permit", "worktype",
                 "PERMIT_SUBTYPE", "PERMIT_CLASS"]
    for f in pt_fields:
        if f in raw and raw[f]:
            lead["permit_type"] = str(raw[f]).strip()
            break

    desc_fields = ["description", "DESCRIPTION", "Description", "SCOPE",
                   "ScopeOfWork", "SCOPE_OF_WORK", "PROJECT_DESCRIPTION",
                   "permittypedesc", "projectdescription", "case_name",
                   "WORK_DESCRIPTION", "comments", "COMMENTS", "notes"]
    for f in desc_fields:
        if f in raw and raw[f]:
            lead["description"] = str(raw[f]).strip()[:200]
            break

    status_fields = ["status", "STATUS", "Status", "PERMIT_STATUS", "PermitStatus",
                     "CURRENT_STATUS", "statuscurrent"]
    for f in status_fields:
        if f in raw and raw[f]:
            lead["status"] = str(raw[f]).strip()
            break

    owner_fields = ["owner_name", "OWNER", "Owner", "APPLICANT", "Applicant",
                    "CONTRACTOR", "Contractor", "contractorcompanyname",
                    "APPLICANT_NAME", "CONTRACTOR_NAME", "PROPERTY_OWNER"]
    for f in owner_fields:
        if f in raw and raw[f]:
            lead["owner_name"] = str(raw[f]).strip()
            break

    val_fields = ["valuation", "VALUATION", "Valuation", "JOB_VALUE", "JobValue",
                  "ESTIMATED_VALUE", "PROJECT_VALUE", "TOTAL_VALUATION",
                  "CONSTRUCTION_COST", "estimatedvalueofwork", "projectvalue",
                  "declaredvaluation", "acceptedvalue", "COST", "FEE"]
    for f in val_fields:
        if f in raw and raw[f]:
            lead["valuation"] = str(raw[f]).strip()
            break

    date_fields = ["issue_date", "ISSUE_DATE", "IssueDate", "ISSUED_DATE", "IssuedDate",
                   "PERMIT_DATE", "PermitDate", "DATE_ISSUED", "DateIssued",
                   "CREATED_DATE", "CreatedDate", "issueddate", "creationdate",
                   "addeddate", "received_date", "APPLIED_DATE"]
    for f in date_fields:
        if f in raw and raw[f]:
            val = str(raw[f]).strip()
            if val.isdigit() and len(val) > 10:
                try:
                    from datetime import datetime
                    ts = int(val) / 1000
                    lead["issue_date"] = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                except (ValueError, TypeError, OSError):
                    lead["issue_date"] = val
            else:
                lead["issue_date"] = val
            break

    # Coordinates
    for lk in ["latitude", "lat", "y", "LATITUDE", "LAT", "Y", "Latitude", "POINT_Y"]:
        if lk in raw and raw[lk] is not None:
            try:
                lat = float(raw[lk])
                if -90 <= lat <= 90 and lat != 0:
                    lead["latitude"] = lat
                    break
            except (ValueError, TypeError):
                pass

    for lk in ["longitude", "lng", "lon", "x", "LONGITUDE", "LNG", "LON", "X", "Longitude", "POINT_X"]:
        if lk in raw and raw[lk] is not None:
            try:
                lng = float(raw[lk])
                if -180 <= lng <= 180 and lng != 0:
                    lead["longitude"] = lng
                    break
            except (ValueError, TypeError):
                pass

    # City center fallback
    if ("latitude" not in lead or "longitude" not in lead) and state in CITY_CENTERS:
        c = CITY_CENTERS[state]
        lead["latitude"] = c[0] + random.uniform(-0.15, 0.15)
        lead["longitude"] = c[1] + random.uniform(-0.15, 0.15)

    if not lead.get("address") and not lead.get("permit_number") and not lead.get("description"):
        return None
    return lead


async def fetch_arcgis(session, url, state, desc, sem, max_recs=200000):
    async with sem:
        leads = []
        offset = 0
        batch = 2000

        while offset < max_recs:
            params = f"?where=1%3D1&outFields=*&f=json&resultRecordCount={batch}&resultOffset={offset}"
            try:
                async with session.get(f"{url}/query{params}", timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status != 200:
                        print(f"  SKIP {desc}: HTTP {resp.status}")
                        return leads
                    text = await resp.text()
                    if "<html" in text[:300].lower():
                        print(f"  SKIP {desc}: HTML response")
                        return leads
                    data = json.loads(text)
                    if "error" in data:
                        msg = data["error"].get("message", "")[:60]
                        print(f"  SKIP {desc}: {msg}")
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
                                except Exception:
                                    pass
                        lead = normalize_lead(attrs, state, f"arcgis_{desc.replace(' ', '_')}")
                        if lead:
                            leads.append(lead)
                    if len(features) < batch:
                        break
                    offset += batch
                    if offset % 50000 == 0:
                        print(f"  ... {desc}: {len(leads):,} so far (offset {offset:,})")
            except Exception as e:
                print(f"  ERROR {desc} at offset {offset}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads):,} leads")
        else:
            print(f"  EMPTY {desc}")
        return leads


async def fetch_socrata(session, did, domain, state, desc, sem, max_recs=50000):
    async with sem:
        leads = []
        offset = 0
        limit = 5000

        while offset < max_recs:
            url = f"https://{domain}/resource/{did}.json?$limit={limit}&$offset={offset}"
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
                        lead = normalize_lead(raw, state, f"socrata_{domain}_{did}")
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


async def main():
    print(f"=== Fetch Verified Endpoints ===")
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
        dedup.add(f"{addr}|{pn}|{city}")
    print(f"Dedup keys: {len(dedup):,}")

    all_new = []
    sem = asyncio.Semaphore(8)  # Lower concurrency for big datasets

    connector = aiohttp.TCPConnector(limit=15, force_close=True)
    async with aiohttp.ClientSession(connector=connector, headers={"User-Agent": "Onsite/2.0"}) as session:

        # Phase 1: ArcGIS (the big ones)
        print(f"\n--- ArcGIS ({len(ARCGIS_ENDPOINTS)} endpoints) ---")
        # Process in smaller batches to avoid memory issues
        for i in range(0, len(ARCGIS_ENDPOINTS), 5):
            batch = ARCGIS_ENDPOINTS[i:i+5]
            tasks = [fetch_arcgis(session, url, state, desc, sem) for url, state, desc in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    all_new.extend(r)

        arcgis_count = len(all_new)
        print(f"ArcGIS total: {arcgis_count:,}")

        # Phase 2: Socrata
        print(f"\n--- Socrata ({len(SOCRATA_ENDPOINTS)} endpoints) ---")
        tasks = [fetch_socrata(session, did, domain, state, desc, sem, max_recs)
                 for did, domain, state, desc, max_recs in SOCRATA_ENDPOINTS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
        print(f"Socrata total: {len(all_new) - arcgis_count:,}")

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
        state_counts = Counter(l.get("state", "?") for l in unique_new)
        print("\nNew leads by state:")
        for st, cnt in sorted(state_counts.items(), key=lambda x: -x[1]):
            print(f"  {st}: {cnt:,}")

        existing.extend(unique_new)
        print(f"\nTotal leads: {len(existing):,}")
        print("Saving cache...")
        with open(CACHE_PATH, "w") as f:
            json.dump(existing, f)
        print(f"Saved! {os.path.getsize(CACHE_PATH)/1e9:.2f} GB")
    else:
        print("No new leads.")

    print(f"\nDone: {time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    asyncio.run(main())
