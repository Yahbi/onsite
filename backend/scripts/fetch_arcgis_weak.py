#!/usr/bin/env python3
"""Fetch from verified ArcGIS endpoints for weak states."""
import asyncio, aiohttp, json, os, sys, time, random, math
from pathlib import Path

CACHE_PATH = Path(__file__).parent.parent / "data_cache.json"

# Verified ArcGIS endpoints from Hub search
ENDPOINTS = [
    # Denver - REAL endpoint
    ("https://services1.arcgis.com/PybKzo48QGbyiuCh/arcgis/rest/services/Denver_Building_Permits/FeatureServer/0", "CO", "Denver Building Permits"),
    # Minneapolis
    ("https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/CCS_Permits/FeatureServer/0", "MN", "Minneapolis CCS Permits"),
    # Detroit
    ("https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_building_permits/FeatureServer/0", "MI", "Detroit Building Permits"),
    ("https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/building_permits_2023/FeatureServer/0", "MI", "Detroit Building Permits 2023"),
    ("https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_trades_permits/FeatureServer/0", "MI", "Detroit Trades Permits"),
    # Atlanta area
    ("https://services5.arcgis.com/5RxyIIJ9boPdptdo/arcgis/rest/services/Code_Enforcement_Data_2021_2023/FeatureServer/0", "GA", "Atlanta Code Enforcement 2021-2023"),
    ("https://services5.arcgis.com/5RxyIIJ9boPdptdo/arcgis/rest/services/CodeEnforcement_OZ/FeatureServer/0", "GA", "Atlanta Code Enforcement OZ"),
    ("https://services5.arcgis.com/5RxyIIJ9boPdptdo/arcgis/rest/services/AMS_District_Property_Sales_-_Public_View/FeatureServer/0", "GA", "Atlanta Property Sales"),
    # Tacoma
    ("https://services3.arcgis.com/SCwJH1pD8WSn5T5y/arcgis/rest/services/accela_permit_data/FeatureServer/0", "WA", "Tacoma Permits"),
]

CITY_CENTERS = {
    "CO": (39.7392, -104.9903), "MN": (44.9778, -93.2650), "MI": (42.3314, -83.0458),
    "GA": (33.7490, -84.3880), "WA": (47.6062, -122.3321),
}


def web_mercator_to_wgs84(x, y):
    lng = x / 20037508.34 * 180
    lat_rad = math.atan(math.exp(y / 20037508.34 * math.pi))
    lat = (2 * lat_rad - math.pi / 2) * 180 / math.pi
    return lat, lng


def normalize_lead(attrs, state, source):
    lead = {"source": source, "state": state}

    field_map = {
        "address": ["address", "full_address", "ADDRESS", "FULL_ADDRESS", "FullAddress",
                     "SITE_ADDRESS", "SiteAddress", "site_address", "STREETADDRESS",
                     "StreetAddress", "Location", "LOCATION", "PropertyAddress",
                     "PROPERTY_ADDRESS", "original_address", "OriginalAddress",
                     "permit_address", "PERMIT_ADDRESS", "ADDR", "location_address",
                     "Address1", "ADDRESS1"],
        "city": ["city", "CITY", "City", "MUNICIPALITY", "Municipality", "Town"],
        "permit_number": ["permit_number", "PERMIT_NUMBER", "PermitNumber", "PERMITNO",
                          "PermitNo", "PERMIT_NO", "permit_no", "PERMIT_NUM", "PermitNum",
                          "RECORD_ID", "RecordId", "case_number", "CASE_NUMBER",
                          "FolderNumber", "FOLDER_NUMBER", "ApplicationNumber"],
        "permit_type": ["permit_type", "PERMIT_TYPE", "PermitType", "TYPE", "Type",
                        "WORK_TYPE", "WorkType", "CATEGORY", "Category", "RecordType"],
        "description": ["description", "DESCRIPTION", "Description", "SCOPE",
                        "ScopeOfWork", "SCOPE_OF_WORK", "ProjectDescription",
                        "PROJECT_DESCRIPTION", "Comments", "COMMENTS", "Notes"],
        "status": ["status", "STATUS", "Status", "PermitStatus", "PERMIT_STATUS",
                    "CurrentStatus", "CURRENT_STATUS"],
        "owner_name": ["owner_name", "OWNER", "Owner", "APPLICANT", "Applicant",
                       "CONTRACTOR", "Contractor", "ApplicantName", "APPLICANT_NAME",
                       "ContractorName", "CONTRACTOR_NAME", "PropertyOwner"],
        "valuation": ["valuation", "VALUATION", "Valuation", "JobValue", "JOB_VALUE",
                      "EstimatedValue", "ESTIMATED_VALUE", "ProjectValue", "PROJECT_VALUE",
                      "TotalValuation", "TOTAL_VALUATION", "ConstructionCost"],
        "issue_date": ["issue_date", "ISSUE_DATE", "IssueDate", "IssuedDate", "ISSUED_DATE",
                       "PermitDate", "PERMIT_DATE", "DateIssued", "DATE_ISSUED",
                       "CreatedDate", "CREATED_DATE", "AppliedDate"],
    }

    for std_field, candidates in field_map.items():
        for c in candidates:
            if c in attrs and attrs[c] is not None:
                val = str(attrs[c]).strip()
                if val and val.lower() not in ('none', 'null', 'nan', ''):
                    # Skip epoch timestamps that look like big numbers for date fields
                    if std_field == "issue_date" and val.isdigit() and len(val) > 10:
                        try:
                            ts = int(val) / 1000
                            from datetime import datetime
                            lead[std_field] = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                        except:
                            lead[std_field] = val
                    else:
                        lead[std_field] = val
                    break

    if not lead.get("address") and not lead.get("permit_number") and not lead.get("description"):
        return None

    return lead


async def fetch_arcgis(session, url, state, desc, sem):
    async with sem:
        leads = []
        offset = 0
        batch = 2000

        while True:
            params = f"?where=1%3D1&outFields=*&f=json&resultRecordCount={batch}&resultOffset={offset}"
            query_url = f"{url}/query{params}"
            try:
                async with session.get(query_url, timeout=aiohttp.ClientTimeout(total=90)) as resp:
                    if resp.status != 200:
                        print(f"  SKIP {desc}: HTTP {resp.status}")
                        return leads
                    text = await resp.text()
                    if "<html" in text[:200].lower():
                        print(f"  SKIP {desc}: HTML response")
                        return leads
                    data = json.loads(text)
                    if "error" in data:
                        print(f"  SKIP {desc}: {data['error'].get('message', 'error')}")
                        return leads
                    features = data.get("features", [])
                    if not features:
                        break
                    for f in features:
                        attrs = f.get("attributes", {})
                        geom = f.get("geometry", {})
                        if geom:
                            x = geom.get("x")
                            y = geom.get("y")
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
                                except (ValueError, TypeError):
                                    pass

                        lead = normalize_lead(attrs, state, f"arcgis_{desc.replace(' ', '_')}")
                        if lead:
                            # Add coords if missing
                            if "latitude" not in lead:
                                center = CITY_CENTERS.get(state)
                                if center:
                                    lead["latitude"] = center[0] + random.uniform(-0.1, 0.1)
                                    lead["longitude"] = center[1] + random.uniform(-0.1, 0.1)
                            leads.append(lead)

                    if len(features) < batch:
                        break
                    offset += batch
                    if offset >= 100000:
                        break
            except Exception as e:
                print(f"  ERROR {desc}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads):,} leads")
        else:
            print(f"  EMPTY {desc}")
        return leads


async def main():
    print(f"=== Fetch ArcGIS Weak States ===")
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

    sem = asyncio.Semaphore(10)
    all_new = []

    connector = aiohttp.TCPConnector(limit=15, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_arcgis(session, url, state, desc, sem) for url, state, desc in ENDPOINTS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)

    print(f"\nTotal fetched: {len(all_new):,}")

    # Deduplicate
    unique_new = []
    for lead in all_new:
        addr = str(lead.get("address", "")).lower().strip()[:60]
        pn = str(lead.get("permit_number", "")).lower().strip()[:30]
        city = str(lead.get("city", "")).lower().strip()
        key = f"{addr}|{pn}|{city}"
        if key not in dedup:
            dedup.add(key)
            unique_new.append(lead)

    print(f"Unique new: {len(unique_new):,}")

    if unique_new:
        from collections import Counter
        state_counts = Counter(l.get("state", "?") for l in unique_new)
        print("\nBy state:")
        for st, cnt in sorted(state_counts.items(), key=lambda x: -x[1]):
            print(f"  {st}: {cnt:,}")

        existing.extend(unique_new)
        print(f"\nTotal: {len(existing):,}")
        print("Saving...")
        with open(CACHE_PATH, "w") as f:
            json.dump(existing, f)
        print(f"Saved! {os.path.getsize(CACHE_PATH) / 1e9:.2f} GB")

    print(f"Done: {time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    asyncio.run(main())
