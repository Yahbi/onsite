#!/usr/bin/env python3
"""Fetch data from verified working Socrata endpoints for weak states - Round 2."""
import asyncio, aiohttp, json, os, sys, time, random
from pathlib import Path

CACHE_PATH = Path(__file__).parent.parent / "data_cache.json"

# State domain mapping
STATE_FROM_DOMAIN = {
    "cos-data.seattle.gov": "WA",
    "data.nola.gov": "LA",
    "data.somervillema.gov": "MA",
    "data.providenceri.gov": "RI",
    "data.bloomington.in.gov": "IN",
    "data.honolulu.gov": "HI",
    "data.brla.gov": "LA",
    "data.princegeorgescountymd.gov": "MD",
    "data.montgomerycountymd.gov": "MD",
    "data.cambridgema.gov": "MA",
    "data.marincounty.gov": "CA",
    "data.chattanooga.gov": "TN",
    "data.raleighnc.gov": "NC",
    "data.wprdc.org": "PA",
    "data.sfgov.org": "CA",
}

# ── Verified working Socrata endpoints ──
SOCRATA_ENDPOINTS = [
    # Washington - Seattle (new domain!)
    ("76t5-zqzr", "cos-data.seattle.gov", "WA", "Seattle Building Permits"),
    ("c4tj-daue", "cos-data.seattle.gov", "WA", "Seattle Electrical Permits"),
    ("c87v-5hwh", "cos-data.seattle.gov", "WA", "Seattle Trade Permits"),
    ("ht3q-kdvx", "cos-data.seattle.gov", "WA", "Seattle Land Use Permits"),
    ("axkr-2p68", "cos-data.seattle.gov", "WA", "Seattle Building Certificates"),
    # Louisiana - New Orleans
    ("nbcf-m6c2", "data.nola.gov", "LA", "NOLA Building Permits 2018+"),
    ("f7tt-z5vu", "data.nola.gov", "LA", "NOLA Historical Permit Data"),
    ("72f9-bi28", "data.nola.gov", "LA", "NOLA BLDS Permits"),
    ("rcm3-fn58", "data.nola.gov", "LA", "NOLA Permits"),
    # Hawaii - Honolulu
    ("4vab-c87q", "data.honolulu.gov", "HI", "Honolulu Building Permits"),
    # Louisiana - Baton Rouge
    ("7fq7-8j7r", "data.brla.gov", "LA", "Baton Rouge Building Permits"),
    # Maryland - extra
    ("weik-ttee", "data.princegeorgescountymd.gov", "MD", "PG County Permits"),
    ("b6ht-fw3x", "data.montgomerycountymd.gov", "MD", "Montgomery Demo Permits"),
    ("ha72-8evs", "data.montgomerycountymd.gov", "MD", "Montgomery Commercial Demo"),
    ("m88u-pqki", "data.montgomerycountymd.gov", "MD", "Montgomery Residential Permits"),
    # Massachusetts - Cambridge
    ("kcfi-ackv", "data.cambridgema.gov", "MA", "Cambridge Demolition Permits"),
    # California - Marin County
    ("mkbn-caye", "data.marincounty.gov", "CA", "Marin Building Permits"),
    # Rhode Island - Providence
    ("ufmm-rbej", "data.providenceri.gov", "RI", "Providence Permits 2009-2018"),
    # Massachusetts - Somerville
    ("vxgw-vmky", "data.somervillema.gov", "MA", "Somerville Permits"),
    ("nneb-s3f7", "data.somervillema.gov", "MA", "Somerville Permit Applications"),
    # Indiana - Bloomington
    ("9q6j-a8rc", "data.bloomington.in.gov", "IN", "Bloomington Rental Permits"),
]

# ── Now search for MORE domains via global Socrata discovery ──
GLOBAL_SEARCHES = [
    # Focus on states with weakest coverage
    "building+permits+Denver+Colorado",
    "building+permits+Aurora+Colorado",
    "building+permits+Lakewood+Colorado",
    "construction+permits+Atlanta+Georgia",
    "building+permits+Savannah+Georgia",
    "building+permits+Augusta+Georgia",
    "building+permits+Portland+Oregon+2024",
    "building+permits+Eugene+Oregon",
    "building+permits+Salem+Oregon",
    "building+permits+Oklahoma+City+2024",
    "building+permits+Tulsa+Oklahoma",
    "building+permits+Norman+Oklahoma",
    "building+permits+Minneapolis+2024",
    "building+permits+St+Paul+Minnesota",
    "building+permits+Detroit+Michigan+2024",
    "building+permits+Grand+Rapids+Michigan",
    "building+permits+Ann+Arbor+Michigan",
    "building+permits+Boise+Idaho+2024",
    "building+permits+Billings+Montana",
    "building+permits+Missoula+Montana",
    "building+permits+Anchorage+Alaska",
    "building+permits+Manchester+New+Hampshire",
    "building+permits+Charleston+South+Carolina",
    "building+permits+Columbia+South+Carolina",
    "building+permits+Greenville+South+Carolina",
    "building+permits+Charleston+West+Virginia",
    "building+permits+Des+Moines+Iowa",
    "building+permits+Cedar+Rapids+Iowa",
    "construction+permits+Connecticut+2024",
    "property+sales+Colorado+2024",
    "property+sales+Georgia+2024",
    "property+sales+Oregon+2024",
    "property+sales+Oklahoma+2024",
    "property+sales+Minnesota+2024",
    "property+sales+Michigan+2024",
    "code+enforcement+Denver",
    "code+enforcement+Atlanta",
    "code+enforcement+Minneapolis",
    "code+enforcement+Detroit",
    "code+enforcement+Seattle",
    "code+enforcement+Portland+Oregon",
    "code+enforcement+Oklahoma+City",
    "demolition+permits+2024",
    "residential+permits+2024",
    "commercial+construction+permits",
    "property+inspections+2024",
    "zoning+permits+2024",
]

CITY_CENTERS = {
    "AK": (61.2181, -149.9003), "NH": (43.2081, -71.5376), "DC": (38.9072, -77.0369),
    "ID": (43.6150, -116.2023), "OR": (45.5152, -122.6784), "KS": (39.0997, -94.5786),
    "OK": (35.4676, -97.5164), "GA": (33.7490, -84.3880), "CO": (39.7392, -104.9903),
    "MN": (44.9778, -93.2650), "IA": (41.5868, -93.6250), "MT": (46.8797, -110.3626),
    "HI": (21.3069, -157.8583), "IL": (41.8781, -87.6298), "MI": (42.3314, -83.0458),
    "WA": (47.6062, -122.3321), "CT": (41.7658, -72.6734), "SC": (32.7765, -79.9311),
    "WV": (38.3498, -81.6326), "LA": (29.9511, -90.0715), "MD": (39.2904, -76.6122),
    "MA": (42.3601, -71.0589), "CA": (34.0522, -118.2437), "RI": (41.8240, -71.4128),
    "IN": (39.7684, -86.1581), "PA": (40.4406, -79.9959), "NC": (35.7796, -78.6382),
    "TN": (36.1627, -86.7816),
}

JUNK_KEYWORDS = ['311', 'tobacco', 'pothole', 'pot hole', 'parking meter', 'bicycle',
                  'dog license', 'cat', 'animal', 'tree removal', 'noise complaint',
                  'graffiti', 'litter', 'trash', 'bus stop', 'traffic signal',
                  'streetlight', 'fire hydrant', 'food truck', 'special event',
                  'sidewalk cafe', 'right-of-way', 'block party', 'film permit',
                  'entertainment', 'festival', 'parade']


def is_permit_related(name):
    name_lower = (name or "").lower()
    if any(b in name_lower for b in JUNK_KEYWORDS):
        return False
    good = ['permit', 'building', 'construction', 'property', 'parcel', 'code enforcement',
            'inspection', 'demolition', 'zoning', 'land use', 'housing', 'real estate',
            'assessment', 'valuation', 'sale', 'transfer', 'deed']
    return any(g in name_lower for g in good)


def normalize_lead(raw, state, source_name):
    lead = {"source": source_name, "state": state}

    field_map = {
        "address": ["address", "full_address", "location", "site_address", "property_address",
                     "streetaddress", "street_address", "addr", "siteaddress", "originaladdress1",
                     "originaladdress", "permit_address", "project_address", "ADDRESS",
                     "FULL_ADDRESS", "FullAddress", "SITEADDRESS", "STREETADDRESS",
                     "stno", "street_address"],
        "city": ["city", "CITY", "City", "municipality", "town", "jurisdiction",
                 "originalcity", "city1", "city_town"],
        "permit_number": ["permit_number", "permitnumber", "permit_no", "permit_num", "PERMIT_NO",
                          "PermitNum", "PERMITNUMBER", "permit_id", "PERMIT_NUMBER", "record_id",
                          "permitnum", "buildingpermitno", "permit_case_id", "permitno",
                          "permit_tracking_id"],
        "permit_type": ["permit_type", "permittype", "type", "work_type", "PERMIT_TYPE",
                        "permitclassmapped", "permittypemapped", "permit_category",
                        "applicationtype", "type_permit", "worktype"],
        "description": ["description", "work_description", "project_description", "DESCRIPTION",
                        "scope_of_work", "permit_description", "comments", "notes",
                        "permittypedesc", "projectdescription", "case_name"],
        "status": ["status", "permit_status", "STATUS", "statuscurrent", "current_status"],
        "owner_name": ["owner_name", "owner", "applicant", "contractor", "OWNER", "APPLICANT",
                       "applicant_name", "contractor_name", "contractorcompanyname",
                       "property_owner", "dba"],
        "valuation": ["valuation", "job_value", "estimated_value", "project_value", "VALUE",
                      "total_valuation", "est_project_cost", "construction_cost",
                      "estimatedvalueofwork", "expected_construction_cost", "construction_value",
                      "projectvalue", "declaredvaluation", "acceptedvalue"],
        "issue_date": ["issue_date", "issued_date", "permit_date", "date_issued", "ISSUE_DATE",
                       "issueddate", "date", "applied_date", "filing_date", "created_date",
                       "permit_issuance_date", "addeddate", "issued_date", "received_date",
                       "creationdate"],
        "zip": ["zip", "zipcode", "zip_code", "postal_code", "ZIP", "ZIPCODE", "originalzip"],
    }

    for std_field, candidates in field_map.items():
        for c in candidates:
            if c in raw and raw[c]:
                val = str(raw[c]).strip()
                if val and val.lower() not in ('none', 'null', 'nan', ''):
                    lead[std_field] = val
                    break

    # Handle stno+stname combo for address
    if "address" not in lead:
        stno = str(raw.get("stno", "")).strip()
        stname = str(raw.get("stname", "")).strip()
        suffix = str(raw.get("suffix", "")).strip()
        if stno and stname:
            lead["address"] = f"{stno} {stname} {suffix}".strip()

    # Extract coordinates
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

    # Check nested location
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
    if "latitude" not in lead or "longitude" not in lead:
        center = CITY_CENTERS.get(state)
        if center:
            lead["latitude"] = center[0] + random.uniform(-0.15, 0.15)
            lead["longitude"] = center[1] + random.uniform(-0.15, 0.15)

    if not lead.get("address") and not lead.get("permit_number") and not lead.get("description"):
        return None

    return lead


async def fetch_socrata(session, dataset_id, domain, state, desc, sem):
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
                    ct = resp.headers.get("content-type", "")
                    if "json" not in ct:
                        print(f"  SKIP {desc}: not JSON ({ct[:40]})")
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
                    if offset >= 50000:
                        break
            except Exception as e:
                print(f"  ERROR {desc}: {e}")
                break

        if leads:
            print(f"  OK {desc}: {len(leads):,} leads")
        return leads


async def discover_and_fetch(session, query, sem, dedup):
    """Global Socrata discovery search and fetch."""
    leads = []
    disco_url = f"https://api.us.socrata.com/api/catalog/v1?q={query}&limit=15"
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
                domain = result.get("metadata", {}).get("domain", "")

                if rtype != "dataset" or not rid:
                    continue
                if not is_permit_related(name):
                    continue

                # Infer state from domain
                state = STATE_FROM_DOMAIN.get(domain, "")
                if not state:
                    # Try to guess from domain
                    if "denver" in domain: state = "CO"
                    elif "colorado" in domain: state = "CO"
                    elif "atlanta" in domain: state = "GA"
                    elif "georgia" in domain: state = "GA"
                    elif "fulton" in domain: state = "GA"
                    elif "portland" in domain or "oregon" in domain: state = "OR"
                    elif "okc" in domain or "oklahoma" in domain or "tulsa" in domain: state = "OK"
                    elif "minneapolis" in domain or "minnesota" in domain or "ramsey" in domain or "hennepin" in domain: state = "MN"
                    elif "detroit" in domain or "michigan" in domain: state = "MI"
                    elif "boise" in domain or "idaho" in domain: state = "ID"
                    elif "montana" in domain or "billings" in domain or "missoula" in domain: state = "MT"
                    elif "anchorage" in domain or "alaska" in domain or "muni.org" in domain: state = "AK"
                    elif "dc.gov" in domain: state = "DC"
                    elif "seattle" in domain or "wa.gov" in domain or "pierce" in domain or "king" in domain: state = "WA"
                    elif "charleston" in domain and "sc" in domain: state = "SC"
                    elif "southcarolina" in domain: state = "SC"
                    elif "iowa" in domain: state = "IA"
                    elif "connecticut" in domain or "ct.gov" in domain: state = "CT"
                    elif "newhampshire" in domain or "nh" in domain: state = "NH"
                    elif "westvirginia" in domain or "wv.gov" in domain: state = "WV"
                    elif "houston" in domain or "texas" in domain or "dallas" in domain or "austin" in domain: state = "TX"
                    elif "newyork" in domain or "ny.gov" in domain: state = "NY"
                    elif "chicago" in domain or "cook" in domain or "illinois" in domain: state = "IL"
                    elif "la" in domain and "city" in domain: state = "CA"
                    elif "howard" in domain or "maryland" in domain or "montgomery" in domain and "md" in domain: state = "MD"
                    elif "nola" in domain or "brla" in domain: state = "LA"
                    elif "somerville" in domain or "cambridge" in domain or "boston" in domain: state = "MA"
                    elif "mesa" in domain or "phoenix" in domain or "arizona" in domain: state = "AZ"
                    elif "providence" in domain: state = "RI"
                    elif "bloomington.in" in domain or "indiana" in domain: state = "IN"
                    elif "pittsburgh" in domain or "wprdc" in domain or "pa.gov" in domain: state = "PA"
                    elif "raleigh" in domain or "charlotte" in domain or "nc" in domain: state = "NC"
                    elif "chattanooga" in domain or "nashville" in domain or "memphis" in domain or "tn" in domain: state = "TN"
                    elif "nj.gov" in domain: state = "NJ"
                    else:
                        continue  # Skip if we can't determine state

                # Skip if already fetched
                fetch_key = f"{domain}/{rid}"
                if fetch_key in dedup:
                    continue
                dedup.add(fetch_key)

                batch = await fetch_socrata(session, rid, domain, state, name, sem)
                leads.extend(batch)
    except Exception as e:
        pass

    return leads


async def main():
    print(f"=== Fetch Weak States Round 2 ===")
    print(f"Started: {time.strftime('%H:%M:%S')}")

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
    fetched_datasets = set()

    connector = aiohttp.TCPConnector(limit=20, force_close=True)
    async with aiohttp.ClientSession(connector=connector, headers={"User-Agent": "Onsite/1.0"}) as session:

        # Phase 1: Hardcoded verified endpoints
        print(f"\n--- Phase 1: Verified endpoints ({len(SOCRATA_ENDPOINTS)}) ---")
        tasks = []
        for did, domain, state, desc in SOCRATA_ENDPOINTS:
            fetched_datasets.add(f"{domain}/{did}")
            tasks.append(fetch_socrata(session, did, domain, state, desc, sem))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                all_new.extend(r)
        print(f"Phase 1 total: {len(all_new):,} leads")

        # Phase 2: Global discovery searches
        print(f"\n--- Phase 2: Global discovery ({len(GLOBAL_SEARCHES)} searches) ---")
        # Process in batches to avoid overwhelming
        batch_size = 10
        phase2 = 0
        for i in range(0, len(GLOBAL_SEARCHES), batch_size):
            batch = GLOBAL_SEARCHES[i:i+batch_size]
            tasks = [discover_and_fetch(session, q, sem, fetched_datasets) for q in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    all_new.extend(r)
                    phase2 += len(r)
        print(f"Phase 2 total: {phase2:,} leads")

    # Deduplicate against existing
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
        from collections import Counter
        state_counts = Counter(l.get("state", "?") for l in unique_new)
        print("\nNew leads by state:")
        for st, cnt in sorted(state_counts.items(), key=lambda x: -x[1]):
            print(f"  {st}: {cnt:,}")

        existing.extend(unique_new)
        print(f"\nTotal leads after merge: {len(existing):,}")
        print("Saving cache...")

        with open(CACHE_PATH, "w") as f:
            json.dump(existing, f)

        print(f"Cache saved! Size: {os.path.getsize(CACHE_PATH) / 1e9:.2f} GB")
    else:
        print("No new leads to add.")

    print(f"\nDone: {time.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    asyncio.run(main())
