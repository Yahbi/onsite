#!/usr/bin/env python3
"""Fix missing cities and scattered coordinates in data_cache.json.

Strategy (all free, no API keys):
1. Extract city from address suffix (double-space pattern: "123 MAIN ST  CHARLOTTE")
2. Extract city from source name (e.g., "Charlotte Residential Permits" → "Charlotte")
3. Infer city from known source→city mapping table
4. Fix scattered coords: when multiple leads share the same address, use the median coordinates
5. Clean up address by removing trailing city name if it was extracted

This script is idempotent — safe to run multiple times.
"""
import json, os, sys, time, re, statistics
from collections import defaultdict

CACHE = os.path.join(os.path.dirname(__file__), "..", "data_cache.json")

# ── Known source → city mappings (covers top missing-city sources) ──
SOURCE_CITY_MAP = {
    "Charlotte Residential Permits": "Charlotte",
    "Charlotte Commercial Permits": "Charlotte",
    "NYC Property Sales": "New York",
    "NYC DOB Permits": "New York",
    "NYC DOB Jobs": "New York",
    "Philadelphia Permits": "Philadelphia",
    "Philadelphia Licenses": "Philadelphia",
    "Portland Building Permits": "Portland",
    "Seattle Building Permits": "Seattle",
    "Denver Building Permits": "Denver",
    "Denver Commercial Permits": "Denver",
    "Austin Building Permits": "Austin",
    "Boston Building Permits": "Boston",
    "Minneapolis Building Permits": "Minneapolis",
    "San Jose Permits": "San Jose",
    "Sacramento Permits": "Sacramento",
    "Nashville Permits": "Nashville",
    "Louisville Permits": "Louisville",
    "Baltimore Permits": "Baltimore",
    "Milwaukee Permits": "Milwaukee",
    "Tucson Permits": "Tucson",
    "Mesa Permits": "Mesa",
    "Fresno Permits": "Fresno",
    "Atlanta Permits": "Atlanta",
    "Raleigh Permits": "Raleigh",
    "Oakland Permits": "Oakland",
    "Tampa Permits": "Tampa",
    "New Orleans Permits": "New Orleans",
    "Baton Rouge Permits": "Baton Rouge",
    "Honolulu Permits": "Honolulu",
}

# ── Known Socrata domain → city ──
DOMAIN_CITY = {
    "data.honolulu.gov": "Honolulu",
    "data.delaware.gov": "",  # state-level, can't assign city
    "data.kcmo.org": "Kansas City",
    "data.somervillema.gov": "Somerville",
    "www.dallasopendata.com": "Dallas",
    "data.cityofchicago.org": "Chicago",
    "data.sfgov.org": "San Francisco",
    "data.lacity.org": "Los Angeles",
    "data.cityofnewyork.us": "New York",
    "data.seattle.gov": "Seattle",
    "data.austintexas.gov": "Austin",
    "data.boston.gov": "Boston",
    "data.nashville.gov": "Nashville",
    "data.louisvilleky.gov": "Louisville",
    "data.baltimorecity.gov": "Baltimore",
    "data.milwaukee.gov": "Milwaukee",
    "data.tucsonaz.gov": "Tucson",
    "data.fresno.gov": "Fresno",
    "data.atlantaga.gov": "Atlanta",
    "data.raleighnc.gov": "Raleigh",
    "data.oaklandca.gov": "Oakland",
    "data.nola.gov": "New Orleans",
    "data.brla.gov": "Baton Rouge",
    "data.providenceri.gov": "Providence",
    "data.hartford.gov": "Hartford",
    "data.cambridgema.gov": "Cambridge",
    "data.detroitmi.gov": "Detroit",
    "data.clevelandohio.gov": "Cleveland",
    "data.cincinnatoh.gov": "Cincinnati",
    "data.stlouis-mo.gov": "St. Louis",
    "data.minneapolismn.gov": "Minneapolis",
    "data.cityofsacramento.org": "Sacramento",
    "data.sanjoseca.gov": "San Jose",
    "data.buffalony.gov": "Buffalo",
    "data.richmondgov.com": "Richmond",
    "data.norfolk.gov": "Norfolk",
    "data.tampagov.net": "Tampa",
    "data.mesaaz.gov": "Mesa",
    "data.jacksonvillenc.gov": "Jacksonville",
    "data.jerseycitynj.gov": "Jersey City",
    "data.cityofboise.org": "Boise",
    "data.wprdc.org": "Pittsburgh",
    "data.montgomeryal.gov": "Montgomery",
    "data.chattanooga.gov": "Chattanooga",
    "data.burlingtonvt.gov": "Burlington",
    "data.capecoralfl.gov": "Cape Coral",
    "data.cityofevanston.org": "Evanston",
    "data.colorado.gov": "",  # state-level
    "data.ct.gov": "",  # state-level
    "data.iowa.gov": "",  # state-level
    "data.maryland.gov": "",  # state-level
    "data.ny.gov": "",  # state-level
    "data.texas.gov": "",  # state-level
    "data.wa.gov": "",  # state-level
}

# ── ArcGIS source → city ──
ARCGIS_CITY = {
    "arcgis_DC_Permits": "Washington",
    "arcgis_Portland_Permits": "Portland",
    "arcgis_Minneapolis": "Minneapolis",
    "arcgis_MN_Metro": "Minneapolis",
    "arcgis_Denver": "Denver",
    "arcgis_Forsyth_County": "Forsyth County",
    "arcgis_Bozeman": "Bozeman",
}


def extract_city_from_address(addr):
    """Extract city from addresses like '123 MAIN ST  CHARLOTTE' (double-space separator)."""
    if not addr:
        return "", addr
    # Look for double-space or triple-space before a trailing city name
    m = re.match(r'^(.+?)\s{2,}([A-Z][A-Za-z .]+)$', addr.strip())
    if m:
        street = m.group(1).strip()
        candidate = m.group(2).strip()
        # Validate: city name should be 2-30 chars, mostly alpha
        if 2 <= len(candidate) <= 30 and candidate.replace(' ', '').replace('.', '').isalpha():
            # Reject if it looks like a street suffix
            suffixes = {'ST', 'AVE', 'BLVD', 'DR', 'CT', 'LN', 'RD', 'PL', 'WAY', 'CIR', 'TER', 'PKY', 'HWY'}
            if candidate.upper() not in suffixes:
                return candidate.title(), street
    return "", addr


def city_from_source(source):
    """Extract city from source name."""
    if not source:
        return ""

    # Direct mapping
    for key, city in SOURCE_CITY_MAP.items():
        if key in source:
            return city

    # Socrata domain pattern: socrata_data.{domain}_{dataset_id}
    m = re.match(r'socrata_(.+?)_[a-z0-9]{4}-[a-z0-9]{4}$', source)
    if m:
        domain = m.group(1)
        if domain in DOMAIN_CITY:
            return DOMAIN_CITY[domain]
        # Try extracting from domain
        dm = re.match(r'data\.([a-z]+?)(?:city|gov|org|com|net)', domain)
        if dm:
            name = dm.group(1)
            if len(name) > 2:
                return name.title()

    # ArcGIS prefix pattern
    for prefix, city in ARCGIS_CITY.items():
        if source.startswith(prefix):
            return city

    # Generic: "CityName Something Permits"
    m2 = re.match(r'^([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s+(?:Residential|Commercial|Building|City)', source)
    if m2:
        return m2.group(1)

    return ""


def fix_scattered_coords(leads):
    """For leads sharing the same address, use median coordinates instead of random offsets."""
    addr_groups = defaultdict(list)
    for i, l in enumerate(leads):
        addr = (l.get('address') or '').strip()
        if addr and l.get('lat') and l.get('lng'):
            try:
                lat = float(l['lat'])
                lng = float(l['lng'])
                if abs(lat) > 1 and abs(lng) > 1:
                    addr_groups[addr].append((i, lat, lng))
            except (ValueError, TypeError):
                pass

    fixed = 0
    for addr, group in addr_groups.items():
        if len(group) < 2:
            continue
        lats = [g[1] for g in group]
        lngs = [g[2] for g in group]
        spread_lat = max(lats) - min(lats)
        spread_lng = max(lngs) - min(lngs)

        # Only fix if coords are scattered (>0.005 deg ≈ 500m)
        if spread_lat > 0.005 or spread_lng > 0.005:
            med_lat = statistics.median(lats)
            med_lng = statistics.median(lngs)
            for idx, _, _ in group:
                leads[idx]['lat'] = round(med_lat, 6)
                leads[idx]['lng'] = round(med_lng, 6)
                fixed += 1

    return fixed


def main():
    print(f"Loading {CACHE}...")
    t0 = time.time()
    with open(CACHE) as f:
        leads = json.load(f)
    print(f"Loaded {len(leads):,} leads in {time.time()-t0:.1f}s")

    # ── Pass 1: Fix cities ──
    fixed_from_addr = 0
    fixed_from_source = 0
    addr_cleaned = 0

    for l in leads:
        city = (l.get('city') or '').strip()
        if city:
            continue  # already has city

        # Strategy 1: Extract from address suffix
        addr = (l.get('address') or '').strip()
        extracted_city, cleaned_addr = extract_city_from_address(addr)
        if extracted_city:
            l['city'] = extracted_city
            if cleaned_addr != addr:
                l['address'] = cleaned_addr
                addr_cleaned += 1
            fixed_from_addr += 1
            continue

        # Strategy 2: Infer from source name
        source = (l.get('source') or '').strip()
        inferred_city = city_from_source(source)
        if inferred_city:
            l['city'] = inferred_city
            fixed_from_source += 1

    # ── Pass 2: Fix scattered coordinates ──
    print("Fixing scattered coordinates...")
    t1 = time.time()
    fixed_coords = fix_scattered_coords(leads)
    print(f"Fixed {fixed_coords:,} scattered coordinates in {time.time()-t1:.1f}s")

    # ── Summary ──
    remaining_no_city = sum(1 for l in leads if not (l.get('city') or '').strip())
    print(f"\nCity fixes:")
    print(f"  From address extraction: {fixed_from_addr:,}")
    print(f"  From source inference:   {fixed_from_source:,}")
    print(f"  Addresses cleaned:       {addr_cleaned:,}")
    print(f"  Still missing city:      {remaining_no_city:,}")
    print(f"  Coords consolidated:     {fixed_coords:,}")

    total_fixed = fixed_from_addr + fixed_from_source + fixed_coords
    if total_fixed == 0:
        print("\nNothing to fix!")
        return

    # ── Save ──
    print(f"\nSaving {len(leads):,} leads...")
    t2 = time.time()
    try:
        import orjson
        with open(CACHE, "wb") as f:
            f.write(orjson.dumps(leads))
        print(f"Saved with orjson in {time.time()-t2:.1f}s")
    except ImportError:
        with open(CACHE, "w") as f:
            json.dump(leads, f)
        print(f"Saved with json in {time.time()-t2:.1f}s")

    print("Done! Restart server to pick up changes.")


if __name__ == "__main__":
    main()
