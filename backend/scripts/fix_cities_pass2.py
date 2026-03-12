#!/usr/bin/env python3
"""Pass 2: Fix remaining missing cities using more advanced patterns.

Handles:
1. Comma-separated: "2509 ESSEX PL, NASHVILLE, TN 37212" → city=Nashville
2. Full address with state abbrev: "5209 MLK WAY S SEATTLE WA 98118" → city=Seattle
3. Known source→city for remaining sources
4. Short addresses (<5 chars) → remove as useless
5. Missing state → fill from source patterns
"""
import json, os, re, time, sys
from collections import Counter

CACHE = os.path.join(os.path.dirname(__file__), "..", "data_cache.json")

# US state abbreviations
STATES = {
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
    'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
    'VA','WA','WV','WI','WY','DC'
}

# Remaining source→city mappings
SOURCE_CITY_MAP = {
    "BLDS Working Copy UT": "Salt Lake City",
    "SALT LAKE CITY": "Salt Lake City",
    "Nashville Historic Permits": "Nashville",
    "Cincinnati Combo Permits": "Cincinnati",
    "Cincinnati Licenses/Use Permits": "Cincinnati",
    "arcgis_Detroit": "Detroit",
    "arcgis_Boise": "Boise",
}

# Socrata domain→city for remaining
DOMAIN_CITY = {
    "data.cincinnati-oh.gov": "Cincinnati",
    "data.bloomington.in.gov": "Bloomington",
    "mydata.iowa.gov": "",  # state-level, various cities
    "cos-data.seattle.gov": "Seattle",
    "data.wa.gov": "",  # state-level
    "data.ct.gov": "",  # state-level
    "datacatalog.cookcountyil.gov": "",  # county-level, various cities
    "citydata.mesaaz.gov": "Mesa",
    "data.delaware.gov": "",  # state-level — but addresses have city embedded
}

# Source→state for filling missing state
SOURCE_STATE = {
    "socrata_data.delaware.gov": "DE",
    "socrata_datacatalog.cookcountyil.gov": "IL",
    "socrata_data.ct.gov": "CT",
    "BLDS Working Copy UT": "UT",
    "SALT LAKE CITY": "UT",
    "socrata_data.cincinnati-oh.gov": "OH",
    "socrata_data.bloomington.in.gov": "IN",
    "socrata_mydata.iowa.gov": "IA",
    "socrata_cos-data.seattle.gov": "WA",
    "socrata_data.wa.gov": "WA",
    "Nashville Historic Permits": "TN",
    "Cincinnati Combo Permits": "OH",
    "Cincinnati Licenses/Use Permits": "OH",
    "arcgis_Detroit": "MI",
    "arcgis_Boise": "ID",
    "socrata_citydata.mesaaz.gov": "AZ",
}


def parse_comma_address(addr):
    """Parse 'STREET, CITY, ST ZIP' or 'STREET, CITY, ST' patterns."""
    # Pattern: ..., CityName, ST 12345
    m = re.match(
        r'^(.+?),\s*([A-Za-z][A-Za-z .]+?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?(?:\s+US)?$',
        addr.strip()
    )
    if m:
        street = m.group(1).strip()
        city = m.group(2).strip().title()
        state = m.group(3).upper()
        zipcode = (m.group(4) or '').strip()
        if state in STATES and len(city) >= 2:
            return street, city, state, zipcode
    return None


def parse_trailing_city_state(addr):
    """Parse 'STREET CITY ST ZIP' (no commas, state abbrev present)."""
    # Pattern: ... CITYNAME ST 12345 or ... CITYNAME ST
    m = re.match(
        r'^(.+?)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?)\s+([A-Z]{2})\s*(\d{5})?$',
        addr.strip()
    )
    if m:
        street = m.group(1).strip()
        city = m.group(2).strip()
        state = m.group(3).upper()
        zipcode = (m.group(4) or '').strip()
        if state in STATES and len(city) >= 3:
            # Make sure city isn't a street suffix
            suffixes = {'ST', 'AVE', 'BLVD', 'DR', 'CT', 'LN', 'RD', 'PL', 'WAY', 'CIR',
                        'TER', 'PKY', 'HWY', 'PKWY', 'STE', 'APT', 'UNIT', 'FL'}
            if city.upper() not in suffixes:
                return street, city.title(), state, zipcode
    return None


def city_from_source_pass2(source):
    """Infer city from source name (pass 2 with more mappings)."""
    if not source:
        return "", ""

    for key, city in SOURCE_CITY_MAP.items():
        if key in source:
            # Find state
            for sk, st in SOURCE_STATE.items():
                if sk in source:
                    return city, st
            return city, ""

    # Socrata domain
    m = re.match(r'socrata_(.+?)_[a-z0-9]{4}-[a-z0-9]{4}$', source)
    if m:
        domain = m.group(1)
        city = DOMAIN_CITY.get(domain, None)
        if city is not None:
            # Find state
            for sk, st in SOURCE_STATE.items():
                if sk in source:
                    return city, st
            return city, ""

    return "", ""


def main():
    print(f"Loading {CACHE}...")
    t0 = time.time()
    with open(CACHE) as f:
        leads = json.load(f)
    print(f"Loaded {len(leads):,} leads in {time.time()-t0:.1f}s")

    fixed_comma = 0
    fixed_trailing = 0
    fixed_source = 0
    fixed_state = 0
    removed_short = 0
    fixed_zip = 0
    addr_cleaned = 0

    to_remove = []

    for i, l in enumerate(leads):
        city = (l.get('city') or '').strip()
        addr = (l.get('address') or '').strip()
        state = (l.get('state') or '').strip()

        # Remove leads with very short/useless addresses
        if len(addr) < 3:
            to_remove.append(i)
            removed_short += 1
            continue

        # Fix missing state from source
        if not state:
            source = l.get('source', '')
            for sk, st in SOURCE_STATE.items():
                if sk in source:
                    l['state'] = st
                    fixed_state += 1
                    state = st
                    break

        if city:
            continue  # already has city

        # Strategy 1: Comma-separated address parsing
        result = parse_comma_address(addr)
        if result:
            street, parsed_city, parsed_state, parsed_zip = result
            l['city'] = parsed_city
            l['address'] = street
            if not state and parsed_state:
                l['state'] = parsed_state
                fixed_state += 1
            if not l.get('zip') and parsed_zip:
                l['zip'] = parsed_zip
                fixed_zip += 1
            fixed_comma += 1
            addr_cleaned += 1
            continue

        # Strategy 2: Trailing CITY ST ZIP (no commas)
        result2 = parse_trailing_city_state(addr)
        if result2:
            street, parsed_city, parsed_state, parsed_zip = result2
            l['city'] = parsed_city
            l['address'] = street
            if not state and parsed_state:
                l['state'] = parsed_state
                fixed_state += 1
            if not l.get('zip') and parsed_zip:
                l['zip'] = parsed_zip
                fixed_zip += 1
            fixed_trailing += 1
            addr_cleaned += 1
            continue

        # Strategy 3: Infer from source
        inferred_city, inferred_state = city_from_source_pass2(l.get('source', ''))
        if inferred_city:
            l['city'] = inferred_city
            if not state and inferred_state:
                l['state'] = inferred_state
                fixed_state += 1
            fixed_source += 1

    # Remove short-address leads (reverse order to preserve indices)
    if to_remove:
        print(f"Removing {len(to_remove):,} leads with addresses < 3 chars...")
        for idx in reversed(to_remove):
            leads.pop(idx)

    remaining_no_city = sum(1 for l in leads if not (l.get('city') or '').strip())
    remaining_no_state = sum(1 for l in leads if not (l.get('state') or '').strip())

    print(f"\nPass 2 results:")
    print(f"  Comma-parse city extraction: {fixed_comma:,}")
    print(f"  Trailing city/state extraction: {fixed_trailing:,}")
    print(f"  Source-inferred city: {fixed_source:,}")
    print(f"  State filled: {fixed_state:,}")
    print(f"  ZIP filled: {fixed_zip:,}")
    print(f"  Addresses cleaned: {addr_cleaned:,}")
    print(f"  Short addresses removed: {removed_short:,}")
    print(f"  Remaining no city: {remaining_no_city:,}")
    print(f"  Remaining no state: {remaining_no_state:,}")
    print(f"  Total leads: {len(leads):,}")

    total_fixed = fixed_comma + fixed_trailing + fixed_source + fixed_state + removed_short
    if total_fixed == 0:
        print("\nNothing to fix!")
        return

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
