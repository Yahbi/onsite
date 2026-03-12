#!/usr/bin/env python3
"""Fix bad addresses in data_cache.json:
1. Extract real addresses from Socrata JSON blobs (human_address)
2. Extract embedded lat/lng from JSON blobs
3. Clean 'unknown', 'n/a', etc. to empty string
4. Clean JSON array addresses to empty string
"""
import json, ast, sys, os, time

CACHE = os.path.join(os.path.dirname(__file__), "..", "data_cache.json")
BAD_ADDR = {"unknown", "not provided", "n/a", "none", "no address", "na", "tbd",
            "pending", "null", "not available", "unspecified", "various", "other",
            "see description", "see plans", "see attached", "no site address"}

def fix_leads(leads):
    fixed_json = 0
    fixed_bad = 0
    extracted_coords = 0
    for l in leads:
        addr = str(l.get("address", "") or "").strip()

        # Fix JSON blob addresses
        if addr.startswith("{"):
            try:
                parsed = ast.literal_eval(addr)
                ha = parsed.get("human_address", "")
                if isinstance(ha, str) and ha.startswith("{"):
                    ha = json.loads(ha)
                if isinstance(ha, dict):
                    real_addr = ha.get("address", "").strip()
                    if real_addr:
                        l["address"] = real_addr
                        fixed_json += 1
                    else:
                        l["address"] = ""
                    # Fill missing city/state/zip from human_address
                    if not l.get("city") and ha.get("city"):
                        l["city"] = ha["city"]
                    if not l.get("state") and ha.get("state"):
                        l["state"] = ha["state"]
                    if not l.get("zip") and ha.get("zip"):
                        l["zip"] = ha["zip"]
                else:
                    l["address"] = ""
                # Extract embedded coordinates
                lat_str = parsed.get("latitude")
                lng_str = parsed.get("longitude")
                if lat_str and lng_str:
                    try:
                        flat = float(lat_str)
                        flng = float(lng_str)
                        if abs(flat) <= 90 and abs(flng) <= 180 and flat != 0 and flng != 0:
                            # Only set if lead doesn't already have coords
                            if not l.get("lat") or not l.get("lng"):
                                l["lat"] = flat
                                l["lng"] = flng
                                extracted_coords += 1
                            elif l.get("lat") == "" or l.get("lng") == "":
                                l["lat"] = flat
                                l["lng"] = flng
                                extracted_coords += 1
                    except (ValueError, TypeError):
                        pass
            except Exception:
                l["address"] = ""
                fixed_json += 1

        # Fix JSON array addresses
        elif addr.startswith("["):
            l["address"] = ""
            fixed_bad += 1

        # Fix known bad values
        elif addr.lower() in BAD_ADDR:
            l["address"] = ""
            fixed_bad += 1

    return leads, fixed_json, fixed_bad, extracted_coords


def main():
    print(f"Loading {CACHE}...")
    t0 = time.time()
    with open(CACHE) as f:
        leads = json.load(f)
    print(f"Loaded {len(leads):,} leads in {time.time()-t0:.1f}s")

    leads, fixed_json, fixed_bad, extracted_coords = fix_leads(leads)
    print(f"Fixed {fixed_json:,} JSON blob addresses")
    print(f"Cleaned {fixed_bad:,} bad/array addresses")
    print(f"Extracted {extracted_coords:,} coordinates from JSON blobs")

    total_fixed = fixed_json + fixed_bad + extracted_coords
    if total_fixed == 0:
        print("Nothing to fix!")
        return

    print(f"Saving {len(leads):,} leads...")
    t0 = time.time()
    try:
        import orjson
        with open(CACHE, "wb") as f:
            f.write(orjson.dumps(leads))
        print(f"Saved with orjson in {time.time()-t0:.1f}s")
    except ImportError:
        with open(CACHE, "w") as f:
            json.dump(leads, f)
        print(f"Saved with json in {time.time()-t0:.1f}s")

    print("Done! Restart server to pick up changes.")


if __name__ == "__main__":
    main()
