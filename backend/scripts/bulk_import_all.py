#!/usr/bin/env python3
"""
Bulk Import ALL — imports every row from the CSV that has a URL
and isn't already in the database.

- API sources (Socrata/ArcGIS/CKAN/REST): validated via HTTP, status=active
- Portal sources (Accela/Tyler/SmartGov/etc.): stored with portal_type, status=active
- County portals: stored with source_type matching platform, status=reference

Usage:
    python3 bulk_import_all.py                 # full import
    python3 bulk_import_all.py --dry-run       # preview only
    python3 bulk_import_all.py --validate-apis # also HTTP-validate API sources
"""

import argparse
import asyncio
import csv
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse, urlencode

import aiohttp

# ---------------------------------------------------------------------------
# State abbreviations
# ---------------------------------------------------------------------------
STATE_ABBREV = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
    "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
    "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND",
    "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
    "Puerto Rico": "PR", "Guam": "GU", "U.S. Virgin Islands": "VI",
    "American Samoa": "AS", "Northern Mariana Islands": "MP",
    "NATIONAL": "US", "National": "US", "Nationwide": "US",
    "Washington DC": "DC",
    # Abbreviations already present in CSV
    "CA": "CA", "IL": "IL", "MA": "MA", "NY": "NY", "NJ": "NJ",
    "OH": "OH", "PA": "PA", "TX": "TX", "MD": "MD", "WA": "WA",
    "AZ": "AZ", "DC": "DC", "LA": "LA", "FL": "FL", "VA": "VA",
    "MO": "MO", "UT": "UT", "CT": "CT", "AR": "AR", "CO": "CO",
    "GA": "GA", "HI": "HI", "IA": "IA", "WI": "WI",
}

_SOCRATA_RID = re.compile(r"([a-z0-9]{4}-[a-z0-9]{4})")
_SOCRATA_URL = re.compile(r"https?://([^/]+)/resource/([a-z0-9]{4}-[a-z0-9]{4})")


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

# Platform → source_type mapping
PLATFORM_MAP = {
    "accela": "Accela",
    "tyler energov": "EnerGov",
    "energov": "EnerGov",
    "tyler esuite": "Tyler_eSuite",
    "esuite": "Tyler_eSuite",
    "tyler css": "Tyler_CSS",
    "opengov": "OpenGov",
    "smartgov": "SmartGov",
    "etrakit": "eTRAKiT",
    "citizenserve": "Citizenserve",
    "citizenserve portal": "Citizenserve",
    "bs&a online": "BSA",
    "bs&a": "BSA",
    "cityview": "CityView",
    "click2gov": "Click2Gov",
    "iworq": "iWorQ",
    "civicgov": "CivicGov",
    "govpilot": "GovPilot",
    "evolve": "Evolve",
    "mgo connect": "MGO",
    "civicplus": "CivicPlus",
    "salesforce": "Salesforce",
    "lama": "LAMA",
    "fasttrackgov": "FastTrackGov",
    "mygov": "MyGov",
    "cityworks": "Cityworks",
    "govbuilt": "GovBuilt",
    "logis": "LOGIS",
    "logis epermits": "LOGIS",
    "sdl portal": "SDL",
    "sagesgov": "SagesGov",
    "permiteyes": "PermitEyes",
    "projectdox": "ProjectDox",
    "viewpoint cloud": "ViewPointCloud",
    "civic access": "CivicAccess",
    "municipal": "Municipal",
    "socrata": "Socrata",
    "socrata soda": "Socrata",
    "arcgis": "ArcGIS",
    "arcgis hub": "ArcGIS_Hub",
    "arcgis rest": "ArcGIS",
    "arcgis open data": "ArcGIS_Hub",
    "ckan": "CKAN",
    "opendatasoft": "OpenDataSoft",
    "nj dca": "NJ_DCA",
    "county portal": "County_Portal",
    "county website": "County_Portal",
    "custom": "Direct",
    "decentralized": "Direct",
}

# Source types that are machine-queryable APIs
API_TYPES = {
    "Socrata", "ArcGIS", "ArcGIS_Hub", "CKAN", "Federal",
    "OpenDataSoft", "Direct",
}

# Portal types that have standardized platforms (scraper candidates)
SCRAPER_PORTAL_TYPES = {
    "Accela", "EnerGov", "Tyler_eSuite", "Tyler_CSS", "OpenGov",
    "SmartGov", "eTRAKiT", "Citizenserve", "BSA", "CityView",
    "Click2Gov", "iWorQ", "CivicGov", "GovPilot",
}


def classify_source(platform: str, data_format: str, url: str) -> str:
    """Determine source_type from CSV fields."""
    plat = platform.lower().strip()
    fmt = data_format.lower().strip()
    url_l = url.lower()

    # Check platform map first
    for key, stype in PLATFORM_MAP.items():
        if key in plat:
            return stype

    # URL-based detection
    if _SOCRATA_URL.search(url_l):
        return "Socrata"
    if "socrata" in fmt or "soda" in fmt:
        return "Socrata"
    if "arcgis" in url_l or "featureserver" in url_l or "mapserver" in url_l:
        if "hub" in url_l or "opendata" in url_l:
            return "ArcGIS_Hub"
        return "ArcGIS"
    if "/api/3/action/" in url_l:
        return "CKAN"
    if "ckan" in fmt:
        return "CKAN"
    if "opendatasoft" in url_l or "api/explore" in url_l:
        return "OpenDataSoft"
    if "json api" in fmt or "rest api" in fmt:
        return "Federal"
    if "csv/json" in fmt:
        return "Direct"

    # Format-based portal detection
    for key, stype in PLATFORM_MAP.items():
        if key in fmt:
            return stype

    # Generic portal with URL
    if "county portal" in fmt or "county portal" in plat:
        return "County_Portal"
    if "web portal" in fmt:
        return "Web_Portal"

    return "Direct"


def extract_domain_rid(url: str, source_type: str):
    """Extract domain and resource_id from URL."""
    domain = urlparse(url).netloc or ""
    resource_id = ""
    if source_type == "Socrata":
        m = _SOCRATA_URL.search(url)
        if m:
            domain, resource_id = m.group(1), m.group(2)
        else:
            rid_m = _SOCRATA_RID.search(urlparse(url).path)
            if rid_m:
                resource_id = rid_m.group(1)
    return domain, resource_id


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

def load_existing(db_path: str):
    """Load existing URLs and domain+rid pairs."""
    urls = set()
    dr_pairs = set()
    with sqlite3.connect(db_path) as conn:
        for domain, rid, api_url in conn.execute(
            "SELECT domain, resource_id, api_url FROM api_sources"
        ):
            if api_url:
                urls.add(api_url.lower().rstrip("/"))
            if domain and rid:
                dr_pairs.add((domain.lower(), rid.lower()))
    return urls, dr_pairs


def is_dup(url: str, domain: str, rid: str, existing_urls: set, existing_dr: set) -> bool:
    if url.lower().rstrip("/") in existing_urls:
        return True
    if domain and rid and (domain.lower(), rid.lower()) in existing_dr:
        return True
    return False


# ---------------------------------------------------------------------------
# API validation
# ---------------------------------------------------------------------------

def build_probe(source_type: str, url: str, domain: str, rid: str) -> str:
    stype = source_type.lower()
    if stype == "socrata" and domain and rid:
        return f"https://{domain}/resource/{rid}.json?$limit=1"
    if stype in ("arcgis", "arcgis_hub"):
        base = url.rstrip("/")
        if "/query" not in base.lower():
            if "featureserver" in base.lower() or "mapserver" in base.lower():
                if not re.search(r"/\d+$", base):
                    base += "/0"
                base += "/query"
        if "?" in base:
            return base
        return f"{base}?{urlencode({'where': '1=1', 'resultRecordCount': '1', 'f': 'json'})}"
    if stype == "ckan":
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}limit=1"
    return url


async def validate_apis(rows: list, workers: int = 30) -> dict:
    """HTTP-validate API sources. Returns {url: (valid, status_code)}."""
    sem = asyncio.Semaphore(workers)
    results = {}
    connector = aiohttp.TCPConnector(limit=workers, force_close=True)

    async def _probe(session, url, probe_url):
        try:
            async with sem:
                async with session.get(
                    probe_url,
                    timeout=aiohttp.ClientTimeout(total=15),
                    headers={"User-Agent": "Onsite/3.2"},
                    ssl=False,
                ) as resp:
                    ok = 200 <= resp.status < 400
                    results[url] = (ok, resp.status)
        except Exception:
            results[url] = (False, 0)

    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in batches
        batch_size = 200
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            tasks = []
            for r in batch:
                url = r["url"]
                probe = build_probe(r["source_type"], url, r["domain"], r["rid"])
                tasks.append(_probe(session, url, probe))
            await asyncio.gather(*tasks, return_exceptions=True)
            done = min(i + batch_size, len(rows))
            valid_so_far = sum(1 for v in results.values() if v[0])
            print(f"  [{done}/{len(rows)}] validated — {valid_so_far} OK so far")

    return results


# ---------------------------------------------------------------------------
# DB insert
# ---------------------------------------------------------------------------

def insert_source(conn, row: dict) -> bool:
    """Insert a single source. Returns True on success."""
    try:
        conn.execute("""
            INSERT INTO api_sources (
                source_type, location, state, data_category, data_subcategory,
                api_name, api_url, format, description, geographic_scope,
                domain, resource_id, query_url, priority_tier, status,
                error_count, verified, source_file, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      0, ?, ?, datetime('now'), datetime('now'))
        """, (
            row["source_type"],
            row["location"],
            row["state"],
            "building_permits",
            "construction",
            row["api_name"],
            row["url"],
            "json",
            row["description"],
            row["scope"],
            row["domain"],
            row["rid"],
            row.get("query_url", ""),
            row["priority"],
            row["status"],
            row["verified"],
            "csv_bulk_import_2026_03",
        ))
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as exc:
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Bulk import ALL missing CSV sources")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--validate-apis", action="store_true",
                        help="HTTP-validate API sources before importing")
    parser.add_argument("--csv", default=str(
        Path.home() / "Desktop" / "US_Construction_Permit_APIs_Complete.csv"))
    parser.add_argument("--db", default=None)
    parser.add_argument("--workers", type=int, default=30)
    args = parser.parse_args()

    # Resolve DB
    if args.db is None:
        for p in [Path("backend/leads.db"), Path("leads.db")]:
            if p.exists():
                args.db = str(p)
                break
        if not args.db:
            print("Cannot find leads.db")
            sys.exit(1)
    print(f"DB: {args.db}")

    # Parse CSV
    print(f"\nParsing CSV: {args.csv}")
    with open(args.csv, "r", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))

    data = [r for r in raw_rows if not (r.get("State", "").startswith("==="))]
    print(f"  Data rows: {len(data)}")

    # Load existing
    existing_urls, existing_dr = load_existing(args.db)
    print(f"  Existing in DB: {len(existing_urls)} URLs")

    # Process each row
    api_rows = []      # Machine-queryable APIs
    portal_rows = []   # Standardized portals (scraper candidates)
    county_rows = []   # Generic county/web portals
    skipped = 0
    dupes = 0

    for r in data:
        url = (r.get("Full API Endpoint / Direct URL") or "").strip()
        if not url.startswith("http"):
            skipped += 1
            continue

        state_full = (r.get("State") or "").strip()
        state = STATE_ABBREV.get(state_full, "")
        jurisdiction = (r.get("Jurisdiction") or "").strip()
        source_name = (r.get("Source Name") or "").strip()
        platform = (r.get("Platform") or "").strip()
        data_format = (r.get("Data Format") or "").strip()
        permit_types = (r.get("Permit Types Covered") or "").strip()
        access_type = (r.get("Access Type") or "").strip()

        source_type = classify_source(platform, data_format, url)
        domain, rid = extract_domain_rid(url, source_type)

        if is_dup(url, domain, rid, existing_urls, existing_dr):
            dupes += 1
            continue

        # Add to existing set to prevent CSV internal dupes
        existing_urls.add(url.lower().rstrip("/"))
        if domain and rid:
            existing_dr.add((domain.lower(), rid.lower()))

        is_state = jurisdiction.lower() in ("statewide", "all", "")
        location = f"{jurisdiction}, {state}" if state else jurisdiction
        description = (
            f"{permit_types or 'Building permits'} for "
            f"{jurisdiction}, {state_full}"
        )

        row_data = {
            "url": url,
            "source_type": source_type,
            "state": state,
            "location": location,
            "api_name": source_name or f"{jurisdiction} Permits",
            "description": description,
            "domain": domain,
            "rid": rid,
            "scope": "state" if is_state else "city",
            "platform": platform,
            "permit_types": permit_types,
            "access_type": access_type,
        }

        if source_type in API_TYPES:
            row_data["priority"] = 2 if is_state or state_full in ("NATIONAL", "National") else 3
            row_data["status"] = "active"
            row_data["verified"] = 0
            row_data["query_url"] = build_probe(source_type, url, domain, rid)
            api_rows.append(row_data)
        elif source_type in SCRAPER_PORTAL_TYPES:
            row_data["priority"] = 4
            row_data["status"] = "active"
            row_data["verified"] = 0
            row_data["query_url"] = ""
            portal_rows.append(row_data)
        else:
            row_data["priority"] = 5
            row_data["status"] = "reference"
            row_data["verified"] = 0
            row_data["query_url"] = ""
            county_rows.append(row_data)

    print(f"\n=== CLASSIFICATION ===")
    print(f"  Skipped (no URL):      {skipped}")
    print(f"  Duplicates:            {dupes}")
    print(f"  API sources (new):     {len(api_rows)}")
    print(f"  Portal sources (new):  {len(portal_rows)}")
    print(f"  County/other (new):    {len(county_rows)}")
    print(f"  TOTAL to import:       {len(api_rows) + len(portal_rows) + len(county_rows)}")

    # Breakdown
    api_types = Counter(r["source_type"] for r in api_rows)
    portal_types = Counter(r["source_type"] for r in portal_rows)
    county_types = Counter(r["source_type"] for r in county_rows)

    print(f"\n  API breakdown:")
    for t, c in api_types.most_common():
        print(f"    {t:20s} {c:5d}")
    print(f"\n  Portal breakdown:")
    for t, c in portal_types.most_common():
        print(f"    {t:20s} {c:5d}")
    print(f"\n  County/other breakdown:")
    for t, c in county_types.most_common():
        print(f"    {t:20s} {c:5d}")

    # Validate APIs if requested
    if args.validate_apis and api_rows:
        print(f"\n=== VALIDATING {len(api_rows)} API SOURCES ===")
        validation = asyncio.run(validate_apis(api_rows, args.workers))
        valid_apis = [r for r in api_rows if validation.get(r["url"], (False, 0))[0]]
        invalid_apis = [r for r in api_rows if not validation.get(r["url"], (False, 0))[0]]
        print(f"  Valid: {len(valid_apis)}, Invalid: {len(invalid_apis)}")
        # Mark valid ones as verified
        for r in valid_apis:
            r["verified"] = 1
        # Still import invalid ones but unverified
        for r in invalid_apis:
            r["verified"] = 0

    if args.dry_run:
        print(f"\n[DRY RUN] Would import {len(api_rows) + len(portal_rows) + len(county_rows)} sources")
        return

    # Insert all
    print(f"\n=== INSERTING INTO DATABASE ===")
    imported = Counter()
    failed = 0

    with sqlite3.connect(args.db) as conn:
        # APIs first
        for r in api_rows:
            if insert_source(conn, r):
                imported[r["source_type"]] += 1
            else:
                failed += 1

        # Portals
        for r in portal_rows:
            if insert_source(conn, r):
                imported[r["source_type"]] += 1
            else:
                failed += 1

        # County/other
        for r in county_rows:
            if insert_source(conn, r):
                imported[r["source_type"]] += 1
            else:
                failed += 1

        conn.commit()

    total_imported = sum(imported.values())
    print(f"\n=== IMPORT COMPLETE ===")
    print(f"  Total imported:  {total_imported}")
    print(f"  Failed/dupes:    {failed}")
    print(f"\n  By type:")
    for t, c in imported.most_common():
        print(f"    {t:20s} {c:5d}")

    # Final DB stats
    with sqlite3.connect(args.db) as conn:
        total = conn.execute("SELECT COUNT(*) FROM api_sources").fetchone()[0]
        active = conn.execute("SELECT COUNT(*) FROM api_sources WHERE status='active'").fetchone()[0]
        ref = conn.execute("SELECT COUNT(*) FROM api_sources WHERE status='reference'").fetchone()[0]
        types = conn.execute(
            "SELECT source_type, COUNT(*) FROM api_sources GROUP BY source_type ORDER BY COUNT(*) DESC"
        ).fetchall()

    print(f"\n=== FINAL DB STATE ===")
    print(f"  Total sources:   {total}")
    print(f"  Active:          {active}")
    print(f"  Reference:       {ref}")
    print(f"\n  By type:")
    for t, c in types:
        print(f"    {t:20s} {c:5d}")


if __name__ == "__main__":
    main()
