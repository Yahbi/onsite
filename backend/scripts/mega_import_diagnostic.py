#!/usr/bin/env python3
"""
Mega Import & Diagnostic — bulk-import new API sources from CSV and
health-check every source in the database.

Usage:
    python3 mega_import_diagnostic.py --import              # import from CSV
    python3 mega_import_diagnostic.py --diagnose            # health-check all
    python3 mega_import_diagnostic.py --full                # both
    python3 mega_import_diagnostic.py --full --dry-run      # preview only
    python3 mega_import_diagnostic.py --report              # view last report
    python3 mega_import_diagnostic.py --import --include-portals  # also store portals
"""

import argparse
import asyncio
import csv
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urlparse

import aiohttp


# ---------------------------------------------------------------------------
# Immutable data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CsvRow:
    state_full: str
    state: str
    jurisdiction: str
    source_name: str
    url: str
    data_format: str
    permit_types: str
    access_type: str
    platform: str
    source_type: str
    domain: str
    resource_id: str
    is_queryable: bool


@dataclass(frozen=True)
class ValidationResult:
    csv_row: CsvRow
    valid: bool
    status_code: int
    response_time_ms: float
    error_message: str
    record_count: int
    query_url: str


@dataclass(frozen=True)
class DiagnosticResult:
    source_id: int
    source_type: str
    api_url: str
    state: str
    location: str
    status_code: int
    response_time_ms: float
    error_message: str
    healthy: bool
    classification: str  # healthy, slow, error, timeout, unreachable


@dataclass(frozen=True)
class ImportSummary:
    total_csv_rows: int
    separator_rows_skipped: int
    no_api_skipped: int
    portal_sources: int
    portals_stored: int
    queryable_candidates: int
    already_in_db: int
    validation_passed: int
    validation_failed: int
    newly_imported: int


@dataclass(frozen=True)
class DiagnosticSummary:
    total_checked: int
    healthy: int
    slow: int
    error_4xx_5xx: int
    timeout: int
    unreachable: int
    newly_disabled: int
    re_enable_candidates: int
    avg_response_ms: float


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
    "NATIONAL": "US",
}

# Regex for Socrata resource IDs
_SOCRATA_RID = re.compile(r"([a-z0-9]{4}-[a-z0-9]{4})")
_SOCRATA_URL = re.compile(
    r"https?://([^/]+)/resource/([a-z0-9]{4}-[a-z0-9]{4})"
)

# ---------------------------------------------------------------------------
# CSV parsing + source-type classification
# ---------------------------------------------------------------------------

def _detect_source_type(platform: str, data_format: str, url: str) -> str:
    """Classify a source's type from CSV fields."""
    plat = platform.lower()
    fmt = data_format.lower()
    url_l = url.lower()

    # Socrata
    if "socrata" in plat or "soda" in fmt:
        return "Socrata"
    if _SOCRATA_URL.search(url_l):
        return "Socrata"

    # ArcGIS
    if "arcgis" in plat or "arcgis" in fmt:
        if "hub" in plat or "hub" in url_l:
            return "ArcGIS_Hub"
        return "ArcGIS"
    if "arcgis" in url_l or "featureserver" in url_l or "mapserver" in url_l:
        return "ArcGIS"

    # CKAN
    if "ckan" in plat or "ckan" in fmt:
        return "CKAN"
    if "/api/3/action/" in url_l:
        return "CKAN"

    # Federal REST / JSON
    if "rest api" in fmt or "json api" in fmt:
        return "Federal"
    if "census" in plat or "fred" in plat:
        return "Federal"

    return "Portal"


def _extract_socrata_parts(url: str) -> tuple:
    """Extract (domain, resource_id) from a Socrata-style URL."""
    m = _SOCRATA_URL.search(url)
    if m:
        return m.group(1), m.group(2)
    # Fallback: find resource_id anywhere + domain from urlparse
    parsed = urlparse(url)
    domain = parsed.netloc
    rid_match = _SOCRATA_RID.search(parsed.path)
    if rid_match and domain:
        return domain, rid_match.group(1)
    return domain or "", ""


def _is_queryable(source_type: str) -> bool:
    """True if this source type can be machine-queried."""
    return source_type in ("Socrata", "ArcGIS", "ArcGIS_Hub", "CKAN", "Federal")


def parse_csv(path: str) -> list:
    """Parse the CSV into a list of CsvRow frozen dataclasses."""
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            state_full = (raw.get("State") or "").strip()
            # Skip separator rows
            if state_full.startswith("==="):
                continue

            jurisdiction = (raw.get("Jurisdiction") or "").strip()
            source_name = (raw.get("Source Name") or "").strip()
            url = (raw.get("Full API Endpoint / Direct URL") or "").strip()
            data_format = (raw.get("Data Format") or "").strip()
            permit_types = (raw.get("Permit Types Covered") or "").strip()
            access_type = (raw.get("Access Type") or "").strip()
            platform = (raw.get("Platform") or "").strip()

            # Skip rows without a URL
            if not url or url.lower() == "n/a":
                continue
            # Skip county-office-only rows (no online portal)
            if data_format == "N/A" and access_type == "County office":
                continue

            state_abbr = STATE_ABBREV.get(state_full, "")
            source_type = _detect_source_type(platform, data_format, url)
            domain, resource_id = "", ""
            if source_type == "Socrata":
                domain, resource_id = _extract_socrata_parts(url)
            elif source_type in ("ArcGIS", "ArcGIS_Hub"):
                domain = urlparse(url).netloc
            elif source_type == "CKAN":
                domain = urlparse(url).netloc

            rows.append(CsvRow(
                state_full=state_full,
                state=state_abbr,
                jurisdiction=jurisdiction,
                source_name=source_name,
                url=url,
                data_format=data_format,
                permit_types=permit_types,
                access_type=access_type,
                platform=platform,
                source_type=source_type,
                domain=domain,
                resource_id=resource_id,
                is_queryable=_is_queryable(source_type),
            ))
    return rows


# ---------------------------------------------------------------------------
# URL builders (pure functions)
# ---------------------------------------------------------------------------

def build_socrata_probe(domain: str, resource_id: str) -> str:
    return f"https://{domain}/resource/{resource_id}.json?$limit=1"


def build_arcgis_probe(url: str) -> str:
    base = url.rstrip("/")
    # Ensure it ends with /query
    if "/query" not in base.lower():
        if "featureserver" in base.lower() or "mapserver" in base.lower():
            # Add /0/query if not present
            if not re.search(r"/\d+$", base):
                base += "/0"
            base += "/query"
        else:
            base += "/query"
    if "?" in base:
        extras = {}
        lower = base.lower()
        if "resultrecordcount" not in lower:
            extras["resultRecordCount"] = "1"
        if "f=" not in lower:
            extras["f"] = "json"
        if "where" not in lower:
            extras["where"] = "1=1"
        if extras:
            return f"{base}&{urlencode(extras)}"
        return base
    params = urlencode({"where": "1=1", "resultRecordCount": "1", "f": "json"})
    return f"{base}?{params}"


def build_ckan_probe(url: str) -> str:
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}limit=1"


def build_probe_url(source_type: str, url: str,
                    domain: str = "", resource_id: str = "") -> str:
    stype = source_type.lower()
    if stype == "socrata" and domain and resource_id:
        return build_socrata_probe(domain, resource_id)
    if stype in ("arcgis", "arcgis_hub"):
        return build_arcgis_probe(url)
    if stype == "ckan":
        return build_ckan_probe(url)
    return url


# ---------------------------------------------------------------------------
# Deduplication engine
# ---------------------------------------------------------------------------

def load_existing_keys(db_path: str) -> set:
    """Load (domain, resource_id) pairs and api_url keys from existing sources."""
    keys = set()
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT domain, resource_id, api_url FROM api_sources"
        ).fetchall()
        for domain, rid, api_url in rows:
            if domain and rid:
                keys.add(("dr", str(domain).lower(), str(rid).lower()))
            if api_url:
                keys.add(("url", str(api_url).lower().rstrip("/")))
    return keys


def is_duplicate(row: CsvRow, existing: set) -> bool:
    if row.domain and row.resource_id:
        if ("dr", row.domain.lower(), row.resource_id.lower()) in existing:
            return True
    if ("url", row.url.lower().rstrip("/")) in existing:
        return True
    return False


# ---------------------------------------------------------------------------
# Async HTTP validation
# ---------------------------------------------------------------------------

PROBE_TIMEOUT = 15
USER_AGENT = "Onsite/3.2 MegaImport"


async def _validate_one(session: aiohttp.ClientSession,
                        sem: asyncio.Semaphore,
                        row: CsvRow) -> ValidationResult:
    """Validate a single source via HTTP probe."""
    probe = build_probe_url(row.source_type, row.url, row.domain, row.resource_id)
    start = time.monotonic()
    try:
        async with sem:
            async with session.get(
                probe,
                timeout=aiohttp.ClientTimeout(total=PROBE_TIMEOUT),
                headers={"User-Agent": USER_AGENT},
                ssl=False,
            ) as resp:
                elapsed = (time.monotonic() - start) * 1000
                ok = 200 <= resp.status < 400
                count = 0
                if ok:
                    try:
                        body = await resp.json(content_type=None)
                        if isinstance(body, list):
                            count = len(body)
                        elif isinstance(body, dict):
                            feats = body.get("features", body.get("result", []))
                            count = len(feats) if isinstance(feats, list) else 1
                    except Exception:
                        count = 1 if ok else 0
                return ValidationResult(
                    csv_row=row, valid=ok, status_code=resp.status,
                    response_time_ms=round(elapsed, 1),
                    error_message="" if ok else f"HTTP {resp.status}",
                    record_count=count, query_url=probe,
                )
    except asyncio.TimeoutError:
        elapsed = (time.monotonic() - start) * 1000
        return ValidationResult(
            csv_row=row, valid=False, status_code=0,
            response_time_ms=round(elapsed, 1),
            error_message="Timeout", record_count=0, query_url=probe,
        )
    except Exception as exc:
        elapsed = (time.monotonic() - start) * 1000
        return ValidationResult(
            csv_row=row, valid=False, status_code=0,
            response_time_ms=round(elapsed, 1),
            error_message=str(exc)[:200], record_count=0, query_url=probe,
        )


async def validate_batch(rows: list, workers: int) -> list:
    """Validate a batch of CsvRows concurrently."""
    sem = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_validate_one(session, sem, r) for r in rows]
        results = []
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
        return results


# ---------------------------------------------------------------------------
# Import orchestrator
# ---------------------------------------------------------------------------

def _insert_source(conn: sqlite3.Connection, row: CsvRow,
                   query_url: str, verified: int, status: str) -> bool:
    """Insert a source into api_sources. Returns True on success."""
    location = f"{row.jurisdiction}, {row.state}" if row.state else row.jurisdiction
    description = (
        f"{row.permit_types or 'Building permits'} for "
        f"{row.jurisdiction}, {row.state_full}"
    )
    is_state = row.jurisdiction.lower() in ("statewide", "all", "")
    priority = 2 if is_state or row.state_full == "NATIONAL" else 3
    try:
        conn.execute("""
            INSERT INTO api_sources (
                source_type, location, state, data_category, data_subcategory,
                api_name, api_url, format, description, geographic_scope,
                domain, resource_id, query_url, priority_tier, status,
                error_count, verified, source_file, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?,
                      datetime('now'), datetime('now'))
        """, (
            row.source_type, location, row.state,
            "building_permits", "construction",
            row.source_name or f"{row.jurisdiction} Building Permits",
            row.url, "json", description,
            "state" if is_state else "city",
            row.domain, row.resource_id, query_url,
            priority, status, verified, "mega_import_2026_03",
        ))
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as exc:
        print(f"  Insert error ({row.jurisdiction}): {exc}")
        return False


async def run_import(args) -> ImportSummary:
    """Parse CSV, validate, and import new API sources."""
    print("=" * 60)
    print("  MEGA IMPORT — CSV -> api_sources")
    print("=" * 60)

    # 1. Parse CSV
    print(f"\nParsing {args.csv} ...")
    all_rows = parse_csv(args.csv)
    total_csv = sum(1 for _ in open(args.csv)) - 1  # rough line count minus header

    queryable = [r for r in all_rows if r.is_queryable]
    portals = [r for r in all_rows if not r.is_queryable]
    separator_count = total_csv - len(all_rows) - (total_csv - len(all_rows) - len(portals))
    no_api = total_csv - len(all_rows)

    print(f"  Total rows parsed:       {len(all_rows)}")
    print(f"  API-queryable:           {len(queryable)}")
    print(f"  Web portals:             {len(portals)}")
    print(f"  Skipped (no API/sep):    {no_api}")

    # 2. Dedup queryable against existing DB
    print(f"\nDeduplicating against existing DB ...")
    existing = load_existing_keys(args.db)
    new_queryable = [r for r in queryable if not is_duplicate(r, existing)]
    dupes = len(queryable) - len(new_queryable)
    print(f"  Already in DB:           {dupes}")
    print(f"  New to validate:         {len(new_queryable)}")

    # 3. Validate new queryable sources
    validated = 0
    failed_val = 0
    imported = 0
    portals_stored = 0

    if new_queryable:
        print(f"\nValidating {len(new_queryable)} new sources "
              f"({args.workers} workers, {PROBE_TIMEOUT}s timeout) ...")
        results = await validate_batch(new_queryable, args.workers)
        passed = [r for r in results if r.valid]
        failed = [r for r in results if not r.valid]
        validated = len(passed)
        failed_val = len(failed)

        print(f"  Validation passed:       {validated}")
        print(f"  Validation failed:       {failed_val}")

        if failed:
            print(f"\n  Sample failures:")
            for r in failed[:10]:
                print(f"    {r.csv_row.jurisdiction}: {r.error_message} "
                      f"({r.response_time_ms:.0f}ms)")

        # 4. Insert validated sources
        if not args.dry_run and passed:
            print(f"\nInserting {len(passed)} validated sources ...")
            with sqlite3.connect(args.db) as conn:
                for r in passed:
                    ok = _insert_source(conn, r.csv_row, r.query_url,
                                        verified=1, status="active")
                    if ok:
                        imported += 1
                conn.commit()
            print(f"  Newly imported:          {imported}")
        elif args.dry_run:
            print(f"\n  [DRY RUN] Would import {validated} sources")
            imported = 0
    else:
        print("  No new queryable sources to import.")

    # 5. Optionally store portal sources
    if args.include_portals and portals:
        new_portals = [r for r in portals if not is_duplicate(r, existing)]
        print(f"\nStoring {len(new_portals)} portal sources as reference ...")
        if not args.dry_run:
            with sqlite3.connect(args.db) as conn:
                for r in new_portals:
                    ok = _insert_source(conn, r, r.url,
                                        verified=0, status="reference")
                    if ok:
                        portals_stored += 1
                conn.commit()
            print(f"  Portals stored:          {portals_stored}")
        else:
            print(f"  [DRY RUN] Would store {len(new_portals)} portals")

    summary = ImportSummary(
        total_csv_rows=total_csv,
        separator_rows_skipped=0,
        no_api_skipped=no_api,
        portal_sources=len(portals),
        portals_stored=portals_stored,
        queryable_candidates=len(queryable),
        already_in_db=dupes,
        validation_passed=validated,
        validation_failed=failed_val,
        newly_imported=imported,
    )
    _print_import_summary(summary)
    return summary


def _print_import_summary(s: ImportSummary) -> None:
    print("\n" + "=" * 60)
    print("  IMPORT SUMMARY")
    print("=" * 60)
    print(f"  CSV rows processed:      {s.total_csv_rows}")
    print(f"  Skipped (no API):        {s.no_api_skipped}")
    print(f"  Portal sources found:    {s.portal_sources}")
    print(f"  Portals stored:          {s.portals_stored}")
    print(f"  Queryable candidates:    {s.queryable_candidates}")
    print(f"  Already in DB:           {s.already_in_db}")
    print(f"  Validation passed:       {s.validation_passed}")
    print(f"  Validation failed:       {s.validation_failed}")
    print(f"  Newly imported:          {s.newly_imported}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Diagnostic orchestrator
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _DbSource:
    id: int
    source_type: str
    api_url: str
    domain: str
    resource_id: str
    query_url: str
    status: str
    error_count: int
    state: str
    location: str


DIAG_TIMEOUT = 15
SLOW_THRESHOLD_MS = 5000.0
DISABLE_AFTER_ERRORS = 5


def _ensure_health_table(db_path: str) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS api_health_checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id INTEGER NOT NULL,
        status_code INTEGER DEFAULT 0,
        response_time_ms REAL DEFAULT 0,
        error_message TEXT DEFAULT '',
        checked_at TEXT NOT NULL,
        healthy INTEGER DEFAULT 0,
        FOREIGN KEY (source_id) REFERENCES api_sources(id)
    )
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(ddl)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_health_source "
            "ON api_health_checks(source_id, checked_at)"
        )
        conn.commit()


def _fetch_all_sources(db_path: str, limit: Optional[int] = None) -> tuple:
    query = (
        "SELECT id, source_type, api_url, domain, resource_id, "
        "query_url, status, error_count, state, location FROM api_sources "
        "ORDER BY priority_tier ASC, id ASC"
    )
    params = []
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()
    return tuple(_DbSource(**dict(r)) for r in rows)


def _classify_result(status_code: int, response_time_ms: float,
                     error_message: str) -> str:
    if "timeout" in error_message.lower() or "Timeout" in error_message:
        return "timeout"
    if status_code == 0:
        return "unreachable"
    if 200 <= status_code < 400:
        if response_time_ms > SLOW_THRESHOLD_MS:
            return "slow"
        return "healthy"
    return "error"


async def _probe_one(session: aiohttp.ClientSession,
                     sem: asyncio.Semaphore,
                     src: _DbSource) -> DiagnosticResult:
    """Probe a single source for the diagnostic."""
    probe = build_probe_url(src.source_type, src.api_url,
                            src.domain, src.resource_id)
    # Use query_url if available and different
    if src.query_url and src.query_url != src.api_url:
        probe = src.query_url
        # Ensure minimal params for ArcGIS query URLs
        if src.source_type.lower() in ("arcgis", "arcgis_hub"):
            probe = build_arcgis_probe(probe)

    now_str = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()
    try:
        async with sem:
            async with session.get(
                probe,
                timeout=aiohttp.ClientTimeout(total=DIAG_TIMEOUT),
                headers={"User-Agent": "Onsite/3.2 Diagnostic"},
                ssl=False,
            ) as resp:
                elapsed = (time.monotonic() - start) * 1000
                ok = 200 <= resp.status < 400
                err = "" if ok else f"HTTP {resp.status}"
                cls = _classify_result(resp.status, elapsed, err)
                return DiagnosticResult(
                    source_id=src.id, source_type=src.source_type,
                    api_url=src.api_url, state=src.state, location=src.location,
                    status_code=resp.status,
                    response_time_ms=round(elapsed, 1),
                    error_message=err, healthy=ok, classification=cls,
                )
    except asyncio.TimeoutError:
        elapsed = (time.monotonic() - start) * 1000
        return DiagnosticResult(
            source_id=src.id, source_type=src.source_type,
            api_url=src.api_url, state=src.state, location=src.location,
            status_code=0, response_time_ms=round(elapsed, 1),
            error_message="Timeout", healthy=False, classification="timeout",
        )
    except Exception as exc:
        elapsed = (time.monotonic() - start) * 1000
        return DiagnosticResult(
            source_id=src.id, source_type=src.source_type,
            api_url=src.api_url, state=src.state, location=src.location,
            status_code=0, response_time_ms=round(elapsed, 1),
            error_message=str(exc)[:200], healthy=False,
            classification="unreachable",
        )


async def run_diagnostic(args) -> DiagnosticSummary:
    """Health-check all sources in the database."""
    print("\n" + "=" * 60)
    print("  MEGA DIAGNOSTIC — health-check all api_sources")
    print("=" * 60)

    _ensure_health_table(args.db)
    sources = _fetch_all_sources(args.db, limit=args.limit)
    total = len(sources)
    print(f"\n  Sources to check: {total}")
    print(f"  Workers: {args.workers}, Timeout: {DIAG_TIMEOUT}s")

    if not sources:
        print("  No sources found.")
        return DiagnosticSummary(0, 0, 0, 0, 0, 0, 0, 0, 0.0)

    # Probe all sources
    sem = asyncio.Semaphore(args.workers)
    connector = aiohttp.TCPConnector(limit=args.workers, force_close=True)
    results = []
    checked = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in batches of 200 for progress reporting
        batch_size = 200
        for i in range(0, total, batch_size):
            batch = sources[i:i + batch_size]
            tasks = [_probe_one(session, sem, s) for s in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, Exception):
                    # Shouldn't happen since _probe_one catches, but safety net
                    continue
                results.append(r)
            checked += len(batch)
            healthy_so_far = sum(1 for r in results if r.healthy)
            pct = (checked / total) * 100
            print(f"  [{checked}/{total}] {pct:.0f}% done — "
                  f"{healthy_so_far} healthy so far")

    # Classify results
    healthy_results = [r for r in results if r.classification == "healthy"]
    slow_results = [r for r in results if r.classification == "slow"]
    error_results = [r for r in results if r.classification == "error"]
    timeout_results = [r for r in results if r.classification == "timeout"]
    unreachable_results = [r for r in results if r.classification == "unreachable"]

    # Persist to api_health_checks
    newly_disabled = 0
    re_enable_candidates = 0

    if not args.dry_run:
        print(f"\n  Saving {len(results)} results to api_health_checks ...")
        now_str = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(args.db) as conn:
            # Bulk insert health checks
            conn.executemany(
                "INSERT INTO api_health_checks "
                "(source_id, status_code, response_time_ms, error_message, "
                "checked_at, healthy) VALUES (?, ?, ?, ?, ?, ?)",
                [(r.source_id, r.status_code, r.response_time_ms,
                  r.error_message, now_str, int(r.healthy)) for r in results]
            )

            # Update api_sources based on results
            for r in results:
                if r.healthy:
                    conn.execute(
                        "UPDATE api_sources SET verified = 1, last_error = '', "
                        "error_count = 0, updated_at = datetime('now') "
                        "WHERE id = ?", (r.source_id,)
                    )
                else:
                    conn.execute(
                        "UPDATE api_sources SET error_count = error_count + 1, "
                        "last_error = ?, updated_at = datetime('now') "
                        "WHERE id = ?", (r.error_message, r.source_id)
                    )

            # Auto-disable sources with too many errors
            rows = conn.execute(
                "SELECT id FROM api_sources "
                "WHERE error_count >= ? AND status = 'active'",
                (DISABLE_AFTER_ERRORS,)
            ).fetchall()
            for (sid,) in rows:
                conn.execute(
                    "UPDATE api_sources SET status = 'disabled', "
                    "last_error = 'Auto-disabled: too many consecutive errors', "
                    "updated_at = datetime('now') WHERE id = ?", (sid,)
                )
                newly_disabled += 1

            # Find disabled sources that responded OK (re-enable candidates)
            healthy_ids = {r.source_id for r in results if r.healthy}
            disabled_rows = conn.execute(
                "SELECT id FROM api_sources WHERE status IN ('disabled', 'error')"
            ).fetchall()
            for (sid,) in disabled_rows:
                if sid in healthy_ids:
                    re_enable_candidates += 1
                    # Actually re-enable them
                    conn.execute(
                        "UPDATE api_sources SET status = 'active', "
                        "error_count = 0, last_error = '', verified = 1, "
                        "updated_at = datetime('now') WHERE id = ?", (sid,)
                    )

            conn.commit()
        print(f"  Newly disabled:          {newly_disabled}")
        print(f"  Re-enabled:              {re_enable_candidates}")
    else:
        print(f"\n  [DRY RUN] Would update {len(results)} sources")

    # Compute average response time
    all_times = [r.response_time_ms for r in results if r.response_time_ms > 0]
    avg_ms = sum(all_times) / len(all_times) if all_times else 0.0

    summary = DiagnosticSummary(
        total_checked=len(results),
        healthy=len(healthy_results),
        slow=len(slow_results),
        error_4xx_5xx=len(error_results),
        timeout=len(timeout_results),
        unreachable=len(unreachable_results),
        newly_disabled=newly_disabled,
        re_enable_candidates=re_enable_candidates,
        avg_response_ms=round(avg_ms, 1),
    )
    _print_diagnostic_summary(summary, results)
    return summary


def _print_diagnostic_summary(s: DiagnosticSummary, results: list) -> None:
    print("\n" + "=" * 60)
    print("  DIAGNOSTIC SUMMARY")
    print("=" * 60)
    total = s.total_checked or 1
    print(f"  Total checked:           {s.total_checked}")
    print(f"  Healthy:                 {s.healthy} ({s.healthy*100/total:.1f}%)")
    print(f"  Slow (>{SLOW_THRESHOLD_MS/1000:.0f}s):            "
          f"{s.slow} ({s.slow*100/total:.1f}%)")
    print(f"  HTTP errors (4xx/5xx):   {s.error_4xx_5xx} "
          f"({s.error_4xx_5xx*100/total:.1f}%)")
    print(f"  Timeout:                 {s.timeout} ({s.timeout*100/total:.1f}%)")
    print(f"  Unreachable:             {s.unreachable} "
          f"({s.unreachable*100/total:.1f}%)")
    print(f"  Avg response time:       {s.avg_response_ms:.0f}ms")
    print(f"  Newly disabled:          {s.newly_disabled}")
    print(f"  Re-enabled:              {s.re_enable_candidates}")

    # By source_type
    from collections import Counter
    type_health = Counter()
    type_total = Counter()
    state_health = Counter()
    state_total = Counter()
    for r in results:
        type_total[r.source_type] += 1
        state_total[r.state] += 1
        if r.healthy:
            type_health[r.source_type] += 1
            state_health[r.state] += 1

    print(f"\n  --- By Source Type ---")
    for stype, cnt in type_total.most_common():
        ok = type_health.get(stype, 0)
        pct = ok * 100 / cnt if cnt else 0
        print(f"    {stype:15s} {ok:5d}/{cnt:5d} healthy ({pct:.0f}%)")

    print(f"\n  --- Top 15 States ---")
    for state, cnt in state_total.most_common(15):
        ok = state_health.get(state, 0)
        pct = ok * 100 / cnt if cnt else 0
        print(f"    {state:5s} {ok:5d}/{cnt:5d} healthy ({pct:.0f}%)")

    # Sample failures
    failed = [r for r in results if not r.healthy]
    if failed:
        print(f"\n  --- Sample Failures (first 15) ---")
        for r in failed[:15]:
            print(f"    ID {r.source_id:5d} [{r.source_type:10s}] "
                  f"{r.classification:12s} - {r.error_message}")

    print("=" * 60)


# ---------------------------------------------------------------------------
# Report from existing health data
# ---------------------------------------------------------------------------

def run_report(db_path: str) -> None:
    """Generate a report from existing api_health_checks data."""
    print("\n" + "=" * 60)
    print("  HEALTH REPORT (from stored data)")
    print("=" * 60)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        # Overall stats
        row = conn.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN healthy = 1 THEN 1 ELSE 0 END) as healthy,
                   AVG(response_time_ms) as avg_ms,
                   MAX(checked_at) as last_check
            FROM api_health_checks
            WHERE checked_at = (SELECT MAX(checked_at) FROM api_health_checks)
        """).fetchone()

        if not row or not row["total"]:
            print("  No health check data found. Run --diagnose first.")
            return

        total = row["total"]
        healthy = row["healthy"]
        print(f"\n  Last check:    {row['last_check']}")
        print(f"  Total:         {total}")
        print(f"  Healthy:       {healthy} ({healthy*100/total:.1f}%)")
        print(f"  Failed:        {total - healthy} ({(total-healthy)*100/total:.1f}%)")
        print(f"  Avg response:  {row['avg_ms']:.0f}ms")

        # Source status overview
        status_rows = conn.execute(
            "SELECT status, COUNT(*) as cnt FROM api_sources "
            "GROUP BY status ORDER BY cnt DESC"
        ).fetchall()
        print(f"\n  --- Source Status ---")
        for r in status_rows:
            print(f"    {r['status']:15s} {r['cnt']:6d}")

        # By type
        type_rows = conn.execute("""
            SELECT s.source_type,
                   COUNT(*) as total,
                   SUM(CASE WHEN h.healthy = 1 THEN 1 ELSE 0 END) as ok
            FROM api_health_checks h
            JOIN api_sources s ON s.id = h.source_id
            WHERE h.checked_at = (SELECT MAX(checked_at) FROM api_health_checks)
            GROUP BY s.source_type ORDER BY total DESC
        """).fetchall()
        print(f"\n  --- By Source Type ---")
        for r in type_rows:
            pct = r["ok"] * 100 / r["total"] if r["total"] else 0
            print(f"    {r['source_type']:15s} {r['ok']:5d}/{r['total']:5d} "
                  f"healthy ({pct:.0f}%)")

        # Recently disabled
        disabled = conn.execute(
            "SELECT id, source_type, api_name, location, last_error "
            "FROM api_sources WHERE status = 'disabled' "
            "ORDER BY updated_at DESC LIMIT 20"
        ).fetchall()
        if disabled:
            print(f"\n  --- Recently Disabled (top 20) ---")
            for r in disabled:
                print(f"    ID {r['id']:5d} [{r['source_type']:10s}] "
                      f"{r['api_name'][:40]} - {r['last_error'][:50]}")

    print("=" * 60)


# ---------------------------------------------------------------------------
# DB path resolver
# ---------------------------------------------------------------------------

def find_db_path() -> str:
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir.parent / "leads.db",
        script_dir.parent / "data" / "leads.db",
        script_dir.parent / "data" / "onsite.db",
        Path("leads.db"),
        Path("backend/leads.db"),
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    raise FileNotFoundError(
        "Cannot find leads.db. Use --db to specify the path."
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mega Import & Diagnostic for Onsite API sources"
    )
    parser.add_argument("--import", dest="do_import", action="store_true",
                        help="Import new sources from CSV")
    parser.add_argument("--diagnose", action="store_true",
                        help="Health-check all existing sources")
    parser.add_argument("--full", action="store_true",
                        help="Run both import and diagnostic")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without DB changes")
    parser.add_argument("--report", action="store_true",
                        help="Show report from last diagnostic")
    parser.add_argument("--workers", type=int, default=30,
                        help="Concurrent workers (default: 30)")
    parser.add_argument("--csv", default=str(
        Path.home() / "Desktop" / "US_Construction_Permit_APIs_Complete.csv"),
                        help="Path to CSV file")
    parser.add_argument("--db", default=None,
                        help="Path to SQLite database")
    parser.add_argument("--include-portals", action="store_true",
                        help="Also store web portal sources as reference")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit sources for diagnostic")
    args = parser.parse_args()

    # Resolve DB path
    if args.db is None:
        args.db = find_db_path()
    print(f"Database: {args.db}")

    if args.report:
        run_report(args.db)
        return

    if not (args.do_import or args.diagnose or args.full):
        parser.print_help()
        print("\nSpecify --import, --diagnose, --full, or --report")
        return

    if args.full or args.do_import:
        asyncio.run(run_import(args))

    if args.full or args.diagnose:
        asyncio.run(run_diagnostic(args))

    print("\nDone.")


if __name__ == "__main__":
    main()
