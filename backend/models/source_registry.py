"""
API Source Registry — SQLite-backed catalog of 4,000+ construction data APIs.
Manages source metadata, sync status, health tracking, and tiered scheduling.
"""

import csv
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from models.database import get_db

logger = logging.getLogger(__name__)

# Default CSV location
DEFAULT_CSV_PATH = str(Path(__file__).parent.parent / "data" / "CONSTRUCTION_RELEVANT_APIS.csv")

# Top 50 US metro areas for T1 (daily sync) prioritization
TOP_METROS = frozenset([
    "new york", "los angeles", "chicago", "houston", "phoenix",
    "philadelphia", "san antonio", "san diego", "dallas", "austin",
    "san jose", "jacksonville", "fort worth", "columbus", "charlotte",
    "indianapolis", "san francisco", "seattle", "denver", "nashville",
    "oklahoma city", "el paso", "washington", "boston", "las vegas",
    "portland", "memphis", "louisville", "baltimore", "milwaukee",
    "albuquerque", "tucson", "fresno", "mesa", "sacramento",
    "atlanta", "kansas city", "omaha", "colorado springs", "raleigh",
    "long beach", "virginia beach", "miami", "oakland", "minneapolis",
    "tampa", "tulsa", "arlington", "new orleans", "detroit",
])


def init_source_tables():
    """Create the api_sources table if it doesn't exist."""
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS api_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,        -- Socrata, ArcGIS, CKAN, etc.
                location TEXT DEFAULT '',
                state TEXT DEFAULT '',
                data_category TEXT DEFAULT '',
                data_subcategory TEXT DEFAULT '',
                api_name TEXT DEFAULT '',
                api_url TEXT NOT NULL,
                format TEXT DEFAULT 'json',
                description TEXT DEFAULT '',
                zip_codes_covered TEXT DEFAULT '',
                geographic_scope TEXT DEFAULT '',
                -- Parsed fields
                domain TEXT DEFAULT '',            -- Socrata domain
                resource_id TEXT DEFAULT '',       -- Socrata resource ID
                query_url TEXT DEFAULT '',         -- ArcGIS query URL
                -- Scheduling
                priority_tier INTEGER DEFAULT 2,   -- 1=daily, 2=weekly, 3=monthly
                -- Health tracking
                status TEXT DEFAULT 'active',      -- active, disabled, error
                error_count INTEGER DEFAULT 0,
                last_error TEXT DEFAULT '',
                last_synced TEXT DEFAULT '',
                last_record_count INTEGER DEFAULT 0,
                -- Metadata
                is_hardcoded INTEGER DEFAULT 0,    -- 1 if matches existing hardcoded source
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_sources_tier ON api_sources(priority_tier);
            CREATE INDEX IF NOT EXISTS idx_sources_status ON api_sources(status);
            CREATE INDEX IF NOT EXISTS idx_sources_type ON api_sources(source_type);
            CREATE INDEX IF NOT EXISTS idx_sources_state ON api_sources(state);
            CREATE INDEX IF NOT EXISTS idx_sources_url ON api_sources(api_url);
        """)
        logger.info("api_sources table initialized")


def get_source_count() -> int:
    """Return total number of sources in registry."""
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM api_sources").fetchone()
        return row["cnt"] if row else 0


def import_from_csv(
    csv_path: str = DEFAULT_CSV_PATH,
    existing_socrata_ids: Optional[set] = None,
    existing_arcgis_urls: Optional[set] = None,
) -> Dict[str, int]:
    """
    Import construction-relevant APIs from CSV into the registry.
    Returns stats: {imported, skipped_duplicate, skipped_hardcoded}.
    """
    existing_socrata_ids = existing_socrata_ids or set()
    existing_arcgis_urls = existing_arcgis_urls or set()

    if not os.path.exists(csv_path):
        logger.warning(f"CSV not found: {csv_path}")
        return {"imported": 0, "skipped_duplicate": 0, "skipped_hardcoded": 0}

    stats = {"imported": 0, "skipped_duplicate": 0, "skipped_hardcoded": 0}

    with get_db() as conn:
        # Check existing URLs to avoid duplicates
        existing_urls = set()
        rows = conn.execute("SELECT api_url FROM api_sources").fetchall()
        for r in rows:
            existing_urls.add(r["api_url"])

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []

            for row in reader:
                url = (row.get("api_url") or "").strip()
                if not url:
                    continue
                if url in existing_urls:
                    stats["skipped_duplicate"] += 1
                    continue

                source_type = (row.get("source_type") or "").strip()
                location = (row.get("location") or "").strip()
                state = (row.get("state") or "").strip()
                data_cat = (row.get("data_category") or "").strip()

                # Parse source-specific fields
                domain, resource_id, query_url = "", "", ""
                is_hardcoded = 0

                if source_type == "Socrata":
                    m = re.match(r"https?://([^/]+)/resource/([^/.]+)", url)
                    if m:
                        domain = m.group(1)
                        resource_id = m.group(2)
                        if resource_id in existing_socrata_ids:
                            is_hardcoded = 1
                            stats["skipped_hardcoded"] += 1

                elif source_type == "ArcGIS":
                    query_url = url
                    if "/query" not in query_url:
                        if re.search(r"/(Feature|Map)Server(/\d+)?$", query_url):
                            query_url = query_url.rstrip("/") + "/query"
                    normalized = url.split("?")[0].rstrip("/")
                    if normalized in existing_arcgis_urls:
                        is_hardcoded = 1
                        stats["skipped_hardcoded"] += 1

                # Assign priority tier
                tier = _assign_tier(location, state, data_cat)

                batch.append((
                    source_type, location, state, data_cat,
                    (row.get("data_subcategory") or "").strip(),
                    (row.get("api_name") or "").strip(),
                    url,
                    (row.get("format") or "json").strip(),
                    (row.get("description") or "").strip(),
                    (row.get("zip_codes_covered") or "").strip(),
                    (row.get("geographic_scope") or "").strip(),
                    domain, resource_id, query_url,
                    tier, is_hardcoded,
                ))
                existing_urls.add(url)
                stats["imported"] += 1

                # Batch insert every 500 rows
                if len(batch) >= 500:
                    _insert_batch(conn, batch)
                    batch = []

            # Insert remaining
            if batch:
                _insert_batch(conn, batch)

    logger.info(
        f"CSV import complete: {stats['imported']} imported, "
        f"{stats['skipped_duplicate']} duplicate, "
        f"{stats['skipped_hardcoded']} hardcoded"
    )
    return stats


def _insert_batch(conn, batch: list):
    """Insert a batch of source records."""
    conn.executemany(
        """INSERT INTO api_sources (
            source_type, location, state, data_category, data_subcategory,
            api_name, api_url, format, description, zip_codes_covered,
            geographic_scope, domain, resource_id, query_url,
            priority_tier, is_hardcoded
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        batch,
    )


def _assign_tier(location: str, state: str, data_category: str) -> int:
    """
    Assign sync priority tier.
    T1 (daily): permits in top 50 metros
    T2 (weekly): all other permits/code enforcement
    T3 (monthly): parcel/property/insurance/other
    """
    loc_lower = location.lower().strip()
    cat_lower = data_category.lower()

    # T3: non-permit categories
    if cat_lower in ("property", "parcel", "insurance", "property_sales", "assessment"):
        return 3

    # T1: permits in top metros
    for metro in TOP_METROS:
        if metro in loc_lower:
            return 1

    # T2: everything else (permits in smaller cities, code enforcement, etc.)
    return 2


def get_sources_by_tier(tier: int, status: str = "active") -> List[dict]:
    """Get all sources for a given priority tier."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM api_sources
               WHERE priority_tier = ? AND status = ? AND is_hardcoded = 0
               ORDER BY last_synced ASC NULLS FIRST""",
            (tier, status),
        ).fetchall()
        return [dict(r) for r in rows]


def get_sources_batch(
    tier: int,
    batch_size: int = 200,
    status: str = "active",
) -> List[dict]:
    """
    Get a batch of sources to sync, prioritizing those least recently synced.
    """
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM api_sources
               WHERE priority_tier = ? AND status = ? AND is_hardcoded = 0
               ORDER BY last_synced ASC NULLS FIRST
               LIMIT ?""",
            (tier, status, batch_size),
        ).fetchall()
        return [dict(r) for r in rows]


def update_source_status(
    source_id: int,
    status: str = "active",
    error: str = "",
    record_count: int = 0,
):
    """Update a source after a sync attempt."""
    now = datetime.now().isoformat()
    with get_db() as conn:
        if status == "error":
            conn.execute(
                """UPDATE api_sources
                   SET status = ?,
                       error_count = error_count + 1,
                       last_error = ?,
                       updated_at = ?
                   WHERE id = ?""",
                (status, error, now, source_id),
            )
            # Auto-disable after 3 consecutive failures
            conn.execute(
                """UPDATE api_sources
                   SET status = 'disabled'
                   WHERE id = ? AND error_count >= 3""",
                (source_id,),
            )
        else:
            conn.execute(
                """UPDATE api_sources
                   SET status = 'active',
                       error_count = 0,
                       last_error = '',
                       last_synced = ?,
                       last_record_count = ?,
                       updated_at = ?
                   WHERE id = ?""",
                (now, record_count, now, source_id),
            )


def toggle_source(source_id: int, enabled: bool) -> bool:
    """Enable or disable a source."""
    status = "active" if enabled else "disabled"
    with get_db() as conn:
        conn.execute(
            "UPDATE api_sources SET status = ?, error_count = 0, updated_at = ? WHERE id = ?",
            (status, datetime.now().isoformat(), source_id),
        )
        return True


def get_sources_paginated(
    page: int = 1,
    per_page: int = 50,
    source_type: str = "",
    state: str = "",
    status: str = "",
    tier: Optional[int] = None,
    search: str = "",
) -> Tuple[List[dict], int]:
    """Get paginated sources with filters for the admin API."""
    conditions = []
    params = []

    if source_type:
        conditions.append("source_type = ?")
        params.append(source_type)
    if state:
        conditions.append("state = ?")
        params.append(state)
    if status:
        conditions.append("status = ?")
        params.append(status)
    if tier is not None:
        conditions.append("priority_tier = ?")
        params.append(tier)
    if search:
        conditions.append("(location LIKE ? OR api_name LIKE ? OR api_url LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s])

    where = " AND ".join(conditions) if conditions else "1=1"
    offset = (page - 1) * per_page

    with get_db() as conn:
        total_row = conn.execute(
            f"SELECT COUNT(*) as cnt FROM api_sources WHERE {where}", params
        ).fetchone()
        total = total_row["cnt"] if total_row else 0

        rows = conn.execute(
            f"""SELECT * FROM api_sources WHERE {where}
                ORDER BY priority_tier ASC, last_synced ASC NULLS FIRST
                LIMIT ? OFFSET ?""",
            params + [per_page, offset],
        ).fetchall()

        return [dict(r) for r in rows], total


def get_registry_stats() -> dict:
    """Get summary statistics for the admin dashboard."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) as c FROM api_sources").fetchone()["c"]
        active = conn.execute("SELECT COUNT(*) as c FROM api_sources WHERE status='active'").fetchone()["c"]
        disabled = conn.execute("SELECT COUNT(*) as c FROM api_sources WHERE status='disabled'").fetchone()["c"]
        errored = conn.execute("SELECT COUNT(*) as c FROM api_sources WHERE status='error'").fetchone()["c"]

        by_type = {}
        for row in conn.execute("SELECT source_type, COUNT(*) as c FROM api_sources GROUP BY source_type").fetchall():
            by_type[row["source_type"]] = row["c"]

        by_tier = {}
        for row in conn.execute("SELECT priority_tier, COUNT(*) as c FROM api_sources GROUP BY priority_tier").fetchall():
            by_tier[f"tier_{row['priority_tier']}"] = row["c"]

        by_state = conn.execute(
            "SELECT state, COUNT(*) as c FROM api_sources WHERE state != '' GROUP BY state ORDER BY c DESC LIMIT 20"
        ).fetchall()

        return {
            "total": total,
            "active": active,
            "disabled": disabled,
            "errored": errored,
            "by_type": by_type,
            "by_tier": by_tier,
            "top_states": {r["state"]: r["c"] for r in by_state},
        }
