"""
Onsite Database Layer
SQLite-backed storage replacing the JSON file cache.
Supports indexed queries, pagination, deduplication, and saved filters.
"""

import sqlite3
import json
import os
import logging
from datetime import datetime
from typing import List, Optional, Dict, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "leads.db")


def _db_path():
    """Resolve database path relative to backend dir."""
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(base), "leads.db")


@contextmanager
def get_db():
    """Context manager for database connections with WAL mode."""
    conn = sqlite3.connect(_db_path(), timeout=60)  # 60s timeout to survive lock contention
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB page cache
    conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O
    conn.execute("PRAGMA busy_timeout=60000")  # 60s retry on SQLITE_BUSY
    conn.execute("PRAGMA wal_autocheckpoint=1000")  # checkpoint every 1000 pages
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize database schema with all required tables and indexes."""
    with get_db() as conn:
        conn.executescript("""
            -- Main leads table (replaces JSON cache)
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY,
                permit_number TEXT DEFAULT '',
                address TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT DEFAULT '',
                zip TEXT DEFAULT '',
                lat REAL DEFAULT 0,
                lng REAL DEFAULT 0,
                work_description TEXT DEFAULT '',
                description_full TEXT DEFAULT '',
                permit_type TEXT DEFAULT '',
                valuation REAL DEFAULT 0,
                issue_date TEXT DEFAULT '',
                days_old INTEGER DEFAULT 0,
                score INTEGER DEFAULT 0,
                temperature TEXT DEFAULT 'cold',
                source TEXT DEFAULT '',
                contractor_name TEXT DEFAULT '',
                contractor_phone TEXT DEFAULT '',
                description_extra TEXT DEFAULT '',
                owner_name TEXT DEFAULT '',
                owner_phone TEXT DEFAULT '',
                owner_email TEXT DEFAULT '',
                owner_address TEXT DEFAULT '',
                apn TEXT DEFAULT '',
                permit_url TEXT DEFAULT '',
                market_value REAL,
                year_built INTEGER,
                square_feet REAL,
                lot_size REAL,
                bedrooms INTEGER,
                bathrooms REAL,
                zoning TEXT DEFAULT '',
                -- Pipeline fields
                stage TEXT DEFAULT 'new',
                notes TEXT DEFAULT '',
                tags TEXT DEFAULT '[]',
                contacted_at TEXT,
                -- Enrichment tracking
                enrichment_status TEXT DEFAULT 'pending',
                enrichment_date TEXT,
                -- AI scoring
                readiness_score REAL,
                recommended_action TEXT,
                contact_window_days INTEGER,
                urgency_level TEXT,
                stage_index INTEGER DEFAULT 0,
                budget_range TEXT DEFAULT '[]',
                competition_level TEXT,
                -- Metadata
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                dedup_key TEXT DEFAULT '',
                is_active INTEGER DEFAULT 1
            );

            -- Performance indexes
            CREATE INDEX IF NOT EXISTS idx_leads_city ON leads(city);
            CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(score DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_temperature ON leads(temperature);
            CREATE INDEX IF NOT EXISTS idx_leads_issue_date ON leads(issue_date DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_stage ON leads(stage);
            CREATE INDEX IF NOT EXISTS idx_leads_enrichment ON leads(enrichment_status);
            CREATE INDEX IF NOT EXISTS idx_leads_dedup ON leads(dedup_key);
            CREATE INDEX IF NOT EXISTS idx_leads_lat_lng ON leads(lat, lng);
            CREATE INDEX IF NOT EXISTS idx_leads_state ON leads(state);
            CREATE INDEX IF NOT EXISTS idx_leads_owner ON leads(owner_name);
            CREATE INDEX IF NOT EXISTS idx_leads_active ON leads(is_active);
            CREATE INDEX IF NOT EXISTS idx_leads_created_at ON leads(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_owner_phone ON leads(owner_phone);
            CREATE INDEX IF NOT EXISTS idx_leads_owner_email ON leads(owner_email);
            CREATE INDEX IF NOT EXISTS idx_leads_city_score ON leads(city, score DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_active_temp ON leads(is_active, temperature);
            CREATE INDEX IF NOT EXISTS idx_leads_active_score ON leads(is_active, score DESC);
            CREATE INDEX IF NOT EXISTS idx_leads_bbox ON leads(is_active, lat, lng);

            -- Saved filters
            CREATE TABLE IF NOT EXISTS saved_filters (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                filters TEXT NOT NULL,  -- JSON
                notify INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                user_id TEXT DEFAULT 'default'
            );

            -- Pipeline history (track stage transitions)
            CREATE TABLE IF NOT EXISTS pipeline_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                from_stage TEXT,
                to_stage TEXT NOT NULL,
                notes TEXT DEFAULT '',
                timestamp TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            -- Lead notes
            CREATE TABLE IF NOT EXISTS lead_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                note TEXT NOT NULL,
                tag TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            -- Sync metadata
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                leads_fetched INTEGER DEFAULT 0,
                leads_new INTEGER DEFAULT 0,
                leads_updated INTEGER DEFAULT 0,
                leads_deduped INTEGER DEFAULT 0,
                duration_ms INTEGER DEFAULT 0,
                status TEXT DEFAULT 'success',
                error TEXT,
                timestamp TEXT DEFAULT (datetime('now'))
            );

            -- API source registry (4,000+ construction data APIs)
            CREATE TABLE IF NOT EXISTS api_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
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
                domain TEXT DEFAULT '',
                resource_id TEXT DEFAULT '',
                query_url TEXT DEFAULT '',
                priority_tier INTEGER DEFAULT 2,
                status TEXT DEFAULT 'active',
                error_count INTEGER DEFAULT 0,
                last_error TEXT DEFAULT '',
                last_synced TEXT DEFAULT '',
                last_record_count INTEGER DEFAULT 0,
                is_hardcoded INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_sources_tier ON api_sources(priority_tier);
            CREATE INDEX IF NOT EXISTS idx_sources_status ON api_sources(status);
            CREATE INDEX IF NOT EXISTS idx_sources_type ON api_sources(source_type);
            CREATE INDEX IF NOT EXISTS idx_sources_state ON api_sources(state);
            CREATE INDEX IF NOT EXISTS idx_sources_url ON api_sources(api_url);

        """)

        # Add extended columns to api_sources (idempotent ALTER TABLE)
        _new_cols = [
            ("date_field", "TEXT DEFAULT ''"),
            ("lookback_days", "INTEGER DEFAULT 60"),
            ("field_mappings", "TEXT DEFAULT '{}'"),
            ("auth_type", "TEXT DEFAULT 'none'"),
            ("rate_limit", "TEXT DEFAULT ''"),
            ("verified", "INTEGER DEFAULT 0"),
            ("source_file", "TEXT DEFAULT ''"),
            ("sample_fields", "TEXT DEFAULT ''"),
            ("record_count", "INTEGER DEFAULT 0"),
        ]
        for col_name, col_def in _new_cols:
            try:
                conn.execute(f"ALTER TABLE api_sources ADD COLUMN {col_name} {col_def}")
            except Exception:
                pass  # Column already exists

        logger.info("Database initialized successfully")


def query_leads_for_map(limit: int = 50000) -> List[list]:
    """Lightweight query returning only map-essential fields.
    Returns list of [id, lat, lng, pri_int, score, city] arrays.
    pri_int: 0=hot, 1=warm, 2=med, 3=cold
    Capped at `limit` rows, ordered by score DESC to show best leads first.
    """
    _PRI_MAP = {"hot": 0, "warm": 1, "med": 2, "cold": 3}
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, lat, lng, temperature, score, city
               FROM leads
               WHERE is_active = 1
                 AND lat != 0 AND lng != 0
                 AND lat IS NOT NULL AND lng IS NOT NULL
                 AND (issue_date = '' OR issue_date >= date('now', '-' || ? || ' days'))
               ORDER BY score DESC
               LIMIT ?""",
            (os.getenv("LEAD_VISIBILITY_DAYS", "60"), limit)
        ).fetchall()
        points = []
        for row in rows:
            try:
                lat_f = float(row["lat"])
                lng_f = float(row["lng"])
            except (ValueError, TypeError):
                continue
            if lat_f == 0.0 and lng_f == 0.0:
                continue
            temp = str(row["temperature"] or "med").lower()
            pri_int = _PRI_MAP.get(temp, 2)
            points.append([
                str(row["id"]),
                lat_f,
                lng_f,
                pri_int,
                int(row["score"] or 50),
                str(row["city"] or ""),
            ])
        return points


def normalize_address(address: str, city: str) -> str:
    """Create a dedup key from address + city."""
    import re
    addr = address.upper().strip()
    # Normalize common abbreviations
    replacements = {
        r'\bSTREET\b': 'ST', r'\bAVENUE\b': 'AVE', r'\bBOULEVARD\b': 'BLVD',
        r'\bDRIVE\b': 'DR', r'\bROAD\b': 'RD', r'\bLANE\b': 'LN',
        r'\bCOURT\b': 'CT', r'\bPLACE\b': 'PL', r'\bCIRCLE\b': 'CIR',
        r'\bNORTH\b': 'N', r'\bSOUTH\b': 'S', r'\bEAST\b': 'E', r'\bWEST\b': 'W',
        r'\bSUITE\b': 'STE', r'\bAPARTMENT\b': 'APT', r'\bUNIT\b': 'UNIT',
        r'\.': '', r',': '', r'#': '',
    }
    for pattern, repl in replacements.items():
        addr = re.sub(pattern, repl, addr)
    addr = re.sub(r'\s+', ' ', addr).strip()
    city_norm = city.upper().strip()
    return f"{addr}|{city_norm}"


def upsert_lead(lead: dict, conn=None) -> Tuple[int, bool]:
    """
    Insert or update a lead. Returns (lead_id, is_new).
    Uses dedup_key to detect duplicates. If duplicate found,
    updates with higher-quality data (more filled fields).
    """
    def _do(c):
        dedup_key = normalize_address(
            lead.get("address", ""),
            lead.get("city", "")
        )
        lead["dedup_key"] = dedup_key
        lead["updated_at"] = datetime.now().isoformat()

        # Check for existing
        existing = c.execute(
            "SELECT id, score, owner_name, owner_phone, owner_email FROM leads WHERE dedup_key = ?",
            (dedup_key,)
        ).fetchone()

        if existing:
            # Merge: keep higher score, fill empty fields
            lead_id = existing["id"]
            updates = {}
            # Only update fields that are better in the new data
            if lead.get("score", 0) > (existing["score"] or 0):
                updates["score"] = lead["score"]
                updates["temperature"] = lead.get("temperature", "cold")
                updates["days_old"] = lead.get("days_old", 0)
            # Fill empty owner fields
            if lead.get("owner_name") and not existing["owner_name"]:
                updates["owner_name"] = lead["owner_name"]
            if lead.get("owner_phone") and not existing["owner_phone"]:
                updates["owner_phone"] = lead["owner_phone"]
            if lead.get("owner_email") and not existing["owner_email"]:
                updates["owner_email"] = lead["owner_email"]
            # Always update valuation if higher
            if lead.get("valuation", 0) > 0:
                updates["valuation"] = lead["valuation"]
            updates["updated_at"] = datetime.now().isoformat()

            if updates:
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                values = list(updates.values()) + [lead_id]
                c.execute(f"UPDATE leads SET {set_clause} WHERE id = ?", values)
            return lead_id, False

        else:
            # Insert new lead
            columns = [
                "permit_number", "address", "city", "state", "zip",
                "lat", "lng", "work_description", "description_full",
                "permit_type", "valuation", "issue_date", "days_old",
                "score", "temperature", "source", "contractor_name",
                "contractor_phone", "description_extra", "owner_name",
                "owner_phone", "owner_email", "owner_address", "apn",
                "permit_url", "dedup_key", "stage", "enrichment_status",
                "market_value", "year_built", "square_feet", "zoning",
                "state_index", "readiness_score", "competition_level",
                "updated_at", "created_at"
            ]
            # Build values, defaulting missing fields
            vals = []
            valid_cols = []
            for col in columns:
                if col in lead:
                    valid_cols.append(col)
                    vals.append(lead[col])
                elif col == "created_at":
                    valid_cols.append(col)
                    vals.append(datetime.now().isoformat())
                elif col == "updated_at":
                    valid_cols.append(col)
                    vals.append(datetime.now().isoformat())
                elif col == "enrichment_status":
                    valid_cols.append(col)
                    vals.append("pending")
                elif col == "stage":
                    valid_cols.append(col)
                    vals.append("new")

            placeholders = ", ".join("?" for _ in vals)
            col_str = ", ".join(valid_cols)
            cursor = c.execute(f"INSERT INTO leads ({col_str}) VALUES ({placeholders})", vals)
            return cursor.lastrowid, True

    if conn:
        return _do(conn)
    with get_db() as c:
        return _do(c)


def bulk_upsert_leads(leads: List[dict]) -> Dict[str, int]:
    """Bulk upsert leads with deduplication. Returns stats."""
    stats = {"new": 0, "updated": 0, "deduped": 0, "total": len(leads)}
    with get_db() as conn:
        for lead in leads:
            try:
                lead_id, is_new = upsert_lead(lead, conn=conn)
                if is_new:
                    stats["new"] += 1
                else:
                    stats["updated"] += 1
                    stats["deduped"] += 1
            except Exception as e:
                logger.error(f"Error upserting lead: {e}")
    return stats


def query_leads(
    limit: int = 500,
    offset: int = 0,
    city: Optional[str] = None,
    state: Optional[str] = None,
    temperature: Optional[str] = None,
    min_score: Optional[int] = None,
    max_days: Optional[int] = None,
    permit_type: Optional[str] = None,
    source: Optional[str] = None,
    stage: Optional[str] = None,
    contact_only: bool = False,
    bbox: Optional[Tuple[float, float, float, float]] = None,
    search: Optional[str] = None,
    sort_by: str = "score",
    sort_dir: str = "DESC",
    zip_codes: Optional[List[str]] = None,
    enrichment_status: Optional[str] = None,
) -> Tuple[List[dict], int]:
    """
    Query leads with filtering, sorting, and pagination.
    Returns (leads, total_count).
    """
    conditions = ["is_active = 1"]
    params = []

    # Auto-hide leads older than LEAD_VISIBILITY_DAYS
    visibility_days = int(os.getenv("LEAD_VISIBILITY_DAYS", "60"))
    if visibility_days > 0:
        conditions.append("(issue_date = '' OR issue_date >= date('now', '-' || ? || ' days'))")
        params.append(str(visibility_days))

    if city:
        conditions.append("city LIKE ?")
        params.append(f"%{city}%")
    if state:
        conditions.append("state = ?")
        params.append(state)
    if temperature:
        conditions.append("temperature = ?")
        params.append(temperature)
    if min_score is not None:
        conditions.append("score >= ?")
        params.append(min_score)
    if max_days is not None:
        conditions.append("days_old <= ?")
        params.append(max_days)
    if permit_type:
        conditions.append("permit_type LIKE ?")
        params.append(f"%{permit_type}%")
    if source:
        conditions.append("source LIKE ?")
        params.append(f"%{source}%")
    if stage:
        conditions.append("stage = ?")
        params.append(stage)
    if contact_only:
        conditions.append("(owner_name != '' OR owner_phone != '' OR owner_email != '')")
    if bbox:
        south, west, north, east = bbox
        conditions.append("lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?")
        params.extend([south, north, west, east])
    if search:
        conditions.append(
            "(address LIKE ? OR owner_name LIKE ? OR permit_number LIKE ? OR apn LIKE ?)"
        )
        search_val = f"%{search}%"
        params.extend([search_val, search_val, search_val, search_val])
    if zip_codes:
        placeholders = ", ".join("?" for _ in zip_codes)
        conditions.append(f"zip IN ({placeholders})")
        params.extend(zip_codes)
    if enrichment_status:
        conditions.append("enrichment_status = ?")
        params.append(enrichment_status)

    where_clause = " AND ".join(conditions)

    # Validate sort_by
    valid_sorts = {"score", "valuation", "issue_date", "days_old", "city", "created_at"}
    if sort_by not in valid_sorts:
        sort_by = "score"
    sort_dir = "DESC" if sort_dir.upper() == "DESC" else "ASC"

    with get_db() as conn:
        # Get total count
        count_row = conn.execute(
            f"SELECT COUNT(*) as cnt FROM leads WHERE {where_clause}", params
        ).fetchone()
        total = count_row["cnt"] if count_row else 0

        # Get paginated results — select only UI-essential columns
        _LEAD_COLS = (
            "id, permit_number, address, city, state, zip, lat, lng, "
            "work_description, permit_type, valuation, issue_date, days_old, "
            "score, temperature, source, contractor_name, contractor_phone, "
            "owner_name, owner_phone, owner_email, owner_address, apn, "
            "permit_url, stage, enrichment_status, notes, tags, "
            "market_value, year_built, square_feet, zoning, "
            "readiness_score, competition_level, created_at, updated_at, is_active"
        )
        rows = conn.execute(
            f"SELECT {_LEAD_COLS} FROM leads WHERE {where_clause} ORDER BY {sort_by} {sort_dir} LIMIT ? OFFSET ?",
            params + [limit, offset]
        ).fetchall()

        leads = [dict(row) for row in rows]

    return leads, total


def get_lead_by_id(lead_id: int) -> Optional[dict]:
    """Get a single lead by ID."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
        return dict(row) if row else None


def update_lead(lead_id: int, updates: dict) -> bool:
    """Update specific fields on a lead."""
    updates["updated_at"] = datetime.now().isoformat()
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [lead_id]
    with get_db() as conn:
        conn.execute(f"UPDATE leads SET {set_clause} WHERE id = ?", values)
        return True


def get_stats() -> dict:
    """Get aggregate lead statistics in a single efficient query."""
    with get_db() as conn:
        # Single query replaces 7 separate COUNT queries
        agg = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN temperature = 'hot' THEN 1 ELSE 0 END) as hot,
                SUM(CASE WHEN temperature = 'warm' THEN 1 ELSE 0 END) as warm,
                SUM(CASE WHEN temperature = 'cold' THEN 1 ELSE 0 END) as cold,
                COALESCE(SUM(valuation), 0) as total_value,
                COALESCE(AVG(score), 0) as avg_score,
                SUM(CASE WHEN enrichment_status = 'enriched' THEN 1 ELSE 0 END) as enriched,
                SUM(CASE WHEN enrichment_status = 'pending' THEN 1 ELSE 0 END) as pending
            FROM leads WHERE is_active = 1
        """).fetchone()

        total = agg["total"]
        hot = agg["hot"]
        warm = agg["warm"]
        cold = agg["cold"]
        total_value = agg["total_value"]
        avg_score = agg["avg_score"]
        enriched = agg["enriched"]
        pending_count = agg["pending"]

        # City breakdown
        cities_rows = conn.execute(
            "SELECT city, COUNT(*) as cnt FROM leads "
            "WHERE is_active = 1 AND city IS NOT NULL AND city != '' "
            "GROUP BY city ORDER BY cnt DESC LIMIT 100"
        ).fetchall()
        cities = {row["city"]: row["cnt"] for row in cities_rows}

        # Source counts
        sources = conn.execute(
            "SELECT COUNT(DISTINCT source) as cnt FROM leads WHERE is_active = 1"
        ).fetchone()["cnt"]

        try:
            sources_active = conn.execute(
                "SELECT COUNT(*) as cnt FROM api_sources WHERE status = 'active'"
            ).fetchone()["cnt"]
        except Exception:
            sources_active = sources

        return {
            "total_leads": total,
            "hot_leads": hot,
            "warm_leads": warm,
            "cold_leads": cold,
            "total_value": total_value,
            "avg_score": round(avg_score, 1),
            "cities": cities,
            "sources": sources,
            "sources_active": sources_active,
            "enrichment": {
                "enriched": enriched,
                "pending": pending_count,
                "rate": round(enriched / max(total, 1) * 100, 1)
            }
        }


def get_leads_needing_enrichment(limit: int = 50) -> List[dict]:
    """Get leads that need enrichment, prioritized by owner_name availability.

    Strategy: leads WITH owner_name are enriched first (phone/email lookup works).
    Leads without owner_name go through Regrid first to get the name.
    Within each group, sorted by score then valuation.
    """
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM leads
               WHERE enrichment_status = 'pending'
                 AND is_active = 1
                 AND (owner_phone = '' OR owner_email = '')
               ORDER BY
                 CASE WHEN owner_name != '' AND owner_name != 'Pending lookup'
                      THEN 0 ELSE 1 END,
                 score DESC, valuation DESC
               LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(row) for row in rows]


def mark_enriched(lead_id: int, data: dict):
    """Mark a lead as enriched with the given data.

    Accepts both raw API field names (name, phone, email, address)
    and DB column names (owner_name, owner_phone, owner_email, owner_address).
    Also persists Regrid property data (market_value, year_built, etc.)
    when present in data["regrid_data"].
    """
    # Map common API response keys to DB column names
    field_map = {
        "name": "owner_name",
        "phone": "owner_phone",
        "email": "owner_email",
        "address": "owner_address",
        "mailing_address": "owner_address",
    }
    # Only allow known lead table columns
    allowed_cols = {
        "owner_name", "owner_phone", "owner_email", "owner_address",
        "alt_phones", "alt_emails",
        "apn", "market_value", "year_built", "square_feet", "lot_size",
        "bedrooms", "bathrooms", "zoning",
        "phone_carrier", "phone_line_type", "phone_valid", "phone_risk", "sms_email",
        "email_valid", "email_disposable",
        "beneficial_owner", "enrichment_sources",
        "enrichment_status", "enrichment_date",
        "readiness_score", "recommended_action",
        "contact_window_days", "urgency_level",
        "contractor_name", "contractor_phone", "score", "temperature",
    }
    updates = {}
    for k, v in data.items():
        if not v or k == "regrid_data":
            continue
        col = field_map.get(k, k)
        if col in allowed_cols:
            # Serialize lists/dicts to JSON strings for SQLite
            if isinstance(v, (list, dict)):
                import json as _json
                updates[col] = _json.dumps(v)
            else:
                updates[col] = v

    # Always persist enrichment fields even if data dict had them
    enrichment_fields = ["owner_name", "owner_phone", "owner_email", "owner_address"]
    for field in enrichment_fields:
        if field not in updates and data.get(field):
            updates[field] = data[field]

    # Persist Regrid property data to the lead row
    regrid = data.get("regrid_data", {})
    if regrid:
        regrid_field_map = {
            "owner_name": "owner_name",
            "owner_address": "owner_address",
            "apn": "apn",
            "market_value": "market_value",
            "year_built": "year_built",
            "square_feet": "square_feet",
            "lot_size": "lot_size",
            "bedrooms": "bedrooms",
            "bathrooms": "bathrooms",
            "zoning": "zoning",
        }
        for src_key, db_col in regrid_field_map.items():
            val = regrid.get(src_key)
            if val and db_col not in updates:
                updates[db_col] = val

    updates["enrichment_status"] = "enriched"
    updates["enrichment_date"] = datetime.now().isoformat()
    update_lead(lead_id, updates)


def mark_enrichment_failed(lead_id: int):
    """Mark enrichment as failed for a lead."""
    update_lead(lead_id, {"enrichment_status": "failed"})


# ============================================================================
# SAVED FILTERS
# ============================================================================

def save_filter(name: str, filters: dict, notify: bool = False, user_id: str = "default") -> str:
    """Save a filter preset. Returns the filter ID."""
    import uuid
    filter_id = str(uuid.uuid4())[:8]
    with get_db() as conn:
        conn.execute(
            "INSERT INTO saved_filters (id, name, filters, notify, user_id) VALUES (?, ?, ?, ?, ?)",
            (filter_id, name, json.dumps(filters), 1 if notify else 0, user_id)
        )
    return filter_id


def get_saved_filters(user_id: str = "default") -> List[dict]:
    """Get all saved filters for a user."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM saved_filters WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,)
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["filters"] = json.loads(d["filters"])
            result.append(d)
        return result


def delete_saved_filter(filter_id: str) -> bool:
    """Delete a saved filter."""
    with get_db() as conn:
        conn.execute("DELETE FROM saved_filters WHERE id = ?", (filter_id,))
        return True


# ============================================================================
# PIPELINE HISTORY
# ============================================================================

def save_stage_transition(lead_id: int, from_stage: str, to_stage: str, notes: str = ""):
    """Record a pipeline stage transition."""
    with get_db() as conn:
        conn.execute(
            "INSERT INTO pipeline_history (lead_id, from_stage, to_stage, notes) VALUES (?, ?, ?, ?)",
            (lead_id, from_stage, to_stage, notes)
        )
        conn.execute(
            "UPDATE leads SET stage = ?, updated_at = ? WHERE id = ?",
            (to_stage, datetime.now().isoformat(), lead_id)
        )


def get_pipeline_history(lead_id: int) -> List[dict]:
    """Get stage transition history for a lead."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM pipeline_history WHERE lead_id = ? ORDER BY timestamp DESC",
            (lead_id,)
        ).fetchall()
        return [dict(row) for row in rows]


# ============================================================================
# NOTIFICATIONS
# ============================================================================

def create_notification(type: str, lead_id: int, message: str, data: dict = None):
    """Create a notification for WebSocket delivery."""
    with get_db() as conn:
        conn.execute(
            "INSERT INTO notifications (type, lead_id, message, data) VALUES (?, ?, ?, ?)",
            (type, lead_id, message, json.dumps(data or {}))
        )


def get_unread_notifications(limit: int = 50) -> List[dict]:
    """Get unread notifications."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM notifications WHERE read = 0 ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["data"] = json.loads(d["data"])
            result.append(d)
        return result


def mark_notifications_read(ids: List[int]):
    """Mark notifications as read."""
    if not ids:
        return
    placeholders = ", ".join("?" for _ in ids)
    with get_db() as conn:
        conn.execute(f"UPDATE notifications SET read = 1 WHERE id IN ({placeholders})", ids)


# ============================================================================
# SYNC LOG
# ============================================================================

def log_sync(source: str, fetched: int, new: int, updated: int, deduped: int,
             duration_ms: int, status: str = "success", error: str = None):
    """Log a sync operation."""
    with get_db() as conn:
        conn.execute(
            """INSERT INTO sync_log (source, leads_fetched, leads_new, leads_updated, 
               leads_deduped, duration_ms, status, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (source, fetched, new, updated, deduped, duration_ms, status, error)
        )


def get_sync_history(limit: int = 20) -> List[dict]:
    """Get recent sync history."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM sync_log ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(row) for row in rows]


# ============================================================================
# MIGRATION: JSON CACHE → SQLite
# ============================================================================

def migrate_from_json_cache(cache_file: str) -> int:
    """
    One-time migration: read leads from JSON cache file and insert into SQLite.
    Returns number of leads migrated.
    """
    if not os.path.exists(cache_file):
        logger.info("No JSON cache file found — skipping migration")
        return 0

    with get_db() as conn:
        # Check if we already have leads
        count = conn.execute("SELECT COUNT(*) as cnt FROM leads").fetchone()["cnt"]
        if count > 0:
            logger.info(f"Database already has {count} leads — skipping migration")
            return 0

    try:
        with open(cache_file, "r") as f:
            data = json.load(f)
        leads = data.get("leads", data if isinstance(data, list) else [])
        logger.info(f"Migrating {len(leads)} leads from JSON cache to SQLite...")

        stats = bulk_upsert_leads(leads)
        logger.info(f"Migration complete: {stats['new']} new, {stats['deduped']} deduped")

        # Rename old cache file
        backup_name = cache_file + ".migrated"
        os.rename(cache_file, backup_name)
        logger.info(f"Renamed JSON cache to {backup_name}")

        return stats["new"]
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 0
