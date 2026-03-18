"""
Onsite - Parcel Data Service
Nationwide parcel boundary and property data via Regrid API v2
All lookups are persisted to SQLite for easy extraction.
"""

import os
import json
import sqlite3
import logging
import aiohttp
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database helpers (regrid_cache table)
# ---------------------------------------------------------------------------

def _db_path():
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(os.path.dirname(base), "leads.db")


@contextmanager
def _get_db():
    conn = sqlite3.connect(_db_path(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_regrid_cache():
    """Create the regrid_cache table if it doesn't exist."""
    with _get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS regrid_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lookup_key TEXT NOT NULL UNIQUE,
                lookup_type TEXT NOT NULL,
                ll_uuid TEXT DEFAULT '',
                parcelnumb TEXT DEFAULT '',
                apn TEXT DEFAULT '',
                address TEXT DEFAULT '',
                city TEXT DEFAULT '',
                state TEXT DEFAULT '',
                zip TEXT DEFAULT '',
                county TEXT DEFAULT '',
                owner_name TEXT DEFAULT '',
                owner_first TEXT DEFAULT '',
                owner_last TEXT DEFAULT '',
                owner_type TEXT DEFAULT '',
                mail_address TEXT DEFAULT '',
                mail_city TEXT DEFAULT '',
                mail_state TEXT DEFAULT '',
                mail_zip TEXT DEFAULT '',
                land_use TEXT DEFAULT '',
                use_description TEXT DEFAULT '',
                zoning TEXT DEFAULT '',
                zoning_description TEXT DEFAULT '',
                lot_size_sqft REAL,
                lot_size_acres REAL,
                building_sqft REAL,
                year_built INTEGER,
                num_stories INTEGER,
                num_units INTEGER,
                num_bedrooms INTEGER,
                num_bathrooms REAL,
                struct_style TEXT DEFAULT '',
                parcel_value REAL,
                land_value REAL,
                improvement_value REAL,
                sale_price REAL,
                sale_date TEXT DEFAULT '',
                tax_amount REAL,
                tax_year INTEGER,
                fema_flood_zone TEXT DEFAULT '',
                centroid_lat REAL,
                centroid_lng REAL,
                geometry_json TEXT DEFAULT '',
                raw_fields TEXT DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_regrid_lookup ON regrid_cache(lookup_key);
            CREATE INDEX IF NOT EXISTS idx_regrid_address ON regrid_cache(address, city, state);
            CREATE INDEX IF NOT EXISTS idx_regrid_owner ON regrid_cache(owner_name);
            CREATE INDEX IF NOT EXISTS idx_regrid_apn ON regrid_cache(apn);
            CREATE INDEX IF NOT EXISTS idx_regrid_state ON regrid_cache(state);
            CREATE INDEX IF NOT EXISTS idx_regrid_uuid ON regrid_cache(ll_uuid);
        """)
        logger.info("regrid_cache table initialized")


def _save_to_cache(lookup_key: str, lookup_type: str, parsed: Dict) -> int:
    """Persist a parsed Regrid result to SQLite. Returns the row id."""
    with _get_db() as conn:
        conn.execute("""
            INSERT INTO regrid_cache (
                lookup_key, lookup_type, ll_uuid, parcelnumb, apn,
                address, city, state, zip, county,
                owner_name, owner_first, owner_last, owner_type,
                mail_address, mail_city, mail_state, mail_zip,
                land_use, use_description, zoning, zoning_description,
                lot_size_sqft, lot_size_acres, building_sqft,
                year_built, num_stories, num_units, num_bedrooms, num_bathrooms,
                struct_style, parcel_value, land_value, improvement_value,
                sale_price, sale_date, tax_amount, tax_year,
                fema_flood_zone, centroid_lat, centroid_lng,
                geometry_json, raw_fields, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, datetime('now')
            )
            ON CONFLICT(lookup_key) DO UPDATE SET
                ll_uuid=excluded.ll_uuid, parcelnumb=excluded.parcelnumb, apn=excluded.apn,
                address=excluded.address, city=excluded.city, state=excluded.state,
                zip=excluded.zip, county=excluded.county,
                owner_name=excluded.owner_name, owner_first=excluded.owner_first,
                owner_last=excluded.owner_last, owner_type=excluded.owner_type,
                mail_address=excluded.mail_address, mail_city=excluded.mail_city,
                mail_state=excluded.mail_state, mail_zip=excluded.mail_zip,
                land_use=excluded.land_use, use_description=excluded.use_description,
                zoning=excluded.zoning, zoning_description=excluded.zoning_description,
                lot_size_sqft=excluded.lot_size_sqft, lot_size_acres=excluded.lot_size_acres,
                building_sqft=excluded.building_sqft,
                year_built=excluded.year_built, num_stories=excluded.num_stories,
                num_units=excluded.num_units, num_bedrooms=excluded.num_bedrooms,
                num_bathrooms=excluded.num_bathrooms, struct_style=excluded.struct_style,
                parcel_value=excluded.parcel_value, land_value=excluded.land_value,
                improvement_value=excluded.improvement_value,
                sale_price=excluded.sale_price, sale_date=excluded.sale_date,
                tax_amount=excluded.tax_amount, tax_year=excluded.tax_year,
                fema_flood_zone=excluded.fema_flood_zone,
                centroid_lat=excluded.centroid_lat, centroid_lng=excluded.centroid_lng,
                geometry_json=excluded.geometry_json, raw_fields=excluded.raw_fields,
                updated_at=datetime('now')
        """, (
            lookup_key, lookup_type,
            parsed.get("ll_uuid", ""), parsed.get("parcelnumb", ""), parsed.get("apn", ""),
            parsed.get("address", ""), parsed.get("city", ""), parsed.get("state", ""),
            parsed.get("zip", ""), parsed.get("county", ""),
            parsed.get("owner_name", ""), parsed.get("owner_first", ""),
            parsed.get("owner_last", ""), parsed.get("owner_type", ""),
            parsed.get("mail_address", ""), parsed.get("mail_city", ""),
            parsed.get("mail_state", ""), parsed.get("mail_zip", ""),
            parsed.get("land_use", ""), parsed.get("use_description", ""),
            parsed.get("zoning", ""), parsed.get("zoning_description", ""),
            parsed.get("lot_size_sqft"), parsed.get("lot_size_acres"),
            parsed.get("building_sqft"),
            parsed.get("year_built"), parsed.get("num_stories"),
            parsed.get("num_units"), parsed.get("num_bedrooms"),
            parsed.get("num_bathrooms"), parsed.get("struct_style", ""),
            parsed.get("parcel_value"), parsed.get("land_value"),
            parsed.get("improvement_value"),
            parsed.get("sale_price"), parsed.get("sale_date", ""),
            parsed.get("tax_amount"), parsed.get("tax_year"),
            parsed.get("fema_flood_zone", ""),
            parsed.get("centroid_lat"), parsed.get("centroid_lng"),
            parsed.get("geometry_json", ""),
            json.dumps(parsed.get("raw_fields", {})),
        ))
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _load_from_cache(lookup_key: str, max_age_days: int = 90) -> Optional[Dict]:
    """Load a cached Regrid result from SQLite. Returns None if stale/missing."""
    with _get_db() as conn:
        row = conn.execute(
            """SELECT * FROM regrid_cache
               WHERE lookup_key = ?
                 AND updated_at > datetime('now', ?)""",
            (lookup_key, f"-{max_age_days} days")
        ).fetchone()
        if row:
            return dict(row)
    return None


def get_all_regrid_data(
    state: str = "",
    city: str = "",
    limit: int = 1000,
    offset: int = 0
) -> List[Dict]:
    """Export saved Regrid data from cache. For easy extraction."""
    with _get_db() as conn:
        clauses = ["1=1"]
        params = []
        if state:
            clauses.append("state = ?")
            params.append(state)
        if city:
            clauses.append("city = ?")
            params.append(city)
        params.extend([limit, offset])
        rows = conn.execute(
            f"""SELECT * FROM regrid_cache
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at DESC
                LIMIT ? OFFSET ?""",
            params
        ).fetchall()
        return [dict(r) for r in rows]


def get_regrid_stats() -> Dict:
    """Get summary stats for cached Regrid data."""
    with _get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM regrid_cache").fetchone()[0]
        by_state = conn.execute(
            "SELECT state, COUNT(*) as cnt FROM regrid_cache GROUP BY state ORDER BY cnt DESC LIMIT 20"
        ).fetchall()
        return {
            "total_cached": total,
            "by_state": {r["state"]: r["cnt"] for r in by_state},
        }


# ---------------------------------------------------------------------------
# Regrid API v2 Client
# ---------------------------------------------------------------------------

class RegridParcelService:
    """
    Regrid API v2 for parcel data.
    Auth: query parameter ?token=KEY (NOT Bearer header).
    All results are persisted to regrid_cache SQLite table.

    NOTE: Coverage depends on the Regrid plan tied to the token.
    The current token covers TEXAS only.  Update `covered_states`
    if the plan is upgraded to nationwide.
    """

    # States covered by the current Regrid plan.
    # Set to None for nationwide coverage.
    covered_states: Optional[set] = {
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
        "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
        "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
        "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"
    }

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or os.getenv("REGRID_API_KEY", "")
        self.base_url = "https://app.regrid.com/api/v2"
        self.timeout = aiohttp.ClientTimeout(total=15)
        # Ensure the cache table exists on first use
        try:
            init_regrid_cache()
        except Exception as exc:
            logger.warning("regrid_cache init deferred: %s", exc)

    def _is_covered(self, state: str) -> bool:
        """Check if a state is within the plan's geographic coverage."""
        if self.covered_states is None:
            return True  # nationwide plan
        return state.upper().strip() in self.covered_states if state else False

    # ------------------------------------------------------------------
    # Public lookup methods
    # ------------------------------------------------------------------

    async def get_parcel_by_coordinates(
        self, lat: float, lng: float, state_hint: str = ""
    ) -> Dict:
        """Get parcel data by lat/lng. Cached in SQLite."""
        if not self.api_key:
            return _no_key_error()
        if state_hint and not self._is_covered(state_hint):
            return {"success": False, "error": f"State {state_hint} not in Regrid coverage"}

        lookup_key = f"point:{lat:.6f},{lng:.6f}"
        cached = _load_from_cache(lookup_key)
        if cached:
            return {**cached, "success": True, "cached": True, "data_source": "Regrid"}

        url = f"{self.base_url}/parcels/point"
        params = {"token": self.api_key, "lat": lat, "lon": lng, "limit": 1}

        result = await self._call_api(url, params, lookup_key, "point")
        return result

    async def get_parcel_by_address(
        self,
        address: str,
        city: str = "",
        state: str = "",
        zip_code: str = ""
    ) -> Dict:
        """Get parcel data by address. Cached in SQLite."""
        if not self.api_key:
            return _no_key_error()
        if not self._is_covered(state):
            return {"success": False, "error": f"State {state} not in Regrid coverage"}

        # Build query: include city + state so Regrid doesn't return wrong-state matches
        query = address
        if city:
            query += f", {city}"
        if state:
            query += f", {state}"
        if zip_code:
            query += f" {zip_code}"

        lookup_key = f"address:{query.strip().lower()}"
        cached = _load_from_cache(lookup_key)
        if cached:
            return {**cached, "success": True, "cached": True, "data_source": "Regrid"}

        url = f"{self.base_url}/parcels/address"
        params = {"token": self.api_key, "query": query, "limit": 1}

        result = await self._call_api(url, params, lookup_key, "address")

        # Validate that the result is actually in the requested state
        if result.get("success") and state:
            result_state = (result.get("state") or "").upper().strip()
            if result_state and result_state != state.upper().strip():
                logger.warning(
                    "Regrid returned %s but requested %s — discarding", result_state, state
                )
                return {"success": False, "error": "Regrid returned wrong state"}

        return result

    async def get_parcel_by_apn(self, apn: str, state: str = "", county: str = "") -> Dict:
        """Get parcel data by Assessor Parcel Number."""
        if not self.api_key:
            return _no_key_error()

        lookup_key = f"apn:{apn}:{state}:{county}".lower()
        cached = _load_from_cache(lookup_key)
        if cached:
            return {**cached, "success": True, "cached": True, "data_source": "Regrid"}

        url = f"{self.base_url}/parcels/apn"
        params = {"token": self.api_key, "parcelnumb": apn, "limit": 1}
        if state:
            params["state_abbr"] = state
        if county:
            params["county"] = county

        result = await self._call_api(url, params, lookup_key, "apn")
        return result

    async def get_parcel_by_owner(self, owner_name: str, state: str = "") -> Dict:
        """Search parcels by owner name."""
        if not self.api_key:
            return _no_key_error()

        lookup_key = f"owner:{owner_name}:{state}".strip().lower()
        cached = _load_from_cache(lookup_key, max_age_days=30)
        if cached:
            return {**cached, "success": True, "cached": True, "data_source": "Regrid"}

        url = f"{self.base_url}/parcels/owner"
        params = {"token": self.api_key, "owner": owner_name, "limit": 5}
        if state:
            params["state_abbr"] = state

        result = await self._call_api(url, params, lookup_key, "owner")
        return result

    async def enrich_lead(self, lead: Dict) -> Dict:
        """
        Enrich a lead with Regrid parcel data.
        Tries address first, falls back to coordinates.
        Returns a NEW dict with enrichment fields (never mutates input).
        """
        enrichment = {}

        address = lead.get("address", "")
        city = lead.get("city", "")
        state = lead.get("state", "")
        zip_code = lead.get("zip", "")
        lat = lead.get("lat", 0)
        lng = lead.get("lng", 0)

        result = None

        # Try address lookup first
        if address and (city or state):
            result = await self.get_parcel_by_address(address, city, state, zip_code)

        # Fall back to coordinates
        if (not result or not result.get("success")) and lat and lng:
            result = await self.get_parcel_by_coordinates(lat, lng, state_hint=state)

        if not result or not result.get("success"):
            return enrichment

        # Map Regrid fields → lead enrichment fields
        if result.get("owner_name") and not lead.get("owner_name"):
            enrichment["owner_name"] = result["owner_name"]
        if result.get("mail_address") and not lead.get("owner_address"):
            mail_parts = [
                result.get("mail_address", ""),
                result.get("mail_city", ""),
                result.get("mail_state", ""),
                result.get("mail_zip", ""),
            ]
            enrichment["owner_address"] = ", ".join(p for p in mail_parts if p)
        if result.get("apn") and not lead.get("apn"):
            enrichment["apn"] = result["apn"]
        if result.get("parcel_value") and not lead.get("market_value"):
            enrichment["market_value"] = result["parcel_value"]
        if result.get("year_built") and not lead.get("year_built"):
            enrichment["year_built"] = result["year_built"]
        if result.get("building_sqft") and not lead.get("square_feet"):
            enrichment["square_feet"] = result["building_sqft"]
        if result.get("lot_size_sqft") and not lead.get("lot_size"):
            enrichment["lot_size"] = result["lot_size_sqft"]
        if result.get("num_bedrooms") and not lead.get("bedrooms"):
            enrichment["bedrooms"] = result["num_bedrooms"]
        if result.get("num_bathrooms") and not lead.get("bathrooms"):
            enrichment["bathrooms"] = result["num_bathrooms"]
        if result.get("zoning") and not lead.get("zoning"):
            enrichment["zoning"] = result["zoning"]

        return enrichment

    # ------------------------------------------------------------------
    # Internal API call + parse + persist
    # ------------------------------------------------------------------

    async def _call_api(
        self, url: str, params: Dict, lookup_key: str, lookup_type: str
    ) -> Dict:
        """Call Regrid API, parse response, persist to SQLite."""
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        parsed = _parse_v2_response(data)
                        if parsed.get("success"):
                            _save_to_cache(lookup_key, lookup_type, parsed)
                            logger.info(
                                "Regrid %s lookup saved: %s → %s, %s",
                                lookup_type, lookup_key,
                                parsed.get("address", "?"), parsed.get("owner_name", "?")
                            )
                        return parsed
                    elif resp.status == 429:
                        return {"success": False, "error": "Rate limited by Regrid", "status": 429}
                    elif resp.status == 401:
                        return {"success": False, "error": "Regrid token invalid or expired", "status": 401}
                    else:
                        body = await resp.text()
                        logger.warning("Regrid API %s: %s", resp.status, body[:200])
                        return {"success": False, "error": body[:200], "status": resp.status}
        except aiohttp.ClientError as exc:
            logger.error("Regrid network error: %s", exc)
            return {"success": False, "error": str(exc)}
        except Exception as exc:
            logger.error("Regrid unexpected error: %s", exc)
            return {"success": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Response parser (v2 format)
# ---------------------------------------------------------------------------

def _parse_v2_response(data: Dict) -> Dict:
    """Parse Regrid API v2 GeoJSON response into flat dict for storage."""
    features = []
    # v2 wraps in {"parcels": {"type": "FeatureCollection", "features": [...]}}
    parcels_wrapper = data.get("parcels", data)
    if isinstance(parcels_wrapper, dict):
        features = parcels_wrapper.get("features", [])

    if not features:
        return {"success": False, "error": "No parcel found"}

    feat = features[0]
    props = feat.get("properties", {})
    fields = props.get("fields", props)
    geometry = feat.get("geometry", {})

    parsed = {
        "success": True,
        "cached": False,
        "data_source": "Regrid",
        # Identifiers
        "ll_uuid": fields.get("ll_uuid", ""),
        "parcelnumb": fields.get("parcelnumb", ""),
        "apn": fields.get("parcelnumb", fields.get("apn", "")),
        # Site address
        "address": fields.get("address", ""),
        "city": fields.get("scity", fields.get("city", "")),
        "state": fields.get("state2", fields.get("state", "")),
        "zip": fields.get("szip", fields.get("zip", "")),
        "county": fields.get("county", ""),
        # Owner info
        "owner_name": fields.get("owner", ""),
        "owner_first": fields.get("ownfrst", ""),
        "owner_last": fields.get("ownlast", ""),
        "owner_type": fields.get("owntype", ""),
        # Mailing address
        "mail_address": fields.get("mailadd", ""),
        "mail_city": fields.get("mail_city", ""),
        "mail_state": fields.get("mail_state2", ""),
        "mail_zip": fields.get("mail_zip", ""),
        # Land use & zoning
        "land_use": fields.get("usecode", ""),
        "use_description": fields.get("usedesc", ""),
        "zoning": fields.get("zoning", ""),
        "zoning_description": fields.get("zoning_description", ""),
        # Measurements
        "lot_size_sqft": _safe_float(fields.get("ll_gisacre")) * 43560 if fields.get("ll_gisacre") else _safe_float(fields.get("sqft")),
        "lot_size_acres": _safe_float(fields.get("ll_gisacre", fields.get("acres"))),
        "building_sqft": _safe_float(fields.get("area_building", fields.get("sqft"))),
        # Structure
        "year_built": _safe_int(fields.get("yearbuilt")),
        "num_stories": _safe_int(fields.get("numstories")),
        "num_units": _safe_int(fields.get("numunits")),
        "num_bedrooms": _safe_int(fields.get("num_bedrooms")),
        "num_bathrooms": _safe_float(fields.get("num_bath")),
        "struct_style": fields.get("structstyle", ""),
        # Valuation
        "parcel_value": _safe_float(fields.get("parval")),
        "land_value": _safe_float(fields.get("landval")),
        "improvement_value": _safe_float(fields.get("improvval")),
        "sale_price": _safe_float(fields.get("saleprice")),
        "sale_date": fields.get("saledate", ""),
        "tax_amount": _safe_float(fields.get("taxamt")),
        "tax_year": _safe_int(fields.get("taxyear")),
        # Risk
        "fema_flood_zone": fields.get("fema_flood_zone", ""),
        # Coordinates
        "centroid_lat": _safe_float(fields.get("lat", fields.get("latitude"))),
        "centroid_lng": _safe_float(fields.get("lon", fields.get("longitude"))),
        # Geometry (store as JSON string for later GIS use)
        "geometry_json": json.dumps(geometry) if geometry else "",
        # Raw fields for anything we missed
        "raw_fields": {k: v for k, v in fields.items() if v},
    }
    return parsed


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _no_key_error() -> Dict:
    return {
        "success": False,
        "error": "Regrid API key not configured. Set REGRID_API_KEY in .env",
    }


# ---------------------------------------------------------------------------
# County Assessor Fallback (FREE)
# ---------------------------------------------------------------------------

class CountyAssessorService:
    """
    Free county assessor APIs as fallback to Regrid.
    Covers major counties with public APIs.
    """

    async def get_parcel_la_county(
        self, apn: str = "", lat: float = None, lng: float = None
    ) -> Dict:
        """LA County Assessor parcel data (FREE)."""
        base_url = "https://maps.assessment.lacounty.gov/GVH_2_2/GVH/wsLegacyService"

        if lat and lng:
            url = f"{base_url}/getParcelByLocation"
            params = {"type": "json", "lat": lat, "lng": lng}
        elif apn:
            url = f"{base_url}/getParcelByAin"
            params = {"type": "json", "ain": apn}
        else:
            return {"success": False, "error": "Either APN or lat/lng required"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return {"success": True, "data_source": "LA County Assessor (FREE)", **data}
                    else:
                        return {"success": False, "error": await resp.text()}
        except Exception as exc:
            return {"success": False, "error": str(exc)}
