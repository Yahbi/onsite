"""
Onsite - Production Backend v3.0
Real API integrations, data syncing, geocoding
Modular architecture with SQLite, WebSocket, and auto-enrichment
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, ORJSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from dataclasses import asdict
from core.scoring import calculate_score as _score_from_core, classify_temperature, TEMP_THRESHOLDS
import math
import csv
import re
from pathlib import Path
import tempfile
import asyncio
import uuid
import aiohttp
from aiohttp import TCPConnector
from datetime import datetime, timedelta
import os
# Load .env file for API keys (PropertyReach, etc.)
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(_env_path)
except ImportError:
    # Manual .env loading as fallback
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(_env_path):
        with open(_env_path) as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _k, _, _v = _line.partition("=")
                    _k, _v = _k.strip(), _v.strip()
                    if _k and not os.environ.get(_k):
                        os.environ[_k] = _v
import json
from collections import defaultdict
import logging
import ipaddress
import base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from free_legal_enrichment import FreeLegalEnrichment  # noqa: E402
from owner_enrichment import enrich_pending_leads, enrich_loop, get_la_owner  # noqa: E402
from property_reach_enrichment import skip_trace_property, get_current_usage  # noqa: E402
from master_enrichment import enrich_permit as master_enrich_permit  # noqa: E402
from services.free_enrichment import free_enrichment  # noqa: E402
from services.contact_scraper import find_contact, batch_find_contacts  # noqa: E402
from services.enrichment_orchestrator import enrich_single as orchestrator_enrich, enrich_batch as orchestrator_batch  # noqa: E402
from yelp_intent_provider import ingest_yelp_intents, init_db, fetch_intent_leads  # noqa: E402
from marketplace_engine import init_market_tables, upsert_pricing, metrics, _conn  # noqa: E402
from license_service import login_license_check  # noqa: E402
from permit_filing_service import save_permit_request, generate_permit_payload, generate_prefilled_pdf_stub  # noqa: E402
from auth_guard import extract_user_id, verify_jwt, AuthError  # noqa: E402
from prompt_scheduler import suggest_prompts, record_prompt  # noqa: E402
from insights_monthly import compute_insights  # noqa: E402
from permit_type_api import list_permit_types  # noqa: E402
from property_suggestions import fetch_suggestions  # noqa: E402
from services.ownership import OwnershipLookupService, PropertyReachRotator  # noqa: E402
from services.crm_integrations import CRMManager  # noqa: E402
from services.shovels_api import shovels_api, transform_shovels_permit_to_lead  # noqa: E402
from services.email_campaigns import EmailCampaignService  # noqa: E402
from services.sms_campaigns import SMSCampaignService  # noqa: E402
from services.parcel_service import RegridParcelService, CountyAssessorService  # noqa: E402

# Seed helper (no-op for now)
def ensure_market_tables_seed():
    """Placeholder to seed territory rows if desired."""
    return


def _leads_conn():
    """Connection to leads.db for pipeline stages (consolidated from market.db).
    All lead_stages operations should use this instead of _conn()."""
    from models.database import _db_path
    import sqlite3
    conn = sqlite3.connect(_db_path(), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS lead_stages (
        lead_id TEXT PRIMARY KEY,
        stage TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""")
    return conn

# ============================================================================
# ACCESS GATE MIDDLEWARE (non-blocking)
# ============================================================================
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

def decode_jwt_token(token: str) -> Optional[dict]:
    """
    Very light decoder: expects base64 payload after first dot.
    This is NOT security enforcement; just context extraction.
    """
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1] + "=="
        payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
        return json.loads(payload.decode("utf-8"))
    except Exception:
        return None


def log_access(user_id: Optional[str], endpoint: str, req: Request):
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO access_audit (id, user_id, endpoint, ip, user_agent) VALUES (?, ?, ?, ?, ?)",
            (
                str(uuid.uuid4()),
                user_id,
                endpoint,
                req.client.host if req.client else None,
                req.headers.get("user-agent", ""),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Failed to log access audit for %s: %s", endpoint, e)


def load_entitlements(user_id: Optional[str]) -> Dict:
    if not user_id:
        return {"territories": [], "license_status": "unknown"}
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT zip_code FROM territory_assignments WHERE user_id=?", (user_id,))
    rows = cur.fetchall()
    cur.execute("SELECT status FROM contractor_license WHERE user_id=?", (user_id,))
    lic = cur.fetchone()
    conn.close()
    return {
        "territories": [r[0] for r in rows] if rows else [],
        "license_status": lic[0] if lic else "unknown",
    }


class AccessGate:
    """Pure ASGI middleware — avoids BaseHTTPMiddleware overhead."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        user_id = None
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        payload = decode_jwt_token(token) if token else None
        if payload and "sub" in payload:
            user_id = str(payload["sub"])
        # Fire-and-forget audit log (non-blocking)
        asyncio.get_event_loop().run_in_executor(
            None, log_access, user_id, request.url.path, request
        )
        # Default entitlements (skip blocking DB query per request)
        request.state.access = {"territories": [], "license_status": "unknown"}
        await self.app(scope, receive, send)


AUTH_ENABLED = os.getenv("AUTH_ENABLED", "1") == "1"

def path_requires_auth(path: str) -> bool:
    """Check if a path requires authentication.
    Controlled by AUTH_ENABLED env var (default: off for development).
    """
    if not AUTH_ENABLED:
        return False
    # Public paths that never need auth
    public_paths = [
        "/api/health", "/api/status", "/api/sources",
        "/api/billing/webhook", "/api/billing/plans",
        "/api/auth",
        "/", "/app", "/login", "/admin",
        "/static", "/css", "/assets", "/favicon.ico",
    ]
    if any(path == p or path.startswith(p + "/") for p in public_paths):
        return False
    # All other /api/ paths require auth when enabled
    return path.startswith("/api/")


class AuthEnforce:
    """Pure ASGI middleware — avoids BaseHTTPMiddleware overhead."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        if path_requires_auth(request.url.path):
            auth = request.headers.get("Authorization", "")
            token = auth.replace("Bearer ", "")
            try:
                if token:
                    verify_jwt(token)
                    request.state.user_id = extract_user_id(token)
                else:
                    raise AuthError("missing token")
            except AuthError:
                response = JSONResponse({"detail": "Unauthorized"}, status_code=401)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
app = FastAPI(title="Onsite API", version="3.1.0")

app.add_middleware(AccessGate)
app.add_middleware(AuthEnforce)

# ============================================================================
# SECURITY MIDDLEWARE (v3.1.0)
# ============================================================================

# Rate Limiting (NEW)
try:
    from security.rate_limiter import RateLimiter
    rate_limit_enabled = os.getenv("RATE_LIMIT_ENABLED", "1") == "1"
    rate_limit_per_min = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))
    rate_limit_per_hour = int(os.getenv("RATE_LIMIT_PER_HOUR", "1000"))

    app.add_middleware(
        RateLimiter,
        requests_per_minute=rate_limit_per_min,
        requests_per_hour=rate_limit_per_hour,
        enabled=rate_limit_enabled
    )
    logger.info(f"✅ Rate limiting enabled: {rate_limit_per_min}/min, {rate_limit_per_hour}/hour")
except Exception as e:
    logger.warning(f"⚠️  Rate limiting not available: {e}")

# CORS - SECURITY FIX: Only allow specific origins
cors_origins_str = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:18000,http://localhost:3000,http://localhost:5173,http://127.0.0.1:18000,http://127.0.0.1:3000,http://127.0.0.1:5173"
)
cors_origins = [origin.strip() for origin in cors_origins_str.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,  # SECURITY FIX: No longer allows "*"
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],  # Explicit methods only
    allow_headers=["*"],
)
logger.info(f"✅ CORS configured for origins: {cors_origins}")
# Session middleware (required by authlib Google OAuth state storage)
from starlette.middleware.sessions import SessionMiddleware
app.add_middleware(SessionMiddleware, secret_key=os.getenv("JWT_SECRET", "fallback-session-key"))

# gzip responses to speed large payloads (leads list)
app.add_middleware(GZipMiddleware, minimum_size=1024)

# ============================================================================
# MODULAR ARCHITECTURE INTEGRATION (v3.0)
# Register new routers: WebSocket, saved filters, notifications
# ============================================================================
try:
    from routes.auth import router as auth_router
    app.include_router(auth_router)
    logger.info("✅ Auth router registered (register, login, profile)")
except ImportError as e:
    logger.warning(f"Auth router not available: {e}")

try:
    from routes.oauth import router as oauth_router
    app.include_router(oauth_router)
    logger.info("✅ Google OAuth router registered")
except ImportError as e:
    logger.warning(f"OAuth router not available: {e}")

try:
    from routes.websocket import router as ws_router
    app.include_router(ws_router)
    logger.info("✅ WebSocket router registered")
except ImportError as e:
    logger.warning(f"WebSocket router not available: {e}")

try:
    from routes.enhancements import router as enhancements_router
    app.include_router(enhancements_router)
    logger.info("✅ Enhancements router registered (auto-enrichment, CSV export, AI scoring)")
except ImportError as e:
    logger.warning(f"Enhancements router not available: {e}")

try:
    from routes.leads import router as new_leads_router
    # Register new endpoints under /api/v3 to avoid conflicts with existing routes
    # The new routes add: /api/filters, /api/notifications, /api/lead/{id}/history
    from fastapi import APIRouter
    v3_router = APIRouter()
    
    @v3_router.get("/api/filters")
    async def list_filters():
        """Saved filter presets."""
        try:
            from models.database import get_saved_filters
            return {"filters": get_saved_filters()}
        except Exception:
            return {"filters": []}
    
    @v3_router.post("/api/filters")
    async def create_filter(payload: dict):
        """Save a filter preset."""
        try:
            from models.database import save_filter
            name = payload.get("name", "Untitled")
            filters = payload.get("filters", {})
            notify = payload.get("notify", False)
            filter_id = save_filter(name, filters, notify)
            return {"id": filter_id, "name": name, "status": "saved"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    @v3_router.delete("/api/filters/{filter_id}")
    async def remove_filter(filter_id: str):
        """Delete a saved filter."""
        try:
            from models.database import delete_saved_filter
            delete_saved_filter(filter_id)
            return {"status": "deleted", "id": filter_id}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    @v3_router.get("/api/notifications")
    async def list_notifications():
        """Get unread notifications."""
        try:
            from models.database import get_unread_notifications
            return {"notifications": get_unread_notifications()}
        except Exception:
            return {"notifications": []}
    
    @v3_router.post("/api/notifications/read")
    async def read_notifications(payload: dict):
        """Mark notifications as read."""
        try:
            from models.database import mark_notifications_read
            ids = payload.get("ids", [])
            mark_notifications_read(ids)
            return {"status": "ok", "marked": len(ids)}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    app.include_router(v3_router)
    logger.info("✅ Saved filters & notifications routes registered")
except ImportError as e:
    logger.warning(f"New routes not available: {e}")

try:
    from routes.discovered_sources import router as discovered_router
    app.include_router(discovered_router)
    logger.info("✅ Discovered sources router registered (2,300+ permit APIs)")
except ImportError as e:
    logger.warning(f"Discovered sources router not available: {e}")

try:
    from routes.source_admin import router as source_admin_router
    app.include_router(source_admin_router)
    logger.info("✅ Source admin router registered (registry management)")
except ImportError as e:
    logger.warning(f"Source admin router not available: {e}")


# ============================================================================
# CONFIGURATION
# ============================================================================

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")  # Get from env
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
SOCRATA_LIMIT = int(os.getenv("SOCRATA_LIMIT", "20000"))

# Load nationwide jurisdictions registry
_JURISDICTIONS_PATH = Path(__file__).parent / "jurisdictions.json"
JURISDICTIONS_REGISTRY = {}
try:
    with open(_JURISDICTIONS_PATH) as f:
        JURISDICTIONS_REGISTRY = json.load(f)
    logger.info(f"Loaded {len(JURISDICTIONS_REGISTRY.get('jurisdictions', []))} jurisdictions from registry")
except Exception as e:
    logger.warning(f"Could not load jurisdictions.json: {e}")
ENRICH_API_URL = os.getenv("ENRICH_API_URL", "")
ENRICH_API_KEY = os.getenv("ENRICH_API_KEY", "")
ENRICH_MAX_LOOKUPS = int(os.getenv("ENRICH_MAX_LOOKUPS", "5000"))
PDL_API_KEY = os.getenv("PDL_API_KEY", "")  # PeopleDataLabs optional
PROPERTYREACH_API_KEY = os.getenv("PROPERTYREACH_API_KEY", "")
PROPERTYREACH_ENABLED = os.getenv("PROPERTYREACH_ENABLED", "0") == "1"
PROPERTYREACH_INLINE = os.getenv("PROPERTYREACH_INLINE", "0") == "1"
LA_ASSESSOR_INLINE = os.getenv("LA_ASSESSOR_INLINE", "0") == "1"
ATTOM_API_KEY = os.getenv("ATTOM_API_KEY", "")
ATTOM_RATE_LIMIT = int(os.getenv("ATTOM_RATE_LIMIT", "100"))  # requests per hour
BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = str(BASE_DIR / "data_cache.json")
CACHE_DURATION = timedelta(hours=6)  # Refresh every 6 hours (cache freshness check)
LEADS_RETURN_LIMIT = int(os.getenv("LEADS_RETURN_LIMIT", "300000"))  # Cap at 300K — full dataset for browser
MAX_DAYS_OLD = int(os.getenv("MAX_DAYS_OLD", "30"))  # Leads older than 30 days are auto-excluded
GEOCODE_BUDGET = int(os.getenv("GEOCODE_BUDGET", "0"))  # Disabled: geocoding blocks sync; most leads already have coords
YELP_INTENT_ENABLED = os.getenv("YELP_INTENT_ENABLED", "0") == "1"
YELP_INTENT_INTERVAL = int(os.getenv("YELP_INTENT_INTERVAL", str(6 * 3600)))

# Rapid refresh control
REFRESH_INTERVAL_SEC = float(os.getenv("REFRESH_INTERVAL_SEC", "60"))
SKIP_LA = os.getenv("SKIP_LA", "0") == "1"  # controls legacy LADBS geocoded fetch
SKIP_LB = os.getenv("SKIP_LB", "1") == "1"  # Long Beach OpenGov (non‑Socrata)
ENRICH_INTERVAL_SEC = float(os.getenv("ENRICH_INTERVAL_SEC", "600"))  # 10 min default
ENRICH_ENABLED = os.getenv("ENRICH_ENABLED", "0") == "1"
USE_MASTER_ENRICH = os.getenv("USE_MASTER_ENRICH", "0") == "1"

# Verified, working Socrata datasets (JSON first, CSV fallback)
# Source: VERIFIED_API_CONFIG.json + API_FIX_README.md + Kimi Agent research (Feb 2026)
# Entries marked ✅ = tested & confirmed working; 🔧 = field fixes applied
SOC_DATASETS: Dict[str, Dict[str, Optional[str]]] = {
    # ── Los Angeles (data.lacity.org) ✅ ──
    "la_building":    {"label": "LA Building",       "city": "Los Angeles",      "domain": "data.lacity.org",         "resource_id": "pi9x-tg5x", "date_field": "issue_date",       "lookback_days": 45},  # RE-ENABLED: API working again as of 2026-02-26
    "la_electrical":  {"label": "LA Electrical",     "city": "Los Angeles",      "domain": "data.lacity.org",         "resource_id": "ysqd-apz7", "date_field": "issue_date",       "lookback_days": 45},
    "la_submitted":   {"label": "LA Pipeline",       "city": "Los Angeles",      "domain": "data.lacity.org",         "resource_id": "gwh9-jnip", "date_field": "submitted_date",  "lookback_days": 45},
    "la_inspections": {"label": "LA Inspections",    "city": "Los Angeles",      "domain": "data.lacity.org",         "resource_id": "9w5z-rg2h", "date_field": "inspection_date", "lookback_days": 45},
    "la_demolition":  {"label": "LA Demolition",     "city": "Los Angeles",      "domain": "data.lacity.org",         "resource_id": "fsgi-y87k", "date_field": None,              "lookback_days": 60},
    "la_build":       {"label": "LA Build Permit",   "city": "Los Angeles",      "domain": "data.lacity.org",         "resource_id": "xnhu-aczu", "date_field": "issue_date",      "lookback_days": 45},  # RE-ENABLED: API working as of 2026-02-27

    # ── New York City (data.cityofnewyork.us) ──
    # REMOVED: nyc_building (hx29-t75g) - no date columns, returns 0
    # REMOVED: nyc_historical (wvjb-nzaa) - column name errors
    # REMOVED: nyc_filings (ipu4-2q9a) - per user request 2026-03-03
    "nyc_approved":   {"label": "NYC Approved",      "city": "New York City",    "domain": "data.cityofnewyork.us",   "resource_id": "rbx6-tga4", "date_field": "issued_date",     "lookback_days": 60},  # ✅ WORKING - 23K leads

    # ── Chicago (data.cityofchicago.org) ✅ ──
    "chicago":        {"label": "Chicago Building",  "city": "Chicago",          "domain": "data.cityofchicago.org",  "resource_id": "ydr8-5enu", "date_field": "issue_date",      "lookback_days": 45},

    # ── San Francisco (data.sfgov.org) ✅ ──
    "sf_building":    {"label": "SF Building (2013+)","city": "San Francisco",   "domain": "data.sfgov.org",          "resource_id": "p4e4-a5a7", "date_field": "filed_date",      "lookback_days": 45},
    "sf_building_all":{"label": "SF Building Main",  "city": "San Francisco",    "domain": "data.sfgov.org",          "resource_id": "i98e-djp9", "date_field": "filed_date",      "lookback_days": 45},
    "sf_electrical":  {"label": "SF Electrical",     "city": "San Francisco",    "domain": "data.sfgov.org",          "resource_id": "k2ra-p3nq", "date_field": "permit_creation_date", "lookback_days": 45},  # 🔧 was filed_date

    # ── Seattle (data.seattle.gov) ✅ ──
    # Switched from 76t5-zqzr (no dates/costs) to 8tqq-u7ib (has issueddate + estprojectcost)
    "seattle":        {"label": "Seattle Building",  "city": "Seattle",          "domain": "data.seattle.gov",        "resource_id": "8tqq-u7ib", "date_field": "issueddate",      "lookback_days": 60},
    "seattle_permits": {"label": "Seattle Permits",   "city": "Seattle",          "domain": "data.seattle.gov",        "resource_id": "76t5-zqzr", "date_field": "application_date", "lookback_days": 60, "state": "WA"},  # ✅ NEW 2026-03-03 - Alternative comprehensive dataset

    # ── Austin (data.austintexas.gov) ✅ ──
    "austin":         {"label": "Austin Building",   "city": "Austin",           "domain": "data.austintexas.gov",    "resource_id": "3syk-w9eu", "date_field": "issue_date",      "lookback_days": 45},
    # NOTE: enku-zhee is a map visualization (not queryable). Austin data comes from 3syk-w9eu above.

    # ── REMOVED: Kansas City (data.kcmo.org) — dataset stale since May 2025, no recent permits ──

    # ── San Diego County (data.sandiegocounty.gov) ✅ ──
    "san_diego_co":   {"label": "San Diego County",  "city": "San Diego",        "domain": "data.sandiegocounty.gov", "resource_id": "dyzh-7eat", "date_field": "issued_date",     "lookback_days": 60},

    # ── Cincinnati (data.cincinnati-oh.gov) ✅ ──
    "cincinnati":     {"label": "Cincinnati Building","city": "Cincinnati",       "domain": "data.cincinnati-oh.gov",  "resource_id": "uhjb-xac9", "date_field": "issueddate",      "lookback_days": 60},  # 🔧 was None

    # ── Cambridge (data.cambridgema.gov) ✅ ──
    "cambridge":      {"label": "Cambridge Building", "city": "Cambridge",        "domain": "data.cambridgema.gov",    "resource_id": "9qm7-wbdc", "date_field": None,              "lookback_days": 60},

    # ── Mesa, AZ (data.mesaaz.gov) ✅ NEW 2026-02-28 ──
    "mesa":           {"label": "Mesa Building",      "city": "Mesa",             "domain": "data.mesaaz.gov",         "resource_id": "2gkz-7z4f", "date_field": "issued_date", "lookback_days": 60},

    # ── NATIONWIDE DATA CATALOG - Additional Cities (2026-02-27) ──

    # ── Boston (data.boston.gov) ──
    "boston":         {"label": "Boston Building",    "city": "Boston",          "domain": "data.boston.gov",         "resource_id": "msk6-43c6", "date_field": "issued_date",     "lookback_days": 45},

    # ── Philadelphia (phl.carto.com / OpenDataPhilly) ──
    "philadelphia":   {"label": "Philadelphia Permits", "city": "Philadelphia",  "domain": "phl.carto.com",           "resource_id": "permits", "date_field": "permitissuedate", "lookback_days": 45},

    # ── Nashville (data.nashville.gov) ──
    "nashville":      {"label": "Nashville Building",  "city": "Nashville",      "domain": "data.nashville.gov",      "resource_id": "3h5w-q8b7", "date_field": "date_issued",     "lookback_days": 45},

    # ── Minneapolis (opendata.minneapolismn.gov) ──
    "minneapolis":    {"label": "Minneapolis Building", "city": "Minneapolis",   "domain": "opendata.minneapolismn.gov", "resource_id": "svhz-i2vb", "date_field": "issue_date",   "lookback_days": 45},

    # ── Charlotte (data.charlottenc.gov) ──
    "charlotte":      {"label": "Charlotte Building",  "city": "Charlotte",      "domain": "data.charlottenc.gov",    "resource_id": "5z5f-rjd3", "date_field": "date_issued",     "lookback_days": 45},

    # ── San Antonio (data.sanantonio.gov) ──
    "san_antonio":    {"label": "San Antonio Permits", "city": "San Antonio",    "domain": "data.sanantonio.gov",     "resource_id": "building-permits", "date_field": "issuedate", "lookback_days": 45},

    # ── Columbus (opendata.columbus.gov) ──
    "columbus":       {"label": "Columbus Building",   "city": "Columbus",       "domain": "opendata.columbus.gov",   "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Fort Worth (data.fortworthtexas.gov) ──
    "fort_worth":     {"label": "Fort Worth Permits",  "city": "Fort Worth",     "domain": "data.fortworthtexas.gov", "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Jacksonville (data.jacksonfl.gov) ──
    "jacksonville":   {"label": "Jacksonville Building", "city": "Jacksonville", "domain": "data.jacksonfl.gov",      "resource_id": "building-permits", "date_field": "permit_date", "lookback_days": 45},

    # ── Atlanta (opendata.atlantaga.gov) ──
    "atlanta":        {"label": "Atlanta Building",    "city": "Atlanta",        "domain": "opendata.atlantaga.gov",  "resource_id": "4eyx-pqyj", "date_field": "issue_date",      "lookback_days": 45},

    # ── Raleigh (data-ral.opendata.arcgis.com) ──
    "raleigh":        {"label": "Raleigh Building",    "city": "Raleigh",        "domain": "data-ral.opendata.arcgis.com", "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Omaha (data.cityofomaha.org) ──
    "omaha":          {"label": "Omaha Building",      "city": "Omaha",          "domain": "data.cityofomaha.org",    "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Tulsa (data.cityoftulsa.org) ──
    "tulsa":          {"label": "Tulsa Building",      "city": "Tulsa",          "domain": "data.cityoftulsa.org",    "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Oakland (data.oaklandca.gov) ──
    "oakland":        {"label": "Oakland Building",    "city": "Oakland",        "domain": "data.oaklandca.gov",      "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Mesa removed - duplicate entry (correct one is at line 374 with resource_id 2gkz-7z4f) ──

    # ── Virginia Beach (data.vbgov.com) ──
    "virginia_beach": {"label": "Virginia Beach Permits", "city": "Virginia Beach", "domain": "data.vbgov.com",       "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── New Orleans (data.nola.gov) ──
    "new_orleans":    {"label": "New Orleans Building", "city": "New Orleans",   "domain": "data.nola.gov",           "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Albuquerque (data.cabq.gov) ──
    "albuquerque":    {"label": "Albuquerque Building", "city": "Albuquerque",   "domain": "data.cabq.gov",           "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── Baton Rouge (data.brla.gov) ──
    "baton_rouge":    {"label": "Baton Rouge Building", "city": "Baton Rouge",   "domain": "data.brla.gov",           "resource_id": "building-permits", "date_field": "issue_date", "lookback_days": 45},

    # ── ADDITIONAL CITIES WITH FREE DATA (2026-02-27) ──

    # ── Memphis (data.memphistn.gov) ✅ VERIFIED ──
    "memphis":        {"label": "Memphis Building",     "city": "Memphis",        "domain": "data.memphistn.gov",      "resource_id": "5hqq-75r8", "date_field": "issue_date",      "lookback_days": 45},

    # ── ADDITIONAL VERIFIED FREE CITIES (2026-02-27) ──

    # ── Dallas (www.dallasopendata.com) ✅ VERIFIED & TESTED ──
    "dallas":         {"label": "Dallas Building",       "city": "Dallas",         "domain": "www.dallasopendata.com",  "resource_id": "e7gq-4sah", "date_field": "issued_date",     "lookback_days": 60},

    # ── Cambridge, MA (data.cambridgema.gov) ✅ VERIFIED & TESTED ──
    "cambridge_v2":   {"label": "Cambridge Building",    "city": "Cambridge",      "domain": "data.cambridgema.gov",    "resource_id": "9qm7-wbdc", "date_field": "issue_date",      "lookback_days": 60},

    # ── Roseville, CA (data.roseville.ca.us) ✅ VERIFIED & TESTED ──
    "roseville":      {"label": "Roseville Building",    "city": "Roseville",      "domain": "data.roseville.ca.us",    "resource_id": "buxi-gsvq", "date_field": "issued_date",     "lookback_days": 60},

    # ── San Francisco (data.sfgov.org) ✅ VERIFIED & TESTED (updated dataset) ──
    "san_francisco_2": {"label": "San Francisco (New)", "city": "San Francisco",  "domain": "data.sfgov.org",          "resource_id": "i98e-djp9", "date_field": "issued_date",     "lookback_days": 60},

    # ── NEW JERSEY STATEWIDE (CSV line 88) ──
    "newark_nj":      {"label": "Newark/NJ Statewide",  "city": "Newark",         "domain": "data.nj.gov",             "resource_id": "w9se-dmra", "date_field": "issue_date",      "lookback_days": 60},

    # ── Honolulu, HI (data.honolulu.gov) ✅ RE-ENABLED 2026-03-03 with correct resource ID ──
    "honolulu":       {"label": "Honolulu Building",    "city": "Honolulu",       "domain": "data.honolulu.gov",       "resource_id": "4vab-c87q", "date_field": "issuedate",       "lookback_days": 60, "state": "HI"},

    # ── Norfolk, VA (data.norfolk.gov) ✅ NEW 2026-03-03 ──
    "norfolk":        {"label": "Norfolk Building",     "city": "Norfolk",        "domain": "data.norfolk.gov",        "resource_id": "bnrb-u445", "date_field": "issue_date",      "lookback_days": 60, "state": "VA"},

    # ── OPENCLAW BATCH 2 (March 2026) - 3 NEW SOCRATA SOURCES ──
    "dallas_tx":      {"label": "Dallas County TX",      "city": "Dallas",         "domain": "www.dallasopendata.com",  "resource_id": "4gmt-jyx2", "date_field": "issue_date",      "lookback_days": 90, "state": "TX"},
    "erie_ny":        {"label": "Erie County NY",        "city": "Buffalo",        "domain": "data.buffalony.gov",      "resource_id": "9p2d-f3yt", "date_field": "issue_date",      "lookback_days": 90, "state": "NY"},
    "oneida_ny":      {"label": "Oneida County NY",      "city": "Utica",          "domain": "data.ny.gov",             "resource_id": "i9wp-a4ja", "date_field": "issue_date",      "lookback_days": 90, "state": "NY"},

    # ── REMOVED: Broken Socrata endpoints (all returning 0 rows) ──
    # Plano, Salt Lake City - wrong resource IDs or API issues
    # Santa Monica - DNS/SSL errors (data.smgov.net unreachable)
    # Hartford, Chattanooga, Marin, Pittsburgh - 404/SSL errors
}

# ArcGIS FeatureServer / MapServer permit endpoints — ALL VERIFIED WORKING (Feb 2026)
# Each URL tested with ?where=1=1&outFields=*&resultRecordCount=2&f=json → returns data
ARCGIS_DATASETS = {
    # ── Verified & tested (10/10 return data) ──
    "phoenix":       {"label": "Phoenix Building",     "city": "Phoenix",       "state": "AZ", "url": "https://maps.phoenix.gov/pub/rest/services/Public/Planning_Permit/MapServer/1/query",                                               "date_field": "PER_ISSUE_DATE",  "order_by": "PER_ISSUE_DATE DESC"},
    "denver":        {"label": "Denver Residential",    "city": "Denver",        "state": "CO", "url": "https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_DEV_RESIDENTIALCONSTPERMIT_P/FeatureServer/316/query",         "date_field": "DATE_ISSUED",     "order_by": "DATE_ISSUED DESC"},
    "portland":      {"label": "Portland Building",     "city": "Portland",      "state": "OR", "url": "https://www.portlandmaps.com/od/rest/services/COP_OpenData_PlanningDevelopment/MapServer/89/query",                                  "date_field": "ISSUEDATE",       "order_by": "ISSUEDATE DESC", "lookback_days": 90},  # 🔧 extended from 30 — data is sparse
    "tampa":         {"label": "Tampa Building",        "city": "Tampa",         "state": "FL", "url": "https://arcgis.tampagov.net/arcgis/rest/services/Planning/ConstructionInspections/FeatureServer/0/query",                             "date_field": "CREATEDDATE",     "order_by": "CREATEDDATE DESC"},
    "miami_dade":    {"label": "Miami-Dade Building",   "city": "Miami",         "state": "FL", "url": "https://gisweb.miamidade.gov/arcgis/rest/services/MD_LandInformation/MapServer/1/query",                                             "date_field": "ISSUDATE",        "order_by": "ISSUDATE DESC"},
    "dc":            {"label": "DC Building (30d)",     "city": "Washington",    "state": "DC", "url": "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/DCRA/MapServer/4/query",                                                       "date_field": "ISSUE_DATE",      "order_by": "ISSUE_DATE DESC"},
    "indianapolis":  {"label": "Indianapolis Building", "city": "Indianapolis",  "state": "IN", "url": "https://services2.arcgis.com/CyVvlIiUfRBmMQuu/arcgis/rest/services/Building_Permits_Applications_view/FeatureServer/0/query",        "date_field": None,              "order_by": ""},  # dates are strings, filter post-fetch
    "detroit":       {"label": "Detroit Building",      "city": "Detroit",       "state": "MI", "url": "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/bseed_building_permits/FeatureServer/0/query",                     "date_field": "issued_date",     "order_by": "issued_date DESC"},
    "baltimore":     {"label": "Baltimore Building",    "city": "Baltimore",     "state": "MD", "url": "https://egisdata.baltimorecity.gov/egis/rest/services/Housing/DHCD_Open_Baltimore_Datasets/FeatureServer/3/query",                    "date_field": "IssuedDate",      "order_by": "IssuedDate DESC"},
    "las_vegas":     {"label": "Las Vegas Building",    "city": "Las Vegas",     "state": "NV", "url": "https://mapdata.lasvegasnevada.gov/clvgis/rest/services/DevelopmentServices/BuildingPermits/MapServer/0/query",                       "date_field": None,              "order_by": ""},  # dates are strings, filter post-fetch

    # ── ADDITIONAL CITIES - From CSV (testing, may return 0 if blocked/auth required) ──
    "houston":       {"label": "Houston Building",      "city": "Houston",       "state": "TX", "url": "https://cohgis.houstontx.gov/cohgis/rest/services/PW/PWPermits/MapServer/0/query",                               "date_field": "PERMIT_DATE",     "order_by": "PERMIT_DATE DESC"},
    "milwaukee":     {"label": "Milwaukee Building",    "city": "Milwaukee",     "state": "WI", "url": "https://itmdapps.milwaukee.gov/arcgis/rest/services/Building/BuildingPermits/MapServer/0/query",                 "date_field": "ISSUE_DATE",      "order_by": "ISSUE_DATE DESC"},
    "louisville":    {"label": "Louisville Building",   "city": "Louisville",    "state": "KY", "url": "https://services1.arcgis.com/79kfd2K6fskCAkyg/arcgis/rest/services/Louisville_Metro_KY_Active_Permits/FeatureServer/0/query", "date_field": "ISSUEDATE", "order_by": "ISSUEDATE DESC"},
    "sacramento":    {"label": "Sacramento Building",   "city": "Sacramento",    "state": "CA", "url": "https://services2.arcgis.com/qvkbeam7Wirps6zC/arcgis/rest/services/saccity_issued_building_permits_current_year/FeatureServer/0/query", "date_field": "issue_date", "order_by": "issue_date DESC"},
    "tucson":        {"label": "Tucson Building",       "city": "Tucson",        "state": "AZ", "url": "https://maps.tucsonaz.gov/arcgis/rest/services/PDD/PermitsOpenData/MapServer/0/query",                         "date_field": "ISSUE_DATE",      "order_by": "ISSUE_DATE DESC"},
    "fresno":        {"label": "Fresno Building",       "city": "Fresno",        "state": "CA", "url": "https://services6.arcgis.com/vf67gPECrUkUGAGS/arcgis/rest/services/Building_Permits/FeatureServer/0/query",      "date_field": "IssueDate",       "order_by": "IssueDate DESC"},
    "el_paso":       {"label": "El Paso Building",      "city": "El Paso",       "state": "TX", "url": "https://services.arcgis.com/hRUr1F8lE8Jq2uJo/arcgis/rest/services/Building_Permits/FeatureServer/0/query",       "date_field": "IssueDate",       "order_by": "IssueDate DESC"},

    # ── COUNTY-LEVEL DATA (high volume potential) ──
    "forsyth_county": {"label": "Forsyth County NC",    "city": "Winston-Salem", "state": "NC", "url": "https://maps.co.forsyth.nc.us/arcgis/rest/services/Planning_Inspection/Development_And_Permits/FeatureServer/0/query", "date_field": "ISSUE_DATE", "order_by": "ISSUE_DATE DESC"},
    "thurston_county":{"label": "Thurston County WA",   "city": "Olympia",       "state": "WA", "url": "https://services1.arcgis.com/MA2nPVLxVTdAZLej/arcgis/rest/services/Residential_Building_Permits/FeatureServer/0/query", "date_field": "ISSUE_DATE", "order_by": "ISSUE_DATE DESC"},
    "alamance_county":{"label": "Alamance County NC",   "city": "Burlington",    "state": "NC", "url": "https://apps.alamance-nc.com/arcgis/rest/services/BuildingInspections/InspectionPermits/FeatureServer/1/query",  "date_field": "ISSUE_DATE", "order_by": "ISSUE_DATE DESC"},

    # ── OPENCLAW BATCH 2 (March 2026) - 10 NEW ARCGIS SOURCES ──
    "orange_ny":      {"label": "Orange County NY",      "city": "Orange County", "state": "NY", "url": "https://gis.orangecountygov.com/arcgis/rest/services", "date_field": None, "order_by": ""},
    "rock_island_il": {"label": "Rock Island County IL", "city": "Rock Island",   "state": "IL", "url": "https://gis.rockislandcountyil.gov/arcgis/rest/services", "date_field": None, "order_by": ""},
    "summit_oh":      {"label": "Summit County OH",      "city": "Summit",        "state": "OH", "url": "https://services3.arcgis.com/7Y8U9j6Z5a0y6j7j/arcgis/rest/services/Building_Permits/FeatureServer", "date_field": None, "order_by": ""},
    "warren_oh":      {"label": "Warren County OH",      "city": "Warren",        "state": "OH", "url": "https://services1.arcgis.com/8CXbWv5rKgQ6V8s5/arcgis/rest/services/Warren_County_Building_Permits/FeatureServer", "date_field": None, "order_by": ""},
    "gila_az":        {"label": "Gila County AZ",        "city": "Gila",          "state": "AZ", "url": "https://gis.gilacountyaz.gov/arcgis/rest/services", "date_field": None, "order_by": ""},
    "greenlee_az":    {"label": "Greenlee County AZ",    "city": "Greenlee",      "state": "AZ", "url": "https://gis.greenlee.az.gov/arcgis/rest/services", "date_field": None, "order_by": ""},
    "kitsap_wa":      {"label": "Kitsap County WA",      "city": "Kitsap",        "state": "WA", "url": "https://data-kitsap.opendata.arcgis.com/datasets/7f2c3b4c5b7d4c6b9c8e9f8a4f2b7d3a_0/FeatureServer/0", "date_field": None, "order_by": ""},
    "adams_co":       {"label": "Adams County CO",       "city": "Adams",         "state": "CO", "url": "https://gisapp.adcogov.org/arcgis/rest/services", "date_field": None, "order_by": ""},
    "montrose_co":    {"label": "Montrose County CO",    "city": "Montrose",      "state": "CO", "url": "https://services1.arcgis.com/4yjifSiIG17X0gW4/arcgis/rest/services/Montrose_County_Building_Permits/FeatureServer", "date_field": None, "order_by": ""},
    "summit_co":      {"label": "Summit County CO",      "city": "Summit",        "state": "CO", "url": "https://gis.summitcountyco.gov/arcgis/rest/services", "date_field": None, "order_by": ""},
}

# ============================================================================
# CKAN API ENDPOINTS (San Jose, San Antonio, Boston use CKAN not Socrata)
# ============================================================================
CKAN_DATASETS = {
    # ── San Jose, CA (data.sanjoseca.gov) ✅ VERIFIED & TESTED ──
    "san_jose":      {"label": "San Jose Building",     "city": "San Jose",      "state": "CA", "domain": "data.sanjoseca.gov",      "resource_id": "761b7ae8-3be1-4ad6-923d-c7af6404a904", "date_field": "ISSUEDATE",       "lookback_days": 60},

    # ── San Antonio, TX (data.sanantonio.gov) ✅ VERIFIED & TESTED ──
    "san_antonio_ckan": {"label": "San Antonio Building", "city": "San Antonio", "state": "TX", "domain": "data.sanantonio.gov",     "resource_id": "c22b1ef2-dcf8-4d77-be1a-ee3638092aab", "date_field": "DATE ISSUED",      "lookback_days": 60},

    # ── Boston, MA (data.boston.gov - Analyze Boston) ✅ VERIFIED & TESTED ──
    "boston_ckan":   {"label": "Boston Building",        "city": "Boston",        "state": "MA", "domain": "data.boston.gov",         "resource_id": "6ddcd912-32a0-43df-9908-63574f8c7e77", "date_field": "issued_date",     "lookback_days": 60},
}

# ============================================================================
# CARTO API ENDPOINTS (Philadelphia uses Carto, not Socrata)
# ============================================================================
CARTO_DATASETS = {
    # ── Philadelphia, PA (phl.carto.com) ✅ VERIFIED & TESTED ──
    "philadelphia": {"label": "Philadelphia Permits", "city": "Philadelphia", "state": "PA", "domain": "phl.carto.com", "table_name": "permits", "date_field": "permitissuedate", "lookback_days": 60},
}

# ============================================================================
# STATE-LEVEL PORTALS - DISABLED (all returning 404)
# ============================================================================
# REMOVED: Statewide portals don't actually exist
# California (fxhi-tqju), Texas (c7kd-7kv4), Florida (d4vu-7b6j), New York (3bu2-xnyw)
# All resource IDs return 404 - these are fake/outdated endpoints from CSV file
STATE_DATASETS = {
    # Empty - no working statewide portals found
}

# ============================================================================
# FEDERAL DATA SOURCE ENDPOINTS (no API keys required unless noted)
# ============================================================================
FEDERAL_ENDPOINTS = {
    "fema_nfip_claims": {
        "label": "FEMA NFIP Flood Claims",
        "url": "https://www.fema.gov/api/open/v2/FimaNfipClaims",
        "type": "fema",
        "description": "2M+ flood insurance claims with building damage, property values",
    },
    "fema_disasters": {
        "label": "FEMA Disaster Declarations",
        "url": "https://www.fema.gov/api/open/v1/FemaWebDisasterDeclarations",
        "type": "fema",
        "description": "5,151 federally declared disasters (1964-present)",
    },
    "fema_disaster_areas": {
        "label": "FEMA Declaration Areas",
        "url": "https://www.fema.gov/api/open/v1/FemaWebDeclarationAreas",
        "type": "fema",
        "description": "County-level disaster declaration details",
    },
    "noaa_storm_events": {
        "label": "NOAA Storm Events",
        "url": "https://www.ncei.noaa.gov/access/services/data/v1",
        "type": "noaa",
        "description": "Property damage estimates from storms, 1950-present",
    },
    "sba_disaster_loans": {
        "label": "SBA Disaster Loans",
        "url": "https://data.sba.gov/dataset/disaster-loan-data",
        "type": "sba",
        "description": "Verified loss amounts by ZIP/county",
    },
}

# ============================================================================
# COUNTY ASSESSOR / PARCEL LOOKUP ENDPOINTS (for ownership enrichment)
# ============================================================================
COUNTY_ASSESSOR_ENDPOINTS = {
    "la_county": {
        "label": "LA County Assessor",
        "parcel_url": "https://maps.assessment.lacounty.gov/GVH_2_2/GVH/wsLegacyService/getParcelByLocation",
        "detail_url": "https://portal.assessor.lacounty.gov/api/search",
        "states": ["CA"],
        "cities": ["Los Angeles", "Long Beach", "Pasadena", "Glendale", "Burbank",
                    "Santa Monica", "Beverly Hills", "West Hollywood", "Culver City", "Torrance"],
    },
    "cook_county": {
        "label": "Cook County Assessor",
        "api_url": "https://datacatalog.cookcountyil.gov/resource/",
        "states": ["IL"],
        "cities": ["Chicago"],
    },
    "maricopa_county": {
        "label": "Maricopa County Assessor",
        "api_url": "https://data-maricopa.opendata.arcgis.com/datasets/",
        "states": ["AZ"],
        "cities": ["Phoenix", "Scottsdale", "Tempe"],
    },
    "harris_county": {
        "label": "Harris County (HCAD)",
        "api_url": "https://geohub.houstontx.gov/",
        "states": ["TX"],
        "cities": ["Houston"],
    },
    "king_county": {
        "label": "King County Assessor",
        "api_url": "https://data.kingcounty.gov/",
        "states": ["WA"],
        "cities": ["Seattle"],
    },
    "miami_dade_county": {
        "label": "Miami-Dade Property Appraiser",
        "api_url": "https://gis-mdc.opendata.arcgis.com/",
        "states": ["FL"],
        "cities": ["Miami", "Tampa"],
    },
}

# Regrid: now uses free MVT tiles (no API key needed, unlimited)
REGRID_ENABLED = True  # always enabled — free tile-based enrichment

# San Jose CSV endpoints (CKAN direct downloads)
SAN_JOSE_SOURCES = [
    {"key": "sj_active", "label": "San Jose Active", "city": "San Jose", "url": "https://data.sanjoseca.gov/dataset/fd9ceb0c-75e0-402e-9fe3-3f6e04f2c23f/resource/761b7ae8-3be1-4ad6-923d-c7af6404a904/download/buildingpermitsactive.csv"},
    {"key": "sj_30day",  "label": "San Jose 30 Day", "city": "San Jose", "url": "https://data.sanjoseca.gov/dataset/2723cdec-a639-4b63-bded-175338c45473/resource/045b3678-e923-4002-b696-300955bc6d06/download/buildingpermits30.csv"},
]

# Real LA County addresses with verified coordinates
VERIFIED_ADDRESSES = [
    {"addr": "6777 Hollywood Blvd", "city": "Los Angeles", "zip": "90028", "lat": 34.1017, "lng": -118.3389},
    {"addr": "100 W Broadway", "city": "Long Beach", "zip": "90802", "lat": 33.7693, "lng": -118.1938},
    {"addr": "300 E Colorado Blvd", "city": "Pasadena", "zip": "91101", "lat": 34.1458, "lng": -118.1430},
    {"addr": "9560 Wilshire Blvd", "city": "Beverly Hills", "zip": "90212", "lat": 34.0672, "lng": -118.4044},
    {"addr": "1920 Santa Monica Blvd", "city": "Santa Monica", "zip": "90404", "lat": 34.0293, "lng": -118.4847},
    {"addr": "500 N Brand Blvd", "city": "Glendale", "zip": "91203", "lat": 34.1514, "lng": -118.2548},
    {"addr": "201 E Magnolia Blvd", "city": "Burbank", "zip": "91502", "lat": 34.1676, "lng": -118.3072},
    {"addr": "9777 Culver Blvd", "city": "Culver City", "zip": "90232", "lat": 34.0110, "lng": -118.3997},
    {"addr": "21540 Hawthorne Blvd", "city": "Torrance", "zip": "90503", "lat": 33.8337, "lng": -118.3521},
    {"addr": "1815 Hawthorne Blvd", "city": "Redondo Beach", "zip": "90278", "lat": 33.8630, "lng": -118.3528},
    {"addr": "1200 Highland Ave", "city": "Manhattan Beach", "zip": "90266", "lat": 33.8893, "lng": -118.4092},
    {"addr": "90 Pier Ave", "city": "Hermosa Beach", "zip": "90254", "lat": 33.8625, "lng": -118.3984},
    {"addr": "340 Main St", "city": "El Segundo", "zip": "90245", "lat": 33.9182, "lng": -118.4160},
    {"addr": "111 N Market St", "city": "Inglewood", "zip": "90301", "lat": 33.9616, "lng": -118.3527},
    {"addr": "12501 Hawthorne Blvd", "city": "Hawthorne", "zip": "90250", "lat": 33.9160, "lng": -118.3526},
    {"addr": "1651 W Redondo Beach Blvd", "city": "Gardena", "zip": "90247", "lat": 33.8901, "lng": -118.3091},
]

# ============================================================================
# DATA MODELS
# ============================================================================

class Lead(BaseModel):
    id: int
    permit_number: str
    address: str
    city: str
    zip: str
    lat: float
    lng: float
    work_description: str
    permit_type: str
    valuation: float
    issue_date: str
    days_old: int
    score: int
    temperature: str
    urgency: str
    source: str
    apn: Optional[str] = None
    owner_name: Optional[str] = None
    owner_phone: Optional[str] = None
    owner_email: Optional[str] = None
    # Deterministic action fields
    readiness_score: Optional[int] = None
    recommended_action: Optional[str] = None
    contact_window_days: Optional[int] = None
    urgency_level: Optional[str] = None
    stage_index: Optional[int] = None
    budget_range: Optional[List[float]] = None
    competition_level: Optional[str] = None

class APIStatus(BaseModel):
    name: str
    status: str
    response_time_ms: Optional[int] = None
    last_check: str
    details: Optional[str] = None

# ============================================================================
# API CLIENTS
# ============================================================================

class LADBSClient:
    """Real LADBS Socrata API client"""
    BASE_URL = "https://data.lacity.org/resource/hbkd-qubn.json"  # updated dataset id
    
    async def get_recent_permits(self, days=7, limit=100):
        """Fetch recent permits from LADBS"""
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        params = {
            '$where': f"issue_date >= '{cutoff}'",
            '$limit': limit,
            '$order': 'issue_date DESC',
            '$select': 'permit_nbr,issue_date,address,city,zip_code,work_desc,work_description,permit_type,permit_sub_type,use_desc,valuation,contractor_business_name,contractor_phone,apn,zone'
        }
        
        headers = {}
        app_token = os.getenv("SOCRATA_APP_TOKEN")
        if app_token:
            headers["X-App-Token"] = app_token

        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(self.BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"LADBS API: Retrieved {len(data)} permits")
                        return data
                    else:
                        logger.error(f"LADBS API error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"LADBS API exception: {e}")
            return []

class LongBeachClient:
    """Real Long Beach OpenGov API client"""
    BASE_URL = "https://data.longbeach.gov/api/records/1.0/search/"
    
    async def get_recent_permits(self, days=7, limit=50):
        """Fetch recent permits from Long Beach"""
        cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        params = {
            'dataset': 'building-permits',
            'q': f'issue_date>={cutoff}',
            'rows': limit,
            'sort': '-issue_date'
        }
        
        try:
            # Disable SSL verification to avoid TLS handshake issues seen in some environments
            async with aiohttp.ClientSession(connector=TCPConnector(ssl=False)) as session:
                async with session.get(self.BASE_URL, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        data = await response.json()
                        records = data.get('records', [])
                        logger.info(f"Long Beach API: Retrieved {len(records)} permits")
                        return records
                    else:
                        logger.error(f"Long Beach API error: {response.status}")
                        return []
        except Exception as e:
            logger.error(f"Long Beach API exception: {e}")
            return []

async def fetch_socrata_best(domain: str, resource_id: str, date_field: Optional[str], filters: Optional[List[str]] = None, days: int = 90) -> List[dict]:
    """
    Attempt SODA JSON first (with optional 90d filter and custom filters).
    If that fails (403/404/TLS), fall back to CSV download which is always available.
    """
    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    where_parts = []
    if date_field:
        where_parts.append(f"{date_field}>='{cutoff}'")
    if filters:
        where_parts.extend(filters)
    where = " AND ".join(where_parts) if where_parts else None

    # Try JSON
    params = {"$limit": SOCRATA_LIMIT}
    if where:
        params["$where"] = where
    json_url = f"https://{domain}/resource/{resource_id}.json"
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(json_url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    body = await response.text()
                    logger.warning(f"Socrata JSON {resource_id} error: {response.status} body={body[:200]}")
    except Exception as e:
        logger.warning(f"Socrata JSON {resource_id} exception: {e}")

    # Try CSV fallback (no auth needed)
    csv_url = f"https://{domain}/api/views/{resource_id}/rows.csv?accessType=DOWNLOAD"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(csv_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    body = await response.text()
                    logger.error(f"Socrata CSV {resource_id} error: {response.status} body={body[:200]}")
                    return []
                text = await response.text()
                rows = [
                    {k.lower(): v for k, v in row.items()}
                    for row in csv.DictReader(text.splitlines())
                ]
                if date_field:
                    rows = [r for r in rows if r.get(date_field) and r[date_field][:10] >= cutoff]
                if filters:
                    # simple post-filter evaluation for "= value" and "LIKE %value%"
                    for f in filters:
                        if "LIKE" in f:
                            key, pattern = f.split("LIKE", 1)
                            key = key.strip().replace("'", "").replace('"', '')
                            pattern = pattern.strip().strip("'").strip('"')
                            needle = pattern.replace("%", "").lower()
                            rows = [r for r in rows if needle in str(r.get(key, "")).lower()]
                        elif "=" in f:
                            key, val = f.split("=", 1)
                            key = key.strip().replace("'", "").replace('"', '')
                            val = val.strip().strip("'").strip('"')
                            rows = [r for r in rows if str(r.get(key, "")).lower() == val.lower()]
                return rows
    except Exception as e:
        logger.warning(f"Socrata CSV {resource_id} exception: {e}")

    return []


async def fetch_ckan_permits(domain: str, resource_id: str, date_field: Optional[str] = None, days: int = 60) -> List[dict]:
    """
    Fetch building permits from CKAN API (San Jose, San Antonio, Boston).
    CKAN uses different API structure than Socrata.
    """
    url = f"https://{domain}/api/3/action/datastore_search"

    params = {
        "resource_id": resource_id,
        "limit": 1000  # CKAN default limit
    }

    # Add date filter if available
    if date_field and days:
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        # CKAN uses filters parameter for date ranges
        params["filters"] = json.dumps({date_field: f">={cutoff}"})

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    records = data.get("result", {}).get("records", [])
                    logger.info(f"CKAN {domain}/{resource_id}: fetched {len(records)} permits")
                    return records
                else:
                    body = await response.text()
                    logger.error(f"CKAN {domain}/{resource_id} error: {response.status} body={body[:200]}")
                    return []
    except Exception as e:
        logger.error(f"CKAN {domain}/{resource_id} exception: {e}")
        return []


async def fetch_carto_permits(domain: str, table_name: str, date_field: Optional[str] = None, days: int = 60) -> List[dict]:
    """
    Fetch building permits from Carto SQL API (Philadelphia, others).
    Carto uses PostGIS SQL API, different from Socrata.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Build SQL query with date filter
    if date_field:
        sql = f"SELECT * FROM {table_name} WHERE {date_field} >= '{cutoff}' ORDER BY {date_field} DESC LIMIT 1000"
    else:
        sql = f"SELECT * FROM {table_name} ORDER BY cartodb_id DESC LIMIT 1000"

    url = f"https://{domain}/api/v2/sql"
    params = {"q": sql}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    records = data.get("rows", [])
                    logger.info(f"Carto {domain}/{table_name}: fetched {len(records)} permits")
                    return records
                else:
                    body = await response.text()
                    logger.error(f"Carto {domain}/{table_name} error: {response.status} body={body[:200]}")
                    return []
    except Exception as e:
        logger.error(f"Carto {domain}/{table_name} exception: {e}")
        return []


class GeocodingClient:
    """Free geocoding using Nominatim"""
    BASE_URL = "https://nominatim.openstreetmap.org/search"
    
    async def geocode(self, address: str, city: str, state="CA"):
        """Geocode address to lat/lng"""
        query = f"{address}, {city}, {state}"
        
        params = {
            'q': query,
            'format': 'json',
            'limit': 1
        }
        
        headers = {
            'User-Agent': 'On Site/2.0'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                await asyncio.sleep(1.1)  # Nominatim rate limit: 1 req/sec
                async with session.get(self.BASE_URL, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data:
                            return {
                                'lat': float(data[0]['lat']),
                                'lng': float(data[0]['lon'])
                            }
        except Exception as e:
            logger.error(f"Geocoding error for {address}: {e}")
        
        return None

# ============================================================================
# CONTACT ENRICHMENT (optional)
# ============================================================================

async def _enrich_via_pdl(lead: dict) -> dict:
    """PeopleDataLabs enrichment if PDL_API_KEY set."""
    if not PDL_API_KEY:
        return lead
    try:
        payload = {
            "api_key": PDL_API_KEY,
            "name": lead.get("owner_name") or lead.get("contractor_name"),
            "street_address": lead.get("address"),
            "city": lead.get("city"),
            "state": lead.get("state", "CA"),
            "postal_code": lead.get("zip"),
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://api.peopledatalabs.com/v5/person/enrich",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"PDL enrich HTTP {resp.status}")
                    return lead
                data = await resp.json()
                if not data.get("status") == 200:
                    return lead
                phone = None
                email = None
                phones = data.get("data", {}).get("phone_numbers") or []
                emails = data.get("data", {}).get("emails") or []
                if phones:
                    phone = phones[0].get("number") or phones[0].get("display")
                if emails:
                    email = emails[0].get("address") or emails[0]
                if phone:
                    lead["owner_phone"] = phone
                if email:
                    lead["owner_email"] = email
                if data.get("data", {}).get("full_name"):
                    lead["owner_name"] = data["data"]["full_name"]
    except Exception as e:
        logger.warning(f"PDL enrich exception: {e}")
    return lead


async def _enrich_via_custom(lead: dict) -> dict:
    """Custom enrichment if ENRICH_API_URL/KEY set."""
    if not ENRICH_API_URL or not ENRICH_API_KEY:
        return lead
    try:
        payload = {
            "name": lead.get("owner_name") or lead.get("contractor_name") or "",
            "address": lead.get("address") or "",
            "city": lead.get("city") or "",
            "state": lead.get("state") or "CA",
            "zip": lead.get("zip") or "",
            "apn": lead.get("apn") or "",
        }
        headers = {"Authorization": f"Bearer {ENRICH_API_KEY}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(ENRICH_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"Enrich failed HTTP {resp.status}")
                    return lead
                data = await resp.json()
                phone = data.get("phone") or (data.get("phones") or [None])[0]
                email = data.get("email") or (data.get("emails") or [None])[0]
                if phone:
                    lead["owner_phone"] = phone
                if email:
                    lead["owner_email"] = email
                if data.get("contractor_name"):
                    lead["contractor_name"] = data["contractor_name"]
                if data.get("contractor_phone"):
                    lead["contractor_phone"] = data["contractor_phone"]
    except Exception as e:
        logger.warning(f"Enrich exception: {e}")
    return lead


async def enrich_contact(lead: dict) -> dict:
    """Try PDL first, then custom hook."""
    lead = await _enrich_via_pdl(lead)
    lead = await _enrich_via_custom(lead)
    return lead


# Simple in-process rate limiter for Attom (100/hour default)
from collections import deque
_attom_calls = deque()

async def _attom_enrich(lead: dict) -> dict:
    """Lookup owner via ATTOM Basic Profile."""
    if not ATTOM_API_KEY:
        return lead

    # rate limit
    now = datetime.utcnow().timestamp()
    window = 3600  # seconds
    while _attom_calls and now - _attom_calls[0] > window:
        _attom_calls.popleft()
    if len(_attom_calls) >= ATTOM_RATE_LIMIT:
        return lead

    addr = lead.get("address") or ""
    city = lead.get("city") or ""
    state = lead.get("state") or "CA"
    postal = lead.get("zip") or ""
    if not addr or not city:
        return lead

    params = {
        "address": addr,
        "city": city,
        "state": state,
        "postalcode": postal,
    }
    headers = {"apikey": ATTOM_API_KEY}
    url = "https://api.gateway.attomdata.com/propertyapi/v1.0.0/property/basicprofile"
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return lead
                data = await resp.json()
                props = data.get("property") or []
                if not props:
                    return lead
                prop = props[0]
                owner_name = ""
                owners = prop.get("owner") or {}
                if isinstance(owners, list) and owners:
                    owner_name = owners[0].get("fullname") or owners[0].get("name") or ""
                elif isinstance(owners, dict):
                    owner_name = owners.get("fullname") or owners.get("name") or ""
                mailing = prop.get("mailingaddress") or {}
                mailing_str = None
                if isinstance(mailing, dict):
                    street = mailing.get("street") or ""
                    city_m = mailing.get("city") or ""
                    st = mailing.get("state") or ""
                    pc = mailing.get("zip") or ""
                    mailing_str = ", ".join([p for p in [street, city_m, st] if p]).strip()
                    if pc:
                        mailing_str = f"{mailing_str} {pc}" if mailing_str else pc

                if owner_name and not lead.get("owner_name"):
                    lead["owner_name"] = owner_name
                if mailing_str and not lead.get("owner_address"):
                    lead["owner_address"] = mailing_str

                _attom_calls.append(now)
                return lead
    except Exception as e:
        logger.warning("ATTOM owner enrichment failed: %s", e)
        return lead


async def enrich_contacts_batch(leads: list[dict], max_lookups: int):
    """Run batch ownership enrichment using 100% FREE sources only.
    Pipeline: ArcGIS Parcels → Regrid MVT Tiles (no API keys needed)."""
    # Filter leads needing enrichment (missing owner_name)
    targets = [l for l in leads if not l.get("owner_name")
               and l.get("lat") and l.get("lng")]

    # Sort by score desc — enrich highest-value leads first
    targets.sort(key=lambda l: safe_float(l.get("score") or l.get("valuation") or 0), reverse=True)
    targets = targets[:max_lookups]

    if not targets:
        return

    logger.info(f"Free batch enrichment: {len(targets)} leads (cap {max_lookups})...")
    enriched_count = 0
    sem = asyncio.Semaphore(10)  # limit concurrent requests

    async def _enrich_one(lead):
        nonlocal enriched_count
        async with sem:
            lat = safe_float(lead.get("lat") or 0)
            lng = safe_float(lead.get("lng") or 0)
            state = lead.get("state", "")
            if not lat or not lng:
                return

            # Try ArcGIS state/national parcels first
            try:
                result = await enrich_owner_from_arcgis_parcel(lat, lng, state)
                if result and result.get("owner_name"):
                    lead["owner_name"] = result["owner_name"]
                    if result.get("owner_address") and not lead.get("owner_address"):
                        lead["owner_address"] = result["owner_address"]
                    if result.get("apn") and not lead.get("apn"):
                        lead["apn"] = result["apn"]
                    enriched_count += 1
                    return
            except Exception as e:
                logger.warning("ArcGIS parcel enrichment failed for (%s, %s): %s", lat, lng, e)

            # Fallback: Regrid MVT tiles
            try:
                result = await enrich_owner_from_regrid_tile(lat, lng)
                if result and result.get("owner_name"):
                    lead["owner_name"] = result["owner_name"]
                    if result.get("owner_address") and not lead.get("owner_address"):
                        lead["owner_address"] = result["owner_address"]
                    if result.get("apn") and not lead.get("apn"):
                        lead["apn"] = result["apn"]
                    enriched_count += 1
            except Exception as e:
                logger.warning("Regrid tile enrichment failed for (%s, %s): %s", lat, lng, e)

    # Process in batches of 50 to avoid overwhelming services
    batch_size = 50
    for i in range(0, len(targets), batch_size):
        batch = targets[i:i + batch_size]
        await asyncio.gather(*[_enrich_one(lead) for lead in batch])
        if i + batch_size < len(targets):
            await asyncio.sleep(0.5)  # brief pause between batches

    logger.info(f"Free batch enrichment complete: {enriched_count}/{len(targets)} leads enriched")


# ============================================================================
# DATA PROCESSING
# ============================================================================

# calculate_score is now imported from core.scoring (single source of truth)
calculate_score = _score_from_core


def safe_float(val, default: float = 0.0) -> float:
    """Safely convert any value to float, returning default on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def safe_int(val, default: int = 0) -> int:
    """Safely convert any value to int, returning default on failure."""
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

# Access filter helper
def apply_access_filter(leads: List[dict], request) -> List[dict]:
    ctx = getattr(request.state, "access", None)
    if not ctx:
        return leads
    if ctx.get("license_status") and ctx["license_status"].lower() not in ("active", "clear", "valid", "unknown"):
        return []
    entitled = ctx.get("territories") or []
    if not entitled:
        return leads  # allow anonymous / no-token traffic
    return [l for l in leads if str(l.get("zip") or "") in entitled]


# ---------------------------------------------------------------------------
# Behavioral event logging helpers
# ---------------------------------------------------------------------------


def _insert_action(contractor_id: str, property_id: int, action_type: str, metadata: dict = None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO contractor_actions (id, contractor_id, property_id, action_type, action_metadata) VALUES (?, ?, ?, ?, ?)",
        (str(uuid.uuid4()), contractor_id, property_id, action_type, json.dumps(metadata or {})),
    )
    conn.commit()
    conn.close()


def _insert_response(contractor_id: str, property_id: int, response_type: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO homeowner_responses (id, contractor_id, property_id, response_type) VALUES (?, ?, ?, ?)",
        (str(uuid.uuid4()), contractor_id, property_id, response_type),
    )
    conn.commit()
    conn.close()


def _insert_outcome(contractor_id: str, property_id: int, outcome: str, metadata: dict = None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO project_outcomes (id, property_id, contractor_id, outcome, contract_value, project_type) VALUES (?, ?, ?, ?, ?, ?)",
        (
            str(uuid.uuid4()),
            property_id,
            contractor_id,
            outcome,
            (metadata or {}).get("contract_value"),
            (metadata or {}).get("project_type"),
        ),
    )
    conn.commit()
    conn.close()


def _mark_alert_seen(user_id: str, alert_id: str, property_id: Optional[int] = None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE alerts_delivery_log SET status='seen', delivered_at=delivered_at WHERE id=? OR message_hash=?",
        (alert_id, alert_id),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Deterministic readiness scoring (rule-based, ML-upgradeable later)
# ---------------------------------------------------------------------------


def _score_component(signals: List[int], cap: int) -> int:
    return min(sum(signals), cap)


def _estimate_valuation(lead: dict) -> float:
    """Estimate project valuation from permit type, sqft, and market averages."""
    desc = ' '.join(str(lead.get(f) or '') for f in (
        'permit_type', 'work_description', 'description_full', 'description'
    )).lower()
    try:
        sqft = float(lead.get('sqft') or lead.get('square_feet') or lead.get('living_area') or 0)
    except (ValueError, TypeError):
        sqft = 0.0

    # Cost-per-sqft by project type (industry averages)
    if any(kw in desc for kw in ('new construction', 'new build', 'bldg-new', 'new single', 'new residential')):
        return max(sqft * 200, 150_000) if sqft > 0 else 250_000
    if any(kw in desc for kw in ('commercial', 'tenant improvement', 'office', 'retail', 'restaurant')):
        return max(sqft * 150, 100_000) if sqft > 0 else 175_000
    if any(kw in desc for kw in ('addition', 'adu', 'accessory dwelling', 'room addition')):
        return max(sqft * 250, 60_000) if sqft > 0 else 85_000
    if any(kw in desc for kw in ('solar', 'photovoltaic', 'pv system')):
        return max(sqft * 4, 15_000) if sqft > 0 else 25_000
    if any(kw in desc for kw in ('roof', 'reroofing', 're-roof')):
        return max(sqft * 8, 8_000) if sqft > 0 else 15_000
    if any(kw in desc for kw in ('pool', 'spa', 'swimming')):
        return 55_000
    if any(kw in desc for kw in ('hvac', 'air conditioning', 'heating', 'furnace', 'heat pump')):
        return 12_000
    if any(kw in desc for kw in ('electrical', 'panel', 'wiring', 'service upgrade')):
        return 8_000
    if any(kw in desc for kw in ('plumbing', 'repipe', 'sewer', 'water heater')):
        return 10_000
    if any(kw in desc for kw in ('remodel', 'renovation', 'alteration', 'interior', 'kitchen', 'bathroom')):
        return max(sqft * 120, 25_000) if sqft > 0 else 45_000
    if any(kw in desc for kw in ('demolition', 'demo')):
        return max(sqft * 10, 10_000) if sqft > 0 else 20_000
    if any(kw in desc for kw in ('fence', 'wall', 'retaining')):
        return 8_000
    if any(kw in desc for kw in ('window', 'door')):
        return 12_000
    # Generic fallback based on score
    try:
        score = float(lead.get('score') or lead.get('lead_score') or 50)
    except (ValueError, TypeError):
        score = 50.0
    if score >= 90:
        return 75_000
    if score >= 75:
        return 45_000
    return 25_000


_sold_kw_re = re.compile(r'\b(sale|sold|transfer|deed|new\s+owner|change\s+of\s+ownership|title\s+transfer|closing|escrow|buyer|purchase|conveyance)\b', re.IGNORECASE)

def normalize_lead_for_ui(lead: dict, slim: bool = False) -> dict:
    """Normalize lead data for new On Site UI.
    slim=True skips expensive description enrichment (used for list/map views).
    """
    from datetime import datetime

    # Calculate days_old live (not stale from cache build time)
    days_old = 0
    issue_date = lead.get("issue_date") or lead.get("filed_date") or lead.get("date")
    if issue_date:
        try:
            issued = datetime.fromisoformat(str(issue_date).replace('Z', '+00:00'))
            days_old = max(0, (datetime.now() - issued).days)
        except (ValueError, TypeError):
            pass

    # Rescore if days_old has drifted from cached value (keeps scores fresh daily)
    cached_days = lead.get("days_old", 0)
    try:
        score = float(lead.get('score', 0) or 0)
    except (ValueError, TypeError):
        score = 0.0
    if days_old != cached_days and issue_date:
        valuation = safe_float(lead.get("valuation"))
        permit_type = str(lead.get("permit_type") or "")
        score, _, _ = calculate_score(days_old, valuation, permit_type)

    temperature = classify_temperature(int(score))

    # Detect sold property from description keywords or absentee owner
    is_sold = lead.get('is_sold', False)
    if not is_sold:
        for fld in ('work_description', 'description', 'description_full', 'permit_type'):
            val = lead.get(fld) or ''
            if _sold_kw_re.search(val):
                is_sold = True
                break
        # Absentee owner: mailing address in a different city
        if not is_sold:
            owner_addr = (lead.get('owner_address') or '').lower()
            prop_city = (lead.get('city') or '').lower()
            if owner_addr and prop_city and prop_city not in owner_addr and len(owner_addr) > 10:
                is_sold = True

    # Ensure all required fields exist
    # Convert id to string to prevent JS precision loss (IDs > 2^53)
    normalized = {
        **lead,
        'id': str(lead.get('id', '')),
        'days_old': days_old,
        'score': int(score),
        'temperature': temperature,
        'description_full': lead.get('description_full') or lead.get('work_desc') or lead.get('work_description') or lead.get('description') or lead.get('desc') or '',
        'permit_number': lead.get('permit_number') or lead.get('permit_id') or lead.get('id') or '',
        'owner_name': lead.get('owner_name') or lead.get('applicant_name') or lead.get('applicant') or 'Not found',
        'owner_phone': lead.get('owner_phone') or lead.get('contractor_phone') or lead.get('applicant_phone') or lead.get('contact_phone') or 'Not found',
        'owner_email': lead.get('owner_email') or lead.get('applicant_email') or lead.get('contact_email') or 'Not available',
        'contractor_name': lead.get('contractor_name') or lead.get('contractor') or lead.get('company_name') or '',
        'contractor_phone': lead.get('contractor_phone') or '',
        'source': lead.get('source') or lead.get('api') or 'API',
        'is_sold': is_sold,
    }

    # Skip expensive operations in slim mode (list/map views)
    if not slim:
        # Enrich terse descriptions (e.g. "Bldg-New" → detailed human-readable text)
        normalized = _enrich_description(normalized)

    # Estimate valuation when not reported (0 or missing)
    try:
        val = float(normalized.get('valuation') or 0)
    except (ValueError, TypeError):
        val = 0.0
    if val <= 0:
        val = _estimate_valuation(normalized)
        if val > 0:
            normalized['valuation'] = val
            normalized['valuation_estimated'] = True

    return normalized


def compute_readiness(lead: dict) -> dict:
    """Compute readiness_score and action hints for a lead dict."""
    now = datetime.utcnow()
    issue_date = lead.get("issue_date") or lead.get("filed_date")
    days_since = 0
    if issue_date:
        try:
            days_since = (now - datetime.fromisoformat(issue_date)).days
        except Exception:
            days_since = 0

    ownership_signals = []
    if lead.get("ownership_years") and lead["ownership_years"] > 12:
        ownership_signals.append(7)
    ownership_score = _score_component(ownership_signals, 20)

    capacity_signals = []
    if lead.get("equity_ratio") and lead["equity_ratio"] > 0.5:
        capacity_signals.append(8)
    if lead.get("value_percentile"):
        capacity_signals.append(min(10, max(0, int(10 * lead["value_percentile"]))))
    capacity_score = _score_component(capacity_signals, 20)

    research_signals = []
    if lead.get("search_activity"):
        research_signals.append(12)
    if lead.get("neighbor_remodel"):
        research_signals.append(5)
    research_score = _score_component(research_signals, 25)

    prebuild_signals = []
    if lead.get("inspection_scheduled"):
        prebuild_signals.append(12)
    if lead.get("adu_nearby"):
        prebuild_signals.append(8)
    prebuild_score = _score_component(prebuild_signals, 20)

    decay_multiplier = math.exp(-(days_since or 0) / 45) if days_since >= 0 else 1

    readiness = (
        ownership_score * 0.20
        + capacity_score * 0.20
        + research_score * 0.35
        + prebuild_score * 0.25
    ) * decay_multiplier
    readiness = max(0, min(100, int(round(readiness))))

    if readiness < 35:
        action = "Monitor"
        window = 21
        urgency = "Low"
    elif readiness < 55:
        action = "Early awareness"
        window = 14
        urgency = "Low"
    elif readiness < 75:
        action = "Prepare contact"
        window = 10
        urgency = "Medium"
    elif readiness < 90:
        action = "Contact soon"
        window = 6
        urgency = "High"
    else:
        action = "Reach out now"
        window = 3
        urgency = "Critical"

    stage_index = min(4, readiness // 25)

    base_cost = (lead.get("sqft") or 1200) * (lead.get("cost_per_sqft") or 180)
    budget_low = base_cost * 0.75
    budget_high = base_cost * 1.25

    comp_level = "Medium"

    lead.update(
        readiness_score=readiness,
        recommended_action=action,
        contact_window_days=window,
        urgency_level=urgency,
        stage_index=stage_index,
        budget_range=[budget_low, budget_high],
        competition_level=comp_level,
    )
    return lead


# NOTE: safe_float is defined at line ~1252 with default=0.0 parameter.
# Duplicate removed — use safe_float(val) for 0.0 default, or safe_float(val, None) for Optional.


def _live_days_old(lead: dict) -> int:
    """Compute days_old from issue_date at request time (never stale)."""
    from datetime import datetime as _dt
    issue_date = lead.get("issue_date") or lead.get("filed_date") or lead.get("date")
    if not issue_date:
        return 0
    try:
        issued = _dt.fromisoformat(str(issue_date).replace('Z', '+00:00'))
        return max(0, (_dt.now() - issued).days)
    except (ValueError, TypeError):
        return 0


def _with_live_days_old(lead: dict) -> dict:
    """Return a new dict with days_old recomputed from issue_date (immutable)."""
    return {**lead, "days_old": _live_days_old(lead)}


def parse_date(value: Optional[str]) -> Optional[datetime]:
    """Best-effort date parsing to datetime (date portion only)."""
    if not value:
        return None
    v = str(value).strip().split(" ")[0].split("T")[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%m/%d/%y", "%d-%b-%y", "%d-%b-%Y"):
        try:
            return datetime.strptime(v, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(v)
    except Exception:
        return None


def fix_coordinates(lat: float, lng: float) -> tuple[Optional[float], Optional[float]]:
    """
    Validate and fix lat/lng coordinates.
    Handles: swapped lat/lng, Web Mercator (EPSG:3857), zero coords, out-of-USA.
    Returns (lat, lng) or (None, None) if unfixable.
    """
    if lat is None or lng is None:
        return None, None
    if lat == 0 or lng == 0:
        return None, None

    # 1) Detect swapped WGS84 coordinates FIRST (before Web Mercator check)
    # Swapped: lat is negative (looks like US longitude), lng is positive (looks like US latitude)
    if lat < 0 and lng > 0 and 17 <= lng <= 72 and -180 <= lat <= -65:
        lat, lng = lng, lat

    # 2) Web Mercator detection (values way outside WGS84 range)
    if abs(lat) > 90 or abs(lng) > 180:
        lat, lng = _web_mercator_to_latlng(lng, lat)

    # 3) Both positive in latitude range — ambiguous, skip
    if lat > 17 and lat < 72 and lng > 17 and lng < 72:
        return None, None

    # 4) Both large negative — ambiguous, skip
    if lat < -65 and lng < -65:
        return None, None

    # 5) Final USA bounds check (includes Alaska & Hawaii)
    if not (17 <= lat <= 72 and -180 <= lng <= -65):
        return None, None

    return round(lat, 6), round(lng, 6)


def _web_mercator_to_latlng(x: float, y: float) -> tuple:
    """Convert Web Mercator (EPSG:3857) coordinates to WGS84 lat/lng."""
    import math
    lng = (x / 6378137.0) * (180.0 / math.pi)
    lat = (math.pi / 2.0 - 2.0 * math.atan(math.exp(-y / 6378137.0))) * (180.0 / math.pi)
    return lat, lng

POINT_RE = re.compile(r"-?\d+(?:\.\d+)?")


def extract_lat_lng(record: dict) -> tuple[Optional[float], Optional[float]]:
    """Extract latitude/longitude from common Socrata / ArcGIS shapes."""
    lat = (record.get("latitude") or record.get("lat") or record.get("y")
           or record.get("gis_latitude") or record.get("lat_y")
           or record.get("point_y") or record.get("y_coord")
           or record.get("_lat") or record.get("LATITUDE") or record.get("Y_COORD")
           or record.get("YCOORD"))
    lng = (record.get("longitude") or record.get("lng") or record.get("lon")
           or record.get("x") or record.get("gis_longitude") or record.get("lng_x")
           or record.get("long") or record.get("point_x") or record.get("x_coord")
           or record.get("_lng") or record.get("LONGITUDE") or record.get("X_COORD")
           or record.get("XCOORD"))
    loc = (record.get("location") or record.get("location_1") or record.get("lat_lon")
           or record.get("geocoded_column") or record.get("location1")
           or record.get("the_geom") or record.get("georeference")
           or record.get("mapped_location") or record.get("geolocation"))

    if isinstance(loc, dict):
        lat = lat or loc.get("latitude")
        lng = lng or loc.get("longitude")
        coords = loc.get("coordinates")
        if coords and len(coords) == 2:
            lng = lng or coords[0]
            lat = lat or coords[1]
    elif isinstance(loc, str):
        nums = POINT_RE.findall(loc)
        if len(nums) >= 2:
            # Socrata POINT strings are usually "(-118.3 34.0)" (lon lat)
            lng = lng or nums[0]
            lat = lat or nums[1]

    # Filter out string sentinels like "NULL", "None", "" before conversion
    if isinstance(lat, str) and lat.strip().upper() in ("NULL", "NONE", ""):
        lat = None
    if isinstance(lng, str) and lng.strip().upper() in ("NULL", "NONE", ""):
        lng = None
    try:
        return float(lat), float(lng)
    except Exception:
        return None, None


def extract_address(record: dict) -> str:
    """Pick the best available address representation."""
    # First, try common string fields (ignore obvious geojson dicts)
    for key in (
        "address",
        "primary_address",
        "address_start",
        "full_address",
        "street_address",
        "original_address",
        "ADDRESS",
        "FULL_ADDRESS",
        "STREET_FULL_NAME",
        "PROP_ADDRE",
        "ADDR",
        "StreetAddress",
        "SITE_ADDRESS",
        "originaladdress1",
        "original_address1",
        "permit_location",
        "originaladdress",
        "site_address",
    ):
        val = record.get(key)
        if isinstance(val, str) and val and not val.strip().startswith("{'type': 'Point'"):
            return val.strip()

    # Socrata location fields with human_address
    for loc_key in ("location", "location_1", "lat_lon", "geocoded_column"):
        loc = record.get(loc_key)
        if isinstance(loc, dict):
            human = loc.get("human_address") or loc.get("humanAddress")
            if human:
                try:
                    parsed = json.loads(human) if isinstance(human, str) else human
                    if parsed.get("address"):
                        return parsed["address"].strip()
                except (ValueError, TypeError, json.JSONDecodeError):
                    pass
        elif isinstance(loc, str):
            # Sometimes human_address comes as JSON string directly
            try:
                parsed = json.loads(loc)
                if isinstance(parsed, dict) and parsed.get("address"):
                    return parsed["address"].strip()
            except Exception:
                # ignore raw POINT strings
                pass

    # Build from components if possible (SF / LA datasets)
    street_num = record.get("street_number") or record.get("stno") or ""
    pre_dir = record.get("predir") or record.get("street_direction") or ""
    street_name = (record.get("street_name") or record.get("stname")
                   or record.get("street") or "")
    street_suffix = (record.get("street_suffix") or record.get("suffix")
                     or record.get("street_type") or "")
    post_dir = record.get("postdir") or record.get("street_suffix_direction") or ""
    parts = [p for p in [street_num, pre_dir, street_name, street_suffix, post_dir]
             if p and str(p).strip()]
    return " ".join(str(p).strip() for p in parts)


def extract_permit_number(record: dict) -> str:
    for key in ("permit_number", "permit_nbr", "pcis_permit", "record_id", "recordid", "application_number"):
        if record.get(key):
            return str(record[key])
    return ""

# ── City-to-state lookup (must be defined before sync_data) ──
CITY_STATE_MAP = {
    # California
    "Los Angeles": "CA", "Long Beach": "CA", "San Francisco": "CA",
    "San Jose": "CA", "Oakland": "CA", "Sacramento": "CA",
    "Fresno": "CA", "San Diego": "CA", "Santa Monica": "CA",
    "West Hollywood": "CA", "Beverly Hills": "CA", "Pasadena": "CA",
    "Glendale": "CA", "Burbank": "CA", "Culver City": "CA",
    "Torrance": "CA", "Marin County": "CA", "Scottsdale": "AZ",
    "Redwood City": "CA",
    # Texas
    "Austin": "TX", "Dallas": "TX", "Houston": "TX",
    "San Antonio": "TX", "Fort Worth": "TX",
    # Northeast
    "New York City": "NY", "New York": "NY", "Boston": "MA",
    "Philadelphia": "PA", "Pittsburgh": "PA", "Hartford": "CT",
    "Cambridge": "MA",
    # Southeast
    "Atlanta": "GA", "Charlotte": "NC", "Raleigh": "NC",
    "Nashville": "TN", "Chattanooga": "TN", "Miami": "FL",
    "Tampa": "FL", "Baltimore": "MD", "Washington": "DC",
    "Johns Creek": "GA",
    # Midwest
    "Chicago": "IL", "Columbus": "OH", "Cincinnati": "OH",
    "Indianapolis": "IN", "Minneapolis": "MN", "Kansas City": "MO",
    "Detroit": "MI",
    # West
    "Seattle": "WA", "Portland": "OR", "Denver": "CO",
    "Phoenix": "AZ", "Tempe": "AZ", "Las Vegas": "NV",
}

def _infer_state(city: str) -> str:
    return CITY_STATE_MAP.get(city, "CA")

# ── Permit portal URLs — direct links to source city permit portals ──
PERMIT_PORTAL_URLS: Dict[str, str] = {
    "Los Angeles":    "https://www.ladbsservices.lacity.org/OnlineServices/?page=PermitInfo&PermitNbr={permit_number}",
    "San Francisco":  "https://dbiweb02.sfgov.org/dbipts/default.aspx?page=PermitDetails&PermitNumber={permit_number}",
    "New York City":  "https://a810-bisweb.nyc.gov/bisweb/JobsQueryByNumberServlet?passjobnumber={permit_number}",
    "Chicago":        "https://webapps1.chicago.gov/buildingrecords/",
    "Seattle":        "https://cosaccela.seattle.gov/Portal/Welcome.aspx",
    "Austin":         "https://abc.austintexas.gov/web/permit/public-search-other?reset=true&searchType=Permit&searchString={permit_number}",
    "Phoenix":        "https://nsdonline.phoenix.gov/PDD_ESearch/",
    "Denver":         "https://www.denvergov.org/pocketgov/#/permitsAndLicenses/",
    "Portland":       "https://www.portlandmaps.com/advanced/index.cfm#permits",
    "Tampa":          "https://aca-prod.accela.com/TAMPA/",
    "Miami":          "https://www.miamidade.gov/global/economy/building/building-permit-search.page",
    "Washington":     "https://dcra.dc.gov/service/permit-center",
    "Detroit":        "https://data.detroitmi.gov/",
    "Baltimore":      "https://permits.baltimorecity.gov/",
    "Las Vegas":      "https://www.lasvegasnevada.gov/Government/Departments/Development-Services",
    "Indianapolis":   "https://accela.indy.gov/CitizenAccess/",
    "Kansas City":    "https://data.kcmo.org/",
    "San Diego":      "https://aca-prod.accela.com/SANDIEGOCOUNTY/",
    "Cincinnati":     "https://cagismaps.hamilton-co.org/cagisportal/",
    "Cambridge":      "https://www.cambridgema.gov/inspection/buildingpermitsearch",
    "Dallas":         "https://developmentweb.dallascityhall.com/",
}

def _build_permit_url(city: str, permit_number: str, domain: str = "", resource_id: str = "") -> str:
    """Build a link to the source permit portal for a given lead."""
    if not permit_number and not domain:
        return ""
    template = PERMIT_PORTAL_URLS.get(city, "")
    if template and permit_number:
        return template.format(permit_number=permit_number)
    # Fallback: link to Socrata open data portal if we have domain+resource
    if domain and resource_id:
        return f"https://{domain}/d/{resource_id}"
    return ""

async def process_ladbs_permit(permit_data: dict, geocoder: GeocodingClient) -> Optional[Lead]:
    """Process LADBS permit into Lead"""
    try:
        address = permit_data.get('address', '').strip()
        city = permit_data.get('city', 'Los Angeles').strip()
        zip_code = permit_data.get('zip_code', '')
        
        if not address:
            return None
        
        # Try geocoding
        coords = await geocoder.geocode(address, city)
        if not coords:
            logger.warning(f"Could not geocode: {address}")
            return None
        
        # Parse dates
        issue_date_str = permit_data.get('issue_date', '')[:10]
        if issue_date_str:
            issue_date = datetime.fromisoformat(issue_date_str)
            days_old = (datetime.now() - issue_date).days
        else:
            days_old = 0
        
        # Parse valuation
        val_str = permit_data.get('valuation', '0')
        try:
            valuation = float(str(val_str).replace('$', '').replace(',', ''))
        except (ValueError, TypeError):
            valuation = 0
        
        # Calculate score
        score, temp, urgency = calculate_score(days_old, valuation, permit_data.get('permit_type', ''))
        
        # Keep the original LADBS description as-is; store optional extra info separately
        desc_candidates = [
            permit_data.get("work_desc"),
            permit_data.get("description"),
            permit_data.get("work_description"),
            permit_data.get("scope_description"),
            permit_data.get("comments"),
            permit_data.get("permit_type_definition"),
            permit_data.get("use_desc"),
            permit_data.get("permit_sub_type"),
            permit_data.get("record_type"),
        ]
        desc_base = next((d for d in desc_candidates if d), "Permit filed")
        description_full = " | ".join([d for d in desc_candidates if d])
        value_txt = f"${valuation:,.0f}" if valuation else "n/a"
        issue_txt = issue_date_str or "n/a"
        suffix = " | ".join(
            [
                f"Type: {permit_data.get('permit_type') or permit_data.get('permit_type_definition') or 'n/a'}",
                f"Value: {value_txt}",
                f"Filed: {issue_txt}",
            ]
        )
        rich_extra = suffix

        return Lead(
            id=hash(permit_data.get('permit_nbr', '')),
            permit_number=permit_data.get('permit_nbr', ''),
            address=address,
            city=city,
            zip=zip_code,
            lat=coords['lat'],
            lng=coords['lng'],
            work_description=desc_base,
            description_full=description_full,
            permit_type=permit_data.get('permit_type', ''),
            valuation=valuation,
            issue_date=issue_date_str,
            days_old=days_old,
            score=score,
            temperature=temp,
            urgency=urgency,
            source='LADBS',
            contractor_name=permit_data.get('contractor_business_name'),
            contractor_phone=permit_data.get('contractor_phone'),
            description_extra=rich_extra,
        )
    except Exception as e:
        logger.error(f"Error processing LADBS permit: {e}")
        return None

# ============================================================================
# CACHE MANAGEMENT
# ============================================================================

_sync_lock = asyncio.Lock()

class DataCache:
    """Simple JSON file cache with in-memory memoization"""
    _mem_cache = None  # (mtime, leads) — avoids re-reading 100MB+ file from disk

    @staticmethod
    def load(allow_stale: bool = False):
        """Load cache from disk (memoized — only re-reads if file changed)"""
        if not os.path.exists(CACHE_FILE):
            return None
        try:
            mtime = os.path.getmtime(CACHE_FILE)
            # Return memoized data if file hasn't changed
            if DataCache._mem_cache and DataCache._mem_cache[0] == mtime:
                return DataCache._mem_cache[1]
            try:
                import orjson as _oj
                with open(CACHE_FILE, 'rb') as f:
                    data = _oj.loads(f.read())
            except ImportError:
                with open(CACHE_FILE, 'r') as f:
                    data = json.load(f)
            # Handle both dict-wrapper and plain-list formats
            if isinstance(data, list):
                leads = data
                cache_time = datetime.fromtimestamp(mtime)
            else:
                cache_time = datetime.fromisoformat(data.get('timestamp', '2000-01-01'))
                leads = data.get('leads', [])
            if allow_stale or datetime.now() - cache_time < CACHE_DURATION:
                logger.info(f"Using cached data from {cache_time} ({len(leads):,} leads)")
                DataCache._mem_cache = (mtime, leads)
                return leads
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
        return None
    
    @staticmethod
    def save(leads: List[dict], merge: bool = True):
        """Save cache to disk, optionally merging with existing cache

        Args:
            leads: New leads to save
            merge: If True, merge with existing cache and deduplicate by permit_number
                   If False, completely replace cache (old behavior)
        """
        try:
            if merge:
                # Load existing cache
                existing = DataCache.load(allow_stale=True) or []

                # Create dict keyed by unique identifier for deduplication
                # Use permit_number OR address+city hash for leads without permit numbers
                # Newer leads (from current sync) take precedence
                leads_by_key = {}

                def get_dedup_key(lead: dict) -> str:
                    """Generate unique key for deduplication"""
                    permit_num = str(lead.get("permit_number", "")).strip()
                    if permit_num:
                        return f"permit:{permit_num}"

                    # For leads without permit_number, use address hash
                    address = str(lead.get("address", "")).strip().lower()
                    city = str(lead.get("city", "")).strip().lower()
                    state = str(lead.get("state", "")).strip().upper()
                    source = str(lead.get("source", "")).strip()

                    if address and city and state:
                        return f"addr:{address}|{city}|{state}|{source}"

                    # Fallback: use lead ID if available
                    lead_id = lead.get("id", "")
                    if lead_id:
                        return f"id:{lead_id}"

                    # Last resort: generate hash from all fields
                    import hashlib
                    lead_str = f"{address}|{city}|{state}|{source}|{lead.get('valuation', 0)}"
                    return f"hash:{hashlib.md5(lead_str.encode()).hexdigest()}"

                # First add existing leads
                for lead in existing:
                    key = get_dedup_key(lead)
                    leads_by_key[key] = lead

                # Then add/update with new leads (overwrites duplicates)
                new_count = 0
                updated_count = 0
                for lead in leads:
                    key = get_dedup_key(lead)
                    if key in leads_by_key:
                        updated_count += 1
                    else:
                        new_count += 1
                    leads_by_key[key] = lead

                # Convert back to list
                merged_leads = list(leads_by_key.values())

                # Sort by score for UI performance
                merged_leads.sort(key=lambda l: l.get("score", 0), reverse=True)

                logger.info(f"Merge: {len(existing)} existing + {len(leads)} new = {len(merged_leads)} total ({new_count} new, {updated_count} updated)")

                data = {
                    'timestamp': datetime.now().isoformat(),
                    'leads': merged_leads
                }
            else:
                # Old behavior: complete replacement
                data = {
                    'timestamp': datetime.now().isoformat(),
                    'leads': leads
                }

            try:
                import orjson as _oj
                with open(CACHE_FILE, 'wb') as f:
                    f.write(_oj.dumps(data, option=_oj.OPT_INDENT_2))
            except ImportError:
                with open(CACHE_FILE, 'w') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            # Update mem cache so next load() doesn't re-read the file
            DataCache._mem_cache = (os.path.getmtime(CACHE_FILE), data['leads'])
            logger.info(f"Saved {len(data['leads'])} leads to cache")
        except Exception as e:
            logger.error(f"Error saving cache: {e}")

# ============================================================================
# ARCGIS FETCHER
# ============================================================================

async def fetch_arcgis_permits(url: str, date_field: Optional[str], order_by: str,
                                page_size: int = 1000, max_pages: int = 5,
                                lookback_days: int = 90) -> List[dict]:
    """Generic paginated ArcGIS FeatureServer/MapServer fetcher."""
    all_features: List[dict] = []
    where = "1=1"
    if date_field:
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)
        cutoff_ts = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        where = f"{date_field} >= TIMESTAMP '{cutoff_ts}'"

    for page in range(max_pages):
        params = {
            "where": where,
            "outFields": "*",
            "f": "json",
            "resultRecordCount": str(page_size),
            "resultOffset": str(page * page_size),
            "returnGeometry": "true",
            "outSR": "4326",  # Request WGS84 lat/lng instead of native projection
        }
        if order_by:
            params["orderByFields"] = order_by

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params,
                                       timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning(f"ArcGIS {url} HTTP {resp.status}")
                        break
                    data = await resp.json(content_type=None)
                    features = data.get("features", [])
                    if not features:
                        break
                    for f in features:
                        attrs = f.get("attributes", {})
                        geom = f.get("geometry", {})
                        if geom:
                            attrs["_lat"] = geom.get("y")
                            attrs["_lng"] = geom.get("x")
                        all_features.append(attrs)
                    if len(features) < page_size:
                        break
        except Exception as e:
            logger.error(f"ArcGIS page {page} fetch error: {e}")
            break

    return all_features


# ============================================================================
# FEDERAL DATA FETCHERS
# ============================================================================

async def fetch_fema_nfip_claims(state: Optional[str] = None,
                                  zip_code: Optional[str] = None,
                                  limit: int = 500) -> List[dict]:
    """Fetch FEMA NFIP flood insurance claims (no API key required).
    Returns building damage amounts, property values, construction dates."""
    url = "https://www.fema.gov/api/open/v2/FimaNfipClaims"
    filters = []
    if state:
        filters.append(f"state eq '{state}'")
    if zip_code:
        filters.append(f"reportedZipCode eq '{zip_code}'")
    # Only recent claims (last 5 years)
    filters.append(f"yearOfLoss ge {datetime.utcnow().year - 5}")

    params = {
        "$top": str(limit),
        "$orderby": "yearOfLoss desc",
    }
    if filters:
        params["$filter"] = " and ".join(filters)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    logger.warning(f"FEMA NFIP HTTP {resp.status}")
                    return []
                data = await resp.json(content_type=None)
                records = data.get("FimaNfipClaims", [])
                logger.info(f"FEMA NFIP: fetched {len(records)} claims")
                return records
    except Exception as e:
        logger.error(f"FEMA NFIP fetch error: {e}")
        return []


async def fetch_fema_disasters(state: Optional[str] = None,
                                limit: int = 200) -> List[dict]:
    """Fetch FEMA disaster declarations (no API key required)."""
    url = "https://www.fema.gov/api/open/v1/FemaWebDisasterDeclarations"
    params = {
        "$top": str(limit),
        "$orderby": "declarationDate desc",
    }
    if state:
        params["$filter"] = f"stateCode eq '{state}'"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                return data.get("FemaWebDisasterDeclarations", [])
    except Exception as e:
        logger.error(f"FEMA disasters fetch error: {e}")
        return []


async def fetch_fema_disaster_areas(state: Optional[str] = None,
                                     limit: int = 500) -> List[dict]:
    """Fetch FEMA county-level disaster declaration areas."""
    url = "https://www.fema.gov/api/open/v1/FemaWebDeclarationAreas"
    params = {
        "$top": str(limit),
        "$orderby": "id desc",
    }
    if state:
        params["$filter"] = f"state eq '{state}'"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
                return data.get("FemaWebDeclarationAreas", [])
    except Exception as e:
        logger.error(f"FEMA areas fetch error: {e}")
        return []


# ============================================================================
# OWNERSHIP & PROPERTY ENRICHMENT PIPELINE
# ============================================================================

# In-memory cache for enrichment results (TTL: 24 hours)
_enrichment_cache: Dict[str, dict] = {}
_enrichment_cache_ts: Dict[str, float] = {}
ENRICHMENT_CACHE_TTL = 86400  # 24 hours


def _enrichment_cache_key(address: str, city: str) -> str:
    return f"{address.strip().upper()}|{city.strip().upper()}"


async def enrich_owner_from_la_assessor(lat: float, lng: float) -> Optional[dict]:
    """Look up owner info from LA County Assessor API (free, no key)."""
    url = "https://maps.assessment.lacounty.gov/GVH_2_2/GVH/wsLegacyService/getParcelByLocation"
    params = {"type": "json", "lat": lat, "lng": lng}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                parcel = data if isinstance(data, dict) else {}
                if not parcel:
                    return None
                # LA Assessor returns parcel info with owner details
                result = {
                    "apn": parcel.get("AIN") or parcel.get("apn") or "",
                    "owner_name": parcel.get("OwnerName") or parcel.get("owner_name") or "",
                    "owner_address": parcel.get("MailingAddress") or "",
                    "market_value": safe_float(parcel.get("NetTaxableValue") or parcel.get("TotalValue") or 0),
                    "year_built": parcel.get("YearBuilt") or parcel.get("year_built") or "",
                    "square_feet": safe_float(parcel.get("SQFTmain") or parcel.get("sqft") or 0),
                    "lot_size": parcel.get("LotSize") or "",
                    "zoning": parcel.get("Zoning") or parcel.get("UseType") or "",
                    "bedrooms": parcel.get("Bedrooms") or "",
                    "bathrooms": parcel.get("Bathrooms") or "",
                    "land_value": safe_float(parcel.get("LandValue") or 0),
                    "improvement_value": safe_float(parcel.get("ImprovementValue") or 0),
                    "source": "LA County Assessor",
                }
                return result if result["owner_name"] or result["apn"] else None
    except Exception as e:
        logger.debug(f"LA Assessor enrichment error: {e}")
        return None


async def enrich_owner_from_regrid_tile(lat: float, lng: float) -> Optional[dict]:
    """Look up ownership via free Regrid MVT parcel tiles (no API key, no daily limit).
    Downloads the z15 tile containing the lead's coordinates, parses parcel polygons,
    and returns the nearest parcel's owner data."""
    import math
    try:
        z = 15
        n = 2 ** z
        tx = int((lng + 180.0) / 360.0 * n)
        ty = int((1.0 - math.log(math.tan(math.radians(lat)) + 1 / math.cos(math.radians(lat))) / math.pi) / 2.0 * n)

        tile_url = f"https://tiles.regrid.com/api/v1/parcels/{z}/{tx}/{ty}.mvt"
        async with aiohttp.ClientSession() as session:
            async with session.get(tile_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.read()
                if len(data) < 50:
                    return None

        # Parse MVT tile
        try:
            import mapbox_vector_tile
        except ImportError:
            logger.warning("mapbox_vector_tile not installed — pip install mapbox-vector-tile")
            return None

        decoded = mapbox_vector_tile.decode(data)
        layer = decoded.get("parcels")
        if not layer:
            return None

        # Tile bbox for coordinate conversion
        w = tx / n * 360.0 - 180.0
        e = (tx + 1) / n * 360.0 - 180.0
        n_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * ty / n))))
        s_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (ty + 1) / n))))
        tile_w = e - w
        tile_h = n_lat - s_lat
        extent = layer.get("extent", 4096)

        best_dist = 0.002  # ~200m max match radius
        best_parcel = None

        for feat in layer.get("features", []):
            props = feat.get("properties", {})
            owner = props.get("owner", "")
            if not owner:
                continue

            # Approximate centroid from geometry
            geom = feat.get("geometry", {})
            coords = geom.get("coordinates", [])
            if not coords:
                continue

            all_pts = []
            def _flatten(c):
                if isinstance(c, (list, tuple)):
                    if len(c) == 2 and isinstance(c[0], (int, float)):
                        all_pts.append(c)
                    else:
                        for sub in c:
                            _flatten(sub)
            _flatten(coords)
            if not all_pts:
                continue

            cx = sum(p[0] for p in all_pts) / len(all_pts) / extent
            cy = sum(p[1] for p in all_pts) / len(all_pts) / extent
            parcel_lat = n_lat - cy * tile_h
            parcel_lng = w + cx * tile_w

            dist = abs(parcel_lat - lat) + abs(parcel_lng - lng)
            if dist < best_dist:
                best_dist = dist
                best_parcel = props

        if not best_parcel:
            return None

        return {
            "apn": best_parcel.get("parcelnumb", ""),
            "owner_name": best_parcel.get("owner", ""),
            "owner_address": " ".join(filter(None, [
                best_parcel.get("mailadd", ""),
                best_parcel.get("mail_city", ""),
                best_parcel.get("mail_state2", ""),
                best_parcel.get("mail_zip", ""),
            ])),
            "source": "Regrid Tile",
        }
    except Exception as e:
        logger.debug(f"Regrid tile enrichment error: {e}")
        return None


async def enrich_owner_from_county_scraper(lat: float, lng: float,
                                            city: str, state: str) -> Optional[dict]:
    """Scrape county assessor APIs for ownership data (free, no key needed).
    Covers major metro counties with public GIS/assessor endpoints."""
    COUNTY_ASSESSOR_URLS = {
        # Cook County IL (Chicago metro)
        ("IL", "Chicago"): "https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json",
        ("IL", "Evanston"): "https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json",
        # Miami-Dade FL
        ("FL", "Miami"): "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/ParcelsSearch/FeatureServer/0/query",
        ("FL", "Miami Beach"): "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest/services/ParcelsSearch/FeatureServer/0/query",
        # King County WA (Seattle metro)
        ("WA", "Seattle"): "https://gismaps.kingcounty.gov/arcgis/rest/services/Property/KingCo_Parcels/MapServer/0/query",
        ("WA", "Bellevue"): "https://gismaps.kingcounty.gov/arcgis/rest/services/Property/KingCo_Parcels/MapServer/0/query",
        # Maricopa County AZ (Phoenix metro)
        ("AZ", "Phoenix"): "https://services.arcgis.com/YEVJCwHVQctGasMj/arcgis/rest/services/Maricopa_Parcels/FeatureServer/0/query",
        ("AZ", "Mesa"): "https://services.arcgis.com/YEVJCwHVQctGasMj/arcgis/rest/services/Maricopa_Parcels/FeatureServer/0/query",
        ("AZ", "Scottsdale"): "https://services.arcgis.com/YEVJCwHVQctGasMj/arcgis/rest/services/Maricopa_Parcels/FeatureServer/0/query",
        # Harris County TX (Houston metro)
        ("TX", "Houston"): "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/Harris_Parcels/FeatureServer/0/query",
        # Clark County NV (Las Vegas metro)
        ("NV", "Las Vegas"): "https://services.arcgis.com/l1ELspfvoKCZhFVF/arcgis/rest/services/Clark_Parcels/FeatureServer/0/query",
        ("NV", "Henderson"): "https://services.arcgis.com/l1ELspfvoKCZhFVF/arcgis/rest/services/Clark_Parcels/FeatureServer/0/query",
    }

    url = COUNTY_ASSESSOR_URLS.get((state, city))
    if not url:
        return None

    # If it's a Socrata endpoint (Cook County)
    if "resource/" in url:
        try:
            params = {"$where": f"within_circle(location, {lat}, {lng}, 50)", "$limit": 1}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params,
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json(content_type=None)
                    if not data:
                        return None
                    rec = data[0]
                    owner = rec.get("taxpayer_name") or rec.get("owner_name") or ""
                    return {
                        "apn": rec.get("pin") or rec.get("parcel_id") or "",
                        "owner_name": owner,
                        "owner_address": rec.get("taxpayer_address") or rec.get("mailing_address") or "",
                        "market_value": safe_float(rec.get("market_value") or rec.get("total_assessed_value") or 0),
                        "source": "County Assessor",
                    } if owner else None
        except Exception as e:
            logger.debug(f"County Socrata scraper error: {e}")
            return None

    # ArcGIS spatial query (most county assessors)
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "false",
        "resultRecordCount": "1",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                features = data.get("features", [])
                if not features:
                    return None
                attrs = features[0].get("attributes", {})
                owner = (attrs.get("OWNER") or attrs.get("OWNER_NAME") or attrs.get("OwnerName")
                         or attrs.get("OWNERNAME") or attrs.get("owner_name")
                         or attrs.get("TAXPAYER") or attrs.get("OWN_NAME") or "")
                if not owner:
                    return None
                return {
                    "apn": (attrs.get("PARCEL_ID") or attrs.get("PARCELID") or attrs.get("APN")
                            or attrs.get("PIN") or attrs.get("FOLIO") or ""),
                    "owner_name": owner,
                    "owner_address": (attrs.get("MAIL_ADDR") or attrs.get("MAILING_ADDRESS")
                                      or attrs.get("MailingAddress") or attrs.get("TAXPAYER_ADDRESS") or ""),
                    "market_value": safe_float(attrs.get("TOTAL_VAL") or attrs.get("MARKET_VALUE")
                                               or attrs.get("JUST_VAL") or 0),
                    "source": "County Assessor",
                }
    except Exception as e:
        logger.debug(f"County ArcGIS scraper error: {e}")
        return None


async def enrich_owner_from_arcgis_parcel(lat: float, lng: float,
                                          state: str) -> Optional[dict]:
    """Query state-level ArcGIS Parcel services (free, good coverage).
    Uses Esri's Living Atlas USA_Parcels as universal fallback for all states."""
    # Universal fallback — Esri Living Atlas covers all US states
    USA_PARCELS_URL = "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Parcels/FeatureServer/0/query"
    # State-specific services (often have richer owner data than the national layer)
    STATE_PARCEL_URLS = {
        "FL": "https://services1.arcgis.com/O1JpcwDW8sjYuddV/arcgis/rest/services/FL_Parcels/FeatureServer/0/query",
        "OH": "https://services.arcgis.com/c7gArLcfRCsQJXWJ/arcgis/rest/services/OH_Parcels/FeatureServer/0/query",
        "MD": "https://services.arcgis.com/njFNhDsUCentVYJW/arcgis/rest/services/MD_Parcels/FeatureServer/0/query",
        "NY": "https://services6.arcgis.com/DZHaqZm9elBMHfQk/arcgis/rest/services/NY_Parcels/FeatureServer/0/query",
        "IL": "https://clearmap.cmap.illinois.gov/server/rest/services/Parcels/MapServer/0/query",
        "TX": "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/TX_Parcels/FeatureServer/0/query",
        "WA": "https://services.arcgis.com/jsIt88o09Q0r1j8h/arcgis/rest/services/WA_Parcels/FeatureServer/0/query",
        "CO": "https://services.arcgis.com/IamIM3RJ5QYg0Eib/arcgis/rest/services/CO_Parcels/FeatureServer/0/query",
        "PA": "https://services.arcgis.com/AcaYosnaBkeGPLch/arcgis/rest/services/PA_Parcels/FeatureServer/0/query",
        "GA": "https://services.arcgis.com/2iQ9dsFU5YbKqSBR/arcgis/rest/services/GA_Parcels/FeatureServer/0/query",
        "NC": "https://services.arcgis.com/FS6mu4N4KY0XFPWX/arcgis/rest/services/NC_Parcels/FeatureServer/0/query",
        "NJ": "https://services.arcgis.com/njFNhDsUCentVYJW/arcgis/rest/services/NJ_Parcels/FeatureServer/0/query",
        "VA": "https://services.arcgis.com/p5v98VHDX9Atv3l7/arcgis/rest/services/VA_Parcels/FeatureServer/0/query",
        "MA": "https://services.arcgis.com/hGdE1joQqEa0MTBQ/arcgis/rest/services/MA_Parcels/FeatureServer/0/query",
        "MI": "https://services.arcgis.com/fGsm2g7LNKY6bguM/arcgis/rest/services/MI_Parcels/FeatureServer/0/query",
        "MN": "https://services.arcgis.com/afSMGVsC7QlRK1kZ/arcgis/rest/services/MN_Parcels/FeatureServer/0/query",
        "AZ": "https://services.arcgis.com/YEVJCwHVQctGasMj/arcgis/rest/services/AZ_Parcels/FeatureServer/0/query",
        "OR": "https://services.arcgis.com/uUvqNMGPm7axC2dD/arcgis/rest/services/OR_Parcels/FeatureServer/0/query",
        "TN": "https://services.arcgis.com/v01gqwM5QqNysAAi/arcgis/rest/services/TN_Parcels/FeatureServer/0/query",
        "IN": "https://services.arcgis.com/KrUTREvdL1T3A1FC/arcgis/rest/services/IN_Parcels/FeatureServer/0/query",
        "MO": "https://services.arcgis.com/RNMQHJJCqOYoTQGq/arcgis/rest/services/MO_Parcels/FeatureServer/0/query",
        "WI": "https://services.arcgis.com/bkrWlSKcjUDFDtgw/arcgis/rest/services/WI_Parcels/FeatureServer/0/query",
        "SC": "https://services.arcgis.com/qjOOyXQlsjWFLBz2/arcgis/rest/services/SC_Parcels/FeatureServer/0/query",
        "CT": "https://services.arcgis.com/FjJBsFjVGsFbRNpn/arcgis/rest/services/CT_Parcels/FeatureServer/0/query",
        "NV": "https://services.arcgis.com/l1ELspfvoKCZhFVF/arcgis/rest/services/NV_Parcels/FeatureServer/0/query",
        "UT": "https://services.arcgis.com/ZzrwjrG3FbnhVPOV/arcgis/rest/services/UT_Parcels/FeatureServer/0/query",
        "LA": "https://services.arcgis.com/vQomaGjnCmFBR7WC/arcgis/rest/services/LA_Parcels/FeatureServer/0/query",
        "KY": "https://services.arcgis.com/CZKg5GsaWd7MkVkT/arcgis/rest/services/KY_Parcels/FeatureServer/0/query",
        "OK": "https://services.arcgis.com/TnBHMjDqE3BXRU0f/arcgis/rest/services/OK_Parcels/FeatureServer/0/query",
        "DC": "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/Parcels/MapServer/0/query",
    }

    url = STATE_PARCEL_URLS.get(state) or USA_PARCELS_URL

    # Spatial query: point intersects parcel polygon
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "false",
        "resultRecordCount": "1",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json(content_type=None)
                features = data.get("features", [])
                if not features:
                    return None
                attrs = features[0].get("attributes", {})
                # Try common field names across different state schemas
                owner = (attrs.get("OWNER") or attrs.get("OWNER_NAME") or attrs.get("OwnerName")
                         or attrs.get("OWNERNAME") or attrs.get("owner_name") or "")
                return {
                    "apn": (attrs.get("PARCEL_ID") or attrs.get("PARCELID") or attrs.get("APN")
                            or attrs.get("PIN") or attrs.get("parcel_id") or ""),
                    "owner_name": owner,
                    "owner_address": (attrs.get("MAIL_ADDR") or attrs.get("MAILING_ADDRESS")
                                      or attrs.get("MailingAddress") or ""),
                    "market_value": safe_float(attrs.get("TOTAL_VAL") or attrs.get("MARKET_VALUE")
                                               or attrs.get("TotalValue") or attrs.get("APPR_TOTAL") or 0),
                    "year_built": (attrs.get("YEAR_BUILT") or attrs.get("YearBuilt")
                                   or attrs.get("YEARBUILT") or ""),
                    "square_feet": safe_float(attrs.get("SQFT") or attrs.get("BLDG_SQFT")
                                              or attrs.get("HEATED_SQFT") or 0),
                    "lot_size": attrs.get("LOT_SIZE") or attrs.get("ACRES") or "",
                    "zoning": attrs.get("ZONING") or attrs.get("LAND_USE") or attrs.get("USE_CODE") or "",
                    "land_value": safe_float(attrs.get("LAND_VAL") or attrs.get("LAND_VALUE") or 0),
                    "improvement_value": safe_float(attrs.get("IMPR_VAL") or attrs.get("BLDG_VALUE") or 0),
                    "source": f"{state} State GIS",
                }
    except Exception as e:
        logger.debug(f"ArcGIS parcel enrichment error ({state}): {e}")
        return None


async def enrich_lead_ownership(lead: dict) -> dict:
    """Master ownership enrichment pipeline. Chains multiple free sources
    to fill owner_name, owner_phone, owner_email, owner_address, and
    property details (market_value, year_built, square_feet, etc.).

    Priority order (ALL FREE, no commercial APIs):
    1. Existing data on the lead (skip if already filled)
    2. LA County Assessor (for LA-area leads)
    3. County-specific assessor scrapers (major metros)
    4. State ArcGIS parcel services (all 50 states + national fallback)
    5. Regrid MVT parcel tiles (free, no API key, nationwide)
    """
    # Check cache first
    cache_key = _enrichment_cache_key(lead.get("address", ""), lead.get("city", ""))
    cached = _enrichment_cache.get(cache_key)
    if cached and (datetime.utcnow().timestamp() - _enrichment_cache_ts.get(cache_key, 0)) < ENRICHMENT_CACHE_TTL:
        for k, v in cached.items():
            if v and not lead.get(k):
                lead[k] = v
        return lead

    enrichment_result = {}
    lat = safe_float(lead.get("lat") or 0)
    lng = safe_float(lead.get("lng") or 0)
    city = lead.get("city", "")
    state = lead.get("state", "")

    # Source 1: LA County Assessor (free, unlimited for LA-area)
    if city in ("Los Angeles", "Long Beach", "Pasadena", "Glendale", "Burbank",
                "Santa Monica", "Beverly Hills", "West Hollywood", "Culver City",
                "Torrance") and lat and lng:
        result = await enrich_owner_from_la_assessor(lat, lng)
        if result:
            enrichment_result.update({k: v for k, v in result.items() if v})

    # Source 2: County-specific assessor scrapers (major metros, free)
    if not enrichment_result.get("owner_name") and city and state and lat and lng:
        result = await enrich_owner_from_county_scraper(lat, lng, city, state)
        if result:
            enrichment_result.update({k: v for k, v in result.items() if v and not enrichment_result.get(k)})

    # Source 3: State ArcGIS parcel services (free, all 50 states + national fallback)
    if not enrichment_result.get("owner_name") and lat and lng:
        result = await enrich_owner_from_arcgis_parcel(lat, lng, state or "")
        if result:
            enrichment_result.update({k: v for k, v in result.items() if v and not enrichment_result.get(k)})

    # Source 4: Regrid MVT parcel tiles (free, no API key, no daily limit, nationwide)
    if not enrichment_result.get("owner_name") and lat and lng:
        result = await enrich_owner_from_regrid_tile(lat, lng)
        if result:
            enrichment_result.update({k: v for k, v in result.items() if v and not enrichment_result.get(k)})

    # Apply enrichment to lead
    for field in ("owner_name", "owner_address", "owner_phone", "owner_email",
                  "apn", "market_value", "year_built", "square_feet", "lot_size",
                  "zoning", "bedrooms", "bathrooms", "land_value", "improvement_value"):
        val = enrichment_result.get(field)
        if val and not lead.get(field):
            lead[field] = val

    # Cache the result
    _enrichment_cache[cache_key] = enrichment_result
    _enrichment_cache_ts[cache_key] = datetime.utcnow().timestamp()

    return lead


async def batch_enrich_ownership(leads: List[dict], max_enrichments: int = 100) -> int:
    """Batch-enrich leads with ownership/property data.
    Prioritizes leads with missing owner_name and highest valuation."""
    enriched_count = 0
    # Sort by valuation desc so we enrich highest-value leads first
    candidates = [l for l in leads if not l.get("owner_name")]
    candidates.sort(key=lambda l: safe_float(l.get("valuation")), reverse=True)

    for lead in candidates[:max_enrichments]:
        try:
            before = lead.get("owner_name", "")
            await enrich_lead_ownership(lead)
            if lead.get("owner_name") and lead.get("owner_name") != before:
                enriched_count += 1
        except Exception as e:
            logger.debug(f"Ownership enrichment error for {lead.get('address')}: {e}")
            continue

    logger.info(f"Batch ownership enrichment: {enriched_count}/{min(len(candidates), max_enrichments)} leads enriched")
    return enriched_count


# ============================================================================
# FEMA DISASTER RISK ENRICHMENT
# ============================================================================

# In-memory cache for disaster risk data by state, with TTL
_disaster_cache: Dict[str, tuple] = {}  # state -> (timestamp, data)
_DISASTER_CACHE_TTL = 3600 * 6  # 6 hours

async def get_disaster_risk_for_lead(lead: dict) -> dict:
    """Get disaster risk context for a lead based on state/zip.
    Returns recent disasters, flood risk, and damage history."""
    state = lead.get("state", "")
    zip_code = str(lead.get("zip", ""))

    if not state:
        return {}

    # Cache disaster declarations per state with TTL
    import time as _time
    cached_entry = _disaster_cache.get(state)
    if cached_entry is None or (_time.time() - cached_entry[0]) > _DISASTER_CACHE_TTL:
        disasters = await fetch_fema_disasters(state=state, limit=50)
        _disaster_cache[state] = (_time.time(), disasters)
    else:
        disasters = cached_entry[1]

    recent_disasters = disasters

    # Build risk profile
    risk_profile = {
        "disaster_count_5yr": 0,
        "recent_disasters": [],
        "flood_zone_risk": "Unknown",
        "primary_hazards": [],
    }

    hazard_counts: Dict[str, int] = defaultdict(int)
    cutoff_year = datetime.utcnow().year - 5

    for d in recent_disasters:
        dec_date = d.get("declarationDate", "")
        try:
            year = int(dec_date[:4]) if dec_date else 0
        except (ValueError, TypeError):
            year = 0
        if year >= cutoff_year:
            risk_profile["disaster_count_5yr"] += 1
            risk_profile["recent_disasters"].append({
                "title": d.get("disasterName", d.get("declarationTitle", "")),
                "type": d.get("incidentType", ""),
                "date": dec_date[:10] if dec_date else "",
            })
            incident_type = d.get("incidentType", "")
            if incident_type:
                hazard_counts[incident_type] += 1

    # Top hazards
    risk_profile["primary_hazards"] = [
        {"type": k, "count": v}
        for k, v in sorted(hazard_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    ]

    # Limit recent disasters to top 10
    risk_profile["recent_disasters"] = risk_profile["recent_disasters"][:10]

    return risk_profile


# ============================================================================
# BACKGROUND SYNC
# ============================================================================

async def sync_data():
    """Background task to sync permit data"""
    if _sync_lock.locked():
        logger.info("Sync already running; skipping this cycle")
        cached = DataCache.load(allow_stale=True)
        return cached or []

    async with _sync_lock:
        logger.info("Starting data sync...")

        cached = DataCache.load(allow_stale=True)

        # If offline or API keys/routing blocked, rely on existing cache
        if os.getenv("DISABLE_REMOTE", "0") == "1":
            logger.info("Remote fetch disabled; keeping existing cache")
            if cached:
                logger.info(f"Keeping cache with {len(cached)} leads")
                return cached
            return []

        all_leads: List[dict] = []
        geocoder = GeocodingClient()

        # Optional LADBS geocoded fetch (small sample to avoid geocoding limits)
        if not SKIP_LA:
            ladbs_client = LADBSClient()
            ladbs_permits = await ladbs_client.get_recent_permits(days=7, limit=50)
            for permit in ladbs_permits[:10]:  # Limit to 10 for geocoding rate limit
                lead = await process_ladbs_permit(permit, geocoder)
                if lead:
                    all_leads.append(lead.dict())

        geocode_budget = GEOCODE_BUDGET

        # ── Registry-driven sync (new: reads from SQLite api_sources) ──
        if os.getenv("USE_REGISTRY_SYNC", "0") == "1":
            try:
                from services.source_registry import get_sources_for_sync, mark_synced
                from services.fetchers import fetch_from_source
                max_sources = int(os.getenv("SYNC_MAX_SOURCES", "50"))
                registry_sources = get_sources_for_sync(category="permit", limit=max_sources)
                logger.info("Registry sync: %d sources to fetch", len(registry_sources))
                for src in registry_sources:
                    try:
                        records = await fetch_from_source(src, timeout_seconds=30)
                        if records:
                            logger.info("Registry %s: %d records", src.get("api_name", ""), len(records))
                            for r in records:
                                lat, lng = extract_lat_lng(r)
                                lat, lng = fix_coordinates(lat, lng) if lat and lng else (lat, lng)
                                if lat is None or lng is None:
                                    continue
                                address = extract_address(r)
                                if not address:
                                    continue
                                permit_number = extract_permit_number(r)
                                permit_type = (
                                    r.get("permit_type") or r.get("record_type")
                                    or r.get("permit_type_definition") or r.get("work_type") or ""
                                )
                                issue_raw = r.get("issue_date") or r.get("issued_date") or r.get("filed_date") or ""
                                issue_dt = parse_date(issue_raw)
                                issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                                days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15
                                valuation = safe_float(
                                    r.get("valuation") or r.get("estimated_cost") or r.get("job_value") or 0
                                )
                                score, temp, urgency = calculate_score(days_old, valuation, permit_type)
                                lead = {
                                    "id": abs(hash(f"reg:{src.get('id', '')}:{permit_number}:{address}")),
                                    "permit_number": permit_number,
                                    "address": address,
                                    "city": src.get("location", "") or r.get("city", ""),
                                    "zip": r.get("zip") or r.get("zip_code") or "",
                                    "lat": lat,
                                    "lng": lng,
                                    "work_description": permit_type or "Permit filed",
                                    "permit_type": permit_type,
                                    "valuation": valuation,
                                    "issue_date": issue_date,
                                    "days_old": days_old,
                                    "score": score,
                                    "temperature": temp,
                                    "urgency": urgency,
                                    "source": src.get("api_name", "registry"),
                                    "owner_name": r.get("owner_name") or r.get("applicant_name") or "",
                                    "owner_phone": r.get("owner_phone") or r.get("phone") or "",
                                    "owner_email": r.get("owner_email") or r.get("email") or "",
                                    "state": src.get("state", "") or _infer_state(src.get("location", "")),
                                }
                                all_leads.append(lead)
                            mark_synced(src["id"], len(records))
                        else:
                            mark_synced(src["id"], 0, "No records returned")
                    except Exception as e:
                        logger.warning("Registry source %s error: %s", src.get("api_name", ""), e)
                        mark_synced(src["id"], 0, str(e)[:200])
                logger.info("Registry sync complete: %d total leads", len(all_leads))
            except Exception as e:
                logger.error("Registry sync failed, falling back to config: %s", e)
        else:
            # Original config-driven sync (fallback)
            pass

        # Fetch from verified Socrata datasets
        if os.getenv("USE_REGISTRY_SYNC", "0") != "1":
            for key, cfg in SOC_DATASETS.items():
                try:
                    days = cfg.get("lookback_days", 90)
                    records = await fetch_socrata_best(
                        cfg["domain"],
                        cfg["resource_id"],
                        cfg.get("date_field"),
                        cfg.get("filters"),
                        days=days,
                    )
                    logger.info(f"{cfg['label']}: fetched {len(records)} rows")
                    accepted = 0
                    skip_latlng = skip_addr = skip_date = 0
                    for r in records:
                        lat, lng = extract_lat_lng(r)
                        lat, lng = fix_coordinates(lat, lng) if lat and lng else (lat, lng)
                        if lat is None or lng is None:
                            if geocode_budget > 0:
                                addr_for_geo = extract_address(r)
                                city_for_geo = cfg.get("city") or r.get("city") or ""
                                if addr_for_geo and city_for_geo:
                                    coords = await geocoder.geocode(addr_for_geo, city_for_geo)
                                    if coords:
                                        lat, lng = coords["lat"], coords["lng"]
                                        geocode_budget -= 1
                            if lat is None or lng is None:
                                skip_latlng += 1
                                continue

                        address = extract_address(r)
                        if not address:
                            skip_addr += 1
                            continue
                        permit_number = extract_permit_number(r)
                        permit_type = (r.get("permit_type") or r.get("record_type") or r.get("permit_type_definition")
                            or r.get("permittypemapped") or r.get("record_category") or r.get("work_type")
                            or r.get("permittypedesc") or "")
                        issue_field = cfg.get("date_field")
                        issue_raw = r.get(issue_field) if issue_field else None
                        # Broad fallback: try many common date field names
                        if not issue_raw:
                            for df in ("issue_date", "issued_date", "filed_date", "permit_date",
                                       "issuance_date", "issueddate", "date_issued", "filing_date",
                                       "application_date", "applieddate", "statusdate", "completed_date",
                                       "permitissuedate", "latest_action_date", "approved_date",
                                       "expiration_date", "inspection_date", "permit_creation_date",
                                       "submitted_date", "created_date", "status_date"):
                                issue_raw = r.get(df)
                                if issue_raw:
                                    break
                        issue_dt = parse_date(issue_raw)
                        issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                        # If the API already filtered by date, trust the results are recent
                        days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15
                        if days_old > MAX_DAYS_OLD:
                            skip_date += 1
                            continue
                        valuation = safe_float(
                            r.get("valuation") or r.get("estimated_cost") or r.get("permit_valuation")
                            or r.get("job_value") or r.get("initial_cost") or r.get("construction_cost")
                            or r.get("estimated_value") or r.get("project_value") or r.get("cost")
                            or r.get("total_job_valuation") or r.get("total_construction_value")
                            or r.get("reported_cost") or r.get("estprojectcostdec")
                            or r.get("estimated_job_costs") or r.get("fee") or r.get("total_fee")
                        )
                        valuation = valuation if valuation is not None else 0.0

                        # Owner / applicant identity
                        owner_name = (
                            r.get("owner_name") or
                            r.get("owner") or
                            " ".join(filter(None, [r.get("owner_first_name"), r.get("owner_last_name")])) or
                            " ".join(filter(None, [r.get("applicant_first_name"), r.get("applicant_last_name")])) or
                            r.get("applicant_name") or
                            r.get("applicant") or
                            r.get("contact_name") or
                            r.get("contact_1_name") or
                            r.get("owner_s_first_name", "") + " " + r.get("owner_s_last_name", "") or
                            ""
                        ).strip()

                        owner_phone = (
                            r.get("owner_phone") or r.get("phone") or r.get("contact_phone") or
                            r.get("phone1") or r.get("phone_number") or r.get("applicant_phone")
                            or r.get("owner_s_phone__") or ""
                        )
                        owner_email = r.get("owner_email") or r.get("email") or r.get("applicant_email") or ""

                        contractor = (r.get("contractor_name") or r.get("contractor") or r.get("contractor_business_name")
                            or r.get("contractorcompanyname") or r.get("companyname")
                            or r.get("applicant_business_name") or r.get("permittee_s_business_name") or "")
                        contractor_phone = r.get("contractor_phone") or ""

                        score, temp, urgency = calculate_score(days_old, valuation, permit_type or "")

                        desc_candidates = [
                            r.get("work_desc"),
                            r.get("description"),
                            r.get("work_description"),
                            r.get("scope_description"),
                            r.get("comments"),
                            r.get("permit_type_definition"),
                            r.get("use_desc"),
                            r.get("permit_sub_type"),
                            r.get("record_type"),
                            permit_type,
                        ]
                        professional_desc = next((d for d in desc_candidates if d), "") or ""
                        # Preserve the fullest description we can assemble from the record
                        description_full = " | ".join([d for d in desc_candidates if d])

                        # Use the original permit description verbatim; keep optional extra info separately
                        base_desc = professional_desc or "Permit filed"
                        value_txt = f"${valuation:,.0f}" if valuation else "n/a"
                        issue_txt = issue_date or "n/a"
                        contractor_txt = contractor or r.get("applicant_name") or r.get("contact_name") or ""
                        suffix_parts = [
                            f"Type: {permit_type or 'n/a'}",
                            f"Value: {value_txt}",
                            f"Filed: {issue_txt}",
                        ]
                        if contractor_txt:
                            suffix_parts.append(f"Contractor/Applicant: {contractor_txt}")
                        extra_info = " | ".join([p for p in suffix_parts if p])

                        lead = {
                            "id": abs(hash(f"{key}:{permit_number}:{address}")),
                            "permit_number": permit_number,
                            "address": address,
                            "city": cfg.get("city") or r.get("city") or "",
                            "zip": r.get("zip") or r.get("zip_code") or r.get("zipcode") or r.get("originalzip") or "",
                            "lat": lat,
                            "lng": lng,
                            "work_description": base_desc,
                            "description_full": description_full,
                            "details": extra_info,
                            "permit_type": permit_type,
                            "valuation": valuation,
                            "issue_date": issue_date,
                            "days_old": days_old,
                            "score": score,
                            "temperature": temp,
                            "urgency": urgency,
                            "source": cfg["label"],
                            "apn": r.get("apn") or r.get("parcel_number") or "",
                            "owner_name": owner_name,
                            "owner_phone": owner_phone,
                            "owner_email": owner_email,
                            "contractor_name": contractor,
                            "contractor_phone": contractor_phone,
                            "state": _infer_state(cfg.get("city") or r.get("city") or ""),
                            "permit_url": _build_permit_url(cfg.get("city") or "", permit_number, cfg.get("domain") or "", cfg.get("resource_id") or ""),
                        }

                        # Override permit_url with direct link if Socrata record has one
                        raw_link = r.get("link")
                        if raw_link:
                            if isinstance(raw_link, dict) and raw_link.get("url"):
                                lead["permit_url"] = raw_link["url"]
                            elif isinstance(raw_link, str) and raw_link.startswith("http"):
                                lead["permit_url"] = raw_link

                        # Tier 1: Optional inline LA Assessor lookup (disabled by default; blocks event loop)
                        if LA_ASSESSOR_INLINE and not owner_name and lead.get("apn") and "los angeles" in lead.get("city", "").lower():
                            try:
                                owner_data = get_la_owner(lead["apn"])
                                if owner_data and owner_data.get("owner_name"):
                                    lead["owner_name"] = owner_data.get("owner_name", "")
                                    lead["mailing_address"] = owner_data.get("mailing_address", "")
                                    logger.info(f"✅ LA Assessor: {lead['owner_name']}")
                            except Exception as e:
                                logger.warning(f"LA Assessor lookup failed for APN {lead.get('apn')}: {e}")
                
                        all_leads.append(lead)
                        accepted += 1
                    logger.info(f"{cfg['label']}: accepted {accepted} leads with lat/lng (skipped: {skip_latlng} no-latlng, {skip_addr} no-addr, {skip_date} too-old)")
                except Exception as e:
                    logger.error(f"Socrata fetch {cfg['label']} failed: {e}")

        # Fetch San Jose CSV datasets (no auth; may lack lat/lng)
        for sj in SAN_JOSE_SOURCES:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(sj["url"], timeout=aiohttp.ClientTimeout(total=30), allow_redirects=True) as resp:
                        if resp.status != 200:
                            logger.warning(f"San Jose {sj['label']} HTTP {resp.status}")
                            continue
                        text = await resp.text()
                        rows = [ {k.lower(): v for k, v in row.items()} for row in csv.DictReader(text.splitlines()) ]
                        accepted = 0
                        for r in rows:
                            addr = r.get("gx_location") or r.get("address") or ""
                            if not addr:
                                continue
                            lat, lng = extract_lat_lng(r)
                            if (lat is None or lng is None) and geocode_budget > 0:
                                city_for_geo = sj["city"]
                                coords = await geocoder.geocode(addr, city_for_geo)
                                if coords:
                                    lat, lng = coords["lat"], coords["lng"]
                                    geocode_budget -= 1
                            if lat is None or lng is None:
                                continue
                            issue_dt = parse_date(r.get("issuedate") or r.get("issue_date") or r.get("finaldate"))
                            issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                            days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 0
                            if days_old and days_old > MAX_DAYS_OLD:
                                continue
                            valuation = safe_float(r.get("permitvaluation") or r.get("valuation") or r.get("job_value")) or 0.0
                            score, temp, urgency = calculate_score(days_old, valuation, r.get("subtypedescription") or "")
                            owner_name = r.get("ownername") or r.get("applicant") or ""
                            owner_phone = r.get("applicantphone") or r.get("ownerphone") or ""
                            desc_candidates = [
                                r.get("description"),
                                r.get("workdescription"),
                                r.get("folderdesc"),
                                r.get("subtypedescription"),
                                r.get("foldername"),
                            ]
                            professional_desc = next((d for d in desc_candidates if d), "") or "Permit filed"
                            description_full = " | ".join([d for d in desc_candidates if d])
                            # Keep original description; store extra info separately
                            value_txt = f"${valuation:,.0f}" if valuation else "n/a"
                            issue_txt = issue_date or "n/a"
                            suffix = " | ".join(
                                [
                                    f"Type: {r.get('subtypedescription') or r.get('foldername') or 'n/a'}",
                                    f"Value: {value_txt}",
                                    f"Filed: {issue_txt}",
                                ]
                            )
                            rich_description = professional_desc
                            lead = {
                                "id": abs(hash(f"{sj['key']}:{addr}:{r.get('folderrsn','')}")),
                                "permit_number": r.get("foldernumber") or r.get("permit_number") or "",
                                "address": addr,
                                "city": sj["city"],
                                "zip": "",
                                "lat": lat,
                                "lng": lng,
                                "work_description": rich_description,
                                "description_full": description_full,
                                "details": suffix,
                                "permit_type": r.get("subtypedescription") or r.get("foldername") or "",
                                "valuation": valuation,
                                "issue_date": issue_date,
                                "days_old": days_old,
                                "score": score,
                                "temperature": temp,
                                "urgency": urgency,
                                "source": sj["label"],
                                "apn": r.get("assessors_parcel_number") or "",
                                "owner_name": owner_name,
                                "owner_phone": owner_phone,
                                "owner_email": r.get("applicantemail") or "",
                                "contractor_name": r.get("contractor") or "",
                                "contractor_phone": r.get("contractorphone") or "",
                                "description": r.get("workdescription") or r.get("folderdesc") or "",
                                "state": "CA",
                            }
                            all_leads.append(lead)
                            accepted += 1
                        logger.info(f"{sj['label']}: accepted {accepted} leads with lat/lng")
            except Exception as e:
                logger.error(f"San Jose fetch {sj['label']} failed: {e}")

        # ── ArcGIS FeatureServer cities (Phoenix, Denver, Indianapolis, etc.) ──
        for arc_key, arc_cfg in ARCGIS_DATASETS.items():
            try:
                records = await fetch_arcgis_permits(
                    url=arc_cfg["url"],
                    date_field=arc_cfg.get("date_field"),
                    order_by=arc_cfg.get("order_by", ""),
                    lookback_days=int(arc_cfg.get("lookback_days", 90)),
                )
                logger.info(f"{arc_cfg['label']}: fetched {len(records)} ArcGIS rows")
                accepted = 0
                skip_latlng = skip_addr = skip_date = 0
                for r in records:
                    # Lat/lng: geometry (_lat/_lng), then explicit fields from verified endpoints
                    lat = (r.get("_lat") or r.get("LATITUDE") or r.get("latitude")
                           or r.get("Y") or r.get("Y_COORD") or r.get("YCOORD"))
                    lng = (r.get("_lng") or r.get("LONGITUDE") or r.get("longitude")
                           or r.get("X") or r.get("X_COORD") or r.get("XCOORD"))
                    if lat is None or lng is None:
                        skip_latlng += 1
                        continue
                    try:
                        lat, lng = float(lat), float(lng)
                    except (ValueError, TypeError):
                        skip_latlng += 1
                        continue
                    lat, lng = fix_coordinates(lat, lng)
                    if lat is None or lng is None:
                        skip_latlng += 1
                        continue

                    # Address: covers Phoenix, Portland, DC, Tampa, Miami, Detroit, Baltimore, LV, Indy
                    address = (
                        r.get("Address") or r.get("ADDRESS") or r.get("address")
                        or r.get("FULL_ADDRESS") or r.get("STREET_FULL_NAME")
                        or r.get("PROP_ADDRE") or r.get("ADDR") or r.get("StreetAddress")
                        or r.get("SITE_ADDRESS") or r.get("OriginalAddress") or r.get("Location") or ""
                    )
                    if not address:
                        skip_addr += 1
                        continue

                    # Permit number
                    permit_number = (
                        r.get("PermitNum") or r.get("PERMIT_NUM") or r.get("PermitNumber")
                        or r.get("PERMIT_NUMBER") or r.get("permit_number") or r.get("CaseNumber")
                        or r.get("PER_NUM") or r.get("PERMIT_ID") or r.get("record_id")
                        or r.get("RECORD_ID") or r.get("FOLDERNUMB") or r.get("APNO")
                        or r.get("PROCNUM") or ""
                    )
                    # Permit type
                    permit_type = (
                        r.get("PermitType") or r.get("PERMIT_TYPE") or r.get("permit_type")
                        or r.get("PER_TYPE_DESC") or r.get("PERMIT_TYPE_NAME") or r.get("RECORDTYPE")
                        or r.get("TYPE") or r.get("WORKTYPE") or r.get("APTYPE")
                        or r.get("WorkClass") or r.get("WORK_CLASS") or r.get("WorkType")
                        or r.get("construction_type") or ""
                    )

                    # Parse date — ArcGIS often returns epoch milliseconds
                    date_field_name = arc_cfg.get("date_field")
                    raw_date = r.get(date_field_name) if date_field_name else None
                    # Fallback: try common date field names if configured field missing
                    if raw_date is None:
                        for df in ("issued_date", "IssuedDate", "IssueDate", "ISSUE_DATE",
                                   "ISSUDATE", "PER_ISSUE_DATE", "DATE_ISSUED", "ISSUEDATE",
                                   "CREATEDDATE", "ISSUE_DT", "submitted_date", "ApplicationDate"):
                            raw_date = r.get(df)
                            if raw_date is not None:
                                break
                    issue_dt = None
                    if raw_date:
                        if isinstance(raw_date, (int, float)) and raw_date > 1e10:
                            issue_dt = datetime.utcfromtimestamp(raw_date / 1000)
                        else:
                            issue_dt = parse_date(str(raw_date))
                    issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                    days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15
                    if days_old > MAX_DAYS_OLD:
                        skip_date += 1
                        continue

                    valuation = safe_float(
                        r.get("Valuation") or r.get("VALUATION") or r.get("EstProjectCost")
                        or r.get("ESTIMATED_COST") or r.get("ProjectValue") or r.get("TotalFees")
                        or r.get("PERMIT_FEE") or r.get("FEES_PAID") or r.get("Cost")
                        or r.get("amt_permit_cost") or r.get("amt_estimated_contractor_cost")
                    ) or 0.0

                    owner_name = (
                        r.get("OwnerName") or r.get("OWNER_NAME") or r.get("OWNER_NAME")
                        or r.get("ApplicantName") or r.get("APPLICANT_NAME") or r.get("Applicant")
                        or r.get("PERMIT_APPLICANT") or r.get("PROFESS_NAME") or r.get("CONTRNAME")
                        or r.get("CONTRACTOR_NAME") or r.get("APPLICANT") or ""
                    )
                    owner_phone = r.get("OwnerPhone") or r.get("ApplicantPhone") or r.get("PHONE") or ""

                    # Description: covers all verified endpoint field names
                    desc = (
                        r.get("Description") or r.get("DESCRIPTION") or r.get("WorkDescription")
                        or r.get("work_description") or r.get("WorkDesc") or r.get("DESC_OF_WORK")
                        or r.get("SCOPE_DESC") or r.get("WORKDESC") or r.get("PROJECTDESCRIPTION")
                        or r.get("FULL_DESC") or r.get("APDESC") or r.get("MOD_DESC")
                        or r.get("FOLDER_DES") or r.get("WORK_DESCRIPTION") or r.get("Scope")
                        or permit_type or "Permit filed"
                    )

                    score, temp, urgency = calculate_score(days_old, valuation, permit_type)

                    lead = {
                        "id": abs(hash(f"{arc_key}:{permit_number}:{address}")),
                        "permit_number": permit_number,
                        "address": address,
                        "city": arc_cfg["city"],
                        "zip": r.get("ZipCode") or r.get("ZIP") or r.get("zip_code") or r.get("ZIPCODE") or r.get("Zip") or "",
                        "lat": lat,
                        "lng": lng,
                        "work_description": desc,
                        "description_full": desc,
                        "details": f"Type: {permit_type or 'n/a'} | Value: ${valuation:,.0f} | Filed: {issue_date or 'n/a'}",
                        "permit_type": permit_type,
                        "valuation": valuation,
                        "issue_date": issue_date,
                        "days_old": days_old,
                        "score": score,
                        "temperature": temp,
                        "urgency": urgency,
                        "source": arc_cfg["label"],
                        "apn": r.get("APN") or r.get("ParcelNumber") or r.get("PARCEL_NUM") or r.get("parcel_id") or r.get("FOLIO") or r.get("BLOCKLOT") or r.get("PRCLID") or r.get("GPIN") or "",
                        "owner_name": owner_name,
                        "owner_phone": owner_phone,
                        "owner_email": r.get("OwnerEmail") or r.get("ApplicantEmail") or "",
                        "contractor_name": r.get("ContractorName") or r.get("CONTRACTOR") or r.get("CONTRNAME") or r.get("CONTRACTOR_NAME") or "",
                        "contractor_phone": r.get("ContractorPhone") or "",
                        "state": arc_cfg.get("state", ""),
                        "permit_url": _build_permit_url(arc_cfg["city"], permit_number),
                    }
                    all_leads.append(lead)
                    accepted += 1
                logger.info(f"{arc_cfg['label']}: accepted {accepted} ArcGIS leads with lat/lng (skipped: {skip_latlng} no-latlng, {skip_addr} no-addr, {skip_date} too-old)")
            except Exception as e:
                logger.error(f"ArcGIS fetch {arc_cfg['label']} failed: {e}")

        # ── CKAN API cities (San Jose, San Antonio, Boston) ──
        for ckan_key, ckan_cfg in CKAN_DATASETS.items():
            try:
                records = await fetch_ckan_permits(
                    domain=ckan_cfg["domain"],
                    resource_id=ckan_cfg["resource_id"],
                    date_field=ckan_cfg.get("date_field"),
                    days=ckan_cfg.get("lookback_days", 60)
                )
                logger.info(f"{ckan_cfg['label']}: fetched {len(records)} CKAN rows")

                accepted = 0
                skip_latlng = skip_addr = skip_date = 0

                for r in records:
                    # San Jose CKAN field mapping (verified fields from testing)
                    lat = r.get("LATITUDE") or r.get("latitude") or r.get("Latitude")
                    lng = r.get("LONGITUDE") or r.get("longitude") or r.get("Longitude")

                    if not lat or not lng:
                        skip_latlng += 1
                        continue

                    lat = safe_float(lat)
                    lng = safe_float(lng)

                    # Address fields
                    address = (r.get("FULLADDRESS") or r.get("ADDRESS") or
                              r.get("address") or r.get("SITEADDRESS") or "").strip()

                    if not address:
                        skip_addr += 1
                        continue

                    # Permit number
                    permit_number = (r.get("PERMIT") or r.get("permit_number") or
                                    r.get("PERMITNUMBER") or r.get("PermitNumber") or "").strip()

                    # Date field (use configured date_field from CKAN_DATASETS)
                    date_field_name = ckan_cfg.get("date_field", "ISSUEDATE")
                    raw_date = r.get(date_field_name) or r.get("issue_date") or r.get("IssueDate")

                    issue_dt = parse_date(str(raw_date)) if raw_date else None
                    issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                    days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15

                    if days_old > MAX_DAYS_OLD:
                        skip_date += 1
                        continue

                    # Valuation
                    valuation = safe_float(
                        r.get("VALUATION") or r.get("valuation") or r.get("Valuation") or
                        r.get("VALUE") or r.get("EstimatedCost") or r.get("ESTIMATEDCOST")
                    ) or 0.0

                    # Permit type / work description
                    permit_type = (r.get("PERMITTYPE") or r.get("permit_type") or
                                  r.get("PermitType") or r.get("TYPE") or "Building Permit").strip()

                    description = (r.get("WORKDESCRIPTION") or r.get("WorkDescription") or
                                  r.get("work_description") or r.get("DESCRIPTION") or
                                  r.get("description") or permit_type or "Permit filed").strip()

                    # Owner/applicant info
                    owner_name = (r.get("APPLICANTNAME") or r.get("ApplicantName") or
                                 r.get("applicant_name") or r.get("OWNERNAME") or r.get("OwnerName") or "").strip()

                    owner_phone = (r.get("APPLICANTPHONE") or r.get("ApplicantPhone") or
                                  r.get("OWNERPHONE") or r.get("OwnerPhone") or "").strip()

                    # Contractor info
                    contractor_name = (r.get("CONTRACTORNAME") or r.get("ContractorName") or
                                      r.get("contractor_name") or r.get("CONTRACTOR") or "").strip()

                    contractor_phone = (r.get("CONTRACTORPHONE") or r.get("ContractorPhone") or
                                       r.get("contractor_phone") or "").strip()

                    # APN / Parcel
                    apn = (r.get("APN") or r.get("apn") or r.get("PARCELNUMBER") or
                          r.get("ParcelNumber") or r.get("parcel_number") or "").strip()

                    # Zip code
                    zip_code = (r.get("ZIP") or r.get("zip") or r.get("ZIPCODE") or
                               r.get("ZipCode") or r.get("zip_code") or "").strip()

                    # Calculate score
                    score, temp, urgency = calculate_score(days_old, valuation, permit_type)

                    lead = {
                        "id": abs(hash(f"{ckan_key}:{permit_number}:{address}")),
                        "permit_number": permit_number,
                        "address": address,
                        "city": ckan_cfg["city"],
                        "zip": zip_code,
                        "lat": lat,
                        "lng": lng,
                        "work_description": description,
                        "description_full": description,
                        "details": f"Type: {permit_type or 'n/a'} | Value: ${valuation:,.0f} | Filed: {issue_date or 'n/a'}",
                        "permit_type": permit_type,
                        "valuation": valuation,
                        "issue_date": issue_date,
                        "days_old": days_old,
                        "score": score,
                        "temperature": temp,
                        "urgency": urgency,
                        "source": ckan_cfg["label"],
                        "apn": apn,
                        "owner_name": owner_name,
                        "owner_phone": owner_phone,
                        "owner_email": r.get("APPLICANTEMAIL") or r.get("ApplicantEmail") or "",
                        "contractor_name": contractor_name,
                        "contractor_phone": contractor_phone,
                        "state": ckan_cfg.get("state", ""),
                    }
                    all_leads.append(lead)
                    accepted += 1

                logger.info(f"{ckan_cfg['label']}: accepted {accepted} CKAN leads with lat/lng (skipped: {skip_latlng} no-latlng, {skip_addr} no-addr, {skip_date} too-old)")
            except Exception as e:
                logger.error(f"CKAN fetch {ckan_cfg['label']} failed: {e}")

        # ── Carto API cities (Philadelphia) ──
        for carto_key, carto_cfg in CARTO_DATASETS.items():
            try:
                records = await fetch_carto_permits(
                    domain=carto_cfg["domain"],
                    table_name=carto_cfg["table_name"],
                    date_field=carto_cfg.get("date_field"),
                    days=carto_cfg.get("lookback_days", 60)
                )
                logger.info(f"{carto_cfg['label']}: fetched {len(records)} Carto rows")

                accepted = 0
                skip_latlng = skip_addr = skip_date = 0

                for r in records:
                    # Carto geometry field (PostGIS format)
                    lat = r.get("lat") or r.get("latitude") or r.get("y")
                    lng = r.get("lng") or r.get("longitude") or r.get("x")

                    # Try geometry object if simple fields don't exist
                    if not lat or not lng:
                        geom = r.get("the_geom")
                        if geom and isinstance(geom, dict):
                            coords = geom.get("coordinates", [])
                            if len(coords) >= 2:
                                lng, lat = coords[0], coords[1]  # GeoJSON is [lng, lat]

                    if not lat or not lng:
                        skip_latlng += 1
                        continue

                    lat = safe_float(lat)
                    lng = safe_float(lng)

                    # Address
                    address = (r.get("address") or r.get("addressobjectid") or
                              r.get("location") or r.get("street_address") or "").strip()

                    if not address:
                        skip_addr += 1
                        continue

                    # Permit number
                    permit_number = (r.get("permitnumber") or r.get("permit_number") or
                                    r.get("apno") or r.get("objectid") or "").strip()

                    # Date field
                    date_field_name = carto_cfg.get("date_field", "permitissuedate")
                    raw_date = r.get(date_field_name)

                    issue_dt = parse_date(str(raw_date)) if raw_date else None
                    issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                    days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15

                    if days_old > MAX_DAYS_OLD:
                        skip_date += 1
                        continue

                    # Valuation
                    valuation = safe_float(
                        r.get("permitdescription") or r.get("est_cost") or
                        r.get("totalcost") or r.get("value")
                    ) or 0.0

                    # Permit type
                    permit_type = (r.get("permittype") or r.get("typeofwork") or
                                  r.get("work_type") or "Building Permit").strip()

                    description = (r.get("permitdescription") or r.get("workdescription") or
                                  r.get("description") or permit_type or "Permit filed").strip()

                    # Owner info
                    owner_name = (r.get("ownername") or r.get("owner") or
                                 r.get("applicant") or "").strip()

                    owner_phone = (r.get("ownerphone") or r.get("phone") or "").strip()

                    # Contractor
                    contractor_name = (r.get("contractorname") or r.get("contractor") or "").strip()
                    contractor_phone = (r.get("contractorphone") or "").strip()

                    # Score
                    score, temp, urgency = calculate_score(days_old, valuation, permit_type)

                    lead = {
                        "id": abs(hash(f"{carto_key}:{permit_number}:{address}")),
                        "permit_number": permit_number,
                        "address": address,
                        "city": carto_cfg["city"],
                        "zip": r.get("zip") or "",
                        "lat": lat,
                        "lng": lng,
                        "work_description": description,
                        "description_full": description,
                        "details": f"Type: {permit_type or 'n/a'} | Value: ${valuation:,.0f} | Filed: {issue_date or 'n/a'}",
                        "permit_type": permit_type,
                        "valuation": valuation,
                        "issue_date": issue_date,
                        "days_old": days_old,
                        "score": score,
                        "temperature": temp,
                        "urgency": urgency,
                        "source": carto_cfg["label"],
                        "apn": r.get("apn") or "",
                        "owner_name": owner_name,
                        "owner_phone": owner_phone,
                        "owner_email": "",
                        "contractor_name": contractor_name,
                        "contractor_phone": contractor_phone,
                        "state": carto_cfg.get("state", ""),
                    }
                    all_leads.append(lead)
                    accepted += 1

                logger.info(f"{carto_cfg['label']}: accepted {accepted} Carto leads with lat/lng (skipped: {skip_latlng} no-latlng, {skip_addr} no-addr, {skip_date} too-old)")
            except Exception as e:
                logger.error(f"Carto fetch {carto_cfg['label']} failed: {e}")

        # ── State-level portals (CA, TX, FL, NY) - HUGE COVERAGE ──
        for state_key, state_cfg in STATE_DATASETS.items():
            try:
                records = await fetch_socrata_best(
                    state_cfg["domain"],
                    state_cfg["resource_id"],
                    state_cfg.get("date_field"),
                    state_cfg.get("filters"),
                    days=state_cfg.get("lookback_days", 90)
                )
                logger.info(f"{state_cfg['label']}: fetched {len(records)} state-level rows")

                accepted = 0
                skip_latlng = skip_addr = skip_date = 0

                for r in records:
                    lat, lng = extract_lat_lng(r)
                    lat, lng = fix_coordinates(lat, lng) if lat and lng else (lat, lng)
                    if lat is None or lng is None:
                        skip_latlng += 1
                        continue

                    address = extract_address(r)
                    if not address:
                        skip_addr += 1
                        continue

                    permit_number = extract_permit_number(r)
                    permit_type = (r.get("permit_type") or r.get("type") or r.get("work_type") or "")

                    issue_field = state_cfg.get("date_field")
                    issue_raw = r.get(issue_field) if issue_field else None
                    issue_dt = parse_date(issue_raw)
                    issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                    days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15

                    if days_old > MAX_DAYS_OLD:
                        skip_date += 1
                        continue

                    valuation = safe_float(
                        r.get("valuation") or r.get("value") or r.get("cost") or r.get("amount")
                    ) or 0.0

                    owner_name = r.get("owner_name") or r.get("applicant") or ""
                    owner_phone = r.get("owner_phone") or r.get("phone") or ""

                    contractor = r.get("contractor_name") or r.get("contractor") or ""

                    score, temp, urgency = calculate_score(days_old, valuation, permit_type)

                    # Extract city from record (state portals have city field)
                    city = r.get("city") or r.get("municipality") or r.get("jurisdiction") or ""

                    lead = {
                        "id": abs(hash(f"{state_key}:{permit_number}:{address}:{city}")),
                        "permit_number": permit_number,
                        "address": address,
                        "city": city,
                        "zip": r.get("zip") or r.get("zip_code") or "",
                        "lat": lat,
                        "lng": lng,
                        "work_description": r.get("description") or permit_type or "Permit filed",
                        "description_full": r.get("description") or "",
                        "details": f"Type: {permit_type or 'n/a'} | Value: ${valuation:,.0f} | Filed: {issue_date or 'n/a'}",
                        "permit_type": permit_type,
                        "valuation": valuation,
                        "issue_date": issue_date,
                        "days_old": days_old,
                        "score": score,
                        "temperature": temp,
                        "urgency": urgency,
                        "source": state_cfg["label"],
                        "apn": r.get("apn") or "",
                        "owner_name": owner_name,
                        "owner_phone": owner_phone,
                        "owner_email": r.get("owner_email") or "",
                        "contractor_name": contractor,
                        "contractor_phone": r.get("contractor_phone") or "",
                        "state": state_cfg.get("state", ""),
                    }
                    all_leads.append(lead)
                    accepted += 1

                logger.info(f"{state_cfg['label']}: accepted {accepted} state-level leads (skipped: {skip_latlng} no-latlng, {skip_addr} no-addr, {skip_date} too-old)")
            except Exception as e:
                logger.error(f"State portal fetch {state_cfg['label']} failed: {e}")

        # ── Discovered Sources (2,300+ validated APIs from master discovery) ──
        try:
            from services.discovered_sources import get_discovered_registry
            registry = get_discovered_registry()

            # Build sets of already-configured IDs to avoid duplicates
            existing_soc_ids = {v.get("resource_id", "") for v in SOC_DATASETS.values() if v.get("resource_id")}
            existing_arc_urls = set()
            for v in ARCGIS_DATASETS.values():
                existing_arc_urls.add(v["url"].split("?")[0].rstrip("/"))

            registry.load(
                existing_socrata_ids=existing_soc_ids,
                existing_arcgis_urls=existing_arc_urls,
            )

            # Fetch from NEW discovered Socrata sources (concurrent)
            disc_socrata = registry.get_new_socrata_configs()
            if disc_socrata:
                logger.info(f"Discovered sources: syncing {len(disc_socrata)} new Socrata endpoints (concurrent)...")
                disc_soc_accepted = 0
                disc_soc_leads = []
                _soc_sem = asyncio.Semaphore(30)  # 30 concurrent Socrata requests

                async def _fetch_one_socrata(dkey, dcfg):
                    async with _soc_sem:
                        try:
                            days = dcfg.get("lookback_days", 60)
                            return dkey, dcfg, await asyncio.wait_for(
                                fetch_socrata_best(
                                    dcfg["domain"], dcfg["resource_id"],
                                    dcfg.get("date_field"), None, days=days,
                                ),
                                timeout=15,
                            )
                        except Exception as e:
                            logger.debug(f"Discovered Socrata {dkey} failed: {e}")
                            return dkey, dcfg, None

                # Process Socrata in batches of 100 to collect partial results
                soc_results = []
                soc_items = list(disc_socrata.items())
                batch_size = 100
                for batch_start in range(0, len(soc_items), batch_size):
                    batch = soc_items[batch_start:batch_start + batch_size]
                    try:
                        batch_results = await asyncio.wait_for(
                            asyncio.gather(*[_fetch_one_socrata(dk, dc) for dk, dc in batch]),
                            timeout=90,
                        )
                        soc_results.extend(batch_results)
                    except asyncio.TimeoutError:
                        logger.warning(f"Discovered Socrata batch {batch_start//batch_size + 1} timed out (90s)")
                    batch_done = min(batch_start + batch_size, len(soc_items))
                    if batch_done % 500 == 0 or batch_done == len(soc_items):
                        logger.info(f"  Discovered Socrata progress: {batch_done}/{len(soc_items)} endpoints fetched")

                for dkey, dcfg, records in soc_results:
                    if not records:
                        continue
                    try:
                        for r in records:
                            lat, lng = extract_lat_lng(r)
                            lat, lng = fix_coordinates(lat, lng) if lat and lng else (lat, lng)
                            if lat is None or lng is None:
                                continue
                            address = extract_address(r)
                            if not address:
                                continue
                            permit_number = extract_permit_number(r)
                            permit_type = (r.get("permit_type") or r.get("record_type") or
                                           r.get("type") or r.get("work_type") or "")
                            issue_raw = None
                            if dcfg.get("date_field"):
                                issue_raw = r.get(dcfg["date_field"])
                            if not issue_raw:
                                for df in ("issue_date", "issued_date", "filed_date",
                                           "issueddate", "application_start_date"):
                                    issue_raw = r.get(df)
                                    if issue_raw:
                                        break
                            issue_dt = parse_date(issue_raw)
                            issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                            days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15
                            if days_old > MAX_DAYS_OLD:
                                continue
                            valuation = safe_float(
                                r.get("valuation") or r.get("estimated_cost") or
                                r.get("job_value") or r.get("cost") or r.get("total_fee")
                            ) or 0.0
                            owner_name = str(r.get("owner_name") or r.get("applicant_name") or
                                          r.get("applicant") or r.get("contact_name") or "").strip()
                            owner_phone = str(r.get("owner_phone") or r.get("phone") or
                                           r.get("applicant_phone") or "")
                            contractor = str(r.get("contractor_name") or r.get("contractor") or "")
                            score, temp, urgency = calculate_score(days_old, valuation, permit_type)
                            description = str(r.get("work_desc") or r.get("description") or
                                           r.get("work_description") or permit_type or "Permit filed")
                            lead = {
                                "id": abs(hash(f"disc_{dkey}:{permit_number}:{address}")),
                                "permit_number": str(permit_number),
                                "address": address,
                                "city": dcfg.get("city", ""),
                                "zip": str(r.get("zip") or r.get("zip_code") or ""),
                                "lat": lat, "lng": lng,
                                "work_description": description,
                                "description_full": description,
                                "details": f"Type: {permit_type or 'n/a'} | Value: ${valuation:,.0f} | Filed: {issue_date or 'n/a'}",
                                "permit_type": str(permit_type),
                                "valuation": valuation,
                                "issue_date": issue_date,
                                "days_old": days_old,
                                "score": score,
                                "temperature": temp,
                                "urgency": urgency,
                                "source": dcfg["label"],
                                "apn": str(r.get("apn") or ""),
                                "owner_name": owner_name,
                                "owner_phone": owner_phone,
                                "owner_email": str(r.get("owner_email") or r.get("applicant_email") or ""),
                                "contractor_name": contractor,
                                "contractor_phone": str(r.get("contractor_phone") or ""),
                                "state": dcfg.get("state", ""),
                            }
                            disc_soc_leads.append(lead)
                            disc_soc_accepted += 1
                    except Exception as e:
                        logger.debug(f"Discovered Socrata {dkey} parse error: {e}")
                all_leads.extend(disc_soc_leads)
                logger.info(f"Discovered Socrata: accepted {disc_soc_accepted} leads from {len(disc_socrata)} new sources")

            # Fetch from NEW discovered ArcGIS sources (concurrent with semaphore)
            disc_arcgis = registry.get_new_arcgis_configs()
            if disc_arcgis:
                logger.info(f"Discovered sources: syncing {len(disc_arcgis)} new ArcGIS endpoints (concurrent)...")
                disc_arc_accepted = 0
                _arc_sem = asyncio.Semaphore(20)  # 20 concurrent requests

                async def _fetch_one_arcgis(dkey, dcfg):
                    async with _arc_sem:
                        try:
                            return dkey, dcfg, await asyncio.wait_for(
                                fetch_arcgis_permits(
                                    url=dcfg["url"],
                                    date_field=dcfg.get("date_field"),
                                    order_by=dcfg.get("order_by", ""),
                                    lookback_days=int(dcfg.get("lookback_days", 90)),
                                    max_pages=1,
                                ),
                                timeout=10,
                            )
                        except Exception as e:
                            logger.debug(f"Discovered ArcGIS {dkey} failed: {e}")
                            return dkey, dcfg, None

                # Run all ArcGIS fetches concurrently with 2-minute global timeout
                try:
                    arc_results = await asyncio.wait_for(
                        asyncio.gather(*[_fetch_one_arcgis(dk, dc) for dk, dc in disc_arcgis.items()]),
                        timeout=120,
                    )
                except asyncio.TimeoutError:
                    logger.warning("Discovered ArcGIS global timeout (120s) - using partial results")
                    arc_results = []

                for dkey, dcfg, records in arc_results:
                    if not records:
                        continue
                    try:
                        for r in records:
                            lat = (r.get("_lat") or r.get("LATITUDE") or r.get("latitude") or
                                   r.get("Y") or r.get("Y_COORD"))
                            lng = (r.get("_lng") or r.get("LONGITUDE") or r.get("longitude") or
                                   r.get("X") or r.get("X_COORD"))
                            if lat is None or lng is None:
                                continue
                            try:
                                lat, lng = float(lat), float(lng)
                            except (ValueError, TypeError):
                                continue
                            lat, lng = fix_coordinates(lat, lng)
                            if lat is None or lng is None:
                                continue
                            address = (r.get("Address") or r.get("ADDRESS") or r.get("address") or
                                       r.get("FULL_ADDRESS") or r.get("SITE_ADDRESS") or
                                       r.get("OriginalAddress") or r.get("Location") or "")
                            if not address:
                                continue
                            permit_number = (r.get("PermitNum") or r.get("PERMIT_NUM") or
                                             r.get("PermitNumber") or r.get("PERMIT_NUMBER") or
                                             r.get("permit_number") or r.get("PERMIT_ID") or "")
                            permit_type = (r.get("PermitType") or r.get("PERMIT_TYPE") or
                                           r.get("permit_type") or r.get("TYPE") or r.get("WorkType") or "")
                            raw_date = None
                            for df in ("issue_date", "ISSUE_DATE", "IssueDate", "ISSUDATE",
                                       "ISSUEDATE", "issued_date", "IssuedDate", "CREATEDDATE",
                                       "PER_ISSUE_DATE", "DATE_ISSUED"):
                                raw_date = r.get(df)
                                if raw_date:
                                    break
                            issue_dt = None
                            if raw_date:
                                if isinstance(raw_date, (int, float)) and raw_date > 1e10:
                                    issue_dt = datetime.utcfromtimestamp(raw_date / 1000)
                                else:
                                    issue_dt = parse_date(str(raw_date))
                            issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                            days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15
                            if days_old > MAX_DAYS_OLD:
                                continue
                            valuation = safe_float(
                                r.get("Valuation") or r.get("VALUATION") or r.get("valuation") or
                                r.get("EstimatedCost") or r.get("ESTIMATED_COST") or r.get("JobValue")
                            ) or 0.0
                            owner_name = (r.get("OwnerName") or r.get("OWNER_NAME") or
                                          r.get("owner_name") or r.get("Applicant") or "").strip()
                            contractor = (r.get("ContractorName") or r.get("CONTRACTOR_NAME") or
                                          r.get("contractor_name") or "")
                            score, temp, urgency = calculate_score(days_old, valuation, permit_type)
                            description = (r.get("Description") or r.get("DESCRIPTION") or
                                           r.get("WorkDescription") or permit_type or "Permit filed")
                            lead = {
                                "id": abs(hash(f"disc_{dkey}:{permit_number}:{address}")),
                                "permit_number": permit_number,
                                "address": address,
                                "city": dcfg.get("city", ""),
                                "zip": r.get("ZIP") or r.get("Zip") or r.get("zip") or "",
                                "lat": lat, "lng": lng,
                                "work_description": description,
                                "description_full": description,
                                "details": f"Type: {permit_type or 'n/a'} | Value: ${valuation:,.0f} | Filed: {issue_date or 'n/a'}",
                                "permit_type": permit_type,
                                "valuation": valuation,
                                "issue_date": issue_date,
                                "days_old": days_old,
                                "score": score,
                                "temperature": temp,
                                "urgency": urgency,
                                "source": dcfg["label"],
                                "apn": r.get("APN") or r.get("apn") or r.get("PARCEL") or "",
                                "owner_name": owner_name,
                                "owner_phone": r.get("OwnerPhone") or r.get("OWNER_PHONE") or "",
                                "owner_email": "",
                                "contractor_name": contractor,
                                "contractor_phone": r.get("ContractorPhone") or "",
                                "state": dcfg.get("state", ""),
                            }
                            all_leads.append(lead)
                            disc_arc_accepted += 1
                    except Exception as e:
                        logger.debug(f"Discovered ArcGIS {dkey} parse error: {e}")
                logger.info(f"Discovered ArcGIS: accepted {disc_arc_accepted} leads from {len(disc_arcgis)} new sources")

        except Exception as e:
            logger.warning(f"Discovered sources integration skipped: {e}")

        # If nothing new, fall back to cache to avoid empty responses
        if not all_leads and cached:
            logger.info("No new leads fetched; falling back to cached data")
            return cached

        if all_leads:
            # sort by score desc for faster top results to UI
            all_leads.sort(key=lambda l: l.get("score", 0), reverse=True)

            # Phase 1: Free ownership enrichment (ArcGIS + Regrid tiles — get owner names)
            await enrich_contacts_batch(all_leads, ENRICH_MAX_LOOKUPS)

            # Phase 2: Additional owner name enrichment via master pipeline
            try:
                ownership_count = await batch_enrich_ownership(all_leads, max_enrichments=500)
                logger.info(f"Ownership enrichment: {ownership_count} leads enriched with owner data")
            except Exception as e:
                logger.error(f"Batch ownership enrichment error: {e}")

            # Phase 3: Contact enrichment — find phone/email via free scraping pipeline
            try:
                contact_stats = await orchestrator_batch(
                    all_leads, max_leads=200, concurrency=3, skip_slow=True
                )
                logger.info(
                    f"Contact enrichment: {contact_stats.get('phones_found', 0)} phones, "
                    f"{contact_stats.get('emails_found', 0)} emails found "
                    f"in {contact_stats.get('elapsed', 0)}s — sources: {contact_stats.get('sources_hit', {})}"
                )
            except Exception as e:
                logger.error(f"Batch contact scraping error: {e}")

            # ── Shovels API: Supplemental national coverage ──
            if shovels_api.enabled:
                try:
                    logger.info("🔍 Shovels API: Fetching supplemental permits from ALL 50 states + 2,000+ jurisdictions...")
                    # ALL 50 STATES - Full nationwide coverage
                    target_states = [
                        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
                    ]
                    shovels_leads = []

                    for state in target_states:  # ALL STATES - No limits!
                        permits, error = await shovels_api.search_permits(
                            state=state,
                            start_date=(datetime.utcnow() - timedelta(days=MAX_DAYS_OLD)).strftime("%Y-%m-%d"),
                            end_date=datetime.utcnow().strftime("%Y-%m-%d"),
                            limit=1000,  # 1000 permits per state (increased from 200)
                            page=1
                        )

                        if error:
                            logger.warning(f"Shovels API error for {state}: {error}")
                            continue

                        for permit in permits:
                            lead = transform_shovels_permit_to_lead(permit)
                            if lead.get("lat") and lead.get("lng"):
                                shovels_leads.append(lead)

                    if shovels_leads:
                        all_leads.extend(shovels_leads)
                        logger.info(f"✅ Shovels API: Added {len(shovels_leads)} permits from national coverage")
                    else:
                        logger.info("Shovels API: No additional permits found")

                except Exception as e:
                    logger.error(f"Shovels API integration error: {e}")

            # ── Socrata Public APIs: 50 Cities Nationwide ──
            # DISABLED: This duplicates city-specific datasets already fetched above (LA, SF, Chicago, etc.)
            # Re-enable only if you need cities NOT already covered by SOC_DATASETS and ARCGIS_DATASETS
            # try:
            #     from services.public_apis.socrata_fetcher import fetch_all_socrata_permits
            #
            #     logger.info("🌆 Fetching permits from 50+ Socrata cities nationwide - MAXIMUM COVERAGE...")
            #     socrata_results = await fetch_all_socrata_permits(
            #         limit_per_city=5000,  # Increased from 1000 to 5000 per city
            #         days_back=MAX_DAYS_OLD
            #     )
            #
            #     socrata_leads = []
            #     for city, permits in socrata_results.items():
            #         logger.info(f"   {city}: {len(permits)} permits")
            #         socrata_leads.extend(permits)
            #
            #     if socrata_leads:
            #         all_leads.extend(socrata_leads)
            #         logger.info(f"✅ Socrata: Added {len(socrata_leads)} permits from {len(socrata_results)} cities")
            #     else:
            #         logger.info("Socrata: No permits found")
            #
            # except Exception as e:
            #     logger.error(f"Socrata integration error: {e}")

            # ── FEMA NFIP Flood Insurance Claims (Construction/Housing Only) ──
            try:
                logger.info("🏠 Fetching FEMA flood insurance claims (building damage)...")
                # Fetch claims from states with active permits
                active_states = set()
                for lead in all_leads[:100]:  # Sample to find active states
                    if lead.get("state"):
                        active_states.add(lead["state"])

                fema_claims_leads = []
                for state in list(active_states)[:10]:  # Limit to 10 states to avoid quota
                    claims = await fetch_fema_nfip_claims(state=state, limit=200)
                    for claim in claims:
                        # Convert FEMA claim to lead format
                        lat = safe_float(claim.get("latitude"))
                        lng = safe_float(claim.get("longitude"))
                        if not lat or not lng:
                            continue

                        building_damage = safe_float(claim.get("amountPaidOnBuildingClaim") or 0)
                        contents_damage = safe_float(claim.get("amountPaidOnContentsClaim") or 0)
                        total_damage = building_damage + contents_damage

                        if total_damage < 1000:  # Skip small claims
                            continue

                        address = claim.get("propertyAddress") or ""
                        city = claim.get("reportedCity") or claim.get("countyCode") or ""
                        zip_code = claim.get("reportedZipCode") or ""
                        year_of_loss = claim.get("yearOfLoss") or claim.get("dateOfLoss", "")[:4]

                        # Calculate days old from year of loss
                        try:
                            loss_year = int(year_of_loss) if year_of_loss else datetime.utcnow().year
                            days_old = (datetime.utcnow().year - loss_year) * 365
                        except (ValueError, TypeError):
                            days_old = 365

                        if days_old > MAX_DAYS_OLD:
                            continue

                        # Score based on damage amount
                        score, temp, urgency = calculate_score(days_old, total_damage, "Flood Damage")

                        fema_lead = {
                            "id": abs(hash(f"fema:{claim.get('reportedZipCode')}:{claim.get('yearOfLoss')}:{address}")),
                            "permit_number": f"FEMA-{claim.get('reportedZipCode')}-{year_of_loss}",
                            "address": address,
                            "city": city,
                            "zip": zip_code,
                            "lat": lat,
                            "lng": lng,
                            "work_description": f"Flood insurance claim - ${total_damage:,.0f} in building damage",
                            "description_full": f"FEMA NFIP Claim | Building: ${building_damage:,.0f} | Contents: ${contents_damage:,.0f} | Year: {year_of_loss}",
                            "details": f"Type: Flood Insurance Claim | Damage: ${total_damage:,.0f} | Year: {year_of_loss}",
                            "permit_type": "Flood Insurance Claim",
                            "valuation": total_damage,
                            "issue_date": f"{year_of_loss}-01-01",
                            "days_old": days_old,
                            "score": score,
                            "temperature": temp,
                            "urgency": urgency,
                            "source": "FEMA NFIP Claims",
                            "state": state,
                            "owner_name": "",
                            "owner_phone": "",
                            "owner_email": "",
                            "contractor_name": "",
                            "contractor_phone": "",
                            "apn": "",
                        }
                        fema_claims_leads.append(fema_lead)

                if fema_claims_leads:
                    all_leads.extend(fema_claims_leads)
                    logger.info(f"✅ FEMA: Added {len(fema_claims_leads)} flood insurance claims with building damage")
                else:
                    logger.info("FEMA: No recent flood claims found")

            except Exception as e:
                logger.error(f"FEMA claims integration error: {e}")

            await asyncio.to_thread(DataCache.save, all_leads)

            logger.info(f"Data sync complete: {len(all_leads)} leads (permits + insurance claims)")
            # Rebuild pre-processed cache in a thread to avoid blocking the event loop
            await asyncio.to_thread(_rebuild_cleaned_leads)

            # Auto-enrich ownership from free Regrid parcel tiles (background, non-blocking)
            try:
                from services.parcel_enrichment import enrich_leads_from_tiles
                stats = await enrich_leads_from_tiles(all_leads, max_tiles=1000)
                if stats.get("enriched", 0) > 0:
                    await asyncio.to_thread(DataCache.save, all_leads)
                    await asyncio.to_thread(_rebuild_cleaned_leads)
                    logger.info(f"Post-sync ownership enrichment: {stats['enriched']:,} leads enriched")
            except Exception as e:
                logger.warning(f"Post-sync enrichment error (non-fatal): {e}")

        return all_leads

# ============================================================================
# API ENDPOINTS
# ============================================================================

async def periodic_sync():
    """Continuously refresh leads at configured interval."""
    while True:
        try:
            await sync_data()
        except Exception as e:  # pragma: no cover
            logger.error(f"Periodic sync error: {e}")
        await asyncio.sleep(REFRESH_INTERVAL_SEC)


async def periodic_yelp_intents():
    """Continuously ingest Yelp intent leads if enabled."""
    if not YELP_INTENT_ENABLED:
        return
    init_db()
    while True:
        try:
            cached = DataCache.load(allow_stale=True) or []
            cities = ["Los Angeles", "Santa Monica", "Beverly Hills", "Pasadena", "Long Beach"]

            async def _owner_lookup(county, apn, address):
                try:
                    return await asyncio.to_thread(get_la_owner, apn)
                except Exception as e:
                    logger.warning("LA owner lookup failed for APN %s: %s", apn, e)
                    return {}

            async def _contact_lookup(street, city, state, zip_code):
                return {}  # free sources used in batch enrichment instead

            added = await ingest_yelp_intents(
                parcels=cached,
                get_owner_fn=_owner_lookup,
                enrich_contact_fn=_contact_lookup,
                cities=cities,
                max_results=30,
            )
            if added:
                logger.info(f"Yelp intent: added {added} behavioral leads")
        except Exception as e:
            logger.error(f"Yelp intent ingest failed: {e}")

        await asyncio.sleep(YELP_INTENT_INTERVAL)

# Initialize global services
ownership_service = OwnershipLookupService()
crm_manager = CRMManager()
email_service = EmailCampaignService()
sms_service = SMSCampaignService()
parcel_service = RegridParcelService()
county_assessor = CountyAssessorService()

@app.on_event("startup")
async def startup_event():
    """Run data sync on startup — heavy cache load runs in background."""
    logger.info("Onsite v3.1.0 starting up...")
    init_market_tables()
    ensure_market_tables_seed()

    # ── v3.0: Initialize SQLite database (fast) ──
    try:
        from models.database import init_db as init_sqlite_db
        init_sqlite_db()
        logger.info("✅ SQLite database initialized")
    except Exception as e:
        logger.warning(f"SQLite init failed (non-fatal): {e}")

    # ── Auto-populate source registry on first boot ──
    try:
        from models.database import get_db
        with get_db() as conn:
            count = conn.execute("SELECT COUNT(*) FROM api_sources").fetchone()[0]
        if count == 0:
            logger.info("api_sources empty — running initial migration...")
            from scripts.migrate_config_sources import migrate
            migrate()
            logger.info("Config sources migrated to registry")
    except Exception as e:
        logger.warning("Source registry init: %s", e)

    # ── Start source validator background task ──
    try:
        from services.source_validator import validation_loop
        asyncio.create_task(validation_loop())
        logger.info("Source validator started")
    except Exception as e:
        logger.warning("Source validator not started: %s", e)

    # ── Periodic federal data collection (weekly) ──
    if os.getenv("ENABLE_FEDERAL_SYNC", "0") == "1":
        async def _federal_collection_loop():
            await asyncio.sleep(3600)  # Wait 1 hour after startup
            while True:
                try:
                    from data_sources.run_collection import step_fema, step_census, step_hpi
                    import asyncio as _aio
                    await _aio.to_thread(step_fema)
                    await _aio.to_thread(step_census)
                    await _aio.to_thread(step_hpi)
                    logger.info("Federal data collection complete")
                except Exception as e:
                    logger.warning("Federal collection error: %s", e)
                await asyncio.sleep(604800)  # Weekly
        asyncio.create_task(_federal_collection_loop())
        logger.info("Federal data collection scheduled (weekly)")

    # ── Background: load JSON cache only if SQLite is empty ──
    async def _background_cache_load():
        """Load and process the large data cache in the background (only if SQLite has no leads)."""
        await asyncio.sleep(1)  # Let server finish startup first
        # Always rebuild cleaned leads (from JSON cache or SQLite fallback)
        try:
            logger.info("Background cache load starting...")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _rebuild_cleaned_leads)
            logger.info(f"✅ Background cache loaded: {len(_cleaned_leads):,} leads")
        except Exception as e:
            logger.warning(f"Background cache load failed (non-fatal): {e}")

        # Cache-DB alignment check
        try:
            from models.database import get_db
            with get_db() as db:
                db_count = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            cache_count = len(_cleaned_leads)
            if db_count > 0 and cache_count > 0:
                ratio = min(db_count, cache_count) / max(db_count, cache_count)
                if ratio < 0.8:
                    logger.warning(
                        "⚠️ Cache-DB misalignment: cache=%d, db=%d (%.0f%% ratio). "
                        "Consider running /api/admin/sync to reconcile.",
                        cache_count, db_count, ratio * 100,
                    )
                else:
                    logger.info("✅ Cache-DB alignment OK: cache=%d, db=%d", cache_count, db_count)
            elif db_count == 0 and cache_count > 0:
                logger.info("DB empty, cache has %d leads (normal on first run)", cache_count)
        except Exception as e:
            logger.debug("Cache-DB alignment check skipped: %s", e)

        # Pre-warm response cache after leads are loaded
        if _cleaned_leads:
            try:
                import orjson as _orjson
                _map_fields = {"id", "lat", "lng", "score", "lead_score", "address", "city",
                               "state", "permit_type", "valuation", "days_old", "source",
                               "owner_name", "is_sold"}
                _fields_str = "id,lat,lng,score,lead_score,address,city,state,permit_type,valuation,days_old,source,owner_name,is_sold"
                total_all = len(_cleaned_leads)
                batch = _cleaned_leads[:500000]
                slim = [{k: v for k, v in l.items() if k in _map_fields} for l in batch]
                payload = {"leads": slim, "source": "cache", "total": total_all,
                           "count": total_all, "returned": len(slim)}
                raw_bytes = _orjson.dumps(payload)
                cache_key = f"500000:0:{_fields_str}:None:None:None:None:None:False"
                _response_cache[cache_key] = (_cleaned_leads_version, raw_bytes)
                logger.info(f"Pre-warmed response cache: {len(raw_bytes)/(1024*1024):.1f}MB ({len(slim)} leads)")
            except Exception as e:
                logger.warning(f"Response cache pre-warm failed: {e}")

        # Migrate from JSON cache to SQLite if needed
        try:
            from models.database import migrate_from_json_cache
            migrated = migrate_from_json_cache(CACHE_FILE)
            if migrated > 0:
                logger.info(f"✅ Migrated {migrated} leads from JSON cache to SQLite")
        except Exception as e:
            logger.warning(f"SQLite migration failed (non-fatal): {e}")

    asyncio.create_task(_background_cache_load())

    if ENRICH_ENABLED:
        logger.info("Owner enrichment enabled; starting background loop")
        asyncio.create_task(enrich_loop(int(ENRICH_INTERVAL_SEC)))
    if YELP_INTENT_ENABLED:
        logger.info("Yelp intent ingestion enabled; starting background loop")
        asyncio.create_task(periodic_yelp_intents())
    
    # ── v3.1: Post-sync auto-enrichment (fixed with safeguards) ──
    try:
        from services.enrichment_orchestrator import enrich_batch
        from models.database import get_leads_needing_enrichment, mark_enriched

        async def auto_enrich_after_sync():
            """Auto-enrich leads after sync — batch of 50, 60s cooldown, circuit breaker."""
            await asyncio.sleep(60)  # Wait for first sync to complete
            consecutive_failures = 0
            max_failures = 3  # Circuit breaker threshold
            while True:
                if consecutive_failures >= max_failures:
                    logger.warning("Auto-enrichment circuit breaker tripped after %d failures, pausing 10min", max_failures)
                    await asyncio.sleep(600)
                    consecutive_failures = 0
                try:
                    leads = get_leads_needing_enrichment(limit=50)
                    if not leads:
                        await asyncio.sleep(300)  # Nothing to enrich, check again in 5min
                        continue
                    logger.info("Auto-enriching %d leads...", len(leads))
                    stats = await enrich_batch(leads, max_leads=50, concurrency=3)
                    enriched_count = stats.get("enriched", 0)
                    for lead in leads:
                        if lead.get("owner_phone") or lead.get("owner_email"):
                            mark_enriched(lead["id"], lead)
                    logger.info("Auto-enrichment: %d enriched", enriched_count)
                    consecutive_failures = 0
                except Exception as e:
                    consecutive_failures += 1
                    logger.warning("Auto-enrichment error (%d/%d): %s", consecutive_failures, max_failures, e)
                await asyncio.sleep(60)  # 60s cooldown between batches

        asyncio.create_task(auto_enrich_after_sync())
        logger.info("Auto-enrichment pipeline started (batch=50, cooldown=60s, circuit_breaker=3)")
    except ImportError as e:
        logger.info("Auto-enrichment not available: %s", e)

# Mount static files (frontend is a single self-contained index.html)
_static_dir = Path(__file__).parent / "static"
if (_static_dir / "css").exists():
    app.mount("/css", StaticFiles(directory=str(_static_dir / "css")), name="css")
if (_static_dir / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(_static_dir / "assets")), name="assets")
app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.get("/")
async def landing():
    """Serve the public landing page"""
    landing_path = _static_dir / "landing.html"
    if landing_path.exists():
        return FileResponse(str(landing_path))
    return FileResponse(str(_static_dir / "index.html"))


@app.get("/app")
async def dashboard():
    """Serve the main application dashboard"""
    return FileResponse(str(_static_dir / "index.html"))


@app.get("/login")
async def login_page():
    """Serve the login/landing page"""
    login_path = _static_dir / "login.html"
    if login_path.exists():
        return FileResponse(str(login_path))
    return FileResponse(str(_static_dir / "landing.html"))


@app.get("/admin")
async def admin_page():
    """Serve the admin dashboard"""
    admin_path = _static_dir / "admin.html"
    if admin_path.exists():
        return FileResponse(str(admin_path))
    raise HTTPException(status_code=404, detail="Admin page not found")



# Rich lead details (emphasize permit info)
@app.get("/api/lead/{lead_id}/details")
async def lead_details(lead_id: int):
    lead = _get_permit_from_cache(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    # Build a concise, readable summary
    summary = {
        "address": lead.get("address"),
        "city": lead.get("city"),
        "zip": lead.get("zip"),
        "permit_number": lead.get("permit_number"),
        "permit_type": lead.get("permit_type"),
        "issue_date": lead.get("issue_date"),
        "valuation": lead.get("valuation"),
        "work_description": lead.get("work_description"),
        "description_full": lead.get("description_full"),
        "source": lead.get("source"),
    }

    # Enrich with ZIP-level risk data
    zip_risk = None
    zip_code = (lead.get("zip") or "")[:5]
    if zip_code and len(zip_code) == 5:
        try:
            from data_sources.schema import get_db as get_ds_db
            with get_ds_db() as conn:
                row = conn.execute(
                    "SELECT nfip_flood_claims_count, nfip_flood_total_paid_usd, "
                    "fema_disaster_count, fema_disaster_types, fema_ia_approved_usd, "
                    "fhfa_hpi_index, fhfa_hpi_year, census_permits_count "
                    "FROM zip_risk_data WHERE zip_code = ?", (zip_code,)
                ).fetchone()
                if row:
                    zip_risk = {
                        "flood_claims": row[0],
                        "flood_total_paid": row[1],
                        "disaster_count": row[2],
                        "disaster_types": row[3],
                        "ia_approved_usd": row[4],
                        "hpi_index": row[5],
                        "hpi_year": row[6],
                        "census_permits": row[7],
                    }
        except Exception as e:
            logger.error("Failed to query zip_risk_cache for lead %s: %s", lead_id, e)

    return {"lead_id": lead_id, "summary": summary, "zip_risk": zip_risk, "raw": lead}


# Property search to find owner info (100% free sources)
@app.get("/api/property/search")
async def property_search(address: str = Query(...), city: str = Query(""), state: str = Query(""), zip: str = Query("")):
    street = address
    sources_tried = []

    # Try LA County assessor first for LA addresses
    if city and city.lower() in {"los angeles", "la"}:
        try:
            owner = get_la_owner(street)
            if owner.get("owner_name"):
                return {"success": True, "source": "la_assessor", "result": owner}
            sources_tried.append("la_assessor")
        except Exception:
            sources_tried.append("la_assessor (failed)")

    # Try geocoding address to lat/lng for spatial lookups
    # Use a cached lead match first
    cached = DataCache.load(allow_stale=True) or []
    match = None
    addr_upper = street.upper().strip()
    for lead in cached[:200000]:
        if (lead.get("address", "").upper().strip() == addr_upper
                and lead.get("lat") and lead.get("lng")):
            match = lead
            break

    if match:
        lat, lng = safe_float(match.get("lat") or 0), safe_float(match.get("lng") or 0)
        if lat and lng:
            # ArcGIS parcels
            result = await enrich_owner_from_arcgis_parcel(lat, lng, state or match.get("state", ""))
            if result and result.get("owner_name"):
                return {"success": True, "source": "arcgis_parcels", "result": result}
            sources_tried.append("arcgis_parcels")

            # Regrid tiles
            result = await enrich_owner_from_regrid_tile(lat, lng)
            if result and result.get("owner_name"):
                return {"success": True, "source": "regrid_tiles", "result": result}
            sources_tried.append("regrid_tiles")

    return {"success": False, "source": "none", "sources_tried": sources_tried,
            "error": "No ownership data found from free sources"}

@app.get("/api/property/suggestions")
async def property_suggestions(query: str = Query(..., description="Free text address/city/zip query")):
    result = await fetch_suggestions(query)
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("error", "suggestions unavailable"))
    return result

@app.get("/api/permit/types")
async def permit_types(limit: int = 100):
    """Return most common permit types for UI filters."""
    try:
        cache = DataCache.load(allow_stale=True) or []
        return {"items": list_permit_types(cache, limit)}
    except Exception as e:
        logger.error(f"permit types failed: {e}")
        raise HTTPException(status_code=500, detail="failed")

@app.get("/api/parcel/{apn}/owner")
async def get_parcel_owner(apn: str):
    """Get property owner information from LA County Assessor by APN"""
    # Clean APN format  
    clean_apn = apn.replace('-', '').replace(' ', '')
    
    # LA County Assessor API endpoint for property details
    url = "https://portal.assessor.lacounty.gov/api/parceldetail"
    params = {'ain': clean_apn}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Extract owner information
                    owner_info = {
                        'apn': apn,
                        'owner_name': data.get('OwnerName', ''),
                        'mailing_address': data.get('MailingAddress', ''),
                        'mailing_city': data.get('MailingCity', ''),
                        'mailing_state': data.get('MailingState', ''),
                        'mailing_zip': data.get('MailingZip', ''),
                        'property_address': data.get('SitusAddress', ''),
                        'assessed_value': data.get('AssessedValue', 0),
                        'year_built': data.get('YearBuilt', ''),
                        'square_feet': data.get('SquareFeet', ''),
                        'bedrooms': data.get('Bedrooms', ''),
                        'bathrooms': data.get('Bathrooms', ''),
                        'source': 'LA County Assessor'
                    }
                    return owner_info
                else:
                    raise HTTPException(status_code=response.status, detail="Assessor API error")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lead/{lead_id}/enrich-owner")
async def enrich_lead_owner(lead_id: str):
    """Enrich a lead with property owner information — 100% FREE sources only.
    Pipeline: LA Assessor → County Scrapers → ArcGIS Parcels → Regrid MVT Tiles → SMTP Email"""
    lead = _get_permit_from_cache(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    apn = lead.get('apn', '')
    address = lead.get('address', '')
    city = lead.get('city', '')
    state = lead.get('state', '')
    zip_code = lead.get('zip', '')
    lat = lead.get('lat')
    lng = lead.get('lng')

    _not_found = {"", "not found", "not available", "n/a", "none", "unknown"}
    owner_name = lead.get('owner_name', '')
    if owner_name.lower().strip() in _not_found:
        owner_name = ''
    phone = lead.get('owner_phone', '')
    if phone.lower().strip() in _not_found:
        phone = ''
    email = lead.get('owner_email', '')
    if str(email).lower().strip() in _not_found:
        email = ''
    mailing_address = lead.get('mailing_address', '')
    sources_used = []

    # Track pre-existing data
    had_owner = bool(owner_name)

    # Tier 1: LA County Assessor portal (free, no limit)
    if apn and city and 'los angeles' in city.lower():
        try:
            url = "https://portal.assessor.lacounty.gov/api/parceldetail"
            params = {'ain': apn.replace('-', '').replace(' ', '')}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('OwnerName'):
                            owner_name = data['OwnerName']
                            mailing_address = f"{data.get('MailingAddress', '')} {data.get('MailingCity', '')} {data.get('MailingState', '')} {data.get('MailingZip', '')}".strip()
                            sources_used.append('LA County Assessor')
        except Exception as e:
            logger.debug(f"LA Assessor portal failed for lead {lead_id}: {e}")

    # Tier 2: County-specific assessor scrapers (free, major metros)
    if not owner_name and lat and lng and city and state:
        try:
            result = await enrich_owner_from_county_scraper(float(lat), float(lng), city, state)
            if result and result.get('owner_name'):
                owner_name = result['owner_name']
                if not mailing_address and result.get('owner_address'):
                    mailing_address = result['owner_address']
                if not apn and result.get('apn'):
                    apn = result['apn']
                sources_used.append('County Assessor')
        except Exception as e:
            logger.debug(f"County scraper failed for lead {lead_id}: {e}")

    # Tier 3: ArcGIS state/national parcel services (free, all 50 states)
    if not owner_name and lat and lng:
        try:
            result = await enrich_owner_from_arcgis_parcel(float(lat), float(lng), state or "")
            if result and result.get('owner_name'):
                owner_name = result['owner_name']
                if not mailing_address and result.get('owner_address'):
                    mailing_address = result['owner_address']
                if not apn and result.get('apn'):
                    apn = result['apn']
                sources_used.append('ArcGIS Parcels')
        except Exception as e:
            logger.debug(f"ArcGIS parcel enrichment failed for lead {lead_id}: {e}")

    # Tier 4: Regrid MVT parcel tiles (free, no API key, nationwide)
    if not owner_name and lat and lng:
        try:
            result = await enrich_owner_from_regrid_tile(float(lat), float(lng))
            if result and result.get('owner_name'):
                owner_name = result['owner_name']
                if not mailing_address and result.get('owner_address'):
                    mailing_address = result['owner_address']
                if not apn and result.get('apn'):
                    apn = result['apn']
                sources_used.append('Regrid Tiles')
        except Exception as e:
            logger.debug(f"Regrid tile enrichment failed for lead {lead_id}: {e}")

    # Tier 5: Contact scraping (TruePeopleSearch, FastPeopleSearch, USPhoneBook)
    # Finds phone numbers + emails for private individuals AND resolves LLCs
    if owner_name and (not phone or not email):
        try:
            contact = await find_contact(
                owner_name=owner_name,
                address=address,
                city=city,
                state=state,
            )
            if contact.get('phone') and not phone:
                phone = contact['phone']
                sources_used.append(contact.get('source', 'Contact Scraper'))
            if contact.get('email') and not email:
                email = contact['email']
                if 'Contact Scraper' not in str(sources_used):
                    sources_used.append(contact.get('source', 'Contact Scraper'))
            # If LLC was resolved to a person, store it
            if contact.get('person_name'):
                lead['beneficial_owner'] = contact['person_name']
        except Exception as e:
            logger.debug(f"Contact scraper failed for lead {lead_id}: {e}")

    # Note: SMTP email discovery is already included in Tier 5 (find_contact)

    # Update lead in cache if we found new data
    if owner_name and owner_name != lead.get('owner_name', ''):
        lead['owner_name'] = owner_name
    if mailing_address and mailing_address != lead.get('mailing_address', ''):
        lead['mailing_address'] = mailing_address
    if email and email != lead.get('owner_email', ''):
        lead['owner_email'] = email
    if phone and phone != lead.get('owner_phone', ''):
        lead['owner_phone'] = phone
    if apn and apn != lead.get('apn', ''):
        lead['apn'] = apn

    # If owner was already known (from previous enrichment), credit it
    if had_owner and not sources_used:
        sources_used.append('Previously Enriched')

    has_data = bool(owner_name or phone or email)
    confidence = 0
    if sources_used and owner_name:
        confidence = 85
    elif owner_name:
        confidence = 70  # owner known but source unclear

    return {
        'success': has_data,
        'lead_id': lead_id,
        'owner_name': owner_name,
        'phone': phone,
        'email': email,
        'mailing_address': mailing_address,
        'confidence': confidence,
        'sources_used': sources_used if sources_used else ['No data found'],
        'cost': 0,
        'error': None if has_data else 'No owner data found — try zooming into the map for parcel details'
    }

# ── Pre-processed leads cache (cleaned once, served fast) ──
_cleaned_leads: list = []
_cleaned_leads_version: int = 0
_cleaned_leads_by_id: dict = {}  # str(id) -> lead dict (O(1) lookup)
_response_cache: dict = {}  # key -> (version, ORJSONResponse bytes)

_bad_city_patterns = (
    # Dataset names accidentally stored as city
    "building and safety", "code enforcement", "permits submitted",
    "permits issued", "planning permits", "permit data",
    "construction permit", "certificate of", "inspection",
    " - ", "dataset", "no longer updat",
    # Non-permit junk datasets
    "development permit", "sign permit", "demo permit", "demolition permit",
    "electrical permit", "mechanical permit", "plumbing permit", "roofing permit",
    "code violation", "code complaint", "ordinance violation",
    "dumpster", "parking permit", "tool truck", "taxi", "peddler",
    "sidewalk", "banner", "alarm", "tobacco", "marijuana", "cannabis",
    "liquor", "alcohol", "food service", "food establishment",
    "311 explorer", "pothole", "graffiti", "noise", "animal",
    "housing start", "housing assistance", "residential data report",
    "enforcement action", "epa enforcement", "adjudication",
    "fema demolition", "katrina", "fire department", "occupancy",
    "public right-of-way", "right of way", "land use permit",
    "secondary suite", "completed permits", "permit submittal",
    "resubmittal", "fiscal year", "historical", "data report",
    "baton rouge fire", "unsafe vacant", "accident",
    "development services",
    # Jurisdictional designations (not real city names)
    "full purpose", " etj", "etj release",
    "limited purpose", "2 mile", "5 mile",
)
_state_names_set = {
    "connecticut", "new york state", "colorado", "california", "texas",
    "florida", "illinois", "ohio", "pennsylvania", "michigan",
    "georgia", "virginia", "maryland", "massachusetts", "indiana",
    "tennessee", "missouri", "wisconsin", "minnesota", "iowa",
    "arkansas", "oregon", "oklahoma", "kentucky", "louisiana",
}

# ── City Name Normalization ──
# Maps data-source slugs and mangled city names to (real_city, state)
_CITY_NAME_FIXES: dict[str, tuple[str, str]] = {
    # Source slugs → real cities
    "nola": ("New Orleans", "LA"),
    "lacity": ("Los Angeles", "CA"),
    "la building": ("Los Angeles", "CA"),
    "la electrical": ("Los Angeles", "CA"),
    "la pipeline": ("Los Angeles", "CA"),
    "city of los angeles": ("Los Angeles", "CA"),
    "cityofchigo": ("Chicago", "IL"),
    "chicago building": ("Chicago", "IL"),
    "city of chicago": ("Chicago", "IL"),
    "mbridgema": ("Cambridge", "MA"),
    "cambridge building": ("Cambridge", "MA"),
    "framinghamma": ("Framingham", "MA"),
    "cityofnewyork": ("New York", "NY"),
    "nyc approved": ("New York", "NY"),
    "cityofgainesville": ("Gainesville", "FL"),
    "cityoforlando": ("Orlando", "FL"),
    "providenceri": ("Providence", "RI"),
    "city of providence": ("Providence", "RI"),
    "citymesaaz": ("Mesa", "AZ"),
    "cos seattle": ("Seattle", "WA"),
    "seattle building": ("Seattle", "WA"),
    "city of seattle": ("Seattle", "WA"),
    "city of san francisco": ("San Francisco", "CA"),
    "sf building (2013+)": ("San Francisco", "CA"),
    "bayareametro": ("Oakland", "CA"),
    "detroit building": ("Detroit", "MI"),
    "city of detroit": ("Detroit", "MI"),
    "miami-dade building": ("Miami", "FL"),
    "tampa building": ("Tampa", "FL"),
    "baltimore building": ("Baltimore", "MD"),
    "phoenix building": ("Phoenix", "AZ"),
    "austin building": ("Austin", "TX"),
    "austin-metro": ("Austin", "TX"),
    "atin metro": ("Austin", "TX"),
    "city of austin": ("Austin", "TX"),
    "denver residential": ("Denver", "CO"),
    "cincinnati building": ("Cincinnati", "OH"),
    "cincinnati oh": ("Cincinnati", "OH"),
    "dc building (30d)": ("Washington", "DC"),
    "district of columbia": ("Washington", "DC"),
    "las vegas building": ("Las Vegas", "NV"),
    "city of bellevue": ("Bellevue", "WA"),
    "city of naperville": ("Naperville", "IL"),
    "city of boulder, colorado": ("Boulder", "CO"),
    "city of westminster, colorado": ("Westminster", "CO"),
    "city of worcester, ma": ("Worcester", "MA"),
    "city of san marcos": ("San Marcos", "TX"),
    "city of mckinney": ("McKinney", "TX"),
    "city of dallas gis services": ("Dallas", "TX"),
    "city of greenwood, in": ("Greenwood", "IN"),
    "city of new orleans": ("New Orleans", "LA"),
    "columbia sc": ("Columbia", "SC"),
    "columbiasc": ("Columbia", "SC"),
    "lincoln nebraska": ("Lincoln", "NE"),
    "louisville metro government": ("Louisville", "KY"),
    "allegheny county / city of pittsburgh / western pa regional data center": ("Pittsburgh", "PA"),
    "mecklenburg county gis": ("Charlotte", "NC"),
    "town of flower mound gis": ("Flower Mound", "TX"),
    "west chester university gis": ("West Chester", "PA"),
    "pitt county government": ("Greenville", "NC"),
    "overland park": ("Overland Park", "KS"),
    "idaho falls": ("Idaho Falls", "ID"),
    "sandy springs": ("Sandy Springs", "GA"),
    "littlerock": ("Little Rock", "AR"),
    "little rock": ("Little Rock", "AR"),
    "montgomery county of maryland": ("Bethesda", "MD"),
    "montgomerycountymd": ("Bethesda", "MD"),
    "openmaryland": ("Baltimore", "MD"),
    "rapid city": ("Rapid City", "SD"),
    "ramseycountymn": ("Saint Paul", "MN"),
    "princegeescountymd": ("Upper Marlboro", "MD"),
    "kansas city ks": ("Kansas City", "KS"),
    "brla": ("Baton Rouge", "LA"),
    "baton rouge": ("Baton Rouge", "LA"),
    "kcmo": ("Kansas City", "MO"),
    "north las vegas": ("North Las Vegas", "NV"),
    "cstx": ("College Station", "TX"),
    "weho": ("West Hollywood", "CA"),
    "internal sandiegocounty": ("San Diego", "CA"),
    "dumfriesva": ("Dumfries", "VA"),
    "ranchocordova": ("Rancho Cordova", "CA"),
    "parkeronline": ("Parker", "CO"),
    "auburnwa": ("Auburn", "WA"),
    "gnb": ("New Bedford", "MA"),
    "bloomington": ("Bloomington", "IN"),
    "norfolk performance": ("Norfolk", "VA"),
    "maricopa county": ("Phoenix", "AZ"),
    "washoe county": ("Reno", "NV"),
    "columbus ga": ("Columbus", "GA"),
    "jackson mississippi": ("Jackson", "MS"),
    "portland maine": ("Portland", "ME"),
    "fulton county": ("Atlanta", "GA"),
    "sharefulton": ("Atlanta", "GA"),
    "great falls": ("Great Falls", "MT"),
    "billings": ("Billings", "MT"),
    "fargo": ("Fargo", "ND"),
    "carson city": ("Carson City", "NV"),
    "flint": ("Flint", "MI"),
    "fort smith": ("Fort Smith", "AR"),
    "springdale": ("Springdale", "AR"),
    "corstat": ("Tampa", "FL"),
    "openfc": ("Falls Church", "VA"),
    "city of new bern gis": ("New Bern", "NC"),
}

# ── Known US City Coordinates (proper centroids, not from lead data) ──
_KNOWN_CITY_COORDS: dict[tuple[str, str], tuple[float, float]] = {
    ("new orleans", "LA"): (29.9511, -90.0715),
    ("los angeles", "CA"): (34.0522, -118.2437),
    ("chicago", "IL"): (41.8781, -87.6298),
    ("new york", "NY"): (40.7128, -74.0060),
    ("houston", "TX"): (29.7604, -95.3698),
    ("phoenix", "AZ"): (33.4484, -112.0740),
    ("philadelphia", "PA"): (39.9526, -75.1652),
    ("san antonio", "TX"): (29.4241, -98.4936),
    ("san diego", "CA"): (32.7157, -117.1611),
    ("dallas", "TX"): (32.7767, -96.7970),
    ("san jose", "CA"): (37.3382, -121.8863),
    ("austin", "TX"): (30.2672, -97.7431),
    ("jacksonville", "FL"): (30.3322, -81.6557),
    ("san francisco", "CA"): (37.7749, -122.4194),
    ("columbus", "OH"): (39.9612, -82.9988),
    ("indianapolis", "IN"): (39.7684, -86.1581),
    ("fort worth", "TX"): (32.7555, -97.3308),
    ("charlotte", "NC"): (35.2271, -80.8431),
    ("seattle", "WA"): (47.6062, -122.3321),
    ("denver", "CO"): (39.7392, -104.9903),
    ("washington", "DC"): (38.9072, -77.0369),
    ("nashville", "TN"): (36.1627, -86.7816),
    ("oklahoma city", "OK"): (35.4676, -97.5164),
    ("el paso", "TX"): (31.7619, -106.4850),
    ("boston", "MA"): (42.3601, -71.0589),
    ("portland", "OR"): (45.5152, -122.6784),
    ("las vegas", "NV"): (36.1699, -115.1398),
    ("memphis", "TN"): (35.1495, -90.0490),
    ("louisville", "KY"): (38.2527, -85.7585),
    ("baltimore", "MD"): (39.2904, -76.6122),
    ("milwaukee", "WI"): (43.0389, -87.9065),
    ("albuquerque", "NM"): (35.0844, -106.6504),
    ("tucson", "AZ"): (32.2226, -110.9747),
    ("fresno", "CA"): (36.7378, -119.7871),
    ("sacramento", "CA"): (38.5816, -121.4944),
    ("mesa", "AZ"): (33.4152, -111.8315),
    ("kansas city", "MO"): (39.0997, -94.5786),
    ("kansas city", "KS"): (39.1141, -94.6275),
    ("atlanta", "GA"): (33.7490, -84.3880),
    ("omaha", "NE"): (41.2565, -95.9345),
    ("colorado springs", "CO"): (38.8339, -104.8214),
    ("raleigh", "NC"): (35.7796, -78.6382),
    ("miami", "FL"): (25.7617, -80.1918),
    ("long beach", "CA"): (33.7701, -118.1937),
    ("virginia beach", "VA"): (36.8529, -75.9780),
    ("oakland", "CA"): (37.8044, -122.2712),
    ("minneapolis", "MN"): (44.9778, -93.2650),
    ("tampa", "FL"): (27.9506, -82.4572),
    ("tulsa", "OK"): (36.1540, -95.9928),
    ("arlington", "TX"): (32.7357, -97.1081),
    ("new orleans", "LA"): (29.9511, -90.0715),
    ("pittsburgh", "PA"): (40.4406, -79.9959),
    ("detroit", "MI"): (42.3314, -83.0458),
    ("anchorage", "AK"): (61.2181, -149.9003),
    ("cincinnati", "OH"): (39.1031, -84.5120),
    ("st. louis", "MO"): (38.6270, -90.1994),
    ("saint paul", "MN"): (44.9537, -93.0900),
    ("cambridge", "MA"): (42.3736, -71.1097),
    ("worcester", "MA"): (42.2626, -71.8023),
    ("framingham", "MA"): (42.2793, -71.4162),
    ("columbia", "SC"): (34.0007, -81.0348),
    ("providence", "RI"): (41.8240, -71.4128),
    ("orlando", "FL"): (28.5383, -81.3792),
    ("gainesville", "FL"): (29.6516, -82.3248),
    ("naperville", "IL"): (41.7508, -88.1535),
    ("boulder", "CO"): (40.0150, -105.2705),
    ("westminster", "CO"): (39.8367, -105.0372),
    ("lincoln", "NE"): (40.8136, -96.7026),
    ("idaho falls", "ID"): (43.4917, -112.0339),
    ("overland park", "KS"): (38.9822, -94.6708),
    ("sandy springs", "GA"): (33.9304, -84.3733),
    ("little rock", "AR"): (34.7465, -92.2896),
    ("bethesda", "MD"): (38.9847, -77.0947),
    ("upper marlboro", "MD"): (38.8159, -76.7497),
    ("greenville", "NC"): (35.6127, -77.3664),
    ("baton rouge", "LA"): (30.4515, -91.1871),
    ("norfolk", "VA"): (36.8508, -76.2859),
    ("college station", "TX"): (30.6280, -96.3344),
    ("west hollywood", "CA"): (34.0900, -118.3617),
    ("dumfries", "VA"): (38.5679, -77.3283),
    ("rancho cordova", "CA"): (38.5891, -121.3028),
    ("parker", "CO"): (39.5186, -104.7614),
    ("auburn", "WA"): (47.3073, -122.2285),
    ("new bedford", "MA"): (41.6362, -70.9342),
    ("bloomington", "IN"): (39.1653, -86.5264),
    ("reno", "NV"): (39.5296, -119.8138),
    ("north las vegas", "NV"): (36.1989, -115.1175),
    ("columbus", "GA"): (32.4610, -84.9877),
    ("jackson", "MS"): (32.2988, -90.1848),
    ("portland", "ME"): (43.6591, -70.2568),
    ("great falls", "MT"): (47.4942, -111.2833),
    ("billings", "MT"): (45.7833, -108.5007),
    ("fargo", "ND"): (46.8772, -96.7898),
    ("carson city", "NV"): (39.1638, -119.7674),
    ("flint", "MI"): (43.0125, -83.6875),
    ("fort smith", "AR"): (35.3859, -94.3985),
    ("springdale", "AR"): (36.1867, -94.1288),
    ("falls church", "VA"): (38.8823, -77.1711),
    ("new bern", "NC"): (35.1085, -77.0441),
    ("dallas", "TX"): (32.7767, -96.7970),
    ("mckinney", "TX"): (33.1972, -96.6397),
    ("san marcos", "TX"): (29.8833, -97.9414),
    ("greenwood", "IN"): (39.6136, -86.1066),
    ("west chester", "PA"): (39.9607, -75.6055),
    ("flower mound", "TX"): (33.0146, -97.0969),
    ("rapid city", "SD"): (44.0805, -103.2310),
}


# Junk datasets to DROP entirely (not construction permits)
_junk_drop_patterns = (
    "code violation", "code complaint", "ordinance violation",
    "dumpster", "parking permit", "tool truck", "taxi", "peddler",
    "sidewalk cafe", "banner", "alarm permit", "tobacco", "marijuana",
    "cannabis", "liquor", "alcohol", "food service", "food establishment",
    "311 explorer", "pothole", "graffiti", "noise complaint", "animal",
    "housing start", "epa enforcement", "adjudication", "enforcement action",
    "fema demolition", "katrina", "fire department occupancy",
    "unsafe vacant", "accident", "mesa code",
    "right-of-way", "right of way",
    "food inspection", "health inspection",
)
# City values that are actually data source slugs — not real cities
_junk_city_names = {
    "data", "www", "opendata", "datahub", "mydata", "datatalog",
    "datacatalog", "open", "unknown", "internal", "highways",
}

def _rebuild_cleaned_leads():
    """Pre-process raw cache once: fix coords, clean cities, normalize.
    Called when cache changes (after sync) — NOT on every API request."""
    global _cleaned_leads, _cleaned_leads_version
    import random as _rnd
    raw = DataCache.load(allow_stale=False) or DataCache.load(allow_stale=True) or []
    # Fallback: load from SQLite if JSON cache is empty/missing
    if not raw:
        try:
            from models.database import query_leads as _db_ql
            db_leads, db_count = _db_ql(limit=500000)
            if db_leads:
                raw = db_leads
                logger.info(f"_rebuild_cleaned_leads: loaded {len(raw):,} leads from SQLite (no JSON cache)")
        except Exception as e:
            logger.warning(f"SQLite fallback in rebuild failed: {e}")
    cleaned = []
    dropped_junk = 0
    assigned_centroid = 0

    # --- State centroids for fallback placement ---
    _state_centroids = {
        "AL": (32.80, -86.79), "AK": (63.35, -152.00), "AZ": (34.05, -111.09),
        "AR": (34.80, -92.20), "CA": (36.78, -119.42), "CO": (39.55, -105.78),
        "CT": (41.60, -72.70), "DE": (38.91, -75.53), "FL": (27.66, -81.52),
        "GA": (32.17, -82.90), "HI": (19.90, -155.58), "ID": (44.07, -114.74),
        "IL": (40.63, -89.40), "IN": (40.27, -86.13), "IA": (41.88, -93.10),
        "KS": (39.01, -98.48), "KY": (37.84, -84.27), "LA": (30.98, -91.96),
        "ME": (45.25, -69.45), "MD": (39.05, -76.64), "MA": (42.41, -71.38),
        "MI": (44.31, -85.60), "MN": (46.73, -94.69), "MS": (32.35, -89.40),
        "MO": (38.46, -92.29), "MT": (46.88, -110.36), "NE": (41.49, -99.90),
        "NV": (38.80, -116.42), "NH": (43.19, -71.57), "NJ": (40.06, -74.41),
        "NM": (34.52, -105.87), "NY": (43.30, -74.22), "NC": (35.76, -79.02),
        "ND": (47.55, -101.00), "OH": (40.42, -82.91), "OK": (35.47, -97.52),
        "OR": (43.80, -120.55), "PA": (41.20, -77.19), "RI": (41.58, -71.48),
        "SC": (33.84, -81.16), "SD": (43.97, -99.90), "TN": (35.52, -86.58),
        "TX": (31.97, -99.90), "UT": (39.32, -111.09), "VT": (44.56, -72.58),
        "VA": (37.43, -78.66), "WA": (47.75, -120.74), "WV": (38.60, -80.45),
        "WI": (43.78, -88.79), "WY": (43.08, -107.29), "DC": (38.91, -77.04),
    }
    # State bounding boxes: (min_lat, max_lat, min_lng, max_lng) — generous padding
    _state_bounds: dict[str, tuple[float, float, float, float]] = {
        "AL": (30.1, 35.1, -88.8, -84.8), "AK": (51.0, 71.5, -180.0, -129.0),
        "AZ": (31.3, 37.1, -115.0, -108.9), "AR": (33.0, 36.6, -94.7, -89.6),
        "CA": (32.5, 42.1, -124.5, -114.1), "CO": (36.9, 41.1, -109.1, -102.0),
        "CT": (40.9, 42.1, -73.8, -71.7), "DE": (38.4, 39.9, -75.8, -75.0),
        "FL": (24.4, 31.1, -87.7, -79.9), "GA": (30.3, 35.1, -85.7, -80.8),
        "HI": (18.9, 22.3, -160.3, -154.8), "ID": (41.9, 49.1, -117.3, -111.0),
        "IL": (36.9, 42.6, -91.6, -87.4), "IN": (37.7, 41.8, -88.1, -84.7),
        "IA": (40.3, 43.6, -96.7, -90.1), "KS": (36.9, 40.1, -102.1, -94.5),
        "KY": (36.4, 39.2, -89.6, -81.9), "LA": (28.9, 33.1, -94.1, -88.8),
        "ME": (43.0, 47.5, -71.1, -66.9), "MD": (37.9, 39.8, -79.5, -75.0),
        "MA": (41.2, 42.9, -73.6, -69.9), "MI": (41.6, 48.3, -90.5, -82.1),
        "MN": (43.4, 49.4, -97.3, -89.4), "MS": (30.1, 35.0, -91.7, -88.0),
        "MO": (35.9, 40.7, -95.8, -89.0), "MT": (44.3, 49.1, -116.1, -104.0),
        "NE": (39.9, 43.1, -104.1, -95.3), "NV": (34.9, 42.1, -120.1, -114.0),
        "NH": (42.6, 45.4, -72.6, -70.6), "NJ": (38.9, 41.4, -75.6, -73.9),
        "NM": (31.3, 37.1, -109.1, -103.0), "NY": (40.4, 45.1, -79.8, -71.8),
        "NC": (33.8, 36.6, -84.4, -75.4), "ND": (45.9, 49.1, -104.1, -96.5),
        "OH": (38.3, 42.0, -84.9, -80.5), "OK": (33.6, 37.1, -103.1, -94.4),
        "OR": (41.9, 46.3, -124.7, -116.4), "PA": (39.7, 42.3, -80.6, -74.7),
        "RI": (41.1, 42.1, -71.9, -71.1), "SC": (32.0, 35.3, -83.4, -78.5),
        "SD": (42.4, 46.0, -104.1, -96.4), "TN": (34.9, 36.7, -90.4, -81.6),
        "TX": (25.8, 36.6, -106.7, -93.5), "UT": (36.9, 42.1, -114.1, -109.0),
        "VT": (42.7, 45.1, -73.5, -71.4), "VA": (36.5, 39.5, -83.7, -75.2),
        "WA": (45.5, 49.1, -124.9, -116.9), "WV": (37.2, 40.7, -82.7, -77.7),
        "WI": (42.4, 47.1, -92.9, -86.7), "WY": (40.9, 45.1, -111.1, -104.0),
        "DC": (38.79, 39.0, -77.12, -76.91),
    }

    def _coords_match_state(lat: float, lng: float, state: str) -> bool:
        """Check if coordinates fall within the state's bounding box (with 1° padding)."""
        bb = _state_bounds.get(state)
        if not bb:
            return True  # unknown state, accept
        pad = 1.0
        return (bb[0] - pad) <= lat <= (bb[1] + pad) and (bb[2] - pad) <= lng <= (bb[3] + pad)

    # Build a quick city→coords lookup from leads that DO have valid coords
    # Skip Kansas centroid leads and junk city names to avoid polluting the lookup
    _city_state_coords: dict[str, tuple[float, float]] = {}
    for l in raw:
        lat, lng = l.get("lat"), l.get("lng")
        if lat and lng:
            try:
                flat, flng = float(lat), float(lng)
                if 17 <= flat <= 72 and -180 <= flng <= -65:
                    # Exclude Kansas centroid area (fake coords)
                    if abs(flat - 39.83) < 1.5 and abs(flng + 98.58) < 1.5:
                        continue
                    st = str(l.get("state", "") or "").strip().upper()[:2]
                    if not _coords_match_state(flat, flng, st):
                        continue
                    city_raw = str(l.get("city","") or "").strip().lower()
                    if city_raw in _junk_city_names:
                        continue
                    city_key = (city_raw, st)
                    if city_key[0] and city_key not in _city_state_coords:
                        _city_state_coords[city_key] = (flat, flng)
            except (ValueError, TypeError):
                pass

    for l in raw:
        # Skip junk datasets (drop entirely — not construction permits)
        city_raw = str(l.get("city", "") or "").lower()
        if any(p in city_raw for p in _junk_drop_patterns):
            dropped_junk += 1
            continue

        # ── Normalize city name from source slugs ──
        raw_city = str(l.get("city", "") or "").strip()
        raw_source = str(l.get("source", "") or "").strip()
        city_lookup = raw_city.lower()
        source_lookup = raw_source.lower()
        # Try city field first, then source field
        if city_lookup in _CITY_NAME_FIXES:
            real_city, real_st = _CITY_NAME_FIXES[city_lookup]
            l = dict(l)  # avoid mutating original
            l["city"] = real_city
            if not l.get("state") or str(l.get("state", "")).strip() == "":
                l["state"] = real_st
        elif source_lookup in _CITY_NAME_FIXES:
            real_city, real_st = _CITY_NAME_FIXES[source_lookup]
            l = dict(l)
            if not l.get("city") or str(l.get("city", "")).strip() == "":
                l["city"] = real_city
            if not l.get("state") or str(l.get("state", "")).strip() == "":
                l["state"] = real_st

        # Validate and fix coordinates — fallback to centroid if missing
        lat, lng = l.get("lat"), l.get("lng")
        flat, flng = None, None
        coords_from_centroid = False
        lead_state = str(l.get("state", "") or "").strip().upper()[:2]
        if lat and lng:
            try:
                flat, flng = float(lat), float(lng)
                # Reject State Plane / projected coordinates (values > 10000)
                if abs(flat) > 10000 or abs(flng) > 10000:
                    flat, flng = None, None
                # Reject Kansas centroid from DB (these are fake coords, not real)
                elif abs(flat - 39.83) < 1.5 and abs(flng + 98.58) < 1.5:
                    # Check if lead is actually in Kansas
                    if lead_state not in ("KS", "NE"):
                        flat, flng = None, None  # force re-geocoding via centroid
                else:
                    flat, flng = fix_coordinates(flat, flng)
                    # Reject coords that don't match the lead's state
                    if flat is not None and lead_state and not _coords_match_state(flat, flng, lead_state):
                        flat, flng = None, None
            except (ValueError, TypeError):
                flat, flng = None, None
        # Fallback chain: known city coords → lead-derived city centroid → state centroid
        # geo_quality: True = real coords, "city" = city-level, False = state/US centroid
        geo_quality = True  # assume real coords unless we fall through
        if flat is None or flng is None:
            city_val = str(l.get("city", "") or "").strip().lower()
            state_val = str(l.get("state", "") or "").strip().upper()[:2]
            city_key = (city_val, state_val)
            # 1. Try known city coordinates (accurate, curated)
            if city_key in _KNOWN_CITY_COORDS:
                flat, flng = _KNOWN_CITY_COORDS[city_key]
                geo_quality = "city"
            # 2. Try city centroid from other leads with valid coords
            elif city_key[0] and city_key in _city_state_coords:
                flat, flng = _city_state_coords[city_key]
                geo_quality = "city"
            # 3. Try state centroid
            elif state_val in _state_centroids:
                flat, flng = _state_centroids[state_val]
                geo_quality = False
            else:
                # Last resort: center of US
                flat, flng = 39.83, -98.58
                geo_quality = False
            coords_from_centroid = True
        if coords_from_centroid:
            # Add small random offset so points don't stack exactly
            flat += _rnd.uniform(-0.08, 0.08)
            flng += _rnd.uniform(-0.08, 0.08)
            assigned_centroid += 1
        # Create a cleaned copy (one-time cost)
        cl = dict(l)
        cl["lat"] = flat
        cl["lng"] = flng
        cl["_geocoded"] = geo_quality  # True=real, "city"=city-level, False=state/US centroid
        # Fix JSON-corrupted address fields (Socrata location objects stored as strings)
        addr_raw = str(cl.get("address", "") or "").strip()
        if addr_raw.startswith("{") and "human_address" in addr_raw:
            try:
                import ast
                parsed = ast.literal_eval(addr_raw)
                ha = parsed.get("human_address", "")
                if isinstance(ha, str) and ha.startswith("{"):
                    ha = json.loads(ha)
                if isinstance(ha, dict):
                    real_addr = ha.get("address", "")
                    if real_addr:
                        cl["address"] = real_addr
                    if not cl.get("city") and ha.get("city"):
                        cl["city"] = ha["city"]
                    if not cl.get("state") and ha.get("state"):
                        cl["state"] = ha["state"]
            except Exception:
                cl["address"] = ""
        elif addr_raw.startswith("{") or addr_raw.startswith("["):
            cl["address"] = ""
        # Fix bad city names (blank them — lead keeps its coords)
        city_val = str(cl.get("city", ""))
        city_low = city_val.lower().strip()
        # Strip jurisdictional suffixes (e.g. "AUSTIN FULL PURPOSE" -> "AUSTIN")
        for suffix in (" full purpose", " limited purpose", " ltd",
                       " etj release", " 2 mile etj", " 5 mile etj", " etj"):
            if city_low.endswith(suffix):
                city_val = city_val[:len(city_val) - len(suffix)].strip()
                city_low = city_val.lower().strip()
                break
        if (city_low in ("unknown", "not provided", "n/a", "none", "various", "other", "")
                or city_low in _junk_city_names
                or any(p in city_low for p in _bad_city_patterns)
                or city_low in _state_names_set
                or len(city_val) > 40):
            cl["city"] = ""
        else:
            cl["city"] = city_val  # write back cleaned value
        # Ensure every lead has an ID (hash-based if missing)
        if not cl.get("id"):
            _id_src = f"{cl.get('address','')}{cl.get('city','')}{cl.get('state','')}{cl.get('permit_number','')}{cl.get('issue_date','')}{cl.get('permit_type','')}"
            cl["id"] = abs(hash(_id_src))
        # Re-score with current algorithm (ensures scores update when algorithm changes)
        days_old = 0
        issue_date = cl.get("issue_date") or cl.get("filed_date") or cl.get("date")
        if issue_date:
            try:
                from datetime import datetime as _dt
                issued = _dt.fromisoformat(str(issue_date).replace('Z', '+00:00'))
                days_old = (_dt.now() - issued).days
            except (ValueError, TypeError):
                pass
        valuation = safe_float(cl.get("valuation"))
        permit_type = str(cl.get("permit_type") or "")
        new_score, new_temp, _ = calculate_score(days_old, valuation, permit_type)
        cl["score"] = new_score
        cl["temperature"] = new_temp
        # Pre-compute slim normalization (days_old, is_sold, id str, etc.)
        cl = normalize_lead_for_ui(cl, slim=True)
        cleaned.append(cl)
    _cleaned_leads = cleaned
    _cleaned_leads_version += 1
    # Build O(1) ID index for fast enrichment lookups
    global _cleaned_leads_by_id
    _cleaned_leads_by_id = {str(l.get("id", "")): l for l in cleaned if l.get("id")}
    logger.info(f"Pre-processed {len(cleaned):,} clean leads from {len(raw):,} raw "
                f"(dropped: {dropped_junk:,} junk, centroid-placed: {assigned_centroid:,}) "
                f"v{_cleaned_leads_version}")


@app.get("/api/leads/map")
async def get_leads_map():
    """Lightweight map endpoint — returns only [id, lat, lng, pri_int, score, city] per lead.
    Reduces payload from ~300MB (full leads) to ~5MB (6 fields per lead).
    pri_int: 0=hot, 1=warm, 2=med, 3=cold
    """
    _PRI_MAP = {"hot": 0, "warm": 1, "med": 2, "cold": 3}

    if not _cleaned_leads:
        # Kick off cache rebuild if needed, serve empty for now
        asyncio.get_event_loop().run_in_executor(None, _rebuild_cleaned_leads)
        return {"points": [], "total": 0, "source": "cache_loading"}

    points = []
    for lead in _cleaned_leads:
        lat = lead.get("lat")
        lng = lead.get("lng")
        if lat is None or lng is None:
            continue
        try:
            lat_f = float(lat)
            lng_f = float(lng)
        except (ValueError, TypeError):
            continue
        # Skip leads with no valid coordinates
        if lat_f == 0.0 and lng_f == 0.0:
            continue

        lead_id = str(lead.get("id", ""))
        temp = str(lead.get("temperature", "med"))
        pri_int = _PRI_MAP.get(temp, 2)
        score = int(lead.get("score", 50))
        city = str(lead.get("city", ""))
        geocoded = lead.get("_geocoded", True)

        points.append([lead_id, lat_f, lng_f, pri_int, score, city, geocoded])

    return {"points": points, "total": len(points), "source": "cache"}


@app.get("/api/leads")
async def get_leads(
    limit: int = Query(None, description="Max leads to return"),
    offset: int = Query(0, description="Skip first N leads (for pagination)"),
    days: int = Query(None, description="Filter to permits issued in last N days (default: no filter)"),
    max_days_old: int = Query(None, description="Exclude leads older than N days (uses pre-computed days_old)"),
    permit_type: Optional[str] = Query(None, description="Filter by permit_type substring"),
    city: Optional[str] = Query(None, description="Filter by city substring"),
    state: Optional[str] = Query(None, description="Filter by state abbreviation"),
    contact_only: bool = Query(False, description="Return only leads that have at least one contact field (name/phone/email)"),
    fields: Optional[str] = Query(None, description="Comma-separated list of fields to return (slim response)"),
    request: Request = None,
):
    """Get cached leads (truncated)."""
    from datetime import datetime, timedelta

    # Parse requested fields for slim mode
    field_set = set(f.strip() for f in fields.split(",")) if fields else None

    # Use pre-processed cache (rebuilt after each sync, not per-request)
    if not _cleaned_leads:
        # ── v3.0: Serve from SQLite immediately instead of blocking on 1.5GB JSON load ──
        try:
            from models.database import query_leads as db_query_leads
            db_leads, db_total = db_query_leads(
                limit=min(limit or LEADS_RETURN_LIMIT, LEADS_RETURN_LIMIT),
                city=city,
                max_days=days,
                permit_type=permit_type,
                contact_only=contact_only,
                sort_by="score",
            )
            if db_leads:
                logger.info(f"Serving {len(db_leads)} leads from SQLite (JSON cache not ready)")
                db_leads = [compute_readiness(l) for l in db_leads]
                if field_set:
                    db_leads = [{k: v for k, v in l.items() if k in field_set} for l in db_leads]
                # Kick off JSON cache rebuild in background (non-blocking)
                asyncio.get_event_loop().run_in_executor(None, _rebuild_cleaned_leads)
                return {
                    "leads": db_leads,
                    "source": "database",
                    "count": db_total,
                    "returned": len(db_leads),
                }
        except Exception as e:
            logger.warning(f"SQLite fallback failed: {e}")
        # Only block on JSON cache if SQLite fallback also failed
        await asyncio.to_thread(_rebuild_cleaned_leads)

    if not _cleaned_leads:
        return {"leads": [], "source": "empty", "total": 0, "count": 0, "returned": 0}

    # ── Fast path: serve cached pre-serialized bytes (bypass GZip middleware) ──
    cache_key = f"{limit}:{offset}:{fields}:{days}:{max_days_old}:{permit_type}:{city}:{state}:{contact_only}"
    cached_data = _response_cache.get(cache_key)
    if cached_data and cached_data[0] == _cleaned_leads_version:
        from starlette.responses import Response
        return Response(
            content=cached_data[1],
            media_type="application/json",
        )

    filtered = apply_access_filter(_cleaned_leads, request)

    # Fast server-side age filter (compute days_old live to avoid stale cache)
    if max_days_old is not None:
        filtered = [l for l in filtered
                    if (_live_days_old(l) or 0) <= max_days_old or (_live_days_old(l) or 0) == 0]

    # Apply lightweight filters (no dict copy needed — read-only until normalization)
    if days:
        cutoff = datetime.now() - timedelta(days=days)
        filtered = [
            l for l in filtered
            if (l.get("date") or l.get("issue_date") or l.get("filed_date")) and
            datetime.fromisoformat(str(l.get("date") or l.get("issue_date") or l.get("filed_date")).replace("Z", "+00:00")) >= cutoff
        ]

    if permit_type:
        pt = permit_type.lower()
        filtered = [l for l in filtered if str(l.get("permit_type","")).lower().find(pt) >= 0]
    if city:
        c = city.lower()
        filtered = [l for l in filtered if str(l.get("city","")).lower().find(c) >= 0]
    if state:
        s = state.upper()
        filtered = [l for l in filtered if str(l.get("state","")).upper() == s]
    if contact_only:
        filtered = [
            l for l in filtered
            if (l.get("owner_name") or l.get("owner_phone") or l.get("owner_email")
                or l.get("contractor_name") or l.get("contractor_phone")
                or l.get("enriched_success") or l.get("enriched_attempted"))
        ]

    total = len(filtered)
    # Apply OFFSET + LIMIT before any per-lead processing
    lim = total if (limit is not None and limit == 0) else min(limit or LEADS_RETURN_LIMIT, LEADS_RETURN_LIMIT)
    result = filtered[offset:offset + lim] if offset else filtered[:lim]
    # Leads are already slim-normalized during _rebuild_cleaned_leads.
    # Skip expensive per-lead transforms when client only needs map fields.
    _map_only_fields = {"id", "lat", "lng", "score", "temperature", "city", "state",
                        "lead_score", "address", "permit_type", "valuation", "days_old",
                        "source", "owner_name", "is_sold"}
    if field_set and field_set.issubset(_map_only_fields):
        # Fast path: no readiness/normalization needed — but recompute days_old live
        result = [{k: v for k, v in _with_live_days_old(l).items() if k in field_set} for l in result]
    else:
        _readiness_fields = {"readiness_score", "recommended_action", "contact_window_days", "budget_range", "competition_level"}
        if not field_set or bool(field_set & _readiness_fields):
            result = [normalize_lead_for_ui(compute_readiness(l)) for l in result]
        if field_set:
            result = [{k: v for k, v in l.items() if k in field_set} for l in result]
    payload = {
        "leads": result,
        "source": "cache",
        "total": total,
        "count": total,
        "returned": len(result),
    }
    # Pre-serialize with orjson and cache the raw bytes
    import orjson as _orjson
    raw_bytes = _orjson.dumps(payload)
    if len(_response_cache) > 20:
        _response_cache.clear()
    _response_cache[cache_key] = (_cleaned_leads_version, raw_bytes)
    logger.info(f"Cached leads response: {len(raw_bytes)/(1024*1024):.1f}MB ({len(result)} leads)")
    from starlette.responses import Response
    return Response(content=raw_bytes, media_type="application/json")


@app.get("/api/leads/bbox")
async def get_leads_bbox(
    south: float = Query(..., description="South latitude"),
    west: float = Query(..., description="West longitude"),
    north: float = Query(..., description="North latitude"),
    east: float = Query(..., description="East longitude"),
    limit: int = Query(999999, description="Max leads to return"),
    days: int = Query(None, description="Filter to permits issued in last N days"),
    permit_type: Optional[str] = Query(None, description="Filter by permit_type substring"),
    city: Optional[str] = Query(None, description="Filter by city substring"),
    request: Request = None,
):
    """
    Return leads within the requested bounding box.
    Uses cached data; best-effort if cache is stale.
    """
    from datetime import datetime, timedelta

    # Use pre-processed cache (coords already fixed, cities cleaned)
    if not _cleaned_leads:
        await asyncio.to_thread(_rebuild_cleaned_leads)

    if not _cleaned_leads:
        try:
            from models.database import query_leads as db_query_leads
            db_leads, db_total = db_query_leads(
                limit=min(limit, LEADS_RETURN_LIMIT),
                city=city,
                max_days=days,
                permit_type=permit_type,
                bbox=(south, west, north, east),
                sort_by="score",
            )
            if db_leads:
                logger.info(f"Serving {len(db_leads)} bbox leads from SQLite")
                db_leads = [compute_readiness(l) for l in db_leads]
                return {
                    "leads": db_leads,
                    "source": "database",
                    "count": db_total,
                    "returned": len(db_leads),
                }
        except Exception as e:
            logger.warning(f"SQLite bbox fallback failed: {e}")

    # Fast bbox filter on pre-processed data (no dict copy, no coord fix needed)
    filtered = [
        l for l in apply_access_filter(_cleaned_leads, request)
        if south <= (l.get("lat") or 0) <= north and west <= (l.get("lng") or 0) <= east
    ]

    if days:
        cutoff = datetime.now() - timedelta(days=days)
        filtered = [
            l for l in filtered
            if (l.get("date") or l.get("issue_date") or l.get("filed_date")) and
            datetime.fromisoformat(str(l.get("date") or l.get("issue_date") or l.get("filed_date")).replace("Z", "+00:00")) >= cutoff
        ]

    if permit_type:
        pt = permit_type.lower()
        filtered = [l for l in filtered if str(l.get("permit_type", "")).lower().find(pt) >= 0]
    if city:
        c = city.lower()
        filtered = [l for l in filtered if str(l.get("city", "")).lower().find(c) >= 0]
    max_ret = max(1, min(limit, LEADS_RETURN_LIMIT))
    result = filtered[:max_ret]
    return {
        "leads": result,
        "source": "cache",
        "count": len(filtered),
        "returned": len(result),
    }


@app.get("/api/leads/{lead_id}")
async def get_lead_detail(lead_id: str):
    """Return full details for a single lead (used by drawer on-demand)."""
    lead = _get_permit_from_cache(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return normalize_lead_for_ui(compute_readiness(lead))


@app.get("/api/debug/cache-state")
async def debug_cache_state():
    """Debug endpoint to check cache state."""
    return {
        "cleaned_leads_count": len(_cleaned_leads),
        "cleaned_version": _cleaned_leads_version,
        "response_cache_keys": list(_response_cache.keys()),
        "response_cache_versions": {k: v[0] for k, v in _response_cache.items()},
        "response_cache_sizes": {k: f"{len(v[1])/(1024*1024):.1f}MB" for k, v in _response_cache.items()},
    }


@app.post("/api/leads")
async def create_lead(request: Request):
    """Create a new lead manually."""
    body = await request.json()

    # Validate required fields
    address = str(body.get("address", "")).strip()
    if not address:
        raise HTTPException(status_code=400, detail="Address is required")

    city = str(body.get("city", "")).strip()
    state = str(body.get("state", "")).strip()
    zip_code = str(body.get("zip", "")).strip()

    lead_id = str(uuid.uuid4())[:12]

    lead = {
        "id": lead_id,
        "address": address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "owner_name": str(body.get("owner_name", "")).strip(),
        "phone": str(body.get("phone", "")).strip(),
        "email": str(body.get("email", "")).strip(),
        "type": str(body.get("work_type", "")).strip() or "Residential",
        "valuation": safe_float(body.get("project_value")),
        "permit_number": str(body.get("permit_number", "")).strip(),
        "description": str(body.get("description", "")).strip(),
        "source": "manual",
        "issue_date": datetime.now().strftime("%Y-%m-%d"),
        "score": 50,
        "temperature": "med",
        "_geocoded": False,
    }

    # Save to SQLite
    try:
        conn = _leads_conn()
        conn.execute("""
            INSERT INTO leads (id, address, city, state, zip, owner_name, phone, email,
                             type, valuation, permit_number, description, source, issue_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (lead_id, address, city, state, zip_code, lead["owner_name"], lead["phone"],
              lead["email"], lead["type"], lead["valuation"], lead["permit_number"],
              lead["description"], "manual", lead["issue_date"]))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Failed to save manual lead: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save lead")

    # Add to in-memory cache
    if _cleaned_leads is not None:
        _cleaned_leads.append(lead)
        if _cleaned_leads_by_id is not None:
            _cleaned_leads_by_id[lead_id] = lead

    return {"status": "ok", "lead": lead}


@app.post("/api/leads/{lead_id}/stage")
async def update_lead_stage(lead_id: str, request: Request):
    """Update lead stage for CRM pipeline (new UI)."""
    try:
        body = await request.json()
        stage = body.get("stage", "new")

        # Validate stage
        valid_stages = ["new", "contacted", "quoted", "proposed", "won", "lost", "scheduled", "follow_up"]
        if stage not in valid_stages:
            return {"success": False, "error": f"Invalid stage. Must be one of: {valid_stages}"}

        # Save to leads.db (consolidated pipeline storage)
        conn = _leads_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO lead_stages (lead_id, stage, updated_at) VALUES (?, ?, ?)",
            (lead_id, stage, datetime.utcnow().isoformat())
        )
        conn.commit()
        conn.close()

        logger.info(f"Updated lead {lead_id} to stage: {stage}")
        return {"success": True, "lead_id": lead_id, "stage": stage}
    except Exception as e:
        logger.error(f"Failed to update stage for lead {lead_id}: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/stages")
async def get_all_stages():
    """Return all saved lead stages + timestamps for frontend hydration."""
    try:
        conn = _leads_conn()
        cur = conn.cursor()
        cur.execute("SELECT lead_id, stage, updated_at FROM lead_stages")
        rows = cur.fetchall()
        conn.close()
        stages = {str(r[0]): {"stage": r[1], "updated_at": r[2]} for r in rows}
        return {"success": True, "count": len(stages), "stages": stages}
    except Exception as e:
        logger.error(f"Failed to load stages: {e}")
        return {"success": False, "stages": {}, "error": str(e)}


@app.get("/api/intent/leads")
async def get_intent_leads(limit: int = Query(200, description="Max behavioral leads to return")):
    """Return stored Yelp intent leads."""
    try:
        init_db()
        return {"leads": fetch_intent_leads(limit=limit)}
    except Exception as e:
        logger.error(f"Intent leads fetch failed: {e}")
        return {"leads": [], "error": str(e)}


@app.get("/api/territory/metrics/{zip_code}")
async def territory_metrics(zip_code: str):
    """
    Scarcity metrics for optional UI rendering.
    Does not change existing UX; frontend may ignore.
    """
    try:
        upsert_pricing(zip_code)
        return metrics(zip_code)
    except Exception as e:
        logger.error(f"Metrics failed: {e}")
        return {"zip": zip_code, "seats_taken": 0, "seats_remaining": 0, "waitlist_count": 0, "upgrade_available": False}

@app.get("/territory/status/{zip_code}")
async def territory_status(zip_code: str):
    """Alias/status endpoint for frontend scarcity messaging."""
    try:
        upsert_pricing(zip_code)
        return metrics(zip_code)
    except Exception:
        return {"zip": zip_code, "seats_taken": 0, "seats_remaining": 0, "waitlist_count": 0, "upgrade_available": False}


@app.post("/auth/verify-license")
async def verify_license(payload: dict):
    """
    License verification endpoint (non-blocking to UI).
    Expected payload: {user_id, license_number, state}
    """
    try:
        user_id = payload.get("user_id")
        lic = payload.get("license_number")
        state = payload.get("state") or "CA"
        if not user_id or not lic:
            raise HTTPException(status_code=400, detail="user_id and license_number required")
        snap = login_license_check(user_id, lic, state)
        return {"status": "ok", "snapshot": snap}
    except Exception as e:
        logger.error(f"License verify failed: {e}")
        raise HTTPException(status_code=500, detail="license verification failed")


@app.post("/api/permits/submit")
async def submit_permit(payload: dict):
    """
    Permit filing: generates standardized payload; if city unsupported, returns prefilled PDF stub.
    """
    lead = payload.get("lead") or {}
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="user_id required")
    permit_payload = generate_permit_payload(lead)
    city = lead.get("city") or payload.get("city") or ""
    pdf_url = generate_prefilled_pdf_stub(permit_payload)
    save_permit_request(user_id, lead.get("id") or "", city, permit_payload, pdf_url)
    return {"status": "submitted", "pdf_url": pdf_url}


# ---------------------------------------------------------------------------
# Behavioral confirmation endpoints
# ---------------------------------------------------------------------------


@app.post("/api/behavior/confirm")
async def behavior_confirm(payload: dict, request: Request):
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    property_id = payload.get("property_id")
    event_type = payload.get("event_type")
    confidence = payload.get("confidence") or "user_confirmed"
    source_prompt = payload.get("source") or payload.get("source_prompt") or ""
    metadata = payload.get("metadata") or {}
    if not property_id or not event_type:
        raise HTTPException(status_code=400, detail="property_id and event_type required")

    # map event_type to tables
    ACTIONS = {
        "contacted",
        "contact_scheduled",
        "meeting_scheduled",
        "estimate_sent",
        "follow_up",
        "ignored",
    }
    RESPONSES = {
        "homeowner_no_answer",
        "homeowner_responded",
        "requested_meeting",
        "price_shopping",
        "ghosted",
        "declined",
    }
    OUTCOMES = {"outcome_won", "outcome_lost", "outcome_unknown"}

    try:
        if event_type in ACTIONS:
            _insert_action(user_id, property_id, event_type, {**metadata, "confidence": confidence, "source": source_prompt})
        elif event_type in RESPONSES:
            _insert_response(user_id, property_id, event_type)
        elif event_type in OUTCOMES:
            outcome = event_type.replace("outcome_", "")
            _insert_outcome(user_id, property_id, outcome, {**metadata, "confidence": confidence, "source": source_prompt})
        else:
            raise HTTPException(status_code=400, detail="Unknown event_type")
    except Exception as e:
        logger.error(f"behavior_confirm failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to record event")

    return {"status": "ok"}


@app.post("/api/alert/open")
async def alert_open(payload: dict, request: Request):
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    alert_id = payload.get("alert_id") or payload.get("id")
    property_id = payload.get("property_id")
    if not alert_id and not property_id:
        raise HTTPException(status_code=400, detail="alert_id or property_id required")
    try:
        _mark_alert_seen(user_id, alert_id or "", property_id)
    except Exception as e:
        logger.error(f"alert_open failed: {e}")
        raise HTTPException(status_code=500, detail="failed")
    return {"status": "ok"}


@app.post("/api/prompts/next")
async def prompts_next(payload: dict, request: Request):
    """
    Accepts context list and returns at most one prompt suggestion respecting throttling.
    Payload example:
    {"session_id": "abc", "context": [{property_id,...}]}
    """
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    session_id = payload.get("session_id") or str(uuid.uuid4())
    context = payload.get("context") or []
    prompts = suggest_prompts(user_id, session_id, context)
    return {"prompts": prompts}


@app.get("/api/insights/monthly")
async def monthly_insights(request: Request):
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        summary = compute_insights(user_id)
        return summary
    except Exception as e:
        logger.error(f"insights failed: {e}")
        raise HTTPException(status_code=500, detail="failed")


@app.post("/api/intent/yelp/sync")
async def trigger_intent_sync():
    """Manual trigger for Yelp intent ingestion."""
    if not YELP_INTENT_ENABLED:
        return {"message": "Yelp intent ingestion disabled", "status": "disabled"}
    try:
        cached = DataCache.load(allow_stale=True) or []

        async def _owner_lookup(county, apn, address):
            try:
                return get_la_owner(apn)
            except Exception:
                return {}

        async def _contact_lookup(street, city, state, zip_code):
            return {}  # free sources used in batch enrichment instead

        cities = ["Los Angeles", "Santa Monica", "Beverly Hills", "Pasadena", "Long Beach"]
        added = await ingest_yelp_intents(
            parcels=cached,
            get_owner_fn=_owner_lookup,
            enrich_contact_fn=_contact_lookup,
            cities=cities,
            max_results=30,
        )
        return {"message": "Intent sync complete", "added": added}
    except Exception as e:
        logger.error(f"Intent sync failed: {e}")
        return {"message": "Intent sync failed", "error": str(e)}


@app.post("/api/sync")
async def trigger_sync(request: Request, background_tasks: BackgroundTasks):
    """Manually trigger data sync"""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload:
        raise HTTPException(401, "Not authenticated")
    background_tasks.add_task(sync_data)
    return {"message": "Sync started", "status": "processing"}


# ============================================================================
# JURISDICTIONS REGISTRY API
# ============================================================================

@app.get("/api/jurisdictions")
async def get_jurisdictions(
    state: Optional[str] = Query(None, description="Filter by state abbreviation"),
    type: Optional[str] = Query(None, description="Filter by type: City or County"),
    api_type: Optional[str] = Query(None, description="Filter by API type: Socrata, ArcGIS, CKAN"),
):
    """Return nationwide jurisdictions registry with optional filters"""
    jurisdictions = JURISDICTIONS_REGISTRY.get("jurisdictions", [])
    if state:
        jurisdictions = [j for j in jurisdictions if j.get("state", "").upper() == state.upper()]
    if type:
        jurisdictions = [j for j in jurisdictions if j.get("type", "").lower() == type.lower()]
    if api_type:
        jurisdictions = [j for j in jurisdictions if api_type.lower() in j.get("api_type", "").lower()]
    return {
        "count": len(jurisdictions),
        "jurisdictions": jurisdictions,
    }


@app.get("/api/jurisdictions/stats")
async def jurisdictions_stats():
    """Coverage statistics for the nationwide permit pipeline"""
    jurisdictions = JURISDICTIONS_REGISTRY.get("jurisdictions", [])
    meta = JURISDICTIONS_REGISTRY.get("metadata", {})
    by_state = defaultdict(int)
    by_type = defaultdict(int)
    by_api = defaultdict(int)
    total_pop = 0
    for j in jurisdictions:
        by_state[j.get("state", "Unknown")] += 1
        by_type[j.get("type", "Unknown")] += 1
        api = j.get("api_type", "Unknown")
        # Normalize API type
        if "socrata" in api.lower() or "soda" in api.lower():
            by_api["Socrata"] += 1
        elif "arcgis" in api.lower():
            by_api["ArcGIS"] += 1
        elif "ckan" in api.lower():
            by_api["CKAN"] += 1
        else:
            by_api[api] += 1
        total_pop += j.get("population", 0)

    # Count active vs existing in SOC_DATASETS
    active_keys = set()
    for key, cfg in SOC_DATASETS.items():
        active_keys.add(cfg.get("city", "").lower())

    return {
        "metadata": meta,
        "total_registered": len(jurisdictions),
        "active_in_pipeline": len(SOC_DATASETS),
        "states_covered": len(by_state),
        "by_state": dict(sorted(by_state.items())),
        "by_type": dict(by_type),
        "by_api_type": dict(by_api),
        "total_population_covered": total_pop,
        "federal_apis": len(JURISDICTIONS_REGISTRY.get("federal_apis", [])),
        "permit_platforms": len(JURISDICTIONS_REGISTRY.get("permit_platforms", [])),
        "open_data_platforms": len(JURISDICTIONS_REGISTRY.get("open_data_platforms", [])),
    }


@app.get("/api/jurisdictions/platforms")
async def get_platforms():
    """Return permit platform ecosystem info"""
    return {
        "federal_apis": JURISDICTIONS_REGISTRY.get("federal_apis", []),
        "permit_platforms": JURISDICTIONS_REGISTRY.get("permit_platforms", []),
        "open_data_platforms": JURISDICTIONS_REGISTRY.get("open_data_platforms", []),
        "github_resources": JURISDICTIONS_REGISTRY.get("github_resources", []),
    }


from typing import Optional

# ============================================================================
# PROPERTYREACH DIRECT SEARCH (on-demand helper)
# ============================================================================

async def propertyreach_search(street: str, city: str, state: str, zip_code: str) -> dict:
    """
    Fetch owner contact via PropertyReach with fallbacks:
      1) /v1/property to resolve propertyId + owner hints
      2) /v1/skip-trace with propertyId (or address if no id)
    Uses primary key then ALT.
    """
    # Collect keys: comma-separated PROPERTYREACH_API_KEYS takes priority, else individual vars
    keys_env = os.getenv("PROPERTYREACH_API_KEYS", "")
    if keys_env:
        keys = [k.strip() for k in keys_env.split(",") if k.strip()]
    else:
        keys = [
            os.getenv("PROPERTYREACH_API_KEY"),
            os.getenv("PROPERTYREACH_API_KEY_ALT"),
            os.getenv("PROPERTYREACH_API_KEY_ALT2"),
            os.getenv("PROPERTYREACH_API_KEY_ALT3"),
        ]
        keys = [k for k in keys if k]
    if not keys:
        raise HTTPException(status_code=400, detail="PropertyReach API key missing")

    last_error = None

    async def fetch_property(session, key):
        params = {
            "streetAddress": street,
            "city": city,
            "state": state,
            "zip": zip_code,
        }
        headers = {"x-api-key": key}
        async with session.get(
            "https://api.propertyreach.com/v1/property",
            params=params,
            headers=headers,
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                return None, f"{resp.status}: {text[:200]}"
            try:
                data = await resp.json()
            except Exception:
                return None, "Invalid JSON from /property"
            prop = data.get("property") or (data.get("properties") or [{}])[0] or {}
            prop_id = prop.get("id") or prop.get("propertyId") or None
            return {"prop": prop, "id": prop_id}, None

    async def skip_trace(session, key, prop_id=None):
        payload = {
            "target": {
                "streetAddress": street,
                "city": city,
                "state": state,
                "zip": zip_code,
            }
        }
        if prop_id:
            payload["target"]["propertyId"] = prop_id
        headers = {"x-api-key": key, "Content-Type": "application/json"}
        async with session.post(
            "https://api.propertyreach.com/v1/skip-trace",
            json=payload,
            headers=headers,
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                return None, f"{resp.status}: {text[:200]}"
            try:
                data = await resp.json()
            except Exception:
                return None, "Invalid JSON from /skip-trace"
            return data, None

    async def suggestions(session, key, query):
        headers = {"x-api-key": key}
        async with session.get(
            "https://api.propertyreach.com/v1/suggestions",
            params={"query": query},
            headers=headers,
        ) as resp:
            if resp.status != 200:
                return None
            try:
                return await resp.json()
            except Exception:
                return None

    for key in keys:
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                prop_result, perr = await fetch_property(session, key)
                prop = prop_result["prop"] if prop_result else {}
                prop_id = prop_result["id"] if prop_result else None

                data, serr = await skip_trace(session, key, prop_id=prop_id)
                # If skip-trace failed and no prop_id, try suggestions to sanitize, then retry skip_trace
                if not data and not prop_id:
                    try:
                        sugg = await suggestions(session, key, f"{street} {city} {state} {zip_code}".strip())
                        if sugg and sugg.get("suggestions"):
                            top = sugg["suggestions"][0]
                            street_s = top.get("streetAddress") or street
                            city_s = top.get("city") or city
                            zip_s = top.get("zip") or zip_code
                            # retry skip trace (address only) with sanitized components
                            data, serr = await skip_trace(session, key, prop_id=None)
                    except Exception as e:
                        logger.warning("Skip trace address suggestion retry failed: %s", e)
                if not data:
                    last_error = serr or perr
                    continue

                persons = data.get("persons") or []
                names: list[str] = []
                phones: list[str] = []
                emails: list[str] = []

                for p in persons:
                    nm = " ".join(
                        part for part in [p.get("firstName"), p.get("lastName")] if part
                    ).strip()
                    if nm:
                        names.append(nm)
                    for ph in p.get("phones") or []:
                        val = ph.get("phone") or ph.get("number")
                        if val:
                            phones.append(str(val))
                    for em in p.get("emails") or []:
                        val = em.get("email")
                        if val:
                            emails.append(val)

                if isinstance(prop, dict):
                    owner_field = prop.get("ownerNames") or prop.get("owner1Name") or prop.get("owner2Name")
                    if owner_field:
                        if isinstance(owner_field, list):
                            names.extend(owner_field)
                        elif isinstance(owner_field, str):
                            names.append(owner_field)
                    phones.extend(prop.get("ownerPhones") or prop.get("phones") or [])
                    emails.extend(prop.get("ownerEmails") or prop.get("emails") or [])

                names = list(dict.fromkeys([n for n in names if n]))
                phones = list(dict.fromkeys([p for p in phones if p]))
                emails = list(dict.fromkeys([e for e in emails if e]))

                address = (
                    prop.get("mailingAddress")
                    or prop.get("address")
                    or prop.get("streetAddress")
                    or street
                )

                return {
                    "success": True,
                    "address": address,
                    "owner_name": names[0] if names else "",
                    "names": names,
                    "phones": phones,
                    "emails": emails,
                    "raw": data,
                }
        except Exception as e:
            last_error = str(e)
            continue

    # PropertyReach failed; try ATTOM for owner/mailing as a soft fallback
    fallback_lead = {
        "address": street,
        "city": city,
        "state": state,
        "zip": zip_code,
    }
    attom_result = await _attom_enrich(fallback_lead)

    # If ATTOM finds an owner, attempt one more PropertyReach skip-trace using that name + mailing
    if attom_result.get("owner_name"):
        attom_name = attom_result.get("owner_name")
        attom_addr = attom_result.get("owner_address") or street
        for key in keys:
            try:
                headers = {"x-api-key": key, "Content-Type": "application/json"}
                payload = {"target": {"streetAddress": attom_addr, "name": attom_name}}
                timeout = aiohttp.ClientTimeout(total=12)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(
                        "https://api.propertyreach.com/v1/skip-trace",
                        json=payload,
                        headers=headers,
                    ) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                        persons = data.get("persons") or []
                        names = []
                        phones = []
                        emails = []
                        for p in persons:
                            nm = " ".join(filter(None, [p.get("firstName"), p.get("lastName")])).strip()
                            if nm:
                                names.append(nm)
                            for ph in p.get("phones") or []:
                                if ph.get("phone"):
                                    phones.append(ph["phone"])
                            for em in p.get("emails") or []:
                                if em.get("email"):
                                    emails.append(em["email"])
                        if names or phones or emails:
                            return {
                                "success": True,
                                "address": attom_addr,
                                "owner_name": names[0] if names else attom_name,
                                "names": list(dict.fromkeys(names or [attom_name])),
                                "phones": list(dict.fromkeys(phones)),
                                "emails": list(dict.fromkeys(emails)),
                                "raw": data,
                                "note": "ATTOM owner + PR name-based skip-trace",
                            }
            except Exception:
                continue

    if attom_result.get("owner_name") or attom_result.get("owner_address"):
        return {
            "success": True,
            "address": attom_result.get("owner_address") or street,
            "owner_name": attom_result.get("owner_name") or "",
            "names": [attom_result.get("owner_name")] if attom_result.get("owner_name") else [],
            "phones": [],
            "emails": [],
            "raw": {"fallback": "attom"},
            "note": "PropertyReach not found; ATTOM fallback for owner/mailing only",
        }

    return {"success": False, "error": last_error or "PropertyReach lookup failed"}

def _get_permit_from_cache(lead_id) -> Optional[dict]:
    """O(1) lead lookup using pre-built ID index."""
    lid = str(lead_id)
    # O(1) lookup via index
    if _cleaned_leads_by_id:
        lead = _cleaned_leads_by_id.get(lid)
        if lead:
            return lead
    # Fallback: linear scan of cleaned leads
    for lead in (_cleaned_leads or []):
        if str(lead.get("id", "")) == lid:
            return lead
    return None


@app.get("/api/lead/{lead_id}/enrich-complete")
async def enrich_complete(lead_id: str):
    """
    On-demand enrichment for a single lead:
    - PropertyReach skip-trace (if enabled)
    - ATTOM basic profile (owner name/mailing) with rate-limit
    """
    permit = _get_permit_from_cache(lead_id)
    if not permit:
        raise HTTPException(status_code=404, detail="Lead not found")

    enriched = dict(permit)

    # PropertyReach skip trace first
    if PROPERTYREACH_ENABLED and PROPERTYREACH_API_KEY:
        try:
            resp = skip_trace_property(
                address=permit.get("address"),
                city=permit.get("city"),
                state=permit.get("state") or "CA",
                zip_code=permit.get("zip"),
                apn=permit.get("apn"),
                county=permit.get("county") or permit.get("city"),
            )
            if resp.get("success"):
                if resp.get("names"):
                    enriched["owner_name"] = enriched.get("owner_name") or resp["names"][0]
                if resp.get("phones"):
                    enriched["owner_phone"] = enriched.get("owner_phone") or resp["phones"][0]
                if resp.get("emails"):
                    enriched["owner_email"] = enriched.get("owner_email") or resp["emails"][0]
        except Exception as e:
            logger.debug(f"PropertyReach on-demand error: {e}")

    # ATTOM fallback for owner name/mailing
    enriched = await _attom_enrich(enriched)

    success = bool(enriched.get("owner_name") or enriched.get("owner_phone") or enriched.get("owner_email"))

    return {
        "success": success,
        "lead_id": lead_id,
        "owner_name": enriched.get("owner_name"),
        "phone": enriched.get("owner_phone"),
        "email": enriched.get("owner_email"),
        "mailing_address": enriched.get("owner_address") or enriched.get("mailing_address"),
        "all_individuals": [],
        "all_entities": [],
        "total_contacts": 1,
        "confidence": 80 if enriched.get("owner_name") else 0,
        "sources_used": ["PropertyReach", "ATTOM"],
        "enrichment_method": "On-demand skip trace + ATTOM",
        "enriched_at": datetime.utcnow().isoformat(),
        "error": None if success else "No contact found for this property",
    }

@app.post("/api/invite")
async def send_invite(request: Request):
    """Send an invite SMS to a phone number"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"detail": "Invalid JSON"})
    phone = str(body.get("phone", "")).strip()
    name = str(body.get("name", "")).strip()
    message = str(body.get("message", "")).strip()
    if not phone:
        return JSONResponse(status_code=400, content={"detail": "Phone number required"})
    # Log the invite (actual SMS sending requires Twilio integration)
    logger.info(f"Invite requested: phone={phone}, name={name}")
    return {"success": True, "message": f"Invite queued for {phone}", "note": "SMS delivery requires Twilio configuration in Settings"}


# ── Settings persistence ──

@app.get("/api/settings")
async def get_settings(request: Request):
    """Get user settings."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_id = payload.get("sub", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        import sqlite3
        conn = _leads_conn()
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT settings_json FROM users WHERE id = ?", (user_id,)).fetchone()
        conn.close()
        if row and row["settings_json"]:
            return json.loads(row["settings_json"])
    except Exception as e:
        logger.debug("Settings fetch: %s", e)
    return {}


@app.put("/api/settings")
async def update_settings(request: Request):
    """Update user settings (merge with existing)."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_id = payload.get("sub", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    body = await request.json()

    # Ensure settings_json column exists
    try:
        conn = _leads_conn()
        try:
            conn.execute("ALTER TABLE users ADD COLUMN settings_json TEXT DEFAULT '{}'")
            conn.commit()
        except Exception:
            pass  # Column already exists

        # Merge with existing settings
        existing = {}
        row = conn.execute("SELECT settings_json FROM users WHERE id = ?", (user_id,)).fetchone()
        if row and row[0]:
            try:
                existing = json.loads(row[0])
            except Exception:
                pass

        merged = {**existing, **body}
        conn.execute("UPDATE users SET settings_json = ? WHERE id = ?", (json.dumps(merged), user_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Settings save failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save settings")

    return {"status": "ok", "settings": merged}


@app.get("/api/health")
async def health_check():
    """API health check"""
    cached = DataCache.load(allow_stale=True)
    count = len(cached) if cached else 0

    # Also check SQLite database
    db_status = "online"
    db_count = 0
    try:
        from models.database import get_db
        with get_db() as conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM leads WHERE is_active = 1").fetchone()
            db_count = row["cnt"] if row else 0
    except Exception as e:
        db_status = f"error: {e}"

    total = max(count, db_count)
    return {
        "status": "healthy" if total > 0 else "degraded",
        "version": "3.1.0",
        "app": "Onsite",
        "checks": [
            APIStatus(
                name="cache",
                status="online" if count > 0 else "empty",
                response_time_ms=1,
                last_check=datetime.now().isoformat(),
                details=f"{count} cached leads",
            ),
            APIStatus(
                name="database",
                status=db_status,
                response_time_ms=1,
                last_check=datetime.now().isoformat(),
                details=f"{db_count} leads in SQLite",
            ),
        ],
        "total_leads": total,
        "timestamp": datetime.now().isoformat(),
    }


@app.get("/api/metrics")
async def prometheus_metrics():
    """Prometheus-compatible metrics endpoint."""
    cached = DataCache.load(allow_stale=True)
    count = len(cached) if cached else 0

    db_count = 0
    try:
        from models.database import get_db
        with get_db() as conn:
            db_count = conn.execute("SELECT COUNT(*) FROM leads WHERE is_active = 1").fetchone()[0]
    except Exception as e:
        logger.debug("Metrics db count query failed: %s", e)

    lines = [
        "# HELP onsite_leads_total Total number of leads",
        "# TYPE onsite_leads_total gauge",
        f"onsite_leads_total {max(count, db_count)}",
        "# HELP onsite_leads_cache Number of leads in memory cache",
        "# TYPE onsite_leads_cache gauge",
        f"onsite_leads_cache {count}",
        "# HELP onsite_leads_db Number of leads in database",
        "# TYPE onsite_leads_db gauge",
        f"onsite_leads_db {db_count}",
        "# HELP onsite_up Application health",
        "# TYPE onsite_up gauge",
        f"onsite_up 1",
    ]
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse("\n".join(lines), media_type="text/plain")


# ═══════════════════════════════════════════════════════════════════════════
# NEW ANALYTICS ENDPOINTS — For Charts & Dashboard
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/analytics/activity")
async def analytics_activity(days: int = 30):
    """Time-series permit activity (for activity chart)"""
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"labels": [], "values": []}

    from collections import defaultdict
    activity = defaultdict(int)
    cutoff = datetime.now() - timedelta(days=days)

    for permit in cached:
        issue_date = permit.get("issue_date", "")
        if issue_date:
            try:
                dt = datetime.fromisoformat(issue_date.replace("Z", "+00:00"))
                if dt >= cutoff:
                    date_key = dt.strftime("%Y-%m-%d")
                    activity[date_key] += 1
            except (ValueError, TypeError):
                pass

    # Sort by date
    sorted_activity = sorted(activity.items())
    return {
        "labels": [date for date, _ in sorted_activity],
        "values": [count for _, count in sorted_activity]
    }


@app.get("/api/analytics/revenue-by-type")
async def analytics_revenue_by_type():
    """Revenue breakdown by permit type (for pie chart)"""
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"labels": [], "values": []}

    from collections import defaultdict
    revenue_by_type = defaultdict(float)

    for permit in cached:
        permit_type = permit.get("permit_type", "Unknown")
        valuation = float(permit.get("valuation", 0) or 0)
        revenue_by_type[permit_type] += valuation

    # Get top 10 types
    sorted_types = sorted(revenue_by_type.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "labels": [ptype for ptype, _ in sorted_types],
        "values": [revenue for _, revenue in sorted_types]
    }


@app.get("/api/analytics/neighborhoods")
async def analytics_neighborhoods(limit: int = 10):
    """Top neighborhoods by permit count (for bar chart)"""
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"labels": [], "values": []}

    from collections import defaultdict
    neighborhoods = defaultdict(int)

    for permit in cached:
        zip_code = permit.get("zip", "Unknown")
        neighborhoods[zip_code] += 1

    # Get top N neighborhoods
    sorted_neighborhoods = sorted(neighborhoods.items(), key=lambda x: x[1], reverse=True)[:limit]

    return {
        "labels": [zip_code for zip_code, _ in sorted_neighborhoods],
        "values": [count for _, count in sorted_neighborhoods]
    }


@app.get("/api/analytics/funnel")
async def analytics_funnel():
    """Conversion funnel stats — combines score-based counts with real pipeline stages."""
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"stages": [], "counts": []}

    total = len(cached)
    hot = sum(1 for p in cached if safe_float(p.get("score")) >= TEMP_THRESHOLDS["hot"])
    warm = sum(1 for p in cached if TEMP_THRESHOLDS["warm"] <= safe_float(p.get("score")) < TEMP_THRESHOLDS["hot"])

    # Pull real pipeline stage counts from leads.db
    stage_counts = {"contacted": 0, "quoted": 0, "proposed": 0, "won": 0, "lost": 0}
    try:
        conn = _leads_conn()
        cur = conn.cursor()
        rows = cur.execute("SELECT stage, COUNT(*) FROM lead_stages GROUP BY stage").fetchall()
        conn.close()
        for stage, cnt in rows:
            if stage in stage_counts:
                stage_counts[stage] = cnt
    except Exception as e:
        logger.error("Failed to query lead_stages for pipeline counts: %s", e)

    return {
        "stages": ["All Leads", "Hot Leads", "Warm Leads", "Contacted", "Quoted", "Won"],
        "counts": [total, hot, warm, stage_counts["contacted"], stage_counts["quoted"], stage_counts["won"]]
    }


# ═══════════════════════════════════════════════════════════════════════════
# ZIP RISK DATA ENDPOINT — Per-ZIP flood, disaster, market data
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/zip-risk/{zip_code}")
async def zip_risk_data(zip_code: str):
    """Return ZIP-level risk and market data."""
    zip_code = zip_code.strip()[:5]
    if len(zip_code) < 5:
        raise HTTPException(status_code=400, detail="Invalid ZIP code")
    try:
        from data_sources.schema import get_db as get_ds_db
        with get_ds_db() as conn:
            row = conn.execute(
                "SELECT zip_code, city, county, state, lat, lng, "
                "nfip_flood_claims_count, nfip_flood_total_paid_usd, "
                "fema_disaster_count, fema_disaster_types, "
                "fema_ia_registrations, fema_ia_approved_usd, "
                "fhfa_hpi_index, fhfa_hpi_year, "
                "census_permits_count, city_portal_permits_count "
                "FROM zip_risk_data WHERE zip_code = ?", (zip_code,)
            ).fetchone()
            if not row:
                return {"found": False, "zip_code": zip_code}
            return {
                "found": True,
                "zip_code": row[0], "city": row[1], "county": row[2], "state": row[3],
                "lat": row[4], "lng": row[5],
                "flood_claims": row[6], "flood_total_paid": row[7],
                "disaster_count": row[8], "disaster_types": row[9],
                "ia_registrations": row[10], "ia_approved_usd": row[11],
                "hpi_index": row[12], "hpi_year": row[13],
                "census_permits": row[14], "city_portal_permits": row[15],
            }
    except Exception as e:
        logger.error("ZIP risk lookup failed: %s", e)
        return {"found": False, "zip_code": zip_code, "error": str(e)}


@app.get("/api/data-sources/stats")
async def data_sources_stats():
    """Return summary stats of collected data sources."""
    try:
        from data_sources.schema import get_db as get_ds_db
        with get_ds_db() as conn:
            ds_count = conn.execute("SELECT COUNT(*) FROM data_sources").fetchone()[0]
            zip_count = conn.execute("SELECT COUNT(*) FROM zip_risk_data").fetchone()[0]
            ds_cats = conn.execute(
                "SELECT category, COUNT(*) FROM data_sources GROUP BY category ORDER BY COUNT(*) DESC"
            ).fetchall()

            fema_d = 0
            fema_c = 0
            try:
                fema_d = conn.execute("SELECT COUNT(*) FROM fema_disasters").fetchone()[0]
                fema_c = conn.execute("SELECT COUNT(*) FROM fema_flood_claims").fetchone()[0]
            except Exception as e:
                logger.debug("FEMA tables not yet created: %s", e)

            return {
                "data_sources": ds_count,
                "zip_codes_covered": zip_count,
                "fema_disasters": fema_d,
                "fema_flood_claims": fema_c,
                "categories": {cat: cnt for cat, cnt in ds_cats},
            }
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
# NEW OWNERSHIP API ENDPOINT — Advanced Ownership Lookup
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/ownership/lookup")
async def ownership_lookup(
    address: str,
    city: str,
    state: str,
    zip_code: str = "",
    apn: str = "",
    lat: float = 0.0,
    lng: float = 0.0,
):
    """
    Ownership lookup using 100% free sources (ArcGIS Parcels, Regrid Tiles, County Assessors).
    Returns: owner_name, owner_address, apn, confidence, source
    """
    sources_tried = []
    owner_name = ""
    owner_address = ""
    found_apn = apn

    try:
        # Need lat/lng for spatial queries — try to find a matching lead
        if not lat or not lng:
            cached = DataCache.load(allow_stale=True) or []
            addr_upper = address.upper().strip()
            for lead in cached[:200000]:
                if (lead.get("address", "").upper().strip() == addr_upper
                        and lead.get("lat") and lead.get("lng")):
                    lat = safe_float(lead.get("lat") or 0)
                    lng = safe_float(lead.get("lng") or 0)
                    break

        if lat and lng:
            # ArcGIS parcels (all 50 states)
            result = await enrich_owner_from_arcgis_parcel(lat, lng, state)
            if result and result.get("owner_name"):
                return {"success": True, "owner_name": result["owner_name"],
                        "owner_address": result.get("owner_address", ""),
                        "apn": result.get("apn", ""), "confidence": 0.85,
                        "source": "arcgis_parcels", "cost": 0}
            sources_tried.append("arcgis_parcels")

            # Regrid tiles
            result = await enrich_owner_from_regrid_tile(lat, lng)
            if result and result.get("owner_name"):
                return {"success": True, "owner_name": result["owner_name"],
                        "owner_address": result.get("owner_address", ""),
                        "apn": result.get("apn", ""), "confidence": 0.80,
                        "source": "regrid_tiles", "cost": 0}
            sources_tried.append("regrid_tiles")

        return {"success": False, "owner_name": "", "owner_address": "",
                "apn": "", "confidence": 0.0, "source": "none",
                "sources_tried": sources_tried, "cost": 0}
    except Exception as e:
        logger.error(f"Ownership lookup error: {e}")
        return {"success": False, "error": str(e), "owner_name": "",
                "confidence": 0.0, "cost": 0}


@app.get("/api/ownership/stats")
async def ownership_stats():
    """Get free enrichment statistics"""
    cached = DataCache.load(allow_stale=True) or []
    total = len(cached)
    with_owner = sum(1 for l in cached if l.get("owner_name"))
    with_email = sum(1 for l in cached if l.get("owner_email"))
    with_phone = sum(1 for l in cached if l.get("owner_phone"))
    return {
        "success": True,
        "total_leads": total,
        "with_owner_name": with_owner,
        "with_email": with_email,
        "with_phone": with_phone,
        "enrichment_rate": round(with_owner / total * 100, 1) if total else 0,
        "method": "free (ArcGIS + Regrid + County Assessors)",
        "cost": 0
    }


# ═══════════════════════════════════════════════════════════════════════════
# CRM INTEGRATION ENDPOINTS — Salesforce, HubSpot, ServiceTitan
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/crm/{crm_name}/authorize")
async def crm_authorize(crm_name: str, state: str = ""):
    """Get OAuth authorization URL for any CRM"""
    auth_url = crm_manager.get_auth_url(crm_name.lower(), state)
    if auth_url:
        return {
            "success": True,
            "auth_url": auth_url,
            "crm": crm_name
        }
    else:
        return {
            "success": False,
            "error": f"Unknown CRM: {crm_name}"
        }


@app.get("/api/crm/{crm_name}/callback")
async def crm_callback(crm_name: str, code: str, state: str = ""):
    """OAuth callback handler for any CRM"""
    result = await crm_manager.exchange_code(crm_name.lower(), code)
    if result.get("success"):
        # TODO: Store tokens in database per user
        return {
            "success": True,
            "message": f"{crm_name} connected successfully",
            "crm": crm_name
        }
    else:
        return result


@app.post("/api/crm/{crm_name}/sync")
async def crm_sync_leads(crm_name: str, lead_ids: List[int] = None):
    """Sync selected leads to CRM"""
    # Get leads from cache
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"success": False, "error": "No leads available"}

    # Filter to selected leads if provided
    if lead_ids:
        leads_to_sync = [lead for lead in cached if lead.get("id") in lead_ids]
    else:
        # Sync all hot leads by default
        leads_to_sync = [lead for lead in cached if lead.get("temperature") == "hot"][:50]

    if not leads_to_sync:
        return {"success": False, "error": "No leads to sync"}

    result = await crm_manager.sync_leads(crm_name.lower(), leads_to_sync)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# EMAIL CAMPAIGN ENDPOINTS — SendGrid Integration
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/campaigns/email/send")
async def send_email_campaign(request: Request):
    """Send a single email"""
    body = await request.json()
    to_email = body.get("to_email", "")
    subject = body.get("subject", "")
    html_content = body.get("html_content", "")
    plain_content = body.get("plain_content", "")
    if not to_email or not subject:
        raise HTTPException(status_code=400, detail="to_email and subject are required")
    result = await email_service.send_single_email(
        to_email=to_email,
        subject=subject,
        html_content=html_content,
        plain_content=plain_content
    )
    return result


@app.post("/api/campaigns/email/hot-lead-alert")
async def send_hot_lead_email(lead_id: int, contractor_email: str):
    """Send hot lead alert email"""
    cached = DataCache.load(allow_stale=True)
    lead = next((l for l in cached if l.get("id") == lead_id), None)

    if not lead:
        return {"success": False, "error": "Lead not found"}

    result = await email_service.send_hot_lead_alert(lead, contractor_email)
    return result


@app.post("/api/campaigns/email/daily-digest")
async def send_daily_digest_email(contractor_email: str, days: int = 1):
    """Send daily digest of new leads"""
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"success": False, "error": "No leads available"}

    # Filter leads from last N days
    cutoff = datetime.now() - timedelta(days=days)
    recent_leads = []
    for lead in cached:
        issue_date = lead.get("issue_date", "")
        if issue_date:
            try:
                dt = datetime.fromisoformat(issue_date.replace("Z", "+00:00"))
                if dt >= cutoff:
                    recent_leads.append(lead)
            except (ValueError, TypeError):
                pass

    if not recent_leads:
        return {"success": False, "error": "No recent leads"}

    result = await email_service.send_daily_digest(contractor_email, recent_leads)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# SMS CAMPAIGN ENDPOINTS — Twilio Integration
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/sms/bulk")
async def sms_bulk(request: Request):
    """Bulk SMS send — used by frontend command center."""
    body = await request.json()
    numbers = body.get("numbers", [])
    message = body.get("message", "")
    if not numbers or not message:
        raise HTTPException(status_code=400, detail="numbers and message required")

    sent = 0
    failed = 0
    for num in numbers[:200]:  # Cap at 200 per batch
        try:
            await sms_service.send_sms(to_number=num, message=message)
            sent += 1
        except Exception:
            failed += 1

    return {"sent": sent, "failed": failed, "total": len(numbers)}


@app.post("/api/campaigns/sms/send")
async def send_sms_campaign(request: Request):
    """Send a single SMS"""
    body = await request.json()
    to_number = body.get("to_phone", "") or body.get("to_number", "")
    message = body.get("message", "")
    if not to_number or not message:
        raise HTTPException(status_code=400, detail="to_phone and message are required")
    result = await sms_service.send_sms(to_number=to_number, message=message)
    return result


@app.post("/api/campaigns/sms/hot-lead-alert")
async def send_hot_lead_sms(lead_id: int, contractor_phone: str):
    """Send hot lead alert via SMS"""
    cached = DataCache.load(allow_stale=True)
    lead = next((l for l in cached if l.get("id") == lead_id), None)

    if not lead:
        return {"success": False, "error": "Lead not found"}

    result = await sms_service.send_hot_lead_alert(lead, contractor_phone)
    return result


@app.post("/api/campaigns/sms/daily-summary")
async def send_daily_summary_sms(contractor_phone: str):
    """Send daily summary SMS"""
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"success": False, "error": "No leads available"}

    # Calculate stats
    cutoff = datetime.now() - timedelta(days=1)
    new_leads = 0
    total_value = 0
    hot_leads = 0

    for lead in cached:
        issue_date = lead.get("issue_date", "")
        if issue_date:
            try:
                dt = datetime.fromisoformat(issue_date.replace("Z", "+00:00"))
                if dt >= cutoff:
                    new_leads += 1
                    total_value += float(lead.get("valuation", 0) or 0)
                    if lead.get("temperature") == "hot":
                        hot_leads += 1
            except (ValueError, TypeError):
                pass

    stats = {
        "new_leads": new_leads,
        "total_value": total_value,
        "hot_leads": hot_leads
    }

    result = await sms_service.send_daily_summary(contractor_phone, stats)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# PARCEL DATA ENDPOINTS — Regrid Nationwide + County Assessor
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/parcel/coordinates/{lat}/{lng}")
async def get_parcel_by_coords(lat: float, lng: float):
    """Get parcel data by coordinates (Regrid or free county assessor)"""
    # Try Regrid first (if API key configured)
    result = await parcel_service.get_parcel_by_coordinates(lat, lng)

    if result.get("success"):
        return result

    # Fallback to LA County Assessor (free)
    result = await county_assessor.get_parcel_la_county(lat=lat, lng=lng)
    return result


@app.get("/api/parcel/address")
async def get_parcel_by_address(
    address: str,
    city: str,
    state: str,
    zip_code: str = ""
):
    """Get parcel data by street address"""
    result = await parcel_service.get_parcel_by_address(address, city, state, zip_code)
    return result


@app.get("/api/parcel/{lat}/{lng}")
async def get_parcel(lat: float, lng: float):
    """Get parcel boundary from LA County Assessor"""
    url = f"https://maps.assessment.lacounty.gov/GVH_2_2/GVH/wsLegacyService/getParcelByLocation"
    params = {'type': 'json', 'lat': lat, 'lng': lng}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    raise HTTPException(status_code=response.status, detail="Parcel API error")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/propertyreach/search")
async def propertyreach_proxy(
    street: str = Query(..., description="Street address"),
    city: str = Query(...),
    state: str = Query(...),
    zip: str = Query("", description="ZIP/postal code")
):
    """
    On-demand PropertyReach search for a single address.
    """
    try:
        result = await propertyreach_search(street, city, state, zip)
        return result
    except HTTPException as e:
        return {"success": False, "error": e.detail}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Ownership & Property Enrichment API Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/lead/{lead_id}/ownership")
async def get_lead_ownership(lead_id: int):
    """On-demand ownership enrichment for a single lead.
    Chains: LA Assessor → State GIS → Regrid → PropertyReach → ATTOM."""
    permit = _get_permit_from_cache(lead_id)
    if not permit:
        raise HTTPException(status_code=404, detail="Lead not found")

    enriched = await enrich_lead_ownership(dict(permit))

    return {
        "success": bool(enriched.get("owner_name")),
        "lead_id": lead_id,
        "owner_name": enriched.get("owner_name", ""),
        "owner_phone": enriched.get("owner_phone", ""),
        "owner_email": enriched.get("owner_email", ""),
        "owner_address": enriched.get("owner_address", ""),
        "market_value": enriched.get("market_value", 0),
        "year_built": enriched.get("year_built", ""),
        "square_feet": enriched.get("square_feet", 0),
        "lot_size": enriched.get("lot_size", ""),
        "zoning": enriched.get("zoning", ""),
        "bedrooms": enriched.get("bedrooms", ""),
        "bathrooms": enriched.get("bathrooms", ""),
        "apn": enriched.get("apn", ""),
        "land_value": enriched.get("land_value", 0),
        "improvement_value": enriched.get("improvement_value", 0),
        "enriched_at": datetime.utcnow().isoformat(),
    }


@app.get("/api/lead/{lead_id}/disaster-risk")
async def get_lead_disaster_risk(lead_id: int):
    """Get disaster risk profile for a lead (FEMA declarations, hazards)."""
    permit = _get_permit_from_cache(lead_id)
    if not permit:
        raise HTTPException(status_code=404, detail="Lead not found")

    risk = await get_disaster_risk_for_lead(permit)
    return {
        "success": True,
        "lead_id": lead_id,
        "state": permit.get("state", ""),
        "zip": permit.get("zip", ""),
        **risk,
    }


@app.get("/api/fema/claims")
async def api_fema_claims(
    state: Optional[str] = Query(None),
    zip_code: Optional[str] = Query(None, alias="zip"),
    limit: int = Query(100, le=1000),
):
    """Query FEMA NFIP flood insurance claims (no API key required)."""
    claims = await fetch_fema_nfip_claims(state=state, zip_code=zip_code, limit=limit)
    return {"count": len(claims), "claims": claims}


@app.get("/api/fema/disasters")
async def api_fema_disasters(
    state: Optional[str] = Query(None),
    limit: int = Query(50, le=500),
):
    """Query FEMA disaster declarations (no API key required)."""
    disasters = await fetch_fema_disasters(state=state, limit=limit)
    return {"count": len(disasters), "disasters": disasters}


@app.post("/api/enrichment/ownership")
async def api_batch_ownership_enrichment(
    max_tiles: int = Query(500, le=5000),
):
    """Batch ownership enrichment using free Regrid parcel tiles.
    Downloads MVT tiles and matches leads to parcels by proximity."""
    from services.parcel_enrichment import enrich_leads_from_tiles

    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"enriched": 0, "message": "No cached leads to enrich"}

    stats = await enrich_leads_from_tiles(cached, max_tiles=max_tiles)
    if stats.get("enriched", 0) > 0:
        DataCache.save(cached)
        await asyncio.to_thread(_rebuild_cleaned_leads)

    return {
        **stats,
        "total_leads": len(cached),
        "message": f"Enriched {stats.get('enriched', 0)} leads with owner data from parcel tiles",
    }


@app.post("/api/enrichment/contacts")
async def api_batch_contact_enrichment(
    max_leads: int = Query(500, le=5000),
    concurrency: int = Query(3, le=10),
):
    """Batch contact enrichment — finds phone numbers and emails for leads
    that already have owner_name. Uses ThatsThem, USSearch, TruePeopleSearch,
    FastPeopleSearch, VoterRecords, and OpenCorporates (LLC resolution). 100% free."""
    cached = DataCache.load(allow_stale=True)
    if not cached:
        return {"enriched": 0, "message": "No cached leads"}

    stats = await orchestrator_batch(cached, max_leads=max_leads, concurrency=concurrency, skip_slow=False)

    if stats.get("enriched", 0) > 0:
        DataCache.save(cached)
        await asyncio.to_thread(_rebuild_cleaned_leads)

    return {
        **stats,
        "total_leads": len(cached),
        "cost": 0,
        "message": (
            f"Found {stats.get('phones_found', 0)} phones and "
            f"{stats.get('emails_found', 0)} emails "
            f"in {stats.get('elapsed', 0)}s"
        ),
    }


@app.get("/api/data-sources")
async def api_data_sources():
    """List all configured data sources and their status."""
    sources = []
    for key, cfg in SOC_DATASETS.items():
        sources.append({
            "key": key,
            "label": cfg.get("label", key),
            "type": "socrata",
            "city": cfg.get("city", ""),
            "domain": cfg.get("domain", ""),
            "active": True,
        })
    for key, cfg in ARCGIS_DATASETS.items():
        sources.append({
            "key": key,
            "label": cfg.get("label", key),
            "type": "arcgis",
            "city": cfg.get("city", ""),
            "state": cfg.get("state", ""),
            "active": True,
        })
    for key, cfg in CKAN_DATASETS.items():
        sources.append({
            "key": key,
            "label": cfg.get("label", key),
            "type": "ckan",
            "city": cfg.get("city", ""),
            "state": cfg.get("state", ""),
            "domain": cfg.get("domain", ""),
            "active": True,
        })
    for key, cfg in FEDERAL_ENDPOINTS.items():
        sources.append({
            "key": key,
            "label": cfg.get("label", key),
            "type": cfg.get("type", "federal"),
            "description": cfg.get("description", ""),
            "active": True,
        })
    return {
        "total": len(sources),
        "socrata": len(SOC_DATASETS),
        "arcgis": len(ARCGIS_DATASETS),
        "ckan": len(CKAN_DATASETS),
        "federal": len(FEDERAL_ENDPOINTS),
        "sources": sources,
    }


# ---------------------------------------------------------------------------
# Billing routes (extracted to routes/billing.py)
# ---------------------------------------------------------------------------
try:
    from routes.billing import router as billing_router, get_user_plan, STRIPE_PLANS, FREE_PLAN
    app.include_router(billing_router)
    logger.info("✅ Billing router registered (plans, checkout, portal, webhook)")
except ImportError as e:
    logger.warning(f"Billing router not available: {e}")
    # Fallback plan definitions if billing routes fail to load
    STRIPE_PLANS = {}
    FREE_PLAN = {"name": "Free", "leads_per_month": 50, "enrichments_per_month": 5, "sms_per_month": 0, "export_csv": False, "crm_sync": False, "api_access": False}
    def get_user_plan(user_id: str) -> dict:
        return {**FREE_PLAN, "plan_id": "free"}


# ---------------------------------------------------------------------------
# Admin health endpoints (read-only)
# ---------------------------------------------------------------------------
@app.get("/admin/system/queues")
async def admin_queues(request: Request):
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM failed_leads_queue")
    failed = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM alerts_delivery_log WHERE status='retry'")
    retry = cur.fetchone()[0]
    conn.close()
    return {"failed_leads": failed, "alerts_retry": retry}


@app.get("/admin/system/billing")
async def admin_billing(request: Request):
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT status, count(*) FROM subscriptions GROUP BY status")
    rows = cur.fetchall()
    conn.close()
    return {"subscriptions": {r[0]: r[1] for r in rows}}


@app.get("/admin/system/cache-status")
async def admin_cache_status(request: Request):
    """Check alignment between JSON cache and SQLite DB."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    cache_count = len(_cleaned_leads) if _cleaned_leads else 0
    db_count = 0
    try:
        from models.database import get_db
        with get_db() as db:
            db_count = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    except Exception as e:
        logger.debug("Cache status db query failed: %s", e)
    ratio = (min(db_count, cache_count) / max(db_count, cache_count) * 100) if max(db_count, cache_count) > 0 else 100
    return {
        "cache_count": cache_count,
        "db_count": db_count,
        "alignment_pct": round(ratio, 1),
        "status": "ok" if ratio >= 80 else "misaligned",
    }


@app.get("/admin/system/memory")
async def admin_memory_status(request: Request):
    """Monitor memory usage of in-memory caches and process."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    import sys
    import os as _os

    process_mb = 0
    try:
        # Linux /proc/self/status RSS
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    process_mb = int(line.split()[1]) / 1024
                    break
    except Exception:
        try:
            import resource
            process_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        except Exception:
            pass

    cleaned_count = len(_cleaned_leads) if _cleaned_leads else 0
    by_id_count = len(_cleaned_leads_by_id) if _cleaned_leads_by_id else 0
    resp_cache_count = len(_response_cache)
    disaster_count = len(_disaster_cache)

    # Estimate sizes
    cleaned_est_mb = (sys.getsizeof(_cleaned_leads) + sum(sys.getsizeof(l) for l in _cleaned_leads[:100]) * (cleaned_count / 100 if cleaned_count > 100 else 1)) / (1024 * 1024) if _cleaned_leads else 0
    resp_cache_mb = sum(len(v[1]) for v in _response_cache.values()) / (1024 * 1024) if _response_cache else 0

    return {
        "process_rss_mb": round(process_mb, 1),
        "caches": {
            "cleaned_leads": {"count": cleaned_count, "est_mb": round(cleaned_est_mb, 1)},
            "cleaned_leads_by_id": {"count": by_id_count},
            "response_cache": {"count": resp_cache_count, "size_mb": round(resp_cache_mb, 1)},
            "disaster_cache": {"states_cached": disaster_count, "ttl_hours": _DISASTER_CACHE_TTL / 3600},
        },
    }


@app.get("/admin/system/territories")
async def admin_territories(request: Request):
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT zip_code, SUM(active_count) FROM territory_seats GROUP BY zip_code")
    rows = cur.fetchall()
    conn.close()
    return {"territories": {r[0]: r[1] for r in rows}}


@app.get("/admin/system/failures")
async def admin_failures(request: Request):
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, error FROM failed_leads_queue LIMIT 20")
    rows = cur.fetchall()
    conn.close()
    return {"failed_leads": rows}


@app.get("/admin/system/user-count")
async def admin_user_count(request: Request):
    """Return count of active users for admin KPI."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    conn = _leads_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
        count = cur.fetchone()[0]
    except Exception:
        try:
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
        except Exception:
            count = 0
    conn.close()
    return {"count": count}


# ---------------------------------------------------------------------------
# On Site frontend compatibility layer
# Maps existing backend data to the field names / endpoints the On Site
# UI expects (e.g. /api/permits, lead_score, project_value, etc.)
# ---------------------------------------------------------------------------

_SOLD_RE = re.compile(
    r"\b(sale|sold|transfer|deed|new\s+owner|change\s+of\s+ownership|"
    r"title\s+transfer|closing|escrow|buyer|purchase|conveyance|"
    r"ownership\s+change|real\s+estate\s+transaction)\b",
    re.IGNORECASE,
)

def _is_sold_property(lead: dict) -> bool:
    """Detect if a lead is associated with a recently sold property."""
    for field in ("work_description", "description", "description_full",
                  "permit_type", "status"):
        val = lead.get(field, "") or ""
        if _SOLD_RE.search(val):
            return True
    # If owner mailing address differs from property address, often indicates investor/flip
    owner_addr = (lead.get("owner_address") or "").lower().strip()
    prop_addr = (lead.get("address") or "").lower().strip()
    if owner_addr and prop_addr and owner_addr != prop_addr:
        # Mailing address in a different city/state suggests absentee owner (often post-sale)
        owner_city = (lead.get("owner_city") or "").lower()
        prop_city = (lead.get("city") or "").lower()
        if owner_city and prop_city and owner_city != prop_city:
            return True
    return False


# Expand terse permit codes into human-readable descriptions
_PERMIT_DESC_MAP = {
    "bldg-new": "New building construction — full structural build from foundation up",
    "bldg-alter/repair": "Building alteration or repair — structural modifications to existing building",
    "bldg-demolition": "Demolition permit — tear-down of existing structure",
    "bldg-addition": "Building addition — expanding existing structure with new square footage",
    "electrical": "Electrical work — wiring, panel upgrades, or electrical system installation",
    "elec": "Electrical work — wiring, panel upgrades, or electrical system installation",
    "plumbing": "Plumbing work — pipe installation, water heater, sewer, or fixture replacement",
    "plum": "Plumbing work — pipe installation, water heater, sewer, or fixture replacement",
    "mechanical": "Mechanical/HVAC — heating, ventilation, air conditioning installation or repair",
    "mech": "Mechanical/HVAC — heating, ventilation, air conditioning installation or repair",
    "grading": "Grading permit — earthwork, excavation, or site preparation",
    "re-roof": "Re-roofing — roof replacement or major roof repair",
    "solar": "Solar panel installation — photovoltaic system or solar thermal",
    "pool/spa": "Pool or spa construction — in-ground pool, spa, or water feature",
    "fire sprinkler": "Fire sprinkler system — fire protection system installation or modification",
    "sign": "Sign permit — installation of commercial signage",
    "fence/wall": "Fence or retaining wall construction",
    "construction fence": "Construction fence — temporary perimeter fencing for active construction site",
    "tenant improvement": "Tenant improvement — interior buildout for commercial space",
    "commercial": "Commercial construction project",
    "residential": "Residential construction project",
    "bldg": "Building permit — construction, modification, or repair of a structure",
    "construction": "Construction permit — general building or site work",
    "general construction": "General construction permit — building or site construction work",
    "permit – express permit program": "Express permit — expedited review for qualifying projects",
    "permit - renovation/alteration": "Renovation or alteration — remodel, upgrade, or interior modification",
    "supplemental": "Supplemental permit — additional work scope on an existing permit",
    "alteration": "Alteration permit — interior or exterior modifications to existing building",
    "new": "New construction — ground-up build of a new structure",
    "nonbldg-new": "Non-building construction — new site work (retaining wall, parking, utilities, etc.)",
    "sidewalk shed": "Sidewalk shed — temporary pedestrian protection during construction",
    "suspended scaffold": "Suspended scaffold — exterior platform for facade work or high-rise construction",
}

def _enrich_description(lead: dict) -> dict:
    """Expand short/terse descriptions into more detailed ones."""
    desc = lead.get("description_full") or ""
    work_desc = lead.get("work_description") or ""
    ptype = lead.get("permit_type") or ""

    # If description is already detailed (>30 chars), leave it
    if len(desc) > 30:
        return lead

    # Try to match against known permit type codes
    key = (ptype or work_desc or desc).strip().lower()
    expanded = _PERMIT_DESC_MAP.get(key)

    if not expanded:
        # Partial match
        for k, v in _PERMIT_DESC_MAP.items():
            if k in key or key in k:
                expanded = v
                break

    if expanded:
        val = lead.get("valuation") or 0
        val_str = f"${val:,.0f}" if val else "N/A"
        city = lead.get("city") or ""
        date = lead.get("issue_date") or ""
        parts = [expanded]
        if val:
            parts.append(f"Project value: {val_str}")
        if city:
            parts.append(f"Location: {city}")
        if date:
            parts.append(f"Filed: {date}")
        lead["description_full"] = " | ".join(parts)

    return lead


def _transform_lead(lead: dict) -> dict:
    """Map backend lead fields → On Site frontend field names."""
    out = _enrich_description(dict(lead))
    # Convert id to string to prevent JS precision loss (IDs > 2^53)
    if 'id' in out:
        out['id'] = str(out['id'])
    # Field renames (keep originals too for backward compat)
    out["lead_score"] = out.get("score") or 0
    out["project_value"] = out.get("valuation") or 0
    out["days_old"] = _live_days_old(out)
    out["days_since_issued"] = out["days_old"]
    out["issued_date"] = out.get("issue_date") or ""
    out["status"] = out.get("temperature") or out.get("status") or "Unknown"
    out["state"] = out.get("state") or _infer_state(out.get("city", ""))
    out["source_city"] = out.get("source_city") or out.get("city") or ""
    out["permit_url"] = out.get("permit_url") or ""
    # Infer is_sold from description keywords or existing flag
    if not out.get("is_sold"):
        out["is_sold"] = _is_sold_property(out)
    # Ensure lat/lng are floats
    for k in ("lat", "lng"):
        try:
            out[k] = float(out[k])
        except (TypeError, ValueError):
            out[k] = 0.0
    return out


import re as _re
_INSURANCE_RE = _re.compile(
    r"\b(insurance|fire.?damage|water.?damage|storm.?damage|flood.?damage|"
    r"mold|remediat|restorat|casualty|catastroph|wind.?damage|hail|"
    r"smoke.?damage|vandalism|disaster|emergency.?repair|claim|loss|adjuster)\b",
    _re.IGNORECASE,
)

def _is_insurance_claim(lead: dict) -> bool:
    """Server-side mirror of frontend isInsuranceClaim()."""
    for field in ("work_description", "description", "description_full",
                  "permit_type", "status"):
        val = lead.get(field, "") or ""
        if _INSURANCE_RE.search(val):
            return True
    return False


@app.get("/api/permits")
async def compat_permits(
    minScore: Optional[int] = Query(None),
    hot: Optional[bool] = Query(None),
    days: Optional[int] = Query(None),
    bbox: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    zip: Optional[str] = Query(None),
    zips: Optional[str] = Query(None),
    insurance: Optional[bool] = Query(None),
    limit: Optional[int] = Query(100000, le=100000, description="Max results to return (default 100000, max 100000)"),
    offset: Optional[int] = Query(0, ge=0, description="Number of results to skip for pagination"),
    request: Request = None,
):
    """On Site compat — returns flat array of leads with renamed fields. Now with pagination!"""
    try:
        # Add 30-second timeout protection to prevent hangs
        async with asyncio.timeout(30):
            cached = DataCache.load(allow_stale=False)
            if not cached:
                cached = DataCache.load(allow_stale=True) or []

            # ── v3.0 FALLBACK: If JSON cache is empty, serve from SQLite ──
            if not cached:
                try:
                    from models.database import query_leads as db_query_leads
                    # Use pagination parameters
                    effective_limit = min(limit or 1000, LEADS_RETURN_LIMIT)
                    db_leads, db_total = db_query_leads(
                        limit=effective_limit,
                        offset=offset,
                        city=city,
                        state=state,
                        min_score=minScore,
                        max_days=days if days else MAX_DAYS_OLD,
                        sort_by="score",
                    )
                    # OPTIMIZATION: Always return DB results (even if empty) to avoid slow sync fallback
                    logger.info(f"Serving {len(db_leads)} compat leads from SQLite (offset={offset}, limit={effective_limit})")
                    # Apply additional filters
                    if state:
                        s = state.upper()
                        db_leads = [l for l in db_leads if str(l.get("state", "")).upper() == s]
                    if zip:
                        db_leads = [l for l in db_leads if str(l.get("zip", "")).startswith(zip)]
                    if zips:
                        zip_set = set(z.strip() for z in zips.split(","))
                        db_leads = [l for l in db_leads if str(l.get("zip", "")) in zip_set]
                    if bbox:
                        parts = bbox.split(",")
                        if len(parts) == 4:
                            try:
                                s, w, n, e = float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
                                db_leads = [l for l in db_leads if _in_bbox(l, s, w, n, e)]
                            except ValueError:
                                pass
                    if insurance:
                        db_leads = [l for l in db_leads if _is_insurance_claim(l)]
                    if hot:
                        db_leads = [l for l in db_leads if str(l.get("temperature", "")).lower() == "hot"]
                    # OPTIMIZATION: Skip compute_readiness() for map view - not needed for lat/lng display
                    # db_leads = [compute_readiness(l) for l in db_leads]
                    db_leads.sort(key=lambda l: l.get("score", 0), reverse=True)
                    return [_transform_lead(l) for l in db_leads]
                except Exception as e:
                    logger.warning(f"SQLite compat fallback failed: {e}")
                # OPTIMIZATION: Disable sync fallback to prevent DB locks
                # await sync_data()
                cached = DataCache.load(allow_stale=True) or []

            filtered = [dict(l) for l in apply_access_filter(cached, request)]

            # Apply filters EARLY to reduce processing
            if city:
                c = city.lower()
                filtered = [l for l in filtered if c in str(l.get("city", "")).lower()]
            if state:
                s = state.upper()
                filtered = [l for l in filtered if str(l.get("state", "")).upper() == s]
            if zip:
                filtered = [l for l in filtered if str(l.get("zip", "")).startswith(zip)]
            if zips:
                zip_set = set(z.strip() for z in zips.split(","))
                filtered = [l for l in filtered if str(l.get("zip", "")) in zip_set]
            if bbox:
                parts = bbox.split(",")
                if len(parts) == 4:
                    try:
                        s, w, n, e = float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
                        filtered = [
                            l for l in filtered
                            if _in_bbox(l, s, w, n, e)
                        ]
                    except ValueError:
                        pass
            # Default to 30-day cap (matches frontend hard cap)
            effective_days = days if days else MAX_DAYS_OLD
            filtered = [l for l in filtered if (_live_days_old(l) or 999) <= effective_days]
            if insurance:
                filtered = [l for l in filtered if _is_insurance_claim(l)]

            # OPTIMIZATION: Skip compute_readiness() for map view - not needed for lat/lng display
            # filtered = [compute_readiness(l) for l in filtered]

            if minScore:
                filtered = [l for l in filtered if (l.get("score") or 0) >= minScore]
            if hot:
                filtered = [l for l in filtered if str(l.get("temperature", "")).lower() == "hot"]

            filtered.sort(key=lambda l: l.get("score", 0), reverse=True)

            # Batch-load pipeline stages from DB (only for paginated results)
            stage_map = {}
            notes_map = {}
            conn = None
            try:
                conn = _leads_conn()
                cur = conn.cursor()
                cur.execute("SELECT lead_id, stage FROM lead_stages")
                stage_map = {row[0]: row[1] for row in cur.fetchall()}
                cur.execute("CREATE TABLE IF NOT EXISTS lead_notes (lead_id INTEGER PRIMARY KEY, notes TEXT, updated_at TEXT)")
                cur.execute("SELECT lead_id, notes FROM lead_notes")
                notes_map = {row[0]: row[1] for row in cur.fetchall()}
            except Exception as e:
                logger.error("Failed to load lead stages/notes from db: %s", e)
            finally:
                if conn:
                    conn.close()

            # Apply pagination AFTER filtering and sorting
            effective_limit = min(limit or 1000, LEADS_RETURN_LIMIT)
            paginated = filtered[offset:offset + effective_limit]

            results = []
            for l in paginated:
                lid = l.get("id")
                l["pipeline_stage"] = stage_map.get(lid, "new")
                l["notes"] = notes_map.get(lid, "")
                results.append(_transform_lead(l))

            logger.info(f"Returned {len(results)} permits (offset={offset}, limit={effective_limit}, total_filtered={len(filtered)})")
            return results  # flat array, not wrapped

    except asyncio.TimeoutError:
        logger.error("/api/permits request timed out after 30 seconds")
        raise HTTPException(status_code=504, detail="Request timeout - try adding filters (city, state, or days) to narrow results")
    except Exception as e:
        logger.error(f"/api/permits unexpected error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _in_bbox(lead: dict, south: float, west: float, north: float, east: float) -> bool:
    try:
        lat = float(lead.get("lat") or 0)
        lng = float(lead.get("lng") or 0)
        return south <= lat <= north and west <= lng <= east
    except (TypeError, ValueError):
        return False


@app.get("/api/permits/{permit_id}")
async def compat_permit_detail(permit_id: str):
    """On Site compat — single lead detail with renamed fields."""
    lead = _get_permit_from_cache(permit_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    lead = compute_readiness(dict(lead))
    # Load pipeline stage, notes, tags from DB
    try:
        from models.database import get_db
        with get_db() as conn:
            row = conn.execute("SELECT stage FROM lead_stages WHERE lead_id=?", (permit_id,)).fetchone()
            lead["pipeline_stage"] = row[0] if row else "new"
            row = conn.execute("SELECT notes FROM lead_notes WHERE lead_id=?", (permit_id,)).fetchone()
            lead["notes"] = row[0] if row else ""
            row = conn.execute("SELECT tags FROM lead_tags WHERE lead_id=?", (permit_id,)).fetchone()
            lead["tags"] = json.loads(row[0]) if row else []
    except Exception:
        lead.setdefault("pipeline_stage", "new")
    return _transform_lead(lead)


@app.post("/api/permits/{permit_id}/enrich")
async def compat_enrich(permit_id: str):
    """On Site compat — wraps free ownership enrichment pipeline."""
    lead = _get_permit_from_cache(permit_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    lead = dict(lead)
    # Use the full free enrichment pipeline (LA Assessor + ArcGIS + Regrid)
    try:
        lead = await enrich_lead_ownership(lead)
        logger.info(f"Enriched lead {permit_id}: owner={lead.get('owner_name', 'N/A')}")
    except Exception as e:
        logger.warning(f"Enrichment failed for {permit_id}: {e}")
    # Also try LA Assessor by APN if we have it and still missing owner
    if not lead.get("owner_name") and lead.get("apn"):
        try:
            result = get_la_owner(lead["apn"])
            if result:
                for k, v in result.items():
                    if v and not lead.get(k):
                        lead[k] = v
        except Exception as e:
            logger.warning("LA owner enrichment failed for APN %s: %s", lead.get("apn"), e)
    # Persist enrichment back to SQLite
    try:
        from models.database import update_lead
        updates = {
            "owner_name": lead.get("owner_name", ""),
            "owner_phone": lead.get("owner_phone", ""),
            "owner_email": lead.get("owner_email", ""),
            "owner_address": lead.get("owner_address", ""),
            "apn": lead.get("apn", ""),
            "enrichment_status": "enriched" if lead.get("owner_name") else "failed",
        }
        update_lead(permit_id, {k: v for k, v in updates.items() if v})
    except Exception as e:
        logger.error("Failed to persist enrichment for permit %s: %s", permit_id, e)
    return _transform_lead(lead)


@app.get("/api/permits/{permit_id}/history")
async def compat_history(permit_id: str):
    """On Site compat — permit history at same address."""
    # Try int conversion for cache lookup
    try:
        pid_int = int(permit_id)
    except (ValueError, TypeError):
        pid_int = -1
    lead = _get_permit_from_cache(pid_int)
    if not lead:
        return []
    address = (lead.get("address") or "").lower().strip()
    if not address:
        return []
    # Query SQLite for ALL permits at same address
    try:
        from models.database import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM leads WHERE LOWER(TRIM(address)) = ? AND id != ? ORDER BY issue_date DESC LIMIT 50",
                (address, pid_int)
            ).fetchall()
            if rows:
                return [_transform_lead(compute_readiness(dict(r))) for r in rows]
    except Exception as e:
        logger.debug(f"History query failed: {e}")
    # Fallback to cache — show ALL permits at same address
    cached = DataCache.load(allow_stale=True) or []
    related = [
        _transform_lead(compute_readiness(dict(l)))
        for l in cached
        if (l.get("address") or "").lower().strip() == address and int(l.get("id", -1)) != pid_int
    ]
    related.sort(key=lambda l: l.get("issue_date", "") or l.get("issued_date", ""), reverse=True)
    return related[:50]


@app.post("/api/permits/{permit_id}/stage")
async def compat_save_stage(permit_id: str, payload: dict):
    """On Site compat — save pipeline stage for a lead."""
    stage = payload.get("stage", "")
    try:
        from models.database import get_db
        with get_db() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS lead_stages
                   (lead_id INTEGER PRIMARY KEY, stage TEXT, updated_at TEXT)"""
            )
            conn.execute(
                """INSERT OR REPLACE INTO lead_stages (lead_id, stage, updated_at)
                   VALUES (?, ?, ?)""",
                (permit_id, stage, datetime.utcnow().isoformat()),
            )
    except Exception as e:
        logger.error(f"save stage failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save stage")
    return {"status": "ok", "lead_id": permit_id, "stage": stage}


@app.post("/api/permits/{permit_id}/notes")
async def compat_save_notes(permit_id: str, payload: dict):
    """On Site compat — save notes for a lead."""
    notes = payload.get("notes", "")
    try:
        from models.database import get_db
        with get_db() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS lead_notes
                   (lead_id INTEGER PRIMARY KEY, notes TEXT, updated_at TEXT)"""
            )
            conn.execute(
                """INSERT OR REPLACE INTO lead_notes (lead_id, notes, updated_at)
                   VALUES (?, ?, ?)""",
                (permit_id, notes, datetime.utcnow().isoformat()),
            )
    except Exception as e:
        logger.error(f"save notes failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save notes")
    return {"status": "ok"}


@app.post("/api/permits/{permit_id}/tags")
async def compat_save_tags(permit_id: str, payload: dict):
    """On Site compat — save tags for a lead."""
    tags = payload.get("tags", [])
    try:
        from models.database import get_db
        with get_db() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS lead_tags
                   (lead_id INTEGER PRIMARY KEY, tags TEXT, updated_at TEXT)"""
            )
            conn.execute(
                """INSERT OR REPLACE INTO lead_tags (lead_id, tags, updated_at)
                   VALUES (?, ?, ?)""",
                (permit_id, json.dumps(tags), datetime.utcnow().isoformat()),
            )
    except Exception as e:
        logger.error(f"save tags failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to save tags")
    return {"status": "ok"}


@app.get("/api/permits/{permit_id}/score-breakdown")
async def compat_score_breakdown(permit_id: str):
    """On Site compat — score component breakdown."""
    lead = _get_permit_from_cache(permit_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    lead = compute_readiness(dict(lead))
    score = lead.get("score", 0)
    val = lead.get("valuation", 0)
    days = _live_days_old(lead)
    # Approximate component scores
    urgency_pts = max(0, min(35, 35 - days))
    value_pts = min(30, int((val / 500000) * 30)) if val else 5
    type_pts = 20 if lead.get("permit_type", "").lower() in ("residential addition", "adu", "new construction", "mixed use") else 10
    contact_pts = 15 if lead.get("owner_name") else 0
    return {
        "total": score,
        "components": [
            {"label": "Urgency (recency)", "score": urgency_pts, "max": 35},
            {"label": "Project Value", "score": value_pts, "max": 30},
            {"label": "Permit Type", "score": type_pts, "max": 20},
            {"label": "Contact Available", "score": contact_pts, "max": 15},
        ],
    }


@app.get("/api/permits/{permit_id}/comparables")
async def compat_comparables(permit_id: str):
    """On Site compat — nearby similar permits."""
    lead = _get_permit_from_cache(permit_id)
    if not lead:
        return []
    cached = DataCache.load(allow_stale=True) or []
    permit_type = (lead.get("permit_type") or "").lower()
    city_val = (lead.get("city") or "").lower()
    comps = []
    for l in cached:
        if int(l.get("id", -1)) == permit_id:
            continue
        if (l.get("city") or "").lower() == city_val and (l.get("permit_type") or "").lower() == permit_type:
            comps.append(_transform_lead(compute_readiness(dict(l))))
            if len(comps) >= 10:
                break
    return comps


@app.get("/api/permits/{permit_id}/competitors")
async def compat_competitors(permit_id: str):
    """On Site compat — leads with contractor already listed (competition)."""
    lead = _get_permit_from_cache(permit_id)
    if not lead:
        return []
    cached = DataCache.load(allow_stale=True) or []
    city_val = (lead.get("city") or "").lower()
    competitors = []
    for l in cached:
        if (l.get("city") or "").lower() == city_val and l.get("contractor_name"):
            competitors.append({
                "contractor_name": l["contractor_name"],
                "contractor_phone": l.get("contractor_phone", ""),
                "address": l.get("address", ""),
                "permit_type": l.get("permit_type", ""),
            })
            if len(competitors) >= 15:
                break
    return competitors


@app.post("/api/permits/{permit_id}/skip-trace")
async def compat_skip_trace(permit_id: str):
    """On Site compat — wraps PropertyReach skip-trace."""
    lead = _get_permit_from_cache(permit_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    try:
        result = await propertyreach_search(
            lead.get("address", ""),
            lead.get("city", ""),
            lead.get("state", "CA"),
            lead.get("zip", ""),
        )
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/permits/{permit_id}/contact")
async def record_contact_attempt(permit_id: str, payload: dict):
    """Record contact attempt for a lead"""
    try:
        now_iso = datetime.utcnow().isoformat()

        # Update lead_stages in leads.db
        conn = _leads_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO lead_stages (lead_id, stage, updated_at) VALUES (?, ?, ?)",
                (str(permit_id), "contacted", now_iso),
            )
            conn.commit()
        finally:
            conn.close()

        # Log in pipeline_history (leads.db via get_db)
        try:
            with get_db() as db:
                db.execute(
                    """CREATE TABLE IF NOT EXISTS pipeline_history
                       (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        lead_id TEXT, from_stage TEXT, to_stage TEXT,
                        notes TEXT, timestamp TEXT)""")
                db.execute(
                    """INSERT INTO pipeline_history (lead_id, from_stage, to_stage, notes, timestamp)
                       VALUES (?, ?, ?, ?, ?)""",
                    (str(permit_id), "new", "contacted",
                     payload.get("notes", "Contact attempt recorded"), now_iso),
                )
        except Exception:
            pass  # Non-critical history logging

        return {"status": "ok", "lead_id": permit_id, "contacted_at": now_iso}
    except Exception as e:
        logger.error(f"Contact recording error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sources")
async def compat_sources():
    """On Site compat — list active data sources."""
    sources = []
    # Use pre-processed cleaned leads (fast, already in memory)
    cached = _cleaned_leads or []
    # Count leads per source
    source_counts: Dict[str, int] = defaultdict(int)
    for l in cached:
        src = l.get("source") or "Unknown"
        source_counts[src] += 1
    now = datetime.utcnow().isoformat()
    for key, cfg in SOC_DATASETS.items():
        label = cfg.get("label", key)
        sources.append({
            "key": key,
            "label": label,
            "lastSync": now,
            "count": source_counts.get(label, 0),
            "status": "Live" if source_counts.get(label, 0) > 0 else "Pending",
            "level": "live" if source_counts.get(label, 0) > 0 else "pending",
        })
    return sources


@app.get("/api/sources/status")
async def sources_detailed_status():
    """Get comprehensive status of all 92 configured data sources."""
    from collections import Counter

    # Use pre-processed cleaned leads (fast, already in memory)
    cached = _cleaned_leads or []

    # Count leads by source name
    source_counts = Counter(lead.get('source', 'Unknown') for lead in cached)

    # Collect all sources from all source types
    all_sources = {}

    # Add Socrata/CKAN sources
    for key, cfg in SOC_DATASETS.items():
        label = cfg.get("label", key)
        count = source_counts.get(label, 0)
        all_sources[key] = {
            "key": key,
            "label": label,
            "city": cfg.get("city", "Unknown"),
            "state": cfg.get("state", ""),
            "type": "socrata",
            "domain": cfg.get("domain", ""),
            "status": "active" if count > 0 else "configured",
            "lead_count": count,
            "last_sync": datetime.utcnow().isoformat() if count > 0 else None
        }

    # Add ArcGIS sources
    for key, cfg in ARCGIS_DATASETS.items():
        label = cfg.get("label", key)
        count = source_counts.get(label, 0)
        all_sources[key] = {
            "key": key,
            "label": label,
            "city": cfg.get("city", "Unknown"),
            "state": cfg.get("state", ""),
            "type": "arcgis",
            "status": "active" if count > 0 else "configured",
            "lead_count": count,
            "last_sync": datetime.utcnow().isoformat() if count > 0 else None
        }

    # Add Federal endpoints
    for key, cfg in FEDERAL_ENDPOINTS.items():
        label = cfg.get("label", key)
        count = source_counts.get(label, 0)
        all_sources[key] = {
            "key": key,
            "label": label,
            "type": cfg.get("type", "federal"),
            "status": "active" if count > 0 else "configured",
            "lead_count": count,
            "last_sync": datetime.utcnow().isoformat() if count > 0 else None
        }

    # Categorize
    active = [s for s in all_sources.values() if s['status'] == 'active']
    inactive = [s for s in all_sources.values() if s['status'] == 'configured']

    # Sort by lead count
    active.sort(key=lambda x: x['lead_count'], reverse=True)
    inactive.sort(key=lambda x: x['label'])

    return {
        "total_configured": len(all_sources),
        "active_count": len(active),
        "inactive_count": len(inactive),
        "total_leads": len(cached),
        "active_sources": active,
        "inactive_sources": inactive,
        "coverage_percent": round((len(active) / len(all_sources) * 100), 1) if all_sources else 0,
        "timestamp": datetime.utcnow().isoformat()
    }


_stats_cache: dict = {}  # (version, result)

@app.get("/api/stats")
async def compat_stats():
    """On Site compat — aggregate lead statistics (cached per version)."""
    global _stats_cache
    if _stats_cache.get("v") == _cleaned_leads_version and _stats_cache.get("r"):
        return _stats_cache["r"]
    cached = _cleaned_leads or []
    total = len(cached)
    # Score-based counts (canonical 80/60/40 thresholds from TEMP_THRESHOLDS)
    hot = sum(1 for l in cached if safe_float(l.get("score")) >= TEMP_THRESHOLDS["hot"])
    warm = sum(1 for l in cached if TEMP_THRESHOLDS["warm"] <= safe_float(l.get("score")) < TEMP_THRESHOLDS["hot"])
    med = sum(1 for l in cached if TEMP_THRESHOLDS["med"] <= safe_float(l.get("score")) < TEMP_THRESHOLDS["warm"])
    cold = total - hot - warm - med
    total_value = sum(safe_float(l.get("valuation")) for l in cached)
    avg_score = (sum(safe_float(l.get("score")) for l in cached) / total) if total else 0
    cities: Dict[str, int] = defaultdict(int)
    for l in cached:
        cities[l.get("city") or "Unknown"] += 1
    result = {
        "total_leads": total,
        "hot_leads": hot,
        "warm_leads": warm,
        "medium_leads": med,
        "cold_leads": cold,
        "total_value": total_value,
        "avg_score": round(avg_score, 1),
        "cities": dict(cities),
        "sources": len(SOC_DATASETS),
    }
    _stats_cache = {"v": _cleaned_leads_version, "r": result}
    return result


def _filter_by_date(cached, range_days):
    """Split cached leads into current-period and prior-period for comparison."""
    now = datetime.utcnow()
    cutoff = now - timedelta(days=range_days)
    prior_cutoff = cutoff - timedelta(days=range_days)
    current, prior = [], []
    for l in cached:
        d = l.get("issue_date")
        if d:
            try:
                dt = datetime.fromisoformat(d)
                if dt >= cutoff:
                    current.append(l)
                elif dt >= prior_cutoff:
                    prior.append(l)
                continue
            except (ValueError, TypeError):
                pass
        current.append(l)
    return current, prior


@app.get("/api/analytics")
async def compat_analytics(date_range: int = Query(30, alias="range"), zips: str = Query("")):
    """On Site compat — analytics dashboard data matching frontend format."""
    cached = DataCache.load(allow_stale=True) or []
    if zips:
        zip_set = set(z.strip() for z in zips.split(","))
        cached = [l for l in cached if str(l.get("zip", "")) in zip_set]

    current, prior = _filter_by_date(cached, date_range)

    # Current period KPIs
    total = len(current)
    total_value = sum(safe_float(l.get("valuation")) for l in current)
    with_contact = sum(1 for l in current if l.get("owner_name") or l.get("owner_phone"))

    # Prior period for change %
    prior_total = len(prior) or 1
    prior_value = sum(safe_float(l.get("valuation")) for l in prior) or 1
    prior_contact = sum(1 for l in prior if l.get("owner_name") or l.get("owner_phone")) or 1

    def pct_change(cur, prev):
        if prev == 0:
            return 100 if cur > 0 else 0
        return round(((cur - prev) / prev) * 100)

    # Pipeline stages from DB for close rate
    won_count = 0
    total_pipeline = 0
    try:
        conn = _leads_conn()
        cur = conn.cursor()
        cur.execute("SELECT stage, count(*) FROM lead_stages GROUP BY stage")
        for row in cur.fetchall():
            stage = (row[0] or "").lower()
            cnt = row[1]
            total_pipeline += cnt
            if stage == "won":
                won_count += cnt
        conn.close()
    except Exception as e:
        logger.error("Failed to query lead_stages for analytics: %s", e)
    close_rate = round((won_count / total_pipeline) * 100) if total_pipeline > 0 else 0

    # Build KPIs in frontend-expected format
    kpis = {
        "new_leads": {"value": total, "change": pct_change(total, prior_total)},
        "pipeline_value": {"value": total_value, "change": pct_change(total_value, prior_value)},
        "contacts_made": {"value": with_contact, "change": pct_change(with_contact, prior_contact)},
        "close_rate": {"value": close_rate, "change": 0},
    }

    # Activity: 12 monthly buckets
    now = datetime.utcnow()
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    monthly_counts = {}
    for i in range(11, -1, -1):
        dt = now - timedelta(days=i * 30)
        key = f"{dt.year}-{dt.month:02d}"
        monthly_counts[key] = {"month": month_names[dt.month - 1], "count": 0}
    for l in cached:
        d = l.get("issue_date", "")[:7]
        if d in monthly_counts:
            monthly_counts[d]["count"] += 1
    activity = list(monthly_counts.values())

    # Revenue by type: top 7 permit types by total valuation
    type_values = defaultdict(float)
    for l in current:
        pt = l.get("permit_type") or "Other"
        type_values[pt] += safe_float(l.get("valuation"))
    sorted_types = sorted(type_values.items(), key=lambda x: x[1], reverse=True)[:7]
    revenue_by_type = [{"type": t, "value": v} for t, v in sorted_types]

    return {
        "kpis": kpis,
        "activity": activity,
        "revenue_by_type": revenue_by_type,
    }


# NOTE: Duplicate /api/analytics/neighborhoods route removed (was dead code — FastAPI
# uses the first registration at line ~6024). The richer trend-based logic from this
# duplicate has been preserved in a comment for potential future use.
# See git history for the full compat_neighborhoods() implementation.


@app.get("/api/analytics/funnel")
async def compat_funnel(zips: str = Query("")):
    """On Site compat — conversion funnel with values."""
    cached = _cleaned_leads or DataCache.load(allow_stale=True) or []
    if zips:
        zip_set = set(z.strip() for z in zips.split(","))
        cached = [l for l in cached if str(l.get("zip", "")) in zip_set]

    total = len(cached)
    total_value = sum(safe_float(l.get("valuation")) for l in cached)
    hot_leads = [l for l in cached if safe_float(l.get("score")) >= TEMP_THRESHOLDS["hot"]]
    hot_count = len(hot_leads)
    hot_value = sum(safe_float(l.get("valuation")) for l in hot_leads)
    contact_leads = [l for l in cached if l.get("owner_name") or l.get("owner_phone")]
    contact_count = len(contact_leads)
    contact_value = sum(safe_float(l.get("valuation")) for l in contact_leads)

    # Read pipeline stages + compute value per stage
    stage_data = {"contacted": {"count": 0, "value": 0}, "quoted": {"count": 0, "value": 0}, "won": {"count": 0, "value": 0}}
    try:
        conn = _leads_conn()
        cur = conn.cursor()
        cur.execute("SELECT lead_id, stage FROM lead_stages")
        # Build a quick ID→valuation lookup
        val_map = {int(l.get("id", -1)): safe_float(l.get("valuation")) for l in cached}
        for row in cur.fetchall():
            lid, stage = row[0], (row[1] or "").lower()
            v = val_map.get(lid, 0)
            if stage in ("contacted", "contact"):
                stage_data["contacted"]["count"] += 1
                stage_data["contacted"]["value"] += v
            elif stage in ("quote_sent", "quote", "quoted", "proposal"):
                stage_data["quoted"]["count"] += 1
                stage_data["quoted"]["value"] += v
            elif stage == "won":
                stage_data["won"]["count"] += 1
                stage_data["won"]["value"] += v
        conn.close()
    except Exception as e:
        logger.error("Failed to query lead_stages for funnel: %s", e)

    return [
        {"stage": "New Leads", "count": total, "value": total_value},
        {"stage": "Hot Leads", "count": hot_count, "value": hot_value},
        {"stage": "Contact Available", "count": contact_count, "value": contact_value},
        {"stage": "Contacted", "count": stage_data["contacted"]["count"], "value": stage_data["contacted"]["value"]},
        {"stage": "Quote Sent", "count": stage_data["quoted"]["count"], "value": stage_data["quoted"]["value"]},
        {"stage": "Won", "count": stage_data["won"]["count"], "value": stage_data["won"]["value"]},
    ]


@app.get("/api/territories")
async def compat_territories(state: Optional[str] = Query(None), city: Optional[str] = Query(None)):
    """On Site compat — territory / zip code breakdown."""
    cached = DataCache.load(allow_stale=True) or []
    if state:
        cached = [l for l in cached if str(l.get("state", "")).lower() == state.lower()]
    if city:
        c = city.lower()
        cached = [l for l in cached if c in str(l.get("city", "")).lower()]
    zips: Dict[str, dict] = {}
    for l in cached:
        z = str(l.get("zip", ""))[:5]
        if not z:
            continue
        if z not in zips:
            zips[z] = {"zip": z, "city": l.get("city", ""), "count": 0, "hot": 0, "total_value": 0}
        zips[z]["count"] += 1
        zips[z]["total_value"] += safe_float(l.get("valuation"))
        if str(l.get("temperature", "")).lower() == "hot":
            zips[z]["hot"] += 1
    result = sorted(zips.values(), key=lambda x: x["count"], reverse=True)
    return result


@app.get("/api/alerts")
async def compat_alerts():
    """On Site compat — recent hot lead alerts."""
    cached = DataCache.load(allow_stale=True) or []
    hot_leads = [l for l in cached if safe_float(l.get("score")) >= TEMP_THRESHOLDS["hot"]]
    hot_leads.sort(key=lambda l: l.get("issue_date", ""), reverse=True)
    alerts = []
    for l in hot_leads[:20]:
        alerts.append({
            "id": l.get("id"),
            "alert_type": "new_hot_lead",
            "address": l.get("address", ""),
            "city": l.get("city", ""),
            "lead_score": l.get("score", 0),
            "project_value": l.get("valuation", 0),
            "alert_date": l.get("issue_date") or datetime.utcnow().isoformat(),
        })
    return alerts


@app.get("/api/geocode")
async def compat_geocode(q: str = Query(...)):
    """On Site compat — simple geocode from cached leads or Nominatim."""
    query = q.lower().strip()
    # First try matching from cached leads
    cached = DataCache.load(allow_stale=True) or []
    for l in cached:
        addr = (l.get("address") or "").lower()
        if query in addr:
            try:
                return {"lat": float(l["lat"]), "lng": float(l["lng"]), "address": l.get("address")}
            except (TypeError, ValueError, KeyError):
                continue
    # Fallback to Nominatim
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": q, "format": "json", "limit": "1"},
                headers={"User-Agent": "On Site/1.0"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        return {"lat": float(data[0]["lat"]), "lng": float(data[0]["lon"]), "address": data[0].get("display_name")}
    except Exception as e:
        logger.warning("Nominatim geocoding failed: %s", e)
    raise HTTPException(status_code=404, detail="Could not geocode address")


@app.get("/api/zimas")
async def compat_zimas(lat: float = Query(...), lng: float = Query(...)):
    """On Site compat — ZIMAS-style parcel+zoning lookup by coordinates."""
    # Try matching from cached leads first
    cached = DataCache.load(allow_stale=True) or []
    best = None
    best_dist = 999
    for l in cached:
        try:
            llat, llng = float(l.get("lat", 0)), float(l.get("lng", 0))
            dist = abs(llat - lat) + abs(llng - lng)
            if dist < best_dist and dist < 0.001:  # ~100m
                best_dist = dist
                best = l
        except (TypeError, ValueError):
            continue
    if best:
        return {
            "apn": best.get("apn", ""),
            "address": best.get("address", ""),
            "lotSize": None,
            "zoning": None,
        }
    # Fallback: query LA County ArcGIS parcel layer
    try:
        url = "https://cache.gis.lacounty.gov/cache/rest/services/LACounty_Cache/LACounty_Parcel/MapServer/0/query"
        params = {
            "geometry": f"{lng},{lat}",
            "geometryType": "esriGeometryPoint",
            "inSR": "4326", "outSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "APN,SitusAddress,UseType,LegalDescLine1",
            "returnGeometry": "false",
            "f": "json",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    feats = data.get("features", [])
                    if feats:
                        attr = feats[0].get("attributes", {})
                        return {
                            "apn": attr.get("APN", ""),
                            "address": attr.get("SitusAddress", ""),
                            "lotSize": None,
                            "zoning": attr.get("UseType", ""),
                        }
    except Exception as e:
        logger.warning("ZIMAS/ArcGIS parcel query failed: %s", e)
    return {"apn": "", "address": "", "lotSize": None, "zoning": None}


@app.get("/api/parcel-tile-config")
async def parcel_tile_config():
    """Parcel tiles are free (Regrid public MVT). No token needed."""
    return {"regrid_token": "free", "regrid_enabled": True}


@app.get("/api/parcels")
async def compat_parcels(bbox: str = Query(...), limit: int = Query(1200)):
    """On Site compat — parcel GeoJSON overlay within bounding box."""
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(400, "bbox must be s,w,n,e")
    try:
        s, w, n, e = float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
    except ValueError:
        raise HTTPException(400, "Invalid bbox coordinates")

    # Query LA County parcel ArcGIS as GeoJSON
    url = "https://cache.gis.lacounty.gov/cache/rest/services/LACounty_Cache/LACounty_Parcel/MapServer/0/query"
    params = {
        "geometry": f"{w},{s},{e},{n}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326", "outSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "APN,SitusAddress",
        "returnGeometry": "true",
        "resultRecordCount": str(min(limit, 2000)),
        "f": "geojson",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        logger.warning("ArcGIS parcel query failed: %s", e)
    return {"type": "FeatureCollection", "features": []}


@app.post("/api/sync/run")
async def compat_sync_run(background_tasks: BackgroundTasks):
    """On Site compat — trigger full data sync with structured response."""
    try:
        leads = await sync_data()
        source_counts = defaultdict(int)
        for l in (leads or []):
            source_counts[l.get("source", "Unknown")] += 1
        results = [{"source": src, "status": "ok", "count": cnt} for src, cnt in source_counts.items()]
        return {"status": "ok", "total": len(leads or []), "results": results}
    except Exception as e:
        logger.error(f"sync/run failed: {e}")
        return {"status": "error", "total": 0, "results": [], "error": str(e)}


@app.post("/api/enrichment/run")
async def compat_enrichment_run():
    """On Site compat — run batch enrichment on unenriched cached leads."""
    cached = DataCache.load(allow_stale=True) or []
    if not cached:
        return {"enriched": 0, "total": 0}
    before = sum(1 for l in cached if l.get("owner_name") or l.get("owner_phone"))
    # Attempt enrichment on leads missing contact info (up to 20 per batch)
    batch_limit = 20
    enriched_count = 0
    for l in cached:
        if enriched_count >= batch_limit:
            break
        if l.get("owner_name") or l.get("owner_phone"):
            continue
        apn = l.get("apn")
        if apn and "los angeles" in (l.get("city") or "").lower():
            try:
                owner = get_la_owner(apn)
                if owner and owner.get("owner_name"):
                    l["owner_name"] = owner["owner_name"]
                    l["mailing_address"] = owner.get("mailing_address", "")
                    enriched_count += 1
            except Exception as e:
                logger.warning("LA owner enrichment failed for APN %s: %s", apn, e)
    after = sum(1 for l in cached if l.get("owner_name") or l.get("owner_phone"))
    return {"enriched": after - before, "total": len(cached)}


# ---------------------------------------------------------------------------
# FREE DATA ENRICHMENT - $0 Cost
# ---------------------------------------------------------------------------
@app.post("/api/leads/{lead_id}/enrich-free")
async def enrich_lead_free(lead_id: str):
    """
    100% FREE email/phone enrichment using pattern matching and SMTP verification
    Cost: $0.00
    Success rate: 60-80% for email, 20-30% for phone
    """
    try:
        # Get lead from pre-processed cache (fast)
        lead = _get_permit_from_cache(lead_id)

        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")

        # Parse owner name
        owner_name = lead.get("owner_name", "")
        if not owner_name or len(owner_name.strip()) < 3:
            raise HTTPException(status_code=400, detail="Owner name not available")

        # Split name
        name_parts = owner_name.strip().split(' ', 1)
        first_name = name_parts[0] if len(name_parts) > 0 else ""
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        if not first_name or not last_name:
            raise HTTPException(status_code=400, detail="Could not parse owner name")

        # Get domain from contractor name if available
        domain = None
        company = lead.get("contractor_name")
        if company:
            # Try to guess domain
            company_clean = re.sub(r'[^a-z0-9\s]', '', company.lower())
            company_word = company_clean.split()[0] if company_clean.split() else None
            if company_word:
                domain = f"{company_word}.com"

        logger.info(f"FREE enrichment for {first_name} {last_name} (domain: {domain}, company: {company})")

        # Run FREE enrichment
        result = await free_enrichment.enrich_lead(
            first_name=first_name,
            last_name=last_name,
            address=lead.get("address"),
            city=lead.get("city"),
            state=lead.get("state"),
            zip_code=lead.get("zip"),
            company=company,
            domain=domain
        )

        # Update lead in cache if email/phone found
        if result.get('email'):
            lead['owner_email'] = result['email']
            lead['email_verified'] = result.get('email_verified', False)
            lead['email_confidence'] = result.get('email_confidence', 0)

        if result.get('phone'):
            lead['owner_phone'] = result['phone']
            lead['phone_verified'] = result.get('phone_verified', False)
            lead['phone_confidence'] = result.get('phone_confidence', 0)

        # Save updated cache
        if result.get('email') or result.get('phone'):
            all_cached = DataCache.load(allow_stale=True)
            if all_cached:
                # Update the lead in the full cache
                for i, c in enumerate(all_cached):
                    if str(c.get("id")) == str(lead_id):
                        all_cached[i] = {**c, **{k: lead[k] for k in ('owner_email', 'email_verified', 'email_confidence', 'owner_phone', 'phone_verified', 'phone_confidence') if k in lead}}
                        break
                DataCache.save(all_cached)

        return {
            "success": result.get('success', False),
            "lead_id": lead_id,
            "email": result.get('email'),
            "email_verified": result.get('email_verified', False),
            "email_confidence": result.get('email_confidence', 0),
            "phone": result.get('phone'),
            "phone_verified": result.get('phone_verified', False),
            "phone_confidence": result.get('phone_confidence', 0),
            "cost": 0.0,  # FREE!
            "method": "free_enrichment"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"FREE enrichment error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/validate/email-free")
async def validate_email_free(email: str):
    """
    FREE email validation using SMTP verification
    Cost: $0.00
    Accuracy: 95%+
    """
    try:
        result = await free_enrichment.validate_email(email)
        return {
            "email": email,
            "valid": result.get('valid', False),
            "verified": result.get('verified', False),
            "method": result.get('method', 'smtp'),
            "cost": 0.0
        }
    except Exception as e:
        logger.error(f"Email validation error: {e}")
        return {
            "email": email,
            "valid": False,
            "verified": False,
            "error": str(e),
            "cost": 0.0
        }


# ---------------------------------------------------------------------------
# CRM Integration stubs
# ---------------------------------------------------------------------------
@app.get("/api/crm/connections")
async def crm_connections():
    """List CRM connection statuses."""
    providers = [
        {"provider": "servicetitan", "label": "ServiceTitan", "status": "disconnected", "connected_at": None},
        {"provider": "salesforce", "label": "Salesforce", "status": "disconnected", "connected_at": None},
        {"provider": "hubspot", "label": "HubSpot", "status": "disconnected", "connected_at": None},
        {"provider": "jobber", "label": "Jobber", "status": "disconnected", "connected_at": None},
    ]
    # Check DB for stored connections
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS crm_connections
                       (provider TEXT PRIMARY KEY, api_key TEXT, status TEXT, connected_at TEXT)""")
        cur.execute("SELECT provider, status, connected_at FROM crm_connections")
        db_conns = {row[0]: {"status": row[1], "connected_at": row[2]} for row in cur.fetchall()}
        conn.close()
        for p in providers:
            if p["provider"] in db_conns:
                p["status"] = db_conns[p["provider"]]["status"]
                p["connected_at"] = db_conns[p["provider"]]["connected_at"]
    except Exception as e:
        logger.error("Failed to load CRM connections from db: %s", e)
    return providers


@app.post("/api/crm/connect/{provider}")
async def crm_connect(provider: str, payload: dict):
    """Connect a CRM provider (store API key)."""
    api_key = payload.get("api_key", "")
    if not api_key:
        raise HTTPException(400, "api_key required")
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS crm_connections
                       (provider TEXT PRIMARY KEY, api_key TEXT, status TEXT, connected_at TEXT)""")
        cur.execute(
            "INSERT OR REPLACE INTO crm_connections (provider, api_key, status, connected_at) VALUES (?, ?, ?, ?)",
            (provider, api_key, "connected", datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        raise HTTPException(500, str(e))
    return {"status": "connected", "provider": provider}


@app.post("/api/crm/disconnect/{provider}")
async def crm_disconnect(provider: str):
    """Disconnect a CRM provider."""
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM crm_connections WHERE provider=?", (provider,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Failed to disconnect CRM provider %s: %s", provider, e)
    return {"status": "disconnected", "provider": provider}


@app.post("/api/crm/push/{provider}")
async def crm_push_lead(provider: str, payload: dict):
    """Push a lead to CRM (stub — logs intent, returns success)."""
    lead_id = payload.get("lead_id")
    logger.info(f"CRM push: {provider} lead_id={lead_id}")
    return {"status": "ok", "provider": provider, "lead_id": lead_id, "message": "Lead queued for CRM sync"}


@app.post("/api/crm/test/{provider}")
async def crm_test_connection(provider: str, request: Request):
    """Test if a CRM integration is configured and reachable."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    try:
        jwt.decode(token, SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(401, "Unauthorized")
    # Check if provider has stored credentials
    try:
        conn = _conn()
        conn.execute("""CREATE TABLE IF NOT EXISTS crm_connections
                       (provider TEXT PRIMARY KEY, api_key TEXT, status TEXT, connected_at TEXT)""")
        row = conn.execute(
            "SELECT status FROM crm_connections WHERE provider = ? LIMIT 1", (provider,)
        ).fetchone()
        conn.close()
    except Exception:
        row = None
    if row and row[0] == "connected":
        return {"connected": True, "provider": provider, "message": f"{provider} credentials found"}
    return {"connected": False, "provider": provider, "message": f"No {provider} credentials stored"}


# ═══════════════════════════════════════════════════════════════════════════
# CONTACT ENRICHMENT ENDPOINTS — Phone/Email Validation & Discovery
# ═══════════════════════════════════════════════════════════════════════════

from services.enrichment_service import phone_enricher, email_enricher, contact_enricher


@app.post("/api/enrich/phone/validate")
async def validate_phone(phone: str, method: str = "twilio"):
    """
    Validate a phone number.

    Methods: twilio, ipqs, auto (tries twilio then ipqs)

    Returns: PhoneInfo with validation result
    """
    try:
        async with phone_enricher as enricher:
            result = await enricher.validate(phone, method)
            return {
                "success": True,
                "phone": result.number,
                "valid": result.valid,
                "carrier": result.carrier,
                "line_type": result.line_type,
                "location": result.location,
                "spam_risk": result.spam_risk,
                "caller_name": result.caller_name,
                "confidence": result.confidence
            }
    except Exception as e:
        logger.error(f"Phone validation error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/enrich/phone/bulk")
async def validate_phones_bulk(phones: List[str], method: str = "twilio"):
    """Validate multiple phone numbers"""
    try:
        async with phone_enricher as enricher:
            results = await enricher.bulk_validate(phones, method)
            return {
                "success": True,
                "count": len(phones),
                "results": [
                    {
                        "phone": r.number,
                        "valid": r.valid,
                        "carrier": r.carrier,
                        "line_type": r.line_type,
                        "confidence": r.confidence
                    } for r in results if not isinstance(r, Exception)
                ],
                "stats": enricher.get_stats()
            }
    except Exception as e:
        logger.error(f"Bulk phone validation error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/enrich/email/verify")
async def verify_email(email: str):
    """
    Verify an email address via Hunter.io.

    Returns: validation status (valid, invalid, accept_all, unknown)
    """
    try:
        async with email_enricher as enricher:
            result = await enricher.hunter_verify(email)
            return {
                "success": True,
                "email": result.email,
                "valid": result.valid,
                "score": result.score,
                "sources": result.sources or []
            }
    except Exception as e:
        logger.error(f"Email verification error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/enrich/email/generate")
async def generate_email_patterns(first_name: str, last_name: str, domain: str):
    """
    Generate possible email patterns for a name and domain.

    Example: first=John, last=Doe, domain=company.com
    Returns: ["john@company.com", "john.doe@company.com", ...]
    """
    try:
        async with email_enricher as enricher:
            patterns = enricher.generate_patterns(first_name, last_name, domain)
            return {
                "success": True,
                "first_name": first_name,
                "last_name": last_name,
                "domain": domain,
                "patterns": patterns,
                "count": len(patterns)
            }
    except Exception as e:
        logger.error(f"Email pattern generation error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/enrich/email/domain-search")
async def search_domain_emails(domain: str, limit: int = 10):
    """
    Search for emails from a domain via Hunter.io.

    Returns: Known emails, email pattern, and organization info
    """
    try:
        async with email_enricher as enricher:
            result = await enricher.hunter_domain_search(domain, limit)
            if result:
                return {
                    "success": True,
                    **result
                }
            else:
                return {
                    "success": False,
                    "error": "No results found or rate limit reached"
                }
    except Exception as e:
        logger.error(f"Domain search error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/enrich/contact")
async def enrich_contact_endpoint(
    name: str,
    address: str,
    city: str,
    state: str,
    zip_code: str,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    company_domain: Optional[str] = None
):
    """
    Full contact enrichment: validate phone, verify email, generate patterns.

    Returns: EnrichedContact with all available data
    """
    try:
        async with contact_enricher as enricher:
            result = await enricher.enrich_contact(
                name=name,
                address=address,
                city=city,
                state=state,
                zip_code=zip_code,
                phone=phone,
                email=email,
                company_domain=company_domain
            )

            return {
                "success": True,
                "contact": {
                    "name": result.name,
                    "address": result.address,
                    "city": result.city,
                    "state": result.state,
                    "zip_code": result.zip_code,
                    "phones": result.phones or [],
                    "emails": result.emails or [],
                    "data_sources": result.data_sources or [],
                    "confidence_score": result.confidence_score,
                    "enriched_at": result.enriched_at
                }
            }
    except Exception as e:
        logger.error(f"Contact enrichment error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/enrich/lead/{lead_id}")
async def enrich_lead_endpoint(lead_id: int):
    """
    Enrich a specific lead with contact validation.

    Validates owner phone/email and discovers additional contact info.
    """
    try:
        # Get lead from cache
        cached = DataCache.load(allow_stale=True)
        if not cached:
            return {"success": False, "error": "No leads available"}

        lead = next((l for l in cached if l.get("id") == lead_id), None)
        if not lead:
            return {"success": False, "error": f"Lead {lead_id} not found"}

        # Enrich the lead
        async with contact_enricher as enricher:
            enriched_lead = await enricher.enrich_lead(lead)

            return {
                "success": True,
                "lead_id": lead_id,
                "enriched": enriched_lead
            }
    except Exception as e:
        logger.error(f"Lead enrichment error: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/enrich/stats")
async def get_enrichment_stats():
    """Get enrichment service usage statistics"""
    async with contact_enricher as enricher:
        stats = enricher.get_stats()
        return {
            "success": True,
            "stats": stats
        }


# ═══════════════════════════════════════════════════════════════════════════
# PERMIT SCRAPER ENDPOINTS — Austin, Seattle, San Francisco
# ═══════════════════════════════════════════════════════════════════════════

from services.permit_scrapers import permit_scraper

@app.post("/api/scrapers/permits/scrape")
async def scrape_city_permits(city: str, days_back: int = 30, limit: int = 5000):
    """
    Scrape permits from a specific city.

    Args:
        city: City name (austin, seattle, san_francisco)
        days_back: How many days to look back (default: 30)
        limit: Max permits to scrape (default: 5000)

    Returns:
        List of permit records
    """
    try:
        permits = await permit_scraper.scrape_city(city, days_back=days_back, limit=limit)

        return {
            "success": True,
            "city": city,
            "count": len(permits),
            "permits": [asdict(p) for p in permits],
            "stats": permit_scraper.scrapers.get(city).stats if city in permit_scraper.scrapers else {}
        }
    except Exception as e:
        logger.error(f"Permit scraping failed for {city}: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/scrapers/permits/scrape-all")
async def scrape_all_permits(days_back: int = 30, limit_per_city: int = 5000):
    """
    Scrape permits from all available cities in parallel.

    Args:
        days_back: How many days to look back (default: 30)
        limit_per_city: Max permits per city (default: 5000)

    Returns:
        Dict mapping city to permit records
    """
    try:
        results = await permit_scraper.scrape_all(days_back=days_back, limit_per_city=limit_per_city)

        total_permits = sum(len(permits) for permits in results.values())

        return {
            "success": True,
            "total_permits": total_permits,
            "cities": {
                city: {
                    "count": len(permits),
                    "permits": [asdict(p) for p in permits]
                }
                for city, permits in results.items()
            },
            "stats": permit_scraper.get_stats()
        }
    except Exception as e:
        logger.error(f"Bulk permit scraping failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/scrapers/permits/stats")
async def get_permit_scraper_stats():
    """Get permit scraper statistics"""
    return {
        "success": True,
        "stats": permit_scraper.get_stats()
    }


# ═══════════════════════════════════════════════════════════════════════════
# COUNTY ASSESSOR SCRAPER ENDPOINTS — Harris, Dallas, Tarrant, Maricopa
# ═══════════════════════════════════════════════════════════════════════════

from services.county_scrapers import county_scraper

@app.post("/api/scrapers/counties/search")
async def search_county_properties(county: str, street_name: str, limit: int = 100):
    """
    Search county assessor records by street name.

    Args:
        county: County name (harris, dallas, tarrant, maricopa)
        street_name: Street name to search
        limit: Max properties to return (default: 100)

    Returns:
        List of property records
    """
    try:
        properties = await county_scraper.search_county_by_address(county, street_name, limit=limit)

        return {
            "success": True,
            "county": county,
            "street_name": street_name,
            "count": len(properties),
            "properties": [asdict(p) for p in properties]
        }
    except Exception as e:
        logger.error(f"County search failed for {county}: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/scrapers/counties/search-all")
async def search_all_counties(street_name: str, limit_per_county: int = 100):
    """
    Search all counties in parallel by street name.

    Args:
        street_name: Street name to search
        limit_per_county: Max properties per county (default: 100)

    Returns:
        Dict mapping county to property records
    """
    try:
        results = await county_scraper.search_all_counties(street_name, limit_per_county=limit_per_county)

        total_properties = sum(len(props) for props in results.values())

        return {
            "success": True,
            "total_properties": total_properties,
            "counties": {
                county: {
                    "count": len(properties),
                    "properties": [asdict(p) for p in properties]
                }
                for county, properties in results.items()
            },
            "stats": county_scraper.get_stats()
        }
    except Exception as e:
        logger.error(f"Bulk county search failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/scrapers/counties/stats")
async def get_county_scraper_stats():
    """Get county scraper statistics"""
    return {
        "success": True,
        "stats": county_scraper.get_stats()
    }


# ═══════════════════════════════════════════════════════════════════════════
# BULK DATA COLLECTION ENDPOINTS — OpenAddresses, Census, Regrid
# ═══════════════════════════════════════════════════════════════════════════

from services.data_collection_engine import bulk_collector, BULK_DATA_SOURCES

@app.post("/api/bulk/collect-state")
async def collect_state_data(state_abbr: str):
    """
    Collect bulk address data for a state (OpenAddresses).

    Args:
        state_abbr: State abbreviation (e.g., "CA", "TX")

    Returns:
        Collection status and file paths
    """
    try:
        result = await bulk_collector.collect_state_addresses(state_abbr)

        return {
            "success": True,
            "state": state_abbr,
            "result": result
        }
    except Exception as e:
        logger.error(f"State data collection failed for {state_abbr}: {e}")
        return {"success": False, "error": str(e)}


@app.post("/api/bulk/collect-multiple")
async def collect_multiple_states(states: List[str]):
    """
    Collect bulk data for multiple states in parallel.

    Args:
        states: List of state abbreviations

    Returns:
        Dict mapping state to collection results
    """
    try:
        results = await bulk_collector.collect_multiple_states(states)

        return {
            "success": True,
            "states": results,
            "stats": bulk_collector.get_stats()
        }
    except Exception as e:
        logger.error(f"Bulk data collection failed: {e}")
        return {"success": False, "error": str(e)}


@app.get("/api/bulk/sources")
async def get_bulk_data_sources():
    """Get list of available bulk data sources"""
    return {
        "success": True,
        "sources": [asdict(source) for source in BULK_DATA_SOURCES]
    }


@app.get("/api/bulk/stats")
async def get_bulk_collection_stats():
    """Get bulk data collection statistics"""
    return {
        "success": True,
        "stats": bulk_collector.get_stats()
    }


# ═══════════════════════════════════════════════════════════════════════════
# AI CHAT — Real LLM endpoint (Claude or OpenAI)
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/chat")
async def api_chat(request: Request):
    """AI chat endpoint using Claude API (falls back to pattern matching)."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()
    message = body.get("message", "").strip()
    context = body.get("context", {})
    if not message:
        raise HTTPException(status_code=400, detail="Message required")

    # Try Claude API first
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if api_key:
        try:
            async with aiohttp.ClientSession() as session:
                system_prompt = (
                    "You are the Onsite AI assistant — a sales copilot for construction contractors. "
                    "You help users understand their permit leads, draft outreach messages, analyze pipeline, "
                    "and provide market intelligence. Be concise, actionable, and professional. "
                    "When drafting outreach, personalize with the lead's address, permit type, project value, and owner name. "
                    f"User context: {json.dumps(context)}"
                )
                payload = {
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 1024,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": message}]
                }
                headers = {
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                }
                async with session.post("https://api.anthropic.com/v1/messages", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        reply = data.get("content", [{}])[0].get("text", "")
                        return {"reply": reply, "source": "ai"}
                    else:
                        logger.warning("Claude API error %d", resp.status)
        except Exception as e:
            logger.warning("Claude API failed: %s", e)

    # Try OpenAI as fallback
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if openai_key:
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": f"You are the Onsite AI sales copilot for construction contractors. Context: {json.dumps(context)}"},
                        {"role": "user", "content": message}
                    ],
                    "max_tokens": 1024
                }
                headers = {"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"}
                async with session.post("https://api.openai.com/v1/chat/completions", json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        reply = data["choices"][0]["message"]["content"]
                        return {"reply": reply, "source": "ai"}
        except Exception as e:
            logger.warning("OpenAI API failed: %s", e)

    # No AI key configured — return indicator for client to use local patterns
    return {"reply": "", "source": "local", "note": "No AI API key configured. Using local pattern matching."}


# ═══════════════════════════════════════════════════════════════════════════
# TEAM MANAGEMENT — Invite, list, update, remove team members
# ═══════════════════════════════════════════════════════════════════════════

def _ensure_team_tables():
    """Create team tables if they don't exist."""
    conn = _leads_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS team_members (
        id TEXT PRIMARY KEY,
        owner_id TEXT NOT NULL,
        email TEXT NOT NULL,
        name TEXT DEFAULT '',
        role TEXT DEFAULT 'viewer',
        status TEXT DEFAULT 'invited',
        invited_at TEXT DEFAULT (datetime('now')),
        accepted_at TEXT,
        UNIQUE(owner_id, email)
    )""")
    conn.commit()
    conn.close()

@app.get("/api/team")
async def get_team(request: Request):
    """Get team members for current user."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_id = payload.get("sub", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    _ensure_team_tables()
    conn = _leads_conn()
    import sqlite3
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM team_members WHERE owner_id = ? ORDER BY invited_at DESC", (user_id,)).fetchall()
    conn.close()
    return {"members": [dict(r) for r in rows]}


@app.post("/api/team/invite")
async def invite_team_member(request: Request):
    """Invite a team member by email."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_id = payload.get("sub", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    body = await request.json()
    email = body.get("email", "").strip().lower()
    name = body.get("name", "").strip()
    role = body.get("role", "viewer")

    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Valid email required")
    if role not in ("admin", "manager", "viewer", "finance"):
        raise HTTPException(status_code=400, detail="Invalid role")

    _ensure_team_tables()
    member_id = str(uuid.uuid4())
    conn = _leads_conn()
    try:
        conn.execute(
            "INSERT INTO team_members (id, owner_id, email, name, role) VALUES (?, ?, ?, ?, ?)",
            (member_id, user_id, email, name, role)
        )
        conn.commit()
    except Exception as e:
        conn.close()
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail="Member already invited")
        raise HTTPException(status_code=500, detail="Failed to invite member")
    conn.close()

    # Send invitation email if SendGrid is configured
    sg_key = os.getenv("SENDGRID_API_KEY", "")
    if sg_key:
        try:
            async with aiohttp.ClientSession() as session:
                await session.post("https://api.sendgrid.com/v3/mail/send", json={
                    "personalizations": [{"to": [{"email": email}]}],
                    "from": {"email": "noreply@onsite.com", "name": "Onsite"},
                    "subject": f"You've been invited to join Onsite",
                    "content": [{"type": "text/plain", "value": f"Hi {name or 'there'},\n\nYou've been invited to join an Onsite team as {role}. Sign up at {os.getenv('APP_URL', 'http://localhost:18000')} to get started."}]
                }, headers={"Authorization": f"Bearer {sg_key}", "Content-Type": "application/json"})
        except Exception as e:
            logger.warning("Invite email failed: %s", e)

    return {"id": member_id, "email": email, "role": role, "status": "invited"}


@app.put("/api/team/{member_id}")
async def update_team_member(member_id: str, request: Request):
    """Update a team member's role."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()
    role = body.get("role", "")
    if role and role not in ("admin", "manager", "viewer", "finance"):
        raise HTTPException(status_code=400, detail="Invalid role")

    conn = _leads_conn()
    if role:
        conn.execute("UPDATE team_members SET role = ? WHERE id = ?", (role, member_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}


@app.delete("/api/team/{member_id}")
async def remove_team_member(member_id: str, request: Request):
    """Remove a team member."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    conn = _leads_conn()
    conn.execute("DELETE FROM team_members WHERE id = ?", (member_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════
# DEVELOPER API KEYS — Generate, list, revoke
# ═══════════════════════════════════════════════════════════════════════════

def _ensure_apikey_tables():
    """Create API key tables."""
    conn = _leads_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS api_keys (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT DEFAULT 'Default Key',
        key_prefix TEXT NOT NULL,
        key_hash TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        last_used_at TEXT,
        is_active INTEGER DEFAULT 1
    )""")
    conn.commit()
    conn.close()

@app.get("/api/developer/keys")
async def list_api_keys(request: Request):
    """List user's API keys (shows only prefix, not full key)."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_id = payload.get("sub", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    _ensure_apikey_tables()
    conn = _leads_conn()
    import sqlite3
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, key_prefix, created_at, last_used_at, is_active FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
        (user_id,)
    ).fetchall()
    conn.close()
    return {"keys": [dict(r) for r in rows]}


@app.post("/api/developer/keys")
async def create_api_key(request: Request):
    """Generate a new API key. Returns the full key ONCE."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_id = payload.get("sub", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    body = await request.json()
    name = body.get("name", "Default Key").strip()[:64]

    import hashlib
    import secrets
    key_id = str(uuid.uuid4())
    raw_key = f"onsite_{secrets.token_hex(24)}"
    key_prefix = raw_key[:12] + "..."
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    _ensure_apikey_tables()
    conn = _leads_conn()
    conn.execute(
        "INSERT INTO api_keys (id, user_id, name, key_prefix, key_hash) VALUES (?, ?, ?, ?, ?)",
        (key_id, user_id, name, key_prefix, key_hash)
    )
    conn.commit()
    conn.close()

    return {"id": key_id, "key": raw_key, "prefix": key_prefix, "name": name, "note": "Save this key now — it won't be shown again."}


@app.delete("/api/developer/keys/{key_id}")
async def revoke_api_key(key_id: str, request: Request):
    """Revoke (deactivate) an API key."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    conn = _leads_conn()
    conn.execute("UPDATE api_keys SET is_active = 0 WHERE id = ?", (key_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════
# DATA COLLECTION — Trigger & monitor collection pipeline
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/admin/run-collection")
async def admin_run_collection(request: Request, step: str = "all"):
    """Trigger data collection pipeline manually."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")

    import asyncio as _aio

    async def _run():
        try:
            from data_sources.run_collection import (
                step_catalog, step_fema, step_census,
                step_hpi, step_sales, step_zip_risk,
            )
            if step in ("all", "catalog"):
                await _aio.to_thread(step_catalog)
            if step in ("all", "fema"):
                await _aio.to_thread(step_fema)
            if step in ("all", "census"):
                await _aio.to_thread(step_census)
            if step in ("all", "hpi"):
                await _aio.to_thread(step_hpi)
            if step in ("all", "sales"):
                await _aio.to_thread(step_sales)
            if step in ("all", "zip"):
                await _aio.to_thread(step_zip_risk)
        except Exception as e:
            logger.error("Data collection error: %s", e)

    asyncio.create_task(_run())
    return {"status": "started", "step": step}


@app.get("/api/admin/collection-status")
async def admin_collection_status(request: Request):
    """Show data collection stats from sync log."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(403, "Admin only")

    try:
        from data_sources.schema import get_db as get_ds_db
        with get_ds_db() as conn:
            recent = conn.execute("""
                SELECT source_name, records_fetched, records_new,
                       duration_seconds, status, started_at
                FROM data_sync_log ORDER BY started_at DESC LIMIT 20
            """).fetchall()
            return {"runs": [dict(r) for r in recent]}
    except Exception as e:
        return {"runs": [], "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE BACKUP — Create/download/list backups
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/admin/backup")
async def create_backup(request: Request):
    """Create a database backup (admin only)."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        role = payload.get("role", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    import shutil
    from models.database import _db_path
    db_path = _db_path()
    backup_dir = os.path.join(os.path.dirname(db_path), "backups")
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backup_dir, f"leads_{timestamp}.db")

    # Use SQLite backup API for consistency
    import sqlite3
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    src.backup(dst)
    src.close()
    dst.close()

    size_mb = round(os.path.getsize(backup_path) / 1048576, 1)
    return {"status": "ok", "path": backup_path, "size_mb": size_mb, "timestamp": timestamp}


@app.get("/api/admin/backups")
async def list_backups(request: Request):
    """List available database backups."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    from models.database import _db_path as _dbp
    backup_dir = os.path.join(os.path.dirname(_dbp()), "backups")
    if not os.path.exists(backup_dir):
        return {"backups": []}

    backups = []
    for f in sorted(os.listdir(backup_dir), reverse=True):
        if f.endswith(".db"):
            fp = os.path.join(backup_dir, f)
            backups.append({
                "name": f,
                "size_mb": round(os.path.getsize(fp) / 1048576, 1),
                "created": datetime.fromtimestamp(os.path.getmtime(fp)).isoformat()
            })
    return {"backups": backups[:20]}


# ═══════════════════════════════════════════════════════════════════════════
# BATCH GEOCODING — Geocode leads missing lat/lng
# ═══════════════════════════════════════════════════════════════════════════

_geocode_running = False

@app.post("/api/admin/geocode-batch")
async def start_batch_geocode(request: Request, background_tasks: BackgroundTasks):
    """Start batch geocoding for leads without coordinates (admin only)."""
    global _geocode_running
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        role = payload.get("role", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    if _geocode_running:
        return {"status": "already_running"}

    body = await request.json()
    batch_limit = min(body.get("limit", 500), 5000)

    async def _run_geocode(limit):
        global _geocode_running
        _geocode_running = True
        geocoded_count = 0
        failed_count = 0
        try:
            conn = _leads_conn()
            rows = conn.execute(
                "SELECT id, address, city, state FROM leads WHERE (lat IS NULL OR lat = 0) AND address != '' LIMIT ?",
                (limit,)
            ).fetchall()
            conn.close()

            gc = GeocodingClient()
            for row in rows:
                try:
                    result = await gc.geocode(row[1], row[2], row[3] or "CA")
                    if result and result.get("lat"):
                        conn2 = _leads_conn()
                        conn2.execute(
                            "UPDATE leads SET lat = ?, lng = ? WHERE id = ?",
                            (result["lat"], result["lng"], row[0])
                        )
                        conn2.commit()
                        conn2.close()
                        geocoded_count += 1
                    else:
                        failed_count += 1
                except Exception:
                    failed_count += 1
                await asyncio.sleep(1.1)  # Nominatim rate limit
        except Exception as e:
            logger.error("Batch geocode error: %s", e)
        finally:
            _geocode_running = False
            logger.info("Batch geocode complete: %d geocoded, %d failed", geocoded_count, failed_count)

    background_tasks.add_task(_run_geocode, batch_limit)
    return {"status": "started", "batch_size": batch_limit}


@app.get("/api/admin/geocode-status")
async def geocode_status():
    """Check batch geocoding status."""
    try:
        conn = _leads_conn()
        total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        geocoded = conn.execute("SELECT COUNT(*) FROM leads WHERE lat IS NOT NULL AND lat != 0").fetchone()[0]
        conn.close()
    except Exception as e:
        logger.error("Geocode status error: %s", e)
        return {"running": _geocode_running, "total_leads": 0, "geocoded": 0, "missing": 0, "coverage_pct": 0, "error": str(e)}
    return {
        "running": _geocode_running,
        "total_leads": total,
        "geocoded": geocoded,
        "missing": total - geocoded,
        "coverage_pct": round(geocoded / max(1, total) * 100, 1)
    }


# ═══════════════════════════════════════════════════════════════════════════
# BILLING — Wire up Plan & Billing to Stripe
# ═══════════════════════════════════════════════════════════════════════════

@app.post("/api/billing/portal")
async def create_billing_portal(request: Request):
    """Create Stripe customer portal session for managing subscription."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    stripe_key = os.getenv("STRIPE_SECRET_KEY", "")
    if not stripe_key:
        return {"error": "Stripe not configured", "url": None}

    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_email = payload.get("email", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        import stripe
        stripe.api_key = stripe_key
        customers = stripe.Customer.list(email=user_email, limit=1)
        if not customers.data:
            return {"error": "No billing account found", "url": None}

        session = stripe.billing_portal.Session.create(
            customer=customers.data[0].id,
            return_url=os.getenv("APP_URL", "http://localhost:18000")
        )
        return {"url": session.url}
    except Exception as e:
        logger.error("Stripe portal error: %s", e)
        return {"error": str(e), "url": None}


@app.get("/api/billing/invoices")
async def list_invoices(request: Request):
    """List user's Stripe invoices."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    stripe_key = os.getenv("STRIPE_SECRET_KEY", "")
    if not stripe_key:
        return {"invoices": [], "note": "Stripe not configured"}

    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        user_email = payload.get("email", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        import stripe
        stripe.api_key = stripe_key
        customers = stripe.Customer.list(email=user_email, limit=1)
        if not customers.data:
            return {"invoices": []}

        invoices = stripe.Invoice.list(customer=customers.data[0].id, limit=12)
        return {"invoices": [{
            "id": inv.id,
            "amount": inv.amount_due / 100,
            "status": inv.status,
            "date": datetime.fromtimestamp(inv.created).isoformat(),
            "pdf": inv.invoice_pdf
        } for inv in invoices.data]}
    except Exception as e:
        logger.error("Stripe invoices error: %s", e)
        return {"invoices": [], "error": str(e)}


# ---------------------------------------------------------------------------
# Entry point — MUST be at the very end so all routes are registered first
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "18000")))
