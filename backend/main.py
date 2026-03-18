"""
Onsite - Production Backend v3.0
Real API integrations, data syncing, geocoding
Modular architecture with SQLite, WebSocket, and auto-enrichment
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, ORJSONResponse, RedirectResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from dataclasses import asdict
from core.scoring import calculate_score as _score_from_core, calculate_score_v2 as _score_from_core_v2, classify_temperature, TEMP_THRESHOLDS
import math
import csv
import re
from pathlib import Path
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
import base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Removed: FreeLegalEnrichment (unused), enrich_pending_leads (unused), get_current_usage (unused), master_enrich_permit (unused)
from owner_enrichment import enrich_loop, get_la_owner  # noqa: E402
from property_reach_enrichment import skip_trace_property  # noqa: E402
from services.free_enrichment import free_enrichment  # noqa: E402
# Removed: batch_find_contacts (unused), orchestrator_enrich (unused)
from services.contact_scraper import find_contact  # noqa: E402
from services.enrichment_orchestrator import enrich_batch as orchestrator_batch  # noqa: E402
from yelp_intent_provider import ingest_yelp_intents, init_db, fetch_intent_leads  # noqa: E402
from marketplace_engine import init_market_tables, upsert_pricing, metrics, _conn  # noqa: E402
from license_service import login_license_check  # noqa: E402
from permit_filing_service import save_permit_request, generate_permit_payload, generate_prefilled_pdf_stub  # noqa: E402
from auth_guard import extract_user_id, verify_jwt, AuthError  # noqa: E402
# Removed: record_prompt (unused)
from prompt_scheduler import suggest_prompts  # noqa: E402
from insights_monthly import compute_insights  # noqa: E402
from permit_type_api import list_permit_types  # noqa: E402
from property_suggestions import fetch_suggestions  # noqa: E402
# Removed: PropertyReachRotator (unused)
from services.ownership import OwnershipLookupService  # noqa: E402
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

_settings_col_added = False

# ============================================================================
# EXTRACTED MODULES (split from main.py for maintainability)
# ============================================================================
from data_sources_config import (
    CENSUS_API_KEY, SOCRATA_APP_TOKEN, SOCRATA_LIMIT,
    JURISDICTIONS_REGISTRY, ENRICH_API_URL, ENRICH_API_KEY, ENRICH_MAX_LOOKUPS,
    PDL_API_KEY, PROPERTYREACH_API_KEY, PROPERTYREACH_ENABLED, PROPERTYREACH_INLINE,
    LA_ASSESSOR_INLINE, ATTOM_API_KEY, ATTOM_RATE_LIMIT,
    BASE_DIR, CACHE_FILE, CACHE_DURATION, LEADS_RETURN_LIMIT, MAX_DAYS_OLD,
    LEAD_VISIBILITY_DAYS, GEOCODE_BUDGET, YELP_INTENT_ENABLED, YELP_INTENT_INTERVAL,
    REFRESH_INTERVAL_SEC, SKIP_LA, SKIP_LB, ENRICH_INTERVAL_SEC, ENRICH_ENABLED,
    USE_MASTER_ENRICH, SOC_DATASETS, ARCGIS_DATASETS, CKAN_DATASETS, CARTO_DATASETS,
    STATE_DATASETS, FEDERAL_ENDPOINTS, COUNTY_ASSESSOR_ENDPOINTS,
    REGRID_ENABLED, SAN_JOSE_SOURCES, VERIFIED_ADDRESSES,
)
from data_cache_manager import DataCache, _sync_lock
from api_clients import (
    LADBSClient, LongBeachClient, fetch_socrata_best,
    fetch_ckan_permits, fetch_carto_permits, GeocodingClient,
)
from lead_processing import (
    safe_float, safe_int, apply_access_filter,
    _insert_action, _insert_response, _insert_outcome, _mark_alert_seen,
    _score_component, _estimate_valuation, normalize_lead_for_ui,
    compute_readiness, _live_days_old, _with_live_days_old, parse_date,
    fix_coordinates, _web_mercator_to_latlng, extract_lat_lng,
    _safe_str, extract_address, extract_permit_number,
    _infer_state, _build_permit_url, process_ladbs_permit,
    CITY_STATE_MAP, PERMIT_PORTAL_URLS, Lead, APIStatus,
    _is_sold_property, _enrich_description, _transform_lead, _is_insurance_claim, _SOLD_RE, _PERMIT_DESC_MAP,
)
from enrichment_pipeline import (
    _enrich_via_pdl, _enrich_via_custom, enrich_contact,
    _attom_enrich, enrich_contacts_batch,
    fetch_arcgis_permits, fetch_fema_nfip_claims, fetch_fema_disasters,
    fetch_fema_disaster_areas,
    enrich_owner_from_la_assessor, enrich_owner_from_regrid_tile,
    enrich_owner_from_county_scraper, enrich_owner_from_arcgis_parcel,
    enrich_lead_ownership, batch_enrich_ownership,
    get_disaster_risk_for_lead,
    _enrichment_cache, _enrichment_cache_ts, ENRICHMENT_CACHE_TTL,
    _disaster_cache, _DISASTER_CACHE_TTL,
)
from sync_engine import (
    sync_data, _CITY_COORDS, _STATE_COORDS,
)
from geo_data import (
    _cleaned_leads, _cleaned_leads_version, _cleaned_leads_by_id,
    _response_cache, _bad_city_patterns, _state_names_set,
    _CITY_NAME_FIXES, _KNOWN_CITY_COORDS, _junk_drop_patterns,
    _junk_city_names, _rebuild_cleaned_leads,
    _map_cache, _MAP_CACHE_TTL,
)


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
    # Check if user is admin — grant full access
    cur.execute("SELECT role FROM users WHERE id=?", (user_id,))
    user_row = cur.fetchone()
    if user_row and user_row[0] == "admin":
        conn.close()
        return {
            "territories": [],
            "license_status": "verified",
            "max_zips": 99999,
            "plan": "admin",
            "all_features": True,
        }
    cur.execute("SELECT zip_code FROM territory_assignments WHERE user_id=?", (user_id,))
    rows = cur.fetchall()
    cur.execute("SELECT status FROM contractor_license WHERE user_id=?", (user_id,))
    lic = cur.fetchone()
    conn.close()
    return {
        "territories": [r[0] for r in rows] if rows else [],
        "license_status": lic[0] if lic else "unknown",
    }


# ============================================================================
# PLAN ENFORCEMENT (backend-enforced limits)
# ============================================================================

PLAN_LIMITS = {
    "free":       {"max_leads": 50,     "max_zips": 3,     "csv_export": False, "heatmap": False, "parcels": False, "api_access": False},
    "reveal":     {"max_leads": 50,     "max_zips": 3,     "csv_export": False, "heatmap": False, "parcels": False, "api_access": False},
    "starter":    {"max_leads": 5000,   "max_zips": 5,     "csv_export": True,  "heatmap": False, "parcels": False, "api_access": False},
    "pro":        {"max_leads": 999999, "max_zips": 25,    "csv_export": True,  "heatmap": True,  "parcels": True,  "api_access": True},
    "enterprise": {"max_leads": 999999, "max_zips": 100,   "csv_export": True,  "heatmap": True,  "parcels": True,  "api_access": True},
    "admin":      {"max_leads": 999999, "max_zips": 99999, "csv_export": True,  "heatmap": True,  "parcels": True,  "api_access": True},
}


def get_user_plan_from_request(request) -> dict:
    """Extract user plan info from request. Returns dict with plan name and limits."""
    token = request.headers.get("authorization", "").replace("Bearer ", "")
    if not token or token == "demo":
        return {"plan": "reveal", "max_zips": 0, "role": "demo"}
    user = decode_jwt_token(token)
    if not user:
        return {"plan": "reveal", "max_zips": 0, "role": "anonymous"}
    user_id = str(user.get("sub", ""))
    role = user.get("role", "")
    if role == "admin":
        return {"plan": "admin", "max_zips": 99999, "role": "admin"}
    # Check subscription via billing
    try:
        from routes.billing import get_user_plan as _billing_get_plan
        billing_plan = _billing_get_plan(user_id)
        plan_id = billing_plan.get("plan_id", "free")
        return {"plan": plan_id, "max_zips": PLAN_LIMITS.get(plan_id, PLAN_LIMITS["reveal"]).get("max_zips", 3), "role": role, "user_id": user_id}
    except Exception:
        ents = load_entitlements(user_id)
        plan = ents.get("plan", "reveal").lower()
        return {"plan": plan, "max_zips": ents.get("max_zips", 3), "role": role, "user_id": user_id}


def get_plan_limits(plan_name: str) -> dict:
    """Get the limits dict for a plan name."""
    return PLAN_LIMITS.get(plan_name.lower(), PLAN_LIMITS["reveal"])



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
        "/", "/app", "/login",
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
            # Demo token: allow read-only access to limited dataset, not full access
            if token == "demo":
                request.state.user_id = "demo"
                request.state.access = {"plan": "demo", "max_zips": 1, "role": "demo", "read_only": True}
                await self.app(scope, receive, send)
                return
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
    # Removed: duplicate v3_router definitions for /api/filters, /api/notifications — see routes/leads.py
    app.include_router(new_leads_router)
    logger.info("✅ Leads router registered (filters, notifications, lead details, stats)")
except ImportError as e:
    logger.warning(f"Leads router not available: {e}")

try:
    from routes.campaigns import router as campaigns_router
    app.include_router(campaigns_router)
    logger.info("✅ Campaigns router registered (email, SMS, CRM)")
except ImportError as e:
    logger.warning(f"Campaigns router not available: {e}")

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


# [Extracted to separate module — see imports above]


# [Lead and APIStatus models moved to lead_processing.py]


# [Extracted to separate module — see imports above]


# [Extracted to separate module — see imports above]


# [Extracted to separate module — see imports above]


# [Extracted to separate module — see imports above]


# [Extracted to separate module — see imports above]


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

    # ── Start periodic data sync ──
    asyncio.create_task(periodic_sync())
    logger.info("Periodic data sync started (every %ds)", int(REFRESH_INTERVAL_SEC))

    # ── Background: verify SQLite lead count on startup ──
    async def _startup_check():
        """Verify SQLite has leads and pre-warm map cache."""
        await asyncio.sleep(1)
        try:
            from models.database import get_db
            with get_db() as db:
                db_count = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            logger.info(f"✅ SQLite has {db_count:,} leads (SQL-backed, no in-memory cache)")
        except Exception as e:
            logger.warning(f"Startup DB check failed: {e}")

    asyncio.create_task(_startup_check())

    if ENRICH_ENABLED:
        logger.info("Owner enrichment enabled; starting background loop")
        asyncio.create_task(enrich_loop(int(ENRICH_INTERVAL_SEC)))
    if YELP_INTENT_ENABLED:
        logger.info("Yelp intent ingestion enabled; starting background loop")
        asyncio.create_task(periodic_yelp_intents())
    
    # ── Owner Discovery: free background enrichment ──
    try:
        from services.enrichment_scheduler import owner_discovery_loop
        asyncio.create_task(owner_discovery_loop(interval=300))
        logger.info("Owner discovery background loop started (5-min interval)")
    except Exception as e:
        logger.warning(f"Owner discovery setup failed: {e}")

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
                    leads = get_leads_needing_enrichment(limit=200)
                    if not leads:
                        await asyncio.sleep(300)  # Nothing to enrich, check again in 5min
                        continue
                    logger.info("Auto-enriching %d leads...", len(leads))
                    stats = await enrich_batch(leads, max_leads=200, concurrency=10)
                    enriched_count = stats.get("enriched", 0)
                    for lead in leads:
                        has_contact = lead.get("owner_phone") or lead.get("owner_email")
                        has_owner = lead.get("owner_name") and lead["owner_name"] not in ("", "Pending lookup")
                        if has_contact:
                            mark_enriched(lead["id"], lead)
                        elif has_owner:
                            # Save owner_name + property data from Regrid even without phone/email
                            # Keep enrichment_status as 'pending' so next cycle retries contact lookup
                            mark_enriched(lead["id"], {
                                k: v for k, v in lead.items()
                                if k in ("owner_name", "owner_address", "beneficial_owner",
                                         "apn", "market_value", "year_built", "square_feet",
                                         "lot_size", "bedrooms", "bathrooms", "zoning",
                                         "enrichment_sources")
                                and v
                            })
                    logger.info("Auto-enrichment: %d enriched", enriched_count)
                    consecutive_failures = 0
                except Exception as e:
                    consecutive_failures += 1
                    logger.warning("Auto-enrichment error (%d/%d): %s", consecutive_failures, max_failures, e)
                await asyncio.sleep(15)  # 15s cooldown between batches

        asyncio.create_task(auto_enrich_after_sync())
        logger.info("Auto-enrichment pipeline started (batch=200, cooldown=15s, concurrency=10, circuit_breaker=3)")
    except ImportError as e:
        logger.info("Auto-enrichment not available: %s", e)

    # ── Lead visibility: deactivate leads older than LEAD_VISIBILITY_DAYS ──
    async def deactivate_old_leads():
        """Periodically hide leads older than LEAD_VISIBILITY_DAYS.

        Leads are NOT deleted — just set is_active=0 so they disappear
        from map and API queries.  Runs every 6 hours.
        """
        await asyncio.sleep(120)  # Wait for startup to settle
        while True:
            if LEAD_VISIBILITY_DAYS > 0:
                try:
                    cutoff = (datetime.utcnow() - timedelta(days=LEAD_VISIBILITY_DAYS)).strftime("%Y-%m-%d")
                    with get_db() as conn:
                        cursor = conn.execute(
                            """UPDATE leads SET is_active = 0, updated_at = datetime('now')
                               WHERE is_active = 1
                                 AND issue_date != ''
                                 AND issue_date < ?""",
                            (cutoff,),
                        )
                        hidden = cursor.rowcount
                        conn.commit()
                    if hidden > 0:
                        logger.info("Lead visibility: hid %d leads older than %d days", hidden, LEAD_VISIBILITY_DAYS)
                except Exception as e:
                    logger.warning("Lead visibility error: %s", e)
            await asyncio.sleep(6 * 3600)  # Run every 6 hours

    asyncio.create_task(deactivate_old_leads())
    logger.info("Lead visibility task started (hide after %d days)", LEAD_VISIBILITY_DAYS)

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
        return FileResponse(str(landing_path), headers={
            "Cache-Control": "public, max-age=300",
        })
    return FileResponse(str(_static_dir / "index.html"), headers={
        "Cache-Control": "public, max-age=60",
    })


@app.get("/app")
async def dashboard():
    """Serve the main application dashboard"""
    return FileResponse(str(_static_dir / "index.html"), headers={
        "Cache-Control": "public, max-age=60",
    })


@app.get("/login")
async def login_page():
    """Serve the login/landing page"""
    login_path = _static_dir / "login.html"
    if login_path.exists():
        return FileResponse(str(login_path))
    return FileResponse(str(_static_dir / "landing.html"))


@app.get("/admin")
async def admin_page(request: Request):
    """Serve the admin dashboard — requires admin role. Returns 404 for non-admins."""
    token = request.cookies.get("onsite_token") or request.headers.get("authorization", "").replace("Bearer ", "")
    client_ip = request.client.host if request.client else "unknown"
    if not token or token == "demo":
        logger.warning("Admin access attempt with no/demo token from %s", client_ip)
        raise HTTPException(status_code=404)  # Don't reveal admin exists
    user = decode_jwt_token(token)
    if not user or user.get("role") != "admin":
        logger.warning("Admin access attempt by non-admin user=%s from %s", user.get("email") if user else "unknown", client_ip)
        raise HTTPException(status_code=404)  # Don't reveal admin exists
    # Optional IP whitelist
    admin_ips = os.getenv("ADMIN_IPS", "")
    if admin_ips:
        allowed = [ip.strip() for ip in admin_ips.split(",") if ip.strip()]
        if allowed and client_ip not in allowed:
            logger.warning("Admin access from non-whitelisted IP %s by %s", client_ip, user.get("email"))
            raise HTTPException(status_code=404)
    logger.info("Admin access GRANTED to %s from %s", user.get("email"), client_ip)
    admin_path = _static_dir / "admin.html"
    if admin_path.exists():
        return FileResponse(str(admin_path))
    raise HTTPException(status_code=404)



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
async def enrich_lead_owner(lead_id: str, request: Request = None):
    """Enrich a lead with property owner information — 100% FREE sources only.
    Pipeline: LA Assessor → County Scrapers → ArcGIS Parcels → Regrid MVT Tiles → SMTP Email"""
    # --- Plan enforcement: Starter+ gets free reveals, Reveal plan is logged ---
    plan_info = get_user_plan_from_request(request) if request else {"plan": "reveal"}
    plan_name = plan_info.get("plan", "reveal").lower()
    if plan_name in ("reveal", "free", "demo"):
        logger.info("Owner reveal by free/reveal user (plan=%s, lead=%s)", plan_name, lead_id)

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

# [Extracted to separate module — see imports above]


# ═══════════════════════════════════════════════════════════════════════════
# ZIP COVERAGE — Real ZIP codes with lead counts from database
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/zip-coverage")
async def zip_coverage():
    """Return all ZIP codes that have active leads, with counts and city/state."""
    from models.database import get_db
    with get_db() as conn:
        rows = conn.execute("""
            SELECT zip, city, state, COUNT(*) as lead_count,
                   SUM(CASE WHEN owner_phone != '' AND owner_phone IS NOT NULL THEN 1 ELSE 0 END) as enriched_count
            FROM leads
            WHERE is_active = 1 AND zip IS NOT NULL AND zip != '' AND LENGTH(zip) = 5
            GROUP BY zip
            HAVING lead_count >= 3
            ORDER BY lead_count DESC
        """).fetchall()
        return [{
            "zip": r["zip"],
            "city": r["city"] or "",
            "state": r["state"] or "",
            "leads": r["lead_count"],
            "enriched": r["enriched_count"],
        } for r in rows]


# _MAP_CACHE_TTL moved to geo_data.py

@app.get("/api/leads/map")
async def get_leads_map():
    """Lightweight map endpoint — returns only [id, lat, lng, pri_int, score, city] per lead.
    SQL-backed with 5-minute TTL cache. No in-memory lead list needed.
    pri_int: 0=hot, 1=warm, 2=med, 3=cold
    """
    import time as _time

    # Serve from TTL cache if fresh
    if _map_cache.get("ts") and (_time.time() - _map_cache["ts"]) < _MAP_CACHE_TTL:
        return _map_cache["data"]

    # Query directly from SQLite (only 6 fields, ~3MB result)
    try:
        from models.database import query_leads_for_map
        points = await asyncio.to_thread(query_leads_for_map)
    except Exception as e:
        logger.error(f"Map query error: {e}")
        points = []

    result = {"points": points, "total": len(points), "source": "sql"}
    _map_cache["ts"] = _time.time()
    _map_cache["data"] = result
    return result


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
    """Get leads from SQLite with filtering and pagination."""
    from models.database import query_leads as db_query_leads

    # --- Plan enforcement: cap lead count by plan ---
    plan_info = get_user_plan_from_request(request) if request else {"plan": "reveal"}
    plan_name = plan_info.get("plan", "reveal").lower()
    plan_limits = get_plan_limits(plan_name)

    field_set = set(f.strip() for f in fields.split(",")) if fields else None

    lim = min(limit or LEADS_RETURN_LIMIT, LEADS_RETURN_LIMIT)
    try:
        db_leads, db_total = await asyncio.to_thread(
            db_query_leads,
            limit=lim,
            offset=offset,
            city=city,
            state=state,
            max_days=max_days_old or days,
            permit_type=permit_type,
            contact_only=contact_only,
            sort_by="score",
        )
    except Exception as e:
        logger.error(f"Lead query error: {e}")
        return {"leads": [], "source": "error", "total": 0, "count": 0, "returned": 0}

    if not db_leads:
        return {"leads": [], "source": "sql", "total": 0, "count": 0, "returned": 0}

    # Apply readiness scoring only to the paginated result set (not 300K leads)
    _readiness_fields = {"readiness_score", "recommended_action", "contact_window_days", "budget_range", "competition_level"}
    if not field_set or bool(field_set & _readiness_fields):
        db_leads = [normalize_lead_for_ui(compute_readiness(l)) for l in db_leads]
    if field_set:
        db_leads = [{k: v for k, v in l.items() if k in field_set} for l in db_leads]

    # --- Plan enforcement: truncate to plan limit ---
    max_leads = plan_limits.get("max_leads", 50)
    if len(db_leads) > max_leads:
        db_leads = db_leads[:max_leads]
        db_total = max_leads

    return {
        "leads": db_leads,
        "source": "sql",
        "total": db_total,
        "count": db_total,
        "returned": len(db_leads),
        "plan": plan_name,
        "plan_limit": max_leads,
    }


@app.get("/api/leads/bbox")
async def get_leads_bbox(
    south: float = Query(..., description="South latitude"),
    west: float = Query(..., description="West longitude"),
    north: float = Query(..., description="North latitude"),
    east: float = Query(..., description="East longitude"),
    limit: int = Query(2000, description="Max leads to return"),
    days: int = Query(None, description="Filter to permits issued in last N days"),
    permit_type: Optional[str] = Query(None, description="Filter by permit_type substring"),
    city: Optional[str] = Query(None, description="Filter by city substring"),
    request: Request = None,
):
    """Return leads within the requested bounding box (SQL-backed)."""
    from models.database import query_leads as db_query_leads

    try:
        db_leads, db_total = await asyncio.to_thread(
            db_query_leads,
            limit=min(limit, 5000),
            city=city,
            max_days=days,
            permit_type=permit_type,
            bbox=(south, west, north, east),
            sort_by="score",
        )
        if db_leads:
            db_leads = [compute_readiness(l) for l in db_leads]
        return {
            "leads": db_leads or [],
            "source": "sql",
            "count": db_total,
            "returned": len(db_leads or []),
        }
    except Exception as e:
        logger.error(f"BBox query error: {e}")
        return {"leads": [], "source": "error", "count": 0, "returned": 0}


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
    from models.database import get_db
    with get_db() as db:
        db_count = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    return {
        "sql_leads_count": db_count,
        "map_cache_fresh": bool(_map_cache.get("ts")),
        "mode": "sql-backed",
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
            INSERT INTO leads (id, address, city, state, zip, owner_name, owner_phone, owner_email,
                             permit_type, valuation, permit_number, work_description, source, issue_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (lead_id, address, city, state, zip_code, lead["owner_name"], lead["phone"],
              lead["email"], lead["type"], lead["valuation"], lead["permit_number"],
              lead["description"], "manual", lead["issue_date"]))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Failed to save manual lead: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save lead")

    # Invalidate map cache so new lead shows up
    _map_cache.clear()

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
    """SQL-backed lead lookup by ID (replaces in-memory index)."""
    try:
        lid = int(lead_id)
    except (ValueError, TypeError):
        return None
    try:
        from models.database import get_lead_by_id
        return get_lead_by_id(lid)
    except Exception as e:
        logger.error(f"Lead lookup error for id={lead_id}: {e}")
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
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload:
        raise HTTPException(401, "Not authenticated")
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

    # Ensure settings_json column exists (only once per process)
    global _settings_col_added
    conn = _leads_conn()
    try:
        if not _settings_col_added:
            try:
                conn.execute("ALTER TABLE users ADD COLUMN settings_json TEXT DEFAULT '{}'")
                conn.commit()
            except Exception:
                pass  # Column already exists
            _settings_col_added = True

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
    except Exception as e:
        logger.error("Settings save failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save settings")
    finally:
        conn.close()

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


@app.get("/api/owner-discovery/stats")
async def owner_discovery_stats():
    """Get owner discovery coverage and cumulative run stats."""
    try:
        from services.owner_discovery import get_owner_stats
        from services.enrichment_scheduler import get_cumulative_stats
        coverage = get_owner_stats()
        cumulative = get_cumulative_stats()
        return {"success": True, "coverage": coverage, "cumulative_runs": cumulative}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/owner-discovery/run")
async def owner_discovery_run_now():
    """Trigger an immediate owner discovery batch."""
    try:
        from services.enrichment_scheduler import run_owner_discovery_batch
        stats = await run_owner_discovery_batch()
        return {"success": True, "batch_stats": stats}
    except Exception as e:
        return {"success": False, "error": str(e)}


# Removed: duplicate CRM routes (/api/crm/authorize, callback, sync) — see routes/campaigns.py
# Removed: duplicate /api/campaigns/email/send — see routes/campaigns.py
# Removed: duplicate /api/campaigns/sms/send — see routes/campaigns.py




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
        _map_cache.clear()

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
        _map_cache.clear()

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
    # Also include registry source counts from api_sources table
    registry_stats = {"total": 0, "active": 0, "by_type": {}}
    try:
        conn = _leads_conn()
        cur = conn.cursor()
        cur.execute("SELECT source_type, COUNT(*) as cnt FROM api_sources WHERE status='active' GROUP BY source_type ORDER BY cnt DESC")
        for row in cur.fetchall():
            registry_stats["by_type"][row[0]] = row[1]
            registry_stats["active"] += row[1]
        cur.execute("SELECT COUNT(*) FROM api_sources")
        registry_stats["total"] = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        logger.warning(f"Registry stats error: {e}")

    return {
        "total": len(sources),
        "socrata": len(SOC_DATASETS),
        "arcgis": len(ARCGIS_DATASETS),
        "ckan": len(CKAN_DATASETS),
        "federal": len(FEDERAL_ENDPOINTS),
        "sources": sources,
        "registry": registry_stats,
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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist
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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist
    conn = _leads_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT status, count(*) FROM subscriptions GROUP BY status")
        rows = cur.fetchall()
    finally:
        conn.close()
    return {"subscriptions": {r[0]: r[1] for r in rows}}


@app.get("/admin/system/cache-status")
async def admin_cache_status(request: Request):
    """Check alignment between JSON cache and SQLite DB."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist
    cache_count = 0  # No longer using in-memory cache
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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist
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

    cleaned_count = 0  # SQL-backed, no in-memory leads
    by_id_count = 0
    resp_cache_count = 0
    disaster_count = len(_disaster_cache)

    # Estimate sizes
    cleaned_est_mb = 0
    resp_cache_mb = 0

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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist
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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist
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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist
    conn = _leads_conn()
    try:
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
        return {"count": count}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# On Site frontend compatibility layer
# Maps existing backend data to the field names / endpoints the On Site
# UI expects (e.g. /api/permits, lead_score, project_value, etc.)
# ---------------------------------------------------------------------------

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
                cur.execute("SELECT lead_id, note FROM lead_notes")
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
            row = conn.execute("SELECT note FROM lead_notes WHERE lead_id=? ORDER BY created_at DESC LIMIT 1", (permit_id,)).fetchone()
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
    note_text = payload.get("notes", "")
    try:
        from models.database import get_db
        with get_db() as conn:
            conn.execute(
                """INSERT INTO lead_notes (lead_id, note, created_at)
                   VALUES (?, ?, ?)""",
                (permit_id, note_text, datetime.utcnow().isoformat()),
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


_stats_cache: dict = {}  # {"ts": float, "data": dict}
_STATS_CACHE_TTL = 60  # seconds

@app.get("/api/stats")
async def compat_stats():
    """On Site compat — aggregate lead statistics (SQL-backed, 60s cache)."""
    import time as _t
    if _stats_cache.get("ts") and (_t.time() - _stats_cache["ts"]) < _STATS_CACHE_TTL:
        return _stats_cache["data"]
    try:
        from models.database import get_stats
        result = await asyncio.to_thread(get_stats)
        _stats_cache["ts"] = _t.time()
        _stats_cache["data"] = result
        return result
    except Exception as e:
        logger.error(f"Stats query error: {e}")
        return {"total_leads": 0, "hot_leads": 0, "warm_leads": 0, "cold_leads": 0}


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

    # Activity: 12 monthly buckets (query DB directly for accuracy)
    now = datetime.utcnow()
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    monthly_counts = {}
    for i in range(11, -1, -1):
        dt = now - timedelta(days=i * 30)
        key = f"{dt.year}-{dt.month:02d}"
        monthly_counts[key] = {"month": month_names[dt.month - 1], "count": 0}
    try:
        conn = _leads_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT strftime('%Y-%m', issue_date) as ym, COUNT(*) as cnt
            FROM leads
            WHERE is_active = 1 AND issue_date != '' AND issue_date IS NOT NULL
            GROUP BY ym
        """)
        for row in cur.fetchall():
            ym = row[0]
            if ym and ym in monthly_counts:
                monthly_counts[ym]["count"] = row[1]
        conn.close()
    except Exception as e:
        logger.warning("Analytics activity SQL fallback: %s", e)
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
async def compat_sync_run(request: Request, background_tasks: BackgroundTasks):
    """On Site compat — trigger full data sync with structured response."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload:
        raise HTTPException(401, "Not authenticated")
    try:
        lead_count = await sync_data()
        return {"status": "ok", "total": lead_count or 0, "results": []}
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
    try:
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
    finally:
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
    try:
        import sqlite3
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM team_members WHERE owner_id = ? ORDER BY invited_at DESC", (user_id,)).fetchall()
        return {"members": [dict(r) for r in rows]}
    finally:
        conn.close()


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
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail="Member already invited")
        raise HTTPException(status_code=500, detail="Failed to invite member")
    finally:
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
    try:
        if role:
            conn.execute("UPDATE team_members SET role = ? WHERE id = ?", (role, member_id))
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok"}


@app.delete("/api/team/{member_id}")
async def remove_team_member(member_id: str, request: Request):
    """Remove a team member."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    conn = _leads_conn()
    try:
        conn.execute("DELETE FROM team_members WHERE id = ?", (member_id,))
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════
# DEVELOPER API KEYS — Generate, list, revoke
# ═══════════════════════════════════════════════════════════════════════════

def _ensure_apikey_tables():
    """Create API key tables."""
    conn = _leads_conn()
    try:
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
    finally:
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
    try:
        import sqlite3
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, name, key_prefix, created_at, last_used_at, is_active FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,)
        ).fetchall()
        return {"keys": [dict(r) for r in rows]}
    finally:
        conn.close()


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
    try:
        conn.execute(
            "INSERT INTO api_keys (id, user_id, name, key_prefix, key_hash) VALUES (?, ?, ?, ?, ?)",
            (key_id, user_id, name, key_prefix, key_hash)
        )
        conn.commit()
    finally:
        conn.close()

    return {"id": key_id, "key": raw_key, "prefix": key_prefix, "name": name, "note": "Save this key now — it won't be shown again."}


@app.delete("/api/developer/keys/{key_id}")
async def revoke_api_key(key_id: str, request: Request):
    """Revoke (deactivate) an API key."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    conn = _leads_conn()
    try:
        conn.execute("UPDATE api_keys SET is_active = 0 WHERE id = ?", (key_id,))
        conn.commit()
    finally:
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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist

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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist

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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist

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
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload or payload.get("role") != "admin":
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist

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
        raise HTTPException(status_code=404)  # Don't reveal admin endpoints exist

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
async def geocode_status(request: Request):
    """Check batch geocoding status."""
    token = (request.headers.get("authorization") or "").replace("Bearer ", "")
    payload = decode_jwt_token(token)
    if not payload:
        raise HTTPException(401, "Not authenticated")
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


# Removed: duplicate /api/billing/portal — see routes/billing.py

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
