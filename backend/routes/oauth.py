"""
Google OAuth routes for Onsite.
Handles Google sign-in, callback, and account linking.
"""

import os
import time
import uuid
import logging
import sqlite3
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, JSONResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["oauth"])

# ── Config ──
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.getenv(
    "GOOGLE_REDIRECT_URI",
    "http://localhost:18000/api/auth/google/callback",
)
APP_URL = os.getenv("APP_URL", "http://localhost:18000")

# Short-lived auth codes: {code: {"token": jwt, "expires": timestamp}}
# Exchanged once via POST, avoids leaking JWTs in redirect URLs.
_pending_auth_codes: dict[str, dict] = {}

AUTH_CODE_TTL_SECONDS = 60

# Lazy-init OAuth client (only if credentials are configured)
_oauth = None


def _get_oauth():
    """Lazy-initialize the authlib OAuth client."""
    global _oauth
    if _oauth is not None:
        return _oauth
    try:
        from authlib.integrations.starlette_client import OAuth
        _oauth = OAuth()
        _oauth.register(
            name="google",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )
        return _oauth
    except Exception as exc:
        logger.error("Failed to initialize Google OAuth: %s", exc)
        return None


# ── DB helpers (reuse auth.py patterns) ──

def _db_path():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "leads.db")


def _connect():
    conn = sqlite3.connect(_db_path(), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


# Reuse the JWT token creator from auth routes
def _create_token(user_id: str, email: str, role: str = "user") -> str:
    """Import and delegate to auth module's token creator."""
    try:
        from routes.auth import _create_token as auth_create_token
    except ImportError:
        raise ImportError("Cannot import _create_token from routes.auth")
    return auth_create_token(user_id, email, role)


# ── Schema migration for OAuth columns ──

def _ensure_oauth_columns():
    """Add OAuth columns to users table if they don't exist."""
    conn = _connect()
    try:
        for col, typedef in [
            ("google_id", "TEXT"),
            ("google_avatar", "TEXT"),
            ("auth_provider", "TEXT DEFAULT 'email'"),
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {typedef}")
                conn.commit()
            except sqlite3.OperationalError:
                pass  # Column already exists
    finally:
        conn.close()


# Run on import
_ensure_oauth_columns()


# ── Routes ──

@router.get("/google")
async def google_login(request: Request):
    """Redirect user to Google consent screen."""
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        return JSONResponse(
            {"detail": "Google OAuth not configured. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET env vars."},
            status_code=503,
        )
    oauth = _get_oauth()
    if not oauth:
        return JSONResponse({"detail": "OAuth initialization failed"}, status_code=503)
    return await oauth.google.authorize_redirect(request, GOOGLE_REDIRECT_URI)


@router.get("/google/callback")
async def google_callback(request: Request):
    """Handle Google OAuth callback — create or link account, issue JWT."""
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        return RedirectResponse(f"{APP_URL}/login?error=oauth_not_configured")

    oauth = _get_oauth()
    if not oauth:
        return RedirectResponse(f"{APP_URL}/login?error=oauth_init_failed")

    try:
        token_data = await oauth.google.authorize_access_token(request)
    except Exception as exc:
        logger.error("Google OAuth token exchange failed: %s", exc)
        return RedirectResponse(f"{APP_URL}/login?error=oauth_failed")

    # Extract user info from ID token or userinfo endpoint
    user_info = token_data.get("userinfo")
    if not user_info:
        try:
            user_info = await oauth.google.userinfo()
        except Exception:
            return RedirectResponse(f"{APP_URL}/login?error=no_user_info")

    email = user_info.get("email", "").lower().strip()
    google_id = user_info.get("sub", "")
    full_name = user_info.get("name", "")
    avatar = user_info.get("picture", "")

    if not email:
        return RedirectResponse(f"{APP_URL}/login?error=no_email")

    conn = _connect()
    try:
        # Look up existing user by email or google_id
        row = conn.execute(
            "SELECT * FROM users WHERE email = ? OR google_id = ?",
            (email, google_id),
        ).fetchone()

        if row:
            # Existing user — link Google if not already linked
            user_id = row["id"]
            user_role = row["role"]
            if not row["google_id"]:
                conn.execute(
                    "UPDATE users SET google_id = ?, google_avatar = ?, auth_provider = CASE WHEN auth_provider = 'email' THEN 'email+google' ELSE auth_provider END WHERE id = ?",
                    (google_id, avatar, user_id),
                )
            # Update last login
            conn.execute(
                "UPDATE users SET last_login = datetime('now'), login_count = login_count + 1, email_verified = 1 WHERE id = ?",
                (user_id,),
            )
            conn.commit()
        else:
            # New user — create account with Google
            user_id = str(uuid.uuid4())
            user_role = "user"
            conn.execute(
                """INSERT INTO users (id, email, password_hash, full_name, company, phone, role, is_active, email_verified, google_id, google_avatar, auth_provider, last_login, login_count)
                   VALUES (?, ?, 'GOOGLE_OAUTH', ?, '', '', 'user', 1, 1, ?, ?, 'google', datetime('now'), 1)""",
                (user_id, email, full_name, google_id, avatar),
            )
            conn.commit()
            logger.info("New Google OAuth user created: %s", email)

        # Issue JWT
        jwt_token = _create_token(user_id, email, user_role)

        # Store JWT behind a short-lived auth code (never expose JWT in URL)
        auth_code = str(uuid.uuid4())
        _pending_auth_codes[auth_code] = {
            "token": jwt_token,
            "expires": time.time() + AUTH_CODE_TTL_SECONDS,
        }

        # Redirect to app with opaque code (frontend exchanges via POST)
        return RedirectResponse(f"{APP_URL}/login?auth=google&code={auth_code}")

    except Exception as exc:
        logger.error("Google OAuth DB error: %s", exc)
        return RedirectResponse(f"{APP_URL}/login?error=server_error")
    finally:
        conn.close()


def _purge_expired_codes() -> None:
    """Remove expired auth codes, returning a clean dict."""
    global _pending_auth_codes
    now = time.time()
    _pending_auth_codes = {
        code: entry
        for code, entry in _pending_auth_codes.items()
        if entry["expires"] > now
    }


@router.post("/google/exchange")
async def google_exchange(request: Request):
    """Exchange a short-lived auth code for the JWT token."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "Invalid JSON body"}, status_code=400)

    code = body.get("code", "").strip()
    if not code:
        return JSONResponse({"detail": "Missing code"}, status_code=400)

    # Purge expired codes before lookup
    _purge_expired_codes()

    entry = _pending_auth_codes.get(code)
    if not entry:
        return JSONResponse({"detail": "Invalid or expired code"}, status_code=401)

    # One-time use: remove immediately
    _pending_auth_codes.pop(code, None)

    if entry["expires"] < time.time():
        return JSONResponse({"detail": "Code expired"}, status_code=401)

    return JSONResponse({"token": entry["token"]})
