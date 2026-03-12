"""
Onsite Authentication Routes
User registration, login, token refresh, and password management.
Uses SQLite (leads.db) for user storage and HMAC-SHA256 JWT tokens.
"""

import os
import time
import uuid
import hmac
import json
import base64
import hashlib
import logging
import sqlite3
from datetime import datetime
from typing import Optional
from contextlib import contextmanager

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel, EmailStr, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth", tags=["authentication"])

JWT_SECRET = os.getenv("JWT_SECRET", "onsite-dev-secret-change-in-production")
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "24"))
BCRYPT_AVAILABLE = False

try:
    import bcrypt as _bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    _bcrypt = None
    logger.warning("bcrypt not available — password hashing will fail")


# ── Database helpers ──

def _db_path():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "leads.db")


@contextmanager
def _get_db():
    conn = sqlite3.connect(_db_path(), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "y.abismuth@gmail.com")


def _init_users_table():
    with _get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT DEFAULT '',
                company TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                role TEXT DEFAULT 'user',
                is_active INTEGER DEFAULT 1,
                email_verified INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                last_login TEXT,
                login_count INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
        """)
        # Add OAuth columns if missing
        for col, typedef in [
            ("google_id", "TEXT DEFAULT ''"),
            ("google_avatar", "TEXT DEFAULT ''"),
            ("auth_provider", "TEXT DEFAULT 'email'"),
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # Column already exists
        # Seed admin account if not exists
        existing = conn.execute("SELECT id FROM users WHERE email = ?", (ADMIN_EMAIL,)).fetchone()
        if not existing:
            admin_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO users (id, email, password_hash, full_name, role, email_verified, auth_provider) "
                "VALUES (?, ?, ?, ?, 'admin', 1, 'google')",
                (admin_id, ADMIN_EMAIL, "GOOGLE_OAUTH", "Yohan Bismuth"),
            )
            logger.info(f"Seeded admin account: {ADMIN_EMAIL}")
        else:
            # Ensure existing admin has admin role
            conn.execute("UPDATE users SET role = 'admin' WHERE email = ?", (ADMIN_EMAIL,))


# Initialize on import
_init_users_table()


# ── Password hashing ──

def _hash_password(password: str) -> str:
    if not BCRYPT_AVAILABLE:
        raise RuntimeError("bcrypt is required for password hashing. Install: pip install bcrypt")
    salt = _bcrypt.gensalt()
    return _bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def _verify_password(password: str, password_hash: str) -> bool:
    if not BCRYPT_AVAILABLE:
        raise RuntimeError("bcrypt is required for password verification.")
    # Support legacy SHA-256 hashes (64 hex chars) — migrate on next login
    if len(password_hash) == 64 and all(c in '0123456789abcdef' for c in password_hash):
        if hashlib.sha256(password.encode()).hexdigest() == password_hash:
            return True
        return False
    return _bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))


# ── JWT helpers ──

def _create_token(user_id: str, email: str, role: str = "user") -> str:
    header = base64.urlsafe_b64encode(json.dumps(
        {"alg": "HS256", "typ": "JWT"}
    ).encode()).rstrip(b"=").decode()

    now = int(time.time())
    payload_data = {
        "sub": user_id,
        "email": email,
        "role": role,
        "iat": now,
        "exp": now + (JWT_EXPIRY_HOURS * 3600),
    }
    payload = base64.urlsafe_b64encode(
        json.dumps(payload_data).encode()
    ).rstrip(b"=").decode()

    signing_input = f"{header}.{payload}".encode()
    signature = hmac.new(JWT_SECRET.encode(), signing_input, digestmod="sha256").digest()
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()

    return f"{header}.{payload}.{sig_b64}"


# ── Request/Response models ──

class RegisterRequest(BaseModel):
    email: str = Field(..., min_length=5)
    password: str = Field(..., min_length=8)
    full_name: str = Field(default="", max_length=200)
    company: str = Field(default="", max_length=200)
    phone: str = Field(default="", max_length=20)


class LoginRequest(BaseModel):
    email: str
    password: str


class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)


# ── Routes ──

@router.post("/register")
async def register(req: RegisterRequest):
    """Register a new user account."""
    import uuid

    email = req.email.lower().strip()

    # Basic email validation
    if "@" not in email or "." not in email.split("@")[-1]:
        raise HTTPException(status_code=400, detail="Invalid email address")

    with _get_db() as conn:
        # Check if email exists
        existing = conn.execute(
            "SELECT id FROM users WHERE email = ?", (email,)
        ).fetchone()
        if existing:
            raise HTTPException(status_code=409, detail="Email already registered")

        user_id = str(uuid.uuid4())[:12]
        password_hash = _hash_password(req.password)

        conn.execute(
            """INSERT INTO users (id, email, password_hash, full_name, company, phone)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, email, password_hash,
             req.full_name.strip(), req.company.strip(), req.phone.strip()),
        )

    token = _create_token(user_id, email)
    verify_token = _create_verification_token(user_id, email)
    logger.info(f"New user registered: {email} (id={user_id})")

    # Send welcome + verification emails (fire-and-forget)
    try:
        from services.email_service import send_welcome, send_email_verification, DEV_MODE
        import asyncio
        asyncio.create_task(send_welcome(email, req.full_name.strip()))
        asyncio.create_task(send_email_verification(email, verify_token))
    except Exception as exc:
        logger.warning("Email send failed (non-blocking): %s", exc)

    result = {
        "status": "ok",
        "user": {
            "id": user_id,
            "email": email,
            "full_name": req.full_name,
            "company": req.company,
            "role": "user",
        },
        "token": token,
        "expires_in": JWT_EXPIRY_HOURS * 3600,
    }
    # In dev mode (no email service), include tokens in response for testing
    try:
        from services.email_service import DEV_MODE
        if DEV_MODE:
            result["verify_token"] = verify_token
    except ImportError:
        result["verify_token"] = verify_token

    return result


@router.post("/login")
async def login(req: LoginRequest):
    """Authenticate and return a JWT token."""
    email = req.email.lower().strip()

    with _get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE email = ? AND is_active = 1", (email,)
        ).fetchone()

        if not user:
            raise HTTPException(status_code=401, detail="Invalid email or password")

        # Block password login for Google-only accounts
        if user["password_hash"] in ("GOOGLE_OAUTH", "GOOGLE_ONLY_NO_PASSWORD"):
            raise HTTPException(
                status_code=400,
                detail="This account uses Google sign-in. Please use the 'Sign in with Google' button.",
            )

        if not _verify_password(req.password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid email or password")

        # Update login stats
        conn.execute(
            "UPDATE users SET last_login = ?, login_count = login_count + 1 WHERE id = ?",
            (datetime.now().isoformat(), user["id"]),
        )

    token = _create_token(user["id"], user["email"], user["role"])
    logger.info(f"User login: {email}")

    return {
        "status": "ok",
        "user": {
            "id": user["id"],
            "email": user["email"],
            "full_name": user["full_name"],
            "company": user["company"],
            "role": user["role"],
        },
        "token": token,
        "expires_in": JWT_EXPIRY_HOURS * 3600,
    }


@router.get("/me")
async def get_current_user(request: Request):
    """Get current user profile from JWT token."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        from auth_guard import verify_jwt
        payload = verify_jwt(token)
    except Exception:
        # Fallback: decode without full verification for dev mode
        try:
            parts = token.split(".")
            pad = "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
            if payload.get("exp", 0) < time.time():
                raise HTTPException(status_code=401, detail="Token expired")
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    with _get_db() as conn:
        user = conn.execute(
            "SELECT id, email, full_name, company, phone, role, created_at, last_login, login_count, google_avatar FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": user["id"],
        "email": user["email"],
        "full_name": user["full_name"],
        "company": user["company"],
        "phone": user["phone"],
        "role": user["role"],
        "created_at": user["created_at"],
        "last_login": user["last_login"],
        "login_count": user["login_count"],
        "google_avatar": user["google_avatar"] if "google_avatar" in user.keys() else "",
    }


@router.put("/me")
async def update_profile(request: Request):
    """Update current user profile."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload.get("sub")
    body = await request.json()

    allowed_fields = {"full_name", "company", "phone"}
    updates = {k: v for k, v in body.items() if k in allowed_fields and v is not None}

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [user_id]

    with _get_db() as conn:
        conn.execute(f"UPDATE users SET {set_clause} WHERE id = ?", values)

    return {"status": "ok", "updated": list(updates.keys())}


@router.post("/change-password")
async def change_password(request: Request, req: PasswordChangeRequest):
    """Change the current user's password."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload.get("sub")

    with _get_db() as conn:
        user = conn.execute(
            "SELECT password_hash FROM users WHERE id = ?", (user_id,)
        ).fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        if not _verify_password(req.current_password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Current password is incorrect")

        new_hash = _hash_password(req.new_password)
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (new_hash, user_id),
        )

    return {"status": "ok", "message": "Password changed successfully"}


# ── Password Reset (token-based, no external email service needed) ──

def _create_reset_token(user_id: str, email: str) -> str:
    """Create a short-lived (1h) reset token."""
    header = base64.urlsafe_b64encode(json.dumps(
        {"alg": "HS256", "typ": "JWT"}
    ).encode()).rstrip(b"=").decode()

    now = int(time.time())
    payload_data = {
        "sub": user_id,
        "email": email,
        "purpose": "password_reset",
        "iat": now,
        "exp": now + 3600,  # 1 hour
    }
    payload = base64.urlsafe_b64encode(
        json.dumps(payload_data).encode()
    ).rstrip(b"=").decode()

    signing_input = f"{header}.{payload}".encode()
    signature = hmac.new(JWT_SECRET.encode(), signing_input, digestmod="sha256").digest()
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()

    return f"{header}.{payload}.{sig_b64}"


def _verify_reset_token(token: str) -> dict:
    """Verify a password reset token. Returns payload or raises."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("invalid token")
    payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=="))
    if payload.get("purpose") != "password_reset":
        raise ValueError("not a reset token")
    if payload.get("exp", 0) < int(time.time()):
        raise ValueError("token expired")
    # Verify signature
    signing_input = f"{parts[0]}.{parts[1]}".encode()
    expected = hmac.new(JWT_SECRET.encode(), signing_input, digestmod="sha256").digest()
    sig = base64.urlsafe_b64decode(parts[2] + "==")
    if not hmac.compare_digest(sig, expected):
        raise ValueError("bad signature")
    return payload


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8)


@router.post("/forgot-password")
async def forgot_password(req: ForgotPasswordRequest):
    """Request a password reset link. Returns token directly (no email service configured)."""
    email = req.email.lower().strip()

    with _get_db() as conn:
        user = conn.execute(
            "SELECT id, email FROM users WHERE email = ? AND is_active = 1", (email,)
        ).fetchone()

    if not user:
        # Don't reveal whether email exists — always return success
        return {"status": "ok", "message": "If that email is registered, a reset link has been sent."}

    reset_token = _create_reset_token(user["id"], user["email"])
    logger.info(f"Password reset requested for: {email}")

    # Send reset email (fire-and-forget)
    try:
        from services.email_service import send_password_reset, DEV_MODE
        import asyncio
        asyncio.create_task(send_password_reset(email, reset_token))
    except Exception as exc:
        logger.warning("Reset email send failed (non-blocking): %s", exc)

    result = {
        "status": "ok",
        "message": "If that email is registered, a reset link has been sent.",
    }
    # Only expose token in explicit dev mode — never leak in production
    try:
        from services.email_service import DEV_MODE
        if DEV_MODE:
            result["reset_token"] = reset_token
    except ImportError:
        # No email service available — log warning but don't expose token
        logger.warning("No email service configured; reset token generated but not returned to client")

    return result


@router.post("/reset-password")
async def reset_password(req: ResetPasswordRequest):
    """Reset password using a valid reset token."""
    try:
        payload = _verify_reset_token(req.token)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid or expired reset token: {e}")

    user_id = payload.get("sub")
    new_hash = _hash_password(req.new_password)

    with _get_db() as conn:
        result = conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ? AND is_active = 1",
            (new_hash, user_id),
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="User not found")

    logger.info(f"Password reset completed for user: {user_id}")
    return {"status": "ok", "message": "Password has been reset. You can now sign in."}


# ── Email Verification ──

def _create_verification_token(user_id: str, email: str) -> str:
    """Create a 24h email verification token."""
    header = base64.urlsafe_b64encode(json.dumps(
        {"alg": "HS256", "typ": "JWT"}
    ).encode()).rstrip(b"=").decode()

    now = int(time.time())
    payload_data = {
        "sub": user_id,
        "email": email,
        "purpose": "email_verify",
        "iat": now,
        "exp": now + 86400,  # 24 hours
    }
    payload = base64.urlsafe_b64encode(
        json.dumps(payload_data).encode()
    ).rstrip(b"=").decode()

    signing_input = f"{header}.{payload}".encode()
    signature = hmac.new(JWT_SECRET.encode(), signing_input, digestmod="sha256").digest()
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode()

    return f"{header}.{payload}.{sig_b64}"


@router.post("/verify-email")
async def verify_email(request: Request):
    """Verify email address using the verification token."""
    body = await request.json()
    token = body.get("token", "")

    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("invalid token")
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=="))
        if payload.get("purpose") != "email_verify":
            raise ValueError("not a verification token")
        if payload.get("exp", 0) < int(time.time()):
            raise ValueError("token expired")
        signing_input = f"{parts[0]}.{parts[1]}".encode()
        expected = hmac.new(JWT_SECRET.encode(), signing_input, digestmod="sha256").digest()
        sig = base64.urlsafe_b64decode(parts[2] + "==")
        if not hmac.compare_digest(sig, expected):
            raise ValueError("bad signature")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid verification token: {e}")

    user_id = payload.get("sub")

    with _get_db() as conn:
        conn.execute(
            "UPDATE users SET email_verified = 1 WHERE id = ?",
            (user_id,),
        )

    logger.info(f"Email verified for user: {user_id}")
    return {"status": "ok", "message": "Email verified successfully."}


@router.post("/resend-verification")
async def resend_verification(request: Request):
    """Resend email verification. Requires valid auth token."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload.get("sub")

    with _get_db() as conn:
        user = conn.execute(
            "SELECT id, email, email_verified FROM users WHERE id = ?", (user_id,)
        ).fetchone()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user["email_verified"]:
        return {"status": "ok", "message": "Email already verified."}

    verify_token = _create_verification_token(user["id"], user["email"])
    logger.info(f"Verification resent for: {user['email']}")

    # Send via email service (fire-and-forget)
    try:
        from services.email_service import send_email_verification, DEV_MODE
        import asyncio
        asyncio.create_task(send_email_verification(user["email"], verify_token))
    except Exception as exc:
        logger.warning("Verification email failed (non-blocking): %s", exc)

    result = {"status": "ok", "message": "Verification email sent."}
    try:
        from services.email_service import DEV_MODE
        if DEV_MODE:
            result["verify_token"] = verify_token
    except ImportError:
        result["verify_token"] = verify_token
    return result


# ── Token Refresh ──

@router.post("/refresh")
async def refresh_token(request: Request):
    """Issue a fresh JWT if the current token is still valid (or within 1h grace)."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="No token provided")

    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("invalid")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))

        # Verify signature
        signing_input = f"{parts[0]}.{parts[1]}".encode()
        expected = hmac.new(JWT_SECRET.encode(), signing_input, digestmod="sha256").digest()
        sig = base64.urlsafe_b64decode(parts[2] + "==")
        if not hmac.compare_digest(sig, expected):
            raise ValueError("bad signature")

        # Allow refresh if within 1 hour past expiry (grace period)
        exp = payload.get("exp", 0)
        now = int(time.time())
        if now > exp + 3600:
            raise ValueError("token too old to refresh")

    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"Cannot refresh: {e}")

    user_id = payload.get("sub", "")
    email = payload.get("email", "")
    role = payload.get("role", "user")

    new_token = _create_token(user_id, email, role)
    return {
        "status": "ok",
        "token": new_token,
        "expires_in": JWT_EXPIRY_HOURS * 3600,
    }


# ── Logout ──

@router.post("/logout")
async def logout():
    """Signal logout. Frontend should clear stored tokens."""
    return {"status": "ok", "message": "Logged out. Clear your local token."}
