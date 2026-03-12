import os
import time
import hmac
import json
import base64
import logging
from typing import Optional, Dict

from fastapi import Request, HTTPException

JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_AUD = os.getenv("JWT_AUD", None)
JWT_ISS = os.getenv("JWT_ISS", None)

logger = logging.getLogger(__name__)


class AuthError(Exception):
    pass


def _b64decode(data: str) -> bytes:
    pad = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + pad)


def verify_jwt(token: str) -> Dict:
    if not JWT_SECRET:
        logger.warning("JWT_SECRET not configured — rejecting token")
        raise AuthError("authentication not configured")
    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("invalid token")
    header = json.loads(_b64decode(parts[0]))
    payload = json.loads(_b64decode(parts[1]))
    sig = _b64decode(parts[2])
    signing_input = ".".join(parts[0:2]).encode()
    expected = hmac.new(JWT_SECRET.encode(), signing_input, digestmod="sha256").digest()
    if not hmac.compare_digest(sig, expected):
        raise AuthError("bad signature")
    if "exp" in payload and int(payload["exp"]) < int(time.time()):
        raise AuthError("expired")
    if JWT_AUD and payload.get("aud") != JWT_AUD:
        raise AuthError("aud mismatch")
    if JWT_ISS and payload.get("iss") != JWT_ISS:
        raise AuthError("iss mismatch")
    return payload


def extract_user_id(token: str) -> Optional[str]:
    try:
        payload = verify_jwt(token)
        return str(payload.get("sub")) if payload else None
    except Exception:
        return None


async def get_current_user(request: Request) -> Dict:
    """FastAPI dependency — extract and verify the current user from JWT."""
    auth = request.headers.get("Authorization", "")
    token = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else ""
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = verify_jwt(token)
        return payload
    except AuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc))

