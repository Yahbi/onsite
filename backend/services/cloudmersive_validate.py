"""
Cloudmersive Validate API — email, phone, address, and name validation.

Free tier: 600 API calls/month (shared across all endpoints).
Auth: API key via `Apikey` header.
Cached 7 days per lookup to conserve quota.
"""

import asyncio
import hashlib
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

API_KEY = "efa28b4b-7cf6-4dab-ae39-9a2f9c61dbf7"
BASE_URL = "https://api.cloudmersive.com"

_cache: dict[str, dict] = {}
_cache_ts: dict[str, float] = {}
CACHE_TTL = 7 * 86400
_last_call = 0.0
RATE_LIMIT = 0.5  # 500ms between calls


def _cache_key(prefix: str, value: str) -> str:
    return hashlib.md5(f"cm|{prefix}|{value.lower().strip()}".encode()).hexdigest()


async def _rate_wait():
    global _last_call
    now = time.time()
    wait = RATE_LIMIT - (now - _last_call)
    if wait > 0:
        await asyncio.sleep(wait)
    _last_call = time.time()


async def _post(endpoint: str, payload) -> Optional[dict]:
    """Make authenticated POST to Cloudmersive API."""
    headers = {"Apikey": API_KEY, "Content-Type": "application/json"}
    await _rate_wait()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{BASE_URL}{endpoint}",
                headers=headers,
                json=payload,
            )
        if resp.status_code == 401:
            logger.warning("Cloudmersive API key invalid or expired")
            return None
        if resp.status_code == 429:
            logger.warning("Cloudmersive rate limit hit")
            return None
        if resp.status_code != 200:
            logger.debug(f"Cloudmersive {endpoint} {resp.status_code}: {resp.text[:200]}")
            return None
        return resp.json()
    except Exception as e:
        logger.debug(f"Cloudmersive {endpoint} error: {e}")
        return None


async def validate_email(email: str) -> Optional[dict]:
    """
    Full email validation — syntax, domain, SMTP check, disposable detection.

    Returns dict with: valid, domain, is_free, is_disposable, smtp_verified.
    """
    if not email or not API_KEY:
        return None

    ck = _cache_key("email", email)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    data = await _post("/validate/email/address/full", email.strip())
    if not data:
        return None

    out = {
        "email": email.strip().lower(),
        "valid": data.get("ValidAddress", False),
        "domain": data.get("Domain", ""),
        "is_free": data.get("IsFreeEmailProvider", False),
        "is_disposable": data.get("IsDisposable", False),
        "smtp_verified": data.get("MailServerUsedForValidation", "") != "",
        "source": "Cloudmersive",
    }

    _cache[ck] = out
    _cache_ts[ck] = time.time()
    return out


async def validate_phone(phone: str, country_code: str = "US") -> Optional[dict]:
    """
    Validate phone number syntax and type.

    Returns dict with: valid, phone_type, e164, international_format, country.
    """
    if not phone or not API_KEY:
        return None

    ck = _cache_key("phone", phone)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    payload = {
        "PhoneNumber": phone.strip(),
        "DefaultCountryCode": country_code,
    }
    data = await _post("/validate/phonenumber/basic", payload)
    if not data:
        return None

    out = {
        "phone": phone.strip(),
        "valid": data.get("IsValid", False),
        "phone_type": data.get("PhoneNumberType", "unknown"),
        "e164": data.get("E164Format", ""),
        "international_format": data.get("InternationalFormat", ""),
        "country": data.get("CountryName", ""),
        "country_code": data.get("CountryCode", ""),
        "source": "Cloudmersive",
    }

    _cache[ck] = out
    _cache_ts[ck] = time.time()
    return out


async def validate_address(
    street: str,
    city: str = "",
    state: str = "",
    postal_code: str = "",
) -> Optional[dict]:
    """
    Validate and normalize a US street address with geocoding.

    Returns dict with: valid, normalized address fields, lat, lng.
    """
    if not street or not API_KEY:
        return None

    ck = _cache_key("addr", f"{street}|{city}|{state}")
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    payload = {
        "StreetAddress": street.strip(),
        "City": city.strip(),
        "StateOrProvince": state.strip(),
        "PostalCode": postal_code.strip(),
        "CountryFullName": "United States",
        "CountryCode": "US",
    }
    data = await _post("/validate/address/street-address", payload)
    if not data:
        return None

    out = {
        "valid": data.get("IsValid", False),
        "street": data.get("Street", street),
        "city": data.get("City", city),
        "state": data.get("StateOrProvince", state),
        "postal_code": data.get("PostalCode", postal_code),
        "lat": data.get("Latitude", 0),
        "lng": data.get("Longitude", 0),
        "source": "Cloudmersive",
    }

    _cache[ck] = out
    _cache_ts[ck] = time.time()
    return out


async def parse_name(full_name: str) -> Optional[dict]:
    """
    Parse a full name into components — first, middle, last, title, suffix.

    Returns dict with: valid, first_name, middle_name, last_name, title, suffix, gender.
    """
    if not full_name or not API_KEY:
        return None

    ck = _cache_key("name", full_name)
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    payload = {"FullNameString": full_name.strip()}
    data = await _post("/validate/name/full-name", payload)
    if not data:
        return None

    out = {
        "valid": data.get("Successful", False),
        "first_name": data.get("FirstName", ""),
        "middle_name": data.get("MiddleName", ""),
        "last_name": data.get("LastName", ""),
        "title": data.get("Title", ""),
        "suffix": data.get("Suffix", ""),
        "display_name": data.get("DisplayName", full_name),
        "source": "Cloudmersive",
    }

    # Get gender from first name if available
    if out["first_name"]:
        gender_data = await _post(
            "/validate/name/get-gender",
            {"FirstName": out["first_name"]},
        )
        if gender_data:
            out["gender"] = gender_data.get("Gender", "unknown")

    _cache[ck] = out
    _cache_ts[ck] = time.time()
    return out


async def geocode_address(
    street: str, city: str = "", state: str = "", postal_code: str = "",
) -> Optional[dict]:
    """Geocode an address to lat/lng coordinates."""
    if not street or not API_KEY:
        return None

    ck = _cache_key("geo", f"{street}|{city}|{state}")
    cached = _cache.get(ck)
    if cached and (time.time() - _cache_ts.get(ck, 0)) < CACHE_TTL:
        return cached

    payload = {
        "StreetAddress": street.strip(),
        "City": city.strip(),
        "StateOrProvince": state.strip(),
        "PostalCode": postal_code.strip(),
        "CountryCode": "US",
    }
    data = await _post("/validate/address/geocode", payload)
    if not data:
        return None

    out = {
        "lat": data.get("Latitude", 0),
        "lng": data.get("Longitude", 0),
        "valid": data.get("IsValid", False),
        "source": "Cloudmersive",
    }

    _cache[ck] = out
    _cache_ts[ck] = time.time()
    return out
