"""
Master enrichment pipeline (configurable, backend-only).

- County assessor lookup (owner name/address) via county-specific functions.
- Reverse lookup providers (1Lookup + optional alternate) if API keys are set.
- Optional property API enrichment.
- Optional geocoding (not required for UI; map already has coords).

Everything is keyed by environment variables; no secrets are hard-coded.
All network failures are swallowed and result in graceful empty outputs.
"""

from __future__ import annotations

import os
import json
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import re

logger = logging.getLogger(__name__)

from settings import (
    ONE_LOOKUP_API_KEY,
    ONE_LOOKUP_URL,
    ALT_LOOKUP_URL,
    ALT_LOOKUP_KEY,
    PROPERTY_API_URL,
    PROPERTY_API_KEY,
    GEOCODE_API_KEY,
    DB_PATH,
    ENRICH_BATCH_SIZE,
    HTTP_TIMEOUT,
    CONTACT_CONFIDENCE_THRESHOLD,
    LA_ASSESSOR_URL,
    SOCRATA_APP_TOKEN,
)

# Allow overriding the LA assessor endpoint via env (legacy)
LA_ASSESSOR_URL = LA_ASSESSOR_URL or "https://portal.assessor.lacounty.gov/api/parceldetail/{apn}"

# LA assessor via Socrata (parcel owner dataset)
LA_SOCRATA_RESOURCE = os.getenv("LA_SOCRATA_RESOURCE", "qv4c-k9xz")
LA_SOCRATA_DOMAIN = os.getenv("LA_SOCRATA_DOMAIN", "https://data.lacity.org")

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------

def normalize_la_apn(raw: str) -> str:
    """Normalize LA APN to 0000-000-000."""
    digits = re.sub(r"\\D", "", raw or "")
    if len(digits) != 10:
        return ""
    return f"{digits[0:4]}-{digits[4:7]}-{digits[7:10]}"

# ---------------------------------------------------------------------------
# County assessor dispatch
# ---------------------------------------------------------------------------

def get_owner_from_assessor(county: str, apn_raw: str, address: str = "") -> Optional[Dict[str, Any]]:
    """Dispatch to county-specific assessor lookup."""
    county_low = (county or "").lower()
    if "los angeles" in county_low:
        return get_la_owner(apn_raw)
    if "san diego" in county_low:
        return get_sd_owner(apn_raw)
    # Unknown county: return None to allow fallback strategies later
    return None


def get_la_owner(apn: str) -> Optional[Dict[str, Any]]:
    """LA County assessor lookup (portal.assessor)."""
    if not apn:
        return None
    clean_apn = normalize_la_apn(apn)

    # First try Socrata owner dataset (JSON)
    socrata_url = f"{LA_SOCRATA_DOMAIN}/resource/{LA_SOCRATA_RESOURCE}.json"
    headers = {}
    token = os.getenv("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token
    params_list = []
    digits = clean_apn.replace("-", "")
    params_list.append({"apn": clean_apn, "$limit": 1})
    params_list.append({"apn": digits, "$limit": 1})
    params_list.append({"ain": digits, "$limit": 1})

    for params in params_list:
        try:
            resp = requests.get(socrata_url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200 or not resp.text:
                continue
            data = resp.json()
            if not data:
                continue
            row = data[0]
            owner = row.get("owner") or row.get("owner_name") or row.get("primary_owner")
            mailing = (
                row.get("mailing_address")
                or row.get("mailing_street")
                or row.get("mailing_address1")
                or row.get("mailing_address_line_1")
            )
            situs = row.get("situs_address") or row.get("situs_addr")
            if owner:
                return {
                    "owner_name": str(owner).strip(),
                    "owner_address": mailing,
                    "apn": row.get("apn") or row.get("ain") or clean_apn,
                    "situs_address": situs,
                    "raw": row,
                    "county": "Los Angeles",
                    "source": "LA Socrata owner dataset",
                }
        except Exception as exc:  # pragma: no cover
            logger.debug("LA Socrata lookup failed: %s", exc)
            continue

    # Fallback to legacy HTML endpoint (likely to fail)
    url = LA_ASSESSOR_URL.format(apn=clean_apn)
    try:
        resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={"Accept": "application/json"})
        resp.raise_for_status()
        if "json" in (resp.headers.get("Content-Type") or ""):
            data = resp.json()
            owner = data.get("OwnerName") or data.get("owner")
            if owner:
                return {
                    "owner_name": owner,
                    "owner_address": data.get("MailingAddress") or data.get("SitusAddress"),
                    "apn": data.get("AIN") or clean_apn,
                    "situs_address": data.get("SitusAddress"),
                    "raw": data,
                    "county": "Los Angeles",
                    "source": "LA Assessor portal JSON",
                }
    except Exception as exc:  # pragma: no cover - network
        logger.debug("LA assessor HTML/JSON fallback failed: %s", exc)

    return None


def get_sd_owner(apn: str) -> Optional[Dict[str, Any]]:
    """San Diego County assessor via SANDAG ArcGIS (if reachable)."""
    if not apn:
        return None
    url = "https://sdgis.sandag.org/arcgis/rest/services/Assessor/Parcels/MapServer/0/query"
    apn_clean = apn.replace("-", "").replace(" ", "")
    clauses = [f"APN='{apn}'", f"APN='{apn_clean}'"]
    for clause in clauses:
        params = {
            "where": clause,
            "outFields": "*",
            "f": "json",
            "returnGeometry": "false",
        }
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            feats = payload.get("features") or []
            if not feats:
                continue
            attrs = feats[0].get("attributes", {})
            owner = attrs.get("OWNER_NAME")
            if not owner:
                continue
            return {
                "owner_name": owner,
                "owner_address": attrs.get("MAIL_ADDRESS"),
                "apn": attrs.get("APN") or apn_clean,
                "situs_address": attrs.get("SITUSADDR"),
                "raw": attrs,
                "county": "San Diego",
            }
        except Exception as exc:  # pragma: no cover
            logger.debug("SD assessor lookup attempt failed: %s", exc)
            continue
    return None


# ---------------------------------------------------------------------------
# Reverse lookup providers
# ---------------------------------------------------------------------------

def reverse_lookup_1lookup(owner_name: str, owner_address: str) -> Dict[str, Any]:
    if not owner_name or not owner_address or not ONE_LOOKUP_API_KEY:
        return {}
    try:
        resp = requests.get(
            ONE_LOOKUP_URL,
            params={
                "api_key": ONE_LOOKUP_API_KEY,
                "name": owner_name,
                "address": owner_address,
                "state": "CA",
            },
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json() if resp.text else {}
        return {
            "phones": data.get("phone", []),
            "emails": data.get("email", []),
            "confidence": data.get("risk_score"),
            "provider": "1lookup",
            "raw": data,
        }
    except Exception as exc:  # pragma: no cover
        logger.debug("1Lookup lookup failed: %s", exc)
        return {}


def reverse_lookup_alt(owner_name: str, owner_address: str) -> Dict[str, Any]:
    """Optional secondary provider, fully configurable via env."""
    if not owner_name or not owner_address or not ALT_LOOKUP_URL or not ALT_LOOKUP_KEY:
        return {}
    payload = {
        "api_key": ALT_LOOKUP_KEY,
        "name": owner_name,
        "address": owner_address,
    }
    try:
        resp = requests.post(ALT_LOOKUP_URL, json=payload, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json() if resp.text else {}
        return {
            "phones": data.get("phones", []),
            "emails": data.get("emails", []),
            "confidence": data.get("confidence"),
            "provider": "altlookup",
            "raw": data,
        }
    except Exception as exc:  # pragma: no cover
        logger.debug("Alt lookup failed: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Optional property API enrichment
# ---------------------------------------------------------------------------
def enrich_from_property_api(owner_address: str, apn_raw: str, county: str) -> Dict[str, Any]:
    if not PROPERTY_API_URL or not PROPERTY_API_KEY or not owner_address:
        return {}
    params = {
        "api_key": PROPERTY_API_KEY,
        "address": owner_address,
        "apn": apn_raw,
        "county": county,
    }
    try:
        resp = requests.get(PROPERTY_API_URL, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json() if resp.text else {}
        return data or {}
    except Exception as exc:  # pragma: no cover
        logger.debug("Property API failed: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Merge contact results
# ---------------------------------------------------------------------------
def merge_contact_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not results:
        return {}
    # Simple priority: highest confidence first; then first provider order
    sorted_res = sorted(
        results,
        key=lambda r: (r.get("confidence") or 0, len(r.get("phones") or []) + len(r.get("emails") or [])),
        reverse=True,
    )
    phones: List[str] = []
    emails: List[str] = []
    sources: List[Dict[str, Any]] = []
    for r in sorted_res:
        for p in r.get("phones") or []:
            if p not in phones:
                phones.append(p)
        for e in r.get("emails") or []:
            if e not in emails:
                emails.append(e)
        sources.append({"provider": r.get("provider"), "confidence": r.get("confidence"), "raw": r.get("raw")})

    return {
        "phone_primary": phones[0] if phones else None,
        "phone_alt": phones[1] if len(phones) > 1 else None,
        "email_primary": emails[0] if emails else None,
        "email_alt": emails[1] if len(emails) > 1 else None,
        "sources": sources,
    }


def pick_final_contact(results: List[Dict[str, Any]], confidence_threshold: float = CONTACT_CONFIDENCE_THRESHOLD) -> Dict[str, Any]:
    """
    Apply guardrails:
      - Accept if max confidence >= threshold OR there is intersection across providers.
      - Otherwise, return empty contacts with accepted=False.
    """
    if not results:
        return {"accepted": False}

    merged = merge_contact_results(results)
    primary_phone = merged.get("phone_primary")
    primary_email = merged.get("email_primary")

    max_conf = max((r.get("confidence") or 0 for r in results), default=0)

    intersection_ok = False
    phone_sets = [set(r.get("phones", [])) for r in results if r.get("phones")]
    if len(phone_sets) > 1:
        common = set.intersection(*phone_sets)
        intersection_ok = bool(common)

    if (max_conf < confidence_threshold) and not intersection_ok:
        return {
            "phone_primary": None,
            "phone_alt": None,
            "email_primary": None,
            "email_alt": None,
            "confidence": max_conf,
            "accepted": False,
            "sources": merged.get("sources", []),
        }

    merged["confidence"] = max_conf
    merged["accepted"] = True
    return merged


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def enrich_permit(county: str, apn_raw: str, address: str = "") -> Dict[str, Any]:
    """
    Master pipeline for one permit:
      - Uses county-specific assessor to get owner name/address.
      - Uses 1Lookup + optional alternate reverse lookup to get phones/emails.
      - Optionally uses property API.
      - Merges all contact results.
    Returns {} if owner cannot be resolved.
    """
    assessor_owner = get_owner_from_assessor(county, apn_raw, address)
    if not assessor_owner:
        return {}

    owner_name = assessor_owner.get("owner_name")
    owner_address = assessor_owner.get("owner_address") or address

    res_list: List[Dict[str, Any]] = []
    r1 = reverse_lookup_1lookup(owner_name, owner_address)
    if r1:
        res_list.append(r1)
    r2 = reverse_lookup_alt(owner_name, owner_address)
    if r2:
        res_list.append(r2)

    prop = enrich_from_property_api(owner_address, apn_raw, county)
    if prop:
        res_list.append({
            "phones": prop.get("phones", []),
            "emails": prop.get("emails", []),
            "confidence": prop.get("confidence", 0.5),
            "provider": "property_api",
            "raw": prop,
        })

    contacts = pick_final_contact(res_list) if res_list else {"accepted": False}

    # Only keep contacts when accepted; otherwise blank out contact fields
    if not contacts.get("accepted"):
        contacts = {
            "accepted": False,
            "phone_primary": None,
            "phone_alt": None,
            "email_primary": None,
            "email_alt": None,
            "confidence": contacts.get("confidence", 0),
            "sources": contacts.get("sources", []),
        }

    return {
        "assessor": assessor_owner,
        "contacts": contacts,
    }


# ---------------------------------------------------------------------------
# Batch enrichment (SQLite)
# ---------------------------------------------------------------------------
def enrich_pending_leads(batch_size: int = None, db_path: str = None):
    """Background job to fill missing contact info in permits table."""
    from sqlite3 import connect

    path = db_path or DB_PATH
    bsize = batch_size or ENRICH_BATCH_SIZE
    if not os.path.exists(path):
        logger.info("Enrichment skipped: DB not found at %s", path)
        return

    conn = connect(path)
    conn.row_factory = connect(":memory:").row_factory  # preserve Row type
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='permits'")
    if not cur.fetchone():
        logger.info("Enrichment skipped: permits table missing in %s", path)
        conn.close()
        return

    cur.execute(
        """
        SELECT id, apn_raw, county, address
        FROM permits
        WHERE (phone_primary IS NULL OR phone_primary = '')
          AND (email_primary IS NULL OR email_primary = '')
        LIMIT ?
        """,
        (bsize,),
    )
    rows = cur.fetchall()

    for row in rows:
        try:
            lead_id, apn_raw, county, addr = row
            enriched = enrich_permit(county or "", apn_raw or "", addr or "")
            if not enriched:
                continue
            assessor = enriched.get("assessor", {})
            contacts = enriched.get("contacts", {})

            sources = contacts.get("sources", []) or []
            source_reverse1 = next((s for s in sources if s.get("provider") == "1lookup"), {})
            source_reverse2 = next((s for s in sources if s.get("provider") == "altlookup"), {})
            source_property = next((s for s in sources if s.get("provider") == "property_api"), {})

            cur.execute(
                """
                UPDATE permits
                SET
                    apn_normalized = ?,
                    owner_name     = ?,
                    owner_address  = ?,
                    phone_primary  = ?,
                    phone_alt      = ?,
                    email_primary  = ?,
                    email_alt      = ?,
                    source_assessor= ?,
                    source_reverse1= ?,
                    source_reverse2= ?,
                    source_property= ?,
                    enriched_at    = ?
                WHERE id = ?
                """,
                (
                    assessor.get("apn"),
                    assessor.get("owner_name"),
                    assessor.get("owner_address"),
                    contacts.get("phone_primary"),
                    contacts.get("phone_alt"),
                    contacts.get("email_primary"),
                    contacts.get("email_alt"),
                    json.dumps(assessor),
                    json.dumps(source_reverse1),
                    json.dumps(source_reverse2),
                    json.dumps(source_property),
                    datetime.utcnow().isoformat(),
                    lead_id,
                ),
            )
        except Exception as exc:
            logger.warning("[ENRICH ERROR] id=%s apn=%s : %s", row[0], row[1], exc)
            continue

    conn.commit()
    conn.close()
