"""
Owner enrichment pipeline (backend-only).

- LA County assessor lookup (owner name + mailing) via ArcGIS REST.
- Optional reverse lookup (phones/emails) via 1Lookup (requires ONE_LOOKUP_API_KEY env).
- SQLite batch enrichment job for permits missing phone/email.

Design:
- County-specific accessor functions can be added (e.g., get_sd_owner).
- Reverse lookup is optional; if no key or no result, we return only assessor data.
"""

from __future__ import annotations

import asyncio
import os
import requests
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ONE_LOOKUP_API_KEY = os.getenv("ONE_LOOKUP_API_KEY", "")
ENRICH_DB_PATH = os.getenv("ENRICH_DB_PATH", "leads.db")
ENRICH_BATCH_SIZE = int(os.getenv("ENRICH_BATCH_SIZE", "50"))

# 1Lookup endpoint (example). Operator must supply API key & accept ToS.
ONE_LOOKUP_PERSON_URL = "https://api.1lookup.io/v1/person"


# ---------------------------------------------------------------------------
# LA County Assessor (ArcGIS REST) — owner + mailing
# ---------------------------------------------------------------------------
def get_la_owner(apn: str) -> dict:
    """
    Look up owner name + mailing address from LA County Assessor (ArcGIS REST)
    using the APN (Assessor's Parcel Number).
    Returns a dict with owner_name, mailing_address, apn, situs_address.
    LA County only.
    """
    if not apn:
        return {}

    url = (
        "https://gis.lacounty.gov/arcgis/rest/services/"
        "Assessor_OpenData/AssessorParcel/MapServer/0/query"
    )
    params = {
        "where": f"APN='{apn}'",
        "outFields": "OWNER_NAME,MAILING_ADDRESS,APN,SITUS_ADDRESS",
        "f": "json",
        "returnGeometry": "false",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # pragma: no cover - network
        logger.warning("LA assessor lookup failed for APN %s: %s", apn, exc)
        return {}

    if not data.get("features"):
        return {}

    attrs = data["features"][0]["attributes"]
    return {
        "owner_name": attrs.get("OWNER_NAME"),
        "mailing_address": attrs.get("MAILING_ADDRESS"),
        "apn": attrs.get("APN"),
        "situs_address": attrs.get("SITUS_ADDRESS"),
    }


# ---------------------------------------------------------------------------
# Reverse lookup (phones/emails) via 1Lookup — optional
# ---------------------------------------------------------------------------
def reverse_lookup_owner(owner_name: str, mailing_address: str) -> Dict:
    """
    Use the 1Lookup reverse-lookup API to enrich an owner record
    with phone numbers and email addresses.

    The API key MUST be supplied via env ONE_LOOKUP_API_KEY.
    Returns a dict with phones (list), emails (list), confidence score,
    or {} if nothing is found or key is missing.
    """
    if not owner_name or not mailing_address:
        return {}
    if not ONE_LOOKUP_API_KEY:
        return {}

    params = {
        "api_key": ONE_LOOKUP_API_KEY,
        "name": owner_name,
        "address": mailing_address,
        "state": "CA",
    }
    try:
        resp = requests.get(ONE_LOOKUP_PERSON_URL, params=params, timeout=30)
        resp.raise_for_status()
        if not resp.text:
            return {}
        data = resp.json()
    except Exception as exc:  # pragma: no cover - network
        logger.warning("1Lookup reverse lookup failed for '%s': %s", owner_name, exc)
        return {}

    return {
        "phones": data.get("phone", []),
        "emails": data.get("email", []),
        "confidence": data.get("risk_score"),
    }


# ---------------------------------------------------------------------------
# Combined enrichment for a single permit (LA only for now)
# ---------------------------------------------------------------------------
def process_permit_lead(apn: str) -> dict:
    """
    Full enrichment pipeline for a single permit lead in Los Angeles County:
      1) APN -> LA Assessor -> owner name + mailing address
      2) Owner + mailing -> reverse lookup -> phone + email (if API key set)
    Returns merged dict or {} on failure.
    """
    owner = get_la_owner(apn)
    if not owner:
        return {}

    contact = reverse_lookup_owner(
        owner_name=owner.get("owner_name"),
        mailing_address=owner.get("mailing_address"),
    )
    owner.update(contact)
    return owner


# ---------------------------------------------------------------------------
# Batch enrichment job (SQLite example)
# ---------------------------------------------------------------------------
def enrich_pending_leads(batch_size: int | None = None):
    """
    Background job / cron task.
    - Select up to batch_size permits where owner_phone & owner_email are missing.
    - For each, run process_permit_lead(APN).
    - Update the row with owner_name, primary phone, primary email, enriched_at.
    """
    bsize = batch_size or ENRICH_BATCH_SIZE
    if not os.path.exists(ENRICH_DB_PATH):
        logger.info("Enrichment skipped: DB not found at %s", ENRICH_DB_PATH)
        return

    conn = sqlite3.connect(ENRICH_DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='permits'
        """
    )
    if not cur.fetchone():
        logger.debug("Enrichment skipped: permits table missing in %s", ENRICH_DB_PATH)
        conn.close()
        return

    cur.execute(
        """
        SELECT id, apn FROM permits
        WHERE (owner_phone IS NULL OR owner_phone = '')
          AND (owner_email IS NULL OR owner_email = '')
        LIMIT ?
        """,
        (bsize,),
    )
    rows = cur.fetchall()

    for lead_id, apn in rows:
        try:
            enriched = process_permit_lead(apn)
            if not enriched:
                continue
            phones = enriched.get("phones") or []
            emails = enriched.get("emails") or []
            primary_phone = phones[0] if phones else None
            primary_email = emails[0] if emails else None

            cur.execute(
                """
                UPDATE permits
                SET owner_name  = ?,
                    owner_phone = ?,
                    owner_email = ?,
                    enriched_at = ?
                WHERE id = ?
                """,
                (
                    enriched.get("owner_name"),
                    primary_phone,
                    primary_email,
                    datetime.utcnow().isoformat(),
                    lead_id,
                ),
            )
        except Exception as exc:
            logger.warning("Failed to enrich APN %s: %s", apn, exc)
            continue

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Optional periodic loop helper (can be scheduled by caller)
# ---------------------------------------------------------------------------
async def enrich_loop(interval_seconds: int = 600):
    """
    Async loop to periodically enrich pending leads.
    Runs sync enrichment in a thread to avoid blocking the event loop.
    """
    import asyncio
    while True:
        try:
            # Run in thread pool to prevent blocking API responses
            await asyncio.to_thread(enrich_pending_leads)
        except Exception as exc:  # pragma: no cover
            logger.warning("Enrich loop error: %s", exc)
        finally:
            await asyncio.sleep(interval_seconds)
