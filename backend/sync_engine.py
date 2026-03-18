"""
Background sync engine — sync_data() and coordinate/state lookup helpers.
Extracted from main.py.
"""
import os
import re
import csv
import asyncio
import aiohttp
import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Cross-module imports
from core.scoring import calculate_score as _score_from_core, calculate_score_v2 as _score_from_core_v2, classify_temperature, TEMP_THRESHOLDS
from data_sources_config import (
    SOCRATA_APP_TOKEN, SOCRATA_LIMIT, SOC_DATASETS, ARCGIS_DATASETS,
    CKAN_DATASETS, CARTO_DATASETS, SKIP_LA, SKIP_LB, GEOCODE_BUDGET,
    MAX_DAYS_OLD, LEADS_RETURN_LIMIT, LEAD_VISIBILITY_DAYS,
    PROPERTYREACH_ENABLED, PROPERTYREACH_INLINE, LA_ASSESSOR_INLINE,
    ATTOM_API_KEY, SAN_JOSE_SOURCES,
    STATE_DATASETS,
)
from data_cache_manager import DataCache, _sync_lock
from api_clients import (
    LADBSClient, LongBeachClient, fetch_socrata_best,
    fetch_ckan_permits, fetch_carto_permits, GeocodingClient,
)
from lead_processing import (
    safe_float, safe_int, parse_date, fix_coordinates,
    extract_lat_lng, extract_address, extract_permit_number,
    _safe_str, _infer_state, _build_permit_url, process_ladbs_permit,
    Lead, normalize_lead_for_ui,
)
from enrichment_pipeline import fetch_arcgis_permits

# ============================================================================
# BACKGROUND SYNC
# ============================================================================

# Approximate city/state centers for hash-based coordinate fallback
_CITY_COORDS = {
    "newyork": (40.7128, -74.0060), "nyc": (40.7128, -74.0060),
    "losangeles": (34.0522, -118.2437), "la": (34.0522, -118.2437),
    "chicago": (41.8781, -87.6298), "houston": (29.7604, -95.3698),
    "phoenix": (33.4484, -112.0740), "philadelphia": (39.9526, -75.1652),
    "sanantonio": (29.4241, -98.4936), "sandiego": (32.7157, -117.1611),
    "dallas": (32.7767, -96.7970), "sanjose": (37.3382, -121.8863),
    "austin": (30.2672, -97.7431), "jacksonville": (30.3322, -81.6557),
    "fortworth": (32.7555, -97.3308), "columbus": (39.9612, -82.9988),
    "charlotte": (35.2271, -80.8431), "sanfrancisco": (37.7749, -122.4194),
    "sf": (37.7749, -122.4194), "indianapolis": (39.7684, -86.1581),
    "seattle": (47.6062, -122.3321), "denver": (39.7392, -104.9903),
    "washington": (38.9072, -77.0369), "dc": (38.9072, -77.0369),
    "nashville": (36.1627, -86.7816), "oklahomacity": (35.4676, -97.5164),
    "elpaso": (31.7619, -106.4850), "boston": (42.3601, -71.0589),
    "portland": (45.5152, -122.6784), "lasvegas": (36.1699, -115.1398),
    "memphis": (35.1495, -90.0490), "louisville": (38.2527, -85.7585),
    "baltimore": (39.2904, -76.6122), "milwaukee": (43.0389, -87.9065),
    "albuquerque": (35.0844, -106.6504), "tucson": (32.2226, -110.9747),
    "fresno": (36.7378, -119.7871), "sacramento": (38.5816, -121.4944),
    "mesa": (33.4152, -111.8315), "citymesaaz": (33.4152, -111.8315),
    "kansascity": (39.0997, -94.5786), "atlanta": (33.7490, -84.3880),
    "omaha": (41.2565, -95.9345), "raleigh": (35.7796, -78.6382),
    "miami": (25.7617, -80.1918), "miamidade": (25.7617, -80.1918),
    "cleveland": (41.4993, -81.6944), "tulsa": (36.1540, -95.9928),
    "oakland": (37.8044, -122.2712), "minneapolis": (44.9778, -93.2650),
    "tampa": (27.9506, -82.4572), "neworleans": (29.9511, -90.0715),
    "arlington": (32.7357, -97.1081), "bakersfield": (35.3733, -119.0187),
    "aurora": (39.7294, -104.8319), "anaheim": (33.8366, -117.9143),
    "honolulu": (21.3069, -157.8583), "santaana": (33.7455, -117.8677),
    "riverside": (33.9533, -117.3962), "stockton": (37.9577, -121.2908),
    "pittsburgh": (40.4406, -79.9959), "stlouis": (38.6270, -90.1994),
    "cincinnati": (39.1031, -84.5120), "anchorage": (61.2181, -149.9003),
    "detroit": (42.3314, -83.0458), "buffalo": (42.8864, -78.8784),
    "rochester": (43.1566, -77.6088), "hartford": (41.7658, -72.6734),
    "cambridge": (42.3736, -71.1097), "chattanooga": (35.0456, -85.3097),
    "richmond": (37.5407, -77.4360), "spokane": (47.6588, -117.4260),
    "boulder": (40.0150, -105.2705), "santamonica": (34.0195, -118.4912),
    "glendale": (34.1425, -118.2551), "madison": (43.0731, -89.4012),
    "batonrouge": (30.4515, -91.1871), "cookcounty": (41.8781, -87.6298),
}
_STATE_COORDS = {
    "AL": (32.36, -86.28), "AK": (64.20, -152.49), "AZ": (34.05, -111.09),
    "AR": (34.80, -92.20), "CA": (36.78, -119.42), "CO": (39.55, -105.78),
    "CT": (41.60, -72.73), "DE": (39.16, -75.52), "FL": (27.66, -81.52),
    "GA": (32.16, -82.90), "HI": (19.90, -155.58), "ID": (44.07, -114.74),
    "IL": (40.63, -89.40), "IN": (40.27, -86.13), "IA": (42.01, -93.21),
    "KS": (38.50, -98.00), "KY": (37.84, -84.27), "LA": (30.98, -91.96),
    "ME": (45.25, -69.45), "MD": (39.05, -76.64), "MA": (42.41, -71.38),
    "MI": (44.31, -85.60), "MN": (46.73, -94.69), "MS": (32.35, -89.40),
    "MO": (37.96, -91.83), "MT": (46.88, -110.36), "NE": (41.49, -99.90),
    "NV": (38.80, -116.42), "NH": (43.19, -71.57), "NJ": (40.06, -74.41),
    "NM": (34.52, -105.87), "NY": (43.30, -74.22), "NC": (35.76, -79.02),
    "ND": (47.55, -101.00), "OH": (40.42, -82.91), "OK": (35.47, -97.52),
    "OR": (43.80, -120.55), "PA": (41.20, -77.19), "RI": (41.58, -71.48),
    "SC": (33.84, -81.16), "SD": (44.30, -99.44), "TN": (35.52, -86.58),
    "TX": (31.97, -99.90), "UT": (39.32, -111.09), "VT": (44.56, -72.58),
    "VA": (37.43, -78.66), "WA": (47.75, -120.74), "WV": (38.60, -80.95),
    "WI": (43.78, -88.79), "WY": (43.08, -107.29), "DC": (38.91, -77.04),
}

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

        all_leads: List[dict] = []  # Kept for compatibility but cleared per-source
        total_accepted = 0
        sync_start_time = datetime.utcnow().isoformat()
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
        if os.getenv("USE_REGISTRY_SYNC", "1") == "1":
            try:
                from services.source_registry import get_sources_for_sync, mark_synced, retry_disabled_sources
                from services.fetchers import fetch_from_source
                max_sources = int(os.getenv("SYNC_MAX_SOURCES", "20"))  # Throttled to avoid fd exhaustion
                # Re-enable disabled sources past cooldown for retry
                try:
                    retried = retry_disabled_sources(cooldown_days=7, max_retries=20)
                    if retried:
                        logger.info("Re-enabled %d disabled sources for retry", retried)
                except Exception:
                    pass
                registry_sources = get_sources_for_sync(limit=max_sources)
                logger.info("Registry sync: %d sources to fetch", len(registry_sources))
                for src in registry_sources:
                    try:
                        records = await fetch_from_source(src, timeout_seconds=30)
                        if records:
                            logger.info("Registry %s: %d records", src.get("api_name", ""), len(records))
                            accepted = 0
                            for r in records:
                                address = extract_address(r)
                                if not address:
                                    continue
                                permit_number = extract_permit_number(r)
                                lat, lng = extract_lat_lng(r)
                                lat, lng = fix_coordinates(lat, lng) if lat and lng else (lat, lng)
                                # Skip leads without real coordinates — no fake hash-based fallback
                                # Map endpoint filters out NULL lat/lng automatically
                                permit_type = (
                                    r.get("permit_type") or r.get("record_type")
                                    or r.get("permit_type_definition") or r.get("work_type") or ""
                                )
                                # Skip non-construction permit types
                                _pt_lower = permit_type.lower()
                                if any(junk in _pt_lower for junk in (
                                    "special event", "block party", "food establishment",
                                    "business license", "filming", "animal boarding",
                                    "accommodation & food", "arts, entertainment",
                                    "tent permit (special", "valet parking",
                                    "event-temporary", "street closure"
                                )):
                                    continue
                                issue_raw = r.get("issue_date") or r.get("issued_date") or r.get("filed_date") or ""
                                issue_dt = parse_date(issue_raw)
                                issue_date = issue_dt.strftime("%Y-%m-%d") if issue_dt else ""
                                days_old = (datetime.utcnow() - issue_dt).days if issue_dt else 15
                                valuation = safe_float(
                                    r.get("valuation") or r.get("estimated_cost") or r.get("job_value") or 0
                                )
                                owner_name = _safe_str(r.get("owner_name") or r.get("applicant_name"))
                                owner_phone = _safe_str(r.get("owner_phone") or r.get("phone"))
                                owner_email = _safe_str(r.get("owner_email") or r.get("email"))
                                lead_for_score = {
                                    "days_old": days_old, "valuation": valuation, "permit_type": permit_type,
                                    "owner_name": owner_name, "owner_phone": owner_phone, "owner_email": owner_email,
                                    "city": r.get("city", "") or r.get("jurisdiction", "") or src.get("location", ""),
                                    "state": r.get("state", "") or src.get("state", "") or _infer_state(src.get("location", "")),
                                }
                                score, temp, _ = _score_from_core_v2(lead_for_score)
                                urgency = "HIGH" if days_old <= 7 and score >= 30 else "MEDIUM" if days_old <= 14 else "LOW"
                                lead = {
                                    "id": abs(hash(f"reg:{src.get('id', '')}:{permit_number}:{address}")),
                                    "permit_number": permit_number,
                                    "address": address,
                                    "city": lead_for_score["city"],
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
                                    "owner_name": owner_name,
                                    "owner_phone": owner_phone,
                                    "owner_email": owner_email,
                                    "state": lead_for_score["state"],
                                }
                                all_leads.append(lead)
                                accepted += 1
                            logger.info("Registry %s: %d accepted / %d raw", src.get("api_name", ""), accepted, len(records))
                            # Batch-save per source to prevent OOM
                            if all_leads:
                                try:
                                    import asyncio as _aio
                                    await _aio.to_thread(DataCache.save, all_leads)
                                    total_accepted += len(all_leads)
                                    all_leads.clear()  # Free memory immediately
                                except Exception as _save_err:
                                    logger.error("Per-source save for %s failed: %s", src.get("api_name", ""), _save_err)
                            try:
                                mark_synced(src["id"], accepted)
                            except Exception:
                                pass
                        else:
                            try:
                                mark_synced(src["id"], 0, "No records returned")
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning("Registry source %s error: %s", src.get("api_name", ""), e)
                        try:
                            mark_synced(src["id"], 0, str(e)[:200])
                        except Exception:
                            pass
                # Save any remaining leads from registry sync
                if all_leads:
                    try:
                        await asyncio.to_thread(DataCache.save, all_leads)
                        total_accepted += len(all_leads)
                        all_leads.clear()
                    except Exception as save_err:
                        logger.error("Registry final save failed: %s", save_err)
                logger.info("Registry sync complete: %d total leads saved", total_accepted)
            except Exception as e:
                logger.error("Registry sync failed, falling back to config: %s", e)
        else:
            # Original config-driven sync (fallback)
            pass

        # Fetch from verified Socrata datasets
        if os.getenv("USE_REGISTRY_SYNC", "1") != "1":
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


        # ── Batch save to prevent memory accumulation ──
        if all_leads:
            try:
                await asyncio.to_thread(DataCache.save, all_leads)
                total_accepted += len(all_leads)
                logger.info("Batch save: %d leads persisted", len(all_leads))
                all_leads.clear()
            except Exception as _bs_err:
                logger.error("Batch save failed: %s", _bs_err)

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

        # ── Batch save to prevent memory accumulation ──
        if all_leads:
            try:
                await asyncio.to_thread(DataCache.save, all_leads)
                total_accepted += len(all_leads)
                logger.info("Batch save: %d leads persisted", len(all_leads))
                all_leads.clear()
            except Exception as _bs_err:
                logger.error("Batch save failed: %s", _bs_err)

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
            # Invalidate map cache so new data appears
            _map_cache.clear()

            # ── WebSocket notifications for new/hot leads ──
            try:
                from routes.websocket import notify_new_lead, notify_hot_lead
                from models.database import create_notification
                hot_leads = [l for l in all_leads if l.get("temperature") == "hot"]
                for hl in hot_leads[:10]:  # Cap broadcast to avoid flooding
                    await notify_hot_lead(hl)
                    create_notification(
                        "hot_lead", hl.get("id"),
                        f"HOT LEAD: {hl.get('address', '')} — Score {hl.get('score', 0)}, ${hl.get('valuation', 0):,.0f}"
                    )
                if len(all_leads) > 0:
                    await notify_new_lead({"address": f"{len(all_leads)} leads synced", "city": "", "score": 0})
            except Exception as e:
                logger.warning(f"WebSocket notification error (non-fatal): {e}")

            # Auto-enrich ownership from free Regrid parcel tiles (background, non-blocking)
            try:
                from services.parcel_enrichment import enrich_leads_from_tiles
                stats = await enrich_leads_from_tiles(all_leads, max_tiles=1000)
                if stats.get("enriched", 0) > 0:
                    await asyncio.to_thread(DataCache.save, all_leads)
                    logger.info(f"Post-sync ownership enrichment: {stats['enriched']:,} leads enriched")
            except Exception as e:
                logger.warning(f"Post-sync enrichment error (non-fatal): {e}")

        lead_count = len(all_leads)
        # Explicitly free the large list to prevent memory leak
        del all_leads
        import gc
        gc.collect()
        logger.info(f"Sync complete: {lead_count} leads processed, memory freed")

        return lead_count

