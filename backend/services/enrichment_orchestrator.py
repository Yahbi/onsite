"""
Enrichment Orchestrator — chains ALL free sources to find owner phone + email.

Pipeline order (optimized for throughput with parallel groups):
0. Regrid parcel data (owner name, property data) — sequential, provides owner_name
Then in PARALLEL:
  Group A (address-based): ThatsThem-address, Nuwber-address, SpyDialer-address
  Group B (name-based): USSearch, ThatsThem-name, VoterRecords, Nuwber-name,
                         SearchPeopleFree, Zenserp, TruePeopleSearch, FastPeopleSearch
10. FreeEmailFinder (SMTP verification)
Post-enrichment: 3-way phone validation + email validation

Key design principles:
- Parallel source groups: address-based and name-based run concurrently
- 10s per-source timeout prevents single slow source from blocking pipeline
- Concurrent batch processing with configurable semaphore
- All results cached 7 days per source
- Rate-limited per source to avoid blocks
- Returns NEW dict, never mutates input leads
"""

import asyncio
import re
import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Lazy imports to avoid circular deps and speed up startup
_thatsthem = None
_ussearch = None
_truepeoplesearch = None
_fastpeoplesearch = None
_voterrecords = None
_zenserp = None
_phone_intel = None
_cloudmersive = None
_veriphone = None
_nuwber = None
_searchpeoplefree = None
_spydialer = None
_contact_scraper = None
_regrid_service = None


def _load_sources():
    global _thatsthem, _ussearch, _truepeoplesearch, _fastpeoplesearch
    global _voterrecords, _zenserp, _phone_intel, _cloudmersive, _veriphone
    global _nuwber, _searchpeoplefree, _spydialer, _contact_scraper
    global _regrid_service
    if _thatsthem is None:
        from services import thatsthem_scraper as _tt
        from services import truepeoplesearch_scraper as _tps
        from services import fastpeoplesearch_scraper as _fps
        from services import voterrecords_scraper as _vr
        from services import zenserp_scraper as _zs
        from services import phone_intelligence as _pi
        from services import cloudmersive_validate as _cm
        from services import veriphone_api as _vp
        from services import nuwber_scraper as _nw
        from services import searchpeoplefree_scraper as _spf
        from services import spydialer_scraper as _sd
        from services import contact_scraper as _cs
        _thatsthem = _tt
        _truepeoplesearch = _tps
        _fastpeoplesearch = _fps
        _voterrecords = _vr
        _zenserp = _zs
        _phone_intel = _pi
        _cloudmersive = _cm
        _veriphone = _vp
        _nuwber = _nw
        _searchpeoplefree = _spf
        _spydialer = _sd
        _contact_scraper = _cs
    if _regrid_service is None:
        regrid_enabled = os.getenv("REGRID_ENABLED", "0") == "1"
        regrid_key = os.getenv("REGRID_API_KEY", "")
        if regrid_enabled and regrid_key:
            try:
                from services.parcel_service import RegridParcelService
                _regrid_service = RegridParcelService(api_key=regrid_key)
                logger.info("Regrid parcel service initialized (nationwide)")
            except Exception as e:
                logger.warning("Failed to initialize Regrid: %s", e)
                _regrid_service = None
        else:
            logger.info("Regrid disabled (REGRID_ENABLED=%s)", os.getenv("REGRID_ENABLED", "0"))


def _is_llc(name: str) -> bool:
    upper = (name or "").upper()
    patterns = (
        ' LLC', ' L.L.C', ' INC', ' CORP', ' LTD', ' LP', ' L.P.',
        ' TRUST', ' REVOCABLE', ' IRREVOCABLE', ' LIVING TRUST',
        ' HOLDINGS', ' PROPERTIES', ' PARTNERS', ' INVESTMENTS',
        ' ASSOCIATES', ' ENTERPRISES', ' GROUP', ' MANAGEMENT',
        ' CAPITAL', ' VENTURES', ' DEVELOPMENT', ' REALTY',
        ' ESTATE', ' FUND',
    )
    return any(p in upper for p in patterns)



def _validate_phone(phone: str) -> bool:
    """Check if phone looks real (10+ digits, not a known junk number)."""
    digits = re.sub(r"\D", "", str(phone or ""))
    if len(digits) < 10:
        return False
    # Known junk patterns
    if digits.startswith("000") or digits == "0" * 10:
        return False
    return True


def _validate_email(email: str) -> bool:
    """Check if email looks real (not a scraper own email)."""
    if not email or "@" not in email:
        return False
    junk_domains = [
        "thatsthem.com", "spokeo.com", "nuwber.com", "searchpeoplefree.com",
        "truepeoplesearch.com", "fastpeoplesearch.com", "spydialer.com",
        "example.com", "test.com",
    ]
    domain = email.split("@")[1].lower()
    return domain not in junk_domains


async def enrich_single(
    lead: dict,
    skip_slow: bool = False,
) -> dict:
    """
    Enrich a single lead with phone + email from all free sources.

    Args:
        lead: Lead dict with address, city, state, owner_name fields
        skip_slow: If True, skip sources with heavy Cloudflare (TruePeople, FastPeople)

    Returns:
        New dict with enrichment fields (never mutates input):
        {phone, email, phones, emails, person_name, sources_tried, sources_hit}
    """
    _load_sources()

    address = str(lead.get("address", "") or "").strip()
    city = str(lead.get("city", "") or "").strip()
    state = str(lead.get("state", "") or "").strip()
    owner_name = str(lead.get("owner_name", "") or "").strip()

    existing_phone = str(lead.get("owner_phone", "") or "").strip()
    existing_email = str(lead.get("owner_email", "") or "").strip()

    result = {
        "phone": existing_phone,
        "email": existing_email,
        "phones": [existing_phone] if existing_phone else [],
        "emails": [existing_email] if existing_email else [],
        "person_name": "",
        "sources_tried": [],
        "sources_hit": [],
    }

    need_phone = not existing_phone
    need_email = not existing_email

    if not need_phone and not need_email:
        return result

    # Resolve LLC to real person name first
    search_name = owner_name
    if owner_name and _is_llc(owner_name):
        try:
            llc_result = await _contact_scraper.resolve_llc_owner(owner_name, state)
            if llc_result:
                person = llc_result.get("person_name") or llc_result.get("agent_name") or ""
                if person:
                    result["person_name"] = person
                    search_name = person
                    logger.debug(f"LLC {owner_name} → {person}")
        except Exception as e:
            logger.warning(f"LLC resolve error: {e}")

    def _merge(source_result: Optional[dict]):
        """Merge a source result into the accumulated result."""
        nonlocal need_phone, need_email
        if not source_result:
            return False

        hit = False
        if need_phone and source_result.get("phone") and _validate_phone(source_result["phone"]):
            result["phone"] = source_result["phone"]
            need_phone = False
            hit = True
            for p in source_result.get("phones", [source_result["phone"]]):
                if p and _validate_phone(p) and p not in result["phones"]:
                    result["phones"].append(p)

        if need_email and source_result.get("email") and _validate_email(source_result["email"]):
            result["email"] = source_result["email"]
            need_email = False
            hit = True
            for e in source_result.get("emails", [source_result["email"]]):
                if e and _validate_email(e) and e not in result["emails"]:
                    result["emails"].append(e)

        # Collect additional phones/emails even if we already have primary
        for p in source_result.get("phones", []):
            if p and _validate_phone(p) and p not in result["phones"]:
                result["phones"].append(p)
        for e in source_result.get("emails", []):
            if e and _validate_email(e) and e not in result["emails"]:
                result["emails"].append(e)

        return hit

    # ── Source 0: Regrid parcel data (owner name, mailing address, property data) ──
    # Runs first to populate owner_name which all other scrapers need.
    regrid_enrichment = {}
    if _regrid_service and _regrid_service.api_key and address:
        result["sources_tried"].append("Regrid")
        try:
            regrid_enrichment = await _regrid_service.enrich_lead(lead)
            if regrid_enrichment:
                result["sources_hit"].append("Regrid")
                # If Regrid found an owner name and we didn't have one, use it
                if regrid_enrichment.get("owner_name") and not owner_name:
                    owner_name = regrid_enrichment["owner_name"]
                    search_name = owner_name
                    if _is_llc(owner_name):
                        try:
                            llc_result = await _contact_scraper.resolve_llc_owner(owner_name, state)
                            if llc_result:
                                person = llc_result.get("person_name") or llc_result.get("agent_name") or ""
                                if person:
                                    result["person_name"] = person
                                    search_name = person
                        except Exception:
                            pass
                logger.debug(f"Regrid enriched: {regrid_enrichment.keys()}")
        except Exception as e:
            logger.warning(f"Regrid enrichment error: {e}")

    # Attach Regrid property data to result for caller to persist
    result["regrid_data"] = regrid_enrichment

    # ── Parallel enrichment groups ──
    # All sources run concurrently with per-source timeouts.
    # Group A: address-based (no name needed)
    # Group B: name-based (needs search_name from Regrid or input)

    async def _safe(coro, source_name):
        """Run a source safely, return (source_name, result) or None on error."""
        try:
            r = await asyncio.wait_for(coro, timeout=10)
            return (source_name, r)
        except asyncio.TimeoutError:
            logger.debug(f"{source_name} timeout")
            return None
        except Exception as e:
            logger.warning(f"{source_name} error: {e}")
            return None

    tasks = []
    task_names = []

    # Group A: Address-based (always available if we have an address)
    if address and (need_phone or need_email):
        tasks.append(_safe(_thatsthem.reverse_address_lookup(address, city, state), "ThatsThem-address"))
        task_names.append("ThatsThem-address")
        tasks.append(_safe(_nuwber.search_by_address(address, city, state), "Nuwber-address"))
        task_names.append("Nuwber-address")
        tasks.append(_safe(_spydialer.reverse_address(address, city, state), "SpyDialer-address"))
        task_names.append("SpyDialer-address")

    # Group B: Name-based (needs search_name)
    if search_name and (need_phone or need_email):
        tasks.append(_safe(_contact_scraper.scrape_ussearch(search_name, city, state), "USSearch"))
        task_names.append("USSearch")
        tasks.append(_safe(_thatsthem.name_search(search_name, city, state), "ThatsThem-name"))
        task_names.append("ThatsThem-name")
        tasks.append(_safe(_voterrecords.search(search_name, city, state), "VoterRecords"))
        task_names.append("VoterRecords")
        tasks.append(_safe(_nuwber.search_by_name(search_name, city, state), "Nuwber-name"))
        task_names.append("Nuwber-name")
        tasks.append(_safe(_searchpeoplefree.search(search_name, city, state), "SearchPeopleFree"))
        task_names.append("SearchPeopleFree")
        if _zenserp and getattr(_zenserp, "API_KEY", ""):
            tasks.append(_safe(_zenserp.search_owner(search_name, city, state, address), "Zenserp"))
            task_names.append("Zenserp")
        if not skip_slow:
            tasks.append(_safe(_truepeoplesearch.search(search_name, city, state), "TruePeopleSearch"))
            task_names.append("TruePeopleSearch")
            tasks.append(_safe(_fastpeoplesearch.search(search_name, city, state), "FastPeopleSearch"))
            task_names.append("FastPeopleSearch")

    # Track all sources we're trying
    result["sources_tried"].extend(task_names)

    # Run ALL sources in parallel
    if tasks:
        raw_results = await asyncio.gather(*tasks)
        for r in raw_results:
            if r is not None:
                source_name, source_result = r
                if _merge(source_result):
                    result["sources_hit"].append(source_name)

    # LLC fallback — try original LLC name if resolved person didn't work
    if (need_phone or need_email) and search_name != owner_name and owner_name:
        r = await _safe(_contact_scraper.scrape_ussearch(owner_name, city, state), "USSearch-llc")
        if r:
            _, sr = r
            if _merge(sr):
                result["sources_hit"].append("USSearch-llc")

    # ── Step 10: Free email finder (SMTP verification) ──
    if not result.get("email") and search_name and lead.get("city"):
        result["sources_tried"].append("FreeEmailFinder")
        try:
            from services.free_enrichment import FreeEmailFinder
            finder = FreeEmailFinder()
            name_parts = search_name.strip().split()
            if len(name_parts) >= 2:
                _first = name_parts[0]
                _last = name_parts[-1]
                fe_result = await asyncio.wait_for(
                    finder.find_email(_first, _last),
                    timeout=8,
                )
                if fe_result and fe_result.get("email"):
                    _merge({"email": fe_result["email"], "emails": [fe_result["email"]]})
                    result["sources_hit"].append("FreeEmailFinder")
                    need_email = False
                    logger.info("FreeEmailFinder found email for %s", search_name)
        except asyncio.TimeoutError:
            logger.debug("FreeEmailFinder timeout for %s", search_name)
        except Exception as e:
            logger.warning("FreeEmailFinder error: %s", e)

    # ── Phone Validation: Abstract + Veriphone + Cloudmersive (parallel) ──
    if result.get("phone"):
        try:
            abstract_task = _phone_intel.lookup(result["phone"])
            veriphone_task = _veriphone.verify(result["phone"])
            cloudm_task = _cloudmersive.validate_phone(result["phone"])
            abs_r, veri_r, cm_r = await asyncio.wait_for(
                asyncio.gather(
                    abstract_task, veriphone_task, cloudm_task,
                    return_exceptions=True,
                ),
                timeout=8,
            )

            intel = abs_r if not isinstance(abs_r, Exception) else None
            veri = veri_r if not isinstance(veri_r, Exception) else None
            cm = cm_r if not isinstance(cm_r, Exception) else None

            # Merge validation results (consensus approach)
            phone_meta = {}
            valid_votes = 0
            total_votes = 0

            if intel:
                phone_meta["abstract"] = intel
                total_votes += 1
                if intel.get("is_valid"):
                    valid_votes += 1
                if intel.get("sms_email") and not result.get("email"):
                    result["sms_email"] = intel["sms_email"]

            if veri:
                phone_meta["veriphone"] = veri
                total_votes += 1
                if veri.get("valid"):
                    valid_votes += 1
                phone_meta["carrier"] = veri.get("carrier", "")
                phone_meta["line_type"] = veri.get("phone_type", "")
                phone_meta["region"] = veri.get("region", "")

            if cm:
                phone_meta["cloudmersive"] = cm
                total_votes += 1
                if cm.get("valid"):
                    valid_votes += 1
                if not phone_meta.get("line_type"):
                    phone_meta["line_type"] = cm.get("phone_type", "")

            phone_meta["valid"] = valid_votes > 0 and valid_votes >= (total_votes / 2)
            phone_meta["risk_level"] = (intel or {}).get("risk_level", "unknown")
            result["phone_intel"] = phone_meta

            # If phone fails validation consensus, try alternates
            if not phone_meta["valid"] and total_votes > 0:
                logger.debug(f"Phone {result['phone']} failed validation ({valid_votes}/{total_votes})")
                for alt in result.get("phones", [])[1:]:
                    alt_veri = await _veriphone.verify(alt)
                    if alt_veri and alt_veri.get("valid"):
                        result["phone"] = alt
                        result["phone_intel"] = {"veriphone": alt_veri, "valid": True}
                        logger.debug(f"Swapped to valid alt phone: {alt}")
                        break

            result["sources_hit"].append("PhoneValidation")
        except asyncio.TimeoutError:
            logger.debug("Phone validation timeout for %s", result["phone"])
        except Exception as e:
            logger.warning(f"Phone validation error: {e}")

    # ── Email Validation: Cloudmersive full check ──
    if result.get("email"):
        try:
            ev = await _cloudmersive.validate_email(result["email"])
            if ev:
                result["email_valid"] = ev.get("valid", False)
                result["email_disposable"] = ev.get("is_disposable", False)
                if not ev.get("valid") or ev.get("is_disposable"):
                    for alt_email in result.get("emails", [])[1:]:
                        alt_ev = await _cloudmersive.validate_email(alt_email)
                        if alt_ev and alt_ev.get("valid") and not alt_ev.get("is_disposable"):
                            result["email"] = alt_email
                            result["email_valid"] = True
                            result["email_disposable"] = False
                            logger.debug(f"Swapped to valid alt email: {alt_email}")
                            break
                result["sources_hit"].append("EmailValidation")
        except Exception as e:
            logger.warning(f"Email validation error: {e}")

    return result


async def enrich_batch(
    leads: list[dict],
    max_leads: int = 1000,
    concurrency: int = 10,
    skip_slow: bool = True,
) -> dict:
    """
    Batch enrich leads with phone + email from all free sources.

    Args:
        leads: List of lead dicts
        max_leads: Max leads to process
        concurrency: Concurrent workers (default 10 for high throughput)
        skip_slow: Skip Cloudflare-heavy sources (TruePeople, FastPeople)

    Returns:
        Stats dict + mutates lead dicts in-place (for cache update compatibility)
    """
    t0 = time.time()

    candidates = []
    skip_names = ("not found", "unknown", "n/a", "pending lookup")
    for lead in leads:
        owner = str(lead.get("owner_name", "") or "").strip()
        phone = str(lead.get("owner_phone", "") or "").strip()
        email = str(lead.get("owner_email", "") or "").strip()
        if phone and email:
            continue
        if owner.lower() in skip_names:
            owner = ""
            lead["owner_name"] = ""
        candidates.append(lead)

    if not candidates:
        return {"enriched": 0, "total_candidates": 0, "elapsed": 0}

    # Prioritize by score (highest value leads first)
    candidates.sort(key=lambda l: float(l.get("score", 0) or 0), reverse=True)
    batch = candidates[:max_leads]

    logger.info(
        f"Enrichment orchestrator: {len(batch)} leads "
        f"(of {len(candidates)} needing enrichment, concurrency={concurrency})"
    )

    sem = asyncio.Semaphore(concurrency)
    stats = {
        "enriched": 0,
        "phones_found": 0,
        "emails_found": 0,
        "sources_hit": {},
    }

    async def _process(lead):
        async with sem:
            enrichment = await enrich_single(lead, skip_slow=skip_slow)

            updated = False
            if enrichment.get("phone") and not lead.get("owner_phone", "").strip():
                lead["owner_phone"] = enrichment["phone"]
                stats["phones_found"] += 1
                updated = True

            if enrichment.get("email") and not lead.get("owner_email", "").strip():
                lead["owner_email"] = enrichment["email"]
                stats["emails_found"] += 1
                updated = True

            if enrichment.get("person_name") and _is_llc(lead.get("owner_name", "")):
                lead["beneficial_owner"] = enrichment["person_name"]
                updated = True

            # Persist Regrid parcel data back to lead
            regrid = enrichment.get("regrid_data") or {}
            for rk, lk in (
                ("owner_name", "owner_name"),
                ("market_value", "market_value"),
                ("year_built", "year_built"),
                ("square_feet", "square_feet"),
                ("lot_size", "lot_size"),
                ("bedrooms", "bedrooms"),
                ("bathrooms", "bathrooms"),
                ("zoning", "zoning"),
                ("apn", "apn"),
                ("owner_address", "owner_address"),
            ):
                if regrid.get(rk) and not lead.get(lk):
                    lead[lk] = regrid[rk]
                    updated = True

            # Store all found phones/emails
            if len(enrichment.get("phones", [])) > 1:
                lead["alt_phones"] = enrichment["phones"][1:5]
            if len(enrichment.get("emails", [])) > 1:
                lead["alt_emails"] = enrichment["emails"][1:5]

            # Phone validation metadata
            pi = enrichment.get("phone_intel")
            if pi:
                lead["phone_carrier"] = pi.get("carrier", "")
                lead["phone_line_type"] = pi.get("line_type", "")
                lead["phone_valid"] = pi.get("valid", False)
                lead["phone_risk"] = pi.get("risk_level", "")
                if enrichment.get("sms_email"):
                    lead["sms_email"] = enrichment["sms_email"]

            # Email validation metadata
            if "email_valid" in enrichment:
                lead["email_valid"] = enrichment["email_valid"]
                lead["email_disposable"] = enrichment.get("email_disposable", False)

            # Persist enrichment source trail
            if enrichment.get("sources_hit"):
                import json as _json
                lead["enrichment_sources"] = _json.dumps(enrichment["sources_hit"])

            if enrichment.get("sms_email"):
                lead["sms_email"] = enrichment["sms_email"]

            if updated:
                stats["enriched"] += 1

            for src in enrichment.get("sources_hit", []):
                stats["sources_hit"][src] = stats["sources_hit"].get(src, 0) + 1

    # Process in chunks with pauses to respect rate limits
    chunk_size = concurrency * 2
    for i in range(0, len(batch), chunk_size):
        chunk = batch[i:i + chunk_size]
        await asyncio.gather(*[_process(lead) for lead in chunk])
        if i + chunk_size < len(batch):
            await asyncio.sleep(1)

    elapsed = round(time.time() - t0, 1)
    stats["total_candidates"] = len(candidates)
    stats["batch_size"] = len(batch)
    stats["elapsed"] = elapsed

    rate = stats["enriched"] / max(elapsed, 0.1)
    logger.info(
        f"Enrichment done: {stats['enriched']} enriched "
        f"({stats['phones_found']} phones, {stats['emails_found']} emails) "
        f"in {elapsed}s ({rate:.1f}/s). Sources: {stats['sources_hit']}"
    )

    return stats
