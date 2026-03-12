"""
Enrichment Orchestrator — chains ALL free sources to find owner phone + email.

Pipeline order (fastest + highest hit-rate first):
1. ThatsThem reverse address (best — we always have the property address)
2. USSearch name search (proven — good phone hit rate)
3. ThatsThem name search (fallback — un-obfuscated emails)
4. VoterRecords (supplementary — best in FL, CO, WA, NC, OH, OR, PA)
5. Zenserp Google SERP (searches Google for phone/email)
6. Nuwber reverse address + name (118M addresses, 305M emails)
7. SearchPeopleFree (unlimited free, phone + email)
8. SpyDialer (free reverse address/name, voicemail feature)
9. TruePeopleSearch + FastPeopleSearch (parallel, heavy Cloudflare)
10. OpenCorporates LLC resolution (entity owners → real person name)
Post-enrichment: 3-way phone validation + email validation

Key design principles:
- Stop early: once we have BOTH phone AND email, skip remaining sources
- Concurrent batch processing with configurable semaphore
- All results cached 7 days per source
- Rate-limited per source to avoid blocks
- Reuse aiohttp sessions across batch (not per-lead)
- Returns NEW dict, never mutates input leads
"""

import asyncio
import logging
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


def _load_sources():
    global _thatsthem, _ussearch, _truepeoplesearch, _fastpeoplesearch
    global _voterrecords, _zenserp, _phone_intel, _cloudmersive, _veriphone
    global _nuwber, _searchpeoplefree, _spydialer, _contact_scraper
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
            logger.debug(f"LLC resolve error: {e}")

    def _merge(source_result: Optional[dict]):
        """Merge a source result into the accumulated result."""
        nonlocal need_phone, need_email
        if not source_result:
            return False

        hit = False
        if need_phone and source_result.get("phone"):
            result["phone"] = source_result["phone"]
            need_phone = False
            hit = True
            for p in source_result.get("phones", [source_result["phone"]]):
                if p and p not in result["phones"]:
                    result["phones"].append(p)

        if need_email and source_result.get("email"):
            result["email"] = source_result["email"]
            need_email = False
            hit = True
            for e in source_result.get("emails", [source_result["email"]]):
                if e and e not in result["emails"]:
                    result["emails"].append(e)

        # Collect additional phones/emails even if we already have primary
        for p in source_result.get("phones", []):
            if p and p not in result["phones"]:
                result["phones"].append(p)
        for e in source_result.get("emails", []):
            if e and e not in result["emails"]:
                result["emails"].append(e)

        return hit

    # ── Source 1: ThatsThem reverse address (highest value) ──
    if address and (need_phone or need_email):
        result["sources_tried"].append("ThatsThem-address")
        try:
            r = await _thatsthem.reverse_address_lookup(address, city, state)
            if _merge(r):
                result["sources_hit"].append("ThatsThem-address")
        except Exception as e:
            logger.debug(f"ThatsThem address error: {e}")

    # ── Source 2: USSearch name (proven for phones) ──
    if search_name and (need_phone or need_email):
        result["sources_tried"].append("USSearch")
        try:
            r = await _contact_scraper.scrape_ussearch(search_name, city, state)
            if _merge(r):
                result["sources_hit"].append("USSearch")
        except Exception as e:
            logger.debug(f"USSearch error: {e}")

    # ── Source 3: ThatsThem name search ──
    if search_name and (need_phone or need_email):
        result["sources_tried"].append("ThatsThem-name")
        try:
            r = await _thatsthem.name_search(search_name, city, state)
            if _merge(r):
                result["sources_hit"].append("ThatsThem-name")
        except Exception as e:
            logger.debug(f"ThatsThem name error: {e}")

    # Early exit if we have both
    if not need_phone and not need_email:
        return result

    # ── Source 4: VoterRecords (best for FL, CO, WA, NC, OH, OR, PA) ──
    if search_name and (need_phone or need_email):
        result["sources_tried"].append("VoterRecords")
        try:
            r = await _voterrecords.search(search_name, city, state)
            if _merge(r):
                result["sources_hit"].append("VoterRecords")
        except Exception as e:
            logger.debug(f"VoterRecords error: {e}")

    if not need_phone and not need_email:
        return result

    # ── Source 5: Zenserp Google SERP (searches Google for contact info) ──
    if search_name and (need_phone or need_email):
        result["sources_tried"].append("Zenserp")
        try:
            r = await _zenserp.search_owner(search_name, city, state, address)
            if _merge(r):
                result["sources_hit"].append("Zenserp")
        except Exception as e:
            logger.debug(f"Zenserp error: {e}")

    if not need_phone and not need_email:
        return result

    # ── Source 6: Nuwber (118M addresses, 305M emails) ──
    if address and (need_phone or need_email):
        result["sources_tried"].append("Nuwber-address")
        try:
            r = await _nuwber.search_by_address(address, city, state)
            if _merge(r):
                result["sources_hit"].append("Nuwber-address")
        except Exception as e:
            logger.debug(f"Nuwber address error: {e}")

    if search_name and (need_phone or need_email):
        result["sources_tried"].append("Nuwber-name")
        try:
            r = await _nuwber.search_by_name(search_name, city, state)
            if _merge(r):
                result["sources_hit"].append("Nuwber-name")
        except Exception as e:
            logger.debug(f"Nuwber name error: {e}")

    if not need_phone and not need_email:
        return result

    # ── Source 7: SearchPeopleFree (unlimited free) ──
    if search_name and (need_phone or need_email):
        result["sources_tried"].append("SearchPeopleFree")
        try:
            r = await _searchpeoplefree.search(search_name, city, state)
            if _merge(r):
                result["sources_hit"].append("SearchPeopleFree")
        except Exception as e:
            logger.debug(f"SearchPeopleFree error: {e}")

    if not need_phone and not need_email:
        return result

    # ── Source 8: SpyDialer (free reverse address/name) ──
    if address and (need_phone or need_email):
        result["sources_tried"].append("SpyDialer")
        try:
            r = await _spydialer.reverse_address(address, city, state)
            if _merge(r):
                result["sources_hit"].append("SpyDialer-address")
        except Exception as e:
            logger.debug(f"SpyDialer error: {e}")

    if search_name and not result.get("phone") and (need_phone or need_email):
        try:
            r = await _spydialer.search_by_name(search_name, city, state)
            if _merge(r):
                result["sources_hit"].append("SpyDialer-name")
        except Exception as e:
            logger.debug(f"SpyDialer name error: {e}")

    if not need_phone and not need_email:
        return result

    # ── Source 9: TruePeopleSearch + FastPeopleSearch (parallel, heavy CF) ──
    if not skip_slow and search_name and (need_phone or need_email):
        result["sources_tried"].extend(["TruePeopleSearch", "FastPeopleSearch"])
        try:
            tps_task = _truepeoplesearch.search(search_name, city, state)
            fps_task = _fastpeoplesearch.search(search_name, city, state)
            tps_r, fps_r = await asyncio.gather(tps_task, fps_task, return_exceptions=True)

            if not isinstance(tps_r, Exception) and _merge(tps_r):
                result["sources_hit"].append("TruePeopleSearch")
            if not isinstance(fps_r, Exception) and _merge(fps_r):
                result["sources_hit"].append("FastPeopleSearch")
        except Exception as e:
            logger.debug(f"PeopleSearch error: {e}")

    # If LLC and USSearch didn't work with resolved person, try original LLC name
    if (need_phone or need_email) and search_name != owner_name and owner_name:
        try:
            r = await _contact_scraper.scrape_ussearch(owner_name, city, state)
            if _merge(r):
                result["sources_hit"].append("USSearch-llc")
        except Exception as e:
            logger.debug(f"USSearch LLC fallback error: {e}")

    # ── Phone Validation: Abstract + Veriphone + Cloudmersive (parallel) ──
    if result.get("phone"):
        try:
            abstract_task = _phone_intel.lookup(result["phone"])
            veriphone_task = _veriphone.verify(result["phone"])
            cloudm_task = _cloudmersive.validate_phone(result["phone"])
            abs_r, veri_r, cm_r = await asyncio.gather(
                abstract_task, veriphone_task, cloudm_task,
                return_exceptions=True,
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
                # Veriphone has better carrier/type detection
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
        except Exception as e:
            logger.debug(f"Phone validation error: {e}")

    # ── Email Validation: Cloudmersive full check ──
    if result.get("email"):
        try:
            ev = await _cloudmersive.validate_email(result["email"])
            if ev:
                result["email_valid"] = ev.get("valid", False)
                result["email_disposable"] = ev.get("is_disposable", False)
                # If email is invalid/disposable, try alternates
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
            logger.debug(f"Email validation error: {e}")

    return result


async def enrich_batch(
    leads: list[dict],
    max_leads: int = 1000,
    concurrency: int = 5,
    skip_slow: bool = True,
) -> dict:
    """
    Batch enrich leads with phone + email from all free sources.

    Args:
        leads: List of lead dicts
        max_leads: Max leads to process
        concurrency: Concurrent workers
        skip_slow: Skip Cloudflare-heavy sources (TruePeople, FastPeople)

    Returns:
        Stats dict + mutates lead dicts in-place (for cache update compatibility)
    """
    t0 = time.time()

    candidates = []
    for lead in leads:
        owner = str(lead.get("owner_name", "") or "").strip()
        if not owner or owner.lower() in ("not found", "unknown", "n/a"):
            continue
        phone = str(lead.get("owner_phone", "") or "").strip()
        email = str(lead.get("owner_email", "") or "").strip()
        if phone and email:
            continue
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

            # Store all found phones/emails
            if len(enrichment.get("phones", [])) > 1:
                lead["alt_phones"] = enrichment["phones"][1:5]
            if len(enrichment.get("emails", [])) > 1:
                lead["alt_emails"] = enrichment["emails"][1:5]

            # Phone validation metadata (consensus from Abstract + Veriphone + Cloudmersive)
            pi = enrichment.get("phone_intel")
            if pi:
                lead["phone_carrier"] = pi.get("carrier", "")
                lead["phone_line_type"] = pi.get("line_type", "")
                lead["phone_valid"] = pi.get("valid", False)
                lead["phone_risk"] = pi.get("risk_level", "")
                if enrichment.get("sms_email"):
                    lead["sms_email"] = enrichment["sms_email"]

            # Email validation metadata (Cloudmersive)
            if "email_valid" in enrichment:
                lead["email_valid"] = enrichment["email_valid"]
                lead["email_disposable"] = enrichment.get("email_disposable", False)

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
