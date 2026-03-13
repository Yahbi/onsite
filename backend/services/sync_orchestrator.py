"""
Sync Orchestrator — Manages tiered batch scheduling for 4,000+ API sources.
Coordinates parallel fetching with rate limiting, error tracking, and progress logging.
"""

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from models.database import bulk_upsert_leads, log_sync
from models.source_registry import (
    get_sources_batch,
    get_sources_by_tier,
    update_source_status,
)
from services.fetchers.arcgis_fetcher import (
    fetch_arcgis,
    normalize_arcgis_record,
)
from services.fetchers.ckan_fetcher import (
    fetch_ckan,
    normalize_ckan_record,
)
from services.fetchers.socrata_fetcher import (
    fetch_socrata,
    normalize_socrata_record,
)

logger = logging.getLogger(__name__)

# Batch sizes per tier
TIER_BATCH_SIZES = {
    1: 9999,   # T1: fetch ALL daily (top metros)
    2: 500,    # T2: rotate 500 per sync run (~7 day rotation for 3,600 sources)
    3: 100,    # T3: rotate 100 per sync run
}

# Source types we can actually fetch from (others are skipped)
SUPPORTED_SOURCE_TYPES = {"socrata", "arcgis", "ckan", "ckan/json", "ckan/csv", "ckan/geojson"}

# Max concurrent API calls
MAX_CONCURRENCY = 20

# Rate limit: max requests per domain per second
DOMAIN_DELAY_SEC = 1.0


class SyncOrchestrator:
    """
    Orchestrates tiered data sync across 4,000+ API sources.
    Each sync run:
      - Fetches ALL T1 sources (daily, top metros)
      - Rotates through T2 sources in batches (weekly cadence)
      - Rotates through T3 sources in batches (monthly cadence)
    """

    def __init__(self, calculate_score_fn: Optional[Callable] = None):
        self.calculate_score_fn = calculate_score_fn
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self._domain_locks: Dict[str, float] = {}  # domain -> last_request_time

    async def run_batch(self) -> Dict[str, int]:
        """
        Run one sync batch across all tiers.
        Returns aggregate stats.
        """
        start = time.time()
        stats = {"total_sources": 0, "total_records": 0, "new_leads": 0, "errors": 0}

        for tier in (1, 2, 3):
            batch_size = TIER_BATCH_SIZES[tier]
            sources = get_sources_batch(tier, batch_size=batch_size)

            if not sources:
                continue

            logger.info(f"T{tier}: syncing {len(sources)} sources")
            tier_stats = await self._sync_sources(sources)

            stats["total_sources"] += tier_stats["sources_attempted"]
            stats["total_records"] += tier_stats["records_fetched"]
            stats["new_leads"] += tier_stats["new_leads"]
            stats["errors"] += tier_stats["errors"]

        elapsed_ms = int((time.time() - start) * 1000)
        logger.info(
            f"Sync batch complete in {elapsed_ms}ms: "
            f"{stats['total_sources']} sources, "
            f"{stats['total_records']} records, "
            f"{stats['new_leads']} new leads, "
            f"{stats['errors']} errors"
        )

        log_sync(
            source="orchestrator",
            fetched=stats["total_records"],
            new=stats["new_leads"],
            updated=0,
            deduped=0,
            duration_ms=elapsed_ms,
            status="success" if stats["errors"] == 0 else "partial",
        )

        return stats

    async def _sync_sources(self, sources: List[dict]) -> dict:
        """Sync a list of sources in parallel with rate limiting."""
        stats = {"sources_attempted": 0, "records_fetched": 0, "new_leads": 0, "errors": 0}

        # Filter to only supported source types
        sources = [
            s for s in sources
            if s.get("source_type", "").lower() in SUPPORTED_SOURCE_TYPES
        ]

        tasks = [self._sync_single_source(src) for src in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for src, result in zip(sources, results):
            stats["sources_attempted"] += 1
            if isinstance(result, Exception):
                stats["errors"] += 1
                update_source_status(
                    src["id"], status="error", error=str(result)[:200]
                )
                logger.warning(f"Source {src.get('location', src['id'])} failed: {result}")
            elif result:
                stats["records_fetched"] += result.get("fetched", 0)
                stats["new_leads"] += result.get("new", 0)

        return stats

    async def _sync_single_source(self, source: dict) -> Optional[dict]:
        """Fetch and normalize records from a single source."""
        async with self._semaphore:
            # Rate limit per domain
            await self._rate_limit(source)

            source_type = source.get("source_type", "").lower()
            source_id = source["id"]
            label = source.get("location") or source.get("api_name") or str(source_id)

            try:
                # Fetch raw records
                raw_records = await self._fetch_by_type(source, source_type)

                if not raw_records:
                    update_source_status(source_id, status="active", record_count=0)
                    return {"fetched": 0, "new": 0}

                # Normalize records
                leads = self._normalize_records(raw_records, source, source_type)

                if not leads:
                    update_source_status(source_id, status="active", record_count=0)
                    return {"fetched": len(raw_records), "new": 0}

                # Upsert to database
                upsert_stats = bulk_upsert_leads(leads)

                update_source_status(
                    source_id,
                    status="active",
                    record_count=len(leads),
                )

                log_sync(
                    source=label,
                    fetched=len(raw_records),
                    new=upsert_stats["new"],
                    updated=upsert_stats["updated"],
                    deduped=upsert_stats["deduped"],
                    duration_ms=0,
                )

                return {
                    "fetched": len(raw_records),
                    "new": upsert_stats["new"],
                }

            except Exception as e:
                update_source_status(source_id, status="error", error=str(e)[:200])
                logger.warning(f"Sync {label} error: {e}")
                return None

    async def _fetch_by_type(
        self, source: dict, source_type: str
    ) -> List[dict]:
        """Dispatch to the correct fetcher based on source type."""
        if source_type == "socrata":
            return await fetch_socrata(source)
        elif source_type == "arcgis":
            return await fetch_arcgis(source)
        elif source_type in ("ckan", "ckan/json", "ckan/csv", "ckan/geojson"):
            return await fetch_ckan(source)
        else:
            logger.debug(f"Unknown source type: {source_type}")
            return []

    def _normalize_records(
        self, records: List[dict], source: dict, source_type: str
    ) -> List[dict]:
        """Normalize raw records using the appropriate normalizer."""
        leads = []
        for rec in records:
            try:
                if source_type == "socrata":
                    lead = normalize_socrata_record(rec, source, self.calculate_score_fn)
                elif source_type == "arcgis":
                    lead = normalize_arcgis_record(rec, source, self.calculate_score_fn)
                else:
                    lead = normalize_ckan_record(rec, source, self.calculate_score_fn)

                if lead and lead.get("address"):
                    leads.append(lead)
            except Exception as e:
                logger.debug(f"Skip record: {e}")
        return leads

    async def _rate_limit(self, source: dict):
        """Rate limit requests to the same domain."""
        url = source.get("api_url") or source.get("url") or ""
        if not url:
            return

        # Extract domain
        import re
        m = re.match(r"https?://([^/]+)", url)
        if not m:
            return
        domain = m.group(1)

        now = time.time()
        last = self._domain_locks.get(domain, 0)
        wait = DOMAIN_DELAY_SEC - (now - last)
        if wait > 0:
            await asyncio.sleep(wait)
        self._domain_locks[domain] = time.time()
