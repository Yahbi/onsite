"""
Onsite - Enhancement Features
Auto-enrichment, CSV export, AI scoring
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from typing import Optional, List
from datetime import datetime
import csv
import io
import asyncio
import sqlite3
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/enhancements", tags=["enhancements"])


# ═══════════════════════════════════════════════════════════════════════════
# AUTO-ENRICHMENT PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/auto-enrich")
async def trigger_auto_enrichment(
    background_tasks: BackgroundTasks,
    limit: int = Query(100, description="Max leads to enrich"),
    city: Optional[str] = Query(None, description="Filter by city"),
    min_valuation: Optional[int] = Query(None, description="Min permit value")
):
    """
    Automatically enrich unenriched leads using PropertyReach API keys.

    - Targets leads without owner_name, owner_phone, or owner_email
    - Uses 10-key rotation system (2M lookups/month capacity)
    - Runs in background to avoid timeout
    """

    try:
        # Import enrichment services
        from services.ownership import OwnershipLookupService
        from models.database import get_db

        # Find unenriched leads
        with get_db() as conn:
            query = """
                SELECT id, address, city, state, zip, apn, owner_name, owner_phone
                FROM leads
                WHERE (owner_name IS NULL OR owner_name = '' OR owner_phone IS NULL OR owner_phone = '')
            """
            filters = []
            params = []

            if city:
                filters.append("city = ?")
                params.append(city)

            if min_valuation:
                filters.append("valuation >= ?")
                params.append(min_valuation)

            if filters:
                query += " AND " + " AND ".join(filters)

            limit = min(int(limit), 5000)
            query += " ORDER BY valuation DESC, score DESC LIMIT ?"
            params.append(limit)

            cursor = conn.execute(query, params)
            unenriched = cursor.fetchall()

        if not unenriched:
            return {
                "status": "ok",
                "message": "No unenriched leads found",
                "total_checked": 0,
                "queued_for_enrichment": 0
            }

        # Queue enrichment in background
        async def enrich_batch():
            service = OwnershipLookupService()
            enriched_count = 0

            for lead in unenriched:
                try:
                    # Attempt enrichment
                    result = await service.lookup_owner(
                        address=lead["address"],
                        city=lead["city"],
                        state=lead["state"],
                        zip_code=lead["zip"],
                        apn=lead["apn"]
                    )

                    if result:
                        # Update database
                        with get_db() as conn:
                            conn.execute("""
                                UPDATE leads
                                SET owner_name = ?,
                                    owner_phone = ?,
                                    owner_email = ?,
                                    owner_address = ?,
                                    enrichment_status = 'enriched',
                                    enrichment_date = ?
                                WHERE id = ?
                            """, (
                                result.get("name", ""),
                                result.get("phone", ""),
                                result.get("email", ""),
                                result.get("address", ""),
                                datetime.now().isoformat(),
                                lead["id"]
                            ))
                        enriched_count += 1

                        # Rate limiting - don't hammer the API
                        await asyncio.sleep(0.5)

                except Exception as e:
                    logger.warning(f"Enrichment failed for lead {lead['id']}: {e}")
                    continue

            logger.info(f"Auto-enrichment complete: {enriched_count}/{len(unenriched)} leads enriched")

        background_tasks.add_task(enrich_batch)

        return {
            "status": "processing",
            "message": "Enrichment started in background",
            "total_checked": len(unenriched),
            "queued_for_enrichment": len(unenriched),
            "estimated_time_minutes": len(unenriched) * 0.5 / 60  # 0.5s per lead
        }

    except Exception as e:
        logger.error(f"Auto-enrichment error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enrichment-stats")
async def get_enrichment_stats():
    """Get enrichment statistics and API key usage"""

    try:
        from models.database import get_db
        from services.ownership import PropertyReachRotator

        with get_db() as conn:
            stats = {
                "total_leads": conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0],
                "enriched_leads": conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE owner_name IS NOT NULL AND owner_name != ''"
                ).fetchone()[0],
                "with_phone": conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE owner_phone IS NOT NULL AND owner_phone != ''"
                ).fetchone()[0],
                "with_email": conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE owner_email IS NOT NULL AND owner_email != ''"
                ).fetchone()[0],
            }

        stats["enrichment_percentage"] = round(
            (stats["enriched_leads"] / stats["total_leads"] * 100) if stats["total_leads"] > 0 else 0,
            2
        )

        # Get PropertyReach API stats
        try:
            rotator = PropertyReachRotator()
            stats["api_keys"] = rotator.get_stats()
        except Exception as e:
            logger.warning("Failed to load API key stats: %s", e)
            stats["api_keys"] = {"error": "Could not load API key stats"}

        return stats

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════
# CSV EXPORT WITH FILTERS
# ═══════════════════════════════════════════════════════════════════════════

@router.get("/export/csv")
async def export_leads_csv(
    request: Request = None,
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None),
    min_valuation: Optional[int] = Query(None),
    max_days_old: Optional[int] = Query(None),
    temperature: Optional[str] = Query(None, description="Hot, Warm, or Cold"),
    limit: int = Query(1000, le=10000, description="Max leads to export"),
    enriched_only: bool = Query(False, description="Only export leads with owner data")
):
    """
    Export filtered leads to CSV format.

    Perfect for:
    - "Export 100 hot LA leads"
    - "Export all $500k+ permits with owner contacts"
    - "Export Seattle leads from last 30 days"
    Requires Starter plan or higher.
    """
    # --- Plan enforcement: CSV export requires Starter+ ---
    try:
        from main import get_user_plan_from_request, get_plan_limits
        _plan_info = get_user_plan_from_request(request) if request else {"plan": "reveal"}
        _plan_name = _plan_info.get("plan", "reveal").lower()
        _plan_lim = get_plan_limits(_plan_name)
        if not _plan_lim.get("csv_export", False):
            raise HTTPException(status_code=403, detail="CSV export requires Starter plan or higher")
    except HTTPException:
        raise
    except ImportError:
        # If main module import fails, deny by default for safety
        raise HTTPException(status_code=403, detail="CSV export requires Starter plan or higher")

    try:
        from models.database import get_db

        # Build dynamic query
        query = "SELECT * FROM leads WHERE 1=1"
        params = []

        if city:
            query += " AND city = ?"
            params.append(city)

        if state:
            query += " AND state = ?"
            params.append(state)

        if min_score:
            query += " AND score >= ?"
            params.append(min_score)

        if min_valuation:
            query += " AND valuation >= ?"
            params.append(min_valuation)

        if max_days_old:
            query += " AND days_old <= ?"
            params.append(max_days_old)

        if temperature:
            query += " AND temperature = ?"
            params.append(temperature.lower())

        if enriched_only:
            query += " AND owner_name IS NOT NULL AND owner_name != ''"

        limit = min(int(limit), 5000)
        query += " ORDER BY score DESC, valuation DESC LIMIT ?"
        params.append(limit)

        # Execute query
        with get_db() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            leads = [dict(row) for row in cursor.fetchall()]

        if not leads:
            raise HTTPException(status_code=404, detail="No leads found matching criteria")

        # Generate CSV
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=leads[0].keys())
        writer.writeheader()
        writer.writerows(leads)

        # Create streaming response
        output.seek(0)
        filename = f"permits_{city or 'all'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════════
# AI LEAD SCORING (Enhanced)
# ═══════════════════════════════════════════════════════════════════════════

@router.post("/rescore-leads")
async def rescore_all_leads(background_tasks: BackgroundTasks):
    """
    Recalculate AI scores for all leads using enhanced algorithm.

    Scoring factors:
    - Recency (35 pts): Newer = hotter
    - Valuation (30 pts): Higher value = higher score
    - Permit type (20 pts): Residential, ADU, New Construction prioritized
    - Owner data availability (15 pts): Enriched leads score higher
    """

    async def rescore_batch():
        try:
            from models.database import get_db

            with get_db() as conn:
                leads = conn.execute("SELECT id, days_old, valuation, permit_type, owner_name FROM leads").fetchall()

                for lead in leads:
                    # Calculate score components
                    urgency_score = max(0, min(35, 35 - lead["days_old"]))
                    value_score = min(30, int((lead["valuation"] / 500000) * 30)) if lead["valuation"] else 5

                    # Permit type scoring
                    permit_type = (lead["permit_type"] or "").lower()
                    if any(t in permit_type for t in ["residential addition", "adu", "new construction", "mixed use"]):
                        type_score = 20
                    elif any(t in permit_type for t in ["remodel", "renovation", "alteration"]):
                        type_score = 15
                    else:
                        type_score = 10

                    # Enrichment bonus
                    enrichment_score = 15 if lead["owner_name"] else 0

                    # Total score
                    total_score = urgency_score + value_score + type_score + enrichment_score

                    # Determine temperature
                    if total_score >= 70:
                        temperature = "hot"
                    elif total_score >= 50:
                        temperature = "warm"
                    else:
                        temperature = "cold"

                    # Update database
                    conn.execute("""
                        UPDATE leads
                        SET score = ?, temperature = ?
                        WHERE id = ?
                    """, (total_score, temperature, lead["id"]))

                logger.info(f"Rescored {len(leads)} leads")

        except Exception as e:
            logger.error(f"Rescoring error: {e}")

    background_tasks.add_task(rescore_batch)

    return {
        "status": "processing",
        "message": "Lead rescoring started in background"
    }


@router.get("/top-opportunities")
async def get_top_opportunities(
    limit: int = Query(50, le=500),
    city: Optional[str] = Query(None)
):
    """
    Get top-scoring leads (AI-ranked opportunities).

    Returns highest-value, most recent, enriched leads.
    Perfect for "Show me my best 50 leads this week"
    """

    try:
        from models.database import get_db

        query = """
            SELECT id, permit_number, address, city, valuation, score, temperature,
                   owner_name, owner_phone, owner_email, days_old, permit_type, issue_date
            FROM leads
            WHERE score >= 50
        """
        params = []

        if city:
            query += " AND city = ?"
            params.append(city)

        limit = min(int(limit), 5000)
        query += " ORDER BY score DESC, valuation DESC LIMIT ?"
        params.append(limit)

        with get_db() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            opportunities = [dict(row) for row in cursor.fetchall()]

        return {
            "total": len(opportunities),
            "avg_score": round(sum(o["score"] for o in opportunities) / len(opportunities), 2) if opportunities else 0,
            "avg_valuation": round(sum(o["valuation"] or 0 for o in opportunities) / len(opportunities), 2) if opportunities else 0,
            "leads": opportunities
        }

    except Exception as e:
        logger.error(f"Top opportunities error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
