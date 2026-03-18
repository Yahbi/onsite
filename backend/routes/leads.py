"""
Onsite Lead Routes
All lead-related API endpoints
"""

from fastapi import APIRouter, HTTPException, Query, Request, BackgroundTasks
from fastapi.responses import StreamingResponse
from typing import Optional, List
import csv
import io
import logging

from models.database import (
    query_leads, get_lead_by_id, update_lead, get_stats,
    get_leads_needing_enrichment, save_filter, get_saved_filters,
    delete_saved_filter, save_stage_transition, get_pipeline_history,
    create_notification, get_unread_notifications, mark_notifications_read,
)
from core.scoring import compute_readiness, score_breakdown, is_insurance_claim

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["leads"])


# NOTE: GET /leads is served by main.py (memory-cache-backed, 1.9M+ leads).
# This router provides detail/filter endpoints that query the DB directly.


@router.get("/leads/db")
async def get_leads_from_db(
    limit: int = Query(500, description="Max leads to return"),
    offset: int = Query(0, description="Pagination offset"),
    days: int = Query(None, description="Filter to permits issued in last N days"),
    permit_type: Optional[str] = Query(None, description="Filter by permit_type"),
    city: Optional[str] = Query(None, description="Filter by city"),
    state: Optional[str] = Query(None, description="Filter by state"),
    temperature: Optional[str] = Query(None, description="Filter by temperature"),
    min_score: Optional[int] = Query(None, description="Minimum score"),
    stage: Optional[str] = Query(None, description="Pipeline stage filter"),
    contact_only: bool = Query(False, description="Only leads with contact info"),
    search: Optional[str] = Query(None, description="Search term"),
    sort_by: str = Query("score", description="Sort field"),
    sort_dir: str = Query("DESC", description="Sort direction"),
    zips: Optional[str] = Query(None, description="Comma-separated zip codes"),
    source: Optional[str] = Query(None, description="Filter by data source"),
    request: Request = None,
):
    """Get leads directly from database with filtering (for admin/detail views)."""
    zip_list = [z.strip() for z in zips.split(",")] if zips else None

    leads, total = query_leads(
        limit=min(limit, 5000),
        offset=offset,
        city=city,
        state=state,
        temperature=temperature,
        min_score=min_score,
        max_days=days,
        permit_type=permit_type,
        source=source,
        stage=stage,
        contact_only=contact_only,
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
        zip_codes=zip_list,
    )

    # Add readiness scores (batch — avoid N+1 dict-merge overhead)
    enriched = [{**lead, **compute_readiness(lead)} for lead in leads]

    return {
        "leads": enriched,
        "total": total,
        "returned": len(enriched),
        "offset": offset,
        "source": "database",
    }


@router.get("/leads/bbox")
async def get_leads_bbox(
    south: float = Query(..., description="South latitude"),
    west: float = Query(..., description="West longitude"),
    north: float = Query(..., description="North latitude"),
    east: float = Query(..., description="East longitude"),
    limit: int = Query(2000),
    min_score: Optional[int] = Query(None),
    permit_type: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    request: Request = None,
):
    """Return leads within bounding box."""
    leads, total = query_leads(
        limit=limit,
        bbox=(south, west, north, east),
        min_score=min_score,
        permit_type=permit_type,
        city=city,
        sort_by="score",
    )
    return {
        "leads": leads,
        "total": total,
        "returned": len(leads),
    }


@router.get("/lead/{lead_id}/details")
async def lead_details(lead_id: str):
    """Get detailed info for a single lead."""
    lead = get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    readiness = compute_readiness(lead)
    lead.update(readiness)
    lead["is_insurance"] = is_insurance_claim(lead)
    lead["score_breakdown"] = score_breakdown(lead)

    return lead


@router.get("/lead/{lead_id}/score-breakdown")
async def get_score_breakdown(lead_id: str):
    """Get detailed score breakdown for a lead."""
    lead = get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return score_breakdown(lead)


# NOTE: POST /lead/{lead_id}/stage is served by main.py (updates in-memory cache + DB).
# This router provides DB-only stage persistence for integer DB IDs.

@router.post("/lead/{lead_id}/stage/db")
async def save_lead_stage_db(lead_id: str, payload: dict):
    """Update pipeline stage for a lead (DB-only, use /api/lead/{id}/stage for primary)."""
    lead = get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found in DB")

    new_stage = payload.get("stage", "new")
    old_stage = lead.get("stage", "new")
    notes = payload.get("notes", "")

    save_stage_transition(lead_id, old_stage, new_stage, notes)

    if new_stage == "won":
        create_notification(
            "stage_change", lead_id,
            f"Lead won: {lead.get('address', 'Unknown')} (${lead.get('valuation', 0):,.0f})"
        )

    return {"status": "ok", "lead_id": lead_id, "stage": new_stage}


@router.post("/lead/{lead_id}/notes")
async def save_lead_notes(lead_id: str, payload: dict):
    """Save notes for a lead."""
    lead = get_lead_by_id(lead_id)
    if not lead:
        # Fall through gracefully — note still saved via main.py's /note endpoint
        return {"status": "ok", "lead_id": lead_id, "message": "Lead not in DB, use /api/lead/{id}/note"}

    notes = payload.get("notes", "")
    update_lead(lead_id, {"notes": notes})
    return {"status": "ok", "lead_id": lead_id}


@router.post("/lead/{lead_id}/tags")
async def save_lead_tags(lead_id: str, payload: dict):
    """Save tags for a lead."""
    import json
    lead = get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    tags = payload.get("tags", [])
    update_lead(lead_id, {"tags": json.dumps(tags)})
    return {"status": "ok", "lead_id": lead_id, "tags": tags}


@router.get("/lead/{lead_id}/history")
async def get_lead_history(lead_id: str):
    """Get pipeline history for a lead."""
    return {"history": get_pipeline_history(lead_id)}


@router.get("/lead/{lead_id}/comparables")
async def get_comparables(lead_id: str):
    """Find comparable permits nearby."""
    lead = get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    lat, lng = lead.get("lat", 0), lead.get("lng", 0)
    if not lat or not lng:
        return {"comparables": []}

    # Find nearby leads with similar permit type
    comps, _ = query_leads(
        limit=10,
        bbox=(lat - 0.01, lng - 0.01, lat + 0.01, lng + 0.01),
        sort_by="valuation",
    )
    # Exclude the lead itself
    comps = [c for c in comps if c.get("id") != lead_id][:5]

    return {"comparables": comps}


@router.get("/lead/{lead_id}/competitors")
async def get_competitors(lead_id: str):
    """Find leads with contractors already listed (competition)."""
    lead = get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    lat, lng = lead.get("lat", 0), lead.get("lng", 0)
    if not lat or not lng:
        return {"competitors": []}

    nearby, _ = query_leads(
        limit=20,
        bbox=(lat - 0.02, lng - 0.02, lat + 0.02, lng + 0.02),
        contact_only=True,
    )
    competitors = [
        {
            "contractor": n.get("contractor_name"),
            "address": n.get("address"),
            "permit_type": n.get("permit_type"),
            "valuation": n.get("valuation"),
            "distance": "nearby",
        }
        for n in nearby
        if n.get("contractor_name") and str(n.get("id")) != str(lead_id)
    ][:10]

    return {"competitors": competitors}


@router.get("/stats")
async def get_lead_stats():
    """Get aggregate lead statistics."""
    return get_stats()


# ============================================================================
# SAVED FILTERS
# ============================================================================

@router.get("/filters")
async def list_filters():
    """List saved filter presets."""
    return {"filters": get_saved_filters()}


@router.post("/filters")
async def create_filter(payload: dict):
    """Save a new filter preset."""
    name = payload.get("name", "Untitled")
    filters = payload.get("filters", {})
    notify = payload.get("notify", False)

    filter_id = save_filter(name, filters, notify)
    return {"id": filter_id, "name": name, "status": "saved"}


@router.delete("/filters/{filter_id}")
async def remove_filter(filter_id: str):
    """Delete a saved filter."""
    delete_saved_filter(filter_id)
    return {"status": "deleted", "id": filter_id}


# ============================================================================
# NOTIFICATIONS
# ============================================================================

@router.post("/leads/bulk-stage")
async def bulk_stage_change(payload: dict):
    """Bulk update pipeline stage for multiple leads."""
    lead_ids = payload.get("lead_ids", [])
    new_stage = payload.get("stage", "")
    notes = payload.get("notes", "")

    if not lead_ids or not new_stage:
        raise HTTPException(status_code=400, detail="lead_ids and stage are required")
    if len(lead_ids) > 500:
        raise HTTPException(status_code=400, detail="Max 500 leads per bulk operation")

    valid_stages = {"new", "contacted", "qualified", "proposal", "won", "lost"}
    if new_stage not in valid_stages:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {', '.join(valid_stages)}")

    updated = 0
    for lid in lead_ids:
        lead = get_lead_by_id(str(lid))
        if lead:
            old_stage = lead.get("stage", "new")
            save_stage_transition(str(lid), old_stage, new_stage, notes)
            update_lead(str(lid), {"stage": new_stage})
            updated += 1

    return {"status": "ok", "updated": updated, "stage": new_stage}


@router.get("/leads/export")
async def export_leads_csv(
    days: int = Query(None, description="Filter to permits issued in last N days"),
    permit_type: Optional[str] = Query(None),
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    temperature: Optional[str] = Query(None),
    min_score: Optional[int] = Query(None),
    stage: Optional[str] = Query(None),
    contact_only: bool = Query(False),
    search: Optional[str] = Query(None),
    zips: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    limit: int = Query(50000, description="Max rows to export"),
):
    """Export leads as CSV with the same filters as /api/leads/db."""
    zip_list = [z.strip() for z in zips.split(",")] if zips else None
    capped_limit = min(limit, 50000)

    leads, total = query_leads(
        limit=capped_limit,
        offset=0,
        city=city,
        state=state,
        temperature=temperature,
        min_score=min_score,
        max_days=days,
        permit_type=permit_type,
        source=source,
        stage=stage,
        contact_only=contact_only,
        search=search,
        sort_by="score",
        sort_dir="DESC",
        zip_codes=zip_list,
    )

    if not leads:
        raise HTTPException(status_code=404, detail="No leads match the given filters")

    # Build CSV in memory
    columns = [
        "address", "city", "state", "zip", "permit_number", "permit_type",
        "work_description", "valuation", "issue_date", "score", "temperature",
        "owner_name", "owner_phone", "owner_email", "contractor_name",
        "contractor_phone", "lat", "lng", "source", "stage",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for lead in leads:
        writer.writerow({col: lead.get(col, "") for col in columns})

    csv_bytes = output.getvalue().encode("utf-8")
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=onsite_leads.csv"},
    )


@router.get("/notifications")
async def list_notifications(limit: int = Query(50)):
    """Get unread notifications."""
    return {"notifications": get_unread_notifications(limit)}


@router.post("/notifications/read")
async def read_notifications(payload: dict):
    """Mark notifications as read."""
    ids = payload.get("ids", [])
    mark_notifications_read(ids)
    return {"status": "ok", "marked": len(ids)}
