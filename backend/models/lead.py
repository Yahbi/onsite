"""
Onsite Data Models
Pydantic models for leads, API status, and pipeline
"""

from pydantic import BaseModel
from typing import List, Optional


class Lead(BaseModel):
    id: int
    permit_number: str
    address: str
    city: str
    zip: str
    lat: float
    lng: float
    work_description: str
    description_full: str = ""
    permit_type: str
    valuation: float
    issue_date: str
    days_old: int
    score: int
    temperature: str
    source: str
    contractor_name: str = ""
    contractor_phone: str = ""
    description_extra: str = ""
    owner_name: str = ""
    owner_phone: str = ""
    owner_email: str = ""
    owner_address: str = ""
    apn: str = ""
    state: str = ""
    permit_url: str = ""
    market_value: Optional[float] = None
    year_built: Optional[int] = None
    square_feet: Optional[float] = None
    lot_size: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    zoning: Optional[str] = None
    # Pipeline & CRM fields
    stage: Optional[str] = "new"
    notes: Optional[str] = ""
    tags: Optional[List[str]] = None
    contacted_at: Optional[str] = None
    # Enrichment metadata
    enrichment_status: Optional[str] = None  # pending, enriched, failed
    enrichment_date: Optional[str] = None
    # AI scoring
    readiness_score: Optional[float] = None
    recommended_action: Optional[str] = None
    contact_window_days: Optional[int] = None
    urgency_level: Optional[str] = None
    stage_index: Optional[int] = None
    budget_range: Optional[List[float]] = None
    competition_level: Optional[str] = None


class APIStatus(BaseModel):
    name: str
    status: str
    response_time_ms: Optional[int] = None
    last_check: str
    details: Optional[str] = None


class SavedFilter(BaseModel):
    """User-saved filter preset"""
    id: Optional[str] = None
    name: str
    filters: dict  # {city, score_min, temperature, days, permit_type, etc.}
    created_at: Optional[str] = None
    notify: bool = False  # Send alerts for new matching leads


class PipelineStage(BaseModel):
    """Pipeline stage transition"""
    lead_id: int
    stage: str  # new, contacted, quoted, proposal, won, lost
    notes: Optional[str] = None
    timestamp: Optional[str] = None
