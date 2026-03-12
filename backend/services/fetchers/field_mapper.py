"""
Universal field mapper for heterogeneous permit/parcel API schemas.
Maps raw API records to the standard lead format using ordered alias lists.
"""

import re
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# ── Ordered alias lists: first match wins ──────────────────────────────────
FIELD_ALIASES: Dict[str, List[str]] = {
    "address": [
        "address", "full_address", "street_address", "location_address",
        "site_address", "property_address", "primary_address", "addr",
        "original_address1", "originaladdress1", "originaladdress",
        "project_address", "location", "situs_address",
    ],
    "city": [
        "city", "original_city", "originalcity", "jurisdiction",
        "municipality", "place_name", "community", "town",
    ],
    "state": [
        "state", "st", "state_code", "state_abbreviation",
    ],
    "zip": [
        "zip_code", "zipcode", "zip", "original_zip", "originalzip",
        "postal_code", "postcode",
    ],
    "lat": [
        "latitude", "lat", "y",
    ],
    "lng": [
        "longitude", "lng", "lon", "long", "x",
    ],
    "permit_number": [
        "permit_nbr", "permit_number", "permit_num", "permit_no",
        "permit_", "application_permit_number", "permitnumber",
        "permit_id", "job__", "case_number", "record_number",
    ],
    "permit_type": [
        "permit_type", "permittype", "permit_type_desc", "work_class",
        "category", "type", "permit_class", "record_type",
        "permit_category", "work_type",
    ],
    "work_description": [
        "work_description", "work_desc", "description", "scope_of_work",
        "permit_type_definition", "purpose", "action_type",
        "project_description", "job_description", "comments",
    ],
    "valuation": [
        "valuation", "estimated_cost", "est_cost", "reported_cost",
        "total_job_valuation", "job_value", "value", "estimated_job_cost_",
        "const_cost", "total_fees", "declared_valuation", "project_cost",
        "construction_cost", "job_cost",
    ],
    "issue_date": [
        "issue_date", "issued_date", "issueddate", "issuedate",
        "permit_date", "date_issued", "permit_issue_date",
        "issuance_date", "filing_date", "filed_date",
        "application_date", "applied_date", "applieddate",
        "created_date", "date_entered", "application_start_date",
        "statusdate", "application_creation_date",
    ],
    "owner_name": [
        "owner_name", "ownername", "property_owner", "owner",
        "owner_s_business_name", "owner_business_name",
        "contact_1_name", "applicant_name",
    ],
    "owner_phone": [
        "owner_phone", "owner_s_phone__", "applicant_phone",
        "contact_phone", "phone",
    ],
    "owner_email": [
        "owner_email", "applicant_email", "contact_email", "email",
    ],
    "contractor_name": [
        "contractor_business_name", "contractorcompanyname",
        "contractor_company", "contractor", "contractor_name",
        "gc_name", "builder_name",
    ],
    "contractor_phone": [
        "contractor_phone", "gc_phone",
    ],
    "apn": [
        "apn", "parcel_number", "parcelid", "pin1", "block_lot",
        "assessor_parcel", "tax_id", "parcel_id",
    ],
    "zoning": [
        "zoning", "zone", "zoning_code", "zone_type",
    ],
}

# ArcGIS uses PascalCase and UPPER_CASE — build case-insensitive lookup
_ARCGIS_FIELD_ALIASES: Dict[str, List[str]] = {
    "address": [
        "Address", "ADDRESS", "FULL_ADDRESS", "FullAddress",
        "SiteAddress", "SITE_ADDRESS", "PropertyAddress",
    ],
    "permit_number": [
        "PermitNumber", "PERMIT_NUMBER", "PERMITNUMBER", "OBJECTID",
        "RecordNumber", "CaseNumber",
    ],
    "permit_type": [
        "PermitType", "PERMIT_TYPE", "Type", "RecordType",
        "WorkClass", "Category",
    ],
    "work_description": [
        "Description", "DESCRIPTION", "WorkDescription",
        "ScopeOfWork", "Comments",
    ],
    "valuation": [
        "Valuation", "VALUATION", "EstProjectCost", "JobValue",
        "EstimatedCost", "ConstructionCost",
    ],
    "issue_date": [
        "IssuedDate", "ISSUED_DATE", "IssueDate", "DateIssued",
        "ApplicationDate", "CreatedDate", "EditDate",
    ],
    "owner_name": [
        "OwnerName", "OWNER", "PropertyOwner", "ApplicantName",
    ],
    "contractor_name": [
        "ContractorName", "CONTRACTOR", "BuilderName",
    ],
    "apn": [
        "APN", "ParcelNumber", "PARCEL_NUMBER", "PIN",
    ],
    "zoning": [
        "Zoning", "ZONING", "ZoneType",
    ],
    "zip": [
        "ZipCode", "ZIP", "Zip", "PostalCode",
    ],
    "lat": ["y", "Y", "latitude", "Latitude", "LAT"],
    "lng": ["x", "X", "longitude", "Longitude", "LNG", "LON"],
}


def map_record(raw: Dict[str, Any], source_type: str = "socrata") -> Dict[str, Any]:
    """
    Map a raw API record to the standard lead field names.
    Returns a dict with canonical field names and cleaned values.
    source_type: 'socrata', 'arcgis', or 'ckan'
    """
    aliases = FIELD_ALIASES if source_type != "arcgis" else _ARCGIS_FIELD_ALIASES
    mapped = {}

    # Build case-insensitive lookup for the raw record
    raw_lower = {k.lower(): k for k in raw}

    for canonical, alias_list in aliases.items():
        for alias in alias_list:
            # Try exact match first
            if alias in raw and raw[alias] not in (None, "", "null", "NULL"):
                mapped[canonical] = raw[alias]
                break
            # Try case-insensitive
            alias_lc = alias.lower()
            if alias_lc in raw_lower:
                orig_key = raw_lower[alias_lc]
                val = raw[orig_key]
                if val not in (None, "", "null", "NULL"):
                    mapped[canonical] = val
                    break

    # For Socrata, also try composite address fields
    if source_type == "socrata" and "address" not in mapped:
        mapped["address"] = _build_composite_address(raw)

    # For ArcGIS, extract geometry
    if source_type == "arcgis":
        geom = raw.get("geometry", {})
        if geom:
            if "lat" not in mapped and "y" in geom:
                mapped["lat"] = geom["y"]
            if "lng" not in mapped and "x" in geom:
                mapped["lng"] = geom["x"]

    # For Socrata location objects
    if source_type == "socrata" and "lat" not in mapped:
        _extract_socrata_location(raw, mapped)

    return mapped


def _build_composite_address(raw: Dict[str, Any]) -> str:
    """Build address from component fields (SF/NYC style)."""
    # SF style: street_number + street_name + street_suffix
    sn = str(raw.get("street_number", "")).strip()
    sname = str(raw.get("street_name", "")).strip()
    ssuf = str(raw.get("street_suffix", "")).strip()
    if sn and sname:
        return f"{sn} {sname} {ssuf}".strip()

    # NYC style: house__ + street_name
    house = str(raw.get("house__", "")).strip()
    if house and sname:
        return f"{house} {sname}".strip()

    # Generic: street_number + direction + name + suffix
    sdir = str(raw.get("street_direction", "")).strip()
    suffix = str(raw.get("suffix", "")).strip()
    if sn and sname:
        return f"{sn} {sdir} {sname} {suffix}".strip()

    return ""


def _extract_socrata_location(raw: Dict[str, Any], mapped: Dict[str, Any]):
    """Extract lat/lng from Socrata location objects."""
    for loc_key in ("location", "mapped_location", "geocoded_location"):
        loc = raw.get(loc_key)
        if isinstance(loc, dict):
            lat = loc.get("latitude") or loc.get("lat")
            lng = loc.get("longitude") or loc.get("lng") or loc.get("lon")
            if lat and lng:
                try:
                    mapped["lat"] = float(lat)
                    mapped["lng"] = float(lng)
                    return
                except (ValueError, TypeError):
                    pass


def clean_valuation(raw_val: Any) -> float:
    """Parse valuation from various formats."""
    if raw_val is None:
        return 0.0
    s = str(raw_val).replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def parse_date(raw_date: Any) -> tuple:
    """
    Parse a date from various formats.
    Returns (iso_date_str, days_old) tuple.
    """
    if not raw_date:
        return ("", 0)

    # Epoch milliseconds (ArcGIS)
    if isinstance(raw_date, (int, float)) and raw_date > 1e10:
        dt = datetime.fromtimestamp(raw_date / 1000)
        iso = dt.strftime("%Y-%m-%d")
        days = max(0, (datetime.now() - dt).days)
        return (iso, days)

    # Epoch seconds
    if isinstance(raw_date, (int, float)) and raw_date > 1e8:
        dt = datetime.fromtimestamp(raw_date)
        iso = dt.strftime("%Y-%m-%d")
        days = max(0, (datetime.now() - dt).days)
        return (iso, days)

    s = str(raw_date).strip()

    # ISO 8601: 2024-03-15T... or 2024-03-15
    iso_match = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if iso_match:
        iso = iso_match.group(1)
        try:
            dt = datetime.fromisoformat(iso)
            days = max(0, (datetime.now() - dt).days)
            return (iso, days)
        except ValueError:
            return (iso, 0)

    # MM/DD/YYYY
    us_match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if us_match:
        m, d, y = us_match.groups()
        iso = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        try:
            dt = datetime(int(y), int(m), int(d))
            days = max(0, (datetime.now() - dt).days)
            return (iso, days)
        except ValueError:
            return (iso, 0)

    # DD-Mon-YYYY (e.g., 15-Mar-2024)
    mon_match = re.match(r"(\d{1,2})-(\w{3})-(\d{4})", s)
    if mon_match:
        try:
            dt = datetime.strptime(s[:11], "%d-%b-%Y")
            iso = dt.strftime("%Y-%m-%d")
            days = max(0, (datetime.now() - dt).days)
            return (iso, days)
        except ValueError:
            pass

    return (s[:10], 0)


def split_combined_address(addr: str) -> Dict[str, str]:
    """
    Split a combined address like '123 Main St, City, ST 12345'
    into components.
    """
    result = {"address": addr, "city": "", "state": "", "zip": ""}
    if not addr:
        return result

    # Try: "123 Main St, City, ST 12345"
    m = re.match(
        r"^(.+?),\s*([A-Za-z\s]+?),\s*([A-Z]{2})\s*(\d{5})?",
        addr.strip()
    )
    if m:
        result["address"] = m.group(1).strip()
        result["city"] = m.group(2).strip()
        result["state"] = m.group(3).strip()
        if m.group(4):
            result["zip"] = m.group(4).strip()
        return result

    # Try: "123 Main St, City ST 12345"
    m = re.match(
        r"^(.+?),\s*([A-Za-z\s]+?)\s+([A-Z]{2})\s*(\d{5})?",
        addr.strip()
    )
    if m:
        result["address"] = m.group(1).strip()
        result["city"] = m.group(2).strip()
        result["state"] = m.group(3).strip()
        if m.group(4):
            result["zip"] = m.group(4).strip()
        return result

    return result
