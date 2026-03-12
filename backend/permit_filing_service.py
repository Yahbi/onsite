import uuid
import datetime as dt
import json
from marketplace_engine import _conn

def save_permit_request(user_id: str, lead_id: str, city: str, payload: dict, pdf_url: str = None):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO permit_requests (id, user_id, lead_id, city, payload_json, pdf_url, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (str(uuid.uuid4()), user_id, lead_id, city, json.dumps(payload), pdf_url, "submitted", dt.datetime.utcnow()),
    )
    conn.commit()
    conn.close()


def generate_permit_payload(lead: dict) -> dict:
    return {
        "owner_name": lead.get("owner_name"),
        "address": lead.get("address"),
        "city": lead.get("city"),
        "zip": lead.get("zip"),
        "permit_type": lead.get("permit_type"),
        "description": lead.get("description") or lead.get("work_description"),
        "valuation": lead.get("valuation"),
        "apn": lead.get("apn"),
    }


def generate_prefilled_pdf_stub(payload: dict) -> str:
    # Placeholder: in production render PDF template
    return "s3://prefill/placeholder.pdf"
