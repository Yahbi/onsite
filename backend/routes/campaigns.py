"""
Onsite — Campaign & CRM Routes
Email/SMS sending and CRM integration endpoints.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["campaigns"])

# ── Lazy-load services (graceful if env vars missing) ──

_email_service = None
_sms_service = None
_crm_manager = None


def _get_email_service():
    global _email_service
    if _email_service is None:
        try:
            from services.email_service import send_email
            _email_service = send_email
        except Exception as e:
            logger.warning(f"Email service unavailable: {e}")
    return _email_service


def _get_sms_service():
    global _sms_service
    if _sms_service is None:
        try:
            from services.sms_campaigns import SMSCampaignService
            _sms_service = SMSCampaignService()
        except Exception as e:
            logger.warning(f"SMS service unavailable: {e}")
    return _sms_service


def _get_crm_manager():
    global _crm_manager
    if _crm_manager is None:
        try:
            from services.crm_integrations import CRMManager
            _crm_manager = CRMManager()
        except Exception as e:
            logger.warning(f"CRM manager unavailable: {e}")
    return _crm_manager


# ============================================================================
# EMAIL CAMPAIGNS
# ============================================================================

@router.post("/campaigns/email/send")
async def send_email_campaign(request: Request):
    """Send a single email to a lead."""
    body = await request.json()
    to_email = body.get("to_email", "")
    subject = body.get("subject", "")
    html_content = body.get("html_content", "")
    plain_content = body.get("plain_content", "")

    if not to_email or not subject:
        raise HTTPException(status_code=400, detail="to_email and subject required")

    svc = _get_email_service()
    if not svc:
        return {"success": False, "error": "Email service not configured. Set SENDGRID_API_KEY env var."}

    try:
        result = await svc(to=to_email, subject=subject, html_body=html_content)
        return {"success": result, "to": to_email}
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================================
# SMS CAMPAIGNS
# ============================================================================

@router.post("/campaigns/sms/send")
async def send_sms_campaign(request: Request):
    """Send a single SMS to a lead."""
    body = await request.json()
    to_phone = body.get("to_phone", "")
    message = body.get("message", "")

    if not to_phone or not message:
        raise HTTPException(status_code=400, detail="to_phone and message required")

    svc = _get_sms_service()
    if not svc:
        return {"success": False, "error": "SMS service not configured. Set TWILIO_ACCOUNT_SID env var."}

    try:
        result = await svc.send_sms(to_number=to_phone, message=message)
        return result
    except Exception as e:
        logger.error(f"SMS send failed: {e}")
        return {"success": False, "error": str(e)}


# ============================================================================
# CRM INTEGRATION
# ============================================================================

@router.get("/crm/{crm_name}/authorize")
async def crm_authorize(crm_name: str):
    """Get OAuth authorization URL for a CRM."""
    mgr = _get_crm_manager()
    if not mgr:
        return {"success": False, "error": "CRM integrations not configured"}

    try:
        auth_url = mgr.get_auth_url(crm_name.lower())
        if auth_url:
            return {"success": True, "auth_url": auth_url, "crm": crm_name}
        return {"success": False, "error": f"CRM '{crm_name}' not supported or not configured"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/crm/{crm_name}/callback")
async def crm_callback(crm_name: str, code: str, state: str = ""):
    """OAuth callback handler for CRM."""
    mgr = _get_crm_manager()
    if not mgr:
        return {"success": False, "error": "CRM integrations not configured"}

    try:
        result = await mgr.exchange_code(crm_name.lower(), code)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/crm/{crm_name}/sync")
async def crm_sync_leads(crm_name: str, request: Request):
    """Sync leads to a CRM."""
    mgr = _get_crm_manager()
    if not mgr:
        return {"success": False, "error": "CRM integrations not configured", "synced": 0}

    try:
        body = await request.json()
    except Exception:
        body = {}
    lead_ids = body.get("lead_ids")

    return {"status": "not_implemented", "message": "CRM sync is not yet available. Coming soon."}


@router.post("/crm/disconnect/{crm_name}")
async def crm_disconnect(crm_name: str):
    """Disconnect a CRM integration."""
    return {"status": "not_implemented", "message": "CRM disconnect is not yet available. Token cleanup not implemented."}
