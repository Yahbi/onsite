"""
Billing routes for Onsite — Stripe subscription management.
Handles checkout, portal, webhook, and plan queries.
"""

import os
import json
import base64
import logging
import sqlite3
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from backend.services.billing_webhook_handler import handle_event as stripe_handle_event

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/billing", tags=["billing"])

# ---------------------------------------------------------------------------
# Plan definitions
# ---------------------------------------------------------------------------

ANNUAL_DISCOUNT = 0.20  # 20% off for annual billing

STRIPE_PLANS = {
    "starter": {
        "name": "Starter",
        "price_monthly": 74900,
        "price_annual": int(74900 * 12 * (1 - ANNUAL_DISCOUNT)),  # ~$7,190/yr ($599/mo effective)
        "leads_per_month": 500,
        "enrichments_per_month": 50,
        "sms_per_month": 50,
        "export_csv": True,
        "crm_sync": False,
        "api_access": False,
    },
    "pro": {
        "name": "Pro",
        "price_monthly": 149900,
        "price_annual": int(149900 * 12 * (1 - ANNUAL_DISCOUNT)),  # ~$14,390/yr ($1,199/mo effective)
        "leads_per_month": -1,
        "enrichments_per_month": 500,
        "sms_per_month": 200,
        "export_csv": True,
        "crm_sync": False,
        "api_access": False,
    },
    "enterprise": {
        "name": "Enterprise",
        "price_monthly": 255500,
        "price_annual": int(255500 * 12 * (1 - ANNUAL_DISCOUNT)),  # ~$24,528/yr ($2,044/mo effective)
        "leads_per_month": -1,
        "enrichments_per_month": 5000,
        "sms_per_month": 2000,
        "export_csv": True,
        "crm_sync": True,
        "api_access": True,
    },
}

FREE_PLAN = {
    "name": "Free",
    "leads_per_month": 50,
    "enrichments_per_month": 5,
    "sms_per_month": 0,
    "export_csv": False,
    "crm_sync": False,
    "api_access": False,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_path() -> str:
    return str(Path(__file__).resolve().parent.parent / "leads.db")


def _extract_user_from_token(request: Request) -> tuple[str | None, str | None]:
    """Extract user_id and email from JWT token. Returns (user_id, email)."""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token or token == "demo":
        return None, None
    try:
        parts = token.split(".")
        pad = "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
        return str(payload.get("sub", "")), payload.get("email")
    except Exception:
        return None, None


def get_user_plan(user_id: str) -> dict:
    """Get the active plan for a user. Returns FREE_PLAN if no subscription."""
    try:
        conn = sqlite3.connect(_db_path(), timeout=10)
        cur = conn.cursor()
        cur.execute(
            "SELECT plan FROM subscriptions WHERE user_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        conn.close()
        if row and row[0] in STRIPE_PLANS:
            return {**STRIPE_PLANS[row[0]], "plan_id": row[0]}
        return {**FREE_PLAN, "plan_id": "free"}
    except Exception:
        return {**FREE_PLAN, "plan_id": "free"}


def _get_stripe_customer_id(user_id: str) -> str | None:
    """Look up the Stripe customer ID for a user."""
    try:
        conn = sqlite3.connect(_db_path(), timeout=10)
        cur = conn.cursor()
        cur.execute(
            "SELECT stripe_customer_id FROM subscriptions WHERE user_id = ? AND stripe_customer_id IS NOT NULL ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/my-plan")
async def my_plan(request: Request):
    """Return the current user's plan and feature entitlements."""
    user_id, _ = _extract_user_from_token(request)
    if not user_id:
        return {**FREE_PLAN, "plan_id": "free"}
    return get_user_plan(user_id)


@router.get("/plans")
async def list_plans():
    """Return available subscription plans with annual discount info."""
    return {"plans": STRIPE_PLANS, "annual_discount_pct": int(ANNUAL_DISCOUNT * 100)}


@router.post("/checkout")
async def create_checkout_session(request: Request):
    """Create a Stripe Checkout session for a subscription plan."""
    stripe_key = os.getenv("STRIPE_SECRET_KEY", "")
    if not stripe_key:
        raise HTTPException(status_code=503, detail="Stripe not configured — set STRIPE_SECRET_KEY")

    body = await request.json()
    plan_id = body.get("plan", "pro")
    interval = body.get("interval", "month")  # "month" or "year"
    success_url = body.get("success_url")
    cancel_url = body.get("cancel_url")

    if plan_id not in STRIPE_PLANS:
        raise HTTPException(status_code=400, detail=f"Invalid plan: {plan_id}")
    if interval not in ("month", "year"):
        raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}. Use 'month' or 'year'.")

    plan = STRIPE_PLANS[plan_id]
    user_id, user_email = _extract_user_from_token(request)

    # Pick price based on interval
    if interval == "year":
        unit_amount = plan["price_annual"]
    else:
        unit_amount = plan["price_monthly"]

    # Build default URLs
    origin = str(request.base_url).rstrip("/")
    if not success_url:
        success_url = f"{origin}/app?payment=success"
    if not cancel_url:
        cancel_url = f"{origin}/app?payment=cancelled"

    try:
        import stripe
        stripe.api_key = stripe_key

        session_params = {
            "mode": "subscription",
            "line_items": [{
                "price_data": {
                    "currency": "usd",
                    "unit_amount": unit_amount,
                    "recurring": {"interval": interval},
                    "product_data": {"name": f"Onsite {plan['name']} Plan ({interval}ly)"},
                },
                "quantity": 1,
            }],
            "success_url": success_url,
            "cancel_url": cancel_url,
            "metadata": {
                "plan_id": plan_id,
                "user_id": user_id or "",
                "interval": interval,
            },
            "subscription_data": {
                "metadata": {
                    "plan_id": plan_id,
                    "user_id": user_id or "",
                    "interval": interval,
                },
            },
        }
        if user_email:
            session_params["customer_email"] = user_email

        session = stripe.checkout.Session.create(**session_params)

        return {
            "checkout_url": session.url,
            "session_id": session.id,
        }
    except ImportError:
        raise HTTPException(status_code=503, detail="stripe package not installed")
    except Exception as e:
        logger.error("Stripe checkout error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/portal")
async def create_billing_portal(request: Request):
    """Create a Stripe Customer Portal session for managing subscriptions."""
    stripe_key = os.getenv("STRIPE_SECRET_KEY", "")
    if not stripe_key:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    # Try to get customer_id from request body or from user's subscription
    body = await request.json()
    customer_id = body.get("customer_id", "")

    if not customer_id:
        user_id, _ = _extract_user_from_token(request)
        if user_id:
            customer_id = _get_stripe_customer_id(user_id)

    if not customer_id:
        raise HTTPException(status_code=400, detail="No active subscription found")

    try:
        import stripe
        stripe.api_key = stripe_key

        origin = str(request.base_url).rstrip("/")
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=body.get("return_url", f"{origin}/app"),
        )
        return {"portal_url": session.url}
    except Exception as e:
        logger.error("Stripe portal error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/webhook")
async def billing_webhook(request: Request):
    """Handle Stripe webhook events with optional signature verification."""
    stripe_secret = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    body = await request.body()

    if stripe_secret:
        try:
            import stripe
            sig_header = request.headers.get("stripe-signature", "")
            event = stripe.Webhook.construct_event(body, sig_header, stripe_secret)
            return stripe_handle_event(event)
        except Exception as e:
            logger.error("Stripe webhook verification failed: %s", e)
            raise HTTPException(status_code=400, detail="Invalid signature")
    else:
        # Dev mode: accept raw payload without signature verification
        payload = json.loads(body)
        return stripe_handle_event(payload)
