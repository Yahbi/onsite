"""
Stripe webhook handler for Onsite.
Processes subscription lifecycle events and updates the subscriptions table in leads.db.
"""

import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "leads.db"


class WebhookError(Exception):
    pass


def _connect():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _get_user_email(user_id: str) -> str | None:
    """Look up user email by ID."""
    conn = _connect()
    try:
        row = conn.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
        return row["email"] if row else None
    finally:
        conn.close()


def _send_email_async(coro):
    """Fire-and-forget an async email send from sync context."""
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(coro)
        else:
            loop.run_until_complete(coro)
    except Exception as exc:
        log.warning("Email notification failed (non-blocking): %s", exc)


def handle_event(event: dict) -> dict:
    """Route a Stripe webhook event to the appropriate handler."""
    etype = event.get("type", "")
    data = event.get("data", {}).get("object", {})

    log.info("Stripe webhook: %s", etype)

    try:
        if etype == "checkout.session.completed":
            _handle_checkout_completed(data)
        elif etype == "invoice.paid":
            _handle_invoice_paid(data)
        elif etype == "invoice.payment_failed":
            _handle_payment_failed(data)
        elif etype in ("customer.subscription.updated", "customer.subscription.deleted"):
            _handle_subscription_change(data, etype)
        else:
            log.debug("Unhandled webhook event: %s", etype)
    except Exception as exc:
        log.error("Webhook handler error for %s: %s", etype, exc)

    return {"received": True}


def _handle_checkout_completed(session: dict):
    """New subscription created via checkout."""
    from models.subscription import upsert_subscription

    metadata = session.get("metadata", {})
    user_id = metadata.get("user_id")
    plan = metadata.get("plan_id") or metadata.get("plan", "free")
    customer_id = session.get("customer")
    subscription_id = session.get("subscription")

    if not user_id:
        log.warning("checkout.session.completed missing user_id in metadata")
        return

    upsert_subscription(
        user_id=user_id,
        plan=plan,
        status="active",
        stripe_customer_id=customer_id,
        stripe_subscription_id=subscription_id,
    )
    log.info("Subscription created: user=%s plan=%s", user_id, plan)

    # Send confirmation email
    email = _get_user_email(user_id)
    if email:
        from services.email_service import send_subscription_confirmation
        _send_email_async(send_subscription_confirmation(email, plan.title()))


def _handle_invoice_paid(invoice: dict):
    """Recurring payment succeeded — keep subscription active."""
    from models.subscription import update_subscription_status

    sub_id = invoice.get("subscription")
    if not sub_id:
        return

    period_end = invoice.get("lines", {}).get("data", [{}])[0].get("period", {}).get("end")
    end_str = None
    if period_end:
        end_str = datetime.fromtimestamp(period_end, tz=timezone.utc).isoformat()

    update_subscription_status(sub_id, "active", current_period_end=end_str)
    log.info("Invoice paid for subscription %s", sub_id)


def _handle_payment_failed(invoice: dict):
    """Payment failed — set subscription to past_due with grace period."""
    from models.subscription import update_subscription_status, get_subscription_by_stripe_id

    sub_id = invoice.get("subscription")
    if not sub_id:
        return

    grace_end = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    update_subscription_status(sub_id, "past_due", current_period_end=grace_end)
    log.warning("Payment failed for subscription %s — past_due with 3-day grace", sub_id)

    # Notify user
    sub = get_subscription_by_stripe_id(sub_id)
    if sub:
        email = _get_user_email(sub["user_id"])
        if email:
            from services.email_service import send_payment_failed
            _send_email_async(send_payment_failed(email, sub["plan"].title()))


def _handle_subscription_change(data: dict, event_type: str):
    """Handle subscription updates or cancellations."""
    from models.subscription import update_subscription_status, cancel_subscription

    sub_id = data.get("id")
    if not sub_id:
        return

    if event_type == "customer.subscription.deleted":
        cancel_subscription(sub_id)
        log.info("Subscription canceled: %s", sub_id)
    else:
        status = data.get("status", "active")
        period_end = data.get("current_period_end")
        end_str = None
        if period_end:
            end_str = datetime.fromtimestamp(period_end, tz=timezone.utc).isoformat()
        update_subscription_status(sub_id, status, current_period_end=end_str)
        log.info("Subscription updated: %s → %s", sub_id, status)
