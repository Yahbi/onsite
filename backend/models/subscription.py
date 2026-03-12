"""
Subscription model — stores plan subscriptions in leads.db (same DB as users).
All functions return new dicts; nothing is mutated in-place.
"""

import sqlite3
import uuid
import logging
from pathlib import Path
from datetime import datetime, timezone

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "leads.db"

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id                    TEXT PRIMARY KEY,
    user_id               TEXT NOT NULL,
    stripe_customer_id    TEXT,
    stripe_subscription_id TEXT,
    plan                  TEXT NOT NULL DEFAULT 'free',
    status                TEXT NOT NULL DEFAULT 'active',
    current_period_start  TEXT,
    current_period_end    TEXT,
    created_at            TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at            TEXT
);

CREATE INDEX IF NOT EXISTS idx_sub_user   ON subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_sub_stripe ON subscriptions(stripe_customer_id);
CREATE INDEX IF NOT EXISTS idx_sub_status ON subscriptions(status);
"""


def init_subscriptions_table():
    """Create the subscriptions table if it doesn't exist."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
        log.info("Subscriptions table ready in leads.db")
    finally:
        conn.close()


# Run on import
init_subscriptions_table()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_to_dict(row, columns) -> dict:
    """Convert a sqlite3 row tuple to a dict (immutable pattern)."""
    return dict(zip(columns, row))


def _connect():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

_COLUMNS = (
    "id", "user_id", "stripe_customer_id", "stripe_subscription_id",
    "plan", "status", "current_period_start", "current_period_end",
    "created_at", "updated_at",
)


def get_active_subscription(user_id: str) -> dict | None:
    """Return the active subscription for a user, or None."""
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT * FROM subscriptions WHERE user_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return _row_to_dict(row, _COLUMNS)
    finally:
        conn.close()


def get_subscription_by_stripe_id(stripe_subscription_id: str) -> dict | None:
    """Lookup subscription by Stripe subscription ID."""
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT * FROM subscriptions WHERE stripe_subscription_id = ? LIMIT 1",
            (stripe_subscription_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return _row_to_dict(row, _COLUMNS)
    finally:
        conn.close()


def get_subscription_by_customer(stripe_customer_id: str) -> dict | None:
    """Lookup active subscription by Stripe customer ID."""
    conn = _connect()
    try:
        cur = conn.execute(
            "SELECT * FROM subscriptions WHERE stripe_customer_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1",
            (stripe_customer_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return _row_to_dict(row, _COLUMNS)
    finally:
        conn.close()


def upsert_subscription(
    user_id: str,
    plan: str,
    status: str = "active",
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
    current_period_start: str | None = None,
    current_period_end: str | None = None,
) -> dict:
    """Create or update a subscription. Returns the new subscription dict."""
    now = _now()
    conn = _connect()
    try:
        # Check if user already has a subscription
        cur = conn.execute(
            "SELECT id FROM subscriptions WHERE user_id = ? ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        existing = cur.fetchone()

        if existing:
            sub_id = existing[0]
            conn.execute(
                """UPDATE subscriptions
                   SET plan = ?, status = ?, stripe_customer_id = COALESCE(?, stripe_customer_id),
                       stripe_subscription_id = COALESCE(?, stripe_subscription_id),
                       current_period_start = COALESCE(?, current_period_start),
                       current_period_end = COALESCE(?, current_period_end),
                       updated_at = ?
                   WHERE id = ?""",
                (plan, status, stripe_customer_id, stripe_subscription_id,
                 current_period_start, current_period_end, now, sub_id),
            )
        else:
            sub_id = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO subscriptions
                   (id, user_id, stripe_customer_id, stripe_subscription_id, plan, status,
                    current_period_start, current_period_end, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (sub_id, user_id, stripe_customer_id, stripe_subscription_id,
                 plan, status, current_period_start, current_period_end, now, now),
            )

        conn.commit()
        return {
            "id": sub_id,
            "user_id": user_id,
            "stripe_customer_id": stripe_customer_id,
            "stripe_subscription_id": stripe_subscription_id,
            "plan": plan,
            "status": status,
            "current_period_start": current_period_start,
            "current_period_end": current_period_end,
            "updated_at": now,
        }
    finally:
        conn.close()


def update_subscription_status(
    stripe_subscription_id: str,
    status: str,
    current_period_end: str | None = None,
) -> bool:
    """Update subscription status by Stripe subscription ID. Returns True if found."""
    conn = _connect()
    try:
        if current_period_end:
            conn.execute(
                "UPDATE subscriptions SET status = ?, current_period_end = ?, updated_at = ? WHERE stripe_subscription_id = ?",
                (status, current_period_end, _now(), stripe_subscription_id),
            )
        else:
            conn.execute(
                "UPDATE subscriptions SET status = ?, updated_at = ? WHERE stripe_subscription_id = ?",
                (status, _now(), stripe_subscription_id),
            )
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def cancel_subscription(stripe_subscription_id: str) -> bool:
    """Mark a subscription as canceled. Returns True if found."""
    return update_subscription_status(stripe_subscription_id, "canceled")
