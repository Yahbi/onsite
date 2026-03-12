"""
Marketplace engine implementing territorial scarcity, dynamic pricing,
and entitlement-aware lead filtering without changing existing UI routes.
Uses a lightweight SQLite db (market.db) by default to avoid breaking
the current stack; can be pointed at Postgres via MARKET_DB_PATH.
"""

import os
import uuid
import sqlite3
import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional, List

BASE_DIR = Path(__file__).parent
MARKET_DB_PATH = os.getenv("MARKET_DB_PATH", str(BASE_DIR / "market.db"))

ENTRY_PRICE = Decimal("249")
PRIORITY_PRICE = Decimal("599")
EXCLUSIVE_BASE = Decimal("1500")

DEMAND_STEPS = [
    (0, Decimal("1.0")),
    (3, Decimal("1.2")),
    (6, Decimal("1.45")),
    (10, Decimal("1.9")),
]

def _conn():
    return sqlite3.connect(MARKET_DB_PATH)


def init_market_tables():
    mig = BASE_DIR / "migrations" / "001_marketplace.sql"
    if not mig.exists():
        return
    conn = _conn()
    with open(mig) as f:
        sql = f.read()
    conn.executescript(sql)
    conn.commit()
    conn.close()


def demand_multiplier(waitlist_count: int) -> Decimal:
    mult = Decimal("1.0")
    for threshold, factor in DEMAND_STEPS:
        if waitlist_count >= threshold:
            mult = factor
    return mult


def value_multiplier(median_home_value: Decimal, permit_score: Decimal) -> Decimal:
    # Simple heuristic: normalize median home value around $800k and permit_score 0-1
    base = Decimal("800000")
    mv_factor = (median_home_value / base) if base > 0 else Decimal("1")
    mv_factor = max(Decimal("0.6"), min(Decimal("1.6"), mv_factor))
    ps_factor = max(Decimal("0.8"), min(Decimal("1.3"), (permit_score or Decimal("0")) + Decimal("1")))
    return mv_factor * ps_factor


def compute_price(zip_code: str, plan: str) -> Decimal:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT median_home_value, permit_volume_score, base_price FROM territories WHERE zip_code=?", (zip_code,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return ENTRY_PRICE
    median_home_value = Decimal(str(row[0] or 0))
    permit_score = Decimal(str(row[1] or 0))
    base_price = Decimal(str(row[2] or ENTRY_PRICE))
    cur.execute("SELECT waitlist_count FROM territory_seats WHERE zip_code=? AND plan_type=?", (zip_code, plan))
    seat = cur.fetchone()
    waitlist = seat[0] if seat else 0
    conn.close()

    dm = demand_multiplier(waitlist)
    vm = value_multiplier(median_home_value, permit_score)
    price = base_price * dm * vm
    if plan == "ENTRY":
        price = ENTRY_PRICE * dm * vm
    elif plan == "PRIORITY":
        price = PRIORITY_PRICE * dm * vm
    else:  # EXCLUSIVE
        price = EXCLUSIVE_BASE * dm * vm
    return price.quantize(Decimal("0.01"))


def upsert_pricing(zip_code: str):
    conn = _conn()
    cur = conn.cursor()
    for plan in ("ENTRY", "PRIORITY", "EXCLUSIVE"):
        price = compute_price(zip_code, plan)
        cur.execute(
            """
            INSERT INTO territory_pricing (id, zip_code, plan_type, price, computed_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(zip_code, plan_type) DO UPDATE SET price=excluded.price, computed_at=excluded.computed_at
            """,
            (str(uuid.uuid4()), zip_code, plan, float(price), dt.datetime.utcnow()),
        )
    conn.commit()
    conn.close()


def metrics(zip_code: str) -> Dict:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT plan_type, seat_limit, active_count, waitlist_count FROM territory_seats WHERE zip_code=?", (zip_code,))
    rows = cur.fetchall()
    cur.execute("SELECT count(*) FROM territory_waitlist WHERE zip_code=?", (zip_code,))
    wl_row = cur.fetchone()
    wl = wl_row[0] if wl_row else 0
    cur.execute("SELECT price FROM territory_pricing WHERE zip_code=? AND plan_type='EXCLUSIVE' ORDER BY computed_at DESC LIMIT 1", (zip_code,))
    ex_price_row = cur.fetchone()
    conn.close()
    seats_taken = sum(r[2] for r in rows) if rows else 0
    seats_total = sum(r[1] for r in rows) if rows else 0
    plan_availability = {r[0]: max(0, r[1]-r[2]) for r in rows} if rows else {}
    next_price = {p: float(compute_price(zip_code, p)) for p in ("ENTRY","PRIORITY","EXCLUSIVE")}
    return {
        "zip": zip_code,
        "seats_taken": seats_taken,
        "seats_remaining": max(0, seats_total - seats_taken),
        "contractors_monitoring": seats_taken,
        "waitlist_count": wl,
        "exclusive_price": ex_price_row[0] if ex_price_row else None,
        "upgrade_available": any(r[0] != "EXCLUSIVE" and r[2] < r[1] for r in rows) if rows else True,
        "plan_availability": plan_availability,
        "next_price": next_price,
    }
