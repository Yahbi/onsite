"""
Yelp Intent Provider (non-scraping)
-----------------------------------
Collects public, indexed Yelp-related text via search-engine APIs (e.g., SerpAPI),
classifies renovation intent, approximates location, matches parcels, and enriches
owner contact using existing assessor + enrichment modules.

No direct Yelp scraping, no auth bypass. All inputs must come from public search
result snippets or cached public pages reachable via search APIs.
"""

import asyncio
import hashlib
import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple

import aiohttp

SERP_API_KEY = os.getenv("SERP_API_KEY")  # SerpAPI key (Google/Bing results). Optional.

INTENT_DB_PATH = os.getenv("INTENT_DB_PATH", "/app/intent_leads.db")

# Basic neighborhood centroids (partial list, extend as needed)
NEIGHBORHOODS = {
    "sherman oaks": (34.1510, -118.4490),
    "studio city": (34.1396, -118.3871),
    "van nuys": (34.1867, -118.4490),
    "westwood": (34.0635, -118.4460),
    "brentwood": (34.0524, -118.4730),
    "pacific palisades": (34.0422, -118.5572),
    "santa monica": (34.0195, -118.4912),
    "culver city": (34.0211, -118.3965),
    "mar vista": (34.0026, -118.4306),
    "venice": (33.9934, -118.4790),
    "silver lake": (34.0869, -118.2702),
    "echo park": (34.0772, -118.2606),
    "hollywood": (34.0928, -118.3287),
    "los feliz": (34.1067, -118.2915),
}

# Heuristic keywords for classification
HIGH_INTENT = [
    "addition", "adu", "plans approved", "permit", "permits", "architect",
    "structural", "engineer", "rebuild", "extension", "full remodel",
    "gut", "foundation", "load bearing", "plan check", "building dept",
]
LOW_INTENT = ["repair", "patch", "fix", "handyman", "faucet", "paint job", "minor"]


@dataclass
class YelpIntentLead:
    reviewer_name: str
    review_text: str
    project_type: str
    intent_stage: str
    confidence: float
    budget_signal: str
    mentions_permits: bool
    mentions_architect: bool
    geo_circle: dict
    apn_candidates: list
    selected_apn: Optional[str]
    owner_name: Optional[str]
    phones: list
    emails: list
    lead_score: float
    created_at: float
    reviewer_hash: str
    source_url: str
    city_hint: str


# SQLite storage -------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(INTENT_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS yelp_intent_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reviewer_hash TEXT UNIQUE,
            reviewer_name TEXT,
            review_text TEXT,
            project_type TEXT,
            intent_stage TEXT,
            confidence REAL,
            budget_signal TEXT,
            mentions_permits INTEGER,
            mentions_architect INTEGER,
            geo_circle TEXT,
            apn_candidates TEXT,
            selected_apn TEXT,
            owner_name TEXT,
            phones TEXT,
            emails TEXT,
            lead_score REAL,
            created_at REAL,
            source_url TEXT,
            city_hint TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def save_lead(lead: YelpIntentLead):
    conn = sqlite3.connect(INTENT_DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT OR IGNORE INTO yelp_intent_leads (
                reviewer_hash, reviewer_name, review_text, project_type, intent_stage, confidence,
                budget_signal, mentions_permits, mentions_architect, geo_circle, apn_candidates,
                selected_apn, owner_name, phones, emails, lead_score, created_at, source_url, city_hint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lead.reviewer_hash,
                lead.reviewer_name,
                lead.review_text,
                lead.project_type,
                lead.intent_stage,
                lead.confidence,
                lead.budget_signal,
                int(lead.mentions_permits),
                int(lead.mentions_architect),
                json.dumps(lead.geo_circle),
                json.dumps(lead.apn_candidates),
                lead.selected_apn,
                lead.owner_name,
                json.dumps(lead.phones),
                json.dumps(lead.emails),
                lead.lead_score,
                lead.created_at,
                lead.source_url,
                lead.city_hint,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_intent_leads(limit: int = 200):
    conn = sqlite3.connect(INTENT_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT reviewer_name, review_text, project_type, intent_stage, confidence, "
        "budget_signal, mentions_permits, mentions_architect, geo_circle, apn_candidates, "
        "selected_apn, owner_name, phones, emails, lead_score, created_at, source_url, city_hint "
        "FROM yelp_intent_leads ORDER BY created_at DESC LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    results = []
    for r in rows:
        results.append(
            {
                "reviewer_name": r[0],
                "review_text": r[1],
                "project_type": r[2],
                "intent_stage": r[3],
                "confidence": r[4],
                "budget_signal": r[5],
                "mentions_permits": bool(r[6]),
                "mentions_architect": bool(r[7]),
                "geo_circle": json.loads(r[8]),
                "apn_candidates": json.loads(r[9]),
                "selected_apn": r[10],
                "owner_name": r[11],
                "phones": json.loads(r[12]),
                "emails": json.loads(r[13]),
                "lead_score": r[14],
                "created_at": r[15],
                "source_url": r[16],
                "city_hint": r[17],
            }
        )
    return results


# Utility --------------------------------------------------------------------

def reviewer_hash(name: str, text: str) -> str:
    h = hashlib.sha256()
    h.update((name or "").lower().encode("utf-8"))
    h.update((text or "").lower().encode("utf-8"))
    return h.hexdigest()


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Discovery via SerpAPI ------------------------------------------------------

async def serp_search(session: aiohttp.ClientSession, query: str, num: int = 5) -> List[Dict]:
    if not SERP_API_KEY:
        return []
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERP_API_KEY,
        "num": num,
    }
    url = "https://serpapi.com/search"
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        if resp.status != 200:
            return []
        data = await resp.json()
        return data.get("organic_results", [])


def build_queries(cities: List[str]) -> List[str]:
    base = [
        '"kitchen remodel"',
        '"home addition" contractor review',
        '"ADU" "we hired"',
        '"bathroom renovation" "our house"',
        '"permits" contractor',
        '"architect" project',
        '"foundation work"',
        '"full gut renovation"',
        '"we spent" remodel',
    ]
    qs = []
    for city in cities:
        for b in base:
            qs.append(f'site:yelp.com {b} "{city}"')
    return qs


# Parsing & classification ----------------------------------------------------

def parse_snippet(item: Dict) -> Tuple[str, str, str]:
    title = item.get("title") or ""
    snippet = item.get("snippet") or ""
    link = item.get("link") or ""
    reviewer_name = title.split("–")[0].strip() if "–" in title else title[:60].strip()
    return reviewer_name, snippet, link


def classify_intent(text: str) -> Dict:
    t = (text or "").lower()
    score = 0.0
    for kw in HIGH_INTENT:
        if kw in t:
            score += 0.12
    for kw in LOW_INTENT:
        if kw in t:
            score -= 0.08
    score = max(0, min(1, score))
    budget = "medium"
    if any(x in t for x in ["million", "1m", "2m", "expensive", "high-end", "custom home"]):
        budget = "high"
    if any(x in t for x in ["cheap", "budget", "affordable", "low cost"]):
        budget = "low"
    stage = "planning"
    if any(x in t for x in ["bidding", "bid", "quote"]):
        stage = "bidding"
    if any(x in t for x in ["under construction", "in progress", "demo", "permits approved"]):
        stage = "in_progress"
    if any(x in t for x in ["finished", "completed", "done with our remodel"]):
        stage = "completed"
    return {
        "project_type": "remodel" if "remodel" in t else "addition" if "addition" in t or "adu" in t else "project",
        "confidence": score,
        "budget_signal": budget,
        "stage": stage,
        "mentions_permits": any(x in t for x in ["permit", "permits", "plan check"]),
        "mentions_architect": any(x in t for x in ["architect", "architectural", "architects"]),
    }


# Location resolution --------------------------------------------------------

def resolve_geocircle(text: str, city_hint: str) -> Optional[dict]:
    t = (text or "").lower()
    for n, (lat, lng) in NEIGHBORHOODS.items():
        if n in t:
            return {"lat": lat, "lng": lng, "radius": 1200}
    if city_hint and "los angeles" in city_hint.lower():
        return {"lat": 34.05, "lng": -118.24, "radius": 8000}
    return None


# Parcel matching ------------------------------------------------------------

def match_parcels(geo_circle: dict, parcels: List[dict], max_candidates: int = 5) -> List[dict]:
    if not geo_circle:
        return []
    lat0, lng0, r = geo_circle["lat"], geo_circle["lng"], geo_circle["radius"]
    candidates = []
    for p in parcels:
        plat = p.get("lat") or p.get("latitude")
        plng = p.get("lng") or p.get("longitude")
        if plat is None or plng is None:
            continue
        try:
            d = haversine(lat0, lng0, float(plat), float(plng))
        except Exception:
            continue
        if d <= r:
            val = float(p.get("valuation") or p.get("val") or 0)
            yr = p.get("year_built") or 0
            candidates.append((d, val, -yr, p))
    candidates.sort(key=lambda x: (x[0], -x[1], x[2]))
    return [c[-1] for c in candidates[:max_candidates]]


# Main ingestion -------------------------------------------------------------

async def ingest_yelp_intents(parcels: List[dict], get_owner_fn, enrich_contact_fn, cities: List[str], max_results: int = 30) -> int:
    """
    parcels: list of cached parcel leads (with lat/lng, apn, address, city, valuation)
    get_owner_fn: async function(county, apn, address) -> dict
    enrich_contact_fn: async function(street, city, state, zip) -> dict
    """
    init_db()
    if not SERP_API_KEY:
        return 0
    queries = build_queries(cities)[:15]
    found = 0
    async with aiohttp.ClientSession() as session:
        for q in queries:
            results = await serp_search(session, q, num=5)
            for item in results:
                reviewer_name, snippet, url = parse_snippet(item)
                if not snippet:
                    continue
                clf = classify_intent(snippet)
                if clf["confidence"] < 0.55:
                    continue
                geo = resolve_geocircle(snippet, q)
                apn_candidates = match_parcels(geo, parcels, max_candidates=5) if geo else []
                selected_apn = apn_candidates[0].get("apn") if apn_candidates else None
                owner_name = None
                phones: List[str] = []
                emails: List[str] = []
                if selected_apn:
                    # Attempt owner lookup
                    county = apn_candidates[0].get("city") or "Los Angeles"
                    owner_data = await get_owner_fn(county, selected_apn, apn_candidates[0].get("address", ""))
                    owner_name = owner_data.get("owner_name") if owner_data else None
                    # Enrich contacts using existing PropertyReach helper
                    addr = apn_candidates[0].get("address") or ""
                    city = apn_candidates[0].get("city") or ""
                    zip_code = apn_candidates[0].get("zip") or ""
                    contact = await enrich_contact_fn(addr, city, "CA", zip_code)
                    if contact.get("phones"):
                        phones = contact["phones"]
                    if contact.get("emails"):
                        emails = contact["emails"]

                score = (
                    clf["confidence"] * 40
                    + (10 if clf["budget_signal"] == "high" else 4 if clf["budget_signal"] == "medium" else 1)
                    + (5 if clf["mentions_permits"] else 0)
                    + (5 if clf["mentions_architect"] else 0)
                )
                if score < 25:
                    continue

                rhash = reviewer_hash(reviewer_name, snippet)
                lead = YelpIntentLead(
                    reviewer_name=reviewer_name or "Yelp User",
                    review_text=snippet,
                    project_type=clf["project_type"],
                    intent_stage=clf["stage"],
                    confidence=clf["confidence"],
                    budget_signal=clf["budget_signal"],
                    mentions_permits=clf["mentions_permits"],
                    mentions_architect=clf["mentions_architect"],
                    geo_circle=geo or {},
                    apn_candidates=[c.get("apn") for c in apn_candidates],
                    selected_apn=selected_apn,
                    owner_name=owner_name,
                    phones=phones,
                    emails=emails,
                    lead_score=score,
                    created_at=time.time(),
                    reviewer_hash=rhash,
                    source_url=url,
                    city_hint=q,
                )
                save_lead(lead)
                found += 1
                if found >= max_results:
                    return found
    return found
