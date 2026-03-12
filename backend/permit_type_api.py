import json
from typing import List, Dict
from marketplace_engine import _conn


def list_permit_types(cache: List[Dict], limit: int = 100):
    counts = {}
    for l in cache or []:
        pt = (l.get("permit_type") or "unknown").strip() or "unknown"
        counts[pt] = counts.get(pt, 0) + 1
    items = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    return [{"permit_type": k, "count": v} for k, v in items]
