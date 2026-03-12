"""
Prompt scheduling and throttling for passive confirmations.
"""
import datetime as dt
from typing import List, Dict, Optional
from marketplace_engine import _conn

PROMPT_COOLDOWN_MIN = 30
PROPERTY_COOLDOWN_DAYS = 7


def _now():
    return dt.datetime.utcnow()


def record_prompt(contractor_id: str, property_id: int, session_id: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO prompt_throttle (contractor_id, property_id, last_prompt_at, session_id)"
        " VALUES (?, ?, ?, ?) ON CONFLICT(contractor_id, property_id) DO UPDATE SET last_prompt_at=excluded.last_prompt_at, session_id=excluded.session_id",
        (contractor_id, property_id, _now(), session_id),
    )
    conn.commit()
    conn.close()


def _recent_prompt(conn, contractor_id: str, property_id: int, session_id: Optional[str]):
    cur = conn.cursor()
    cur.execute(
        "SELECT last_prompt_at, session_id FROM prompt_throttle WHERE contractor_id=? AND property_id=?",
        (contractor_id, property_id),
    )
    row = cur.fetchone()
    if not row:
        return False
    last, sid = row
    if isinstance(last, str):
        last_dt = dt.datetime.fromisoformat(last)
    else:
        last_dt = last
    if session_id and sid and sid == session_id:
        if (_now() - last_dt).total_seconds() < PROMPT_COOLDOWN_MIN * 60:
            return True
    if (_now() - last_dt).days < PROPERTY_COOLDOWN_DAYS:
        return True
    return False


def suggest_prompts(contractor_id: str, session_id: str, context: List[Dict]) -> List[Dict]:
    """
    context: [{property_id, readiness_score, stage_index, view_count_48h, readiness_drop,
               last_contact_ts, last_alert_seen_ts, idle, prev_stage?, view_cluster_hours?}]
    Returns up to one prompt honoring throttling.
    """
    if not context:
        return []
    conn = _conn()
    prompts = []
    for item in context:
        pid = item.get("property_id")
        if not pid:
            continue
        if item.get("idle") is False:
            continue
        if _recent_prompt(conn, contractor_id, pid, session_id):
            continue

        rs = item.get("readiness_score") or 0
        stage = item.get("stage_index") or 0
        views = item.get("view_count_48h") or 0
        drop = item.get("readiness_drop") or 0
        last_contact = item.get("last_contact_ts")
        last_alert_seen = item.get("last_alert_seen_ts")
        prev_stage = item.get("prev_stage") or stage
        now = _now()

        # Repeat views prompt
        if views >= 3:
            prompts.append({
                "property_id": pid,
                "type": "reach_out",
                "text": "Have you reached out to this homeowner?",
                "buttons": ["Yes", "Not yet"],
                "source": "repeat_view_prompt",
            })
            record_prompt(contractor_id, pid, session_id)
            break

        # Readiness drop / stage advance prompt
        if drop >= 15 or stage > prev_stage:
            prompts.append({
                "property_id": pid,
                "type": "outcome",
                "text": "Did this project move forward?",
                "buttons": ["Won it", "Lost it", "Not sure"],
                "source": "readiness_drop_prompt",
            })
            record_prompt(contractor_id, pid, session_id)
            break

        # Post-contact delay prompt
        if last_contact:
            try:
                delta = (now - dt.datetime.fromisoformat(last_contact)).days
            except Exception:
                delta = 0
            if delta >= 7:
                prompts.append({
                    "property_id": pid,
                    "type": "estimate_followup",
                    "text": "Did you send an estimate?",
                    "buttons": ["Sent", "Still discussing", "They stopped replying"],
                    "source": "post_contact_delay_prompt",
                })
                record_prompt(contractor_id, pid, session_id)
                break

        # Alert follow-up prompt
        if last_alert_seen:
            try:
                delta_hours = (now - dt.datetime.fromisoformat(last_alert_seen)).total_seconds() / 3600
            except Exception:
                delta_hours = 0
            if delta_hours >= 48:
                prompts.append({
                    "property_id": pid,
                    "type": "alert_followup",
                    "text": "Did you act on this opportunity?",
                    "buttons": ["Yes", "Later", "Ignored"],
                    "source": "alert_followup_prompt",
                })
                record_prompt(contractor_id, pid, session_id)
                break

        # Meeting detection prompt
        if views >= 2 and rs > 55 and item.get("view_cluster_hours", 999) < 2:
            prompts.append({
                "property_id": pid,
                "type": "meeting",
                "text": "Did you meet the homeowner?",
                "buttons": ["Yes", "Scheduled", "No"],
                "source": "meeting_detection_prompt",
            })
            record_prompt(contractor_id, pid, session_id)
            break

    conn.close()
    return prompts

