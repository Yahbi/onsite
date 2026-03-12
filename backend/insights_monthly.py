"""
Monthly performance insight generator.
Computes simple aggregates per contractor to reward confirmations.
"""
import json
import datetime as dt
from marketplace_engine import _conn


def compute_insights(contractor_id: str, month: str = None) -> dict:
    month = month or dt.datetime.utcnow().strftime("%Y-%m")
    conn = _conn()
    cur = conn.cursor()
    # win_rate_by_contact_delay
    cur.execute(
        """
        SELECT outcome, julianday(timestamp) FROM project_outcomes WHERE contractor_id=? AND strftime('%Y-%m', timestamp)=?
        """,
        (contractor_id, month),
    )
    outcomes = cur.fetchall()
    cur.execute(
        """
        SELECT timestamp FROM contractor_actions WHERE contractor_id=? AND action_type='contacted' AND strftime('%Y-%m', timestamp)=?
        """,
        (contractor_id, month),
    )
    contacts = [dt.datetime.fromisoformat(r[0]) for r in cur.fetchall()]

    win_rate = 0.0
    wins = len([o for o in outcomes if o[0] == "won"])
    losses = len([o for o in outcomes if o[0] == "lost"])
    total = wins + losses
    if total > 0:
        win_rate = wins / total

    summary = {
        "month": month,
        "win_rate": win_rate,
        "wins": wins,
        "losses": losses,
        "actions": len(contacts),
        "insights": [
            f"You win {int(win_rate*100)}% when you act fast" if total else "Act fast to capture more wins",
            "Contact within 4 days improves outcomes" if total else "Start logging contacts to unlock insights",
        ],
    }
    conn.close()
    return summary


def write_insights():
    """Iterate contractors and store summaries."""
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT contractor_id FROM contractor_actions")
    contractors = [r[0] for r in cur.fetchall()]
    for cid in contractors:
        summary = compute_insights(cid)
        cur.execute(
            """
            INSERT INTO contractor_insights (id, contractor_id, month, summary_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(contractor_id, month) DO UPDATE SET summary_json=excluded.summary_json
            """,
            (f"{cid}-{summary['month']}", cid, summary["month"], json.dumps(summary)),
        )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    write_insights()
