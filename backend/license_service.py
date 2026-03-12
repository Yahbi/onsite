import datetime as dt
import requests
from bs4 import BeautifulSoup
from typing import Dict, Optional

from marketplace_engine import _conn


def store_license_snapshot(user_id: str, license_number: str, status: str, classification: str, expiration: Optional[str]):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO contractor_license (user_id, license_number, status, classification, expiration, last_verified_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            license_number=excluded.license_number,
            status=excluded.status,
            classification=excluded.classification,
            expiration=excluded.expiration,
            last_verified_at=excluded.last_verified_at
        """,
        (user_id, license_number, status, classification, expiration, dt.datetime.utcnow()),
    )
    conn.commit()
    conn.close()


def parse_cslb_html(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    status = "unknown"
    classification = ""
    expiration = None
    for row in soup.find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cells) >= 2:
            if "status" in cells[0].lower():
                status = cells[1]
            if "classification" in cells[0].lower():
                classification = cells[1]
            if "expires" in cells[0].lower():
                expiration = cells[1]
    return {"status": status, "classification": classification, "expiration": expiration}


def verify_license(license_number: str, state: str = "CA") -> Dict:
    if not license_number:
        return {"status": "unknown"}
    if state.upper() != "CA":
        return {"status": "unknown"}
    try:
        url = f"https://www2.cslb.ca.gov/OnlineServices/CheckLicenseII/LicenseDetail.aspx?LicNum={license_number}"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return {"status": "unknown"}
        return parse_cslb_html(r.text)
    except Exception:
        return {"status": "unknown"}


def login_license_check(user_id: str, license_number: str, state: str = "CA"):
    snap = verify_license(license_number, state)
    store_license_snapshot(user_id, license_number, snap.get("status", "unknown"), snap.get("classification", ""), snap.get("expiration"))
    return snap
