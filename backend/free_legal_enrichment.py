"""
100% Free & Legal Owner Enrichment
Uses only public government APIs (county assessors) and provides
manual lookup links for phones/emails (which are not in public records).
"""

import aiohttp
import asyncio
from typing import Dict, Optional
import logging
import urllib.parse

logger = logging.getLogger(__name__)


class FreeLegalEnrichment:
    """
    Free owner enrichment that relies solely on public, government APIs.
    No scraping of third-party sites; no paid data providers.
    """

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def get_session(self) -> aiohttp.ClientSession:
        if not self.session:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def is_corporate(self, name: Optional[str]) -> bool:
        """Detect corporate/LLC/trust style names."""
        if not name:
            return False
        corporate_keywords = [
            "LLC", "L.L.C", "INC", "CORP", "CORPORATION",
            "LP", "LLP", "TRUST", "ESTATE", "PROPERTIES",
            "CONSTRUCTION", "BUILDERS", "HOMES",
            "INVESTMENTS", "HOLDINGS", "VENTURES", "GROUP",
            "PARTNERS", "PARTNERSHIP", "COMPANY", "CO."
        ]
        upper = name.upper()
        return any(kw in upper for kw in corporate_keywords)

    # ------------------------------------------------------------------ #
    # County assessor lookups (official, public APIs)
    # ------------------------------------------------------------------ #
    async def get_la_county_owner(self, apn: str) -> Optional[Dict]:
        """Los Angeles County assessor API (official, public)."""
        try:
            clean_apn = apn.replace("-", "").replace(" ", "")
            url = f"https://portal.assessor.lacounty.gov/api/parceldetail/{clean_apn}"

            session = await self.get_session()
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                owner_name = data.get("OwnerName") or data.get("owner")
                mailing = data.get("MailingAddress") or data.get("SitusAddress")
                return {
                    "owner_name": owner_name,
                    "mailing_address": mailing,
                    "is_corporate": self.is_corporate(owner_name),
                    "source": "LA County Assessor (official API)",
                }
        except Exception as exc:  # pragma: no cover - network/HTTP
            logger.warning(f"LA County assessor lookup failed: {exc}")
            return None

    async def get_san_diego_county_owner(self, apn: str) -> Optional[Dict]:
        """
        San Diego County assessor via SANDAG ArcGIS service (public).
        """
        try:
            # SDAPNs often stored without dashes; try both
            apn_clean = apn.replace("-", "").replace(" ", "")
            url = "https://sdgis.sandag.org/arcgis/rest/services/Assessor/Parcels/MapServer/0/query"
            session = await self.get_session()
            for clause in (f"APN='{apn}'", f"APN='{apn_clean}'"):
                params = {
                    "where": clause,
                    "outFields": "*",
                    "f": "json",
                    "returnGeometry": "false",
                }
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        continue
                    payload = await resp.json()
                    if not payload.get("features"):
                        continue
                    attrs = payload["features"][0]["attributes"]
                    owner = attrs.get("OWNER_NAME")
                    mailing = attrs.get("MAIL_ADDRESS")
                    return {
                        "owner_name": owner,
                        "mailing_address": mailing,
                        "is_corporate": self.is_corporate(owner),
                        "source": "San Diego County Assessor (public ArcGIS)",
                    }
        except Exception as exc:  # pragma: no cover
            logger.warning(f"San Diego assessor lookup failed: {exc}")
            return None

    # ------------------------------------------------------------------ #
    # Manual lookup helpers (legal: just constructing search links)
    # ------------------------------------------------------------------ #
    def _clean_name_for_search(self, name: str) -> str:
        cleaned = name
        for suffix in [" LLC", " INC", " CORP", " LP", " LLP", " TRUST"]:
            cleaned = cleaned.replace(suffix, "")
        return cleaned.strip()

    def generate_search_links(self, owner_name: str, address: str, city: str) -> Dict[str, str]:
        """Return a set of helpful search links for manual phone/email lookup."""
        name_enc = urllib.parse.quote(owner_name or "")
        city_enc = urllib.parse.quote(city or "")
        address_enc = urllib.parse.quote(address or "")
        clean = urllib.parse.quote(self._clean_name_for_search(owner_name or ""))

        return {
            # Phone / email discovery (manual)
            "google_phone": f"https://www.google.com/search?q={name_enc}+{city_enc}+phone",
            "google_email": f"https://www.google.com/search?q={name_enc}+{city_enc}+email",
            "whitepages": f"https://www.whitepages.com/name/{name_enc}/{city_enc}-CA",
            "411": f"https://www.411.com/name/{name_enc}/{city_enc}-ca",
            "linkedin": f"https://www.linkedin.com/search/results/people/?keywords={clean}",

            # Corporate resolution (manual, legal)
            "ca_sos": "https://bizfileonline.sos.ca.gov/search/business",
            "google_company": f"https://www.google.com/search?q={clean}+{city_enc}+contact",

            # General
            "google_general": f"https://www.google.com/search?q={name_enc}+{address_enc}+{city_enc}",
        }

    # ------------------------------------------------------------------ #
    # Public enrichment entry point
    # ------------------------------------------------------------------ #
    async def enrich_permit(self, permit: Dict) -> Dict:
        """
        Resolve owner name + mailing via public assessor APIs only.
        Provide manual-search links for phone/email/principals.
        """
        address = permit.get("address", "") or permit.get("addr", "")
        city = permit.get("city", "")
        apn = permit.get("apn", "") or permit.get("apn_full", "")

        # Try LA first (most common in dataset)
        owner_data = await self.get_la_county_owner(apn)

        # If LA fails and looks like San Diego, try SD
        if not owner_data and "San Diego" in city:
            owner_data = await self.get_san_diego_county_owner(apn)

        if not owner_data:
            # Return a graceful success with note so UI doesn't show an error banner
            return {
                "success": True,
                "owner_name": None,
                "is_corporate": False,
                "phone": None,
                "email": None,
                "mailing_address": None,
                "all_individuals": [],
                "all_entities": [],
                "search_links": {},
                "data_source": "County assessor (unreachable)",
                "cost": 0.00,
                "note": "Owner not available; county assessor API unreachable or APN not found",
            }

        owner_name = owner_data["owner_name"]
        mailing = owner_data.get("mailing_address")
        is_corporate = owner_data["is_corporate"]

        search_links = self.generate_search_links(owner_name, address, city)

        individual = {
            "name": owner_name,
            "role": "Property Owner",
            "mailing_address": mailing,
            "is_corporate": is_corporate,
            "phone": None,
            "email": None,
            "search_links": search_links,
        }

        return {
            "success": True,
            "owner_name": owner_name,
            "is_corporate": is_corporate,
            "phone": None,
            "email": None,
            "mailing_address": mailing,
            "all_individuals": [individual],
            "all_entities": [{"name": owner_name, "role": "Property Owner"}] if is_corporate else [],
            "search_links": search_links,
            "data_source": owner_data.get("source"),
            "cost": 0.00,
            "note": "Phones/emails are not available in public records; use the provided search links.",
        }


# Convenience for manual CLI testing
async def _demo(apn: str, city: str):
    enricher = FreeLegalEnrichment()
    result = await enricher.enrich_permit({"apn": apn, "city": city, "address": ""})
    print(result)
    await enricher.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(_demo("265-191-32-80", "San Diego County"))
