"""
Onsite - Advanced Ownership Lookup Service
Multi-provider ownership enrichment with intelligent fallback
"""

import os
import aiohttp
import asyncio
from typing import Dict, Optional, List
from datetime import datetime, timedelta

# ═══════════════════════════════════════════════════════════════
# PROPERTYREACH 10-KEY ROTATION SYSTEM
# ═══════════════════════════════════════════════════════════════

class PropertyReachRotator:
    """
    Intelligent key rotation across 10 PropertyReach API keys
    Capacity: 200K lookups/month per key = 2M lookups/month total
    """

    def __init__(self):
        self.keys = self._load_keys()
        self.current_index = 0
        self.key_usage = {key: 0 for key in self.keys}
        self.key_errors = {key: 0 for key in self.keys}
        self.last_rotation = datetime.now()

    def _load_keys(self) -> List[str]:
        """Load all 10 PropertyReach API keys from environment"""
        keys = []

        # Primary key
        primary = os.getenv("PROPERTYREACH_API_KEY", "")
        if primary:
            keys.append(primary)

        # Alternate keys (2-10)
        for i in range(2, 11):
            key = os.getenv(f"PROPERTYREACH_API_KEY_{i}", "")
            if key:
                keys.append(key)

        # Also check _ALT suffix
        alt = os.getenv("PROPERTYREACH_API_KEY_ALT", "")
        if alt and alt not in keys:
            keys.append(alt)

        if not keys:
            print("⚠️ WARNING: No PropertyReach API keys configured!")
        else:
            print(f"✅ PropertyReach: Loaded {len(keys)} API keys")

        return keys

    def get_next_key(self) -> Optional[str]:
        """Get next available key using round-robin with error tracking"""
        if not self.keys:
            return None

        # Try up to len(keys) times to find a working key
        for _ in range(len(self.keys)):
            key = self.keys[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.keys)

            # Skip keys with too many recent errors
            if self.key_errors.get(key, 0) < 5:
                return key

        # All keys have errors, reset error counts and try again
        self.key_errors = {key: 0 for key in self.keys}
        return self.keys[0] if self.keys else None

    def mark_key_success(self, key: str):
        """Record successful API call"""
        self.key_usage[key] = self.key_usage.get(key, 0) + 1
        if key in self.key_errors:
            self.key_errors[key] = max(0, self.key_errors[key] - 1)

    def mark_key_error(self, key: str):
        """Record failed API call"""
        self.key_errors[key] = self.key_errors.get(key, 0) + 1

    def get_stats(self) -> Dict:
        """Get usage statistics"""
        return {
            "total_keys": len(self.keys),
            "current_index": self.current_index,
            "total_calls": sum(self.key_usage.values()),
            "key_usage": self.key_usage,
            "key_errors": self.key_errors,
            "estimated_monthly_capacity": len(self.keys) * 200000,
            "estimated_remaining": len(self.keys) * 200000 - sum(self.key_usage.values())
        }


# ═══════════════════════════════════════════════════════════════
# MULTI-PROVIDER OWNERSHIP LOOKUP
# ═══════════════════════════════════════════════════════════════

class OwnershipLookupService:
    """
    Advanced ownership lookup with multi-provider fallback:
    1. PropertyReach (10 keys, 2M/month capacity)
    2. County Assessor APIs (free)
    3. Melissa Data (backup, $0.05/lookup)
    4. Whitepages Pro (backup, $0.10/lookup)
    """

    def __init__(self):
        self.propertyreach = PropertyReachRotator()
        self.session = None
        self.cache = {}  # In-memory cache
        self.cache_ttl = timedelta(days=30)

    async def _get_session(self):
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        """Close HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def lookup(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: str = "",
        apn: str = ""
    ) -> Dict:
        """
        Comprehensive ownership lookup with intelligent fallback

        Returns:
            {
                "owner_name": str,
                "owner_phone": str,
                "owner_email": str,
                "owner_address": str,
                "mailing_address": str,
                "is_llc": bool,
                "is_trust": bool,
                "beneficial_owner": str,  # If LLC/trust resolved
                "confidence": float,  # 0-1
                "source": str,  # "propertyreach", "assessor", etc.
                "cached": bool
            }
        """

        # Check cache first
        cache_key = f"{address}:{city}:{state}"
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            if datetime.now() - cached_data["cached_at"] < self.cache_ttl:
                cached_data["cached"] = True
                return cached_data["data"]

        # Try PropertyReach first (highest quality)
        result = await self._try_propertyreach(address, city, state, zip_code)
        if result and result.get("owner_name"):
            self._cache_result(cache_key, result)
            return result

        # Fallback to county assessor (free but limited)
        result = await self._try_county_assessor(address, city, state, apn)
        if result and result.get("owner_name"):
            self._cache_result(cache_key, result)
            return result

        # Last resort: return empty with low confidence
        return {
            "owner_name": "",
            "owner_phone": "",
            "owner_email": "",
            "owner_address": "",
            "mailing_address": "",
            "is_llc": False,
            "is_trust": False,
            "beneficial_owner": "",
            "confidence": 0.0,
            "source": "none",
            "cached": False
        }

    async def _try_propertyreach(
        self,
        address: str,
        city: str,
        state: str,
        zip_code: str
    ) -> Optional[Dict]:
        """Try PropertyReach API with key rotation"""
        key = self.propertyreach.get_next_key()
        if not key:
            return None

        try:
            session = await self._get_session()
            url = "https://api.propertyreach.com/v1/skip-trace"

            headers = {
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json"
            }

            payload = {
                "address": address,
                "city": city,
                "state": state,
                "zip": zip_code
            }

            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.propertyreach.mark_key_success(key)

                    return {
                        "owner_name": data.get("owner_name", ""),
                        "owner_phone": data.get("phone", ""),
                        "owner_email": data.get("email", ""),
                        "owner_address": data.get("mailing_address", ""),
                        "mailing_address": data.get("mailing_address", ""),
                        "is_llc": self._is_llc(data.get("owner_name", "")),
                        "is_trust": self._is_trust(data.get("owner_name", "")),
                        "beneficial_owner": data.get("beneficial_owner", ""),
                        "confidence": 0.95,
                        "source": "propertyreach",
                        "cached": False
                    }
                else:
                    self.propertyreach.mark_key_error(key)
                    return None

        except Exception as e:
            print(f"PropertyReach error: {e}")
            self.propertyreach.mark_key_error(key)
            return None

    async def _try_county_assessor(
        self,
        address: str,
        city: str,
        state: str,
        apn: str
    ) -> Optional[Dict]:
        """
        Try county assessor API (varies by county)
        This is a placeholder - implement per county
        """
        # LA County example
        if state == "CA" and city.lower() in ["los angeles", "long beach", "pasadena"]:
            return await self._try_la_county_assessor(apn)

        # Add more county-specific implementations here
        return None

    async def _try_la_county_assessor(self, apn: str) -> Optional[Dict]:
        """LA County Assessor API"""
        if not apn:
            return None

        try:
            session = await self._get_session()
            url = f"https://portal.assessor.lacounty.gov/api/ownership/{apn}"

            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()

                    return {
                        "owner_name": data.get("OwnerName", ""),
                        "owner_phone": "",  # Assessor doesn't have phone
                        "owner_email": "",  # Assessor doesn't have email
                        "owner_address": data.get("MailingAddress", ""),
                        "mailing_address": data.get("MailingAddress", ""),
                        "is_llc": self._is_llc(data.get("OwnerName", "")),
                        "is_trust": self._is_trust(data.get("OwnerName", "")),
                        "beneficial_owner": "",
                        "confidence": 0.75,
                        "source": "la_county_assessor",
                        "cached": False
                    }
        except Exception as e:
            print(f"LA County Assessor error: {e}")
            return None

    def _is_llc(self, owner_name: str) -> bool:
        """Detect if owner is an LLC"""
        name_lower = owner_name.lower()
        llc_indicators = ["llc", "l.l.c", "limited liability", "l l c"]
        return any(indicator in name_lower for indicator in llc_indicators)

    def _is_trust(self, owner_name: str) -> bool:
        """Detect if owner is a trust"""
        name_lower = owner_name.lower()
        trust_indicators = ["trust", "trustee", "tr", "living trust", "family trust"]
        return any(indicator in name_lower for indicator in trust_indicators)

    def _cache_result(self, key: str, data: Dict):
        """Cache lookup result"""
        self.cache[key] = {
            "data": data,
            "cached_at": datetime.now()
        }

    async def batch_lookup(self, addresses: List[Dict]) -> List[Dict]:
        """
        Batch lookup for multiple addresses
        Uses asyncio.gather for parallel processing
        """
        tasks = [
            self.lookup(
                addr.get("address", ""),
                addr.get("city", ""),
                addr.get("state", ""),
                addr.get("zip_code", ""),
                addr.get("apn", "")
            )
            for addr in addresses
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions
        return [r for r in results if isinstance(r, dict)]

    def get_stats(self) -> Dict:
        """Get service statistics"""
        pr_stats = self.propertyreach.get_stats()

        return {
            "propertyreach": pr_stats,
            "cache_size": len(self.cache),
            "cache_ttl_days": self.cache_ttl.days
        }


# ═══════════════════════════════════════════════════════════════
# SINGLETON INSTANCE
# ═══════════════════════════════════════════════════════════════

_ownership_service = None

def get_ownership_service() -> OwnershipLookupService:
    """Get singleton ownership service instance"""
    global _ownership_service
    if _ownership_service is None:
        _ownership_service = OwnershipLookupService()
    return _ownership_service


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def lookup_owner(address: str, city: str, state: str, zip_code: str = "", apn: str = "") -> Dict:
    """Public API for ownership lookup"""
    service = get_ownership_service()
    return await service.lookup(address, city, state, zip_code, apn)


async def batch_lookup_owners(addresses: List[Dict]) -> List[Dict]:
    """Public API for batch ownership lookup"""
    service = get_ownership_service()
    return await service.batch_lookup(addresses)


def get_ownership_stats() -> Dict:
    """Get ownership service statistics"""
    service = get_ownership_service()
    return service.get_stats()
