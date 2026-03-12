"""
Onsite - Contact Enrichment Service
Phone validation, email discovery, and contact enrichment
Supports: Twilio, Hunter.io, IPQualityScore
"""

import asyncio
import aiohttp
import os
import re
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class PhoneInfo:
    """Phone number validation result"""
    number: str
    valid: bool
    carrier: str = "Unknown"
    line_type: str = "Unknown"  # mobile, landline, voip
    location: str = ""
    spam_risk: Optional[str] = None
    caller_name: Optional[str] = None
    confidence: float = 0.0


@dataclass
class EmailInfo:
    """Email validation result"""
    email: str
    valid: str = "unknown"  # valid, invalid, unknown
    score: int = 0  # 0-100
    sources: List[str] = None


@dataclass
class EnrichedContact:
    """Complete enriched contact information"""
    name: str
    address: str
    city: str
    state: str
    zip_code: str
    phones: List[Dict] = None
    emails: List[str] = None
    properties: List[Dict] = None
    relatives: List[str] = None
    age: Optional[int] = None
    data_sources: List[str] = None
    enriched_at: str = ""
    confidence_score: float = 0.0


class PhoneEnricher:
    """
    Phone number validation and enrichment.
    Supports: Twilio Lookup, IPQualityScore
    """

    def __init__(self):
        self.twilio_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        self.twilio_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        self.ipqs_key = os.getenv("IPQS_API_KEY", "")
        self.session = None

        # Stats
        self.stats = {
            "twilio": 0,
            "ipqs": 0,
            "cache": 0,
            "failed": 0
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    def normalize_phone(self, phone: str) -> str:
        """Normalize phone number to E.164 format (+1XXXXXXXXXX)"""
        if not phone:
            return ""

        # Remove all non-digits
        digits = re.sub(r'\D', '', phone)

        # Handle US/Canada numbers
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+{digits}"

        return digits

    async def validate_twilio(self, phone: str) -> PhoneInfo:
        """
        Validate phone via Twilio Lookup API
        Cost: $0.005 per lookup
        Free tier: $15 credit (3,000 lookups)
        """
        if not self.twilio_sid or not self.twilio_token:
            logger.warning("Twilio credentials not configured")
            return PhoneInfo(number=phone, valid=False)

        normalized = self.normalize_phone(phone)
        if not normalized:
            return PhoneInfo(number=phone, valid=False)

        url = f"https://lookups.twilio.com/v1/PhoneNumbers/{normalized}"
        params = {"Type": ["carrier", "caller-name"]}

        try:
            auth = aiohttp.BasicAuth(self.twilio_sid, self.twilio_token)
            async with self.session.get(url, params=params, auth=auth, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    carrier = data.get('carrier', {})
                    caller = data.get('caller_name', {})

                    self.stats["twilio"] += 1
                    return PhoneInfo(
                        number=phone,
                        valid=True,
                        carrier=carrier.get('name', 'Unknown'),
                        line_type=carrier.get('type', 'Unknown'),
                        location=f"{carrier.get('mobile_country_code', '')} {carrier.get('mobile_network_code', '')}".strip(),
                        caller_name=caller.get('caller_name'),
                        confidence=0.9
                    )
                else:
                    logger.debug(f"Twilio validation failed for {phone}: {resp.status}")
                    self.stats["failed"] += 1
                    return PhoneInfo(number=phone, valid=False)
        except Exception as e:
            logger.error(f"Twilio validation error for {phone}: {e}")
            self.stats["failed"] += 1
            return PhoneInfo(number=phone, valid=False)

    async def validate_ipqs(self, phone: str) -> PhoneInfo:
        """
        Validate phone via IPQualityScore
        Free tier: 5,000 requests/month
        """
        if not self.ipqs_key:
            logger.warning("IPQualityScore API key not configured")
            return PhoneInfo(number=phone, valid=False)

        normalized = self.normalize_phone(phone)
        if not normalized:
            return PhoneInfo(number=phone, valid=False)

        url = f"https://ipqualityscore.com/api/json/phone/{self.ipqs_key}/{normalized}"

        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()

                    if not data.get('success', False):
                        return PhoneInfo(number=phone, valid=False)

                    self.stats["ipqs"] += 1
                    return PhoneInfo(
                        number=phone,
                        valid=data.get('valid', False),
                        carrier=data.get('carrier', 'Unknown'),
                        line_type=data.get('line_type', 'Unknown'),
                        location=data.get('city', '') + ', ' + data.get('region', ''),
                        spam_risk="high" if data.get('fraud_score', 0) > 75 else "low",
                        confidence=1 - (data.get('fraud_score', 0) / 100)
                    )
                else:
                    self.stats["failed"] += 1
                    return PhoneInfo(number=phone, valid=False)
        except Exception as e:
            logger.error(f"IPQS validation error for {phone}: {e}")
            self.stats["failed"] += 1
            return PhoneInfo(number=phone, valid=False)

    async def validate(self, phone: str, method: str = "twilio") -> PhoneInfo:
        """Validate phone number with specified method"""
        if method == "twilio":
            return await self.validate_twilio(phone)
        elif method == "ipqs":
            return await self.validate_ipqs(phone)
        else:
            # Try Twilio first, fallback to IPQS
            result = await self.validate_twilio(phone)
            if not result.valid and self.ipqs_key:
                result = await self.validate_ipqs(phone)
            return result

    async def bulk_validate(self, phones: List[str], method: str = "twilio") -> List[PhoneInfo]:
        """Validate multiple phone numbers"""
        tasks = [self.validate(phone, method) for phone in phones]
        return await asyncio.gather(*tasks, return_exceptions=True)

    def get_stats(self) -> Dict:
        """Get validation statistics"""
        return self.stats.copy()


class EmailEnricher:
    """
    Email discovery and validation.
    Supports: Hunter.io
    """

    # Common email patterns for pattern generation
    PATTERNS = [
        "{first}@{domain}",
        "{first}.{last}@{domain}",
        "{first}{last}@{domain}",
        "{f}{last}@{domain}",
        "{first}_{last}@{domain}",
        "{first}{l}@{domain}",
        "{last}.{first}@{domain}",
        "{first}-{last}@{domain}",
        "{last}@{domain}",
    ]

    def __init__(self):
        self.hunter_key = os.getenv("HUNTER_API_KEY", "")
        self.session = None

        # Stats
        self.stats = {
            "hunter_verify": 0,
            "hunter_search": 0,
            "patterns": 0,
            "failed": 0
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    def generate_patterns(self, first: str, last: str, domain: str) -> List[str]:
        """
        Generate possible email patterns
        Example: generate_patterns("John", "Doe", "company.com")
        Returns: ["john@company.com", "john.doe@company.com", ...]
        """
        if not first or not last or not domain:
            return []

        emails = []
        first_clean = first.lower().replace(' ', '').replace('-', '')
        last_clean = last.lower().replace(' ', '').replace('-', '')
        f = first_clean[0] if first_clean else ""
        l = last_clean[0] if last_clean else ""
        domain_clean = domain.lower().strip()

        for pattern in self.PATTERNS:
            try:
                email = pattern.format(
                    first=first_clean,
                    last=last_clean,
                    f=f,
                    l=l,
                    domain=domain_clean
                )
                if email not in emails and '@' in email:
                    emails.append(email)
            except KeyError:
                continue

        self.stats["patterns"] += len(emails)
        return emails

    async def hunter_domain_search(self, domain: str, limit: int = 10) -> Dict:
        """
        Search Hunter.io for emails from a domain
        Free tier: 25 searches/month, 50 verifications/month
        """
        if not self.hunter_key:
            logger.warning("Hunter.io API key not configured")
            return {}

        url = "https://api.hunter.io/v2/domain-search"
        params = {
            "domain": domain,
            "limit": limit,
            "api_key": self.hunter_key
        }

        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result_data = data.get("data", {})

                    self.stats["hunter_search"] += 1
                    return {
                        "domain": domain,
                        "pattern": result_data.get("pattern"),
                        "organization": result_data.get("organization"),
                        "emails": [
                            {
                                "value": e.get("value"),
                                "type": e.get("type"),
                                "confidence": e.get("confidence"),
                                "first_name": e.get("first_name"),
                                "last_name": e.get("last_name"),
                                "position": e.get("position")
                            }
                            for e in result_data.get("emails", [])
                        ],
                        "pattern_detected": result_data.get("pattern") is not None
                    }
                elif resp.status == 429:
                    logger.warning("Hunter.io rate limit reached")
                    return {}
                else:
                    logger.debug(f"Hunter domain search failed for {domain}: {resp.status}")
                    self.stats["failed"] += 1
                    return {}
        except Exception as e:
            logger.error(f"Hunter domain search error for {domain}: {e}")
            self.stats["failed"] += 1
            return {}

    async def hunter_verify(self, email: str) -> EmailInfo:
        """
        Verify email via Hunter.io
        Free tier: 50 verifications/month
        """
        if not self.hunter_key:
            logger.warning("Hunter.io API key not configured")
            return EmailInfo(email=email, valid="unknown")

        url = "https://api.hunter.io/v2/email-verifier"
        params = {
            "email": email,
            "api_key": self.hunter_key
        }

        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result_data = data.get("data", {})

                    self.stats["hunter_verify"] += 1
                    return EmailInfo(
                        email=email,
                        valid=result_data.get("status", "unknown"),  # valid, invalid, accept_all, unknown
                        score=result_data.get("score", 0),
                        sources=[s.get("domain") for s in result_data.get("sources", [])]
                    )
                elif resp.status == 429:
                    logger.warning("Hunter.io rate limit reached")
                    return EmailInfo(email=email, valid="unknown")
                else:
                    self.stats["failed"] += 1
                    return EmailInfo(email=email, valid="unknown")
        except Exception as e:
            logger.error(f"Hunter email verification error for {email}: {e}")
            self.stats["failed"] += 1
            return EmailInfo(email=email, valid="unknown")

    async def bulk_verify(self, emails: List[str]) -> List[EmailInfo]:
        """Verify multiple emails"""
        tasks = [self.hunter_verify(email) for email in emails]
        return await asyncio.gather(*tasks, return_exceptions=True)

    def get_stats(self) -> Dict:
        """Get email enrichment statistics"""
        return self.stats.copy()


class ContactEnricher:
    """
    Complete contact enrichment orchestrator.
    Combines phone validation, email discovery, and contact data.
    """

    def __init__(self):
        self.phone_enricher = PhoneEnricher()
        self.email_enricher = EmailEnricher()

    async def __aenter__(self):
        await self.phone_enricher.__aenter__()
        await self.email_enricher.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self.phone_enricher.__aexit__(*args)
        await self.email_enricher.__aexit__(*args)

    async def enrich_contact(self, name: str, address: str, city: str, state: str,
                            zip_code: str, phone: str = None, email: str = None,
                            company_domain: str = None) -> EnrichedContact:
        """
        Enrich a contact with all available data.

        Args:
            name: Full name
            address: Street address
            city: City
            state: State
            zip_code: ZIP code
            phone: Phone number (optional)
            email: Email address (optional)
            company_domain: Company domain for email pattern detection (optional)

        Returns:
            EnrichedContact with validated phones, discovered emails, etc.
        """
        enriched = EnrichedContact(
            name=name,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            phones=[],
            emails=[],
            data_sources=[],
            enriched_at=datetime.now().isoformat(),
            confidence_score=0.0
        )

        confidence_components = []

        # Validate phone if provided
        if phone:
            phone_info = await self.phone_enricher.validate(phone)
            if phone_info.valid:
                enriched.phones.append(asdict(phone_info))
                enriched.data_sources.append(f"phone_validated")
                confidence_components.append(phone_info.confidence)

        # Verify email if provided
        if email:
            email_info = await self.email_enricher.hunter_verify(email)
            if email_info.valid == "valid":
                enriched.emails.append(email)
                enriched.data_sources.append("email_verified")
                confidence_components.append(email_info.score / 100)

        # Discover additional emails if company domain provided
        if company_domain and name:
            # Split name into first/last
            name_parts = name.split()
            if len(name_parts) >= 2:
                first = name_parts[0]
                last = name_parts[-1]

                # Generate email patterns
                patterns = self.email_enricher.generate_patterns(first, last, company_domain)
                enriched.emails.extend([p for p in patterns if p not in enriched.emails])

                if patterns:
                    enriched.data_sources.append("email_patterns_generated")

        # Calculate overall confidence score
        if confidence_components:
            enriched.confidence_score = sum(confidence_components) / len(confidence_components)

        return enriched

    async def enrich_lead(self, lead_data: Dict) -> Dict:
        """
        Enrich a lead record with contact information.

        Args:
            lead_data: Dict with keys: owner_name, address, city, state, zip,
                      owner_phone (optional), owner_email (optional),
                      contractor_name (optional), contractor_phone (optional)

        Returns:
            Enriched lead_data with validated contacts
        """
        enriched_lead = lead_data.copy()

        # Enrich owner contact
        if lead_data.get('owner_name'):
            owner_contact = await self.enrich_contact(
                name=lead_data.get('owner_name', ''),
                address=lead_data.get('address', ''),
                city=lead_data.get('city', ''),
                state=lead_data.get('state', ''),
                zip_code=lead_data.get('zip', ''),
                phone=lead_data.get('owner_phone'),
                email=lead_data.get('owner_email')
            )

            enriched_lead['owner_contact_enriched'] = asdict(owner_contact)
            enriched_lead['owner_phones_validated'] = owner_contact.phones
            enriched_lead['owner_emails_discovered'] = owner_contact.emails

        # Enrich contractor contact
        if lead_data.get('contractor_name') and lead_data.get('contractor_phone'):
            contractor_phone_info = await self.phone_enricher.validate(lead_data['contractor_phone'])
            if contractor_phone_info.valid:
                enriched_lead['contractor_phone_validated'] = asdict(contractor_phone_info)

        return enriched_lead

    def get_stats(self) -> Dict:
        """Get combined enrichment statistics"""
        return {
            "phone": self.phone_enricher.get_stats(),
            "email": self.email_enricher.get_stats()
        }


# Singleton instances for easy import
phone_enricher = PhoneEnricher()
email_enricher = EmailEnricher()
contact_enricher = ContactEnricher()
