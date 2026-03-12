"""
Onsite - CRM Integration Service
OAuth flows and sync for Salesforce, HubSpot, ServiceTitan
"""

import os
import aiohttp
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
import json

# ═══════════════════════════════════════════════════════════════════════════
# SALESFORCE INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════

class SalesforceIntegration:
    """Salesforce OAuth and lead sync"""

    def __init__(self):
        self.client_id = os.getenv("SALESFORCE_CLIENT_ID", "")
        self.client_secret = os.getenv("SALESFORCE_CLIENT_SECRET", "")
        self.redirect_uri = os.getenv("SALESFORCE_REDIRECT_URI", "http://localhost:18000/api/crm/salesforce/callback")
        self.instance_url = None
        self.access_token = None

    def get_auth_url(self, state: str = "") -> str:
        """Get Salesforce OAuth authorization URL"""
        base_url = "https://login.salesforce.com/services/oauth2/authorize"
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "state": state
        }
        query = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{query}"

    async def exchange_code(self, code: str) -> Dict:
        """Exchange authorization code for access token"""
        url = "https://login.salesforce.com/services/oauth2/token"
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    self.access_token = result.get("access_token")
                    self.instance_url = result.get("instance_url")
                    return {
                        "success": True,
                        "access_token": self.access_token,
                        "instance_url": self.instance_url,
                        "refresh_token": result.get("refresh_token")
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": error}

    async def sync_leads(self, leads: List[Dict]) -> Dict:
        """Push leads to Salesforce as Lead objects"""
        if not self.access_token or not self.instance_url:
            return {"success": False, "error": "Not authenticated"}

        url = f"{self.instance_url}/services/data/v57.0/composite/sobjects"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        # Convert Onsite leads to Salesforce Lead format
        sf_leads = []
        for lead in leads:
            sf_lead = {
                "attributes": {"type": "Lead"},
                "Company": lead.get("owner_name", "Unknown Owner"),
                "LastName": lead.get("owner_name", "").split()[-1] if lead.get("owner_name") else "Unknown",
                "FirstName": " ".join(lead.get("owner_name", "").split()[:-1]) if lead.get("owner_name") else "",
                "Street": lead.get("address", ""),
                "City": lead.get("city", ""),
                "State": lead.get("state", ""),
                "PostalCode": lead.get("zip", ""),
                "Phone": lead.get("owner_phone", ""),
                "Email": lead.get("owner_email", ""),
                "LeadSource": "Onsite",
                "Status": "New",
                "Description": f"Permit: {lead.get('permit_type', 'Unknown')} - ${lead.get('valuation', 0):,.0f}"
            }
            sf_leads.append(sf_lead)

        payload = {"records": sf_leads}

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status in [200, 201]:
                    result = await resp.json()
                    return {
                        "success": True,
                        "synced": len(sf_leads),
                        "results": result
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": error}


# ═══════════════════════════════════════════════════════════════════════════
# HUBSPOT INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════

class HubSpotIntegration:
    """HubSpot OAuth and contact sync"""

    def __init__(self):
        self.client_id = os.getenv("HUBSPOT_CLIENT_ID", "")
        self.client_secret = os.getenv("HUBSPOT_CLIENT_SECRET", "")
        self.redirect_uri = os.getenv("HUBSPOT_REDIRECT_URI", "http://localhost:18000/api/crm/hubspot/callback")
        self.access_token = None

    def get_auth_url(self, state: str = "") -> str:
        """Get HubSpot OAuth authorization URL"""
        base_url = "https://app.hubspot.com/oauth/authorize"
        scopes = "crm.objects.contacts.write,crm.objects.contacts.read"
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": scopes,
            "state": state
        }
        query = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{query}"

    async def exchange_code(self, code: str) -> Dict:
        """Exchange authorization code for access token"""
        url = "https://api.hubapi.com/oauth/v1/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
            "code": code
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    self.access_token = result.get("access_token")
                    return {
                        "success": True,
                        "access_token": self.access_token,
                        "refresh_token": result.get("refresh_token")
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": error}

    async def sync_leads(self, leads: List[Dict]) -> Dict:
        """Push leads to HubSpot as Contacts"""
        if not self.access_token:
            return {"success": False, "error": "Not authenticated"}

        url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        # Convert Onsite leads to HubSpot Contact format
        hs_contacts = []
        for lead in leads:
            hs_contact = {
                "properties": {
                    "firstname": lead.get("owner_name", "").split()[0] if lead.get("owner_name") else "Unknown",
                    "lastname": " ".join(lead.get("owner_name", "").split()[1:]) if lead.get("owner_name") else "",
                    "email": lead.get("owner_email", ""),
                    "phone": lead.get("owner_phone", ""),
                    "address": lead.get("address", ""),
                    "city": lead.get("city", ""),
                    "state": lead.get("state", ""),
                    "zip": lead.get("zip", ""),
                    "lifecyclestage": "lead",
                    "lead_source": "Onsite",
                    "permit_type": lead.get("permit_type", ""),
                    "permit_value": str(lead.get("valuation", 0))
                }
            }
            hs_contacts.append(hs_contact)

        payload = {"inputs": hs_contacts}

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status in [200, 201]:
                    result = await resp.json()
                    return {
                        "success": True,
                        "synced": len(hs_contacts),
                        "results": result
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": error}


# ═══════════════════════════════════════════════════════════════════════════
# SERVICETITAN INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════

class ServiceTitanIntegration:
    """ServiceTitan OAuth and customer sync"""

    def __init__(self):
        self.client_id = os.getenv("SERVICETITAN_CLIENT_ID", "")
        self.client_secret = os.getenv("SERVICETITAN_CLIENT_SECRET", "")
        self.tenant_id = os.getenv("SERVICETITAN_TENANT_ID", "")
        self.access_token = None

    def get_auth_url(self, state: str = "") -> str:
        """Get ServiceTitan OAuth authorization URL"""
        base_url = "https://auth.servicetitan.io/connect/authorize"
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": "http://localhost:18000/api/crm/servicetitan/callback",
            "scope": "customers,leads",
            "state": state
        }
        query = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{query}"

    async def exchange_code(self, code: str) -> Dict:
        """Exchange authorization code for access token"""
        url = "https://auth.servicetitan.io/connect/token"
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": "http://localhost:18000/api/crm/servicetitan/callback"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    self.access_token = result.get("access_token")
                    return {
                        "success": True,
                        "access_token": self.access_token,
                        "refresh_token": result.get("refresh_token")
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": error}

    async def sync_leads(self, leads: List[Dict]) -> Dict:
        """Push leads to ServiceTitan as Customers"""
        if not self.access_token or not self.tenant_id:
            return {"success": False, "error": "Not authenticated"}

        url = f"https://api.servicetitan.io/crm/v2/tenant/{self.tenant_id}/customers/batch"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "ST-App-Key": self.client_id
        }

        # Convert Onsite leads to ServiceTitan Customer format
        st_customers = []
        for lead in leads:
            st_customer = {
                "name": lead.get("owner_name", "Unknown Owner"),
                "type": "Residential",
                "address": {
                    "street": lead.get("address", ""),
                    "city": lead.get("city", ""),
                    "state": lead.get("state", ""),
                    "zip": lead.get("zip", "")
                },
                "contacts": [{
                    "type": "Phone",
                    "value": lead.get("owner_phone", "")
                }, {
                    "type": "Email",
                    "value": lead.get("owner_email", "")
                }],
                "customFields": [{
                    "name": "Lead Source",
                    "value": "Onsite"
                }, {
                    "name": "Permit Type",
                    "value": lead.get("permit_type", "")
                }, {
                    "name": "Permit Value",
                    "value": str(lead.get("valuation", 0))
                }]
            }
            st_customers.append(st_customer)

        payload = {"customers": st_customers}

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status in [200, 201]:
                    result = await resp.json()
                    return {
                        "success": True,
                        "synced": len(st_customers),
                        "results": result
                    }
                else:
                    error = await resp.text()
                    return {"success": False, "error": error}


# ═══════════════════════════════════════════════════════════════════════════
# UNIFIED CRM MANAGER
# ═══════════════════════════════════════════════════════════════════════════

class CRMManager:
    """Unified interface for all CRM integrations"""

    def __init__(self):
        self.salesforce = SalesforceIntegration()
        self.hubspot = HubSpotIntegration()
        self.servicetitan = ServiceTitanIntegration()

    def get_auth_url(self, crm: str, state: str = "") -> Optional[str]:
        """Get OAuth URL for any CRM"""
        if crm == "salesforce":
            return self.salesforce.get_auth_url(state)
        elif crm == "hubspot":
            return self.hubspot.get_auth_url(state)
        elif crm == "servicetitan":
            return self.servicetitan.get_auth_url(state)
        return None

    async def exchange_code(self, crm: str, code: str) -> Dict:
        """Exchange OAuth code for any CRM"""
        if crm == "salesforce":
            return await self.salesforce.exchange_code(code)
        elif crm == "hubspot":
            return await self.hubspot.exchange_code(code)
        elif crm == "servicetitan":
            return await self.servicetitan.exchange_code(code)
        return {"success": False, "error": "Unknown CRM"}

    async def sync_leads(self, crm: str, leads: List[Dict]) -> Dict:
        """Sync leads to any CRM"""
        if crm == "salesforce":
            return await self.salesforce.sync_leads(leads)
        elif crm == "hubspot":
            return await self.hubspot.sync_leads(leads)
        elif crm == "servicetitan":
            return await self.servicetitan.sync_leads(leads)
        return {"success": False, "error": "Unknown CRM"}
