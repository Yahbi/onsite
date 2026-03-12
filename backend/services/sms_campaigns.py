"""
Onsite - SMS Campaign Service
Twilio integration for SMS automation and campaigns
"""

import os
import aiohttp
import base64
from typing import List, Dict, Optional
from datetime import datetime

class SMSCampaignService:
    """
    SMS campaign service using Twilio
    Cost: ~$0.0079 per SMS in US
    Twilio Trial: FREE with test credits
    """

    def __init__(self):
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        self.from_number = os.getenv("TWILIO_FROM_NUMBER", "")
        self.base_url = f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}"

    def _get_auth_header(self) -> str:
        """Generate Basic Auth header for Twilio"""
        credentials = f"{self.account_sid}:{self.auth_token}"
        b64_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {b64_credentials}"

    async def send_sms(
        self,
        to_number: str,
        message: str
    ) -> Dict:
        """Send a single SMS via Twilio"""
        if not self.account_sid or not self.auth_token or not self.from_number:
            return {
                "success": False,
                "error": "Twilio credentials not configured",
                "message": "Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER"
            }

        url = f"{self.base_url}/Messages.json"
        headers = {
            "Authorization": self._get_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # Ensure phone numbers are in E.164 format (+1XXXXXXXXXX)
        if not to_number.startswith("+"):
            to_number = f"+1{to_number.replace('-', '').replace('(', '').replace(')', '').replace(' ', '')}"

        data = {
            "From": self.from_number,
            "To": to_number,
            "Body": message
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, headers=headers) as resp:
                if resp.status in [200, 201]:
                    result = await resp.json()
                    return {
                        "success": True,
                        "message": "SMS sent successfully",
                        "to": to_number,
                        "sid": result.get("sid"),
                        "status": result.get("status")
                    }
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }

    async def send_bulk_sms(
        self,
        recipients: List[Dict],  # [{"phone": "...", "message": "..."}]
    ) -> Dict:
        """Send bulk SMS messages"""
        results = []
        success_count = 0
        fail_count = 0

        for recipient in recipients:
            result = await self.send_sms(
                to_number=recipient["phone"],
                message=recipient["message"]
            )
            results.append(result)

            if result["success"]:
                success_count += 1
            else:
                fail_count += 1

        return {
            "success": fail_count == 0,
            "total": len(recipients),
            "sent": success_count,
            "failed": fail_count,
            "results": results
        }

    async def send_hot_lead_alert(self, lead: Dict, contractor_phone: str) -> Dict:
        """Send hot lead alert via SMS"""
        message = (
            f"🔥 HOT LEAD: ${lead.get('valuation', 0):,.0f} {lead.get('permit_type', 'Permit')}\n"
            f"📍 {lead.get('address', '')}, {lead.get('city', '')}\n"
            f"👤 {lead.get('owner_name', 'Unknown')}\n"
            f"📞 {lead.get('owner_phone', 'N/A')}\n"
            f"View: http://localhost:18000"
        )

        return await self.send_sms(
            to_number=contractor_phone,
            message=message
        )

    async def send_daily_summary(self, contractor_phone: str, stats: Dict) -> Dict:
        """Send daily summary SMS"""
        message = (
            f"📊 Onsite Daily Summary\n"
            f"🆕 {stats.get('new_leads', 0)} new leads\n"
            f"💰 ${stats.get('total_value', 0):,.0f} total value\n"
            f"🔥 {stats.get('hot_leads', 0)} hot leads\n"
            f"View dashboard: http://localhost:18000"
        )

        return await self.send_sms(
            to_number=contractor_phone,
            message=message
        )

    async def send_lead_reminder(self, lead: Dict, contractor_phone: str) -> Dict:
        """Send reminder to follow up on a lead"""
        message = (
            f"⏰ Follow-up Reminder\n"
            f"Lead: {lead.get('owner_name', 'Unknown')}\n"
            f"📍 {lead.get('address', '')}\n"
            f"💰 ${lead.get('valuation', 0):,.0f}\n"
            f"Added {lead.get('days_ago', 0)} days ago"
        )

        return await self.send_sms(
            to_number=contractor_phone,
            message=message
        )

    async def send_two_way_message(self, to_number: str, message: str) -> Dict:
        """
        Send SMS that supports two-way messaging
        Requires Twilio webhook setup for incoming messages
        """
        return await self.send_sms(to_number, message)

    async def get_message_status(self, message_sid: str) -> Dict:
        """Check the delivery status of a sent message"""
        if not self.account_sid or not self.auth_token:
            return {"success": False, "error": "Twilio credentials not configured"}

        url = f"{self.base_url}/Messages/{message_sid}.json"
        headers = {
            "Authorization": self._get_auth_header()
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return {
                        "success": True,
                        "status": result.get("status"),
                        "to": result.get("to"),
                        "from": result.get("from"),
                        "date_sent": result.get("date_sent"),
                        "error_code": result.get("error_code"),
                        "error_message": result.get("error_message")
                    }
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }

    async def send_template_sms(
        self,
        to_number: str,
        template_name: str,
        variables: Dict
    ) -> Dict:
        """Send SMS using a pre-defined template"""
        templates = {
            "hot_lead": (
                "🔥 HOT LEAD: ${valuation} {permit_type}\n"
                "📍 {address}, {city}\n"
                "👤 {owner_name}\n"
                "📞 {owner_phone}"
            ),
            "follow_up": (
                "Hi {owner_name}, following up on the {permit_type} "
                "permit at {address}. Would love to provide a quote. "
                "Call/text me at {contractor_phone}."
            ),
            "quote_sent": (
                "Hi {owner_name}, I've sent you a quote for the "
                "{permit_type} project at {address}. "
                "Let me know if you have any questions!"
            ),
            "won_deal": (
                "🎉 Congratulations! You won the {permit_type} project "
                "at {address} worth ${valuation}!"
            )
        }

        template = templates.get(template_name)
        if not template:
            return {"success": False, "error": f"Template '{template_name}' not found"}

        # Replace variables in template
        message = template.format(**variables)

        return await self.send_sms(to_number, message)
