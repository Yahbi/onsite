"""
Onsite - Email Campaign Service
SendGrid integration for email automation and campaigns
"""

import os
import aiohttp
from typing import List, Dict, Optional
from datetime import datetime

class EmailCampaignService:
    """
    Email campaign service using SendGrid
    FREE tier: 100 emails/day
    Paid tiers: $19.95/mo for 50K emails
    """

    def __init__(self):
        self.api_key = os.getenv("SENDGRID_API_KEY", "")
        self.from_email = os.getenv("SENDGRID_FROM_EMAIL", "leads@onsite.com")
        self.from_name = os.getenv("SENDGRID_FROM_NAME", "Onsite")
        self.base_url = "https://api.sendgrid.com/v3"
        self.app_url = os.getenv("ONSITE_APP_URL", "http://localhost:18000")

    async def send_single_email(
        self,
        to_email: str,
        subject: str,
        html_content: str,
        plain_content: str = ""
    ) -> Dict:
        """Send a single email via SendGrid"""
        if not self.api_key:
            return {
                "success": False,
                "error": "SendGrid API key not configured",
                "message": "Set SENDGRID_API_KEY environment variable"
            }

        url = f"{self.base_url}/mail/send"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "personalizations": [{
                "to": [{"email": to_email}],
                "subject": subject
            }],
            "from": {
                "email": self.from_email,
                "name": self.from_name
            },
            "content": [
                {
                    "type": "text/html",
                    "value": html_content
                }
            ]
        }

        if plain_content:
            payload["content"].insert(0, {
                "type": "text/plain",
                "value": plain_content
            })

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 202:  # SendGrid returns 202 for success
                    return {
                        "success": True,
                        "message": "Email sent successfully",
                        "to": to_email
                    }
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }

    async def send_bulk_emails(
        self,
        recipients: List[Dict],  # [{"email": "...", "name": "...", "custom_fields": {...}}]
        subject: str,
        html_template: str,
        plain_template: str = ""
    ) -> Dict:
        """Send bulk emails with personalization"""
        if not self.api_key:
            return {
                "success": False,
                "error": "SendGrid API key not configured"
            }

        url = f"{self.base_url}/mail/send"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # Build personalizations for each recipient
        personalizations = []
        for recipient in recipients:
            personalization = {
                "to": [{"email": recipient["email"], "name": recipient.get("name", "")}],
                "subject": subject,
                "substitutions": recipient.get("custom_fields", {})
            }
            personalizations.append(personalization)

        payload = {
            "personalizations": personalizations,
            "from": {
                "email": self.from_email,
                "name": self.from_name
            },
            "content": [
                {
                    "type": "text/html",
                    "value": html_template
                }
            ]
        }

        if plain_template:
            payload["content"].insert(0, {
                "type": "text/plain",
                "value": plain_template
            })

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status == 202:
                    return {
                        "success": True,
                        "message": f"Sent {len(recipients)} emails",
                        "count": len(recipients)
                    }
                else:
                    error = await resp.text()
                    return {
                        "success": False,
                        "error": error,
                        "status": resp.status
                    }

    async def send_hot_lead_alert(self, lead: Dict, contractor_email: str) -> Dict:
        """Send hot lead alert email"""
        subject = f"🔥 Hot Lead: ${lead.get('valuation', 0):,.0f} {lead.get('permit_type', 'Permit')}"

        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                <h1 style="color: white; margin: 0;">🔥 Hot Lead Alert!</h1>
            </div>

            <div style="padding: 30px; background: #f8f9fa;">
                <h2 style="color: #333; margin-top: 0;">High-Value Permit Opportunity</h2>

                <div style="background: white; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="color: #667eea; margin-top: 0;">Permit Details</h3>
                    <p><strong>Type:</strong> {lead.get('permit_type', 'Unknown')}</p>
                    <p><strong>Value:</strong> ${lead.get('valuation', 0):,.0f}</p>
                    <p><strong>Address:</strong> {lead.get('address', '')}, {lead.get('city', '')}, {lead.get('state', '')} {lead.get('zip', '')}</p>
                    <p><strong>Issue Date:</strong> {lead.get('issue_date', 'Unknown')}</p>
                </div>

                <div style="background: white; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h3 style="color: #667eea; margin-top: 0;">Property Owner</h3>
                    <p><strong>Name:</strong> {lead.get('owner_name', 'Unknown')}</p>
                    <p><strong>Phone:</strong> {lead.get('owner_phone', 'Not available')}</p>
                    <p><strong>Email:</strong> {lead.get('owner_email', 'Not available')}</p>
                </div>

                <div style="text-align: center; margin-top: 30px;">
                    <a href="{self.app_url}" style="background: #667eea; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">
                        View in Onsite
                    </a>
                </div>

                <p style="color: #666; font-size: 14px; margin-top: 30px; text-align: center;">
                    This lead was automatically detected by Onsite based on your criteria.
                </p>
            </div>
        </body>
        </html>
        """

        plain_content = f"""
        HOT LEAD ALERT!

        High-Value Permit Opportunity

        Permit Details:
        - Type: {lead.get('permit_type', 'Unknown')}
        - Value: ${lead.get('valuation', 0):,.0f}
        - Address: {lead.get('address', '')}, {lead.get('city', '')}, {lead.get('state', '')} {lead.get('zip', '')}
        - Issue Date: {lead.get('issue_date', 'Unknown')}

        Property Owner:
        - Name: {lead.get('owner_name', 'Unknown')}
        - Phone: {lead.get('owner_phone', 'Not available')}
        - Email: {lead.get('owner_email', 'Not available')}

        View in Onsite: {self.app_url}

        This lead was automatically detected by Onsite.
        """

        return await self.send_single_email(
            to_email=contractor_email,
            subject=subject,
            html_content=html_content,
            plain_content=plain_content
        )

    async def send_daily_digest(self, contractor_email: str, leads: List[Dict]) -> Dict:
        """Send daily digest of new leads"""
        total_value = sum(float(lead.get('valuation', 0) or 0) for lead in leads)

        subject = f"📊 Daily Digest: {len(leads)} New Leads (${total_value:,.0f} total value)"

        leads_html = ""
        for lead in leads[:20]:  # Limit to 20 leads in email
            leads_html += f"""
            <div style="background: white; padding: 15px; margin: 10px 0; border-left: 4px solid #667eea;">
                <p style="margin: 0;"><strong>{lead.get('permit_type', 'Unknown')}</strong> - ${lead.get('valuation', 0):,.0f}</p>
                <p style="margin: 5px 0; color: #666; font-size: 14px;">{lead.get('address', '')}, {lead.get('city', '')}</p>
                <p style="margin: 5px 0; color: #666; font-size: 14px;">Owner: {lead.get('owner_name', 'Unknown')}</p>
            </div>
            """

        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
                <h1 style="color: white; margin: 0;">📊 Daily Digest</h1>
            </div>

            <div style="padding: 30px; background: #f8f9fa;">
                <div style="background: white; padding: 20px; border-radius: 8px; margin: 20px 0;">
                    <h2 style="color: #333; margin-top: 0;">Summary</h2>
                    <p><strong>New Leads:</strong> {len(leads)}</p>
                    <p><strong>Total Value:</strong> ${total_value:,.0f}</p>
                    <p><strong>Average Value:</strong> ${total_value / len(leads) if leads else 0:,.0f}</p>
                </div>

                <h3 style="color: #333;">Recent Leads</h3>
                {leads_html}

                <div style="text-align: center; margin-top: 30px;">
                    <a href="{self.app_url}" style="background: #667eea; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px; display: inline-block;">
                        View All Leads
                    </a>
                </div>
            </div>
        </body>
        </html>
        """

        return await self.send_single_email(
            to_email=contractor_email,
            subject=subject,
            html_content=html_content
        )

    async def send_drip_campaign(
        self,
        campaign_id: str,
        recipient_email: str,
        step: int,
        lead_data: Dict
    ) -> Dict:
        """Send a drip campaign email (sequence of timed emails)"""
        # Drip campaign templates
        campaigns = {
            "new_lead_nurture": {
                1: {
                    "subject": f"Following up on {lead_data.get('address')} project",
                    "template": "templates/drip_step1.html"
                },
                2: {
                    "subject": "Quick question about your renovation",
                    "template": "templates/drip_step2.html"
                },
                3: {
                    "subject": "Final follow-up - still interested?",
                    "template": "templates/drip_step3.html"
                }
            }
        }

        campaign = campaigns.get(campaign_id, {})
        step_data = campaign.get(step)

        if not step_data:
            return {"success": False, "error": "Invalid campaign or step"}

        # Inline drip templates (step 1-3)
        address = lead_data.get('address', 'your property')
        owner = lead_data.get('owner_name', 'Homeowner')
        permit_type = lead_data.get('permit_type', 'project')
        drip_templates = {
            1: f"""
            <html><body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <div style="padding: 30px;">
                <h2>Hi {owner},</h2>
                <p>We noticed a <strong>{permit_type}</strong> permit was recently filed for
                <strong>{address}</strong>.</p>
                <p>We specialize in this type of work and would love to provide a free estimate.
                Our team has completed hundreds of similar projects in your area.</p>
                <p>Would you have 15 minutes for a quick call this week?</p>
                <p>Best regards,<br/>The Onsite Team</p>
              </div>
            </body></html>""",
            2: f"""
            <html><body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <div style="padding: 30px;">
                <h2>Hi {owner},</h2>
                <p>Just following up on my earlier message about the {permit_type} project
                at {address}.</p>
                <p>I know things get busy — I just wanted to make sure you have the help
                you need for this project. We offer free consultations and competitive pricing.</p>
                <p>Feel free to reply to this email or give us a call anytime.</p>
                <p>Best regards,<br/>The Onsite Team</p>
              </div>
            </body></html>""",
            3: f"""
            <html><body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
              <div style="padding: 30px;">
                <h2>Hi {owner},</h2>
                <p>This is my final follow-up regarding the {permit_type} work at {address}.</p>
                <p>If you've already found a contractor, no worries at all! If you're still
                looking, we'd be happy to help. Just reply to this email.</p>
                <p>Wishing you all the best with your project!</p>
                <p>Best regards,<br/>The Onsite Team</p>
              </div>
            </body></html>""",
        }
        html_content = drip_templates.get(step, f"<p>Drip step {step} for {address}</p>")

        return await self.send_single_email(
            to_email=recipient_email,
            subject=step_data["subject"],
            html_content=html_content
        )
