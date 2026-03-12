"""
Email service for Onsite — sends transactional emails.
Uses Resend API in production, logs emails in dev mode.
"""

import os
import logging

import aiohttp

logger = logging.getLogger(__name__)

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "Onsite <noreply@onsite.app>")
APP_URL = os.getenv("APP_URL", "http://localhost:18000")

DEV_MODE = not RESEND_API_KEY


# ---------------------------------------------------------------------------
# Core sender
# ---------------------------------------------------------------------------

async def send_email(to: str, subject: str, html_body: str) -> bool:
    """Send an email. Returns True on success, False on failure."""
    if DEV_MODE:
        logger.info("[DEV EMAIL] To: %s | Subject: %s", to, subject)
        logger.debug("[DEV EMAIL] Body: %s", html_body[:200])
        return True

    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": EMAIL_FROM,
                    "to": [to],
                    "subject": subject,
                    "html": html_body,
                },
            )
            if resp.status in (200, 201):
                logger.info("Email sent to %s: %s", to, subject)
                return True
            body = await resp.text()
            logger.error("Resend API error %d: %s", resp.status, body)
            return False
    except Exception as exc:
        logger.error("Email send failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

def _base_template(content: str) -> str:
    """Wrap content in a styled email template."""
    return f"""
    <div style="font-family:'DM Sans',system-ui,sans-serif;max-width:560px;margin:0 auto;padding:32px 24px">
        <div style="text-align:center;margin-bottom:24px">
            <div style="display:inline-block;width:40px;height:40px;background:linear-gradient(135deg,#3d8b7a,#267063);border-radius:10px;line-height:40px;color:#fff;font-weight:800;font-size:16px">OS</div>
            <div style="font-size:20px;font-weight:800;color:#0f172a;margin-top:8px">Onsite</div>
        </div>
        {content}
        <div style="margin-top:32px;padding-top:16px;border-top:1px solid #e2e8f0;text-align:center;font-size:11px;color:#94a3b8">
            Onsite — Contractor Lead Intelligence<br>
            You received this email because you have an Onsite account.
        </div>
    </div>
    """


# ---------------------------------------------------------------------------
# Transactional emails
# ---------------------------------------------------------------------------

async def send_welcome(email: str, full_name: str) -> bool:
    """Send a welcome email after registration."""
    name = full_name or "there"
    html = _base_template(f"""
        <h2 style="color:#0f172a;font-size:22px;margin-bottom:8px">Welcome to Onsite, {name}!</h2>
        <p style="color:#475569;font-size:14px;line-height:1.6">
            Your account is set up and ready to go. Start discovering construction leads in your area.
        </p>
        <div style="text-align:center;margin:24px 0">
            <a href="{APP_URL}/app" style="display:inline-block;padding:12px 32px;background:#267063;color:#fff;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Open Dashboard</a>
        </div>
    """)
    return await send_email(email, "Welcome to Onsite", html)


async def send_email_verification(email: str, verify_token: str) -> bool:
    """Send email verification link."""
    link = f"{APP_URL}/login?verify_token={verify_token}"
    html = _base_template(f"""
        <h2 style="color:#0f172a;font-size:22px;margin-bottom:8px">Verify Your Email</h2>
        <p style="color:#475569;font-size:14px;line-height:1.6">
            Click the button below to verify your email address and activate your account.
        </p>
        <div style="text-align:center;margin:24px 0">
            <a href="{link}" style="display:inline-block;padding:12px 32px;background:#267063;color:#fff;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Verify Email</a>
        </div>
        <p style="color:#94a3b8;font-size:12px">This link expires in 24 hours.</p>
    """)
    return await send_email(email, "Verify your Onsite email", html)


async def send_password_reset(email: str, reset_token: str) -> bool:
    """Send password reset link."""
    link = f"{APP_URL}/login?reset_token={reset_token}"
    html = _base_template(f"""
        <h2 style="color:#0f172a;font-size:22px;margin-bottom:8px">Reset Your Password</h2>
        <p style="color:#475569;font-size:14px;line-height:1.6">
            We received a request to reset your password. Click the button below to set a new one.
        </p>
        <div style="text-align:center;margin:24px 0">
            <a href="{link}" style="display:inline-block;padding:12px 32px;background:#267063;color:#fff;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Reset Password</a>
        </div>
        <p style="color:#94a3b8;font-size:12px">This link expires in 1 hour. If you didn't request this, you can safely ignore it.</p>
    """)
    return await send_email(email, "Reset your Onsite password", html)


async def send_subscription_confirmation(email: str, plan_name: str) -> bool:
    """Send confirmation after successful subscription."""
    html = _base_template(f"""
        <h2 style="color:#0f172a;font-size:22px;margin-bottom:8px">Subscription Confirmed!</h2>
        <p style="color:#475569;font-size:14px;line-height:1.6">
            You're now on the <strong>{plan_name}</strong> plan. All features are unlocked and ready to use.
        </p>
        <div style="text-align:center;margin:24px 0">
            <a href="{APP_URL}/app" style="display:inline-block;padding:12px 32px;background:#267063;color:#fff;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Go to Dashboard</a>
        </div>
    """)
    return await send_email(email, f"Onsite {plan_name} Plan Activated", html)


async def send_payment_failed(email: str, plan_name: str) -> bool:
    """Notify user of failed payment."""
    html = _base_template(f"""
        <h2 style="color:#0f172a;font-size:22px;margin-bottom:8px">Payment Failed</h2>
        <p style="color:#475569;font-size:14px;line-height:1.6">
            We couldn't process your payment for the <strong>{plan_name}</strong> plan.
            Please update your payment method to keep your subscription active.
        </p>
        <div style="text-align:center;margin:24px 0">
            <a href="{APP_URL}/app?tab=settings" style="display:inline-block;padding:12px 32px;background:#dc2626;color:#fff;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Update Payment</a>
        </div>
    """)
    return await send_email(email, "Onsite: Payment Failed", html)
