"""
Onsite WebSocket Handler  
Real-time notifications for new leads, hot leads, and stage changes
"""

from fastapi import WebSocket, WebSocketDisconnect, APIRouter
import asyncio
import json
import logging
from typing import Set
from datetime import datetime

from backend.models.database import get_unread_notifications, mark_notifications_read

logger = logging.getLogger(__name__)

router = APIRouter(tags=["websocket"])

# Connected WebSocket clients
_clients: Set[WebSocket] = set()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time lead notifications."""
    await websocket.accept()
    _clients.add(websocket)
    logger.info(f"WebSocket client connected ({len(_clients)} total)")
    
    try:
        # Send initial state
        await websocket.send_json({
            "type": "connected",
            "timestamp": datetime.now().isoformat(),
            "message": "Connected to Onsite real-time feed",
        })
        
        # Send any unread notifications
        unread = get_unread_notifications(limit=10)
        if unread:
            for notif in unread:
                await websocket.send_json({
                    "type": notif["type"],
                    "message": notif["message"],
                    "data": notif.get("data", {}),
                    "timestamp": notif.get("created_at"),
                })
            mark_notifications_read([n["id"] for n in unread])
        
        # Keep alive + poll for new notifications
        while True:
            try:
                # Wait for message from client (heartbeat/ping)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
                elif data == "poll":
                    # Client requesting latest notifications
                    notifs = get_unread_notifications(limit=5)
                    if notifs:
                        for n in notifs:
                            await websocket.send_json({
                                "type": n["type"],
                                "message": n["message"],
                                "data": n.get("data", {}),
                                "timestamp": n.get("created_at"),
                            })
                        mark_notifications_read([n["id"] for n in notifs])
                    
            except asyncio.TimeoutError:
                # Send heartbeat
                try:
                    await websocket.send_json({"type": "heartbeat"})
                except Exception:
                    break
                    
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        _clients.discard(websocket)


async def broadcast(message: dict):
    """Broadcast a message to all connected WebSocket clients."""
    if not _clients:
        return
    
    disconnected = set()
    for client in _clients:
        try:
            await client.send_json(message)
        except Exception:
            disconnected.add(client)
    
    _clients -= disconnected


async def notify_new_lead(lead: dict):
    """Send notification about a new lead."""
    await broadcast({
        "type": "new_lead",
        "message": f"New lead: {lead.get('address', 'Unknown')} ({lead.get('city', '')})",
        "data": {
            "id": lead.get("id"),
            "address": lead.get("address"),
            "city": lead.get("city"),
            "score": lead.get("score"),
            "valuation": lead.get("valuation"),
            "temperature": lead.get("temperature"),
        },
        "timestamp": datetime.now().isoformat(),
    })


async def notify_hot_lead(lead: dict):
    """Send urgent notification about a hot lead."""
    await broadcast({
        "type": "hot_lead",
        "message": f"🔥 HOT LEAD: {lead.get('address', '')} — Score {lead.get('score', 0)}, ${lead.get('valuation', 0):,.0f}",
        "data": {
            "id": lead.get("id"),
            "address": lead.get("address"),
            "city": lead.get("city"),
            "score": lead.get("score"),
            "valuation": lead.get("valuation"),
        },
        "timestamp": datetime.now().isoformat(),
    })
