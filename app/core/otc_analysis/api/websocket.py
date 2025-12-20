from fastapi import WebSocket, WebSocketDisconnect
from typing import List, Dict, Set
import asyncio
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ConnectionManager:
    """
    WebSocket connection manager for live OTC activity streaming.
    
    Handles multiple clients and broadcasts events.
    """
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.subscriptions: Dict[WebSocket, Set[str]] = {}  # websocket -> event_types
        
    async def connect(self, websocket: WebSocket):
        """Accept new WebSocket connection."""
        await websocket.accept()
        self.active_connections.append(websocket)
        self.subscriptions[websocket] = set()
        logger.info(f"✅ New WebSocket connection. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.subscriptions:
            del self.subscriptions[websocket]
        logger.info(f"❌ WebSocket disconnected. Total: {len(self.active_connections)}")
    
    def subscribe(self, websocket: WebSocket, event_types: List[str]):
        """Subscribe to specific event types."""
        if websocket in self.subscriptions:
            self.subscriptions[websocket].update(event_types)
            logger.info(f"📡 Client subscribed to: {event_types}")
    
    def unsubscribe(self, websocket: WebSocket, event_types: List[str]):
        """Unsubscribe from event types."""
        if websocket in self.subscriptions:
            self.subscriptions[websocket].difference_update(event_types)
            logger.info(f"📴 Client unsubscribed from: {event_types}")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send message to specific client."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
    
    async def broadcast(self, message: dict, event_type: str):
        """
        Broadcast message to all subscribed clients.
        
        Args:
            message: Message to send
            event_type: Type of event (for filtering subscriptions)
        """
        disconnected = []
        
        for connection in self.active_connections:
            # Check if client is subscribed to this event type
            subscribed_events = self.subscriptions.get(connection, set())
            
            if not subscribed_events or event_type in subscribed_events:
                try:
                    await connection.send_json(message)
                except WebSocketDisconnect:
                    disconnected.append(connection)
                except Exception as e:
                    logger.error(f"Error broadcasting to client: {e}")
                    disconnected.append(connection)
        
        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)


# Global connection manager
manager = ConnectionManager()


async def handle_websocket_connection(websocket: WebSocket):
    """
    Handle WebSocket connection for live OTC stream.
    
    WebSocket endpoint: /ws/otc/live
    
    Event types:
    - new_large_transfer: Immediate notification of large transfers
    - cluster_activity: Activity in monitored clusters
    - desk_interaction: Transaction with known OTC desk
    """
    await manager.connect(websocket)
    
    try:
        # Send welcome message
        await manager.send_personal_message({
            "type": "connection",
            "message": "Connected to OTC live stream",
            "timestamp": datetime.utcnow().isoformat()
        }, websocket)
        
        while True:
            # Receive messages from client
            data = await websocket.receive_text()
            message = json.loads(data)
            
            message_type = message.get('type')
            
            if message_type == 'subscribe':
                # Subscribe to event types
                event_types = message.get('events', [])
                manager.subscribe(websocket, event_types)
                
                await manager.send_personal_message({
                    "type": "subscription_confirmed",
                    "events": event_types,
                    "timestamp": datetime.utcnow().isoformat()
                }, websocket)
            
            elif message_type == 'unsubscribe':
                # Unsubscribe from event types
                event_types = message.get('events', [])
                manager.unsubscribe(websocket, event_types)
                
                await manager.send_personal_message({
                    "type": "unsubscription_confirmed",
                    "events": event_types,
                    "timestamp": datetime.utcnow().isoformat()
                }, websocket)
            
            elif message_type == 'ping':
                # Respond to ping
                await manager.send_personal_message({
                    "type": "pong",
                    "timestamp": datetime.utcnow().isoformat()
                }, websocket)
            
            else:
                logger.warning(f"Unknown message type: {message_type}")
                await manager.send_personal_message({
                    "type": "error",
                    "message": f"Unknown message type: {message_type}"
                }, websocket)
    
    except WebSocketDisconnect:
        logger.info("Client disconnected")
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}", exc_info=True)
        manager.disconnect(websocket)


async def broadcast_otc_event(event_data: Dict):
    """
    Broadcast OTC event to all connected clients.
    
    Called by background workers when new OTC activity is detected.
    
    Args:
        event_data: {
            "type": "new_large_transfer" | "cluster_activity" | "desk_interaction",
            "tx_hash": "0x...",
            "confidence_score": 85.5,
            "usd_value": 1500000,
            "from_address": "0x...",
            "to_address": "0x...",
            "timestamp": "2024-..."
        }
    """
    event_type = event_data.get('type')
    
    logger.info(f"📢 Broadcasting {event_type} event: {event_data.get('tx_hash', '')[:16]}...")
    
    message = {
        "event": event_type,
        "data": event_data,
        "broadcast_time": datetime.utcnow().isoformat()
    }
    
    await manager.broadcast(message, event_type)


# Example: Background task that would stream live OTC detections
async def live_otc_monitor():
    """
    Background task that monitors blockchain for OTC activity
    and broadcasts to WebSocket clients.
    
    This would run continuously in production.
    """
    from app.core.otc_analysis.blockchain.block_scanner import BlockScanner
    from app.core.otc_analysis.detection.otc_detector import OTCDetector
    
    logger.info("🚀 Starting live OTC monitor...")
    
    scanner = BlockScanner(chain_id=1)
    detector = OTCDetector()
    
    scanner.start_from_latest()
    
    def on_new_transaction(tx):
        """Callback for new transactions."""
        try:
            # Quick filter
            if tx.get('usd_value', 0) < 100000:
                return
            
            logger.info(f"💰 Large transaction detected: ${tx.get('usd_value'):,.0f}")
            
            # Broadcast immediately
            asyncio.create_task(broadcast_otc_event({
                "type": "new_large_transfer",
                "tx_hash": tx['tx_hash'],
                "usd_value": tx.get('usd_value'),
                "from_address": tx['from_address'],
                "to_address": tx['to_address'],
                "timestamp": datetime.utcnow().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
    
    # This would run continuously
    # scanner.scan_continuous(callback=on_new_transaction)
