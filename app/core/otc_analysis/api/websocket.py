from fastapi import WebSocket, WebSocketDisconnect
from typing import List, Dict, Set
import asyncio
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Socket.IO server reference (injected from main.py)
_sio_instance = None


def set_socketio(sio):
    """Inject the Socket.IO server so we can broadcast to both WS and SIO clients."""
    global _sio_instance
    _sio_instance = sio
    logger.info("Socket.IO instance registered in websocket module")


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
        logger.info(f"New WebSocket connection. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Remove WebSocket connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.subscriptions:
            del self.subscriptions[websocket]
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    def subscribe(self, websocket: WebSocket, event_types: List[str]):
        """Subscribe to specific event types."""
        if websocket in self.subscriptions:
            self.subscriptions[websocket].update(event_types)
            logger.info(f"Client subscribed to: {event_types}")

    def unsubscribe(self, websocket: WebSocket, event_types: List[str]):
        """Unsubscribe from event types."""
        if websocket in self.subscriptions:
            self.subscriptions[websocket].difference_update(event_types)
            logger.info(f"Client unsubscribed from: {event_types}")

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


async def broadcast_to_all(event_type: str, data: dict):
    """
    Broadcast an OTC event to both native WebSocket clients and Socket.IO clients.

    Args:
        event_type: e.g. "new_large_transfer", "cluster_activity", "desk_interaction"
        data: Event payload
    """
    message = {
        "event": event_type,
        "data": data,
        "broadcast_time": datetime.utcnow().isoformat(),
    }

    # Native WebSocket clients
    await manager.broadcast(message, event_type)

    # Socket.IO clients
    if _sio_instance is not None:
        try:
            await _sio_instance.emit(event_type, message)
        except Exception as e:
            logger.error(f"Socket.IO broadcast error: {e}")


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
                event_types = message.get('events', [])
                manager.subscribe(websocket, event_types)

                await manager.send_personal_message({
                    "type": "subscription_confirmed",
                    "events": event_types,
                    "timestamp": datetime.utcnow().isoformat()
                }, websocket)

            elif message_type == 'unsubscribe':
                event_types = message.get('events', [])
                manager.unsubscribe(websocket, event_types)

                await manager.send_personal_message({
                    "type": "unsubscription_confirmed",
                    "events": event_types,
                    "timestamp": datetime.utcnow().isoformat()
                }, websocket)

            elif message_type == 'ping':
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
    """
    event_type = event_data.get('type', 'unknown')
    logger.info(f"Broadcasting {event_type} event: {event_data.get('tx_hash', '')[:16]}...")
    await broadcast_to_all(event_type, event_data)


async def live_otc_monitor(shutdown_event: asyncio.Event):
    """
    Background task that polls known OTC desk addresses for recent large
    transfers via Moralis and broadcasts matches to WebSocket clients.

    Runs continuously until shutdown_event is set.
    """
    logger.info("Starting live OTC monitor...")

    backoff = 30  # seconds between polls

    while not shutdown_event.is_set():
        try:
            from app.core.otc_analysis.blockchain.moralis import MoralisAPI
            from app.core.otc_analysis.data_sources.otc_desks import OTCDeskRegistry

            moralis = MoralisAPI()
            registry = OTCDeskRegistry()

            desk_addresses = registry.get_all_otc_addresses()
            if not desk_addresses:
                logger.warning("No OTC desk addresses found, retrying in 60s")
                await asyncio.sleep(60)
                continue

            for address in desk_addresses:
                if shutdown_event.is_set():
                    break

                try:
                    result = moralis.get_wallet_history(address=address, limit=10)
                    transactions = result.get('result', []) if result else []

                    for tx in transactions:
                        value_str = tx.get('value', '0')
                        try:
                            value_wei = int(value_str)
                        except (ValueError, TypeError):
                            value_wei = 0

                        value_eth = value_wei / 1e18
                        value_usd = value_eth * 3000  # rough estimate

                        if value_usd >= 100_000:
                            await broadcast_to_all("new_large_transfer", {
                                "type": "new_large_transfer",
                                "tx_hash": tx.get('hash', ''),
                                "from_address": tx.get('from_address', ''),
                                "to_address": tx.get('to_address', ''),
                                "value_eth": round(value_eth, 4),
                                "usd_value": round(value_usd, 2),
                                "from_entity": tx.get('from_address_entity', ''),
                                "to_entity": tx.get('to_address_entity', ''),
                                "timestamp": tx.get('block_timestamp', datetime.utcnow().isoformat()),
                            })

                except Exception as e:
                    logger.error(f"Error polling address {address[:10]}...: {e}")

            # Reset backoff on success
            backoff = 30

        except Exception as e:
            logger.error(f"Live OTC monitor error: {e}", exc_info=True)
            backoff = min(backoff * 2, 300)  # exponential backoff, max 5 min

        # Wait for next poll or shutdown
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=backoff)
            break  # shutdown_event was set
        except asyncio.TimeoutError:
            pass  # timeout expired, poll again

    logger.info("Live OTC monitor stopped.")
