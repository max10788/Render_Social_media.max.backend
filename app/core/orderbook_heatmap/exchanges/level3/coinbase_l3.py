"""
Coinbase Level 3 (Full Orderbook) Integration

Provides full order-by-order data from Coinbase Pro.
Requires authentication for L3 data access.
"""
import asyncio
import json
import logging
import time
import hmac
import hashlib
import base64
from typing import Optional, Dict, Any
from datetime import datetime
import aiohttp
import websockets
import os

from app.core.orderbook_heatmap.models.orderbook import Exchange
from app.core.orderbook_heatmap.models.level3 import (
    L3Order, L3Orderbook, L3EventType, L3Side
)
from .base_l3 import L3Exchange


logger = logging.getLogger(__name__)


class CoinbaseL3(L3Exchange):
    """
    Coinbase Level 3 orderbook integration

    Provides full order-by-order data via the 'full' channel.
    Note: Requires API credentials for Level 3 access.
    """

    REST_API = "https://api.exchange.coinbase.com"
    WS_API = "wss://ws-feed.exchange.coinbase.com"

    def __init__(self):
        super().__init__(Exchange.COINBASE)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None

        # Load credentials from environment
        self.api_key = os.getenv("COINBASE_API_KEY")
        self.api_secret = os.getenv("COINBASE_API_SECRET")
        self.api_passphrase = os.getenv("COINBASE_API_PASSPHRASE")

        if not all([self.api_key, self.api_secret, self.api_passphrase]):
            logger.warning("Coinbase API credentials not found. L3 access may be limited.")

    def _get_auth_headers(self, method: str, request_path: str, body: str = "") -> Dict[str, str]:
        """
        Generate authentication headers for Coinbase API

        Args:
            method: HTTP method (GET, POST, etc.)
            request_path: API endpoint path
            body: Request body (for POST requests)

        Returns:
            Dictionary of authentication headers
        """
        if not all([self.api_key, self.api_secret, self.api_passphrase]):
            return {}

        timestamp = str(time.time())
        message = timestamp + method + request_path + body
        message = message.encode('ascii')
        hmac_key = base64.b64decode(self.api_secret)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')

        return {
            "CB-ACCESS-KEY": self.api_key,
            "CB-ACCESS-SIGN": signature_b64,
            "CB-ACCESS-TIMESTAMP": timestamp,
            "CB-ACCESS-PASSPHRASE": self.api_passphrase,
            "Content-Type": "application/json"
        }

    async def get_l3_snapshot(self, symbol: str) -> Optional[L3Orderbook]:
        """
        Fetch Level 3 orderbook snapshot via REST API

        Args:
            symbol: Trading pair (e.g. "BTC/USDT")

        Returns:
            L3Orderbook with all orders
        """
        try:
            normalized = self.normalize_symbol(symbol)
            request_path = f"/products/{normalized}/book?level=3"
            url = f"{self.REST_API}{request_path}"

            if not self.session:
                self.session = aiohttp.ClientSession()

            # Add authentication headers
            headers = self._get_auth_headers("GET", request_path)
            headers["User-Agent"] = "orderbook-heatmap-l3/1.0"

            async with self.session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    logger.error(f"Coinbase L3 API error: {resp.status}")
                    text = await resp.text()
                    logger.error(f"Response: {text}")
                    return None

                data = await resp.json()

                # Parse L3 snapshot
                timestamp = datetime.utcnow()
                sequence = data.get("sequence", 0)

                bids = []
                asks = []

                # Parse bids [price, size, order_id]
                for bid in data.get("bids", []):
                    bids.append(L3Order(
                        exchange="coinbase",
                        symbol=symbol,
                        order_id=bid[2],
                        side=L3Side.BID,
                        price=float(bid[0]),
                        size=float(bid[1]),
                        event_type=L3EventType.OPEN,
                        timestamp=timestamp,
                        sequence=sequence
                    ))

                # Parse asks [price, size, order_id]
                for ask in data.get("asks", []):
                    asks.append(L3Order(
                        exchange="coinbase",
                        symbol=symbol,
                        order_id=ask[2],
                        side=L3Side.ASK,
                        price=float(ask[0]),
                        size=float(ask[1]),
                        event_type=L3EventType.OPEN,
                        timestamp=timestamp,
                        sequence=sequence
                    ))

                snapshot = L3Orderbook(
                    exchange="coinbase",
                    symbol=symbol,
                    sequence=sequence,
                    timestamp=timestamp,
                    bids=bids,
                    asks=asks
                )

                logger.info(f"Fetched Coinbase L3 snapshot: {len(bids)} bids, {len(asks)} asks")
                return snapshot

        except Exception as e:
            logger.error(f"Error fetching Coinbase L3 snapshot: {e}")
            return None

    async def subscribe_l3_updates(self, symbol: str):
        """
        Subscribe to Coinbase Level 3 WebSocket feed

        Connects to the 'full' channel which provides all order lifecycle events.
        """
        normalized = self.normalize_symbol(symbol)
        self._current_symbol = symbol

        try:
            async with websockets.connect(
                self.WS_API,
                ping_interval=30,
                ping_timeout=10
            ) as ws:
                self.ws = ws

                # Subscribe message
                subscribe_msg = {
                    "type": "subscribe",
                    "product_ids": [normalized],
                    "channels": ["full"]  # Full channel provides L3 data
                }

                # Add authentication if available
                if all([self.api_key, self.api_secret, self.api_passphrase]):
                    timestamp = str(time.time())
                    message = timestamp + "GET" + "/users/self/verify"
                    message = message.encode('ascii')
                    hmac_key = base64.b64decode(self.api_secret)
                    signature = hmac.new(hmac_key, message, hashlib.sha256)
                    signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')

                    subscribe_msg["signature"] = signature_b64
                    subscribe_msg["key"] = self.api_key
                    subscribe_msg["passphrase"] = self.api_passphrase
                    subscribe_msg["timestamp"] = timestamp

                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"Subscribed to Coinbase L3 for {normalized}")

                # Process messages
                async for message in ws:
                    try:
                        data = json.loads(message)
                        msg_type = data.get("type")

                        # Skip non-order messages
                        if msg_type in ["subscriptions", "heartbeat", "error"]:
                            if msg_type == "error":
                                logger.error(f"Coinbase WebSocket error: {data.get('message')}")
                            continue

                        # Parse L3 event
                        order = self.parse_l3_event(data)
                        if order:
                            await self._emit_l3_order(order)

                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON from Coinbase: {e}")
                    except Exception as e:
                        logger.error(f"Error processing Coinbase L3 message: {e}")

        except websockets.exceptions.WebSocketException as e:
            logger.error(f"Coinbase WebSocket error: {e}")
        except Exception as e:
            logger.error(f"Error in Coinbase L3 subscription: {e}")
        finally:
            self.ws = None
            self.is_connected = False

    def parse_l3_event(self, data: Dict[str, Any]) -> Optional[L3Order]:
        """
        Parse Coinbase L3 message into L3Order

        Coinbase message types:
        - received: New order received (not yet on book)
        - open: Order placed on book
        - done: Order removed (filled or canceled)
        - match: Trade executed
        - change: Order size changed

        Args:
            data: Raw WebSocket message

        Returns:
            L3Order or None
        """
        try:
            msg_type = data.get("type")

            # Map Coinbase message types to L3EventType
            event_map = {
                "open": L3EventType.OPEN,
                "done": L3EventType.DONE,
                "match": L3EventType.MATCH,
                "change": L3EventType.CHANGE
            }

            # Skip "received" messages (not yet on book)
            if msg_type not in event_map:
                return None

            event_type = event_map[msg_type]

            # Extract common fields
            order_id = data.get("order_id")
            if not order_id:
                return None

            side = L3Side.BID if data.get("side") == "buy" else L3Side.ASK
            sequence = data.get("sequence", 0)
            timestamp = datetime.fromisoformat(data.get("time", datetime.utcnow().isoformat()).replace("Z", "+00:00"))

            # Price and size handling varies by message type
            price = None
            size = None

            if msg_type == "open":
                price = float(data.get("price", 0))
                size = float(data.get("remaining_size", 0))

            elif msg_type == "done":
                price = float(data.get("price", 0))
                size = float(data.get("remaining_size", 0))

            elif msg_type == "match":
                price = float(data.get("price", 0))
                size = float(data.get("size", 0))

            elif msg_type == "change":
                price = float(data.get("price", 0))
                size = float(data.get("new_size", 0))

            if price is None or size is None:
                return None

            # Build metadata
            metadata = {
                "reason": data.get("reason"),
                "maker_order_id": data.get("maker_order_id"),
                "taker_order_id": data.get("taker_order_id"),
                "trade_id": data.get("trade_id")
            }
            # Remove None values
            metadata = {k: v for k, v in metadata.items() if v is not None}

            order = L3Order(
                exchange="coinbase",
                symbol=self._current_symbol or data.get("product_id", ""),
                order_id=order_id,
                sequence=sequence,
                side=side,
                price=price,
                size=size,
                event_type=event_type,
                timestamp=timestamp,
                metadata=metadata if metadata else None
            )

            return order

        except Exception as e:
            logger.error(f"Error parsing Coinbase L3 event: {e}")
            return None

    async def disconnect(self):
        """Disconnect from Coinbase"""
        if self.ws:
            await self.ws.close()
            self.ws = None

        if self.session:
            await self.session.close()
            self.session = None

        self.is_connected = False
        logger.info("Disconnected from Coinbase L3")

    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol for Coinbase (BTC/USDT -> BTC-USD)

        Args:
            symbol: Symbol in standard format

        Returns:
            Coinbase format symbol
        """
        # Coinbase uses USD instead of USDT
        normalized = symbol.replace("/", "-").replace("USDT", "USD")
        return normalized
