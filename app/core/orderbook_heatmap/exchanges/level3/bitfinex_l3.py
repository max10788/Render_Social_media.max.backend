"""
Bitfinex Level 3 (Raw Order Books) Integration

Provides full order-by-order data from Bitfinex using Raw Books (R0 precision).
No authentication required for public data.
"""
import asyncio
import json
import logging
from typing import Optional, Dict, Any, Set
from datetime import datetime
import aiohttp
import websockets

from app.core.orderbook_heatmap.models.orderbook import Exchange
from app.core.orderbook_heatmap.models.level3 import (
    L3Order, L3Orderbook, L3EventType, L3Side
)
from .base_l3 import L3Exchange


logger = logging.getLogger(__name__)


class BitfinexL3(L3Exchange):
    """
    Bitfinex Level 3 orderbook integration using Raw Books

    Raw Books (R0 precision) provide individual orders with unique IDs.
    Format: [ORDER_ID, PRICE, AMOUNT]
    - AMOUNT > 0: bid
    - AMOUNT < 0: ask
    - PRICE = 0: order removed
    """

    REST_API = "https://api-pub.bitfinex.com/v2"
    WS_API = "wss://api-pub.bitfinex.com/ws/2"

    def __init__(self):
        super().__init__(Exchange.COINBASE)  # Using COINBASE enum, but can extend
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._channel_id: Optional[int] = None
        self._order_ids: Set[int] = set()  # Track active orders

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
            url = f"{self.REST_API}/book/{normalized}/R0"

            if not self.session:
                self.session = aiohttp.ClientSession()

            async with self.session.get(url) as resp:
                if resp.status != 200:
                    logger.error(f"Bitfinex L3 API error: {resp.status}")
                    return None

                data = await resp.json()

                # Parse snapshot
                # Format: [[ORDER_ID, PRICE, AMOUNT], ...]
                timestamp = datetime.utcnow()
                sequence = 0  # Bitfinex doesn't provide sequence numbers

                bids = []
                asks = []

                for entry in data:
                    if len(entry) < 3:
                        continue

                    order_id = str(entry[0])
                    price = float(entry[1])
                    amount = float(entry[2])

                    if amount == 0:
                        continue  # Skip empty orders

                    # Positive amount = bid, negative = ask
                    if amount > 0:
                        bids.append(L3Order(
                            exchange="bitfinex",
                            symbol=symbol,
                            order_id=order_id,
                            side=L3Side.BID,
                            price=price,
                            size=abs(amount),
                            event_type=L3EventType.OPEN,
                            timestamp=timestamp,
                            sequence=sequence
                        ))
                        self._order_ids.add(int(order_id))
                    else:
                        asks.append(L3Order(
                            exchange="bitfinex",
                            symbol=symbol,
                            order_id=order_id,
                            side=L3Side.ASK,
                            price=price,
                            size=abs(amount),
                            event_type=L3EventType.OPEN,
                            timestamp=timestamp,
                            sequence=sequence
                        ))
                        self._order_ids.add(int(order_id))

                snapshot = L3Orderbook(
                    exchange="bitfinex",
                    symbol=symbol,
                    sequence=sequence,
                    timestamp=timestamp,
                    bids=bids,
                    asks=asks
                )

                logger.info(f"Fetched Bitfinex L3 snapshot: {len(bids)} bids, {len(asks)} asks")
                return snapshot

        except Exception as e:
            logger.error(f"Error fetching Bitfinex L3 snapshot: {e}")
            return None

    async def subscribe_l3_updates(self, symbol: str):
        """
        Subscribe to Bitfinex Raw Books WebSocket feed

        Subscribes to 'book' channel with R0 precision (raw orders).
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

                # Subscribe to Raw Books (R0)
                subscribe_msg = {
                    "event": "subscribe",
                    "channel": "book",
                    "symbol": normalized,
                    "prec": "R0",  # Raw order precision
                    "len": "100"   # Length (number of orders, can be 25, 100, 250)
                }

                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"Subscribed to Bitfinex L3 for {normalized}")

                # Process messages
                async for message in ws:
                    try:
                        data = json.loads(message)

                        # Handle subscription confirmation
                        if isinstance(data, dict):
                            event = data.get("event")

                            if event == "subscribed":
                                self._channel_id = data.get("chanId")
                                logger.info(f"Bitfinex channel {self._channel_id} subscribed")
                                continue

                            elif event == "error":
                                logger.error(f"Bitfinex error: {data.get('msg')}")
                                continue

                            elif event == "info":
                                logger.info(f"Bitfinex info: {data.get('msg')}")
                                continue

                        # Handle data messages
                        if isinstance(data, list):
                            channel_id = data[0]

                            # Verify it's our channel
                            if self._channel_id and channel_id != self._channel_id:
                                continue

                            # Skip heartbeat
                            if len(data) == 2 and data[1] == "hb":
                                continue

                            # Parse order updates
                            orders = self._parse_bitfinex_updates(data)
                            for order in orders:
                                await self._emit_l3_order(order)

                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON from Bitfinex: {e}")
                    except Exception as e:
                        logger.error(f"Error processing Bitfinex L3 message: {e}")

        except websockets.exceptions.WebSocketException as e:
            logger.error(f"Bitfinex WebSocket error: {e}")
        except Exception as e:
            logger.error(f"Error in Bitfinex L3 subscription: {e}")
        finally:
            self.ws = None
            self.is_connected = False

    def _parse_bitfinex_updates(self, data: list) -> list[L3Order]:
        """
        Parse Bitfinex WebSocket updates

        Message formats:
        - Snapshot: [CHANNEL_ID, [[ORDER_ID, PRICE, AMOUNT], ...]]
        - Update: [CHANNEL_ID, [ORDER_ID, PRICE, AMOUNT]]

        Args:
            data: Raw WebSocket message

        Returns:
            List of L3Order objects
        """
        orders = []

        try:
            if len(data) < 2:
                return orders

            payload = data[1]

            # Handle snapshot (list of lists)
            if isinstance(payload, list) and len(payload) > 0 and isinstance(payload[0], list):
                for entry in payload:
                    order = self._parse_single_order(entry)
                    if order:
                        orders.append(order)

            # Handle single update (list)
            elif isinstance(payload, list):
                order = self._parse_single_order(payload)
                if order:
                    orders.append(order)

        except Exception as e:
            logger.error(f"Error parsing Bitfinex updates: {e}")

        return orders

    def _parse_single_order(self, entry: list) -> Optional[L3Order]:
        """
        Parse a single Bitfinex order entry

        Format: [ORDER_ID, PRICE, AMOUNT]
        - ORDER_ID: unique order ID
        - PRICE: order price (0 = order removed)
        - AMOUNT: positive = bid, negative = ask

        Args:
            entry: Order data array

        Returns:
            L3Order or None
        """
        try:
            if len(entry) < 3:
                return None

            order_id = str(entry[0])
            price = float(entry[1])
            amount = float(entry[2])

            timestamp = datetime.utcnow()
            sequence = self.current_sequence
            self.current_sequence += 1

            # Determine event type
            if price == 0:
                # Order removed
                event_type = L3EventType.DONE
                side = L3Side.BID if int(order_id) in self._order_ids else L3Side.ASK

                # Remove from tracking
                self._order_ids.discard(int(order_id))

                # Use last known price (we don't have it, use 0)
                price = 0.0
                size = 0.0

            else:
                # Determine side based on amount
                side = L3Side.BID if amount > 0 else L3Side.ASK
                size = abs(amount)

                # Check if this is a new order or update
                if int(order_id) in self._order_ids:
                    event_type = L3EventType.CHANGE
                else:
                    event_type = L3EventType.OPEN
                    self._order_ids.add(int(order_id))

            order = L3Order(
                exchange="bitfinex",
                symbol=self._current_symbol or "",
                order_id=order_id,
                sequence=sequence,
                side=side,
                price=price,
                size=size,
                event_type=event_type,
                timestamp=timestamp
            )

            return order

        except Exception as e:
            logger.error(f"Error parsing Bitfinex order: {e}")
            return None

    def parse_l3_event(self, data: Dict[str, Any]) -> Optional[L3Order]:
        """
        Parse exchange-specific L3 message

        Note: Not used for Bitfinex as we handle parsing in subscribe_l3_updates

        Args:
            data: Raw message data

        Returns:
            None (handled elsewhere)
        """
        return None

    async def disconnect(self):
        """Disconnect from Bitfinex"""
        if self.ws:
            await self.ws.close()
            self.ws = None

        if self.session:
            await self.session.close()
            self.session = None

        self.is_connected = False
        self._order_ids.clear()
        logger.info("Disconnected from Bitfinex L3")

    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol for Bitfinex

        Bitfinex uses different naming:
        - BTC/USD -> tBTCUSD
        - BTC/USDT -> tBTCUST (note: UST, not USDT!)
        - ETH/USD -> tETHUSD
        - ETH/USDT -> tETHUST

        Args:
            symbol: Symbol in standard format (e.g. "BTC/USDT")

        Returns:
            Bitfinex format symbol (e.g. "tBTCUST")
        """
        # Remove slash
        normalized = symbol.replace("/", "")

        # Bitfinex uses "UST" for Tether, not "USDT"
        normalized = normalized.replace("USDT", "UST")

        # Add 't' prefix for trading pairs
        normalized = "t" + normalized

        return normalized

    # ============================================================================
    # BaseExchange abstract methods (required but not used for L3)
    # ============================================================================

    async def connect(self, symbol: str) -> bool:
        """
        Connect to Bitfinex (BaseExchange requirement)

        Note: For L3, use start_l3_stream() instead

        Args:
            symbol: Trading pair

        Returns:
            True if successful
        """
        try:
            await self.start_l3_stream(symbol)
            return self.is_connected
        except Exception as e:
            logger.error(f"Error connecting to Bitfinex: {e}")
            return False

    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100):
        """
        Get orderbook snapshot (BaseExchange requirement)

        Note: For L3, use get_l3_snapshot() instead

        Args:
            symbol: Trading pair
            limit: Not used for L3

        Returns:
            None (use get_l3_snapshot for L3 data)
        """
        logger.warning("get_orderbook_snapshot() called on L3 exchange - use get_l3_snapshot() instead")
        return None

    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """
        Handle orderbook update (BaseExchange requirement)

        Note: For L3, updates are handled in subscribe_l3_updates()

        Args:
            data: Raw data
        """
        # Not used for L3 - updates handled in subscribe_l3_updates
        pass
