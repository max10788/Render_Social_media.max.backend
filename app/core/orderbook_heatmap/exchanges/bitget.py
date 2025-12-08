"""
Bitget Exchange Integration - COMPLETELY FIXED VERSION
All 4 critical bugs resolved:
1. instType: "SPOT" (uppercase, not "sp")
2. WebSocket symbol: "BTCUSDT" (no _SPBL suffix)
3. Ping/Pong: String format, not JSON
4. WebSocket URL: V2 API endpoint
"""
import asyncio
import json
import logging
from typing import Optional, Dict, Any
import aiohttp
import websockets

from app.core.orderbook_heatmap.exchanges.base import CEXExchange
from app.core.orderbook_heatmap.models.orderbook import (
    Orderbook, OrderbookLevel, OrderbookSide, Exchange
)


logger = logging.getLogger(__name__)


class BitgetExchange(CEXExchange):
    """Bitget Exchange Integration - Fully Fixed"""
    
    # REST API - V1 (funktioniert mit _SPBL)
    REST_API = "https://api.bitget.com"
    
    # WebSocket API - V2 (aktuellere, stabilere Version)
    WS_API = "wss://ws.bitget.com/v2/ws/public"
    
    MAX_RECONNECT_ATTEMPTS = 5
    RECONNECT_DELAY = 5  # Sekunden
    MESSAGE_TIMEOUT = 30  # Sekunden ohne Nachricht bevor reconnect
    
    def __init__(self):
        super().__init__(Exchange.BITGET)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._reconnect_count = 0
        self._successful_messages = 0
        
        # Lokales Orderbook für inkrementelle Updates
        self._local_orderbook: Optional[Dict[str, Any]] = None
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu Bitget WebSocket"""
        try:
            self._current_symbol = symbol
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # Hole initialen Snapshot
            snapshot = await self.get_orderbook_snapshot(symbol)
            if snapshot:
                await self._emit_orderbook(snapshot)
            
            # Reset Reconnect Counter beim ersten Connect
            self._reconnect_count = 0
            
            # Starte WebSocket
            self._ws_task = asyncio.create_task(self._ws_loop(symbol))
            self.is_connected = True
            
            logger.info(f"Connected to Bitget for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Bitget: {e}")
            return False
    
    async def disconnect(self):
        """Trennt Verbindung"""
        self.is_connected = False
        
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        if self.ws:
            await self.ws.close()
        
        if self.session:
            await self.session.close()
            self.session = None
        
        logger.info("Disconnected from Bitget")
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """
        Holt Orderbuch-Snapshot via REST API
        REST API verwendet V1 Format: BTCUSDT_SPBL
        """
        try:
            # REST API: Verwende V1 Format mit _SPBL
            normalized = self.normalize_symbol_rest(symbol)
            url = f"{self.REST_API}/api/spot/v1/market/depth"
            params = {"symbol": normalized, "type": "step0", "limit": limit}
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"Bitget API error: {resp.status}")
                    return None
                
                result = await resp.json()
                if result.get("code") != "00000":
                    logger.error(f"Bitget API error: {result.get('msg')}")
                    return None
                
                data = result.get("data", {})
                
                # Store local orderbook
                self._local_orderbook = data
                
                return self._parse_orderbook(data, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get Bitget snapshot: {e}")
            return None
    
    async def _ws_loop(self, symbol: str):
        """
        WebSocket Loop mit korrekter V2 API Implementation
        """
        # WebSocket: Verwende V2 Format ohne _SPBL
        ws_symbol = self.normalize_symbol_ws(symbol)
        
        while self.is_connected:
            # Prüfe Reconnect-Limit
            if self._reconnect_count >= self.MAX_RECONNECT_ATTEMPTS:
                logger.error(
                    f"Bitget WebSocket: Max reconnect attempts ({self.MAX_RECONNECT_ATTEMPTS}) reached. "
                    f"Giving up."
                )
                self.is_connected = False
                break
            
            try:
                # Inkrementiere Counter VOR dem Connect
                self._reconnect_count += 1
                logger.info(
                    f"Bitget WebSocket connecting... "
                    f"(attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS})"
                )
                
                async with websockets.connect(
                    self.WS_API,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5
                ) as ws:
                    self.ws = ws
                    self._successful_messages = 0
                    
                    # Subscribe zu Orderbook - V2 API Format
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [{
                            "instType": "SPOT",  # ✅ FIXED: Uppercase!
                            "channel": "books",
                            "instId": ws_symbol   # ✅ FIXED: Ohne _SPBL
                        }]
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    
                    logger.info(f"Bitget WebSocket subscribed: {ws_symbol}")
                    
                    # Message-Loop
                    while self.is_connected:
                        try:
                            # Warte auf Nachricht mit Timeout
                            message = await asyncio.wait_for(
                                ws.recv(),
                                timeout=self.MESSAGE_TIMEOUT
                            )
                            
                            # ✅ FIXED: Ping/Pong handling
                            # Bitget sendet "ping" als String, nicht als JSON
                            if message == "ping":
                                await ws.send("pong")
                                continue
                            
                            # Parse JSON für andere Messages
                            try:
                                data = json.loads(message)
                            except json.JSONDecodeError:
                                # Könnte "pong" sein oder andere String-Message
                                if message == "pong":
                                    continue
                                logger.warning(f"Bitget WebSocket: Non-JSON message: {message}")
                                continue
                            
                            # Handle subscription confirmation
                            if data.get("event") == "subscribe":
                                logger.info(f"Bitget subscription confirmed: {data}")
                                continue
                            
                            # Process orderbook update
                            await self._handle_orderbook_update(data)
                            
                            # ✅ Reset Counter nach erfolgreichen Messages
                            self._successful_messages += 1
                            if self._successful_messages >= 3:
                                if self._reconnect_count > 0:
                                    logger.info(
                                        f"Bitget WebSocket stable after {self._successful_messages} messages. "
                                        f"Resetting reconnect counter."
                                    )
                                self._reconnect_count = 0
                                self._successful_messages = 0
                            
                        except asyncio.TimeoutError:
                            logger.warning(
                                f"Bitget WebSocket: No message received for {self.MESSAGE_TIMEOUT}s"
                            )
                            if ws.closed:
                                logger.warning("Bitget WebSocket is closed, breaking loop...")
                                break
                            continue
                    
            except websockets.ConnectionClosed as e:
                logger.warning(
                    f"Bitget WebSocket closed (code: {e.code}, reason: {e.reason}). "
                    f"Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS}"
                )
                
                if self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS:
                    delay = min(self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60)
                    logger.info(f"Waiting {delay}s before reconnecting...")
                    await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(
                    f"Bitget WebSocket error: {e}. "
                    f"Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS}"
                )
                
                if self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS:
                    delay = min(self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60)
                    await asyncio.sleep(delay)
        
        logger.info(f"Bitget WebSocket loop ended (reconnect_count: {self._reconnect_count})")
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """
        Verarbeitet Orderbuch-Updates von V2 WebSocket API
        
        Bitget V2 sendet:
        - action: "snapshot" → Komplettes Orderbook
        - action: "update" → Inkrementelle Updates
        """
        try:
            action = data.get("action")
            
            if action not in ["snapshot", "update"]:
                return
            
            orderbook_data = data.get("data", [{}])[0]
            
            if action == "snapshot":
                # Store snapshot as local orderbook
                self._local_orderbook = orderbook_data
                orderbook = self._parse_orderbook(orderbook_data, self._current_symbol)
            else:
                # Apply incremental updates
                if self._local_orderbook:
                    # Update bids
                    if "bids" in orderbook_data:
                        self._apply_updates(self._local_orderbook, "bids", orderbook_data["bids"])
                    
                    # Update asks
                    if "asks" in orderbook_data:
                        self._apply_updates(self._local_orderbook, "asks", orderbook_data["asks"])
                    
                    orderbook = self._parse_orderbook(self._local_orderbook, self._current_symbol)
                else:
                    # No local orderbook, fetch snapshot
                    logger.warning("No local orderbook, fetching snapshot...")
                    orderbook = await self.get_orderbook_snapshot(self._current_symbol)
            
            if orderbook:
                await self._emit_orderbook(orderbook)
                
        except Exception as e:
            logger.error(f"Failed to handle Bitget update: {e}")
    
    def _apply_updates(self, local_ob: Dict, side: str, updates: list):
        """
        Wendet inkrementelle Updates an
        
        Args:
            local_ob: Lokales Orderbook
            side: "bids" oder "asks"
            updates: Liste von [price, qty] Updates
        """
        if side not in local_ob:
            local_ob[side] = []
        
        # Convert to dict for easier updates
        levels = {float(level[0]): float(level[1]) for level in local_ob[side]}
        
        # Apply updates
        for price_str, qty_str in updates:
            price = float(price_str)
            qty = float(qty_str)
            
            if qty == 0:
                # Remove level
                levels.pop(price, None)
            else:
                # Update/add level
                levels[price] = qty
        
        # Convert back to list and sort
        local_ob[side] = [[str(p), str(q)] for p, q in levels.items()]
        
        # Sort: bids descending, asks ascending
        reverse = (side == "bids")
        local_ob[side].sort(key=lambda x: float(x[0]), reverse=reverse)
        
        # Keep only top 100
        local_ob[side] = local_ob[side][:100]
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> Optional[Orderbook]:
        """Parst Bitget Orderbuch-Daten"""
        try:
            # Bids
            bids_data = data.get("bids", [])
            bids = OrderbookSide(
                levels=[
                    OrderbookLevel(
                        price=float(bid[0]),
                        quantity=float(bid[1])
                    )
                    for bid in bids_data
                ]
            )
            
            # Asks
            asks_data = data.get("asks", [])
            asks = OrderbookSide(
                levels=[
                    OrderbookLevel(
                        price=float(ask[0]),
                        quantity=float(ask[1])
                    )
                    for ask in asks_data
                ]
            )
            
            return Orderbook(
                exchange=self.exchange,
                exchange_type=self.exchange_type,
                symbol=symbol,
                bids=bids,
                asks=asks,
                is_snapshot=data.get("action") == "snapshot" if "action" in data else True
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Bitget orderbook: {e}")
            return None
    
    def normalize_symbol_rest(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Bitget REST API (V1)
        BTC/USDT -> BTCUSDT_SPBL
        """
        base_symbol = symbol.replace("/", "").upper()
        return f"{base_symbol}_SPBL"
    
    def normalize_symbol_ws(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Bitget WebSocket API (V2)
        BTC/USDT -> BTCUSDT (ohne _SPBL!)
        """
        return symbol.replace("/", "").upper()
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Legacy method - verwendet REST format
        """
        return self.normalize_symbol_rest(symbol)
