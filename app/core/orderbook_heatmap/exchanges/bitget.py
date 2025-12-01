"""
Bitget Exchange Integration - FULLY FIXED VERSION
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
    """Bitget Exchange Integration"""
    
    REST_API = "https://api.bitget.com"
    WS_API = "wss://ws.bitget.com/spot/v1/stream"
    
    MAX_RECONNECT_ATTEMPTS = 5
    RECONNECT_DELAY = 5  # Sekunden
    MESSAGE_TIMEOUT = 30  # Sekunden ohne Nachricht bevor reconnect
    
    def __init__(self):
        super().__init__(Exchange.BITGET)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._reconnect_count = 0
        self._successful_messages = 0  # ← NEU: Zählt erfolgreiche Messages
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu Bitget WebSocket"""
        try:
            self._current_symbol = symbol
            normalized_symbol = self.normalize_symbol(symbol)
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # Hole initialen Snapshot
            snapshot = await self.get_orderbook_snapshot(symbol)
            if snapshot:
                await self._emit_orderbook(snapshot)
            
            # Reset Reconnect Counter beim ersten Connect
            self._reconnect_count = 0
            
            # Starte WebSocket
            self._ws_task = asyncio.create_task(self._ws_loop(normalized_symbol))
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
        """Holt Orderbuch-Snapshot via REST"""
        try:
            normalized = self.normalize_symbol(symbol)
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
                return self._parse_orderbook(data, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get Bitget snapshot: {e}")
            return None
    
    async def _ws_loop(self, symbol: str):
        """
        WebSocket Loop mit korrektem Reconnect-Counter
        """
        while self.is_connected:
            # Prüfe Reconnect-Limit VOR dem Connect-Versuch
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
                    
                    # Subscribe zu Orderbook
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [{
                            "instType": "sp",
                            "channel": "books",
                            "instId": symbol
                        }]
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    
                    logger.info(f"Bitget WebSocket subscribed: {symbol}")
                    
                    # Message-Loop
                    while self.is_connected:
                        try:
                            # Warte auf Nachricht mit Timeout
                            message = await asyncio.wait_for(
                                ws.recv(),
                                timeout=self.MESSAGE_TIMEOUT
                            )
                            
                            data = json.loads(message)
                            
                            # Ping/Pong handling
                            if data.get("event") == "ping":
                                await ws.send(json.dumps({"event": "pong"}))
                                continue
                            
                            # Process message
                            await self._handle_orderbook_update(data)
                            
                            # ✅ KRITISCH: Reset Counter nach erfolgreicher Message!
                            self._successful_messages += 1
                            if self._successful_messages >= 3:
                                # Nach 3 erfolgreichen Messages: Connection ist stabil
                                if self._reconnect_count > 0:
                                    logger.info(
                                        f"Bitget WebSocket stable after {self._successful_messages} messages. "
                                        f"Resetting reconnect counter."
                                    )
                                self._reconnect_count = 0
                                self._successful_messages = 0  # Reset für nächstes Mal
                            
                        except asyncio.TimeoutError:
                            logger.warning(
                                f"Bitget WebSocket: No message received for {self.MESSAGE_TIMEOUT}s"
                            )
                            # Check if WebSocket is still open
                            if ws.closed:
                                logger.warning("Bitget WebSocket is closed, breaking loop...")
                                break
                            # Sonst: Continue waiting
                            continue
                        
                        except json.JSONDecodeError as e:
                            logger.error(f"Bitget WebSocket: Invalid JSON: {e}")
                            continue
                    
            except websockets.ConnectionClosed as e:
                logger.warning(
                    f"Bitget WebSocket closed (code: {e.code}, reason: {e.reason}). "
                    f"Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS}"
                )
                
                # Exponentielles Backoff
                if self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS:
                    delay = min(self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60)
                    logger.info(f"Waiting {delay}s before reconnecting...")
                    await asyncio.sleep(delay)
                
            except Exception as e:
                logger.error(
                    f"Bitget WebSocket error: {e}. "
                    f"Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS}"
                )
                
                # Exponentielles Backoff
                if self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS:
                    delay = min(self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60)
                    await asyncio.sleep(delay)
        
        logger.info(f"Bitget WebSocket loop ended (reconnect_count: {self._reconnect_count})")
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """Verarbeitet Orderbuch-Updates"""
        try:
            if data.get("action") != "snapshot" and data.get("action") != "update":
                return
            
            orderbook_data = data.get("data", [{}])[0]
            orderbook = self._parse_orderbook(orderbook_data, self._current_symbol)
            if orderbook:
                await self._emit_orderbook(orderbook)
                
        except Exception as e:
            logger.error(f"Failed to handle Bitget update: {e}")
    
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
                is_snapshot=data.get("action") == "snapshot"
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Bitget orderbook: {e}")
            return None
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Bitget
        BTC/USDT -> BTCUSDT_SPBL
        """
        base_symbol = symbol.replace("/", "").upper()
        return f"{base_symbol}_SPBL"
