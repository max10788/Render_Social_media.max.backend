"""
Bitget Exchange Integration - FIXED VERSION
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
    
    # ← FIXED: Reconnect-Limits hinzugefügt
    MAX_RECONNECT_ATTEMPTS = 5
    RECONNECT_DELAY = 5  # Sekunden
    
    def __init__(self):
        super().__init__(Exchange.BITGET)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._reconnect_count = 0  # ← FIXED: Reconnect-Counter
        
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
        WebSocket Loop mit Reconnect-Limit
        FIXED: Verhindert endlose Reconnect-Schleife
        """
        self._reconnect_count = 0  # Reset counter
        
        while self.is_connected and self._reconnect_count < self.MAX_RECONNECT_ATTEMPTS:
            try:
                logger.info(f"Bitget WebSocket connecting... (attempt {self._reconnect_count + 1}/{self.MAX_RECONNECT_ATTEMPTS})")
                
                async with websockets.connect(
                    self.WS_API,
                    ping_interval=20,  # ← FIXED: Ping alle 20 Sekunden
                    ping_timeout=10,    # ← FIXED: Timeout nach 10 Sekunden
                    close_timeout=5     # ← FIXED: Close Timeout
                ) as ws:
                    self.ws = ws
                    self._reconnect_count = 0  # Reset bei erfolgreicher Verbindung
                    
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
                    
                    # ← FIXED: Explizite Message-Loop mit Timeout
                    while self.is_connected:
                        try:
                            # Warte auf Nachricht mit Timeout
                            message = await asyncio.wait_for(
                                ws.recv(),
                                timeout=30.0  # 30 Sekunden Timeout
                            )
                            
                            data = json.loads(message)
                            
                            # Ping/Pong handling
                            if data.get("event") == "ping":
                                await ws.send(json.dumps({"event": "pong"}))
                                continue
                            
                            await self._handle_orderbook_update(data)
                            
                        except asyncio.TimeoutError:
                            logger.warning("Bitget WebSocket: No message received for 30s, checking connection...")
                            # Prüfe ob WebSocket noch offen ist
                            if ws.closed:
                                logger.warning("Bitget WebSocket is closed, reconnecting...")
                                break
                            # Sonst: Warte weiter auf Nachrichten
                            continue
                        
                        except json.JSONDecodeError as e:
                            logger.error(f"Bitget WebSocket: Invalid JSON: {e}")
                            continue
                    
            except websockets.ConnectionClosed as e:
                self._reconnect_count += 1
                logger.warning(
                    f"Bitget WebSocket closed (code: {e.code}, reason: {e.reason}). "
                    f"Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS}"
                )
                
                if self._reconnect_count >= self.MAX_RECONNECT_ATTEMPTS:
                    logger.error(f"Bitget WebSocket: Max reconnect attempts reached. Giving up.")
                    self.is_connected = False
                    break
                
                # ← FIXED: Exponentielles Backoff
                delay = min(self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60)
                logger.info(f"Waiting {delay}s before reconnecting...")
                await asyncio.sleep(delay)
                
            except Exception as e:
                self._reconnect_count += 1
                logger.error(
                    f"Bitget WebSocket error: {e}. "
                    f"Reconnect attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS}"
                )
                
                if self._reconnect_count >= self.MAX_RECONNECT_ATTEMPTS:
                    logger.error(f"Bitget WebSocket: Max reconnect attempts reached. Giving up.")
                    self.is_connected = False
                    break
                
                delay = min(self.RECONNECT_DELAY * (2 ** (self._reconnect_count - 1)), 60)
                await asyncio.sleep(delay)
        
        logger.info("Bitget WebSocket loop ended")
    
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
