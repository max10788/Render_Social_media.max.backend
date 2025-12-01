"""
Binance Exchange Integration
"""
import asyncio
import json
import logging
from typing import Optional, Dict, Any
import aiohttp
import websockets

from .base import CEXExchange
from app.core.orderbook_heatmap.models.orderbook import (
    Orderbook, OrderbookLevel, OrderbookSide, Exchange
)


logger = logging.getLogger(__name__)


class BinanceExchange(CEXExchange):
    """Binance Exchange Integration"""
    
    REST_API = "https://api.binance.com"
    WS_API = "wss://stream.binance.com:9443/ws"
    
    def __init__(self):
        super().__init__(Exchange.BINANCE)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu Binance WebSocket"""
        try:
            self._current_symbol = symbol
            normalized_symbol = self.normalize_symbol(symbol)
            
            # Erstelle HTTP Session
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # Hole initialen Snapshot
            snapshot = await self.get_orderbook_snapshot(symbol)
            if snapshot:
                await self._emit_orderbook(snapshot)
            
            # Starte WebSocket
            stream = f"{normalized_symbol.lower()}@depth@100ms"
            ws_url = f"{self.WS_API}/{stream}"
            
            self._ws_task = asyncio.create_task(self._ws_loop(ws_url))
            self.is_connected = True
            
            logger.info(f"Connected to Binance for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Binance: {e}")
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
        
        logger.info("Disconnected from Binance")
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """Holt Orderbuch-Snapshot via REST"""
        try:
            normalized = self.normalize_symbol(symbol)
            url = f"{self.REST_API}/api/v3/depth"
            params = {"symbol": normalized, "limit": limit}
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"Binance API error: {resp.status}")
                    return None
                
                data = await resp.json()
                return self._parse_orderbook(data, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get Binance snapshot: {e}")
            return None
    
    async def _ws_loop(self, ws_url: str):
        """WebSocket Loop"""
        while self.is_connected:
            try:
                async with websockets.connect(ws_url) as ws:
                    self.ws = ws
                    logger.info(f"WebSocket connected: {ws_url}")
                    
                    async for message in ws:
                        if not self.is_connected:
                            break
                        
                        data = json.loads(message)
                        await self._handle_orderbook_update(data)
                        
            except websockets.ConnectionClosed:
                logger.warning("Binance WebSocket closed, reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(2)
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """Verarbeitet Orderbuch-Updates"""
        try:
            if "e" not in data or data["e"] != "depthUpdate":
                return
            
            orderbook = self._parse_orderbook(data, self._current_symbol)
            if orderbook:
                await self._emit_orderbook(orderbook)
                
        except Exception as e:
            logger.error(f"Failed to handle Binance update: {e}")
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> Optional[Orderbook]:
        """Parst Binance Orderbuch-Daten"""
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
                is_snapshot="lastUpdateId" in data
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Binance orderbook: {e}")
            return None
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Binance
        BTC/USDT -> BTCUSDT
        """
        return symbol.replace("/", "").upper()
