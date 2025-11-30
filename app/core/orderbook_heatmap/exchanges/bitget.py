"""
Bitget Exchange Integration
"""
import asyncio
import json
import logging
from typing import Optional, Dict, Any
import aiohttp
import websockets

from .base import CEXExchange
from ..models.orderbook import (
    Orderbook, OrderbookLevel, OrderbookSide, Exchange
)


logger = logging.getLogger(__name__)


class BitgetExchange(CEXExchange):
    """Bitget Exchange Integration"""
    
    REST_API = "https://api.bitget.com"
    WS_API = "wss://ws.bitget.com/spot/v1/stream"
    
    def __init__(self):
        super().__init__(Exchange.BITGET)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        
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
        """WebSocket Loop"""
        while self.is_connected:
            try:
                async with websockets.connect(self.WS_API) as ws:
                    self.ws = ws
                    
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
                    
                    async for message in ws:
                        if not self.is_connected:
                            break
                        
                        data = json.loads(message)
                        
                        # Ping/Pong handling
                        if data.get("event") == "ping":
                            await ws.send(json.dumps({"event": "pong"}))
                            continue
                        
                        await self._handle_orderbook_update(data)
                        
            except websockets.ConnectionClosed:
                logger.warning("Bitget WebSocket closed, reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Bitget WebSocket error: {e}")
                await asyncio.sleep(2)
    
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
