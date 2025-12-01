"""
Kraken Exchange Integration
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


class KrakenExchange(CEXExchange):
    """Kraken Exchange Integration"""
    
    REST_API = "https://api.kraken.com"
    WS_API = "wss://ws.kraken.com"
    
    # Kraken Symbol Mappings
    SYMBOL_MAP = {
        "BTC/USDT": "XBT/USDT",
        "BTC/USD": "XBT/USD",
        "ETH/USDT": "ETH/USDT",
        "ETH/USD": "ETH/USD",
    }
    
    def __init__(self):
        super().__init__(Exchange.KRAKEN)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu Kraken WebSocket"""
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
            
            logger.info(f"Connected to Kraken for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kraken: {e}")
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
        
        logger.info("Disconnected from Kraken")
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """Holt Orderbuch-Snapshot via REST"""
        try:
            normalized = self.normalize_symbol(symbol)
            url = f"{self.REST_API}/0/public/Depth"
            params = {"pair": normalized, "count": limit}
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"Kraken API error: {resp.status}")
                    return None
                
                result = await resp.json()
                if result.get("error"):
                    logger.error(f"Kraken API error: {result['error']}")
                    return None
                
                # Kraken returns data with pair name as key
                data = list(result.get("result", {}).values())[0]
                return self._parse_orderbook(data, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get Kraken snapshot: {e}")
            return None
    
    async def _ws_loop(self, symbol: str):
        """WebSocket Loop"""
        while self.is_connected:
            try:
                async with websockets.connect(self.WS_API) as ws:
                    self.ws = ws
                    
                    # Subscribe zu Orderbook
                    subscribe_msg = {
                        "event": "subscribe",
                        "pair": [symbol],
                        "subscription": {
                            "name": "book",
                            "depth": 100
                        }
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    
                    logger.info(f"Kraken WebSocket subscribed: {symbol}")
                    
                    async for message in ws:
                        if not self.is_connected:
                            break
                        
                        data = json.loads(message)
                        
                        # Ping/Pong handling
                        if isinstance(data, dict) and data.get("event") == "heartbeat":
                            continue
                        
                        await self._handle_orderbook_update(data)
                        
            except websockets.ConnectionClosed:
                logger.warning("Kraken WebSocket closed, reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Kraken WebSocket error: {e}")
                await asyncio.sleep(2)
    
    async def _handle_orderbook_update(self, data: Any):
        """Verarbeitet Orderbuch-Updates"""
        try:
            # Kraken sendet Arrays für Updates
            if not isinstance(data, list) or len(data) < 2:
                return
            
            # Format: [channelID, data, channelName, pair]
            if len(data) >= 4:
                orderbook_data = data[1]
                
                # Unterscheide zwischen Snapshot und Update
                if "as" in orderbook_data and "bs" in orderbook_data:
                    # Snapshot
                    parsed_data = {
                        "asks": orderbook_data.get("as", []),
                        "bids": orderbook_data.get("bs", [])
                    }
                else:
                    # Update - sammle nur die Updates
                    return  # Für Simplicity nur Snapshots verarbeiten
                
                orderbook = self._parse_orderbook(parsed_data, self._current_symbol)
                if orderbook:
                    orderbook.is_snapshot = True
                    await self._emit_orderbook(orderbook)
                    
        except Exception as e:
            logger.error(f"Failed to handle Kraken update: {e}")
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> Optional[Orderbook]:
        """Parst Kraken Orderbuch-Daten"""
        try:
            # Bids - Format: [[price, volume, timestamp], ...]
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
                asks=asks
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Kraken orderbook: {e}")
            return None
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Kraken
        BTC/USDT -> XBT/USDT (Kraken verwendet XBT statt BTC)
        """
        # Nutze Mapping falls vorhanden
        if symbol in self.SYMBOL_MAP:
            return self.SYMBOL_MAP[symbol]
        return symbol
