"""
Coinbase Exchange Integration
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


class CoinbaseExchange(CEXExchange):
    """Coinbase Exchange Integration"""
    
    REST_API = "https://api.exchange.coinbase.com"
    WS_API = "wss://ws-feed.exchange.coinbase.com"
    
    def __init__(self):
        super().__init__(Exchange.COINBASE)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._local_orderbook: Optional[Dict[str, Any]] = None
        self._sequence: Optional[int] = None
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu Coinbase WebSocket"""
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
            self._ws_task = asyncio.create_task(self._ws_loop(normalized_symbol))
            self.is_connected = True
            
            logger.info(f"Connected to Coinbase for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Coinbase: {e}")
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
        
        logger.info("Disconnected from Coinbase")
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """Holt Orderbuch-Snapshot via REST"""
        try:
            normalized = self.normalize_symbol(symbol)
            url = f"{self.REST_API}/products/{normalized}/book"
            params = {"level": 2}  # Level 2: Top 50 bids/asks
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # Coinbase requires User-Agent header
            headers = {"User-Agent": "orderbook-heatmap/1.0"}
            
            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    logger.error(f"Coinbase API error: {resp.status}")
                    return None
                
                data = await resp.json()
                
                # Store snapshot
                self._local_orderbook = {
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", [])
                }
                self._sequence = data.get("sequence")
                
                return self._parse_orderbook(data, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get Coinbase snapshot: {e}")
            return None
    
    async def _ws_loop(self, symbol: str):
        """WebSocket Loop"""
        while self.is_connected:
            try:
                async with websockets.connect(self.WS_API) as ws:
                    self.ws = ws
                    
                    # Subscribe to level2 channel
                    subscribe_msg = {
                        "type": "subscribe",
                        "product_ids": [symbol],
                        "channels": ["level2"]
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Subscribed to Coinbase orderbook: {symbol}")
                    
                    async for message in ws:
                        if not self.is_connected:
                            break
                        
                        data = json.loads(message)
                        await self._handle_orderbook_update(data)
                        
            except websockets.ConnectionClosed:
                logger.warning("Coinbase WebSocket closed, reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Coinbase WebSocket error: {e}")
                await asyncio.sleep(2)
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """
        Verarbeitet Orderbuch-Updates
        
        Coinbase sendet:
        - "snapshot": Vollständiges Orderbook
        - "l2update": Inkrementelle Updates
        """
        try:
            msg_type = data.get("type")
            
            if msg_type == "snapshot":
                # Vollständiger Snapshot
                self._local_orderbook = {
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", [])
                }
                self._sequence = None
                
            elif msg_type == "l2update":
                # Inkrementelles Update
                if not self._local_orderbook:
                    # Kein lokales Orderbook, hole Snapshot
                    snapshot = await self.get_orderbook_snapshot(self._current_symbol)
                    if snapshot:
                        await self._emit_orderbook(snapshot)
                    return
                
                # Apply changes
                changes = data.get("changes", [])
                for change in changes:
                    side, price_str, size_str = change
                    self._apply_change(side, price_str, size_str)
            
            # Parse und emittiere
            if self._local_orderbook:
                orderbook = self._parse_orderbook(self._local_orderbook, self._current_symbol)
                if orderbook:
                    await self._emit_orderbook(orderbook)
                
        except Exception as e:
            logger.error(f"Failed to handle Coinbase update: {e}")
    
    def _apply_change(self, side: str, price_str: str, size_str: str):
        """Wendet eine Änderung an"""
        if not self._local_orderbook:
            return
        
        # "buy" -> "bids", "sell" -> "asks"
        book_side = "bids" if side == "buy" else "asks"
        
        if book_side not in self._local_orderbook:
            self._local_orderbook[book_side] = []
        
        # Convert to dict
        levels = {level[0]: level for level in self._local_orderbook[book_side]}
        
        # Apply change: [price, size, num_orders]
        if float(size_str) == 0:
            levels.pop(price_str, None)
        else:
            # Find existing or create new
            if price_str in levels:
                levels[price_str][1] = size_str
            else:
                levels[price_str] = [price_str, size_str, "1"]
        
        # Convert back and sort
        self._local_orderbook[book_side] = list(levels.values())
        reverse = (book_side == "bids")
        self._local_orderbook[book_side].sort(key=lambda x: float(x[0]), reverse=reverse)
        self._local_orderbook[book_side] = self._local_orderbook[book_side][:50]
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> Optional[Orderbook]:
        """Parst Coinbase Orderbuch-Daten"""
        try:
            # Bids: [price, size, num_orders]
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
                sequence=self._sequence,
                is_snapshot=True
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Coinbase orderbook: {e}")
            return None
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Coinbase
        BTC/USDT -> BTC-USDT (but USDT might not be available, use USD)
        Note: Coinbase uses USD not USDT in many cases
        """
        # Coinbase typically uses USD, not USDT
        normalized = symbol.replace("/", "-").upper()
        # Common conversions
        normalized = normalized.replace("-USDT", "-USD")
        return normalized
