"""
Deribit Exchange Integration
Note: Deribit is primarily derivatives (futures/options) but also has spot
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


class DeribitExchange(CEXExchange):
    """Deribit Exchange Integration"""
    
    REST_API = "https://www.deribit.com/api/v2"
    WS_API = "wss://www.deribit.com/ws/api/v2"
    
    def __init__(self):
        super().__init__(Exchange.DERIBIT)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._local_orderbook: Optional[Dict[str, Any]] = None
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu Deribit WebSocket"""
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
            
            logger.info(f"Connected to Deribit for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Deribit: {e}")
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
        
        logger.info("Disconnected from Deribit")
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """Holt Orderbuch-Snapshot via REST"""
        try:
            normalized = self.normalize_symbol(symbol)
            url = f"{self.REST_API}/public/get_order_book"
            params = {
                "instrument_name": normalized,
                "depth": min(limit, 10000)  # Deribit max: 10000
            }
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"Deribit API error: {resp.status}")
                    return None
                
                data = await resp.json()
                
                if "error" in data:
                    logger.error(f"Deribit API error: {data['error']}")
                    return None
                
                result = data.get("result", {})
                self._local_orderbook = result
                
                return self._parse_orderbook(result, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get Deribit snapshot: {e}")
            return None
    
    async def _ws_loop(self, symbol: str):
        """WebSocket Loop"""
        while self.is_connected:
            try:
                async with websockets.connect(self.WS_API) as ws:
                    self.ws = ws
                    
                    # Subscribe to orderbook
                    # Use book.{instrument}.100ms for high-frequency updates
                    subscribe_msg = {
                        "jsonrpc": "2.0",
                        "method": "public/subscribe",
                        "params": {
                            "channels": [f"book.{symbol}.100ms"]
                        },
                        "id": 1
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Subscribed to Deribit orderbook: {symbol}")
                    
                    async for message in ws:
                        if not self.is_connected:
                            break
                        
                        data = json.loads(message)
                        await self._handle_orderbook_update(data)
                        
            except websockets.ConnectionClosed:
                logger.warning("Deribit WebSocket closed, reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Deribit WebSocket error: {e}")
                await asyncio.sleep(2)
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """
        Verarbeitet Orderbuch-Updates
        
        Deribit sendet:
        - "snapshot": Vollständiges Orderbook
        - "change": Inkrementelle Updates
        """
        try:
            # Check if this is a notification
            if "params" not in data:
                return
            
            params = data["params"]
            if "data" not in params:
                return
            
            orderbook_data = params["data"]
            update_type = orderbook_data.get("type")
            
            if update_type == "snapshot":
                # Vollständiger Snapshot
                self._local_orderbook = orderbook_data
            elif update_type == "change":
                # Inkrementelles Update
                if self._local_orderbook:
                    self._apply_change(orderbook_data)
                else:
                    # Kein lokales Orderbook, hole Snapshot
                    snapshot = await self.get_orderbook_snapshot(self._current_symbol)
                    if snapshot:
                        await self._emit_orderbook(snapshot)
                    return
            
            # Parse und emittiere
            if self._local_orderbook:
                orderbook = self._parse_orderbook(self._local_orderbook, self._current_symbol)
                if orderbook:
                    await self._emit_orderbook(orderbook)
                
        except Exception as e:
            logger.error(f"Failed to handle Deribit update: {e}")
    
    def _apply_change(self, change: Dict[str, Any]):
        """Wendet Change-Update an"""
        if not self._local_orderbook:
            return
        
        # Update bids
        if "bids" in change:
            self._apply_updates(self._local_orderbook, "bids", change["bids"])
        
        # Update asks
        if "asks" in change:
            self._apply_updates(self._local_orderbook, "asks", change["asks"])
    
    def _apply_updates(self, local_ob: Dict, side: str, updates: list):
        """
        Wendet Updates auf eine Seite an
        
        Deribit format: ["new"|"change"|"delete", price, amount]
        """
        if side not in local_ob:
            local_ob[side] = []
        
        # Convert to dict: price -> [action, price, amount]
        levels = {}
        for level in local_ob[side]:
            if len(level) >= 2:
                levels[level[0]] = level
        
        # Apply updates
        for update in updates:
            if len(update) < 3:
                continue
            
            action, price, amount = update[0], update[1], update[2]
            
            if action == "delete" or amount == 0:
                levels.pop(price, None)
            else:
                # "new" or "change"
                levels[price] = ["change", price, amount]
        
        # Convert back and sort
        local_ob[side] = list(levels.values())
        # Deribit uses [action, price, amount] format - extract price for sorting
        reverse = (side == "bids")
        local_ob[side].sort(key=lambda x: float(x[1]), reverse=reverse)
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> Optional[Orderbook]:
        """Parst Deribit Orderbuch-Daten"""
        try:
            # Bids: [action, price, amount] or [price, amount]
            bids_data = data.get("bids", [])
            bids = OrderbookSide(
                levels=[
                    OrderbookLevel(
                        price=float(bid[1]) if len(bid) == 3 else float(bid[0]),
                        quantity=float(bid[2]) if len(bid) == 3 else float(bid[1])
                    )
                    for bid in bids_data
                ]
            )
            
            # Asks
            asks_data = data.get("asks", [])
            asks = OrderbookSide(
                levels=[
                    OrderbookLevel(
                        price=float(ask[1]) if len(ask) == 3 else float(ask[0]),
                        quantity=float(ask[2]) if len(ask) == 3 else float(ask[1])
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
                is_snapshot="timestamp" in data
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Deribit orderbook: {e}")
            return None
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Deribit
        BTC/USD -> BTC-PERPETUAL or BTC_USDC (spot)
        
        Note: Deribit uses specific formats:
        - Perpetuals: BTC-PERPETUAL, ETH-PERPETUAL
        - Futures: BTC-30JUN23, ETH-30JUN23
        - Options: BTC-30JUN23-50000-C
        - Spot: BTC_USDC, ETH_USDC
        """
        # For spot trading, convert to Deribit's format
        if "/" in symbol:
            parts = symbol.split("/")
            if len(parts) == 2:
                base, quote = parts
                # Deribit spot uses underscore
                return f"{base.upper()}_{quote.upper()}"
        
        # If already in Deribit format, return as-is
        return symbol.upper()
