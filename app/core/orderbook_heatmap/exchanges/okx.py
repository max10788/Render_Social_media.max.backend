"""
OKX Exchange Integration
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


class OKXExchange(CEXExchange):
    """OKX Exchange Integration"""
    
    REST_API = "https://www.okx.com"
    WS_API = "wss://ws.okx.com:8443/ws/v5/public"
    
    def __init__(self):
        super().__init__(Exchange.OKX)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._local_orderbook: Optional[Dict[str, Any]] = None
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu OKX WebSocket"""
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
            
            logger.info(f"Connected to OKX for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to OKX: {e}")
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
        
        logger.info("Disconnected from OKX")
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """Holt Orderbuch-Snapshot via REST"""
        try:
            normalized = self.normalize_symbol(symbol)
            url = f"{self.REST_API}/api/v5/market/books"
            params = {
                "instId": normalized,
                "sz": min(limit, 400)  # OKX max: 400
            }
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"OKX API error: {resp.status}")
                    return None
                
                data = await resp.json()
                
                if data.get("code") != "0":
                    logger.error(f"OKX API error: {data.get('msg')}")
                    return None
                
                result_list = data.get("data", [])
                if not result_list:
                    return None
                
                result = result_list[0]
                self._local_orderbook = result
                
                return self._parse_orderbook(result, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get OKX snapshot: {e}")
            return None
    
    async def _ws_loop(self, symbol: str):
        """WebSocket Loop"""
        while self.is_connected:
            try:
                async with websockets.connect(self.WS_API) as ws:
                    self.ws = ws
                    
                    # Subscribe to orderbook (books: 400 levels, books5: 5 levels)
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [{
                            "channel": "books",
                            "instId": symbol
                        }]
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Subscribed to OKX orderbook: {symbol}")
                    
                    async for message in ws:
                        if not self.is_connected:
                            break
                        
                        data = json.loads(message)
                        await self._handle_orderbook_update(data)
                        
            except websockets.ConnectionClosed:
                logger.warning("OKX WebSocket closed, reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"OKX WebSocket error: {e}")
                await asyncio.sleep(2)
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """
        Verarbeitet Orderbuch-Updates
        
        OKX sendet:
        - "snapshot": Vollständiges Orderbook
        - "update": Inkrementelle Updates
        """
        try:
            # Skip non-data messages
            if "event" in data:
                return
            
            if "arg" not in data or "data" not in data:
                return
            
            channel = data.get("arg", {}).get("channel")
            if channel != "books":
                return
            
            action = data.get("action")
            orderbook_list = data.get("data", [])
            
            if not orderbook_list:
                return
            
            orderbook_data = orderbook_list[0]
            
            if action == "snapshot":
                # Vollständiger Snapshot
                self._local_orderbook = orderbook_data
            elif action == "update":
                # Inkrementelles Update
                if self._local_orderbook:
                    self._apply_update(orderbook_data)
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
            logger.error(f"Failed to handle OKX update: {e}")
    
    def _apply_update(self, update: Dict[str, Any]):
        """Wendet Update an"""
        if not self._local_orderbook:
            return
        
        # Update bids
        if "bids" in update:
            self._apply_updates(self._local_orderbook, "bids", update["bids"])
        
        # Update asks
        if "asks" in update:
            self._apply_updates(self._local_orderbook, "asks", update["asks"])
        
        # Update checksum
        if "checksum" in update:
            self._local_orderbook["checksum"] = update["checksum"]
    
    def _apply_updates(self, local_ob: Dict, side: str, updates: list):
        """Wendet Updates auf eine Seite an"""
        if side not in local_ob:
            local_ob[side] = []
        
        # Convert to dict
        levels = {level[0]: level for level in local_ob[side]}
        
        # Apply updates: [price, quantity, liquidated_orders, order_count]
        for update in updates:
            price_str = update[0]
            qty_str = update[1]
            
            if float(qty_str) == 0:
                levels.pop(price_str, None)
            else:
                levels[price_str] = update
        
        # Convert back and sort
        local_ob[side] = list(levels.values())
        reverse = (side == "bids")
        local_ob[side].sort(key=lambda x: float(x[0]), reverse=reverse)
        local_ob[side] = local_ob[side][:400]
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> Optional[Orderbook]:
        """Parst OKX Orderbuch-Daten"""
        try:
            # Bids: [price, quantity, liquidated_orders, order_count]
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
                is_snapshot="ts" in data
            )
            
        except Exception as e:
            logger.error(f"Failed to parse OKX orderbook: {e}")
            return None
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für OKX
        BTC/USDT -> BTC-USDT
        """
        return symbol.replace("/", "-").upper()
