"""
Bybit Exchange Integration
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


class BybitExchange(CEXExchange):
    """Bybit Exchange Integration"""
    
    REST_API = "https://api.bybit.com"
    WS_API = "wss://stream.bybit.com/v5/public/spot"
    
    def __init__(self):
        super().__init__(Exchange.BYBIT)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._current_symbol: Optional[str] = None
        self._local_orderbook: Optional[Dict[str, Any]] = None
        
    async def connect(self, symbol: str) -> bool:
        """Verbindet zu Bybit WebSocket"""
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
            
            logger.info(f"Connected to Bybit for {symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Bybit: {e}")
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
        
        logger.info("Disconnected from Bybit")
    
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """Holt Orderbuch-Snapshot via REST"""
        try:
            normalized = self.normalize_symbol(symbol)
            url = f"{self.REST_API}/v5/market/orderbook"
            params = {
                "category": "spot",
                "symbol": normalized,
                "limit": min(limit, 200)  # Bybit max: 200
            }
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"Bybit API error: {resp.status}")
                    return None
                
                data = await resp.json()
                
                if data.get("retCode") != 0:
                    logger.error(f"Bybit API error: {data.get('retMsg')}")
                    return None
                
                result = data.get("result", {})
                self._local_orderbook = result
                
                return self._parse_orderbook(result, symbol)
                
        except Exception as e:
            logger.error(f"Failed to get Bybit snapshot: {e}")
            return None
    
    async def _ws_loop(self, symbol: str):
        """WebSocket Loop"""
        while self.is_connected:
            try:
                async with websockets.connect(self.WS_API) as ws:
                    self.ws = ws
                    
                    # Subscribe to orderbook
                    subscribe_msg = {
                        "op": "subscribe",
                        "args": [f"orderbook.50.{symbol}"]
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Subscribed to Bybit orderbook: {symbol}")
                    
                    async for message in ws:
                        if not self.is_connected:
                            break
                        
                        data = json.loads(message)
                        
                        # Skip ping/pong
                        if data.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        
                        await self._handle_orderbook_update(data)
                        
            except websockets.ConnectionClosed:
                logger.warning("Bybit WebSocket closed, reconnecting...")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Bybit WebSocket error: {e}")
                await asyncio.sleep(2)
    
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """
        Verarbeitet Orderbuch-Updates
        
        Bybit sendet:
        - "snapshot": Vollständiges Orderbook
        - "delta": Inkrementelle Updates
        """
        try:
            topic = data.get("topic", "")
            if "orderbook" not in topic:
                return
            
            msg_type = data.get("type")
            orderbook_data = data.get("data", {})
            
            if msg_type == "snapshot":
                # Vollständiger Snapshot
                self._local_orderbook = orderbook_data
            elif msg_type == "delta":
                # Inkrementelles Update
                if self._local_orderbook:
                    self._apply_delta(orderbook_data)
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
            logger.error(f"Failed to handle Bybit update: {e}")
    
    def _apply_delta(self, delta: Dict[str, Any]):
        """Wendet Delta-Updates an"""
        if not self._local_orderbook:
            return
        
        # Update bids
        if "b" in delta:
            self._apply_updates(self._local_orderbook, "b", delta["b"])
        
        # Update asks
        if "a" in delta:
            self._apply_updates(self._local_orderbook, "a", delta["a"])
        
        # Update sequence
        if "u" in delta:
            self._local_orderbook["u"] = delta["u"]
    
    def _apply_updates(self, local_ob: Dict, side: str, updates: list):
        """Wendet Updates auf eine Seite an"""
        if side not in local_ob:
            local_ob[side] = []
        
        # Convert to dict
        levels = {level[0]: level[1] for level in local_ob[side]}
        
        # Apply updates
        for price_str, qty_str in updates:
            if float(qty_str) == 0:
                levels.pop(price_str, None)
            else:
                levels[price_str] = qty_str
        
        # Convert back and sort
        local_ob[side] = [[p, q] for p, q in levels.items()]
        reverse = (side == "b")
        local_ob[side].sort(key=lambda x: float(x[0]), reverse=reverse)
        local_ob[side] = local_ob[side][:50]
    
    def _parse_orderbook(self, data: Dict[str, Any], symbol: str) -> Optional[Orderbook]:
        """Parst Bybit Orderbuch-Daten"""
        try:
            # Bids
            bids_data = data.get("b", [])
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
            asks_data = data.get("a", [])
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
                is_snapshot="s" in data or "seq" in data
            )
            
        except Exception as e:
            logger.error(f"Failed to parse Bybit orderbook: {e}")
            return None
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für Bybit
        BTC/USDT -> BTCUSDT
        """
        return symbol.replace("/", "").upper()
