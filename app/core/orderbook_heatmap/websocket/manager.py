"""
WebSocket Manager für Live-Updates
"""
import asyncio
import json
import logging
from typing import Optional, Dict, List, Set, Any  # ← FIXED: Set import hinzugefügt
from datetime import datetime
from fastapi import WebSocket


logger = logging.getLogger(__name__)


class WebSocketManager:
    """
    Verwaltet WebSocket-Verbindungen und Broadcasting
    """
    
    def __init__(self):
        # Dict: symbol -> Set of WebSocket connections
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._broadcast_lock = asyncio.Lock()
        
    async def connect(self, websocket: WebSocket, symbol: str):
        """
        Fügt neue WebSocket-Verbindung hinzu
        
        Args:
            websocket: WebSocket connection
            symbol: Trading Pair
        """
        await websocket.accept()
        
        if symbol not in self.active_connections:
            self.active_connections[symbol] = set()
        
        self.active_connections[symbol].add(websocket)
        logger.info(f"WebSocket connected for {symbol}. Total: {len(self.active_connections[symbol])}")
    
    def disconnect(self, websocket: WebSocket):
        """
        Entfernt WebSocket-Verbindung
        
        Args:
            websocket: WebSocket connection
        """
        for symbol, connections in list(self.active_connections.items()):  # ← FIXED: list() hinzugefügt für safe iteration
            if websocket in connections:
                connections.remove(websocket)
                logger.info(f"WebSocket disconnected from {symbol}. Remaining: {len(connections)}")
                
                # Cleanup leere Sets
                if not connections:
                    del self.active_connections[symbol]
                
                break
    
    async def broadcast_update(self, aggregator):
        """
        Sendet Update an alle verbundenen Clients
        
        Args:
            aggregator: OrderbookAggregator instance
        """
        async with self._broadcast_lock:
            for symbol, connections in list(self.active_connections.items()):
                if not connections:
                    continue
                
                try:
                    # Hole neuesten Snapshot
                    snapshot = await aggregator.get_latest_heatmap(symbol)
                    
                    if not snapshot:
                        continue
                    
                    # Konvertiere zu Matrix-Format
                    exchanges = list(aggregator.exchanges.keys())
                    matrix_data = snapshot.to_matrix(exchanges)
                    
                    # Erstelle Nachricht
                    message = {
                        "type": "heatmap_update",
                        "symbol": symbol,
                        "data": matrix_data
                    }
                    
                    # Sende an alle Clients
                    disconnected = []
                    for websocket in list(connections):  # ← FIXED: list() für safe iteration
                        try:
                            await websocket.send_json(message)
                        except Exception as e:
                            logger.error(f"Failed to send to websocket: {e}")
                            disconnected.append(websocket)
                    
                    # Entferne disconnected websockets
                    for ws in disconnected:
                        connections.discard(ws)
                    
                except Exception as e:
                    logger.error(f"Failed to broadcast update for {symbol}: {e}")


    async def handle_dex_event(
        self,
        event_type: str,
        pool_address: str,
        event_data: Dict[str, Any],
        symbol: str
    ):
        """
        Verarbeitet On-Chain DEX Events und sendet an WebSocket Clients
        
        Args:
            event_type: "Mint", "Burn", "Swap", "pool_state_update"
            pool_address: Pool Contract Adresse
            event_data: Event-spezifische Daten
            symbol: Trading Pair
        """
        try:
            # Erstelle Event-Message basierend auf Typ
            if event_type == "Mint":
                message = self._create_liquidity_change_message(
                    "liquidity_added",
                    pool_address,
                    event_data,
                    symbol
                )
            
            elif event_type == "Burn":
                message = self._create_liquidity_change_message(
                    "liquidity_removed",
                    pool_address,
                    event_data,
                    symbol
                )
            
            elif event_type == "Swap":
                message = self._create_swap_message(
                    pool_address,
                    event_data,
                    symbol
                )
            
            elif event_type == "pool_state_update":
                message = self._create_pool_state_message(
                    pool_address,
                    event_data,
                    symbol
                )
            
            else:
                logger.warning(f"Unknown DEX event type: {event_type}")
                return
            
            # Sende an alle Clients für dieses Symbol
            await self._broadcast_to_symbol(symbol, message)
            
            logger.debug(f"📡 Broadcasted DEX event: {event_type} for {symbol}")
            
        except Exception as e:
            logger.error(f"❌ Failed to handle DEX event: {e}")
    
    def _create_liquidity_change_message(
        self,
        change_type: str,
        pool_address: str,
        event_data: Dict[str, Any],
        symbol: str
    ) -> Dict[str, Any]:
        """
        Erstellt Liquidity-Change Message
        
        Args:
            change_type: "liquidity_added" oder "liquidity_removed"
            pool_address: Pool Adresse
            event_data: Event Daten
            symbol: Trading Pair
            
        Returns:
            Message Dict
        """
        return {
            "type": "liquidity_change",
            "symbol": symbol,
            "pool_address": pool_address,
            "change_type": change_type,
            "tick_lower": event_data.get("tick_lower"),
            "tick_upper": event_data.get("tick_upper"),
            "liquidity_delta": event_data.get("liquidity_delta", 0.0),
            "amount0": event_data.get("amount0", 0.0),
            "amount1": event_data.get("amount1", 0.0),
            "provider": event_data.get("provider", "unknown"),
            "tx_hash": event_data.get("tx_hash", ""),
            "block_number": event_data.get("block_number", 0),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _create_swap_message(
        self,
        pool_address: str,
        event_data: Dict[str, Any],
        symbol: str
    ) -> Dict[str, Any]:
        """
        Erstellt Swap Event Message
        
        Args:
            pool_address: Pool Adresse
            event_data: Event Daten
            symbol: Trading Pair
            
        Returns:
            Message Dict
        """
        return {
            "type": "swap_executed",
            "symbol": symbol,
            "pool_address": pool_address,
            "amount0": event_data.get("amount0", 0.0),
            "amount1": event_data.get("amount1", 0.0),
            "sqrt_price_x96": event_data.get("sqrt_price_x96", 0),
            "liquidity": event_data.get("liquidity", 0.0),
            "tick": event_data.get("tick", 0),
            "price": event_data.get("price", 0.0),
            "tx_hash": event_data.get("tx_hash", ""),
            "block_number": event_data.get("block_number", 0),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _create_pool_state_message(
        self,
        pool_address: str,
        event_data: Dict[str, Any],
        symbol: str
    ) -> Dict[str, Any]:
        """
        Erstellt Pool State Update Message
        
        Args:
            pool_address: Pool Adresse
            event_data: Pool State Daten
            symbol: Trading Pair
            
        Returns:
            Message Dict
        """
        return {
            "type": "pool_state_update",
            "symbol": symbol,
            "pool_address": pool_address,
            "current_price": event_data.get("current_price", 0.0),
            "current_tick": event_data.get("current_tick", 0),
            "total_liquidity": event_data.get("total_liquidity", 0.0),
            "tvl_usd": event_data.get("tvl_usd", 0.0),
            "last_event": event_data.get("last_event", {}),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def broadcast_concentration_alert(
        self,
        symbol: str,
        pool_address: str,
        concentration_metrics: Dict[str, float],
        alert_level: str = "info"
    ):
        """
        Sendet Concentration Alert an Clients
        
        Args:
            symbol: Trading Pair
            pool_address: Pool Adresse
            concentration_metrics: Konzentrations-Metriken
            alert_level: "info", "warning", "high"
        """
        message = {
            "type": "concentration_alert",
            "symbol": symbol,
            "pool_address": pool_address,
            "alert_level": alert_level,
            "message": self._generate_concentration_message(
                concentration_metrics,
                alert_level
            ),
            "concentration_metrics": concentration_metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self._broadcast_to_symbol(symbol, message)
        
        logger.info(
            f"⚠️ Concentration alert for {symbol}: "
            f"{concentration_metrics.get('within_1_percent', 0)}% within ±1%"
        )
    
    def _generate_concentration_message(
        self,
        metrics: Dict[str, float],
        alert_level: str
    ) -> str:
        """
        Generiert Human-Readable Concentration Message
        
        Args:
            metrics: Concentration Metrics
            alert_level: Alert Level
            
        Returns:
            Message String
        """
        within_1pct = metrics.get("within_1_percent", 0)
        within_2pct = metrics.get("within_2_percent", 0)
        
        if alert_level == "high":
            return (
                f"⚠️ HIGH CONCENTRATION: {within_1pct:.1f}% of liquidity "
                f"within ±1% of current price"
            )
        elif alert_level == "warning":
            return (
                f"⚠ Increased concentration: {within_2pct:.1f}% of liquidity "
                f"within ±2% of current price"
            )
        else:
            return (
                f"ℹ️ Liquidity distribution: {within_1pct:.1f}% within ±1%, "
                f"{within_2pct:.1f}% within ±2%"
            )
    
    async def _broadcast_to_symbol(
        self,
        symbol: str,
        message: Dict[str, Any]
    ):
        """
        Sendet Message an alle Clients die ein Symbol subscribed haben
        
        Args:
            symbol: Trading Pair
            message: Message Dict
        """
        if symbol not in self.active_connections:
            return
        
        connections = self.active_connections[symbol]
        disconnected = []
        
        for websocket in list(connections):
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.error(f"Failed to send to websocket: {e}")
                disconnected.append(websocket)
        
        # Entferne disconnected websockets
        for ws in disconnected:
            connections.discard(ws)
    
    async def send_personal_message(self, message: Dict, websocket: WebSocket):
        """
        Sendet Nachricht an einzelnen Client
        
        Args:
            message: Nachricht als Dict
            websocket: WebSocket connection
        """
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(f"Failed to send personal message: {e}")
            self.disconnect(websocket)
    
    def get_connection_count(self, symbol: Optional[str] = None) -> int:
        """
        Gibt Anzahl aktiver Verbindungen zurück
        
        Args:
            symbol: Optional - für spezifisches Symbol
            
        Returns:
            Anzahl Verbindungen
        """
        if symbol:
            return len(self.active_connections.get(symbol, set()))
        
        return sum(len(conns) for conns in self.active_connections.values())
    
    def get_status(self) -> Dict:
        """
        Gibt Status zurück

        Returns:
            Status-Dict
        """
        return {
            "total_connections": self.get_connection_count(),
            "symbols": {
                symbol: len(connections)
                for symbol, connections in self.active_connections.items()
            }
        }

    # ============================================================================
    # Level 3 Order Book Broadcasting
    # ============================================================================

    async def broadcast_l3_order(self, order):
        """
        Stream individual L3 order to connected clients

        Args:
            order: L3Order instance
        """
        message = {
            "type": "l3_order",
            "exchange": order.exchange,
            "symbol": order.symbol,
            "data": {
                "order_id": order.order_id,
                "sequence": order.sequence,
                "side": order.side.value,
                "price": order.price,
                "size": order.size,
                "event_type": order.event_type.value,
                "timestamp": order.timestamp.isoformat(),
                "metadata": order.metadata
            }
        }

        await self._broadcast_to_symbol(order.symbol, message)

    async def broadcast_l3_snapshot(self, snapshot):
        """
        Send full L3 snapshot to new clients

        Args:
            snapshot: L3Orderbook instance
        """
        from app.core.orderbook_heatmap.models.level3 import L3Side

        # Create compressed snapshot message
        message = {
            "type": "l3_snapshot",
            "exchange": snapshot.exchange,
            "symbol": snapshot.symbol,
            "sequence": snapshot.sequence,
            "timestamp": snapshot.timestamp.isoformat(),
            "statistics": {
                "total_orders": snapshot.get_total_orders(),
                "bid_count": len(snapshot.bids),
                "ask_count": len(snapshot.asks),
                "total_bid_volume": snapshot.get_total_volume(L3Side.BID),
                "total_ask_volume": snapshot.get_total_volume(L3Side.ASK),
                "best_bid": snapshot.get_best_bid(),
                "best_ask": snapshot.get_best_ask(),
                "spread": snapshot.get_spread(),
                "mid_price": snapshot.get_mid_price()
            },
            "bids": [
                {
                    "order_id": order.order_id,
                    "price": order.price,
                    "size": order.size
                }
                for order in snapshot.bids[:100]  # Top 100 bids
            ],
            "asks": [
                {
                    "order_id": order.order_id,
                    "price": order.price,
                    "size": order.size
                }
                for order in snapshot.asks[:100]  # Top 100 asks
            ]
        }

        await self._broadcast_to_symbol(snapshot.symbol, message)

    async def broadcast_l3_statistics(self, exchange: str, symbol: str, stats: Dict[str, Any]):
        """
        Broadcast L3 statistics update

        Args:
            exchange: Exchange name
            symbol: Trading pair
            stats: Statistics dictionary
        """
        message = {
            "type": "l3_statistics",
            "exchange": exchange,
            "symbol": symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "data": stats
        }

        await self._broadcast_to_symbol(symbol, message)
