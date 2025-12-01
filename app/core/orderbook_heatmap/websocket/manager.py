"""
WebSocket Manager für Live-Updates
"""
import asyncio
import json
import logging
from typing import Optional, Dict, List, Set  # ← FIXED: Set import hinzugefügt
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
