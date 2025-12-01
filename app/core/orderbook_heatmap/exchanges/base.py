"""
Basis-Klasse für alle Exchange-Integrationen
"""
from abc import ABC, abstractmethod
from typing import Optional, Callable, Dict, Any
import asyncio
import logging
from datetime import datetime

from app.core.orderbook_heatmap.models.orderbook import Orderbook, Exchange, ExchangeType


logger = logging.getLogger(__name__)


class BaseExchange(ABC):
    """
    Abstrakte Basis-Klasse für Exchange-Integrationen
    """
    
    def __init__(self, exchange: Exchange, exchange_type: ExchangeType):
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.is_connected = False
        self.orderbook_callback: Optional[Callable] = None
        self._ws_task: Optional[asyncio.Task] = None
        
    @abstractmethod
    async def connect(self, symbol: str) -> bool:
        """
        Verbindet zur Börse und startet WebSocket-Stream
        
        Args:
            symbol: Trading-Pair (z.B. "BTC/USDT")
            
        Returns:
            True wenn erfolgreich verbunden
        """
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Trennt Verbindung"""
        pass
    
    @abstractmethod
    async def get_orderbook_snapshot(self, symbol: str, limit: int = 100) -> Optional[Orderbook]:
        """
        Holt Orderbuch-Snapshot via REST API
        
        Args:
            symbol: Trading-Pair
            limit: Anzahl der Levels pro Seite
            
        Returns:
            Orderbook-Objekt oder None bei Fehler
        """
        pass
    
    @abstractmethod
    async def _handle_orderbook_update(self, data: Dict[str, Any]):
        """
        Verarbeitet Orderbuch-Updates vom WebSocket
        
        Args:
            data: Rohdaten vom WebSocket
        """
        pass
    
    def set_orderbook_callback(self, callback: Callable[[Orderbook], None]):
        """
        Setzt Callback-Funktion für Orderbuch-Updates
        
        Args:
            callback: Funktion die bei jedem Update aufgerufen wird
        """
        self.orderbook_callback = callback
    
    async def _emit_orderbook(self, orderbook: Orderbook):
        """
        Emittiert Orderbuch an registrierte Callbacks
        
        Args:
            orderbook: Orderbook-Objekt
        """
        if self.orderbook_callback:
            try:
                if asyncio.iscoroutinefunction(self.orderbook_callback):
                    await self.orderbook_callback(orderbook)
                else:
                    self.orderbook_callback(orderbook)
            except Exception as e:
                logger.error(f"Error in orderbook callback for {self.exchange}: {e}")
    
    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol für diese Börse
        
        Args:
            symbol: Symbol in Standard-Format (z.B. "BTC/USDT")
            
        Returns:
            Symbol im Exchange-spezifischen Format
        """
        pass
    
    def get_status(self) -> Dict[str, Any]:
        """
        Gibt Status der Exchange-Verbindung zurück
        
        Returns:
            Status-Dict
        """
        return {
            "exchange": self.exchange.value,
            "exchange_type": self.exchange_type.value,
            "connected": self.is_connected,
            "timestamp": datetime.utcnow().isoformat()
        }


class CEXExchange(BaseExchange):
    """
    Basis-Klasse speziell für CEX-Integrationen
    """
    
    def __init__(self, exchange: Exchange):
        super().__init__(exchange, ExchangeType.CEX)
        self.api_key: Optional[str] = None
        self.api_secret: Optional[str] = None
        
    def set_credentials(self, api_key: str, api_secret: str):
        """Setzt API-Credentials (optional für public data)"""
        self.api_key = api_key
        self.api_secret = api_secret


class DEXExchange(BaseExchange):
    """
    Basis-Klasse speziell für DEX-Integrationen
    """
    
    def __init__(self, exchange: Exchange):
        super().__init__(exchange, ExchangeType.DEX)
        self.rpc_url: Optional[str] = None
        self.chain_id: Optional[int] = None
        
    def set_rpc_config(self, rpc_url: str, chain_id: int):
        """Setzt RPC-Konfiguration"""
        self.rpc_url = rpc_url
        self.chain_id = chain_id
    
    @abstractmethod
    async def get_pool_liquidity(self, pool_address: str) -> Dict[str, Any]:
        """
        Holt Pool-Liquidität
        
        Args:
            pool_address: Pool Contract Address
            
        Returns:
            Pool-Liquidität-Daten
        """
        pass
