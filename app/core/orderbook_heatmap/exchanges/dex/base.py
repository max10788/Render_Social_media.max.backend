"""
Basis-Klasse für DEX-Integrationen
"""
from abc import abstractmethod
from typing import Dict, Any, List, Optional
import logging

from app.core.orderbook_heatmap.exchanges.base import BaseExchange
from app.core.orderbook_heatmap.models.orderbook import (
    Orderbook, OrderbookLevel, OrderbookSide, DEXLiquidityTick
)


logger = logging.getLogger(__name__)


class BaseDEX(DEXExchange):
    """
    Basis-Klasse für DEX-Integrationen (Uniswap, Raydium, etc.)
    """
    
    def __init__(self, exchange):
        super().__init__(exchange)
        self.poll_interval = 10  # Sekunden
        
    @abstractmethod
    async def get_pool_info(self, pool_address: str) -> Dict[str, Any]:
        """
        Holt Pool-Informationen
        
        Args:
            pool_address: Pool Contract Address
            
        Returns:
            Pool-Info Dict
        """
        pass
    
    @abstractmethod
    async def get_liquidity_ticks(self, pool_address: str) -> List[DEXLiquidityTick]:
        """
        Holt Liquiditäts-Ticks vom Pool
        
        Args:
            pool_address: Pool Contract Address
            
        Returns:
            Liste von Liquiditäts-Ticks
        """
        pass
    
    def ticks_to_orderbook(
        self, 
        ticks: List[DEXLiquidityTick], 
        current_price: float,
        symbol: str
    ) -> Orderbook:
        """
        Konvertiert DEX Ticks zu Orderbuch-Format
        
        Args:
            ticks: Liste von Liquiditäts-Ticks
            current_price: Aktueller Pool-Preis
            symbol: Trading-Pair
            
        Returns:
            Orderbook-Objekt
        """
        bids = []
        asks = []
        
        for tick in ticks:
            level = tick.to_orderbook_level()
            
            # Ticks unter current_price = Bids
            # Ticks über current_price = Asks
            if tick.price_upper <= current_price:
                bids.append(level)
            elif tick.price_lower >= current_price:
                asks.append(level)
            else:
                # Tick überlappt current_price - aufteilen
                # Vereinfachung: zur näheren Seite zuordnen
                mid_price = (tick.price_lower + tick.price_upper) / 2
                if mid_price < current_price:
                    bids.append(level)
                else:
                    asks.append(level)
        
        # Sortiere Bids (höchste zuerst) und Asks (niedrigste zuerst)
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)
        
        return Orderbook(
            exchange=self.exchange,
            exchange_type=self.exchange_type,
            symbol=symbol,
            bids=OrderbookSide(levels=bids),
            asks=OrderbookSide(levels=asks),
            is_snapshot=True
        )
    
    async def connect(self, symbol: str) -> bool:
        """
        Verbindet zu DEX (startet Polling-Loop)
        
        Args:
            symbol: Pool Address oder Trading Pair
            
        Returns:
            True wenn erfolgreich
        """
        # DEX hat keine WebSocket - nutze Polling
        logger.info(f"DEX {self.exchange.value} does not use WebSocket - use polling via get_orderbook_snapshot")
        self.is_connected = True
        return True
    
    async def disconnect(self):
        """Trennt Verbindung"""
        self.is_connected = False
        logger.info(f"Disconnected from {self.exchange.value}")
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalisiert Symbol (für DEX meist Pool Address)
        
        Args:
            symbol: Pool Address oder Trading Pair
            
        Returns:
            Normalisiertes Symbol
        """
        return symbol.lower()
