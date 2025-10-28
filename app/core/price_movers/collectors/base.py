"""
Base Collector - Abstract Base Class

Basis-Klasse für alle Daten-Collectors
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime


logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Abstract Base Class für alle Collectors
    
    Definiert das Interface für Datensammlung
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialisiert den Collector
        
        Args:
            config: Optionale Konfiguration
        """
        self.config = config or {}
        self._is_initialized = False
        
        logger.debug(f"{self.__class__.__name__} initialisiert")
    
    @abstractmethod
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetcht eine einzelne Candle
        
        Args:
            symbol: Trading Pair
            timeframe: Timeframe
            timestamp: Zeitpunkt
            
        Returns:
            Candle Dictionary
        """
        pass
    
    @abstractmethod
    async def fetch_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None
    ) -> list:
        """
        Fetcht Trade History
        
        Args:
            symbol: Trading Pair
            start_time: Start
            end_time: Ende
            limit: Max Anzahl
            
        Returns:
            Liste von Trades
        """
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """
        Prüft ob Collector funktionsfähig ist
        
        Returns:
            True wenn OK
        """
        pass
    
    @abstractmethod
    async def close(self):
        """Schließt Connections und räumt auf"""
        pass
    
    def is_initialized(self) -> bool:
        """
        Prüft ob Collector initialisiert ist
        
        Returns:
            True wenn initialisiert
        """
        return self._is_initialized
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"
    
    def __repr__(self) -> str:
        return self.__str__()
