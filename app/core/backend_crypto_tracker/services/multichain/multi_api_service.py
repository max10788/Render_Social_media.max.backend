# services/multichain/multi_api_service.py
import asyncio
import logging
import random
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.cache import CacheManager
from .api_providers import (
    BaseAPIProvider, CoinGeckoProvider, CoinMarketCapProvider, 
    CryptoCompareProvider, BinanceProvider, TokenPriceData
)

logger = get_logger(__name__)

class MultiAPIService:
    """Service zur Verwaltung und Lastverteilung mehrerer API-Anbieter"""
    
    def __init__(self):
        self.providers: List[BaseAPIProvider] = []
        self.cache = CacheManager()
        self.strategy = "round_robin"  # round_robin, random, priority
        self.current_provider_index = 0
        self.provider_stats = {}
        
        # Initialisiere die Provider
        self._initialize_providers()
    
    def _initialize_providers(self):
        """Initialisiert alle verfügbaren API-Anbieter"""
        # Hole API-Schlüssel aus Umgebungsvariablen
        coingecko_key = None  # Demo-API, kein Schlüssel nötig
        coinmarketcap_key = None  # Kostenlos ohne Schlüssel
        cryptocompare_key = None  # Kostenlos ohne Schlüssel
        binance_key = None  # Kostenlos ohne Schlüssel
        
        # Provider erstellen
        providers = [
            CoinGeckoProvider(coingecko_key),
            CoinMarketCapProvider(coinmarketcap_key),
            CryptoCompareProvider(cryptocompare_key),
            BinanceProvider(binance_key)
        ]
        
        self.providers = providers
        
        # Initialisiere Statistiken
        for provider in providers:
            self.provider_stats[provider.name] = {
                'requests': 0,
                'errors': 0,
                'last_used': None,
                'available': True
            }
        
        logger.info(f"Initialized {len(self.providers)} API providers")
    
    async def __aenter__(self):
        """Initialisiert alle Provider"""
        for provider in self.providers:
            await provider.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Schließt alle Provider"""
        for provider in self.providers:
            await provider.__aexit__(exc_type, exc_val, exc_tb)
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Holt Token-Preis mit Lastverteilung und Fallback"""
        # Prüfe Cache
        cache_key = f"token_price:{chain}:{token_address}"
        cached_data = self.cache.get(cache_key)
        if cached_data:
            logger.debug(f"Using cached data for {token_address}")
            return cached_data
        
        # Versuche verschiedene Provider in der gewählten Strategie
        result = await self._try_all_providers(token_address, chain)
        
        if result:
            # Speichere im Cache
            self.cache.set(cache_key, result, expire_in=300)  # 5 Minuten Cache
            return result
        
        return None
    
    async def _try_all_providers(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Versucht alle Provider in der gewählten Strategie"""
        providers_to_try = self._get_provider_order()
        
        for provider_name in providers_to_try:
            provider = self._get_provider_by_name(provider_name)
            if not provider or not provider.check_availability():
                continue
            
            try:
                logger.debug(f"Trying {provider.name} for {token_address}")
                
                # Aktualisiere Statistiken
                self.provider_stats[provider.name]['requests'] += 1
                self.provider_stats[provider.name]['last_used'] = datetime.utcnow()
                
                # Führe Anfrage aus
                result = await provider.get_token_price(token_address, chain)
                
                if result:
                    logger.debug(f"Successfully fetched from {provider.name}")
                    return result
                
            except Exception as e:
                logger.error(f"Error with {provider.name}: {e}")
                self.provider_stats[provider.name]['errors'] += 1
                
                # Deaktiviere Provider bei zu vielen Fehlern
                if self.provider_stats[provider.name]['errors'] > 5:
                    provider.is_available = False
                    self.provider_stats[provider.name]['available'] = False
                    logger.warning(f"Disabled {provider.name} due to too many errors")
        
        return None
    
    def _get_provider_order(self) -> List[str]:
        """Gibt die Reihenfolge der Provider basierend auf der Strategie zurück"""
        if self.strategy == "round_robin":
            return self._round_robin_order()
        elif self.strategy == "random":
            return self._random_order()
        elif self.strategy == "priority":
            return self._priority_order()
        else:
            return self._round_robin_order()
    
    def _round_robin_order(self) -> List[str]:
        """Round-Robin-Strategie"""
        available_providers = [p.name for p in self.providers if p.check_availability()]
        if not available_providers:
            return []
        
        # Rotiere durch die Provider
        self.current_provider_index = (self.current_provider_index + 1) % len(available_providers)
        
        # Erstelle Reihenfolge: beginnend mit dem aktuellen Provider
        ordered = available_providers[self.current_provider_index:] + available_providers[:self.current_provider_index]
        return ordered
    
    def _random_order(self) -> List[str]:
        """Zufällige Reihenfolge"""
        available_providers = [p.name for p in self.providers if p.check_availability()]
        random.shuffle(available_providers)
        return available_providers
    
    def _priority_order(self) -> List[str]:
        """Prioritätsreihenfolge basierend auf Erfolgsrate"""
        available_providers = [p.name for p in self.providers if p.check_availability()]
        
        # Sortiere nach Erfolgsrate (weniger Fehler = höhere Priorität)
        def get_success_rate(provider_name):
            stats = self.provider_stats.get(provider_name, {})
            requests = stats.get('requests', 1)
            errors = stats.get('errors', 0)
            return (requests - errors) / requests if requests > 0 else 0
        
        return sorted(available_providers, key=get_success_rate, reverse=True)
    
    def _get_provider_by_name(self, name: str) -> Optional[BaseAPIProvider]:
        """Holt einen Provider anhand seines Namens"""
        for provider in self.providers:
            if provider.name == name:
                return provider
        return None
    
    def get_provider_stats(self) -> Dict[str, Any]:
        """Gibt Statistiken über alle Provider zurück"""
        return self.provider_stats
    
    def update_strategy(self, strategy: str):
        """Aktualisiert die Lastverteilungsstrategie"""
        if strategy in ["round_robin", "random", "priority"]:
            self.strategy = strategy
            logger.info(f"Updated strategy to {strategy}")
        else:
            logger.error(f"Unknown strategy: {strategy}")

class CacheManager:
    """Einfacher Cache-Manager"""
    
    def __init__(self):
        self.cache = {}
    
    def get(self, key: str):
        """Holt Daten aus dem Cache"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            if datetime.utcnow() - timestamp < timedelta(seconds=300):  # 5 Minuten
                return data
            del self.cache[key]
        return None
    
    def set(self, key: str, data: Any, expire_in: int = 300):
        """Speichert Daten im Cache"""
        self.cache[key] = (data, datetime.utcnow())
    
    def clear(self):
        """Leert den Cache"""
        self.cache.clear()
