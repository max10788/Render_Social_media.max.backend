"""
Unified Collector - Hybrid CEX/DEX Router

Automatisches Routing zwischen:
- CEX Collectors (Bitget, Binance, Kraken) → Pattern-based Analysis
- DEX Collectors (Jupiter, Raydium, Uniswap) → Wallet-based Analysis

Entscheidet basierend auf Exchange-Typ welcher Collector genutzt wird
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Union

from .exchange_collector import ExchangeCollector, ExchangeCollectorFactory
from .birdeye_collector import BirdeyeCollector
from .dex_collector import DEXCollector
from ..utils.constants import (
    SupportedExchange,
    SupportedDEX,
    EXCHANGE_CONFIGS,
    DEX_CONFIGS,
    ERROR_MESSAGES
)


logger = logging.getLogger(__name__)


class UnifiedCollector:
    """
    Unified Collector für CEX + DEX
    
    Automatische Auswahl basierend auf Exchange-Typ:
    
    CEX (Bitget/Binance/Kraken):
    ├─ Pattern-based Analysis
    ├─ Keine echten Wallet-IDs
    └─ OHLCV-Fallback für historische Daten
    
    DEX (Jupiter/Raydium/Uniswap):
    ├─ Wallet-based Analysis
    ├─ ECHTE Wallet-IDs! 🎯
    └─ On-chain Data (permanent)
    
    Usage:
        collector = UnifiedCollector(
            cex_credentials={...},
            dex_api_keys={...}
        )
        
        # Automatisches Routing
        trades = await collector.fetch_trades(
            exchange='jupiter',  # → DEX Collector
            symbol='SOL/USDC',
            ...
        )
    """
    
    def __init__(
        self,
        cex_credentials: Optional[Dict[str, Dict[str, str]]] = None,
        dex_api_keys: Optional[Dict[str, str]] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialisiert Unified Collector
        
        Args:
            cex_credentials: CEX API Credentials
                {
                    'binance': {'api_key': '...', 'api_secret': '...'},
                    'bitget': {...},
                    'kraken': {...}
                }
            
            dex_api_keys: DEX API Keys
                {
                    'birdeye': 'YOUR_BIRDEYE_API_KEY',
                    'helius': 'YOUR_HELIUS_API_KEY'
                }
            
            config: Zusätzliche Konfiguration
        """
        self.config = config or {}
        
        # CEX Collectors
        self.cex_collectors: Dict[str, ExchangeCollector] = {}
        self._init_cex_collectors(cex_credentials or {})
        
        # DEX Collectors
        self.dex_collectors: Dict[str, DEXCollector] = {}
        self._init_dex_collectors(dex_api_keys or {})
        
        logger.info(
            f"✓ Unified Collector initialisiert: "
            f"CEX={list(self.cex_collectors.keys())}, "
            f"DEX={list(self.dex_collectors.keys())}"
        )
    
    def _init_cex_collectors(self, credentials: Dict[str, Dict[str, str]]):
        """Initialisiert CEX Collectors"""
        for exchange in SupportedExchange:
            creds = credentials.get(exchange.value, {})
            
            try:
                collector = ExchangeCollectorFactory.create(
                    exchange_name=exchange.value,
                    api_key=creds.get('api_key'),
                    api_secret=creds.get('api_secret')
                )
                self.cex_collectors[exchange.value] = collector
                logger.info(f"✓ CEX Collector created: {exchange.value}")
                
            except Exception as e:
                logger.warning(f"✗ Failed to create CEX collector {exchange.value}: {e}")
                # Continue with other exchanges
    
    def _init_dex_collectors(self, api_keys: Dict[str, str]):
        """Initialisiert DEX Collectors - PRIORITY: Helius > Birdeye > Mock"""
        
        # 1. Versuch Helius (BESTE Option - 100k req/day FREE!)
        helius_key = api_keys.get('helius')
        
        if helius_key:
            try:
                from .helius_collector import HeliusCollector
                helius = HeliusCollector(api_key=helius_key)
                
                # Registriere für alle Solana DEXs
                for dex in [SupportedDEX.JUPITER, SupportedDEX.RAYDIUM, SupportedDEX.ORCA]:
                    self.dex_collectors[dex.value] = helius
                
                logger.info("✅ Helius Collector created (Jupiter/Raydium/Orca)")
                return  # Fertig! Helius funktioniert
                
            except Exception as e:
                logger.error(f"✗ Helius Collector failed: {e}")
                # Fall through to Birdeye
        
        # 2. Fallback: Birdeye (wenn Helius fehlt/failed)
        birdeye_key = api_keys.get('birdeye')
        
        if birdeye_key:
            try:
                birdeye = BirdeyeCollector(api_key=birdeye_key)
                
                # Registriere für alle Solana DEXs
                for dex in [SupportedDEX.JUPITER, SupportedDEX.RAYDIUM, SupportedDEX.ORCA]:
                    self.dex_collectors[dex.value] = birdeye
                
                logger.info("✅ Birdeye Collector created (Jupiter/Raydium/Orca)")
                return
                
            except Exception as e:
                logger.error(f"✗ Birdeye Collector failed: {e}")
        
        # 3. Fallback: Mock (für Development ohne API Keys)
        logger.warning("⚠️ Keine DEX API Keys - Using MOCK DEX")
        logger.info("💡 Tipp: Setze HELIUS_API_KEY für echte DEX Daten!")
    
    async def fetch_trades(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> Dict[str, Any]:
        """
        Fetcht Trades mit automatischem Routing
        
        Args:
            exchange: Exchange Name (bitget/binance/jupiter/raydium/etc.)
            symbol: Trading Pair
            start_time: Start
            end_time: Ende
            limit: Max Trades
            
        Returns:
            {
                'trades': List[Dict],
                'has_wallet_ids': bool,  # ← KEY Unterschied!
                'data_source': 'cex' oder 'dex',
                'exchange': str
            }
        """
        exchange = exchange.lower()
        
        # Route zu CEX oder DEX
        if exchange in self.cex_collectors:
            return await self._fetch_from_cex(
                exchange, symbol, start_time, end_time, limit
            )
        
        elif exchange in self.dex_collectors:
            return await self._fetch_from_dex(
                exchange, symbol, start_time, end_time, limit
            )
        
        else:
            available_cex = list(self.cex_collectors.keys())
            available_dex = list(self.dex_collectors.keys())
            
            raise ValueError(
                f"Exchange '{exchange}' nicht verfügbar. "
                f"CEX: {available_cex}, DEX: {available_dex}"
            )
    
    async def _fetch_from_cex(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> Dict[str, Any]:
        """
        Fetcht von CEX
        
        CEX = Keine echten Wallet-IDs
        """
        collector = self.cex_collectors[exchange]
        
        logger.info(f"📊 Fetching from CEX: {exchange}")
        
        trades = await collector.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        return {
            'trades': trades,
            'has_wallet_ids': False,  # ← CEX = keine Wallet IDs
            'data_source': 'cex',
            'exchange': exchange,
            'warning': 'CEX data - virtual entities based on patterns'
        }
    
    async def _fetch_from_dex(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> Dict[str, Any]:
        """
        Fetcht von DEX
        
        DEX = ECHTE Wallet-IDs! 🎯
        """
        collector = self.dex_collectors[exchange]
        
        logger.info(f"🔗 Fetching from DEX: {exchange}")
        
        trades = await collector.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        return {
            'trades': trades,
            'has_wallet_ids': True,  # ← DEX = ECHTE Wallet IDs! 🎯
            'data_source': 'dex',
            'exchange': exchange,
            'blockchain': collector.blockchain.value,
        }
    
    async def fetch_candle_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetcht Candle-Daten mit automatischem Routing
        
        Args:
            exchange: Exchange Name
            symbol: Trading Pair
            timeframe: Timeframe
            timestamp: Candle-Zeitpunkt
            
        Returns:
            Candle Dictionary
        """
        exchange = exchange.lower()
        
        # Route zu passenden Collector
        if exchange in self.cex_collectors:
            collector = self.cex_collectors[exchange]
        elif exchange in self.dex_collectors:
            collector = self.dex_collectors[exchange]
        else:
            raise ValueError(f"Exchange '{exchange}' nicht verfügbar")
        
        return await collector.fetch_candle_data(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=timestamp
        )
    
    def get_exchange_info(self, exchange: str) -> Dict[str, Any]:
        """
        Gibt Info über Exchange zurück
        
        Args:
            exchange: Exchange Name
            
        Returns:
            Exchange Info Dictionary
        """
        exchange = exchange.lower()
        
        # Check CEX
        if exchange in self.cex_collectors:
            config = EXCHANGE_CONFIGS.get(exchange, {})
            return {
                'exchange': exchange,
                'type': 'cex',
                'has_wallet_ids': False,
                'available': True,
                'config': config
            }
        
        # Check DEX
        elif exchange in self.dex_collectors:
            config = DEX_CONFIGS.get(exchange, {})
            return {
                'exchange': exchange,
                'type': 'dex',
                'has_wallet_ids': True,  # 🎯
                'available': True,
                'blockchain': config.get('blockchain'),
                'config': config
            }
        
        else:
            return {
                'exchange': exchange,
                'type': 'unknown',
                'available': False,
                'error': f"Exchange '{exchange}' nicht konfiguriert"
            }
    
    def list_available_exchanges(self) -> Dict[str, List[str]]:
        """
        Listet alle verfügbaren Exchanges
        
        Returns:
            {
                'cex': ['bitget', 'binance', ...],
                'dex': ['jupiter', 'raydium', ...]
            }
        """
        return {
            'cex': list(self.cex_collectors.keys()),
            'dex': list(self.dex_collectors.keys())
        }
    
    async def health_check(self) -> Dict[str, Dict[str, bool]]:
        """
        Prüft Health aller Collectors
        
        Returns:
            {
                'cex': {'bitget': True, 'binance': True, ...},
                'dex': {'jupiter': True, ...}
            }
        """
        results = {
            'cex': {},
            'dex': {}
        }
        
        # Check CEX
        for name, collector in self.cex_collectors.items():
            try:
                is_healthy = await collector.health_check()
                results['cex'][name] = is_healthy
            except Exception as e:
                logger.error(f"CEX health check failed {name}: {e}")
                results['cex'][name] = False
        
        # Check DEX
        for name, collector in self.dex_collectors.items():
            try:
                is_healthy = await collector.health_check()
                results['dex'][name] = is_healthy
            except Exception as e:
                logger.error(f"DEX health check failed {name}: {e}")
                results['dex'][name] = False
        
        return results
    
    async def close(self):
        """Schließt alle Collectors"""
        # Close CEX
        for name, collector in self.cex_collectors.items():
            try:
                await collector.close()
                logger.debug(f"Closed CEX collector: {name}")
            except Exception as e:
                logger.error(f"Error closing CEX collector {name}: {e}")
        
        # Close DEX
        for name, collector in self.dex_collectors.items():
            try:
                await collector.close()
                logger.debug(f"Closed DEX collector: {name}")
            except Exception as e:
                logger.error(f"Error closing DEX collector {name}: {e}")
        
        logger.info("✓ All collectors closed")
    
    def __str__(self) -> str:
        return (
            f"UnifiedCollector(CEX={len(self.cex_collectors)}, "
            f"DEX={len(self.dex_collectors)})"
        )
    
    def __repr__(self) -> str:
        return self.__str__()
