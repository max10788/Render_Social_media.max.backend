"""
Unified Collector - FINAL MERGED VERSION

Hybrid CEX/DEX Router mit Birdeye Priority für OHLCV Charts.

Features:
- ✅ CEX: Bitget, Binance, Kraken
- ✅ DEX: Jupiter, Raydium, Orca (via Helius/Birdeye)
- ✅ Birdeye FIRST für Charts (1 call = 100 candles!)
- ✅ Helius für Wallet-Analyse
- ✅ Automatisches Routing
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
    Unified Collector für CEX + DEX mit optimierter DEX Strategie
    
    WICHTIG - DEX Strategie:
    ========================
    1. BIRDEYE für Charts (OHLCV)
       - 1 API Call = bis zu 1000 Candles
       - Schnell (<1s)
       - Keine Rate Limits
    
    2. HELIUS für Wallet-Analyse
       - Nur bei Candle-Click
       - Trade-Details mit Wallet-Adressen
       - Caching + Rate Limiting
    
    CEX (Bitget/Binance/Kraken):
    ├─ Pattern-based Analysis
    ├─ Keine echten Wallet-IDs
    └─ OHLCV direkt verfügbar
    
    DEX (Jupiter/Raydium/Orca):
    ├─ Birdeye: OHLCV Charts (PRIMARY)
    ├─ Helius: Wallet-Analyse (SECONDARY)
    └─ On-chain Data (permanent)
    
    Usage:
        collector = UnifiedCollector(
            cex_credentials={...},
            dex_api_keys={'birdeye': '...', 'helius': '...'}
        )
        
        # Chart laden (nutzt Birdeye)
        candles = await collector.birdeye_collector.fetch_ohlcv_batch(...)
        
        # Wallet-Analyse (nutzt Helius)
        trades = await collector.fetch_trades(exchange='jupiter', ...)
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
                    'bitget': {'api_key': '...', 'api_secret': '...'},
                    'kraken': {'api_key': '...', 'api_secret': '...'}
                }
            
            dex_api_keys: DEX API Keys
                {
                    'birdeye': 'YOUR_BIRDEYE_API_KEY',  # PRIMARY für Charts
                    'helius': 'YOUR_HELIUS_API_KEY'     # SECONDARY für Wallets
                }
            
            config: Zusätzliche Konfiguration
        """
        self.config = config or {}
        
        # CEX Collectors
        self.cex_collectors: Dict[str, ExchangeCollector] = {}
        self._init_cex_collectors(cex_credentials or {})
        
        # DEX Collectors - BIRDEYE FIRST!
        self.birdeye_collector: Optional[BirdeyeCollector] = None
        self.helius_collector: Optional[DEXCollector] = None
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
        """
        Initialisiert DEX Collectors
        
        PRIORITY:
        1. BIRDEYE (für Charts - FASTEST!)
        2. HELIUS (für Wallet-Analyse)
        """
        
        # 1️⃣ BIRDEYE FIRST (für OHLCV Charts)
        birdeye_key = api_keys.get('birdeye')
        
        if birdeye_key:
            try:
                self.birdeye_collector = BirdeyeCollector(
                    api_key=birdeye_key,
                    config={'max_requests_per_minute': 100}
                )
                logger.info("✅ Birdeye Collector initialized (Solana OHLCV)")
            except Exception as e:
                logger.error(f"❌ Birdeye Collector failed: {e}")
                self.birdeye_collector = None
        else:
            logger.warning("⚠️ Birdeye API Key not provided - Charts will be slow!")
        
        # 2️⃣ HELIUS (für Wallet-Analyse mit Birdeye Fallback)
        helius_key = api_keys.get('helius')
        
        if helius_key:
            try:
                from .helius_collector import create_helius_collector
                self.helius_collector = create_helius_collector(
                    api_key=helius_key,
                    birdeye_collector=self.birdeye_collector,  # Fallback!
                    config={
                        'max_requests_per_second': 5,
                        'cache_ttl_seconds': 300,
                    }
                )
                logger.info("✅ Helius Collector initialized (with Birdeye fallback)")
            except Exception as e:
                logger.error(f"❌ Helius Collector failed: {e}")
                self.helius_collector = None
        else:
            logger.warning("⚠️ Helius API Key not provided")
        
        # Registriere für alle Solana DEXs
        if self.helius_collector or self.birdeye_collector:
            # Prefer Helius for trade fetching (has wallet addresses)
            # But Birdeye for OHLCV (much faster)
            primary_collector = self.helius_collector or self.birdeye_collector
            
            for dex in [SupportedDEX.JUPITER, SupportedDEX.RAYDIUM, SupportedDEX.ORCA]:
                self.dex_collectors[dex.value] = primary_collector
            
            dex_list = []
            if self.helius_collector:
                dex_list.append("helius")
            if self.birdeye_collector:
                dex_list.append("birdeye")
            
            logger.info(f"✅ DEX Collectors available: {dex_list}")
        else:
            logger.warning("⚠️ No DEX API Keys - DEX functionality disabled!")
            logger.info("💡 Set BIRDEYE_API_KEY and/or HELIUS_API_KEY for DEX data")
    
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
        Fetcht von DEX (nutzt Helius für Trades)
        
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
        
        WICHTIG: Für DEX wird BIRDEYE bevorzugt (falls verfügbar)
        
        Args:
            exchange: Exchange Name
            symbol: Trading Pair
            timeframe: Timeframe
            timestamp: Candle-Zeitpunkt
            
        Returns:
            Candle Dictionary
        """
        exchange = exchange.lower()
        
        # CEX: Direct routing
        if exchange in self.cex_collectors:
            collector = self.cex_collectors[exchange]
            return await collector.fetch_candle_data(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=timestamp
            )
        
        # DEX: Prefer Birdeye für OHLCV!
        elif exchange in self.dex_collectors:
            if self.birdeye_collector:
                logger.info("📊 Using Birdeye for OHLCV (faster!)")
                return await self.birdeye_collector.fetch_candle_data(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp
                )
            else:
                # Fallback zu Helius
                collector = self.dex_collectors[exchange]
                return await collector.fetch_candle_data(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp
                )
        
        else:
            raise ValueError(f"Exchange '{exchange}' nicht verfügbar")
    
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
                'has_birdeye': self.birdeye_collector is not None,
                'has_helius': self.helius_collector is not None,
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
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Prüft Health aller Collectors
        
        Returns:
            {
                'cex': {'bitget': True, 'binance': True, ...},
                'dex': {
                    'birdeye': True,
                    'helius': True,
                    'jupiter': True,
                    ...
                },
                'overall': 'healthy' | 'degraded' | 'unhealthy'
            }
        """
        results = {
            'cex': {},
            'dex': {},
            'overall': 'healthy'
        }
        
        # Check CEX
        for name, collector in self.cex_collectors.items():
            try:
                is_healthy = await collector.health_check()
                results['cex'][name] = is_healthy
                if not is_healthy:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"CEX health check failed {name}: {e}")
                results['cex'][name] = False
                results['overall'] = 'degraded'
        
        # Check DEX - Birdeye
        if self.birdeye_collector:
            try:
                is_healthy = await self.birdeye_collector.health_check()
                results['dex']['birdeye'] = is_healthy
                if not is_healthy:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"Birdeye health check failed: {e}")
                results['dex']['birdeye'] = False
                results['overall'] = 'degraded'
        
        # Check DEX - Helius
        if self.helius_collector:
            try:
                is_healthy = await self.helius_collector.health_check()
                results['dex']['helius'] = is_healthy
                if not is_healthy:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"Helius health check failed: {e}")
                results['dex']['helius'] = False
                results['overall'] = 'degraded'
        
        # DEX Exchanges inherit health from their collectors
        for dex_name in self.dex_collectors.keys():
            if self.birdeye_collector:
                results['dex'][dex_name] = results['dex'].get('birdeye', False)
            elif self.helius_collector:
                results['dex'][dex_name] = results['dex'].get('helius', False)
        
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
        
        # Close Birdeye
        if self.birdeye_collector:
            try:
                await self.birdeye_collector.close()
                logger.debug("Closed Birdeye collector")
            except Exception as e:
                logger.error(f"Error closing Birdeye collector: {e}")
        
        # Close Helius
        if self.helius_collector:
            try:
                await self.helius_collector.close()
                logger.debug("Closed Helius collector")
            except Exception as e:
                logger.error(f"Error closing Helius collector: {e}")
        
        logger.info("✓ All collectors closed")
    
    def __str__(self) -> str:
        dex_info = []
        if self.birdeye_collector:
            dex_info.append("birdeye")
        if self.helius_collector:
            dex_info.append("helius")
        
        return (
            f"UnifiedCollector("
            f"CEX={len(self.cex_collectors)}, "
            f"DEX={len(self.dex_collectors)}, "
            f"Providers={dex_info})"
        )
    
    def __repr__(self) -> str:
        return self.__str__()
