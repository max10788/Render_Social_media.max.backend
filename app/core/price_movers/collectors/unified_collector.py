# app/core/price_movers/collectors/unified_collector.py (korrigierte und vollständige Version)

import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Union

from .dexscreener_collector import DexscreenerCollector # Import hinzufügen
from .exchange_collector import ExchangeCollector, ExchangeCollectorFactory
from .birdeye_collector import BirdeyeCollector
from .dex_collector import DEXCollector
from .bitquery_collector import SolanaDexCollector # Import hinzufügen
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
    Unified Collector für CEX + DEX mit integriertem Bitquery Support.
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
            dex_api_keys: DEX API Keys {'birdeye': '...', 'helius': '...', 'bitquery': '...'}
        """
        self.config = config or {}
        
        # CEX Collectors (unverändert)
        self.cex_collectors: Dict[str, ExchangeCollector] = {}
        self._init_cex_collectors(cex_credentials or {})
        
        # DEX Collectors - NEUE Reihenfolge/Logik
        self.birdeye_collector: Optional[BirdeyeCollector] = None
        self.helius_collector: Optional[DEXCollector] = None
        self.solana_dex_collector: Optional[SolanaDexCollector] = None
        self.dexscreener_collector: Optional[DexscreenerCollector] = None # Neu
        self.dex_collectors: Dict[str, DEXCollector] = {} # Für Trades
        self._init_dex_collectors(dex_api_keys or {})
        
        logger.info(
            f"✓ Unified Collector initialisiert: "
            f"CEX={len(self.cex_collectors)}, "
            f"DEX={len(self.dex_collectors)}, "
            f"Sources=[birdeye={self.birdeye_collector is not None}, helius={self.helius_collector is not None}, bitquery={self.solana_dex_collector is not None}]"
        )
    
    def _init_cex_collectors(self, credentials: Dict[str, Dict[str, str]]):
        """Initialisiert CEX Collectors (unverändert)"""
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
        Priorität für OHLCV: Birdeye > SolanaDex (Bitquery) > Helius
        Priorität für Trades mit Wallets: Helius > SolanaDex (wenn Helius als Fallback)
        """
        
        # 1. Birdeye
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

        # 2. Helius
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

        # 3. Bitquery (SolanaDexCollector)
        bitquery_key = api_keys.get('bitquery')
        if bitquery_key or os.getenv("BITQUERY_API_KEY"):
            try:
                solana_config = {
                    'bitquery_api_key': bitquery_key,
                    'helius_collector_instance': self.helius_collector # Als Fallback für Trades mit Wallets
                }
                self.solana_dex_collector = SolanaDexCollector(config=solana_config)
                logger.info("✅ SolanaDexCollector (Bitquery) initialized")
            except Exception as e:
                logger.error(f"❌ SolanaDexCollector (Bitquery) failed: {e}")
                self.solana_dex_collector = None
        else:
            logger.info("ℹ️ Bitquery API Key not provided.")

        # --- DEX Zuweisung Logik ---
        # Wenn Birdeye da: dex_collectors bekommt Trade-Collector (Helius > SolanaDex)
        # Wenn Birdeye NICHT da: dex_collectors bekommt OHLCV-Collector (SolanaDex > Helius)
        primary_dex_collector = None
        if self.birdeye_collector:
            # Birdeye ist da -> es wird für OHLCV genutzt
            # `dex_collectors` bekommt den besten Trade-Collector
            primary_dex_collector = self.helius_collector or self.solana_dex_collector
        else:
            # Birdeye ist nicht da -> OHLCV geht an Collector in `dex_collectors`
            # `dex_collectors` bekommt den besten OHLCV-Collector
            primary_dex_collector = self.solana_dex_collector or self.helius_collector

        if primary_dex_collector:
            for dex in [SupportedDEX.JUPITER, SupportedDEX.RAYDIUM, SupportedDEX.ORCA]:
                self.dex_collectors[dex.value] = primary_dex_collector
            logger.info(f"✅ DEX Collectors set to: {primary_dex_collector.__class__.__name__}")
        else:
            logger.warning("⚠️ No DEX API Keys - DEX functionality disabled!")
            logger.info("💡 Set BIRDEYE_API_KEY, HELIUS_API_KEY, or BITQUERY_API_KEY for DEX data")

    async def fetch_trades(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> Dict[str, Any]:
        """
        Fetcht Trades mit automatischem Routing (unverändert)
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
        Fetcht von CEX (unverändert)
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
        Fetcht von DEX.
        Berücksichtigt, ob der verwendete Collector Wallet-IDs liefert.
        """
        collector = self.dex_collectors[exchange]
        
        logger.info(f"🔗 Fetching from DEX using {collector.__class__.__name__}: {exchange}")
        
        trades = await collector.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )

        # Bestimme has_wallet_ids basierend auf dem Collector-Typ
        has_wallets = False
        if hasattr(collector, 'provides_wallet_ids'):
             # Nutze Methode aus SolanaDexCollector
             has_wallets = collector.provides_wallet_ids()
        elif hasattr(collector, '__class__') and collector.__class__.__name__ == 'DEXCollector': # Vermutlich Helius
             has_wallets = True # Helius-basierter Collector liefert angenommen Trades mit Wallets

        logger.info(f"🔗 Fetched {len(trades)} trades. Has wallet IDs: {has_wallets}")

        return {
            'trades': trades,
            'has_wallet_ids': has_wallets, # <-- Dynamisch basierend auf Collector
            'data_source': 'dex',
            'exchange': exchange,
            'blockchain': getattr(collector, 'blockchain', 'solana').value,
        }
    

    async def fetch_candle_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetcht Candle-Daten mit automatischem Routing.
        Wenn Birdeye für DEX fehlschlägt, versucht es den SolanaDexCollector (Bitquery) als Fallback für OHLCV.
        """
        exchange = exchange.lower()

        # CEX: Direct routing (unverändert)
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
                try:
                    # Versuche Birdeye OHLCV
                    return await self.birdeye_collector.fetch_candle_data(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.warning(f"Birdeye OHLCV failed: {e}. Falling back to SolanaDex (Bitquery) OHLCV if available.")
                    # Wenn Birdeye fehlschlägt UND SolanaDexCollector verfügbar ist, nutze diesen als Fallback für OHLCV
                    if self.solana_dex_collector:
                         logger.info("📊 Using SolanaDexCollector (Bitquery) for OHLCV (fallback).")
                         return await self.solana_dex_collector.fetch_candle_data(
                             symbol=symbol,
                             timeframe=timeframe,
                             timestamp=timestamp
                         )
                    else:
                        # Kein Fallback verfügbar, werfe Fehler oder gib leere Candle zurück
                        logger.error(f"No fallback OHLCV collector available after Birdeye failed for {exchange}.")
                        raise e # Re-raise den ursprünglichen Fehler
            else:
                # Kein Birdeye -> Nutze den Collector, der in `dex_collectors` eingetragen ist
                # Dies ist entweder SolanaDex (Bitquery) oder Helius
                collector = self.dex_collectors[exchange]
                logger.info(f"📊 Using {collector.__class__.__name__} for OHLCV (no Birdeye)")
                return await collector.fetch_candle_data(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp
                )

        else:
            raise ValueError(f"Exchange '{exchange}' nicht verfügbar")
    
    def get_exchange_info(self, exchange: str) -> Dict[str, Any]:
        """
        Gibt Info über Exchange zurück (angepasst für neue Collector)
        """
        exchange = exchange.lower()
        
        # Check CEX (unverändert)
        if exchange in self.cex_collectors:
            config = EXCHANGE_CONFIGS.get(exchange, {})
            return {
                'exchange': exchange,
                'type': 'cex',
                'has_wallet_ids': False,
                'available': True,
                'config': config
            }
        
        # Check DEX (angepasst)
        elif exchange in self.dex_collectors:
            config = DEX_CONFIGS.get(exchange, {})
            # Bestimme, ob der zugewiesene Collector Wallets liefert
            collector = self.dex_collectors[exchange]
            has_wallets = False
            if hasattr(collector, 'provides_wallet_ids'):
                has_wallets = collector.provides_wallet_ids()
            elif collector.__class__.__name__ == 'DEXCollector': # Vermutlich Helius
                has_wallets = True

            return {
                'exchange': exchange,
                'type': 'dex',
                'has_wallet_ids': has_wallets,  # Dynamisch
                'available': True,
                'blockchain': getattr(config.get('blockchain'), 'value', 'solana'), # Annahme
                'has_birdeye': self.birdeye_collector is not None,
                'has_helius': self.helius_collector is not None,
                'has_bitquery': self.solana_dex_collector is not None, # Neu
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
        Listet alle verfügbaren Exchanges (unverändert)
        """
        return {
            'cex': list(self.cex_collectors.keys()),
            'dex': list(self.dex_collectors.keys())
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Prüft Health aller Collectors (angepasst für neuen Collector)
        """
        results = {
            'cex': {},
            'dex': {},
            'overall': 'healthy'
        }
        
        # Check CEX (unverändert)
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
        
        # Check DEX - Birdeye (unverändert)
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
        
        # Check DEX - Helius (unverändert)
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

        # Check DEX - SolanaDex (Bitquery) (Neu)
        if self.solana_dex_collector:
            try:
                is_healthy = await self.solana_dex_collector.health_check()
                results['dex']['solana_bitquery'] = is_healthy
                if not is_healthy:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"SolanaDex (Bitquery) health check failed: {e}")
                results['dex']['solana_bitquery'] = False
                results['overall'] = 'degraded'
        
        # DEX Exchanges inherit health from their primary collectors
        for dex_name in self.dex_collectors.keys():
            primary_collector = self.dex_collectors[dex_name]
            collector_name_key = 'unknown'
            if primary_collector == self.birdeye_collector:
                collector_name_key = 'birdeye'
            elif primary_collector == self.helius_collector:
                collector_name_key = 'helius'
            elif primary_collector == self.solana_dex_collector:
                collector_name_key = 'solana_bitquery'
            results['dex'][dex_name] = results['dex'].get(collector_name_key, False)
        
        return results
    
    async def close(self):
        """Schließt alle Collectors (angepasst für neuen Collector)"""
        # Close CEX (unverändert)
        for name, collector in self.cex_collectors.items():
            try:
                await collector.close()
                logger.debug(f"Closed CEX collector: {name}")
            except Exception as e:
                logger.error(f"Error closing CEX collector {name}: {e}")
        
        # Close Birdeye (unverändert)
        if self.birdeye_collector:
            try:
                await self.birdeye_collector.close()
                logger.debug("Closed Birdeye collector")
            except Exception as e:
                logger.error(f"Error closing Birdeye collector: {e}")
        
        # Close Helius (unverändert)
        if self.helius_collector:
            try:
                await self.helius_collector.close()
                logger.debug("Closed Helius collector")
            except Exception as e:
                logger.error(f"Error closing Helius collector: {e}")

        # Close SolanaDex (Bitquery) (Neu)
        if self.solana_dex_collector:
            try:
                await self.solana_dex_collector.close()
                logger.debug("Closed SolanaDex collector")
            except Exception as e:
                logger.error(f"Error closing SolanaDex collector: {e}")
        
        logger.info("✓ All collectors closed")
    
    def __str__(self) -> str:
        dex_info = []
        if self.birdeye_collector:
            dex_info.append("birdeye")
        if self.helius_collector:
            dex_info.append("helius")
        if self.solana_dex_collector:
            dex_info.append("solana_bitquery") # Neu

        return (
            f"UnifiedCollector("
            f"CEX={len(self.cex_collectors)}, "
            f"DEX={len(self.dex_collectors)}, "
            f"Providers={dex_info})"
        )
    
    def __repr__(self) -> str:
        return self.__str__()
