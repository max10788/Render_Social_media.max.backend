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
        Priorität für OHLCV (in fetch_candle_data):
        1. DEXSCREENER (kostenlos, begrenzte Timeframes)
        2. BIRDEYE (wenn nicht suspended)
        3. SOLANADEX (Bitquery)
        4. HELIUS (als letzter Fallback für OHLCV)
    
        Priorität für Trades (in _fetch_from_dex):
        1. HELIUS (wenn verfügbar)
        2. SOLANADEX (wenn Helius als Fallback gesetzt)
        """
        
        # 1. Dexscreener (immer verfügbar, keine Auth)
        # NUR FÜR OHLCV, NICHT FÜR TRADES!
        try:
            self.dexscreener_collector = DexscreenerCollector()
            logger.info("✅ Dexscreener Collector initialized (OHLCV only)")
        except Exception as e:
            logger.error(f"❌ Dexscreener Collector failed: {e}")
            self.dexscreener_collector = None
    
        # 2. Birdeye
        birdeye_key = api_keys.get('birdeye')
        if birdeye_key:
            try:
                self.birdeye_collector = BirdeyeCollector(
                    api_key=birdeye_key,
                    config={'max_requests_per_minute': 100}
                )
                self.birdeye_healthy_at_init = True
                logger.info("✅ Birdeye Collector initialized (Solana OHLCV)")
            except Exception as e:
                logger.error(f"❌ Birdeye Collector failed init: {e}")
                self.birdeye_collector = None
                self.birdeye_healthy_at_init = False
        else:
            self.birdeye_healthy_at_init = False
            logger.info("ℹ️ Birdeye API Key not provided.")
    
        # 3. Helius
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
    
        # 4. Bitquery (SolanaDexCollector)
        bitquery_key = api_keys.get('bitquery')
        if bitquery_key or os.getenv("BITQUERY_API_KEY"):
            try:
                solana_config = {
                    'bitquery_api_key': bitquery_key or os.getenv("BITQUERY_API_KEY"),
                    'helius_collector_instance': self.helius_collector
                }
                self.solana_dex_collector = SolanaDexCollector(config=solana_config)
                logger.info("✅ SolanaDexCollector (Bitquery) initialized")
            except Exception as e:
                logger.error(f"❌ SolanaDexCollector (Bitquery) failed: {e}")
                self.solana_dex_collector = None
        else:
            logger.info("ℹ️ Bitquery API Key not provided.")
    
        # --- DEX Trade Collectors Zuweisung ---
        # WICHTIG: Nur Helius und SolanaDex können Trades liefern!
        # Dexscreener und Birdeye sind NUR für OHLCV!
        
        primary_trade_collector = self.helius_collector or self.solana_dex_collector
    
        if primary_trade_collector:
            # Weise den Trade-Collector allen DEX-Exchanges zu
            for dex in [SupportedDEX.JUPITER, SupportedDEX.RAYDIUM, SupportedDEX.ORCA]:
                self.dex_collectors[dex.value] = primary_trade_collector
            
            logger.info(
                f"✅ DEX Trade Collectors set to: {primary_trade_collector.__class__.__name__}"
            )
        else:
            logger.warning("⚠️ No DEX Trade Collectors available!")
            logger.info("💡 Set HELIUS_API_KEY or BITQUERY_API_KEY for DEX trade data")
        
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
        Fetcht Candle-Daten mit neuer Priorität.
        Priorität: Dexscreener > Birdeye (wenn healthy) > SolanaDex (Bitquery) > Helius
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

        # DEX: Neue Prioritäts-Logik
        elif exchange in self.dex_collectors: # DEX-Collector ist hier für Trades, aber wir nutzen die Liste zur Validierung
            # 1. Versuche Dexscreener (kostenlos)
            if self.dexscreener_collector:
                logger.info("📊 Using Dexscreener for OHLCV (free!)")
                try:
                    return await self.dexscreener_collector.fetch_candle_data(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.warning(f"Dexscreener OHLCV failed: {e}")

            # 2. Versuche Birdeye, wenn vorhanden und gesund
            if self.birdeye_collector and getattr(self, 'birdeye_healthy_at_init', True): # Berücksichtige init-Status
                logger.info("📊 Using Birdeye for OHLCV (faster, if working!)")
                try:
                    return await self.birdeye_collector.fetch_candle_data(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.warning(f"Birdeye OHLCV failed: {e}. This might indicate a suspended key.")

            # 3. Versuche SolanaDex (Bitquery)
            if self.solana_dex_collector:
                logger.info("📊 Using SolanaDexCollector (Bitquery) for OHLCV (fallback).")
                try:
                    return await self.solana_dex_collector.fetch_candle_data(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.warning(f"SolanaDex (Bitquery) OHLCV failed: {e}")

            # 4. Fallback zu Helius (wenn es OHLCV kann)
            if self.helius_collector:
                logger.info("📊 Using Helius for OHLCV (last resort).")
                try:
                    return await self.helius_collector.fetch_candle_data(
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.warning(f"Helius OHLCV failed: {e}")

            # Wenn alle fehlschlagen
            logger.error(f"All OHLCV collectors failed for {exchange} {symbol} {timeframe} @ {timestamp}")
            return {
                'timestamp': timestamp,
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': 0.0,
                'volume_usd': 0.0,
                'trade_count': 0
            }

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
        
        # DEX Checks
        if self.dexscreener_collector:
            try:
                results['dex']['dexscreener'] = await self.dexscreener_collector.health_check()
                if not results['dex']['dexscreener']:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"Dexscreener health check failed: {e}")
                results['dex']['dexscreener'] = False
                results['overall'] = 'degraded'

        if self.birdeye_collector:
            try:
                results['dex']['birdeye'] = await self.birdeye_collector.health_check()
                if not results['dex']['birdeye']:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"Birdeye health check failed: {e}")
                results['dex']['birdeye'] = False
                results['overall'] = 'degraded'

        if self.helius_collector:
            try:
                results['dex']['helius'] = await self.helius_collector.health_check()
                if not results['dex']['helius']:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"Helius health check failed: {e}")
                results['dex']['helius'] = False
                results['overall'] = 'degraded'

        if self.solana_dex_collector:
            try:
                results['dex']['solana_bitquery'] = await self.solana_dex_collector.health_check()
                if not results['dex']['solana_bitquery']:
                    results['overall'] = 'degraded'
            except Exception as e:
                logger.error(f"SolanaDex (Bitquery) health check failed: {e}")
                results['dex']['solana_bitquery'] = False
                results['overall'] = 'degraded'

        # DEX Exchanges inherit health
        # Hier könntest du entscheiden, welche Quelle die "Gesundheit" bestimmt.
        # Annahme: Die Quelle, die für Trades zuständig ist, bestimmt die Gesundheit.
        for dex_name in self.dex_collectors.keys():
            primary_trade_collector = self.dex_collectors[dex_name]
            collector_name_key = 'unknown'
            if primary_trade_collector == self.helius_collector:
                collector_name_key = 'helius'
            elif primary_trade_collector == self.solana_dex_collector:
                collector_name_key = 'solana_bitquery'
            # Wenn Dexscreener nur für OHLCV genutzt wird, beeinflusst es die Trade-Gesundheit nicht direkt.
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
        
        # Schließe DEX Collector
        for collector in [self.dexscreener_collector, self.birdeye_collector, self.helius_collector, self.solana_dex_collector]:
            if collector:
                try: await collector.close()
                except: pass

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
