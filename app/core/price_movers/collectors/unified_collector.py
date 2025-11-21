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
        Initialisiert DEX Collectors mit ETH + Solana Support
        
        Priorität für OHLCV:
        1. DEXSCREENER (kostenlos, current only)
        2. MORALIS (Solana + Ethereum, historisch!)
        3. BIRDEYE (Solana only, wenn nicht suspended)
        4. SOLANADEX (Bitquery)
        5. HELIUS (Solana fallback)
        
        Priorität für Trades:
        1. HELIUS (Solana)
        2. SOLANADEX (Bitquery Solana)
        """
        
        # 1. Dexscreener (OHLCV only, no trades)
        try:
            self.dexscreener_collector = DexscreenerCollector()
            logger.info("✅ Dexscreener Collector initialized (OHLCV only)")
        except Exception as e:
            logger.error(f"❌ Dexscreener Collector failed: {e}")
            self.dexscreener_collector = None
        
        # 2. Moralis (OHLCV for Solana + Ethereum!)
        moralis_keys = [
            api_keys.get('moralis'),
            api_keys.get('moralis_fallback'),
            api_keys.get('moralis_fallback2')
        ]
        moralis_keys = [k for k in moralis_keys if k]  # Remove None
        
        if moralis_keys:
            try:
                from .moralis_collector import MoralisCollector
                self.moralis_collector = MoralisCollector(
                    api_keys=moralis_keys,
                    config={'max_requests_per_minute': 100}
                )
                logger.info(f"✅ Moralis Collector initialized with {len(moralis_keys)} keys (Solana + Ethereum)")
            except Exception as e:
                logger.error(f"❌ Moralis Collector failed: {e}")
                self.moralis_collector = None
        else:
            self.moralis_collector = None
            logger.info("ℹ️ Moralis API Keys not provided")
        
        # 3. Birdeye (Solana OHLCV only)
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
            logger.info("ℹ️ Birdeye API Key not provided")
        
        # 4. Helius (Solana Trades + OHLCV fallback)
        helius_key = api_keys.get('helius')
        if helius_key:
            try:
                from .helius_collector import create_helius_collector
                self.helius_collector = create_helius_collector(
                    api_key=helius_key,
                    birdeye_collector=self.birdeye_collector,
                    config={
                        'max_requests_per_second': 5,
                        'cache_ttl_seconds': 300,
                    }
                )
                logger.info("✅ Helius Collector initialized (Solana Trades + OHLCV)")
            except Exception as e:
                logger.error(f"❌ Helius Collector failed: {e}")
                self.helius_collector = None
        else:
            self.helius_collector = None
        
        # 5. Bitquery (SolanaDexCollector for Solana)
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
            self.solana_dex_collector = None
            logger.info("ℹ️ Bitquery API Key not provided")
        
        # --- Assign Trade Collectors to DEX Exchanges ---
        primary_trade_collector = self.helius_collector or self.solana_dex_collector
        
        if primary_trade_collector:
            # Solana DEXes
            for dex in [SupportedDEX.JUPITER, SupportedDEX.RAYDIUM, SupportedDEX.ORCA]:
                self.dex_collectors[dex.value] = primary_trade_collector
            
            logger.info(
                f"✅ Solana DEX Trade Collectors set to: {primary_trade_collector.__class__.__name__}"
            )
        else:
            logger.warning("⚠️ No Solana Trade Collectors available!")
        
        # Ethereum DEXes (Moralis for OHLCV, no trades yet)
        if self.moralis_collector:
            # For now, we only have OHLCV for Ethereum via Moralis
            # Trades would need separate implementation
            logger.info("✅ Ethereum OHLCV available via Moralis (trades not implemented yet)")
        
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
        Fetcht Candle-Daten mit Multi-Chain Support
        
        Priorität für Solana:
        1. Dexscreener (current only)
        2. Birdeye (wenn healthy)
        3. SolanaDex (Bitquery)
        4. Helius (last resort)
        
        Priorität für EVM (Ethereum, BSC, Polygon, etc.):
        1. Moralis (primary für alle EVM chains)
        2. (weitere Quellen können hinzugefügt werden)
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
    
        # DEX: Multi-Chain Routing
        elif exchange in self.dex_collectors or exchange in ['uniswap', 'uniswapv2', 'uniswapv3', 'sushiswap', 'pancakeswap', 'quickswap', 'traderjoe']:
            
            # Detect blockchain from DEX name
            solana_dexes = ['jupiter', 'raydium', 'orca']
            evm_dexes = {
                'ethereum': ['uniswap', 'uniswapv2', 'uniswapv3', 'sushiswap'],
                'bsc': ['pancakeswap', 'pancakeswapv2', 'pancakeswapv3'],
                'polygon': ['quickswap', 'sushiswap'],
                'avalanche': ['traderjoe', 'pangolin'],
                'arbitrum': ['uniswapv3', 'sushiswap', 'camelot'],
                'optimism': ['uniswapv3', 'velodrome'],
                'base': ['uniswapv3', 'aerodrome', 'baseswap'],
                'fantom': ['spookyswap', 'spiritswap']
            }
            
            # Determine blockchain
            blockchain = None
            if exchange in solana_dexes:
                blockchain = 'solana'
            else:
                # Check EVM chains
                for chain, dexes in evm_dexes.items():
                    if exchange in dexes:
                        blockchain = chain
                        break
                
                if not blockchain:
                    blockchain = 'ethereum'  # Default
                    logger.warning(f"Unknown DEX '{exchange}', defaulting to Ethereum")
            
            logger.info(f"🔍 Fetching {symbol} from {exchange} on {blockchain}")
            
            # Calculate time range
            now = datetime.now(timezone.utc)
            time_diff = (now - timestamp).total_seconds() / 3600
            is_recent = time_diff < 1
            
            # ============ SOLANA CHAIN ============
            if blockchain == 'solana':
                
                # 1. Try Dexscreener for current
                if is_recent and self.dexscreener_collector:
                    logger.info("🎯 Strategy: Dexscreener (Solana, current)")
                    try:
                        candle = await self.dexscreener_collector.fetch_candle_data(
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=timestamp
                        )
                        
                        if candle and candle.get('open', 0) > 0:
                            logger.info("✅ Dexscreener: Got current candle")
                            return candle
                    except Exception as e:
                        logger.debug(f"Dexscreener failed: {e}")
                
                # 2. Try Birdeye
                if self.birdeye_collector and getattr(self, 'birdeye_healthy_at_init', True):
                    logger.info("🎯 Strategy: Birdeye (Solana)")
                    try:
                        candle = await self.birdeye_collector.fetch_candle_data(
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=timestamp
                        )
                        
                        if candle and candle.get('open', 0) > 0:
                            logger.info("✅ Birdeye: Got Solana candle")
                            return candle
                    except Exception as e:
                        logger.warning(f"Birdeye failed: {e}")
                
                # 3. Try SolanaDex (Bitquery)
                if self.solana_dex_collector:
                    logger.info("🎯 Strategy: SolanaDex/Bitquery")
                    try:
                        candle = await self.solana_dex_collector.fetch_candle_data(
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=timestamp
                        )
                        
                        if candle and candle.get('open', 0) > 0:
                            logger.info("✅ SolanaDex: Got candle")
                            return candle
                    except Exception as e:
                        logger.warning(f"SolanaDex failed: {e}")
                
                # 4. Last resort: Helius
                if self.helius_collector:
                    logger.info("🎯 Strategy: Helius (last resort)")
                    try:
                        candle = await self.helius_collector.fetch_candle_data(
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=timestamp
                        )
                        
                        if candle and candle.get('open', 0) > 0:
                            logger.info("✅ Helius: Got candle")
                            return candle
                    except Exception as e:
                        logger.warning(f"Helius failed: {e}")
            
            # ============ EVM CHAINS (Ethereum, BSC, etc.) ============
            else:
                
                # Use Moralis (only source for EVM)
                if self.moralis_collector:
                    logger.info(f"🎯 Strategy: Moralis ({blockchain})")
                    try:
                        candle = await self.moralis_collector.fetch_candle_data(
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=timestamp,
                            blockchain=blockchain,
                            dex_exchange=exchange
                        )
                        
                        if candle and candle.get('open', 0) > 0:
                            logger.info(f"✅ Moralis: Got {blockchain} candle")
                            return candle
                    except Exception as e:
                        logger.warning(f"Moralis {blockchain} failed: {e}")
            
            # No data from any source
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
