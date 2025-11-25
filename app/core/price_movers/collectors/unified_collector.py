"""
Unified Collector - PRODUCTION VERSION (ULTRA-FLEXIBLE für dependencies.py)

🎯 RESPONSIBILITIES:
- Orchestrates multiple DEX collectors (Helius, Dexscreener)
- Orchestrates multiple CEX collectors (Binance, Bitget, Kraken)
- Aggregates data from all sources
- Provides unified interface
- Handles fallbacks gracefully

🔧 IMPROVEMENTS:
- Robust error handling
- Comprehensive logging
- Fallback strategy
- Performance monitoring
- ✅ ULTRA-FLEXIBLE: Akzeptiert alle Parameter (cex_credentials, dex_api_keys, etc.)
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict


logger = logging.getLogger(__name__)


class UnifiedCollector:
    """
    Unified DEX + CEX Collector - PRODUCTION VERSION
    
    🎯 Features:
    - Multi-source data aggregation (DEX + CEX)
    - Intelligent fallback strategy
    - Error isolation (one failure doesn't break everything)
    - Performance monitoring
    - Comprehensive logging
    - ✅ ULTRA-FLEXIBLE initialization (akzeptiert alle Parameter)
    
    Architecture:
    1. Primary: Helius (fast, token-based) for DEX
    2. Fallback: Dexscreener (slower but reliable) for DEX
    3. CEX: Binance/Bitget/Kraken for historical data
    4. Aggregation: Combine and deduplicate
    """
    
    def __init__(
        self,
        helius_collector: Optional[Any] = None,
        dexscreener_collector: Optional[Any] = None,
        moralis_collector: Optional[Any] = None,
        birdeye_collector: Optional[Any] = None,
        bitquery_collector: Optional[Any] = None,
        cex_collectors: Optional[Dict[str, Any]] = None,
        cex_credentials: Optional[Dict[str, Any]] = None,
        dex_api_keys: Optional[Dict[str, Any]] = None,  # ✅ NEU: für dependencies.py
        config: Optional[Dict[str, Any]] = None,
        **kwargs  # ✅ ULTRA-FLEXIBLE: ignoriert unbekannte Parameter
    ):
        """
        Initialize Unified Collector (ULTRA-FLEXIBLE)
        
        Args:
            helius_collector: Helius DEX collector
            dexscreener_collector: Dexscreener collector
            moralis_collector: Moralis collector
            birdeye_collector: Birdeye collector  
            bitquery_collector: Bitquery collector
            cex_collectors: Dictionary of already initialized CEX collectors
            cex_credentials: Dictionary of CEX credentials (will initialize collectors)
            dex_api_keys: Dictionary of DEX API keys (für dependencies.py)
            config: Configuration dictionary
            **kwargs: Alle anderen Parameter werden ignoriert
        """
        self.config = config or {}
        
        # Log ignored kwargs for debugging
        if kwargs:
            logger.debug(f"ℹ️ Ignoring unused parameters: {list(kwargs.keys())}")
        
        # Initialize DEX collectors
        self.helius_collector = helius_collector
        self.dexscreener_collector = dexscreener_collector
        self.moralis_collector = moralis_collector
        self.birdeye_collector = birdeye_collector
        self.bitquery_collector = bitquery_collector
        
        # Initialize CEX collectors
        if cex_collectors:
            # Already initialized collectors passed in
            self.cex_collectors = cex_collectors
            logger.info(f"✅ Using pre-initialized CEX collectors: {list(cex_collectors.keys())}")
        elif cex_credentials:
            # Initialize CEX collectors from credentials
            self.cex_collectors = self._initialize_cex_collectors(cex_credentials)
            logger.info(f"✅ Initialized CEX collectors from credentials: {list(self.cex_collectors.keys())}")
        else:
            self.cex_collectors = {}
            logger.warning("⚠️ No CEX collectors or credentials provided")
        
        # Performance stats
        self._stats = {
            'helius': {'success': 0, 'errors': 0, 'fallbacks': 0},
            'dexscreener': {'success': 0, 'errors': 0},
            'moralis': {'success': 0, 'errors': 0},
            'birdeye': {'success': 0, 'errors': 0},
            'bitquery': {'success': 0, 'errors': 0},
            'cex': {name: {'success': 0, 'errors': 0} for name in self.cex_collectors.keys()},
            'combined': {'success': 0, 'errors': 0}
        }
        
        logger.info(
            f"🚀 UnifiedCollector initialized (PRODUCTION) - "
            f"DEX: {bool(helius_collector)}/{bool(dexscreener_collector)}, "
            f"CEX: {len(self.cex_collectors)} exchanges"
        )
    
    def _initialize_cex_collectors(self, cex_credentials: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize CEX collectors using CCXT"""
        collectors = {}
        
        logger.info(f"🔧 Initializing CEX with CCXT: {list(cex_credentials.keys())}")
        
        try:
            import ccxt
        except ImportError:
            logger.error("❌ CCXT not installed! Run: pip install ccxt")
            return collectors
        
        # Binance
        binance_creds = cex_credentials.get('binance', {})
        try:
            config = {
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'}
            }
            if binance_creds.get('api_key'):
                config['apiKey'] = binance_creds['api_key']
            if binance_creds.get('api_secret'):
                config['secret'] = binance_creds['api_secret']
            
            collectors['binance'] = ccxt.binance(config)
            logger.info("✅ Binance US initialized (CCXT)")
        except Exception as e:
            logger.error(f"❌ Binance init failed: {e}")
        
        # Bitget
        bitget_creds = cex_credentials.get('bitget', {})
        if bitget_creds.get('api_key') and bitget_creds.get('api_secret'):
            try:
                collectors['bitget'] = ccxt.bitget({
                    'apiKey': bitget_creds['api_key'],
                    'secret': bitget_creds['api_secret'],
                    'password': bitget_creds.get('passphrase', ''),
                    'enableRateLimit': True,
                })
                logger.info("✅ Bitget initialized (CCXT)")
            except Exception as e:
                logger.error(f"❌ Bitget init failed: {e}")
        
        # Kraken
        kraken_creds = cex_credentials.get('kraken', {})
        if kraken_creds.get('api_key') and kraken_creds.get('api_secret'):
            try:
                collectors['kraken'] = ccxt.kraken({
                    'apiKey': kraken_creds['api_key'],
                    'secret': kraken_creds['api_secret'],
                    'enableRateLimit': True,
                })
                logger.info("✅ Kraken initialized (CCXT)")
            except Exception as e:
                logger.error(f"❌ Kraken init failed: {e}")
        
        return collectors
    
    async def fetch_candle_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch candle data with intelligent routing and fallback
        
        Strategy:
        1. Determine if CEX or DEX based on exchange name
        2. For DEX: Try Helius → Dexscreener → Empty
        3. For CEX: Route to appropriate exchange
        
        Args:
            exchange: Exchange name (jupiter/raydium/binance/bitget/etc)
            symbol: Trading pair (e.g., SOL/USDT)
            timeframe: Timeframe (e.g., 5m)
            timestamp: Candle timestamp
            
        Returns:
            Candle data dictionary with metadata
        """
        logger.info(f"📊 Fetching candle: {exchange} {symbol} {timeframe} @ {timestamp}")
        
        # Ensure timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Route based on exchange type
        is_dex = self._is_dex_exchange(exchange)
        
        if is_dex:
            return await self._fetch_dex_candle(symbol, timeframe, timestamp)
        else:
            return await self._fetch_cex_candle(exchange, symbol, timeframe, timestamp)
    
    def _is_dex_exchange(self, exchange: str) -> bool:
        """Check if exchange is a DEX"""
        dex_exchanges = [
            'jupiter', 'raydium', 'orca',  # Solana
            'uniswap', 'uniswapv2', 'uniswapv3', 'sushiswap',  # Ethereum
            'pancakeswap', 'pancakeswapv2', 'pancakeswapv3',  # BSC
        ]
        return exchange.lower() in dex_exchanges
    
    async def _fetch_dex_candle(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch DEX candle with intelligent fallback
        
        Strategy:
        1. Try Helius (fast, token-based)
        2. On failure: Try Dexscreener
        3. On failure: Return empty candle with error flag
        """
        candle = None
        source = None
        error = None
        
        # 1️⃣ TRY HELIUS FIRST
        if self.helius_collector:
            try:
                candle = await asyncio.wait_for(
                    self.helius_collector.fetch_candle_data(
                        symbol, timeframe, timestamp
                    ),
                    timeout=10.0
                )
                
                if self._is_valid_candle(candle):
                    source = 'helius'
                    self._stats['helius']['success'] += 1
                    logger.info(f"✅ Candle from Helius: {symbol}")
                else:
                    logger.warning(f"⚠️ Helius returned invalid candle")
                    candle = None
                    
            except asyncio.TimeoutError:
                error = "Helius timeout"
                self._stats['helius']['errors'] += 1
                logger.warning(f"⏱️ Helius timeout for {symbol}")
            except Exception as e:
                error = str(e)
                self._stats['helius']['errors'] += 1
                logger.warning(f"⚠️ Helius failed: {e}")
        
        # 2️⃣ FALLBACK TO DEXSCREENER
        if not candle and self.dexscreener_collector:
            try:
                logger.info(f"🔄 Falling back to Dexscreener for {symbol}")
                self._stats['helius']['fallbacks'] += 1
                
                candle = await asyncio.wait_for(
                    self.dexscreener_collector.fetch_candle_data(
                        symbol, timeframe, timestamp
                    ),
                    timeout=15.0
                )
                
                if self._is_valid_candle(candle):
                    source = 'dexscreener'
                    self._stats['dexscreener']['success'] += 1
                    logger.info(f"✅ Candle from Dexscreener: {symbol}")
                else:
                    logger.warning(f"⚠️ Dexscreener returned invalid candle")
                    candle = None
                    
            except asyncio.TimeoutError:
                error = "Dexscreener timeout"
                self._stats['dexscreener']['errors'] += 1
                logger.warning(f"⏱️ Dexscreener timeout")
            except Exception as e:
                error = str(e)
                self._stats['dexscreener']['errors'] += 1
                logger.error(f"❌ Dexscreener failed: {e}")
        
        # 3️⃣ FINAL RESULT
        if candle:
            candle['source'] = source
            self._stats['combined']['success'] += 1
            return candle
        else:
            # Return empty candle with error info
            self._stats['combined']['errors'] += 1
            empty_candle = self._create_empty_candle(timestamp)
            empty_candle['error'] = error or "No data available"
            empty_candle['source'] = 'none'
            
            logger.error(
                f"❌ Failed to fetch DEX candle for {symbol}: {error or 'No data'}"
            )
            return empty_candle
    
    async def _fetch_cex_candle(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Fetch CEX candle using CCXT"""
        exchange_lower = exchange.lower()
        
        if exchange_lower not in self.cex_collectors:
            logger.error(f"❌ Unknown CEX: {exchange}")
            return self._create_empty_candle(timestamp)
        
        ccxt_exchange = self.cex_collectors[exchange_lower]
        
        try:
            since = int(timestamp.timestamp() * 1000)
            
            ohlcv = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: ccxt_exchange.fetch_ohlcv(symbol, timeframe, since, 1)
            )
            
            if ohlcv and len(ohlcv) > 0:
                candle = ohlcv[0]
                result = {
                    'timestamp': datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                    'volume': float(candle[5]),
                    'source': f'cex_{exchange_lower}'
                }
                self._stats['cex'][exchange_lower]['success'] += 1
                logger.info(f"✅ Candle from {exchange}: {result['close']}")
                return result
            
            return self._create_empty_candle(timestamp)
            
        except Exception as e:
            self._stats['cex'][exchange_lower]['errors'] += 1
            logger.error(f"❌ {exchange} failed: {e}")
            return self._create_empty_candle(timestamp)
    
    def _is_valid_candle(self, candle: Optional[Dict]) -> bool:
        """
        Check if candle contains valid data
        
        Args:
            candle: Candle dictionary
            
        Returns:
            True if valid, False otherwise
        """
        if not candle:
            return False
        
        # Must have OHLCV data
        required_fields = ['open', 'high', 'low', 'close', 'volume']
        if not all(field in candle for field in required_fields):
            return False
        
        # At least one price must be > 0
        prices = [
            candle.get('open', 0),
            candle.get('high', 0),
            candle.get('low', 0),
            candle.get('close', 0)
        ]
        
        if all(p <= 0 for p in prices):
            return False
        
        # Sanity check: high >= low
        if candle.get('high', 0) < candle.get('low', 0):
            logger.warning("⚠️ Invalid candle: high < low")
            return False
        
        return True
    
    def _create_empty_candle(self, timestamp: datetime) -> Dict[str, Any]:
        """Create empty candle placeholder"""
        return {
            'timestamp': timestamp,
            'open': 0.0,
            'high': 0.0,
            'low': 0.0,
            'close': 0.0,
            'volume': 0.0
        }
    
    async def fetch_trades(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Fetch trades with aggregation from multiple sources
        
        Args:
            exchange: Exchange name (jupiter/raydium/binance/etc)
            symbol: Trading pair
            start_time: Start time
            end_time: End time
            limit: Maximum trades per source
            
        Returns:
            Dictionary with trades and metadata
        """
        logger.info(f"🔍 Fetching trades: {exchange} {symbol} ({start_time} to {end_time})")
        
        # ✅ DEBUG: Log available collectors
        logger.info(f"📊 Available collectors: Helius={bool(self.helius_collector)}, Dexscreener={bool(self.dexscreener_collector)}, CEX={list(self.cex_collectors.keys())}")
        
        # Route based on exchange type
        is_dex = self._is_dex_exchange(exchange)
        logger.info(f"🎯 Exchange '{exchange}' is DEX: {is_dex}")
        
        if is_dex:
            trades = await self._fetch_dex_trades(symbol, start_time, end_time, limit)
        else:
            trades = await self._fetch_cex_trades(exchange, symbol, start_time, end_time, limit)
        
        return {
            'trades': trades,
            'count': len(trades),
            'exchange': exchange,
            'symbol': symbol,
            'start_time': start_time,
            'end_time': end_time
        }
    
    async def _fetch_dex_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch DEX trades with fallback"""
        all_trades = []
        
        logger.info(f"🔄 Fetching DEX trades for {symbol}")
        
        # 1️⃣ FETCH FROM HELIUS (PRIMARY)
        if self.helius_collector:
            try:
                logger.info(f"📡 Calling Helius collector...")
                helius_trades = await asyncio.wait_for(
                    self.helius_collector.fetch_trades(
                        symbol, start_time, end_time, limit
                    ),
                    timeout=30.0  # ✅ Increased timeout for token-based fetching
                )
                
                if helius_trades:
                    logger.info(f"✅ Helius: {len(helius_trades)} trades")
                    all_trades.extend(helius_trades)
                    self._stats['helius']['success'] += 1
                else:
                    logger.warning(f"⚠️ Helius returned 0 trades")
                    
            except asyncio.TimeoutError:
                logger.error(f"⏱️ Helius timeout after 30s")
                self._stats['helius']['errors'] += 1
            except Exception as e:
                logger.error(f"❌ Helius trades failed: {e}", exc_info=True)
                self._stats['helius']['errors'] += 1
        else:
            logger.warning("⚠️ Helius collector not available")
        
        # 2️⃣ FETCH FROM DEXSCREENER (FALLBACK IF NEEDED)
        if self.dexscreener_collector and len(all_trades) < limit / 2:
            try:
                logger.info(f"🔄 Falling back to Dexscreener (have {len(all_trades)} trades so far)")
                dex_trades = await asyncio.wait_for(
                    self.dexscreener_collector.fetch_trades(
                        symbol, start_time, end_time, limit
                    ),
                    timeout=15.0
                )
                
                if dex_trades:
                    logger.info(f"✅ Dexscreener: {len(dex_trades)} trades")
                    all_trades.extend(dex_trades)
                    self._stats['dexscreener']['success'] += 1
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Dexscreener timeout")
                self._stats['dexscreener']['errors'] += 1
            except Exception as e:
                logger.warning(f"⚠️ Dexscreener trades failed: {e}")
                self._stats['dexscreener']['errors'] += 1
        
        # 3️⃣ DEDUPLICATE AND SORT
        unique_trades = self._deduplicate_trades(all_trades)
        unique_trades.sort(key=lambda t: t.get('timestamp', datetime.min))
        
        logger.info(
            f"✅ Total DEX trades: {len(unique_trades)} "
            f"(from {len(all_trades)} raw)"
        )
        
        return unique_trades[:limit]
    
    async def _fetch_cex_trades(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch CEX trades"""
        exchange_lower = exchange.lower()
        
        if exchange_lower not in self.cex_collectors:
            logger.error(f"❌ Unknown CEX exchange: {exchange}")
            return []
        
        collector = self.cex_collectors[exchange_lower]
        
        try:
            trades = await asyncio.wait_for(
                collector.fetch_trades(symbol, start_time, end_time, limit),
                timeout=10.0
            )
            
            logger.info(f"✅ {exchange}: {len(trades)} trades")
            return trades
            
        except Exception as e:
            logger.error(f"❌ {exchange} trades failed: {e}")
            return []
    
    def _deduplicate_trades(
        self, 
        trades: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Remove duplicate trades based on signature/id
        
        Args:
            trades: List of trades
            
        Returns:
            Deduplicated trades
        """
        seen_ids = set()
        unique = []
        
        for trade in trades:
            trade_id = (
                trade.get('signature') or 
                trade.get('transaction_hash') or
                trade.get('id') or 
                trade.get('tx_hash')
            )
            
            if trade_id and trade_id not in seen_ids:
                seen_ids.add(trade_id)
                unique.append(trade)
            elif not trade_id:
                # No ID - keep it (might be aggregated)
                unique.append(trade)
        
        return unique
    
    async def fetch_current_price(self, symbol: str) -> Optional[float]:
        """
        Fetch current price with fallback
        
        Args:
            symbol: Trading pair
            
        Returns:
            Current price or None
        """
        # Try Dexscreener first (more reliable for current price)
        if self.dexscreener_collector:
            try:
                price = await asyncio.wait_for(
                    self.dexscreener_collector.fetch_current_price(symbol),
                    timeout=5.0
                )
                if price and price > 0:
                    return price
            except Exception as e:
                logger.warning(f"⚠️ Dexscreener price failed: {e}")
        
        # Fallback to Helius
        if self.helius_collector:
            try:
                # Get latest candle
                now = datetime.now(timezone.utc)
                candle = await self.fetch_candle_data(
                    'jupiter', symbol, '1m', now
                )
                
                if candle and candle.get('close', 0) > 0:
                    return candle['close']
                    
            except Exception as e:
                logger.warning(f"⚠️ Helius price failed: {e}")
        
        # Try CEX as last resort
        for cex_name, collector in self.cex_collectors.items():
            try:
                price = await asyncio.wait_for(
                    collector.fetch_current_price(symbol),
                    timeout=5.0
                )
                if price and price > 0:
                    logger.info(f"✅ Price from {cex_name}: {price}")
                    return price
            except Exception as e:
                logger.warning(f"⚠️ {cex_name} price failed: {e}")
        
        return None
    
    async def health_check(self) -> Dict[str, bool]:
        """
        Check health of all collectors
        
        Returns:
            Dictionary with health status
        """
        health = {}
        
        # Check Helius
        if self.helius_collector:
            try:
                health['helius'] = await asyncio.wait_for(
                    self.helius_collector.health_check(),
                    timeout=5.0
                )
            except Exception as e:
                logger.error(f"❌ Helius health check failed: {e}")
                health['helius'] = False
        
        # Check Dexscreener
        if self.dexscreener_collector:
            try:
                health['dexscreener'] = await asyncio.wait_for(
                    self.dexscreener_collector.health_check(),
                    timeout=5.0
                )
            except Exception as e:
                logger.error(f"❌ Dexscreener health check failed: {e}")
                health['dexscreener'] = False
        
        # Check CEX collectors
        for cex_name, collector in self.cex_collectors.items():
            try:
                health[f'cex_{cex_name}'] = await asyncio.wait_for(
                    collector.health_check(),
                    timeout=5.0
                )
            except Exception as e:
                logger.error(f"❌ {cex_name} health check failed: {e}")
                health[f'cex_{cex_name}'] = False
        
        logger.info(f"🏥 Health check: {health}")
        return health
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        stats = {
            'collectors': self._stats.copy(),
            'available': {
                'helius': self.helius_collector is not None,
                'dexscreener': self.dexscreener_collector is not None,
                'cex': list(self.cex_collectors.keys())
            }
        }
        
        # Add collector-specific stats
        if self.helius_collector and hasattr(self.helius_collector, 'get_stats'):
            stats['helius_details'] = self.helius_collector.get_stats()
        
        return stats
    
    def list_available_exchanges(self) -> Dict[str, List[str]]:
        """
        List all available exchanges
        
        Returns:
            Dictionary with CEX and DEX exchanges
        """
        return {
            'cex': list(self.cex_collectors.keys()),
            'dex': self._get_available_dex_exchanges()
        }
    
    def _get_available_dex_exchanges(self) -> List[str]:
        """Get list of available DEX exchanges"""
        dex_list = []
        
        if self.helius_collector:
            dex_list.extend(['jupiter', 'raydium', 'orca'])  # Solana DEXs
        
        if self.dexscreener_collector:
            # Dexscreener supports many DEXs
            if 'jupiter' not in dex_list:
                dex_list.extend(['jupiter', 'raydium', 'orca'])
        
        if self.moralis_collector:
            dex_list.extend(['uniswap', 'sushiswap', 'pancakeswap'])
        
        return list(set(dex_list))  # Remove duplicates
    
    def get_collector_for_exchange(self, exchange: str) -> Optional[Any]:
        """
        Get collector instance for a specific exchange
        
        Args:
            exchange: Exchange name
            
        Returns:
            Collector instance or None
        """
        exchange_lower = exchange.lower()
        
        # Check CEX
        if exchange_lower in self.cex_collectors:
            return self.cex_collectors[exchange_lower]
        
        # Check DEX
        dex_exchanges = {
            'jupiter': self.helius_collector,
            'raydium': self.helius_collector,
            'orca': self.helius_collector,
            'dexscreener': self.dexscreener_collector,
        }
        
        return dex_exchanges.get(exchange_lower)
    
    async def close(self):
        """Clean up all collectors"""
        # Close Helius
        if self.helius_collector and hasattr(self.helius_collector, 'close'):
            await self.helius_collector.close()
        
        # Close Dexscreener
        if self.dexscreener_collector and hasattr(self.dexscreener_collector, 'close'):
            await self.dexscreener_collector.close()
        
        # Close CEX collectors
        for cex_name, collector in self.cex_collectors.items():
            if hasattr(collector, 'close'):
                await collector.close()
        
        logger.info(f"📊 Final stats: {self.get_stats()}")
        logger.info("🔌 UnifiedCollector closed")


# Factory function for easy instantiation
def create_unified_collector(
    helius_collector: Optional[Any] = None,
    dexscreener_collector: Optional[Any] = None,
    cex_collectors: Optional[Dict[str, Any]] = None,
    cex_credentials: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    **kwargs
) -> UnifiedCollector:
    """
    Create production-ready Unified Collector
    
    Args:
        helius_collector: Helius collector instance
        dexscreener_collector: Dexscreener collector instance
        cex_collectors: Dictionary of CEX collectors
        cex_credentials: Dictionary of CEX credentials
        config: Configuration dictionary
        **kwargs: Additional parameters (will be ignored)
        
    Returns:
        UnifiedCollector instance
    """
    return UnifiedCollector(
        helius_collector=helius_collector,
        dexscreener_collector=dexscreener_collector,
        cex_collectors=cex_collectors,
        cex_credentials=cex_credentials,
        config=config or {},
        **kwargs
    )
