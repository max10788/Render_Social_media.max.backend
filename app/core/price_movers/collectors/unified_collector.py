# app/core/price_movers/collectors/unified_collector.py (korrigierte und vollständige Version)
"""
Unified Collector - PRODUCTION VERSION

🎯 RESPONSIBILITIES:
- Orchestrates multiple DEX collectors
- Aggregates data from Helius, Dexscreener, etc.
- Provides unified interface
- Handles fallbacks gracefully

🔧 IMPROVEMENTS:
- Robust error handling
- Comprehensive logging
- Fallback strategy
- Performance monitoring
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict

from .helius_collector import HeliusCollector, create_helius_collector
from .dexscreener_collector import DexscreenerCollector
from ..utils.constants import BlockchainNetwork


logger = logging.getLogger(__name__)


class UnifiedCollector:
    """
    Unified DEX Collector - PRODUCTION VERSION
    
    🎯 Features:
    - Multi-source data aggregation
    - Intelligent fallback strategy
    - Error isolation (one failure doesn't break everything)
    - Performance monitoring
    - Comprehensive logging
    
    Architecture:
    1. Primary: Helius (fast, pool-based)
    2. Fallback: Dexscreener (slower but reliable)
    3. Aggregation: Combine and deduplicate
    """
    
    def __init__(
        self,
        helius_api_key: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Unified Collector
        
        Args:
            helius_api_key: Helius API key
            config: Configuration dictionary
        """
        self.config = config or {}
        
        # Initialize collectors
        self.helius: Optional[HeliusCollector] = None
        self.dexscreener: Optional[DexscreenerCollector] = None
        
        # Setup Helius if API key provided
        if helius_api_key:
            try:
                self.helius = create_helius_collector(
                    api_key=helius_api_key,
                    config=self.config.get('helius', {})
                )
                logger.info("✅ Helius collector initialized")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Helius: {e}")
        else:
            logger.warning("⚠️ Helius API key not provided, Helius disabled")
        
        # Setup Dexscreener
        try:
            self.dexscreener = DexscreenerCollector(
                config=self.config.get('dexscreener', {})
            )
            logger.info("✅ Dexscreener collector initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Dexscreener: {e}")
        
        # Performance stats
        self._stats = {
            'helius': {'success': 0, 'errors': 0, 'fallbacks': 0},
            'dexscreener': {'success': 0, 'errors': 0},
            'combined': {'success': 0, 'errors': 0}
        }
        
        logger.info("🚀 UnifiedCollector initialized (PRODUCTION)")
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch candle data with intelligent fallback
        
        Strategy:
        1. Try Helius (fast, pool-based)
        2. On failure: Try Dexscreener
        3. On failure: Return empty candle with error flag
        
        Args:
            symbol: Trading pair (e.g., SOL/USDT)
            timeframe: Timeframe (e.g., 5m)
            timestamp: Candle timestamp
            
        Returns:
            Candle data dictionary with metadata
        """
        logger.info(f"📊 Fetching candle: {symbol} {timeframe} @ {timestamp}")
        
        # Ensure timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        candle = None
        source = None
        error = None
        
        # 1️⃣ TRY HELIUS FIRST
        if self.helius:
            try:
                candle = await self._fetch_helius_candle(
                    symbol, timeframe, timestamp
                )
                
                if self._is_valid_candle(candle):
                    source = 'helius'
                    self._stats['helius']['success'] += 1
                    logger.info(f"✅ Candle from Helius: {symbol}")
                else:
                    logger.warning(f"⚠️ Helius returned invalid candle")
                    candle = None
                    
            except Exception as e:
                error = str(e)
                self._stats['helius']['errors'] += 1
                logger.warning(f"⚠️ Helius failed: {e}")
        
        # 2️⃣ FALLBACK TO DEXSCREENER
        if not candle and self.dexscreener:
            try:
                logger.info(f"🔄 Falling back to Dexscreener for {symbol}")
                self._stats['helius']['fallbacks'] += 1
                
                candle = await self._fetch_dexscreener_candle(
                    symbol, timeframe, timestamp
                )
                
                if self._is_valid_candle(candle):
                    source = 'dexscreener'
                    self._stats['dexscreener']['success'] += 1
                    logger.info(f"✅ Candle from Dexscreener: {symbol}")
                else:
                    logger.warning(f"⚠️ Dexscreener returned invalid candle")
                    candle = None
                    
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
                f"❌ Failed to fetch candle for {symbol}: {error or 'No data'}"
            )
            return empty_candle
    
    async def _fetch_helius_candle(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Optional[Dict[str, Any]]:
        """Fetch candle from Helius with timeout"""
        try:
            return await asyncio.wait_for(
                self.helius.fetch_candle_data(symbol, timeframe, timestamp),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning("⏱️ Helius timeout")
            raise Exception("Helius timeout")
    
    async def _fetch_dexscreener_candle(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Optional[Dict[str, Any]]:
        """Fetch candle from Dexscreener with timeout"""
        try:
            return await asyncio.wait_for(
                self.dexscreener.fetch_candle_data(symbol, timeframe, timestamp),
                timeout=15.0
            )
        except asyncio.TimeoutError:
            logger.warning("⏱️ Dexscreener timeout")
            raise Exception("Dexscreener timeout")
    
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
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades with aggregation from multiple sources
        
        Args:
            symbol: Trading pair
            start_time: Start time
            end_time: End time
            limit: Maximum trades per source
            
        Returns:
            Combined and deduplicated trades
        """
        logger.info(f"🔍 Fetching trades: {symbol} ({start_time} to {end_time})")
        
        all_trades = []
        
        # Fetch from Helius
        if self.helius:
            try:
                helius_trades = await asyncio.wait_for(
                    self.helius.fetch_trades(
                        symbol, start_time, end_time, limit
                    ),
                    timeout=10.0
                )
                
                if helius_trades:
                    logger.info(f"✅ Helius: {len(helius_trades)} trades")
                    all_trades.extend(helius_trades)
                    
            except Exception as e:
                logger.warning(f"⚠️ Helius trades failed: {e}")
        
        # Fetch from Dexscreener (if needed)
        if self.dexscreener and len(all_trades) < limit / 2:
            try:
                dex_trades = await asyncio.wait_for(
                    self.dexscreener.fetch_trades(
                        symbol, start_time, end_time, limit
                    ),
                    timeout=15.0
                )
                
                if dex_trades:
                    logger.info(f"✅ Dexscreener: {len(dex_trades)} trades")
                    all_trades.extend(dex_trades)
                    
            except Exception as e:
                logger.warning(f"⚠️ Dexscreener trades failed: {e}")
        
        # Deduplicate by signature/id
        unique_trades = self._deduplicate_trades(all_trades)
        
        # Sort by timestamp
        unique_trades.sort(key=lambda t: t.get('timestamp', datetime.min))
        
        logger.info(
            f"✅ Total trades: {len(unique_trades)} "
            f"(from {len(all_trades)} raw)"
        )
        
        return unique_trades[:limit]
    
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
        if self.dexscreener:
            try:
                price = await asyncio.wait_for(
                    self.dexscreener.fetch_current_price(symbol),
                    timeout=5.0
                )
                if price and price > 0:
                    return price
            except Exception as e:
                logger.warning(f"⚠️ Dexscreener price failed: {e}")
        
        # Fallback to Helius
        if self.helius:
            try:
                # Get latest candle
                now = datetime.now(timezone.utc)
                candle = await self.fetch_candle_data(symbol, '1m', now)
                
                if candle and candle.get('close', 0) > 0:
                    return candle['close']
                    
            except Exception as e:
                logger.warning(f"⚠️ Helius price failed: {e}")
        
        return None
    
    async def health_check(self) -> Dict[str, bool]:
        """
        Check health of all collectors
        
        Returns:
            Dictionary with health status
        """
        health = {}
        
        if self.helius:
            try:
                health['helius'] = await asyncio.wait_for(
                    self.helius.health_check(),
                    timeout=5.0
                )
            except Exception as e:
                logger.error(f"❌ Helius health check failed: {e}")
                health['helius'] = False
        
        if self.dexscreener:
            try:
                health['dexscreener'] = await asyncio.wait_for(
                    self.dexscreener.health_check(),
                    timeout=5.0
                )
            except Exception as e:
                logger.error(f"❌ Dexscreener health check failed: {e}")
                health['dexscreener'] = False
        
        logger.info(f"🏥 Health check: {health}")
        return health
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        stats = {
            'collectors': self._stats.copy(),
            'available': {
                'helius': self.helius is not None,
                'dexscreener': self.dexscreener is not None
            }
        }
        
        # Add collector-specific stats
        if self.helius:
            stats['helius_details'] = self.helius.get_stats()
        
        return stats
    
    async def close(self):
        """Clean up all collectors"""
        if self.helius:
            await self.helius.close()
        
        if self.dexscreener:
            await self.dexscreener.close()
        
        logger.info(f"📊 Final stats: {self.get_stats()}")
        logger.info("🔌 UnifiedCollector closed")


# Factory function for easy instantiation
def create_unified_collector(
    helius_api_key: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
) -> UnifiedCollector:
    """
    Create production-ready Unified Collector
    
    Args:
        helius_api_key: Helius API key
        config: Configuration dictionary
        
    Returns:
        UnifiedCollector instance
    """
    return UnifiedCollector(
        helius_api_key=helius_api_key,
        config=config or {}
    )
