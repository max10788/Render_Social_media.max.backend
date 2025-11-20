# app/core/price_movers/collectors/dexscreener_collector.py
# OPTIMIZED VERSION with proper API usage and caching

import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from .base import BaseCollector

logger = logging.getLogger(__name__)

class DexscreenerCollector(BaseCollector):
    """
    Optimized Dexscreener Collector with proper API usage and caching.
    - Uses token search endpoint to find pairs
    - Caches pool addresses for faster lookups
    - Intelligent rate limit handling
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.base_url = "https://api.dexscreener.com/latest/dex"
        self.blockchain = "solana"
        
        # Cache für Pool-Adressen und Token-Infos
        self._pool_cache: Dict[str, Tuple[str, datetime]] = {}  # symbol -> (pool_address, timestamp)
        self._token_cache: Dict[str, Dict] = {}  # token_address -> token_info
        self._cache_ttl = timedelta(hours=1)  # Cache für 1 Stunde
        
        # Rate limit tracking
        self._last_request_time = datetime.now()
        self._request_count = 0
        self._rate_limit_reset = datetime.now()
        
        # Known token mappings
        self.TOKEN_MAP = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
            'BONK': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',
            'JTO': 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL',
            'JUP': 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',
            'WIF': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
            'PYTH': 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3',
        }
        
        logger.info("✅ Optimized Dexscreener Collector initialized")
        self._is_initialized = True

    async def _rate_limit_check(self):
        """Check and enforce rate limits"""
        now = datetime.now()
        
        # Reset counter every minute
        if now - self._rate_limit_reset > timedelta(minutes=1):
            self._request_count = 0
            self._rate_limit_reset = now
        
        # If we've made too many requests, wait
        if self._request_count >= 30:  # Max 30 requests per minute (conservative)
            wait_time = 60 - (now - self._rate_limit_reset).seconds
            if wait_time > 0:
                logger.debug(f"Rate limit reached, waiting {wait_time}s")
                await asyncio.sleep(wait_time)
                self._request_count = 0
                self._rate_limit_reset = datetime.now()
        
        # Ensure minimum time between requests (100ms)
        time_since_last = (now - self._last_request_time).total_seconds()
        if time_since_last < 0.1:
            await asyncio.sleep(0.1 - time_since_last)
        
        self._last_request_time = datetime.now()
        self._request_count += 1

    async def _find_pool_address(self, symbol: str) -> Optional[str]:
        """
        Find the pool address for a given symbol pair.
        Uses cache and intelligent search.
        """
        # Check cache first
        if symbol in self._pool_cache:
            pool_addr, cache_time = self._pool_cache[symbol]
            if datetime.now() - cache_time < self._cache_ttl:
                logger.debug(f"Using cached pool for {symbol}: {pool_addr[:8]}...")
                return pool_addr
        
        # Parse symbol
        try:
            base_token, quote_token = symbol.upper().split('/')
        except ValueError:
            logger.error(f"Invalid symbol format: {symbol}")
            return None
        
        # Get token addresses
        base_addr = self.TOKEN_MAP.get(base_token)
        quote_addr = self.TOKEN_MAP.get(quote_token)
        
        if not base_addr:
            logger.warning(f"Unknown base token: {base_token}")
            return None
        
        # Search by token address
        await self._rate_limit_check()
        
        try:
            search_url = f"{self.base_url}/tokens/{base_addr}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(search_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        logger.warning(f"Token search failed for {base_token}: {response.status}")
                        return None
                    
                    data = await response.json()
                    pairs = data.get('pairs', [])
                    
                    if not pairs:
                        logger.warning(f"No pairs found for {base_token}")
                        return None
                    
                    # Find the best matching pair
                    best_pair = None
                    highest_liquidity = 0
                    
                    for pair in pairs:
                        # Check if it's on Solana
                        if pair.get('chainId') != 'solana':
                            continue
                        
                        # Check if it matches our quote token
                        pair_quote = pair.get('quoteToken', {}).get('symbol', '').upper()
                        if quote_addr and pair_quote == quote_token:
                            # Prefer pairs with higher liquidity
                            liquidity = float(pair.get('liquidity', {}).get('usd', 0))
                            if liquidity > highest_liquidity:
                                highest_liquidity = liquidity
                                best_pair = pair
                    
                    # If no exact match, take the pair with highest liquidity
                    if not best_pair and pairs:
                        for pair in pairs:
                            if pair.get('chainId') == 'solana':
                                liquidity = float(pair.get('liquidity', {}).get('usd', 0))
                                if liquidity > highest_liquidity:
                                    highest_liquidity = liquidity
                                    best_pair = pair
                    
                    if best_pair:
                        pool_address = best_pair.get('pairAddress')
                        if pool_address:
                            # Cache the result
                            self._pool_cache[symbol] = (pool_address, datetime.now())
                            logger.info(f"Found pool for {symbol}: {pool_address[:8]}... (liquidity: ${highest_liquidity:,.0f})")
                            return pool_address
                    
                    logger.warning(f"No suitable pair found for {symbol}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error finding pool for {symbol}: {e}")
            return None

    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch OHLCV candle data for a specific timestamp.
        Optimized with proper pool resolution and caching.
        """
        logger.debug(f"DexscreenerCollector.fetch_candle_data: {symbol} {timeframe} @ {timestamp}")
        
        # Find pool address
        pool_address = await self._find_pool_address(symbol)
        if not pool_address:
            logger.debug(f"No pool found for {symbol}, returning empty candle")
            return self._empty_candle(timestamp)
        
        # Map timeframe to Dexscreener format
        # Dexscreener supports: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w
        timeframe_map = {
            '1m': ('1m', 60),
            '5m': ('5m', 300),
            '15m': ('15m', 900),
            '30m': ('30m', 1800),
            '1h': ('1h', 3600),
            '4h': ('4h', 14400),
            '1d': ('1d', 86400),
            '1w': ('1w', 604800),
        }
        
        ds_timeframe, interval_seconds = timeframe_map.get(timeframe, ('5m', 300))
        
        # Dexscreener doesn't have a direct candle endpoint with timestamp
        # We need to get the pair info which includes recent price data
        await self._rate_limit_check()
        
        try:
            pair_url = f"{self.base_url}/pairs/solana/{pool_address}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(pair_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 429:
                        logger.warning("Rate limited by Dexscreener, backing off...")
                        await asyncio.sleep(2)
                        return self._empty_candle(timestamp)
                    
                    if response.status != 200:
                        logger.warning(f"Failed to get pair data: {response.status}")
                        return self._empty_candle(timestamp)
                    
                    data = await response.json()
                    pair = data.get('pair', {})
                    
                    if not pair:
                        return self._empty_candle(timestamp)
                    
                    # Extract price data from pair info
                    # Dexscreener provides current price and 24h price changes
                    price_usd = float(pair.get('priceUsd', 0))
                    
                    if price_usd == 0:
                        return self._empty_candle(timestamp)
                    
                    # Use price change data to estimate OHLC
                    # This is an approximation since we don't have historical candles
                    price_change_5m = float(pair.get('priceChange', {}).get('m5', 0))
                    price_change_1h = float(pair.get('priceChange', {}).get('h1', 0))
                    volume_24h = float(pair.get('volume', {}).get('h24', 0))
                    
                    # Estimate based on timeframe
                    if timeframe == '5m':
                        change_pct = price_change_5m
                    elif timeframe in ['1h', '4h']:
                        change_pct = price_change_1h
                    else:
                        change_pct = price_change_5m  # Default to 5m
                    
                    # Calculate approximate OHLC
                    open_price = price_usd / (1 + change_pct / 100)
                    high_price = max(price_usd, open_price) * 1.001  # Add small variance
                    low_price = min(price_usd, open_price) * 0.999
                    
                    # Estimate volume for this timeframe
                    # Rough approximation: distribute 24h volume across periods
                    periods_in_24h = 86400 / interval_seconds
                    estimated_volume = volume_24h / periods_in_24h
                    
                    return {
                        'timestamp': timestamp,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': price_usd,
                        'volume': estimated_volume,
                        'volume_usd': estimated_volume * price_usd,
                        'trade_count': 0,  # Not available from this endpoint
                        'source': 'dexscreener_estimated'  # Flag that this is estimated
                    }
                    
        except Exception as e:
            logger.error(f"❌ Dexscreener error fetching candle: {e}")
            return self._empty_candle(timestamp)

    async def fetch_ohlcv_batch(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch multiple candles efficiently.
        Since Dexscreener doesn't have historical candle API, we return current data only.
        """
        logger.info(f"Dexscreener batch OHLCV: {symbol} {timeframe} ({start_time} to {end_time})")
        
        # For now, just return a single candle with current data
        # This is a limitation of the free Dexscreener API
        current_candle = await self.fetch_candle_data(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=end_time  # Use end time for most recent data
        )
        
        if current_candle.get('open', 0) > 0:
            return [current_candle]
        
        return []

    def _empty_candle(self, timestamp: datetime) -> Dict[str, Any]:
        """Helper function for empty candle"""
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

    async def fetch_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None
    ) -> list:
        """
        Dexscreener doesn't provide trade-level data in the free API.
        """
        logger.debug("DexscreenerCollector: fetch_trades not supported in free tier")
        return []

    async def health_check(self) -> bool:
        """
        Health check by trying to fetch SOL/USDC pair info
        """
        try:
            # Use a known active pool for health check
            test_pool = "HVFpsSP4QsC8gFfsFWwYcdmvt3FepDRB6U9YStack82p"  # SOL/USDC on Raydium
            
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/pairs/solana/{test_pool}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"❌ Dexscreener health check failed: {e}")
            return False

    async def close(self):
        """Clean up resources"""
        self._pool_cache.clear()
        self._token_cache.clear()

    def clear_cache(self):
        """Manually clear the cache if needed"""
        self._pool_cache.clear()
        self._token_cache.clear()
        logger.info("Dexscreener cache cleared")


# Add missing import at the top
import asyncio
