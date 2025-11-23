"""
Helius Collector - POOL-BASED VERSION (Langfristige Lösung)

🎯 KONZEPT:
Statt Token-Address (USDT) nutzen wir Pool-Address (SOL/USDT Pool)
→ Gibt nur Transaktionen für genau dieses Trading-Pair zurück!

🔧 INTEGRATION:
- Nutzt Dexscreener, um Pool-Adressen zu finden
- Cached Pool-Adressen für Performance
- Fallback zu Token-Address wenn Pool nicht gefunden
"""

import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import time

from .dex_collector import DEXCollector
from ..utils.constants import BlockchainNetwork


logger = logging.getLogger(__name__)


class SimpleCache:
    """Simple in-memory cache with TTL"""
    def __init__(self, ttl_seconds: int = 300):  # 5 min cache for pools
        self.cache = {}
        self.ttl_seconds = ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl_seconds:
                return value
            del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        self.cache[key] = (value, time.time())
    
    def clear(self):
        self.cache.clear()


class HeliusCollector(DEXCollector):
    """
    Helius Collector - POOL-BASED VERSION
    
    🎯 Strategy:
    1. Find pool address for trading pair (via Dexscreener)
    2. Query Helius with pool address instead of token address
    3. Get ONLY transactions for that specific pool
    
    Benefits:
    - ✅ Only relevant transactions (SOL/USDT, not USDT/BTC)
    - ✅ Much higher hit rate
    - ✅ No need for strict time filtering
    """
    
    API_BASE = "https://api-mainnet.helius-rpc.com"
    DEXSCREENER_API = "https://api.dexscreener.com/latest/dex"
    
    TOKEN_MINTS = {
        'SOL': 'So11111111111111111111111111111111111111112',
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    }
    
    def __init__(
        self, 
        api_key: str, 
        config: Optional[Dict[str, Any]] = None,
        dexscreener_collector: Optional[Any] = None
    ):
        super().__init__(
            dex_name="helius",
            blockchain=BlockchainNetwork.SOLANA,
            api_key=api_key,
            config=config or {}
        )
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.pool_cache = SimpleCache(ttl_seconds=300)  # 5 min cache
        self.candle_cache = SimpleCache(ttl_seconds=60)  # 1 min cache
        self.dexscreener = dexscreener_collector
        
        logger.info("✅ Helius Collector initialized (POOL-BASED)")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def _find_pool_address(self, symbol: str) -> Optional[str]:
        """
        Find pool address for a trading pair
        
        Strategy:
        1. Check cache
        2. Use Dexscreener collector if available
        3. Call Dexscreener API directly
        4. Fallback to None (will use token address)
        """
        # Check cache
        cache_key = f"pool_{symbol}"
        cached_pool = self.pool_cache.get(cache_key)
        if cached_pool:
            logger.debug(f"📦 Using cached pool for {symbol}: {cached_pool[:8]}...")
            return cached_pool
        
        # Parse symbol
        try:
            base, quote = symbol.upper().split('/')
        except:
            logger.error(f"Invalid symbol format: {symbol}")
            return None
        
        # Strategy 1: Use Dexscreener collector if available
        if self.dexscreener:
            try:
                pool = await self._find_pool_via_dexscreener_collector(symbol)
                if pool:
                    self.pool_cache.set(cache_key, pool)
                    logger.info(f"✅ Found pool via Dexscreener collector: {pool[:8]}...")
                    return pool
            except Exception as e:
                logger.debug(f"Dexscreener collector failed: {e}")
        
        # Strategy 2: Call Dexscreener API directly
        pool = await self._find_pool_via_api(base, quote)
        if pool:
            self.pool_cache.set(cache_key, pool)
            logger.info(f"✅ Found pool via API: {pool[:8]}...")
            return pool
        
        # Strategy 3: Fallback - return None (will use token address)
        logger.warning(f"⚠️ No pool found for {symbol}, will use token address fallback")
        return None
    
    async def _find_pool_via_dexscreener_collector(self, symbol: str) -> Optional[str]:
        """Use Dexscreener collector to find pool"""
        if not self.dexscreener:
            return None
        
        try:
            # Dexscreener collector has a method to get pool info
            pool_info = await self.dexscreener._find_pool_for_pair(symbol)
            if pool_info and 'pairAddress' in pool_info:
                return pool_info['pairAddress']
        except AttributeError:
            # Method doesn't exist, try alternative
            pass
        
        return None
    
    async def _find_pool_via_api(self, base: str, quote: str) -> Optional[str]:
        """
        Find pool via Dexscreener API
        
        Returns the pool with highest liquidity for this pair
        """
        session = await self._get_session()
        
        # Get token addresses
        base_mint = self.TOKEN_MINTS.get(base)
        quote_mint = self.TOKEN_MINTS.get(quote)
        
        if not base_mint:
            logger.warning(f"Unknown base token: {base}")
            return None
        
        try:
            # Search for pools with this token
            url = f"{self.DEXSCREENER_API}/tokens/{base_mint}"
            
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status != 200:
                    return None
                
                data = await response.json()
                pairs = data.get('pairs', [])
                
                if not pairs:
                    return None
                
                # Filter for Solana and matching quote token
                solana_pairs = [
                    p for p in pairs
                    if p.get('chainId') == 'solana'
                    and (not quote_mint or p.get('quoteToken', {}).get('address') == quote_mint)
                ]
                
                if not solana_pairs:
                    return None
                
                # Get pool with highest liquidity
                best_pool = max(
                    solana_pairs,
                    key=lambda p: float(p.get('liquidity', {}).get('usd', 0))
                )
                
                pool_address = best_pool.get('pairAddress')
                liquidity = best_pool.get('liquidity', {}).get('usd', 0)
                
                logger.info(
                    f"🔍 Found {base}/{quote} pool: {pool_address[:8]}... "
                    f"(liquidity: ${liquidity:,.0f})"
                )
                
                return pool_address
                
        except Exception as e:
            logger.error(f"Dexscreener API error: {e}")
            return None
    
    async def _resolve_symbol_to_address(self, symbol: str) -> Optional[str]:
        """
        Resolve symbol to pool or token address
        
        Priority:
        1. Pool address (best)
        2. Token address (fallback)
        """
        # Try to find pool address first
        pool_address = await self._find_pool_address(symbol)
        if pool_address:
            logger.info(f"🎯 Using pool address for {symbol}")
            return pool_address
        
        # Fallback to token address
        logger.info(f"⚠️ Using token address fallback for {symbol}")
        parts = symbol.upper().split('/')
        if len(parts) != 2:
            return None
        
        base_token, quote_token = parts
        
        # For SOL pairs, query the OTHER token
        if base_token in ['SOL', 'WSOL']:
            return self.TOKEN_MINTS.get(quote_token)
        else:
            return self.TOKEN_MINTS.get(base_token)
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch CURRENT candle using pool-based approach
        """
        logger.info(f"🔗 Helius: Fetching candle for {symbol} (pool-based)")
        
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Check cache
        cache_key = f"candle_{symbol}_{timeframe}_{int(timestamp.timestamp())}"
        cached = self.candle_cache.get(cache_key)
        if cached:
            logger.debug("📦 Using cached candle")
            return cached
        
        timeframe_seconds = {
            '1m': 60, '5m': 300, '15m': 900,
            '1h': 3600, '4h': 14400, '1d': 86400,
        }.get(timeframe, 300)
        
        # 🎯 With pool-based approach, we can use tighter time window
        # because ALL transactions are relevant
        start_time = timestamp
        end_time = timestamp + timedelta(seconds=timeframe_seconds)
        
        logger.info(f"🔍 Time window: {start_time} to {end_time}")
        
        trades = await self.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=100  # Lower limit since all are relevant
        )
        
        if not trades:
            logger.warning(f"⚠️ No trades found for {symbol}")
            return {
                'timestamp': timestamp,
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': 0.0
            }
        
        # Aggregate to candle
        prices = [t['price'] for t in trades if t.get('price', 0) > 0]
        volumes = [t['amount'] for t in trades if t.get('amount', 0) > 0]
        
        if not prices:
            return {
                'timestamp': timestamp,
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': sum(volumes) if volumes else 0.0
            }
        
        candle = {
            'timestamp': timestamp,
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes) if volumes else 0.0
        }
        
        self.candle_cache.set(cache_key, candle)
        
        logger.info(f"✅ Helius: Candle built from {len(trades)} trades")
        return candle
    
    async def fetch_dex_trades(
        self,
        token_address: str,  # Actually pool_address if found
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades from pool or token address
        
        If token_address is actually a pool address, ALL transactions
        are relevant for this trading pair!
        """
        logger.debug(f"🔍 Fetching trades from: {token_address[:8]}...")
        
        session = await self._get_session()
        
        url = f"{self.API_BASE}/v0/addresses/{token_address}/transactions"
        
        params = {
            'api-key': self.api_key,
            'limit': min(limit, 100),
            'type': 'SWAP',
        }
        
        try:
            async with session.get(
                url, 
                params=params, 
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                
                if response.status != 200:
                    logger.error(f"Helius error: {response.status}")
                    return []
                
                data = await response.json()
                
                if not data:
                    return []
                
                # Parse trades
                trades = []
                for tx in data:
                    try:
                        trade = self._parse_transaction(tx)
                        
                        # 🎯 With pool-based approach, we can use looser time filter
                        # because ALL transactions are for our pair
                        if trade:
                            # Optional: Still filter by time for accuracy
                            if start_time <= trade['timestamp'] <= end_time:
                                trades.append(trade)
                            
                    except Exception:
                        continue
                
                logger.info(
                    f"✅ Helius: {len(trades)} trades "
                    f"(from {len(data)} transactions)"
                )
                return trades
                
        except Exception as e:
            logger.error(f"Helius fetch error: {e}")
            return []
    
    def _parse_transaction(self, tx: Dict) -> Optional[Dict[str, Any]]:
        """Parse transaction to trade"""
        try:
            timestamp = datetime.fromtimestamp(tx.get('timestamp', 0), tz=timezone.utc)
            
            token_transfers = tx.get('tokenTransfers', [])
            if not token_transfers:
                return None
            
            transfer = token_transfers[0]
            
            # Get wallet
            wallet = transfer.get('fromUserAccount') or transfer.get('toUserAccount')
            if not wallet:
                return None
            
            # Get amount
            raw_amount = float(transfer.get('tokenAmount', 0))
            if raw_amount <= 0:
                return None
            
            mint = transfer.get('mint', '')
            decimals = 9 if 'So111' in mint else 6
            amount = raw_amount / (10 ** decimals)
            
            # Estimate price
            native_transfers = tx.get('nativeTransfers', [])
            price = 100.0  # Default
            
            if native_transfers:
                sol_amount = sum(float(nt.get('amount', 0)) for nt in native_transfers) / 1e9
                if sol_amount > 0 and amount > 0:
                    price = sol_amount / amount
            
            return {
                'id': tx.get('signature', ''),
                'timestamp': timestamp,
                'trade_type': 'buy',
                'amount': amount,
                'price': price,
                'value_usd': amount * price,
                'wallet_address': wallet,
                'dex': 'jupiter',
                'signature': tx.get('signature'),
                'blockchain': 'solana',
            }
            
        except Exception as e:
            logger.warning(f"Parse error: {e}")
            return None
    
    async def health_check(self) -> bool:
        """Quick health check"""
        try:
            session = await self._get_session()
            url = f"{self.API_BASE}/v0/addresses/So11111111111111111111111111111111111111112/transactions"
            
            params = {
                'api-key': self.api_key,
                'limit': 1
            }
            
            async with session.get(
                url, 
                params=params, 
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                return response.status == 200
                    
        except Exception:
            return False
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
        self.pool_cache.clear()
        self.candle_cache.clear()


def create_helius_collector(
    api_key: str, 
    dexscreener_collector: Optional[Any] = None,
    config: Optional[Dict[str, Any]] = None
) -> HeliusCollector:
    """
    Create pool-based Helius Collector
    
    Args:
        api_key: Helius API key
        dexscreener_collector: Optional Dexscreener collector for pool lookup
        config: Optional config
    """
    return HeliusCollector(
        api_key=api_key,
        config=config or {},
        dexscreener_collector=dexscreener_collector
    )
