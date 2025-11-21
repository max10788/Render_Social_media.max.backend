"""
Moralis Collector - Solana DEX OHLCV Data
Endpoint: GET /solana/ohlcv/pair/{address}
"""

import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from .base import BaseCollector

logger = logging.getLogger(__name__)


class MoralisCollector(BaseCollector):
    """
    Moralis Collector for Solana DEX OHLCV data
    
    Features:
    - Historical OHLCV candles
    - Supports Raydium, Jupiter, Orca
    - Token must have >$1k volume
    - Free tier available
    """
    
    def __init__(self, api_keys: List[str], config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_keys = api_keys  # Multiple keys for fallback
        self.current_key_index = 0
        self.base_url = "https://deep-index.moralis.io/api/v2.2"
        
        # Known token addresses
        self.TOKEN_MAP = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
            'BONK': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',
            'JTO': 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL',
            'JUP': 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',
            'WIF': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
        }
        
        logger.info(f"✅ Moralis Collector initialized with {len(self.api_keys)} API keys")
        self._is_initialized = True
    
    def _get_current_api_key(self) -> str:
        """Get current API key with rotation"""
        return self.api_keys[self.current_key_index]
    
    def _rotate_api_key(self):
        """Rotate to next API key on error"""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        logger.info(f"🔄 Rotated to API key #{self.current_key_index + 1}")
    
    async def _resolve_pair_address(self, symbol: str) -> Optional[str]:
        """
        Resolve symbol to Solana pair address
        
        For Moralis, we need the pair contract address.
        Format: BASE/QUOTE (e.g., BONK/SOL)
        """
        try:
            base_token, quote_token = symbol.upper().split('/')
        except ValueError:
            logger.error(f"Invalid symbol format: {symbol}")
            return None
        
        base_addr = self.TOKEN_MAP.get(base_token)
        
        if not base_addr:
            logger.warning(f"Unknown token: {base_token}")
            return None
        
        # For Moralis, we use the token mint address
        # The API will find the most liquid pair automatically
        return base_addr
    
    async def fetch_ohlcv_batch(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV candles from Moralis
        
        Endpoint: GET /solana/ohlcv/pair/{address}
        Query params: ?interval=5m&from=timestamp&to=timestamp
        """
        logger.info(f"Moralis OHLCV batch: {symbol} {timeframe} ({start_time} to {end_time})")
        
        pair_address = await self._resolve_pair_address(symbol)
        if not pair_address:
            return []
        
        # Map timeframe to Moralis format
        # Moralis supports: 1m, 5m, 15m, 30m, 1h, 4h, 1d
        timeframe_map = {
            '1m': '1m',
            '5m': '5m',
            '15m': '15m',
            '30m': '30m',
            '1h': '1h',
            '4h': '4h',
            '1d': '1d'
        }
        
        moralis_timeframe = timeframe_map.get(timeframe, '5m')
        
        # Convert timestamps to Unix
        from_timestamp = int(start_time.timestamp())
        to_timestamp = int(end_time.timestamp())
        
        url = f"{self.base_url}/solana/ohlcv/pair/{pair_address}"
        
        params = {
            'interval': moralis_timeframe,
            'from': from_timestamp,
            'to': to_timestamp,
            'limit': limit or 100
        }
        
        # Try with current API key, rotate on failure
        for attempt in range(len(self.api_keys)):
            api_key = self._get_current_api_key()
            
            headers = {
                'X-API-Key': api_key,
                'Accept': 'application/json'
            }
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as response:
                        
                        if response.status == 429:
                            logger.warning(f"⚠️ Rate limited on key #{self.current_key_index + 1}")
                            self._rotate_api_key()
                            continue
                        
                        if response.status == 401:
                            logger.error(f"❌ Invalid API key #{self.current_key_index + 1}")
                            self._rotate_api_key()
                            continue
                        
                        if response.status != 200:
                            text = await response.text()
                            logger.warning(f"Moralis error {response.status}: {text[:200]}")
                            return []
                        
                        data = await response.json()
                        
                        # Parse response
                        candles = []
                        for item in data:
                            candle = {
                                'timestamp': datetime.fromtimestamp(item['timestamp']),
                                'open': float(item['open']),
                                'high': float(item['high']),
                                'low': float(item['low']),
                                'close': float(item['close']),
                                'volume': float(item.get('volume', 0)),
                                'volume_usd': float(item.get('volume_usd', 0)),
                                'trade_count': item.get('trades', 0)
                            }
                            candles.append(candle)
                        
                        logger.info(f"✅ Moralis: {len(candles)} candles fetched")
                        return candles
                        
            except asyncio.TimeoutError:
                logger.warning("Moralis request timeout")
                return []
            except Exception as e:
                logger.error(f"Moralis error: {e}")
                return []
        
        logger.error("All Moralis API keys exhausted")
        return []
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Fetch single candle"""
        # Fetch a small batch around the timestamp
        start_time = timestamp - timedelta(minutes=5)
        end_time = timestamp + timedelta(minutes=5)
        
        candles = await self.fetch_ohlcv_batch(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            limit=3
        )
        
        if candles:
            # Find closest candle
            closest = min(candles, key=lambda c: abs((c['timestamp'] - timestamp).total_seconds()))
            return closest
        
        return self._empty_candle(timestamp)
    
    def _empty_candle(self, timestamp: datetime) -> Dict[str, Any]:
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
        """Moralis doesn't provide trade-level data in OHLCV API"""
        logger.debug("Moralis: Trade-level data not available")
        return []
    
    async def health_check(self) -> bool:
        """Health check"""
        try:
            # Try to fetch a small batch for SOL/USDC
            test_candles = await self.fetch_ohlcv_batch(
                symbol='SOL/USDC',
                timeframe='5m',
                start_time=datetime.utcnow() - timedelta(hours=1),
                end_time=datetime.utcnow(),
                limit=1
            )
            return len(test_candles) > 0
        except Exception as e:
            logger.error(f"Moralis health check failed: {e}")
            return False
    
    async def close(self):
        """Cleanup"""
        pass
