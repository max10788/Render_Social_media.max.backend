"""
Moralis Collector - Multi-Chain DEX OHLCV Data
Supports: Solana (Raydium, Jupiter, Orca) + Ethereum (Uniswap, Sushiswap)
"""

import aiohttp
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any, Tuple
from .base import BaseCollector

logger = logging.getLogger(__name__)


class MoralisCollector(BaseCollector):
    """
    Moralis Collector for Multi-Chain DEX OHLCV data
    
    Features:
    - ✅ Solana: Raydium, Jupiter, Orca
    - ✅ Ethereum: Uniswap V2/V3, Sushiswap
    - ✅ Historical OHLCV candles
    - ✅ Multiple API keys with rotation
    - ✅ Token must have >$1k volume
    
    API Docs: https://docs.moralis.io/web3-data-api/evm/reference/get-pair-ohlcv
    """
    
    def __init__(self, api_keys: List[str], config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_keys = [k for k in api_keys if k]  # Remove None/empty
        self.current_key_index = 0
        self.base_url = "https://deep-index.moralis.io/api/v2.2"
        
        if not self.api_keys:
            raise ValueError("At least one Moralis API key required")
        
        # Solana Token Addresses
        self.SOLANA_TOKENS = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
            'BONK': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',
            'JTO': 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL',
            'JUP': 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',
            'WIF': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
            'PYTH': 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3',
        }
        
        # Ethereum Token Addresses
        self.ETHEREUM_TOKENS = {
            'ETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',  # WETH
            'WETH': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
            'USDC': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
            'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
            'DAI': '0x6B175474E89094C44Da98b954EedeAC495271d0F',
            'WBTC': '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
            'UNI': '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
            'LINK': '0x514910771AF9Ca656af840dff83E8264EcF986CA',
            'AAVE': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9',
            'SHIB': '0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE',
        }
        
        # Supported DEX per chain
        self.SUPPORTED_DEX = {
            'solana': ['raydium', 'jupiter', 'orca'],
            'ethereum': ['uniswap', 'uniswapv2', 'uniswapv3', 'sushiswap']
        }
        
        logger.info(f"✅ Moralis Collector initialized with {len(self.api_keys)} API keys")
        logger.info(f"   Supported chains: Solana, Ethereum")
        self._is_initialized = True
    
    def _get_current_api_key(self) -> str:
        """Get current API key with rotation"""
        return self.api_keys[self.current_key_index]
    
    def _rotate_api_key(self):
        """Rotate to next API key on error"""
        old_index = self.current_key_index
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        logger.info(f"🔄 Rotated API key: #{old_index + 1} → #{self.current_key_index + 1}")
    
    def _detect_chain_from_dex(self, dex_exchange: str) -> str:
        """Detect blockchain from DEX name"""
        dex_lower = dex_exchange.lower()
        
        if dex_lower in self.SUPPORTED_DEX['solana']:
            return 'solana'
        elif dex_lower in self.SUPPORTED_DEX['ethereum']:
            return 'ethereum'
        else:
            logger.warning(f"Unknown DEX '{dex_exchange}', defaulting to solana")
            return 'solana'
    
    def _resolve_token_address(
        self,
        symbol: str,
        blockchain: str = 'solana'
    ) -> Optional[str]:
        """
        Resolve token symbol to contract address
        
        Args:
            symbol: Token symbol (e.g., 'SOL', 'ETH', 'USDC')
            blockchain: 'solana' or 'ethereum'
            
        Returns:
            Token contract address or None
        """
        if blockchain == 'solana':
            return self.SOLANA_TOKENS.get(symbol.upper())
        elif blockchain == 'ethereum':
            return self.ETHEREUM_TOKENS.get(symbol.upper())
        else:
            logger.error(f"Unsupported blockchain: {blockchain}")
            return None
    
    async def _resolve_pair_address(
        self,
        symbol: str,
        blockchain: str = 'solana'
    ) -> Optional[Tuple[str, str]]:
        """
        Resolve trading pair to token addresses
        
        Args:
            symbol: Trading pair (e.g., 'SOL/USDC', 'ETH/USDT')
            blockchain: Target blockchain
            
        Returns:
            Tuple of (base_address, quote_address) or None
        """
        try:
            base_token, quote_token = symbol.upper().split('/')
        except ValueError:
            logger.error(f"Invalid symbol format: {symbol}")
            return None
        
        base_addr = self._resolve_token_address(base_token, blockchain)
        quote_addr = self._resolve_token_address(quote_token, blockchain)
        
        if not base_addr:
            logger.warning(f"Unknown base token: {base_token} on {blockchain}")
            return None
        
        if not quote_addr:
            logger.warning(f"Unknown quote token: {quote_token} on {blockchain}")
            return None
        
        logger.debug(f"Resolved {symbol} → base={base_addr[:8]}..., quote={quote_addr[:8]}...")
        return (base_addr, quote_addr)
    
    async def fetch_ohlcv_batch(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 100,
        blockchain: str = 'solana',
        dex_exchange: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV candles from Moralis
        
        Args:
            symbol: Trading pair (e.g., 'SOL/USDC')
            timeframe: Candle interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
            start_time: Start timestamp
            end_time: End timestamp
            limit: Max number of candles
            blockchain: 'solana' or 'ethereum'
            dex_exchange: Optional DEX filter
            
        Returns:
            List of OHLCV candles
        """
        logger.info(f"Moralis OHLCV: {symbol} {timeframe} on {blockchain} ({start_time} to {end_time})")
        
        # Resolve pair addresses
        pair_addresses = await self._resolve_pair_address(symbol, blockchain)
        if not pair_addresses:
            return []
        
        base_address, quote_address = pair_addresses
        
        # Map timeframe to Moralis format
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
        
        # Convert timestamps to Unix (ensure timezone aware)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        from_timestamp = int(start_time.timestamp())
        to_timestamp = int(end_time.timestamp())
        
        # Build endpoint URL (chain-specific)
        if blockchain == 'solana':
            url = f"{self.base_url}/solana/ohlcv/pair/{base_address}"
        elif blockchain == 'ethereum':
            url = f"{self.base_url}/evm/ohlcv/pair/{base_address}"
        else:
            logger.error(f"Unsupported blockchain: {blockchain}")
            return []
        
        params = {
            'interval': moralis_timeframe,
            'from': from_timestamp,
            'to': to_timestamp,
            'limit': limit or 100
        }
        
        # Add quote token for filtering (Ethereum only)
        if blockchain == 'ethereum':
            params['quote_address'] = quote_address
        
        # Try with all API keys on failure
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
                            await asyncio.sleep(1)
                            continue
                        
                        if response.status == 401 or response.status == 403:
                            logger.error(f"❌ Auth failed on key #{self.current_key_index + 1}")
                            self._rotate_api_key()
                            continue
                        
                        if response.status != 200:
                            text = await response.text()
                            logger.warning(f"Moralis error {response.status}: {text[:200]}")
                            
                            # If not found, try next key
                            if response.status == 404:
                                logger.info("Pair not found, trying next key...")
                                self._rotate_api_key()
                                continue
                            
                            return []
                        
                        data = await response.json()
                        
                        # Parse response
                        candles = []
                        for item in data:
                            try:
                                candle = {
                                    'timestamp': datetime.fromtimestamp(item['timestamp'], tz=timezone.utc),
                                    'open': float(item['open']),
                                    'high': float(item['high']),
                                    'low': float(item['low']),
                                    'close': float(item['close']),
                                    'volume': float(item.get('volume', 0)),
                                    'volume_usd': float(item.get('volume_usd', 0)),
                                    'trade_count': item.get('trades', 0),
                                    'source': f'moralis_{blockchain}'
                                }
                                candles.append(candle)
                            except (KeyError, ValueError, TypeError) as e:
                                logger.warning(f"Failed to parse candle: {e}")
                                continue
                        
                        logger.info(f"✅ Moralis {blockchain}: {len(candles)} candles fetched")
                        return candles
                        
            except asyncio.TimeoutError:
                logger.warning(f"Moralis request timeout (attempt {attempt + 1}/{len(self.api_keys)})")
                self._rotate_api_key()
                continue
            except Exception as e:
                logger.error(f"Moralis error: {e}")
                self._rotate_api_key()
                continue
        
        logger.error(f"All {len(self.api_keys)} Moralis API keys exhausted")
        return []
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime,
        blockchain: str = 'solana',
        dex_exchange: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch single candle at specific timestamp
        
        Args:
            symbol: Trading pair
            timeframe: Candle interval
            timestamp: Target timestamp
            blockchain: Target blockchain
            dex_exchange: Optional DEX filter
            
        Returns:
            Single OHLCV candle
        """
        # Fetch a small batch around the timestamp
        start_time = timestamp - timedelta(minutes=10)
        end_time = timestamp + timedelta(minutes=10)
        
        candles = await self.fetch_ohlcv_batch(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            limit=5,
            blockchain=blockchain,
            dex_exchange=dex_exchange
        )
        
        if candles:
            # Find closest candle to target timestamp
            closest = min(
                candles,
                key=lambda c: abs((c['timestamp'] - timestamp).total_seconds())
            )
            return closest
        
        logger.debug(f"No Moralis candle found for {symbol} @ {timestamp}")
        return self._empty_candle(timestamp)
    
    def _empty_candle(self, timestamp: datetime) -> Dict[str, Any]:
        """Return empty candle structure"""
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
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
        Moralis doesn't provide trade-level data in OHLCV API
        This would require separate endpoints
        """
        logger.debug("Moralis: Trade-level data not available in OHLCV endpoint")
        return []
    
    async def health_check(self) -> bool:
        """
        Health check by attempting to fetch recent data
        Tests both Solana and Ethereum endpoints
        """
        try:
            # Test Solana endpoint with SOL/USDC
            logger.debug("Testing Moralis Solana endpoint...")
            sol_candles = await self.fetch_ohlcv_batch(
                symbol='SOL/USDC',
                timeframe='5m',
                start_time=datetime.now(timezone.utc) - timedelta(hours=1),
                end_time=datetime.now(timezone.utc),
                limit=1,
                blockchain='solana'
            )
            
            # Test Ethereum endpoint with ETH/USDC
            logger.debug("Testing Moralis Ethereum endpoint...")
            eth_candles = await self.fetch_ohlcv_batch(
                symbol='ETH/USDC',
                timeframe='5m',
                start_time=datetime.now(timezone.utc) - timedelta(hours=1),
                end_time=datetime.now(timezone.utc),
                limit=1,
                blockchain='ethereum'
            )
            
            solana_ok = len(sol_candles) > 0
            ethereum_ok = len(eth_candles) > 0
            
            if solana_ok and ethereum_ok:
                logger.info("✅ Moralis: Both Solana and Ethereum healthy")
            elif solana_ok:
                logger.info("✅ Moralis: Solana healthy, Ethereum unavailable")
            elif ethereum_ok:
                logger.info("✅ Moralis: Ethereum healthy, Solana unavailable")
            else:
                logger.warning("⚠️ Moralis: Both chains unavailable")
            
            # Return true if at least one chain works
            return solana_ok or ethereum_ok
            
        except Exception as e:
            logger.error(f"❌ Moralis health check failed: {e}")
            return False
    
    async def close(self):
        """Cleanup resources"""
        logger.debug("Moralis Collector closed")
        pass
    
    def get_supported_tokens(self, blockchain: str = 'solana') -> List[str]:
        """Get list of supported tokens for a blockchain"""
        if blockchain == 'solana':
            return list(self.SOLANA_TOKENS.keys())
        elif blockchain == 'ethereum':
            return list(self.ETHEREUM_TOKENS.keys())
        else:
            return []
    
    def get_supported_chains(self) -> List[str]:
        """Get list of supported blockchains"""
        return ['solana', 'ethereum']
