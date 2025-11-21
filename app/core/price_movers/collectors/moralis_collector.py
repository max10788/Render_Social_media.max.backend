"""
Moralis Collector - EVM Multi-Chain DEX OHLCV Data
Supports: Ethereum, BSC, Polygon, Avalanche, Arbitrum, Optimism, Base, Fantom
NO SOLANA SUPPORT (Moralis doesn't have Solana OHLCV API)
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
    Moralis Collector for EVM Multi-Chain DEX OHLCV data
    
    Supported Chains:
    - ✅ Ethereum (eth)
    - ✅ BSC (bsc)
    - ✅ Polygon (polygon)
    - ✅ Avalanche (avalanche)
    - ✅ Arbitrum (arbitrum)
    - ✅ Optimism (optimism)
    - ✅ Base (base)
    - ✅ Fantom (fantom)
    
    ❌ NO SOLANA (not supported by Moralis OHLCV API)
    
    API Docs: https://docs.moralis.io/web3-data-api/evm/reference/get-pair-ohlcv
    """
    
    def __init__(self, api_keys: List[str], config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_keys = [k for k in api_keys if k]
        self.current_key_index = 0
        self.base_url = "https://deep-index.moralis.io/api/v2.2"
        
        if not self.api_keys:
            raise ValueError("At least one Moralis API key required")
        
        # Chain ID mapping
        self.CHAIN_IDS = {
            'ethereum': '0x1',
            'eth': '0x1',
            'bsc': '0x38',
            'polygon': '0x89',
            'avalanche': '0xa86a',
            'arbitrum': '0xa4b1',
            'optimism': '0xa',
            'base': '0x2105',
            'fantom': '0xfa'
        }
        
        # Common token addresses per chain
        self.TOKEN_ADDRESSES = {
            'ethereum': {
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
            },
            'bsc': {
                'BNB': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',  # WBNB
                'WBNB': '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c',
                'USDC': '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d',
                'USDT': '0x55d398326f99059fF775485246999027B3197955',
                'BUSD': '0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56',
                'CAKE': '0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82',
            },
            'polygon': {
                'MATIC': '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270',  # WMATIC
                'WMATIC': '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270',
                'USDC': '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',
                'USDT': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F',
                'DAI': '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063',
            },
            'avalanche': {
                'AVAX': '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7',  # WAVAX
                'WAVAX': '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7',
                'USDC': '0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E',
                'USDT': '0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7',
            },
            'arbitrum': {
                'ETH': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',  # WETH
                'WETH': '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1',
                'USDC': '0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8',
                'USDT': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9',
            },
            'optimism': {
                'ETH': '0x4200000000000000000000000000000000000006',  # WETH
                'WETH': '0x4200000000000000000000000000000000000006',
                'USDC': '0x7F5c764cBc14f9669B88837ca1490cCa17c31607',
                'USDT': '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58',
            },
            'base': {
                'ETH': '0x4200000000000000000000000000000000000006',  # WETH
                'WETH': '0x4200000000000000000000000000000000000006',
                'USDC': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
            },
            'fantom': {
                'FTM': '0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83',  # WFTM
                'WFTM': '0x21be370D5312f44cB42ce377BC9b8a0cEF1A4C83',
                'USDC': '0x04068DA6C83AFCFA0e13ba15A6696662335D5B75',
            }
        }
        
        # Supported DEX per chain
        self.SUPPORTED_DEX = {
            'ethereum': ['uniswap', 'uniswapv2', 'uniswapv3', 'sushiswap'],
            'bsc': ['pancakeswap', 'pancakeswapv2', 'pancakeswapv3'],
            'polygon': ['quickswap', 'sushiswap', 'uniswapv3'],
            'avalanche': ['traderjoe', 'pangolin'],
            'arbitrum': ['uniswapv3', 'sushiswap', 'camelot'],
            'optimism': ['uniswapv3', 'velodrome'],
            'base': ['uniswapv3', 'aerodrome', 'baseswap'],
            'fantom': ['spookyswap', 'spiritswap']
        }
        
        logger.info(f"✅ Moralis Collector initialized with {len(self.api_keys)} API keys")
        logger.info(f"   Supported chains: {', '.join(self.CHAIN_IDS.keys())}")
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
        
        for chain, dexes in self.SUPPORTED_DEX.items():
            if dex_lower in dexes:
                return chain
        
        logger.warning(f"Unknown DEX '{dex_exchange}', defaulting to ethereum")
        return 'ethereum'
    
    def _resolve_token_address(
        self,
        symbol: str,
        blockchain: str
    ) -> Optional[str]:
        """Resolve token symbol to contract address"""
        chain_tokens = self.TOKEN_ADDRESSES.get(blockchain, {})
        return chain_tokens.get(symbol.upper())
    
    async def _resolve_pair_address(
        self,
        symbol: str,
        blockchain: str
    ) -> Optional[Tuple[str, str]]:
        """Resolve trading pair to token addresses"""
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
        blockchain: str = 'ethereum',
        dex_exchange: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV candles from Moralis (EVM chains only!)
        
        Args:
            symbol: Trading pair (e.g., 'ETH/USDC')
            timeframe: Candle interval (1m, 5m, 15m, 30m, 1h, 4h, 1d)
            start_time: Start timestamp
            end_time: End timestamp
            limit: Max number of candles
            blockchain: EVM chain (ethereum, bsc, polygon, etc.)
            dex_exchange: Optional DEX filter
            
        Returns:
            List of OHLCV candles
        """
        # Validate chain
        if blockchain not in self.CHAIN_IDS:
            logger.error(f"Unsupported blockchain: {blockchain}")
            return []
        
        logger.info(f"Moralis OHLCV: {symbol} {timeframe} on {blockchain} ({start_time} to {end_time})")
        
        # Resolve pair addresses
        pair_addresses = await self._resolve_pair_address(symbol, blockchain)
        if not pair_addresses:
            return []
        
        base_address, quote_address = pair_addresses
        
        # Map timeframe
        timeframe_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '4h', '1d': '1d'
        }
        moralis_timeframe = timeframe_map.get(timeframe, '5m')
        
        # Ensure timezone aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        from_timestamp = int(start_time.timestamp())
        to_timestamp = int(end_time.timestamp())
        
        # Get chain ID
        chain_id = self.CHAIN_IDS[blockchain]
        
        # Build URL - EVM OHLCV endpoint
        url = f"{self.base_url}/evm/ohlcv/pair/{base_address}"
        
        params = {
            'chain': chain_id,
            'interval': moralis_timeframe,
            'from': from_timestamp,
            'to': to_timestamp,
            'limit': limit or 100
        }
        
        # Try with all API keys
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
                        
                        if response.status in [401, 403]:
                            logger.error(f"❌ Auth failed on key #{self.current_key_index + 1}")
                            self._rotate_api_key()
                            continue
                        
                        if response.status != 200:
                            text = await response.text()
                            logger.warning(f"Moralis error {response.status}: {text[:200]}")
                            
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
                logger.warning(f"Moralis timeout (attempt {attempt + 1}/{len(self.api_keys)})")
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
        blockchain: str = 'ethereum',
        dex_exchange: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch single candle"""
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
            closest = min(
                candles,
                key=lambda c: abs((c['timestamp'] - timestamp).total_seconds())
            )
            return closest
        
        logger.debug(f"No Moralis candle for {symbol} @ {timestamp}")
        return self._empty_candle(timestamp)
    
    def _empty_candle(self, timestamp: datetime) -> Dict[str, Any]:
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
        """Moralis OHLCV API doesn't provide trade-level data"""
        logger.debug("Moralis: Trade-level data not available in OHLCV endpoint")
        return []
    
    async def health_check(self) -> bool:
        """Health check for EVM chains"""
        try:
            # Test Ethereum with ETH/USDC
            logger.debug("Testing Moralis Ethereum endpoint...")
            eth_candles = await self.fetch_ohlcv_batch(
                symbol='ETH/USDC',
                timeframe='5m',
                start_time=datetime.now(timezone.utc) - timedelta(hours=1),
                end_time=datetime.now(timezone.utc),
                limit=1,
                blockchain='ethereum'
            )
            
            if len(eth_candles) > 0:
                logger.info("✅ Moralis: Ethereum healthy")
                return True
            else:
                logger.warning("⚠️ Moralis: Ethereum returned no data")
                return False
            
        except Exception as e:
            logger.error(f"❌ Moralis health check failed: {e}")
            return False
    
    async def close(self):
        """Cleanup"""
        logger.debug("Moralis Collector closed")
        pass
    
    def get_supported_tokens(self, blockchain: str = 'ethereum') -> List[str]:
        """Get list of supported tokens for a blockchain"""
        return list(self.TOKEN_ADDRESSES.get(blockchain, {}).keys())
    
    def get_supported_chains(self) -> List[str]:
        """Get list of supported blockchains"""
        return list(self.CHAIN_IDS.keys())
