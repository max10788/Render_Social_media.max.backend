"""
Price Oracle - WITH Live ETH Price Support
===========================================

✅ FIXED: Price validation with reasonable ranges
✅ FIXED: Fallback prices when API fails
✅ FIXED: Better error handling
✅ NEW: Etherscan ETH price integration

Version: 3.0 with ETH Live Price
Date: 2024-12-29
"""

import requests
import time
from typing import Optional, Dict
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class PriceOracle:
    """
    Fetches historical and current cryptocurrency prices.
    
    Data Sources:
    - Etherscan (for live ETH price) - PRIORITY
    - CoinGecko (for tokens) - FREE API
    - Cache (5 minute TTL)
    
    ✨ NEW: Etherscan integration for live ETH price
    """
    
    def __init__(self, cache_manager=None, etherscan=None):
        """
        Initialize price oracle.
        
        Args:
            cache_manager: CacheManager instance for caching
            etherscan: EtherscanAPI instance for live ETH price
        """
        self.cache = cache_manager
        self.etherscan = etherscan  # ✅ NEW: Etherscan client
        self.coingecko_base = "https://api.coingecko.com/api/v3"
        self.rate_limit_delay = 1.5  # 40 calls/min instead of 50
        self.last_request_time = 0
        self.session = requests.Session()
        
        # Token address to CoinGecko ID mapping
        self.token_id_map = {
            None: 'ethereum',  # Native ETH
            '0xdac17f958d2ee523a2206206994597c13d831ec7': 'tether',  # USDT
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'usd-coin',  # USDC
            '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 'wrapped-bitcoin',  # WBTC
            '0x514910771af9ca656af840dff83e8264ecf986ca': 'chainlink',  # LINK
            '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984': 'uniswap',  # UNI
            '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0': 'matic-network',  # MATIC
        }
        
        # ✅ Reasonable price ranges for validation
        self.price_ranges = {
            'ethereum': (100, 10000),        # ETH: $100-$10K
            'tether': (0.95, 1.05),          # USDT: ~$1
            'usd-coin': (0.95, 1.05),        # USDC: ~$1
            'wrapped-bitcoin': (10000, 200000),  # WBTC: $10K-$200K
            'chainlink': (5, 100),           # LINK: $5-$100
            'uniswap': (3, 50),              # UNI: $3-$50
            'matic-network': (0.3, 5),       # MATIC: $0.3-$5
        }
        
        # ✅ Fallback prices by year (for historical)
        self.fallback_prices = {
            'ETH': 3400.0,  # Current estimate
            'WETH': 3400.0,
            'USDT': 1.0,
            'USDC': 1.0,
            'DAI': 1.0,
        }
    
    def _rate_limit(self):
        """Enforce rate limiting for API calls."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        self.last_request_time = time.time()
    
    def _get_token_id(self, token_address: Optional[str]) -> str:
        """Convert token address to CoinGecko ID."""
        if token_address is None:
            return 'ethereum'
        
        token_lower = token_address.lower()
        return self.token_id_map.get(token_lower, 'ethereum')
    
    def _validate_price(self, token_id: str, price: float) -> bool:
        """
        Validate that price is within reasonable range.
        
        Args:
            token_id: CoinGecko token ID
            price: Price in USD
        
        Returns:
            True if price seems reasonable
        """
        if price <= 0:
            logger.warning(f"❌ Invalid price for {token_id}: ${price}")
            return False
        
        if token_id in self.price_ranges:
            min_price, max_price = self.price_ranges[token_id]
            if not (min_price <= price <= max_price):
                logger.warning(
                    f"⚠️ Price for {token_id} outside expected range: "
                    f"${price:,.2f} (expected ${min_price}-${max_price})"
                )
                return False
        
        return True
    
    # ========================================================================
    # ✨ NEW: LIVE ETH PRICE METHODS
    # ========================================================================
    
    def get_eth_price_live(self) -> float:
        """
        Get live ETH price with multiple fallbacks.
        
        Priority:
        1. Etherscan API (most accurate, real-time)
        2. Cache (if recent < 5 minutes)
        3. CoinGecko API (backup)
        4. Fallback constant
        
        Returns:
            Current ETH price in USD
        """
        # 1️⃣ Try Etherscan first (if available)
        if self.etherscan:
            try:
                price = self.etherscan.get_eth_price_usd()
                if price and price > 0:
                    # Validate
                    if self._validate_price('ethereum', price):
                        # Cache for 5 minutes
                        if self.cache:
                            self.cache.set("eth_price_usd", price, ttl=300)
                        self.fallback_prices['ETH'] = price
                        self.fallback_prices['WETH'] = price
                        logger.info(f"✅ Live ETH price from Etherscan: ${price:,.2f}")
                        return price
            except Exception as e:
                logger.warning(f"⚠️ Etherscan price failed: {e}")
        
        # 2️⃣ Try cache
        if self.cache:
            cached_price = self.cache.get("eth_price_usd")
            if cached_price:
                logger.info(f"💾 Using cached ETH price: ${cached_price:,.2f}")
                return cached_price
        
        # 3️⃣ Try CoinGecko
        try:
            price = self._fetch_current_price('ethereum')
            if price and self._validate_price('ethereum', price):
                if self.cache:
                    self.cache.set("eth_price_usd", price, ttl=300)
                logger.info(f"✅ ETH price from CoinGecko: ${price:,.2f}")
                return price
        except Exception as e:
            logger.warning(f"⚠️ CoinGecko failed: {e}")
        
        # 4️⃣ Fallback
        fallback = self.fallback_prices['ETH']
        logger.warning(f"⚠️ Using fallback ETH price: ${fallback:,.2f}")
        return fallback
    
    # ========================================================================
    # CURRENT PRICE METHODS
    # ========================================================================
    
    def get_current_price(self, token_address: Optional[str] = None) -> Optional[float]:
        """
        Get current USD price for a token.
        
        ✨ NEW: Uses Etherscan for ETH, CoinGecko for others
        
        Args:
            token_address: Token contract address (None for ETH)
            
        Returns:
            Current USD price
        """
        # Special case for ETH
        if token_address is None or token_address.lower() in [
            '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',  # ETH placeholder
            '0x0000000000000000000000000000000000000000',  # Zero address
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'   # WETH
        ]:
            return self.get_eth_price_live()
        
        # Check cache first
        if self.cache:
            cached_price = self.cache.get_price(token_address or 'ETH')
            if cached_price is not None:
                return cached_price
        
        # Fetch from CoinGecko
        token_id = self._get_token_id(token_address)
        price = self._fetch_current_price(token_id)
        
        # Validate price
        if price and not self._validate_price(token_id, price):
            logger.error(f"Price validation failed for {token_id}, using fallback")
            price = self._get_fallback_price(token_id)
        
        # Cache the result
        if price and self.cache:
            self.cache.cache_price(token_address or 'ETH', price)
        
        return price
    
    def _fetch_current_price(self, token_id: str) -> Optional[float]:
        """Fetch current price from CoinGecko."""
        self._rate_limit()
        
        url = f"{self.coingecko_base}/simple/price"
        params = {
            'ids': token_id,
            'vs_currencies': 'usd'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            price = data.get(token_id, {}).get('usd')
            
            if price:
                logger.debug(f"✅ Fetched current price for {token_id}: ${price:,.2f}")
            
            return price
        except requests.exceptions.Timeout:
            logger.warning(f"CoinGecko timeout for {token_id}")
            return None
        except Exception as e:
            logger.debug(f"Price fetch failed for {token_id}: {e}")
            return None
    
    # ========================================================================
    # HISTORICAL PRICE METHODS
    # ========================================================================
    
    def get_historical_price(
        self,
        token_address: Optional[str],
        timestamp: datetime
    ) -> Optional[float]:
        """
        Get historical USD price for a token at a specific timestamp.
        
        Args:
            token_address: Token contract address (None for ETH)
            timestamp: DateTime of transaction
        
        Returns:
            USD price per token at that time
        """
        token_id = self._get_token_id(token_address)
        
        # CoinGecko requires date string DD-MM-YYYY
        date_str = timestamp.strftime('%d-%m-%Y')
        
        # Check cache with date-specific key
        cache_key = f"{token_address or 'ETH'}:{date_str}"
        if self.cache:
            cached_price = self.cache.get(cache_key, prefix='historical_price')
            if cached_price is not None:
                logger.debug(f"✅ Using cached price for {token_id} on {date_str}: ${cached_price:,.2f}")
                return cached_price
        
        # Fetch from API
        price = self._fetch_historical_price(token_id, date_str)
        
        # Validate price
        if price and not self._validate_price(token_id, price):
            logger.warning(f"Historical price validation failed for {token_id} on {date_str}")
            price = self._get_fallback_price(token_id, timestamp.year)
        
        # Cache with 24h TTL (historical prices don't change)
        if price and self.cache:
            self.cache.set(cache_key, price, ttl=86400, prefix='historical_price')
        
        return price
    
    def _fetch_historical_price(self, token_id: str, date: str) -> Optional[float]:
        """Fetch historical price from CoinGecko."""
        self._rate_limit()
        
        url = f"{self.coingecko_base}/coins/{token_id}/history"
        params = {
            'date': date,
            'localization': 'false'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            price = data.get('market_data', {}).get('current_price', {}).get('usd')
            
            if price:
                logger.debug(f"✅ Fetched historical price for {token_id} on {date}: ${price:,.2f}")
            
            return price
        except requests.exceptions.Timeout:
            logger.warning(f"CoinGecko timeout for {token_id} on {date}")
            return None
        except Exception as e:
            logger.debug(f"Historical price fetch failed for {token_id} on {date}: {e}")
            return None
    
    # ========================================================================
    # FALLBACK METHODS
    # ========================================================================
    
    def _get_fallback_price(self, token_id: str, year: Optional[int] = None) -> Optional[float]:
        """
        Get fallback price when API fails.
        
        Args:
            token_id: Token identifier
            year: Optional year for better estimate
        
        Returns:
            Estimated price or None
        """
        # Historical average prices by year for ETH
        eth_fallback_prices = {
            2024: 3400.0,
            2023: 1800.0,
            2022: 1500.0,
            2021: 3000.0,
            2020: 600.0,
            2019: 200.0,
            2018: 500.0,
        }
        
        if token_id == 'ethereum':
            if year and year in eth_fallback_prices:
                price = eth_fallback_prices[year]
                logger.info(f"Using fallback ETH price for {year}: ${price:,.2f}")
                return price
            return 3400.0  # Default fallback
        
        elif token_id in ['tether', 'usd-coin']:
            return 1.0  # Stablecoins
        
        elif token_id == 'wrapped-bitcoin':
            if year and year in eth_fallback_prices:
                return eth_fallback_prices[year] * 15  # BTC typically ~15x ETH
            return 60000.0  # Default
        
        elif token_id == 'chainlink':
            return 15.0  # LINK average
        
        elif token_id == 'uniswap':
            return 10.0  # UNI average
        
        elif token_id == 'matic-network':
            return 1.0  # MATIC average
        
        return None
    
    # ========================================================================
    # BATCH METHODS
    # ========================================================================
    
    def batch_get_current_prices(self, token_addresses: list) -> Dict[str, float]:
        """
        Get current prices for multiple tokens in one call.
        More efficient than individual calls.
        """
        token_ids = [self._get_token_id(addr) for addr in token_addresses]
        unique_ids = list(set(token_ids))
        
        self._rate_limit()
        
        url = f"{self.coingecko_base}/simple/price"
        params = {
            'ids': ','.join(unique_ids),
            'vs_currencies': 'usd'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Map back to addresses
            result = {}
            for addr in token_addresses:
                token_id = self._get_token_id(addr)
                price = data.get(token_id, {}).get('usd')
                
                # Validate before returning
                if price and self._validate_price(token_id, price):
                    result[addr] = price
                else:
                    result[addr] = None
            
            return result
        except Exception as e:
            logger.error(f"Error batch fetching prices: {e}")
            return {}
