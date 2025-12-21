import requests
import time
from typing import Optional, Dict
from datetime import datetime, timedelta
from otc_analysis.utils.cache import CacheManager

class PriceOracle:
    """
    Fetches historical and current cryptocurrency prices.
    Uses CoinGecko and CoinMarketCap APIs.
    Implements caching strategy (5 minute TTL).
    """
    
    def __init__(self, cache_manager: Optional[CacheManager] = None):
        self.cache = cache_manager
        self.coingecko_base = "https://api.coingecko.com/api/v3"
        self.rate_limit_delay = 1.5  # ← Von 1.2 auf 1.5 erhöhen (40 calls/min statt 50)
        self.last_request_time = 0
        
        # Token address to CoinGecko ID mapping
        self.token_id_map = {
            None: 'ethereum',  # Native ETH
            '0xdac17f958d2ee523a2206206994597c13d831ec7': 'tether',  # USDT
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'usd-coin',  # USDC
            '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 'wrapped-bitcoin',  # WBTC
            # Add more as needed
        }
    
    def _rate_limit(self):
        """Enforce rate limiting for API calls."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        self.last_request_time = time.time()
    
    def _get_token_id(self, token_address: Optional[str]) -> str:
        """
        Convert token address to CoinGecko ID.
        Falls back to ETH if unknown.
        """
        if token_address is None:
            return 'ethereum'
        
        token_lower = token_address.lower()
        return self.token_id_map.get(token_lower, 'ethereum')
    
    def get_current_price(self, token_address: Optional[str] = None) -> Optional[float]:
        """
        Get current USD price for a token.
        Checks cache first, fetches if not cached.
        """
        # Check cache
        if self.cache:
            cached_price = self.cache.get_price(token_address or 'ETH')
            if cached_price is not None:
                return cached_price
        
        # Fetch from API
        token_id = self._get_token_id(token_address)
        price = self._fetch_current_price(token_id)
        
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
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            return data.get(token_id, {}).get('usd')
        except requests.exceptions.Timeout:
            logger.warning(f"CoinGecko timeout for {token_id}")
            return None
        except Exception as e:
            logger.debug(f"Price fetch failed for {token_id}: {e}")
            return None
    
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
            USD price at that time
        """
        token_id = self._get_token_id(token_address)
        
        # CoinGecko requires date string DD-MM-YYYY
        date_str = timestamp.strftime('%d-%m-%Y')
        
        # Check cache with date-specific key
        cache_key = f"{token_address or 'ETH'}:{date_str}"
        if self.cache:
            cached_price = self.cache.get(cache_key, prefix='historical_price')
            if cached_price is not None:
                return cached_price
        
        # Fetch from API
        price = self._fetch_historical_price(token_id, date_str)
        
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
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            return data.get('market_data', {}).get('current_price', {}).get('usd')
        except requests.exceptions.Timeout:
            logger.warning(f"CoinGecko timeout for {token_id} on {date}")
            return None
        except Exception as e:
            logger.debug(f"Historical price fetch failed: {e}")
            return None
    
    def get_price_range(
        self,
        token_address: Optional[str],
        days: int = 30
    ) -> Optional[Dict]:
        """
        Get price range (min, max, avg) for last N days.
        Useful for calculating rolling statistics.
        """
        token_id = self._get_token_id(token_address)
        
        self._rate_limit()
        
        url = f"{self.coingecko_base}/coins/{token_id}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': days
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            prices = [p[1] for p in data.get('prices', [])]
            
            if not prices:
                return None
            
            return {
                'min': min(prices),
                'max': max(prices),
                'avg': sum(prices) / len(prices),
                'current': prices[-1],
                'prices': prices
            }
        except Exception as e:
            print(f"Error fetching price range for {token_id}: {e}")
            return None
    
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
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Map back to addresses
            result = {}
            for addr in token_addresses:
                token_id = self._get_token_id(addr)
                result[addr] = data.get(token_id, {}).get('usd')
            
            return result
        except Exception as e:
            print(f"Error batch fetching prices: {e}")
            return {}
