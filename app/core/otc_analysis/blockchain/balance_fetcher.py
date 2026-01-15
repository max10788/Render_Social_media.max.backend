"""
Balance Fetcher - Moralis API Integration
==========================================

Fetches current wallet balances using Moralis API.

Features:
- Native balance (ETH)
- ERC20 token balances
- USD value conversion via PriceOracle
- 5-minute caching
- Rate limit protection
- Retry logic

Version: 1.0
Date: 2025-01-15
"""

import os
import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import requests
from decimal import Decimal

logger = logging.getLogger(__name__)


class BalanceFetcher:
    """
    Fetches current wallet balances using Moralis API.
    
    Moralis API Endpoints:
    - GET /{address}/balance - Native balance
    - GET /{address}/erc20 - ERC20 token balances
    
    Docs: https://docs.moralis.io/web3-data-api/evm/reference/get-native-balance
    """
    
    def __init__(
        self,
        cache_manager=None,
        price_oracle=None,
        api_key: Optional[str] = None,
        chain: str = "eth",
        cache_ttl: int = 300  # 5 minutes
    ):
        """
        Initialize Balance Fetcher.
        
        Args:
            cache_manager: Cache manager instance
            price_oracle: Price oracle for USD conversion
            api_key: Moralis API key (or from env)
            chain: Blockchain chain (default: 'eth')
            cache_ttl: Cache TTL in seconds (default: 300)
        """
        self.cache_manager = cache_manager
        self.price_oracle = price_oracle
        self.api_key = api_key or os.getenv('MORALIS_API_KEY')
        self.chain = chain
        self.cache_ttl = cache_ttl
        
        # Moralis API base URL
        self.base_url = "https://deep-index.moralis.io/api/v2.2"
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.2  # 200ms between requests
        
        # Validate API key
        if not self.api_key:
            logger.warning("⚠️ No Moralis API key found! Balance fetching will fail.")
        else:
            logger.info(f"✅ BalanceFetcher initialized (chain={chain}, cache_ttl={cache_ttl}s)")
    
    
    def _rate_limit_wait(self):
        """Wait if necessary to respect rate limits."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        max_retries: int = 3
    ) -> Optional[Dict]:
        """
        Make HTTP request to Moralis API with retry logic.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            max_retries: Maximum number of retries
            
        Returns:
            Response JSON or None on failure
        """
        if not self.api_key:
            logger.error("❌ Cannot make request: No Moralis API key")
            return None
        
        url = f"{self.base_url}{endpoint}"
        headers = {
            "accept": "application/json",
            "X-API-Key": self.api_key
        }
        
        for attempt in range(max_retries):
            try:
                # Rate limiting
                self._rate_limit_wait()
                
                # Make request
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                # Check status
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limit hit
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"⏱️ Rate limit hit, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ Moralis API error {response.status_code}: {response.text}")
                    return None
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️ Request timeout (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None
            except Exception as e:
                logger.error(f"❌ Request error: {e}")
                return None
        
        return None
    
    
    def get_native_balance(
        self,
        address: str,
        use_cache: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Get native balance (ETH) for address.
        
        Args:
            address: Ethereum address
            use_cache: Use cached data if available
            
        Returns:
            {
                'balance_wei': str,
                'balance_eth': float,
                'balance_usd': float,
                'timestamp': datetime
            }
        """
        # Check cache
        if use_cache and self.cache_manager:
            cache_key = f"balance:native:{address.lower()}"
            cached = self.cache_manager.get(cache_key)
            if cached:
                logger.debug(f"✅ Cache hit: native balance for {address[:10]}...")
                return cached
        
        logger.info(f"📡 Fetching native balance for {address[:10]}...")
        
        try:
            # Call Moralis API
            endpoint = f"/{address}/balance"
            params = {"chain": self.chain}
            
            result = self._make_request(endpoint, params)
            
            if not result:
                logger.warning(f"⚠️ No balance data returned for {address[:10]}")
                return None
            
            # Parse balance
            balance_wei = result.get('balance', '0')
            balance_eth = float(balance_wei) / 1e18
            
            # Get USD value
            balance_usd = 0.0
            if self.price_oracle and balance_eth > 0:
                try:
                    eth_price = self.price_oracle.get_current_price('ETH')
                    if eth_price:
                        balance_usd = balance_eth * eth_price
                except Exception as price_error:
                    logger.debug(f"⚠️ Could not get ETH price: {price_error}")
            
            balance_data = {
                'balance_wei': balance_wei,
                'balance_eth': balance_eth,
                'balance_usd': balance_usd,
                'timestamp': datetime.now()
            }
            
            # Cache result
            if self.cache_manager:
                cache_key = f"balance:native:{address.lower()}"
                self.cache_manager.set(cache_key, balance_data, ttl=self.cache_ttl)
            
            logger.info(
                f"✅ Native balance: {balance_eth:.4f} ETH "
                f"(${balance_usd:,.2f})"
            )
            
            return balance_data
            
        except Exception as e:
            logger.error(f"❌ Error fetching native balance: {e}", exc_info=True)
            return None
    
    
    def get_token_balances(
        self,
        address: str,
        use_cache: bool = True,
        exclude_spam: bool = True
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get ERC20 token balances for address.
        
        Args:
            address: Ethereum address
            use_cache: Use cached data if available
            exclude_spam: Exclude spam/scam tokens
            
        Returns:
            List of token balances:
            [
                {
                    'token_address': str,
                    'symbol': str,
                    'name': str,
                    'decimals': int,
                    'balance': str,
                    'balance_formatted': float,
                    'balance_usd': float,
                    'possible_spam': bool
                }
            ]
        """
        # Check cache
        if use_cache and self.cache_manager:
            cache_key = f"balance:tokens:{address.lower()}"
            cached = self.cache_manager.get(cache_key)
            if cached:
                logger.debug(f"✅ Cache hit: token balances for {address[:10]}...")
                return cached
        
        logger.info(f"📡 Fetching token balances for {address[:10]}...")
        
        try:
            # Call Moralis API
            endpoint = f"/{address}/erc20"
            params = {
                "chain": self.chain,
                "exclude_spam": str(exclude_spam).lower()
            }
            
            result = self._make_request(endpoint, params)
            
            if not result:
                logger.warning(f"⚠️ No token data returned for {address[:10]}")
                return []
            
            # Parse tokens
            tokens = []
            total_usd_value = 0.0
            
            for token_data in result:
                try:
                    # Basic token info
                    token_address = token_data.get('token_address')
                    symbol = token_data.get('symbol', 'UNKNOWN')
                    name = token_data.get('name', '')
                    decimals = int(token_data.get('decimals', 18))
                    balance_raw = token_data.get('balance', '0')
                    possible_spam = token_data.get('possible_spam', False)
                    
                    # Skip spam if requested
                    if exclude_spam and possible_spam:
                        continue
                    
                    # Format balance
                    balance_formatted = float(balance_raw) / (10 ** decimals)
                    
                    # Skip zero balances
                    if balance_formatted == 0:
                        continue
                    
                    # Get USD value
                    balance_usd = 0.0
                    if self.price_oracle and balance_formatted > 0:
                        try:
                            # Try to get token price
                            token_price = self.price_oracle.get_current_price(symbol)
                            if not token_price and token_address:
                                # Fallback: try by token address
                                token_price = self.price_oracle.get_current_price(token_address)
                            
                            if token_price:
                                balance_usd = balance_formatted * token_price
                                total_usd_value += balance_usd
                        except Exception as price_error:
                            logger.debug(f"⚠️ Could not get price for {symbol}: {price_error}")
                    
                    tokens.append({
                        'token_address': token_address,
                        'symbol': symbol,
                        'name': name,
                        'decimals': decimals,
                        'balance': balance_raw,
                        'balance_formatted': balance_formatted,
                        'balance_usd': balance_usd,
                        'possible_spam': possible_spam
                    })
                    
                except Exception as token_error:
                    logger.debug(f"⚠️ Error parsing token: {token_error}")
                    continue
            
            # Sort by USD value (highest first)
            tokens.sort(key=lambda x: x['balance_usd'], reverse=True)
            
            # Cache result
            if self.cache_manager:
                cache_key = f"balance:tokens:{address.lower()}"
                self.cache_manager.set(cache_key, tokens, ttl=self.cache_ttl)
            
            logger.info(
                f"✅ Token balances: {len(tokens)} tokens "
                f"(total: ${total_usd_value:,.2f})"
            )
            
            return tokens
            
        except Exception as e:
            logger.error(f"❌ Error fetching token balances: {e}", exc_info=True)
            return []
    
    
    def get_total_balance_usd(
        self,
        address: str,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get total balance in USD (native + tokens).
        
        Args:
            address: Ethereum address
            use_cache: Use cached data if available
            
        Returns:
            {
                'native_balance_usd': float,
                'token_balance_usd': float,
                'total_balance_usd': float,
                'native_eth': float,
                'token_count': int,
                'top_tokens': List[Dict],
                'timestamp': datetime
            }
        """
        logger.info(f"💰 Calculating total balance for {address[:10]}...")
        
        try:
            # Get native balance
            native = self.get_native_balance(address, use_cache=use_cache)
            native_usd = native['balance_usd'] if native else 0.0
            native_eth = native['balance_eth'] if native else 0.0
            
            # Get token balances
            tokens = self.get_token_balances(address, use_cache=use_cache)
            token_usd = sum(t['balance_usd'] for t in tokens) if tokens else 0.0
            
            # Calculate total
            total_usd = native_usd + token_usd
            
            # Top 5 tokens by value
            top_tokens = []
            if tokens:
                top_tokens = sorted(
                    tokens,
                    key=lambda x: x['balance_usd'],
                    reverse=True
                )[:5]
            
            result = {
                'native_balance_usd': native_usd,
                'token_balance_usd': token_usd,
                'total_balance_usd': total_usd,
                'native_eth': native_eth,
                'token_count': len(tokens) if tokens else 0,
                'top_tokens': [
                    {
                        'symbol': t['symbol'],
                        'balance': t['balance_formatted'],
                        'value_usd': t['balance_usd']
                    }
                    for t in top_tokens
                ],
                'timestamp': datetime.now()
            }
            
            logger.info(
                f"✅ Total balance: ${total_usd:,.2f} "
                f"(Native: ${native_usd:,.2f}, Tokens: ${token_usd:,.2f})"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error calculating total balance: {e}", exc_info=True)
            return {
                'native_balance_usd': 0.0,
                'token_balance_usd': 0.0,
                'total_balance_usd': 0.0,
                'native_eth': 0.0,
                'token_count': 0,
                'top_tokens': [],
                'timestamp': datetime.now()
            }
    
    
    def get_balance_summary(
        self,
        address: str,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get comprehensive balance summary.
        
        Convenience method that returns all balance data in one call.
        
        Returns:
            {
                'address': str,
                'native': {...},
                'tokens': [...],
                'total_usd': float,
                'breakdown': {...},
                'timestamp': datetime
            }
        """
        logger.info(f"📊 Getting balance summary for {address[:10]}...")
        
        try:
            native = self.get_native_balance(address, use_cache=use_cache)
            tokens = self.get_token_balances(address, use_cache=use_cache)
            total = self.get_total_balance_usd(address, use_cache=use_cache)
            
            return {
                'address': address,
                'native': native,
                'tokens': tokens,
                'total_usd': total['total_balance_usd'],
                'breakdown': {
                    'native_usd': total['native_balance_usd'],
                    'native_eth': total['native_eth'],
                    'tokens_usd': total['token_balance_usd'],
                    'token_count': total['token_count'],
                    'top_tokens': total['top_tokens']
                },
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting balance summary: {e}", exc_info=True)
            return {
                'address': address,
                'native': None,
                'tokens': [],
                'total_usd': 0.0,
                'breakdown': {},
                'timestamp': datetime.now()
            }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_balance_fetcher(
    cache_manager=None,
    price_oracle=None,
    api_key: Optional[str] = None
) -> BalanceFetcher:
    """
    Create and initialize a BalanceFetcher instance.
    
    Args:
        cache_manager: Cache manager instance
        price_oracle: Price oracle instance
        api_key: Moralis API key (optional)
        
    Returns:
        Initialized BalanceFetcher
    """
    return BalanceFetcher(
        cache_manager=cache_manager,
        price_oracle=price_oracle,
        api_key=api_key
    )


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    'BalanceFetcher',
    'create_balance_fetcher'
]
