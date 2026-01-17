"""
Price Oracle - WITH Enhanced Error Tracking
============================================

✅ ENHANCED: Detailed error tracking and logging
✅ ENHANCED: Shows exact CoinGecko API responses
✅ ENHANCED: Tracks last_error for debugging
✅ NEW: HTTP status codes and error messages

Version: 3.1 with Error Tracking
Date: 2024-12-30
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
    
    ✨ NEW: Enhanced error tracking for debugging
    """

    def __init__(self, cache_manager=None, etherscan=None):
        """
        Initialize price oracle.
        
        Args:
            cache_manager: CacheManager instance for caching
            etherscan: EtherscanAPI instance for live ETH price
        """
        self.cache = cache_manager
        self.etherscan = etherscan
        self.coingecko_base = "https://api.coingecko.com/api/v3"
        self.rate_limit_delay = 1.5
        self.last_request_time = 0
        self.session = requests.Session()
        
        # Error tracking
        self.last_error = None
        self.error_count = 0
        self.success_count = 0
        
        # ✅ MASSIV ERWEITERTE Token Map (Top 100 Tokens by Market Cap)
        self.token_id_map = {
            # Native & Wrapped
            None: 'ethereum',
            '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee': 'ethereum',
            '0x0000000000000000000000000000000000000000': 'ethereum',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'weth',  # WETH
            
            # Stablecoins (Top Priority!)
            '0xdac17f958d2ee523a2206206994597c13d831ec7': 'tether',  # USDT
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'usd-coin',  # USDC
            '0x6b175474e89094c44da98b954eedeac495271d0f': 'dai',  # DAI
            '0x4fabb145d64652a948d72533023f6e7a623c7c53': 'binance-usd',  # BUSD
            '0x8e870d67f660d95d5be530380d0ec0bd388289e1': 'pax-dollar',  # USDP
            '0x853d955acef822db058eb8505911ed77f175b99e': 'frax',  # FRAX
            '0x5f98805a4e8be255a32880fdec7f6728c6568ba0': 'liquity-usd',  # LUSD
            
            # Major Tokens
            '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 'wrapped-bitcoin',  # WBTC
            '0x514910771af9ca656af840dff83e8264ecf986ca': 'chainlink',  # LINK
            '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984': 'uniswap',  # UNI
            '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0': 'matic-network',  # MATIC
            '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9': 'aave',  # AAVE
            '0xc00e94cb662c3520282e6f5717214004a7f26888': 'compound-governance-token',  # COMP
            '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2': 'maker',  # MKR
            '0x6b3595068778dd592e39a122f4f5a5cf09c90fe2': 'sushi',  # SUSHI
            '0xd533a949740bb3306d119cc777fa900ba034cd52': 'curve-dao-token',  # CRV
            '0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e': 'yearn-finance',  # YFI
            '0xba100000625a3754423978a60c9317c58a424e3d': 'balancer',  # BAL
            '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f': 'havven',  # SNX
            '0x0d8775f648430679a709e98d2b0cb6250d2887ef': 'basic-attention-token',  # BAT
            '0x0f5d2fb29fb7d3cfee444a200298f468908cc942': 'decentraland',  # MANA
            '0x4e15361fd6b4bb609fa63c81a2be19d873717870': 'fantom',  # FTM
            '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0': 'polygon',  # MATIC (duplicate check)
            '0x111111111117dc0aa78b770fa6a738034120c302': '1inch',  # 1INCH
            '0x1985365e9f78359a9b6ad760e32412f4a445e862': 'augur',  # REP
            
            # Layer 2 & Scaling
            '0x42bbfa2e77757c645eeaad1655e0911a7553efbc': 'boba-network',  # BOBA
            '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270': 'matic-network',  # WMATIC
            
            # DeFi Blue Chips
            '0x4d224452801aced8b2f0aebe155379bb5d594381': 'apecoin',  # APE
            '0x5a98fcbea516cf06857215779fd812ca3bef1b32': 'lido-dao',  # LDO
            '0xae7ab96520de3a18e5e111b5eaab095312d7fe84': 'staked-ether',  # stETH
            '0x31429d1856ad1377a8a0079410b297e1a9e214c2': 'angle-protocol',  # ANGLE
            
            # Meme Coins (for completeness)
            '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce': 'shiba-inu',  # SHIB
            '0x4d224452801aced8b2f0aebe155379bb5d594381': 'apecoin',  # APE
            '0xc944e90c64b2c07662a292be6244bdf05cda44a7': 'the-graph',  # GRT
        }
        
        # ✅ NEW: Symbol-to-CoinGecko-ID Map (Fallback)
        self.symbol_to_id_map = {
            'ETH': 'ethereum',
            'WETH': 'weth',
            'USDT': 'tether',
            'USDC': 'usd-coin',
            'DAI': 'dai',
            'BUSD': 'binance-usd',
            'WBTC': 'wrapped-bitcoin',
            'LINK': 'chainlink',
            'UNI': 'uniswap',
            'MATIC': 'matic-network',
            'AAVE': 'aave',
            'COMP': 'compound-governance-token',
            'MKR': 'maker',
            'SUSHI': 'sushi',
            'CRV': 'curve-dao-token',
            'YFI': 'yearn-finance',
            'SNX': 'havven',
            'BAL': 'balancer',
            'BAT': 'basic-attention-token',
            'MANA': 'decentraland',
            'FTM': 'fantom',
            '1INCH': '1inch',
            'LDO': 'lido-dao',
            'APE': 'apecoin',
            'SHIB': 'shiba-inu',
            'GRT': 'the-graph',
        }
        
        # Price ranges for validation
        self.price_ranges = {
            'ethereum': (100, 10000),
            'weth': (100, 10000),
            'tether': (0.95, 1.05),
            'usd-coin': (0.95, 1.05),
            'dai': (0.95, 1.05),
            'wrapped-bitcoin': (10000, 200000),
            'chainlink': (5, 100),
            'uniswap': (3, 50),
            'matic-network': (0.3, 5),
        }
        
        # Fallback prices
        self.fallback_prices = {
            'ETH': 3400.0,
            'WETH': 3400.0,
            'USDT': 1.0,
            'USDC': 1.0,
            'DAI': 1.0,
            'BUSD': 1.0,
        }
    
    def _rate_limit(self):
        """Enforce rate limiting for API calls."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        self.last_request_time = time.time()

    def _get_token_id(
        self, 
        token_address: Optional[str],
        token_symbol: Optional[str] = None
    ) -> tuple[str, str]:
        """
        Convert token address/symbol to CoinGecko ID.
        
        ✅ NEW: Multi-tier lookup strategy:
        1. Try by contract address (most accurate)
        2. Fallback to symbol lookup
        3. Fallback to 'ethereum' (last resort)
        
        Args:
            token_address: Token contract address
            token_symbol: Token symbol (e.g., 'USDT')
            
        Returns:
            Tuple of (token_id, lookup_method)
        """
        # Native ETH
        if token_address is None:
            return ('ethereum', 'native')
        
        # Normalize address
        token_lower = token_address.lower()
        
        # 1️⃣ PRIMARY: Lookup by address
        if token_lower in self.token_id_map:
            token_id = self.token_id_map[token_lower]
            logger.debug(f"✅ Token ID by address: {token_address[:10]}... → {token_id}")
            return (token_id, 'address')
        
        # 2️⃣ SECONDARY: Lookup by symbol
        if token_symbol:
            symbol_upper = token_symbol.upper()
            if symbol_upper in self.symbol_to_id_map:
                token_id = self.symbol_to_id_map[symbol_upper]
                logger.debug(f"✅ Token ID by symbol: {symbol_upper} → {token_id}")
                return (token_id, 'symbol')
        
        # 3️⃣ TERTIARY: Try CoinGecko contract lookup API
        # (Will be implemented in _fetch_price_by_contract)
        logger.debug(f"⚠️ Token {token_address[:10]}... not in maps, will try contract API")
        return (None, 'contract')  # Signal to use contract API
    
    def _validate_price(self, token_id: str, price: float) -> bool:
        """
        Validate that price is within reasonable range.
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
    # ✨ ENHANCED: LIVE ETH PRICE WITH ERROR TRACKING
    # ========================================================================
    
    def get_eth_price_live(self) -> float:
        """
        Get live ETH price with multiple fallbacks.
        
        Priority:
        1. Etherscan API (most accurate, real-time)
        2. Cache (if recent < 5 minutes)
        3. CoinGecko API (backup)
        4. Fallback constant
        """
        # 1️⃣ Try Etherscan first
        if self.etherscan:
            try:
                price = self.etherscan.get_eth_price_usd()
                if price and price > 0:
                    if self._validate_price('ethereum', price):
                        if self.cache:
                            self.cache.set("eth_price_usd", price, ttl=300)
                        self.fallback_prices['ETH'] = price
                        self.fallback_prices['WETH'] = price
                        logger.info(f"✅ Live ETH price from Etherscan: ${price:,.2f}")
                        self.success_count += 1
                        return price
            except Exception as e:
                logger.warning(f"⚠️ Etherscan price failed: {e}")
                self.last_error = f"Etherscan: {str(e)}"
        
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
                self.success_count += 1
                return price
        except Exception as e:
            logger.warning(f"⚠️ CoinGecko failed: {e}")
            self.last_error = f"CoinGecko: {str(e)}"
        
        # 4️⃣ Fallback
        fallback = self.fallback_prices['ETH']
        logger.warning(f"⚠️ Using fallback ETH price: ${fallback:,.2f}")
        return fallback
    
    # ========================================================================
    # CURRENT PRICE METHODS
    # ========================================================================
    
    def get_current_price(self, token_address: Optional[str] = None) -> Optional[float]:
        """Get current USD price for a token."""
        # Special case for ETH
        if token_address is None or token_address.lower() in [
            '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
            '0x0000000000000000000000000000000000000000',
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ]:
            return self.get_eth_price_live()
        
        # Check cache
        if self.cache:
            cached_price = self.cache.get_price(token_address or 'ETH')
            if cached_price is not None:
                return cached_price
        
        # Fetch from CoinGecko
        token_id = self._get_token_id(token_address)
        price = self._fetch_current_price(token_id)
        
        if price and not self._validate_price(token_id, price):
            logger.error(f"Price validation failed for {token_id}, using fallback")
            price = self._get_fallback_price(token_id)
        
        if price and self.cache:
            self.cache.cache_price(token_address or 'ETH', price)
        
        return price
    
    def _fetch_current_price(self, token_id: str) -> Optional[float]:
        """
        ✅ ENHANCED: Fetch current price with detailed error tracking.
        """
        self._rate_limit()
        
        url = f"{self.coingecko_base}/simple/price"
        params = {
            'ids': token_id,
            'vs_currencies': 'usd'
        }
        
        try:
            logger.debug(f"🔍 CoinGecko API: GET {url}?ids={token_id}")
            
            response = self.session.get(url, params=params, timeout=10)
            
            # ✅ NEW: Log HTTP status
            logger.debug(f"   📡 HTTP {response.status_code}")
            
            response.raise_for_status()
            data = response.json()
            
            # ✅ NEW: Log raw response
            logger.debug(f"   📄 Response: {data}")
            
            price = data.get(token_id, {}).get('usd')
            
            if price:
                logger.debug(f"   ✅ Price: ${price:,.2f}")
                self.success_count += 1
                self.last_error = None
                return price
            else:
                logger.debug(f"   ❌ No price found in response")
                self.last_error = f"Token '{token_id}' not found in CoinGecko response"
                self.error_count += 1
                return None
            
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 'unknown'
            error_msg = f"HTTP {status_code}"
            
            # ✅ NEW: Parse CoinGecko error message
            try:
                error_data = e.response.json() if e.response else {}
                if 'error' in error_data:
                    error_msg = f"HTTP {status_code}: {error_data['error']}"
            except:
                pass
            
            logger.debug(f"   ❌ {error_msg}")
            self.last_error = error_msg
            self.error_count += 1
            return None
            
        except requests.exceptions.Timeout:
            logger.debug(f"   ❌ Timeout")
            self.last_error = "CoinGecko timeout"
            self.error_count += 1
            return None
            
        except Exception as e:
            logger.debug(f"   ❌ Error: {str(e)}")
            self.last_error = str(e)
            self.error_count += 1
            return None
    
    # ========================================================================
    # ✨ ENHANCED: HISTORICAL PRICE WITH DETAILED TRACKING
    # ========================================================================
    
    def get_historical_price(
        self,
        token_address: Optional[str],
        timestamp: datetime,
        token_symbol: Optional[str] = None
    ) -> Optional[float]:
        """
        Get historical USD price for a token at a specific timestamp.
        
        ✅ ENHANCED: Multi-tier fallback strategy:
        1. Cache lookup
        2. Token ID by address/symbol → CoinGecko history API
        3. Contract address → CoinGecko contract API
        4. Fallback prices (stablecoins, major tokens)
        
        Args:
            token_address: Token contract address
            timestamp: Historical timestamp
            token_symbol: Token symbol (helps with lookup)
            
        Returns:
            USD price or None
        """
        # CoinGecko requires date string DD-MM-YYYY
        date_str = timestamp.strftime('%d-%m-%Y')
        
        # ====================================================================
        # STEP 1: Check cache
        # ====================================================================
        cache_key = f"{token_address or 'ETH'}:{date_str}"
        if self.cache:
            cached_price = self.cache.get(cache_key, prefix='historical_price')
            if cached_price is not None:
                logger.debug(f"💾 Cached: {token_symbol or token_address[:10]} @ {date_str} = ${cached_price:,.2f}")
                return cached_price
        
        # ====================================================================
        # STEP 2: Get token ID (with multi-tier lookup)
        # ====================================================================
        token_id, lookup_method = self._get_token_id(token_address, token_symbol)
        
        logger.debug(
            f"🔍 Historical: {token_symbol or token_address[:10] or 'ETH'} @ {date_str} "
            f"(method: {lookup_method})"
        )
        
        price = None
        
        # ====================================================================
        # STEP 3: Try appropriate API method
        # ====================================================================
        
        if token_id and lookup_method in ['address', 'symbol', 'native']:
            # Use standard CoinGecko history API
            price = self._fetch_historical_price(token_id, date_str, token_address)
        
        elif lookup_method == 'contract' and token_address:
            # Use CoinGecko contract API (fallback for unknown tokens)
            logger.debug(f"   → Trying contract API for {token_address[:10]}...")
            price = self._fetch_price_by_contract(token_address, timestamp)
        
        # ====================================================================
        # STEP 4: Validate price
        # ====================================================================
        
        if price and token_id:
            if not self._validate_price(token_id, price):
                logger.warning(f"⚠️ Validation failed, trying fallback")
                price = self._get_fallback_price(token_id, timestamp.year, token_symbol)
        
        # ====================================================================
        # STEP 5: Last resort - stablecoin or major token fallback
        # ====================================================================
        
        if not price and token_symbol:
            price = self._get_fallback_price_by_symbol(token_symbol, timestamp.year)
            if price:
                logger.info(f"   💵 Using fallback for {token_symbol}: ${price:,.2f}")
        
        # ====================================================================
        # STEP 6: Cache result (even if None to avoid repeated API calls)
        # ====================================================================
        
        if self.cache:
            # Cache successful prices for 24h, failures for 1h
            ttl = 86400 if price else 3600
            self.cache.set(cache_key, price, ttl=ttl, prefix='historical_price')
        
        if price:
            logger.debug(f"   ✅ Final price: ${price:,.2f}")
        else:
            logger.debug(f"   ❌ Could not find price")
        
        return price
    
    
    def _get_fallback_price_by_symbol(
        self,
        symbol: str,
        year: Optional[int] = None
    ) -> Optional[float]:
        """
        ✅ NEW: Get fallback price by symbol.
        
        Used when all API methods fail but we know it's a common token.
        """
        symbol_upper = symbol.upper()
        
        # Stablecoins - always $1
        if symbol_upper in ['USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDD', 'FRAX']:
            return 1.0
        
        # Major tokens - use reasonable fallback
        fallbacks = {
            'ETH': 3400.0,
            'WETH': 3400.0,
            'WBTC': 60000.0,
            'LINK': 15.0,
            'UNI': 10.0,
            'MATIC': 1.0,
            'AAVE': 150.0,
            'COMP': 80.0,
            'MKR': 2000.0,
            'SNX': 5.0,
            'CRV': 1.0,
            'BAL': 15.0,
        }
        
        if symbol_upper in fallbacks:
            price = fallbacks[symbol_upper]
            logger.debug(f"   💵 Fallback for {symbol_upper}: ${price:,.2f}")
            return price
        
        return None

def get_live_token_price(
    self,
    token_address_or_symbol: str
) -> Optional[float]:
    """
    Get CURRENT live price for any token.
    
    ✨ NEW: Moralis-First Strategy für Live-Preise
    
    Priority:
    1. Moralis Price API (aktuelle DEX-aggregierte Preise)
    2. Etherscan (für ETH)
    3. CoinGecko Current Price API
    4. Stablecoin Constants
    5. Fallback Values
    
    Args:
        token_address_or_symbol: Token address (0x...) or symbol (ETH, USDT)
        
    Returns:
        Current USD price or None
    """
    # ====================================================================
    # STEP 1: Normalize input
    # ====================================================================
    
    is_address = token_address_or_symbol and token_address_or_symbol.startswith('0x')
    
    # ====================================================================
    # STEP 2: Handle ETH specially (Etherscan für beste Genauigkeit)
    # ====================================================================
    
    if not is_address:
        symbol_upper = token_address_or_symbol.upper()
        
        # ETH/WETH → Use Etherscan
        if symbol_upper in ['ETH', 'WETH']:
            price = self.get_eth_price_live()
            if price:
                logger.debug(f"✅ Live ETH: ${price:,.2f} (Etherscan)")
                return price
        
        # Stablecoins → Always $1
        if symbol_upper in ['USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDD', 'FRAX', 'USDP']:
            logger.debug(f"✅ Stablecoin: {symbol_upper} = $1.00")
            return 1.0
    
    # ====================================================================
    # STEP 3: Check cache (5 min TTL für live prices)
    # ====================================================================
    
    cache_key = f"live_price:{token_address_or_symbol}"
    
    if self.cache:
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug(f"💾 Cached live price: ${cached:,.2f}")
            return cached
    
    # ====================================================================
    # STEP 4: Try Moralis Price API (PRIORITY 1)
    # ====================================================================
    
    price = None
    
    if is_address:
        price = self._fetch_moralis_price(token_address_or_symbol)
        
        if price:
            logger.debug(f"✅ Moralis live price: ${price:,.2f}")
            
            # Cache for 5 minutes
            if self.cache:
                self.cache.set(cache_key, price, ttl=300)
            
            return price
    
    # ====================================================================
    # STEP 5: Try CoinGecko Current Price API (Fallback)
    # ====================================================================
    
    if is_address:
        token_id, _ = self._get_token_id(token_address_or_symbol)
    else:
        # Try symbol lookup
        symbol_upper = token_address_or_symbol.upper()
        token_id = self.symbol_to_id_map.get(symbol_upper)
    
    if token_id:
        price = self._fetch_current_price(token_id)
        
        if price:
            logger.debug(f"✅ CoinGecko live price: ${price:,.2f}")
            
            # Cache for 5 minutes
            if self.cache:
                self.cache.set(cache_key, price, ttl=300)
            
            return price
    
    # ====================================================================
    # STEP 6: Fallback to hardcoded values
    # ====================================================================
    
    if not is_address:
        fallback = self.fallback_prices.get(token_address_or_symbol.upper())
        if fallback:
            logger.debug(f"💵 Fallback: {token_address_or_symbol} = ${fallback:,.2f}")
            return fallback
    
    logger.debug(f"❌ No live price found for {token_address_or_symbol}")
    return None


def _fetch_moralis_price(self, token_address: str) -> Optional[float]:
    """
    Fetch current token price from Moralis Price API.
    
    Endpoint: GET /erc20/{address}/price?chain=eth
    
    Returns:
        {
            "usdPrice": 3315.37,
            "exchangeAddress": "0x...",
            "exchangeName": "Uniswap v3"
        }
    """
    if not self.moralis_api_key:
        return None
    
    # Handle ETH/WETH
    if token_address.lower() in [
        '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
        '0x0000000000000000000000000000000000000000'
    ]:
        # Use WETH for price lookup
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    
    try:
        url = f"https://deep-index.moralis.io/api/v2.2/erc20/{token_address}/price"
        headers = {
            'X-API-Key': self.moralis_api_key,
            'Accept': 'application/json'
        }
        params = {'chain': 'eth'}
        
        logger.debug(f"   🔍 Moralis Price API: {token_address[:10]}...")
        
        response = self.session.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            price = data.get('usdPrice')
            exchange = data.get('exchangeName', 'Unknown')
            
            if price:
                logger.debug(f"   ✅ ${price:,.4f} (from {exchange})")
                self.success_count += 1
                return float(price)
            else:
                logger.debug(f"   ❌ No usdPrice in response")
                return None
        
        elif response.status_code == 429:
            logger.warning("   ⏱️  Moralis rate limited")
            self.last_error = "Moralis rate limit"
            return None
        
        elif response.status_code == 404:
            logger.debug(f"   ℹ️  Token not found in Moralis")
            return None
        
        else:
            logger.debug(f"   ❌ HTTP {response.status_code}")
            return None
        
    except requests.exceptions.Timeout:
        logger.debug(f"   ⏱️  Moralis timeout")
        return None
        
    except Exception as e:
        logger.debug(f"   ❌ Moralis error: {e}")
        return None
    
    def _fetch_historical_price(self, token_id: str, date: str, token_address: Optional[str] = None) -> Optional[float]:
        """
        ✅ ENHANCED: Fetch historical price with DETAILED error tracking.
        
        This is where most failures happen - we need to see WHY!
        """
        self._rate_limit()
        
        url = f"{self.coingecko_base}/coins/{token_id}/history"
        params = {
            'date': date,
            'localization': 'false'
        }
        
        try:
            logger.debug(f"   🌐 CoinGecko API: GET {url}")
            logger.debug(f"      • token_id: {token_id}")
            logger.debug(f"      • date: {date}")
            logger.debug(f"      • token_address: {token_address}")
            
            response = self.session.get(url, params=params, timeout=10)
            
            # ✅ NEW: Log HTTP status
            logger.debug(f"   📡 HTTP Status: {response.status_code}")
            
            # ✅ NEW: Check for rate limiting BEFORE raising
            if response.status_code == 429:
                logger.warning(f"   ⏱️  RATE LIMITED by CoinGecko!")
                self.last_error = "Rate limit exceeded (HTTP 429)"
                self.error_count += 1
                return None
            
            response.raise_for_status()
            data = response.json()
            
            # ✅ NEW: Log response structure
            logger.debug(f"   📄 Response keys: {list(data.keys())}")
            
            # Check if market_data exists
            if 'market_data' not in data:
                logger.debug(f"   ❌ No 'market_data' in response")
                logger.debug(f"   📄 Full response: {data}")
                self.last_error = f"No market_data for {token_id} on {date}"
                self.error_count += 1
                return None
            
            market_data = data.get('market_data', {})
            logger.debug(f"   📊 market_data keys: {list(market_data.keys())}")
            
            # Check if current_price exists
            if 'current_price' not in market_data:
                logger.debug(f"   ❌ No 'current_price' in market_data")
                self.last_error = f"No current_price for {token_id} on {date}"
                self.error_count += 1
                return None
            
            current_price = market_data.get('current_price', {})
            logger.debug(f"   💰 current_price currencies: {list(current_price.keys())}")
            
            # Get USD price
            price = current_price.get('usd')
            
            if price:
                logger.debug(f"   ✅ Historical price: ${price:,.2f}")
                self.success_count += 1
                self.last_error = None
                return price
            else:
                logger.debug(f"   ❌ No 'usd' price found")
                self.last_error = f"No USD price for {token_id} on {date}"
                self.error_count += 1
                return None
            
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 'unknown'
            
            # ✅ NEW: Detailed error parsing
            try:
                error_data = e.response.json() if e.response else {}
                error_msg = error_data.get('error', str(e))
                
                logger.debug(f"   ❌ HTTP {status_code}: {error_msg}")
                
                # Special handling for common errors
                if status_code == 404:
                    logger.debug(f"   💡 Token '{token_id}' not found in CoinGecko")
                    self.last_error = f"Token not found: {token_id}"
                elif status_code == 429:
                    logger.debug(f"   ⏱️  Rate limit exceeded")
                    self.last_error = "Rate limit exceeded"
                else:
                    self.last_error = f"HTTP {status_code}: {error_msg}"
                    
            except:
                logger.debug(f"   ❌ HTTP {status_code}: {str(e)}")
                self.last_error = f"HTTP {status_code}"
            
            self.error_count += 1
            return None
            
        except requests.exceptions.Timeout:
            logger.debug(f"   ⏱️  Request timeout")
            self.last_error = "CoinGecko API timeout"
            self.error_count += 1
            return None
            
        except requests.exceptions.ConnectionError as e:
            logger.debug(f"   🌐 Connection error: {str(e)}")
            self.last_error = f"Connection error: {str(e)}"
            self.error_count += 1
            return None
            
        except Exception as e:
            logger.debug(f"   ❌ Unexpected error: {str(e)}")
            logger.debug(f"   📋 Exception type: {type(e).__name__}")
            self.last_error = f"{type(e).__name__}: {str(e)}"
            self.error_count += 1
            return None

    def _fetch_price_by_contract(
        self,
        token_address: str,
        timestamp: datetime
    ) -> Optional[float]:
        """
        ✅ NEW: Fetch price using CoinGecko Contract API.
        
        This is the fallback when token is not in our ID maps.
        Uses: /coins/ethereum/contract/{address}/market_chart/range
        
        Docs: https://docs.coingecko.com/v3.0.1/reference/coins-contract-address-market-chart-range
        
        Args:
            token_address: Token contract address
            timestamp: Historical timestamp
            
        Returns:
            USD price or None
        """
        self._rate_limit()
        
        # Calculate time range (24h window around timestamp)
        from_timestamp = int((timestamp - timedelta(days=1)).timestamp())
        to_timestamp = int((timestamp + timedelta(days=1)).timestamp())
        
        url = f"{self.coingecko_base}/coins/ethereum/contract/{token_address.lower()}/market_chart/range"
        params = {
            'vs_currency': 'usd',
            'from': from_timestamp,
            'to': to_timestamp
        }
        
        try:
            logger.debug(f"   🔍 Contract API: {token_address[:10]}...")
            
            response = self.session.get(url, params=params, timeout=15)
            
            logger.debug(f"   📡 HTTP {response.status_code}")
            
            if response.status_code == 404:
                logger.debug(f"   ❌ Contract not found in CoinGecko")
                self.last_error = f"Contract {token_address} not found"
                self.error_count += 1
                return None
            
            if response.status_code == 429:
                logger.warning(f"   ⏱️  Rate limited")
                self.last_error = "Rate limit exceeded"
                self.error_count += 1
                return None
            
            response.raise_for_status()
            data = response.json()
            
            # Response format: {'prices': [[timestamp_ms, price], ...]}
            prices = data.get('prices', [])
            
            if not prices:
                logger.debug(f"   ❌ No price data in range")
                self.last_error = f"No price data for contract {token_address}"
                self.error_count += 1
                return None
            
            # Find closest price to target timestamp
            target_ts = int(timestamp.timestamp() * 1000)  # CoinGecko uses ms
            
            closest_price = None
            min_diff = float('inf')
            
            for price_point in prices:
                ts_ms, price = price_point
                diff = abs(ts_ms - target_ts)
                
                if diff < min_diff:
                    min_diff = diff
                    closest_price = price
            
            if closest_price:
                logger.debug(f"   ✅ Contract API price: ${closest_price:,.4f}")
                self.success_count += 1
                self.last_error = None
                return closest_price
            else:
                logger.debug(f"   ❌ Could not find closest price")
                self.last_error = "No matching price point"
                self.error_count += 1
                return None
                
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else 'unknown'
            logger.debug(f"   ❌ HTTP {status}")
            self.last_error = f"Contract API HTTP {status}"
            self.error_count += 1
            return None
            
        except Exception as e:
            logger.debug(f"   ❌ Error: {str(e)}")
            self.last_error = f"Contract API: {str(e)}"
            self.error_count += 1
            return None
    
    # ========================================================================
    # FALLBACK METHODS
    # ========================================================================
    
    def _get_fallback_price(self, token_id: str, year: Optional[int] = None) -> Optional[float]:
        """Get fallback price when API fails."""
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
                logger.debug(f"Using fallback ETH price for {year}: ${price:,.2f}")
                return price
            return 3400.0
        
        elif token_id in ['tether', 'usd-coin']:
            return 1.0
        
        elif token_id == 'wrapped-bitcoin':
            if year and year in eth_fallback_prices:
                return eth_fallback_prices[year] * 15
            return 60000.0
        
        elif token_id == 'chainlink':
            return 15.0
        
        elif token_id == 'uniswap':
            return 10.0
        
        elif token_id == 'matic-network':
            return 1.0
        
        return None
    
    # ========================================================================
    # ✅ NEW: STATISTICS METHODS
    # ========================================================================
    
    def get_stats(self) -> Dict:
        """Get API call statistics."""
        total_calls = self.success_count + self.error_count
        success_rate = (self.success_count / total_calls * 100) if total_calls > 0 else 0
        
        return {
            'total_calls': total_calls,
            'successful': self.success_count,
            'failed': self.error_count,
            'success_rate': success_rate,
            'last_error': self.last_error
        }
    
    def reset_stats(self):
        """Reset statistics counters."""
        self.success_count = 0
        self.error_count = 0
        self.last_error = None
    
    # ========================================================================
    # BATCH METHODS
    # ========================================================================
    
    def batch_get_current_prices(self, token_addresses: list) -> Dict[str, float]:
        """Get current prices for multiple tokens in one call."""
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
            
            result = {}
            for addr in token_addresses:
                token_id = self._get_token_id(addr)
                price = data.get(token_id, {}).get('usd')
                
                if price and self._validate_price(token_id, price):
                    result[addr] = price
                else:
                    result[addr] = None
            
            return result
        except Exception as e:
            logger.error(f"Error batch fetching prices: {e}")
            self.last_error = f"Batch fetch: {str(e)}"
            return {}
