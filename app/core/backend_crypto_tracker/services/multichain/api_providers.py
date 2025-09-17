# services/multichain/api_providers.py
import asyncio
import aiohttp
import logging
import time
import json
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, RateLimitExceededException
logger = get_logger(__name__)

@dataclass
class TokenPriceData:
    price: float
    market_cap: float
    volume_24h: float
    price_change_percentage_24h: Optional[float] = None
    source: str = ""  # Welche API hat die Daten geliefert
    # Erweiterte Felder für bessere Nutzung der APIs
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    last_updated: Optional[datetime] = None
    # Historische Daten
    historical_prices: Optional[Dict[str, float]] = None  # Zeitraum -> Preis
    # Token-Metadaten
    token_name: Optional[str] = None
    token_symbol: Optional[str] = None
    description: Optional[str] = None
    website: Optional[str] = None
    social_links: Optional[Dict[str, str]] = None
    # On-chain Daten
    liquidity: Optional[float] = None
    unique_traders_24h: Optional[int] = None
    # Orderbuch-Daten
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    bid_volume: Optional[float] = None
    ask_volume: Optional[float] = None

class BaseAPIProvider(ABC):
    """Basisklasse für alle API-Anbieter"""
    
    def __init__(self, name: str, base_url: str, api_key: Optional[str] = None, api_key_env: Optional[str] = None):
        self.name = name
        self.base_url = base_url
        # Wenn kein API-Schlüssel übergeben wurde, versuchen, ihn aus der Umgebungsvariable zu lesen
        if api_key is None and api_key_env is not None:
            self.api_key = os.getenv(api_key_env)
        else:
            self.api_key = api_key
        self.session = None
        self.rate_limiter = RateLimiter()
        self.last_request_time = 0
        self.min_request_interval = 1.0  # Sekunden zwischen Anfragen
        self.is_available = True
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @abstractmethod
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Abstrakte Methode zum Abrufen von Token-Preisen"""
        pass
    
    @abstractmethod
    def get_rate_limits(self) -> Dict[str, int]:
        """Gibt die Rate-Limits zurück (requests_per_minute, requests_per_hour, etc.)"""
        pass
    
    async def _make_request(self, url: str, params: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        """Interne Methode für HTTP-Anfragen mit Rate-Limiting"""
        # Rate-Limiting prüfen
        rate_limits = self.get_rate_limits()
        if not await self.rate_limiter.acquire(self.name, rate_limits.get("requests_per_minute", 10), 60):
            raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
        
        # Mindestabstand zwischen Anfragen
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 429:
                    error_text = await response.text()
                    logger.warning(f"Rate limit exceeded for {self.name}: {error_text}")
                    raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
                
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"Network error for {self.name}: {e}")
            raise APIException(f"Network error: {str(e)}")
    
    async def _make_post_request(self, url: str, json_data: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        """Interne Methode für POST-Anfragen mit Rate-Limiting"""
        # Rate-Limiting prüfen
        rate_limits = self.get_rate_limits()
        if not await self.rate_limiter.acquire(self.name, rate_limits.get("requests_per_minute", 10), 60):
            raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
        
        # Mindestabstand zwischen Anfragen
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        
        try:
            async with self.session.post(url, json=json_data, headers=headers) as response:
                if response.status == 429:
                    error_text = await response.text()
                    logger.warning(f"Rate limit exceeded for {self.name}: {error_text}")
                    raise RateLimitExceededException(self.name, rate_limits.get("requests_per_minute", 10), "minute")
                
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"Network error for {self.name}: {e}")
            raise APIException(f"Network error: {str(e)}")
    
    def check_availability(self) -> bool:
        """Prüft, ob der Anbieter verfügbar ist"""
        return self.is_available

# ====== COINGECKO (FREE TIER) - Umfangreichste kostenlose API ======
class CoinGeckoProvider(BaseAPIProvider):
    """CoinGecko API-Anbieter - umfangreichste kostenlose API"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("CoinGecko", "https://api.coingecko.com/api/v3", api_key, "COINGECKO_API_KEY")
        self.min_request_interval = 0.5  # Höheres Rate-Limiting
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            platform_id = self._get_platform_id(chain)
            url = f"{self.base_url}/simple/token_price/{platform_id}"
            
            params = {
                'contract_addresses': token_address,
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true'
            }
            
            headers = {}
            if self.api_key:
                headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            token_data = data.get(token_address.lower(), {})
            if token_data:
                return TokenPriceData(
                    price=token_data.get('usd', 0),
                    market_cap=token_data.get('usd_market_cap', 0),
                    volume_24h=token_data.get('usd_24h_vol', 0),
                    price_change_percentage_24h=token_data.get('usd_24h_change'),
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from CoinGecko: {e}")
        
        return None
    
    async def get_token_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Token-Metadaten wie Beschreibung, Website, Social Links"""
        try:
            # Zuerst Coin-ID von der Adresse holen
            coin_id = await self._get_coin_id_from_address(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/coins/{coin_id}"
            params = {
                'localization': 'false',
                'tickers': 'false',
                'market_data': 'false',
                'community_data': 'false',
                'developer_data': 'false'
            }
            
            headers = {}
            if self.api_key:
                headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data:
                return {
                    'name': data.get('name'),
                    'symbol': data.get('symbol'),
                    'description': data.get('description', {}).get('en'),
                    'website': data.get('links', {}).get('homepage', [None])[0],
                    'social_links': {
                        'twitter': data.get('links', {}).get('twitter_screen_name'),
                        'telegram': data.get('links', {}).get('telegram_channel_identifier'),
                        'github': data.get('links', {}).get('repos_url', {}).get('github', [None])[0]
                    }
                }
        except Exception as e:
            logger.error(f"Error fetching token metadata from CoinGecko: {e}")
        
        return None
    
    async def get_historical_prices(self, token_address: str, chain: str, days: int = 30) -> Optional[Dict[str, float]]:
        """Holt historische Preisdaten für einen bestimmten Zeitraum"""
        try:
            # Zuerst Coin-ID von der Adresse holen
            coin_id = await self._get_coin_id_from_address(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/coins/{coin_id}/market_chart"
            params = {
                'vs_currency': 'usd',
                'days': days
            }
            
            headers = {}
            if self.api_key:
                headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('prices'):
                # Konvertiere Zeitstempel in lesbare Daten
                historical_prices = {}
                for timestamp, price in data['prices']:
                    date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                    historical_prices[date] = price
                
                return historical_prices
        except Exception as e:
            logger.error(f"Error fetching historical prices from CoinGecko: {e}")
        
        return None
    
    async def get_global_market_data(self) -> Optional[Dict[str, Any]]:
        """Holt globale Krypto-Marktdaten"""
        try:
            url = f"{self.base_url}/global"
            
            headers = {}
            if self.api_key:
                headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, {}, headers)
            
            if data and data.get('data'):
                return {
                    'total_market_cap_usd': data['data'].get('total_market_cap', {}).get('usd'),
                    'total_volume_24h_usd': data['data'].get('total_volume', {}).get('usd'),
                    'btc_dominance': data['data'].get('market_cap_percentage', {}).get('btc'),
                    'active_cryptocurrencies': data['data'].get('active_cryptocurrencies'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching global market data from CoinGecko: {e}")
        
        return None
    
    async def get_trending_tokens(self) -> Optional[List[Dict[str, Any]]]:
        """Holt Trending Tokens basierend auf Suchanfragen"""
        try:
            url = f"{self.base_url}/search/trending"
            
            headers = {}
            if self.api_key:
                headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, {}, headers)
            
            if data and data.get('coins'):
                trending_tokens = []
                for coin in data['coins']:
                    item = coin.get('item')
                    trending_tokens.append({
                        'id': item.get('id'),
                        'name': item.get('name'),
                        'symbol': item.get('symbol'),
                        'market_cap_rank': item.get('market_cap_rank')
                    })
                
                return trending_tokens
        except Exception as e:
            logger.error(f"Error fetching trending tokens from CoinGecko: {e}")
        
        return None
    
    async def _get_coin_id_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Holt die Coin-ID von einer Contract-Adresse"""
        try:
            platform_id = self._get_platform_id(chain)
            url = f"{self.base_url}/coins/{platform_id}/contract/{token_address}"
            
            headers = {}
            if self.api_key:
                headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, {}, headers)
            
            if data:
                return data.get('id')
        except Exception as e:
            logger.error(f"Error getting coin ID from address in CoinGecko: {e}")
        
        return None
    
    def _get_platform_id(self, chain: str) -> str:
        mapping = {
            'ethereum': 'ethereum',
            'bsc': 'binance-smart-chain',
            'solana': 'solana'
        }
        return mapping.get(chain, 'ethereum')
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 30, "requests_per_hour": 1800}

# ====== BINANCE (PUBLIC API) - Höchstes Rate-Limit ======
class BinanceProvider(BaseAPIProvider):
    """Binance API-Anbieter - höchstes Rate-Limit"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Binance", "https://api.binance.com/api/v3", api_key, "BINANCE_API_KEY")
        self.min_request_interval = 0.05  # Sehr niedrig für hohe Rate-Limits
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            if chain == 'ethereum':
                return await self._get_ethereum_price(token_address)
            elif chain == 'bsc':
                return await self._get_bsc_price(token_address)
            else:
                return None
        except Exception as e:
            logger.error(f"Error fetching from Binance: {e}")
        
        return None
    
    async def get_24h_statistics(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Holt vollständige 24h-Statistiken"""
        try:
            if chain == 'ethereum':
                symbol = self._get_symbol_from_address(token_address, 'ethereum')
            elif chain == 'bsc':
                symbol = self._get_symbol_from_address(token_address, 'bsc')
            else:
                return None
            
            if not symbol:
                return None
            
            url = f"{self.base_url}/ticker/24hr"
            params = {'symbol': symbol}
            
            data = await self._make_request(url, params)
            
            if data:
                return TokenPriceData(
                    price=float(data.get('lastPrice', 0)),
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=float(data.get('volume', 0)),
                    price_change_percentage_24h=float(data.get('priceChangePercent', 0)),
                    high_24h=float(data.get('highPrice', 0)),
                    low_24h=float(data.get('lowPrice', 0)),
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching 24h statistics from Binance: {e}")
        
        return None
    
    async def get_order_book(self, token_address: str, chain: str, limit: int = 100) -> Optional[Dict[str, Any]]:
        """Holt Orderbuchdaten"""
        try:
            if chain == 'ethereum':
                symbol = self._get_symbol_from_address(token_address, 'ethereum')
            elif chain == 'bsc':
                symbol = self._get_symbol_from_address(token_address, 'bsc')
            else:
                return None
            
            if not symbol:
                return None
            
            url = f"{self.base_url}/depth"
            params = {
                'symbol': symbol,
                'limit': limit
            }
            
            data = await self._make_request(url, params)
            
            if data:
                return {
                    'bids': data.get('bids', []),
                    'asks': data.get('asks', []),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching order book from Binance: {e}")
        
        return None
    
    async def get_historical_candles(self, token_address: str, chain: str, interval: str = '1h', limit: int = 100) -> Optional[List[Dict[str, Any]]]:
        """Holt historische Candlestick-Daten"""
        try:
            if chain == 'ethereum':
                symbol = self._get_symbol_from_address(token_address, 'ethereum')
            elif chain == 'bsc':
                symbol = self._get_symbol_from_address(token_address, 'bsc')
            else:
                return None
            
            if not symbol:
                return None
            
            url = f"{self.base_url}/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            data = await self._make_request(url, params)
            
            if data:
                candles = []
                for candle in data:
                    candles.append({
                        'open_time': datetime.fromtimestamp(candle[0] / 1000),
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5]),
                        'close_time': datetime.fromtimestamp(candle[6] / 1000)
                    })
                
                return candles
        except Exception as e:
            logger.error(f"Error fetching historical candles from Binance: {e}")
        
        return None
    
    async def _get_ethereum_price(self, token_address: str) -> Optional[TokenPriceData]:
        # Für Ethereum-Tokens auf Binance
        symbol = self._get_symbol_from_address(token_address, 'ethereum')
        if not symbol:
            return None
            
        url = f"{self.base_url}/ticker/price"
        params = {'symbol': symbol}
        
        data = await self._make_request(url, params)
        
        if data:
            return TokenPriceData(
                price=float(data.get('price', 0)),
                market_cap=0,  # Nicht verfügbar
                volume_24h=0,  # Nicht verfügbar
                price_change_percentage_24h=0,  # Nicht verfügbar
                source=self.name
            )
        
        return None
    
    async def _get_bsc_price(self, token_address: str) -> Optional[TokenPriceData]:
        # Für BSC-Tokens auf Binance
        symbol = self._get_symbol_from_address(token_address, 'bsc')
        if not symbol:
            return None
            
        url = f"{self.base_url}/ticker/price"
        params = {'symbol': symbol}
        
        data = await self._make_request(url, params)
        
        if data:
            return TokenPriceData(
                price=float(data.get('price', 0)),
                market_cap=0,  # Nicht verfügbar
                volume_24h=0,  # Nicht verfügbar
                price_change_percentage_24h=0,  # Nicht verfügbar
                source=self.name
            )
        
        return None
    
    def _get_symbol_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, das Trading-Symbol aus der Contract-Adresse abzuleiten"""
        known_tokens = {
            'ethereum': {
                '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': 'ETHUSDT',  # WETH
                '0x6B175474E89094C44Da98b954EedeAC495271d0F': 'DAIUSDT'   # DAI
            },
            'bsc': {
                '0x55d398326f99059fF775485246999027B3197955': 'USDTUSDT',  # USDT
                '0x2170Ed0880ac9A755fd29B2688956BD959F933F8': 'ETHUSDT'    # WETH
            }
        }
        
        if chain in known_tokens and token_address in known_tokens[chain]:
            return known_tokens[chain][token_address]
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 1200, "requests_per_day": 100000}

# ====== CRYPTOCOMPARE - Gute historische Daten ======
class CryptoCompareProvider(BaseAPIProvider):
    """CryptoCompare API-Anbieter - gute historische Daten"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("CryptoCompare", "https://min-api.cryptocompare.com/data", api_key, "CRYPTOCOMPARE_API_KEY")
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # CryptoCompare benötigt verschiedene Endpunkte für verschiedene Chains
            if chain == 'ethereum':
                return await self._get_ethereum_price(token_address)
            elif chain == 'bsc':
                return await self._get_bsc_price(token_address)
            else:
                return None
        except Exception as e:
            logger.error(f"Error fetching from CryptoCompare: {e}")
        
        return None
    
    async def get_historical_data(self, token_address: str, chain: str, days: int = 30, interval: str = 'day') -> Optional[List[Dict[str, Any]]]:
        """Holt historische OHLCV-Daten"""
        try:
            symbol = self._get_symbol_from_address(token_address, chain)
            if not symbol:
                return None
            
            # Bestimme den Endpunkt basierend auf dem Intervall
            if interval == 'day':
                endpoint = 'histoday'
            elif interval == 'hour':
                endpoint = 'histohour'
            elif interval == 'minute':
                endpoint = 'histominute'
            else:
                endpoint = 'histoday'  # Standard
            
            url = f"{self.base_url}/{endpoint}"
            params = {
                'fsym': symbol,
                'tsym': 'USD',
                'limit': days
            }
            
            headers = {}
            if self.api_key:
                headers['authorization'] = f'Apikey {self.api_key}'
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('Data'):
                historical_data = []
                for item in data['Data']:
                    historical_data.append({
                        'time': datetime.fromtimestamp(item['time']),
                        'open': item.get('open'),
                        'high': item.get('high'),
                        'low': item.get('low'),
                        'close': item.get('close'),
                        'volumefrom': item.get('volumefrom'),
                        'volumeto': item.get('volumeto')
                    })
                
                return historical_data
        except Exception as e:
            logger.error(f"Error fetching historical data from CryptoCompare: {e}")
        
        return None
    
    async def get_social_stats(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt soziale Metriken für einen Token"""
        try:
            symbol = self._get_symbol_from_address(token_address, chain)
            if not symbol:
                return None
            
            url = f"{self.base_url}/socialstats"
            params = {
                'coin_id': symbol
            }
            
            headers = {}
            if self.api_key:
                headers['authorization'] = f'Apikey {self.api_key}'
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('Data'):
                return {
                    'twitter': {
                        'followers': data['Data'].get('Twitter', {}).get('followers'),
                        'statuses': data['Data'].get('Twitter', {}).get('statuses')
                    },
                    'reddit': {
                        'subscribers': data['Data'].get('Reddit', {}).get('subscribers'),
                        'active_users': data['Data'].get('Reddit', {}).get('active_users')
                    },
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching social stats from CryptoCompare: {e}")
        
        return None
    
    async def _get_ethereum_price(self, token_address: str) -> Optional[TokenPriceData]:
        url = f"{self.base_url}/pricemulti"
        params = {
            'fsyms': token_address,
            'tsyms': 'USD'
        }
        
        headers = {}
        if self.api_key:
            headers['authorization'] = f'Apikey {self.api_key}'
        
        data = await self._make_request(url, params, headers)
        
        if data.get('RAW') and token_address in data['RAW']:
            price_data = data['RAW'][token_address]['USD']
            return TokenPriceData(
                price=price_data.get('PRICE', 0),
                market_cap=price_data.get('MKTCAP', 0),
                volume_24h=price_data.get('VOLUME24HOURTO', 0),
                price_change_percentage_24h=price_data.get('CHANGEPCT24HOUR'),
                high_24h=price_data.get('HIGH24HOUR'),
                low_24h=price_data.get('LOW24HOUR'),
                source=self.name,
                last_updated=datetime.now()
            )
        
        return None
    
    async def _get_bsc_price(self, token_address: str) -> Optional[TokenPriceData]:
        # Ähnlich wie Ethereum, aber für BSC
        url = f"{self.base_url}/pricemulti"
        params = {
            'fsyms': token_address,
            'tsyms': 'USD',
            'e': 'Binance'  # Binance Smart Chain
        }
        
        headers = {}
        if self.api_key:
            headers['authorization'] = f'Apikey {self.api_key}'
        
        data = await self._make_request(url, params, headers)
        
        if data.get('RAW') and token_address in data['RAW']:
            price_data = data['RAW'][token_address]['USD']
            return TokenPriceData(
                price=price_data.get('PRICE', 0),
                market_cap=price_data.get('MKTCAP', 0),
                volume_24h=price_data.get('VOLUME24HOURTO', 0),
                price_change_percentage_24h=price_data.get('CHANGEPCT24HOUR'),
                high_24h=price_data.get('HIGH24HOUR'),
                low_24h=price_data.get('LOW24HOUR'),
                source=self.name,
                last_updated=datetime.now()
            )
        
        return None
    
    def _get_symbol_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, das Symbol aus der Contract-Adresse abzuleiten"""
        known_tokens = {
            'ethereum': {
                '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': 'ETH',  # WETH
                '0x6B175474E89094C44Da98b954EedeAC495271d0F': 'DAI'   # DAI
            },
            'bsc': {
                '0x55d398326f99059fF775485246999027B3197955': 'USDT',  # USDT
                '0x2170Ed0880ac9A755fd29B2688956BD959F933F8': 'ETH'    # WETH
            }
        }
        
        if chain in known_tokens and token_address in known_tokens[chain]:
            return known_tokens[chain][token_address]
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 100, "requests_per_hour": 10000}

# ====== KRAKEN (PUBLIC API) - Gute Orderbuchdaten ======
class KrakenProvider(BaseAPIProvider):
    """Kraken API-Anbieter - gute Orderbuchdaten"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Kraken", "https://api.kraken.com/0/public", api_key, "KRAKEN_API_KEY")
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Kraken verwendet Paar-Namen statt Contract-Adressen
            pair = self._get_pair_from_address(token_address, chain)
            if not pair:
                return None
            
            url = f"{self.base_url}/Ticker"
            params = {'pair': pair}
            
            data = await self._make_request(url, params)
            
            if data.get('error') == [] and data.get('result'):
                result = data['result']
                # Das Ergebnis enthält den Pair-Namen als Schlüssel
                pair_key = list(result.keys())[0]
                ticker = result[pair_key]
                
                # Berechne die prozentuale Veränderung aus den Werten
                open_price = float(ticker.get('o', 0))
                close_price = float(ticker.get('c', [0, 0])[0])
                price_change_percentage = ((close_price - open_price) / open_price * 100) if open_price > 0 else 0
                
                return TokenPriceData(
                    price=close_price,
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=float(ticker.get('v', [0, 0])[1]),
                    price_change_percentage_24h=price_change_percentage,
                    high_24h=float(ticker.get('h', [0, 0])[1]),
                    low_24h=float(ticker.get('l', [0, 0])[1]),
                    bid_price=float(ticker.get('b', [0, 0])[0]),
                    ask_price=float(ticker.get('a', [0, 0])[0]),
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from Kraken: {e}")
        
        return None
    
    async def get_order_book(self, token_address: str, chain: str, count: int = 100) -> Optional[Dict[str, Any]]:
        """Holt Orderbuchdaten mit Tiefe"""
        try:
            pair = self._get_pair_from_address(token_address, chain)
            if not pair:
                return None
            
            url = f"{self.base_url}/Depth"
            params = {
                'pair': pair,
                'count': count
            }
            
            data = await self._make_request(url, params)
            
            if data.get('error') == [] and data.get('result'):
                result = data['result']
                # Das Ergebnis enthält den Pair-Namen als Schlüssel
                pair_key = list(result.keys())[0]
                order_book = result[pair_key]
                
                return {
                    'bids': order_book.get('bids', []),
                    'asks': order_book.get('asks', []),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching order book from Kraken: {e}")
        
        return None
    
    async def get_historical_ohlc(self, token_address: str, chain: str, interval: int = 1440, since: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """Holt historische OHLC-Daten"""
        try:
            pair = self._get_pair_from_address(token_address, chain)
            if not pair:
                return None
            
            url = f"{self.base_url}/OHLC"
            params = {
                'pair': pair,
                'interval': interval  # in Minuten (1, 5, 15, 30, 60, 240, 1440, 10080, 21600)
            }
            
            if since:
                params['since'] = since
            
            data = await self._make_request(url, params)
            
            if data.get('error') == [] and data.get('result'):
                result = data['result']
                # Das Ergebnis enthält den Pair-Namen als Schlüssel
                pair_key = list(result.keys())[0]
                ohlc_data = result[pair_key]
                
                ohlc_list = []
                for item in ohlc_data:
                    ohlc_list.append({
                        'time': datetime.fromtimestamp(item[0]),
                        'open': float(item[1]),
                        'high': float(item[2]),
                        'low': float(item[3]),
                        'close': float(item[4]),
                        'vwap': float(item[5]),
                        'volume': float(item[6]),
                        'count': int(item[7])
                    })
                
                return ohlc_list
        except Exception as e:
            logger.error(f"Error fetching historical OHLC from Kraken: {e}")
        
        return None
    
    def _get_pair_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, das Handelspaar aus der Contract-Adresse abzuleiten"""
        known_tokens = {
            'ethereum': {
                '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': 'ETHUSD',  # WETH
                '0x6B175474E89094C44Da98b954EedeAC495271d0F': 'DAIUSD'   # DAI
            },
            'bsc': {
                '0x55d398326f99059fF775485246999027B3197955': 'USDTUSD',  # USDT
                '0x2170Ed0880ac9A755fd29B2688956BD959F933F8': 'ETHUSD'    # WETH
            }
        }
        
        if chain in known_tokens and token_address in known_tokens[chain]:
            return known_tokens[chain][token_address]
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 15, "requests_per_hour": 900}

# ====== BITQUERY (GRAPHQL API) - Beste on-chain Daten ======
class BitqueryProvider(BaseAPIProvider):
    """Bitquery API-Anbieter (GraphQL) - beste on-chain Daten"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Bitquery", "https://graphql.bitquery.io", api_key, "BITQUERY_API_KEY")
        self.min_request_interval = 0.5  # Höheres Rate-Limiting für GraphQL
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Bitquery verwendet GraphQL-Abfragen
            query = self._build_price_query(token_address, chain)
            
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            
            if data.get('data') and data['data'].get('ethereum'):
                token_data = data['data']['ethereum']['dexTrades'][0]
                return TokenPriceData(
                    price=float(token_data.get('quotePrice', 0)),
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=0,  # Nicht verfügbar
                    price_change_percentage_24h=0,  # Nicht verfügbar
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from Bitquery: {e}")
        
        return None
    
    async def get_dex_trades(self, token_address: str, chain: str, hours: int = 24) -> Optional[List[Dict[str, Any]]]:
        """Holt DEX-Trades für einen Token"""
        try:
            query = self._build_dex_trades_query(token_address, chain, hours)
            
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            
            if data.get('data') and data['data'].get('ethereum'):
                trades = data['data']['ethereum']['dexTrades']
                dex_trades = []
                
                for trade in trades:
                    dex_trades.append({
                        'transaction_hash': trade.get('transaction', {}).get('hash'),
                        'timestamp': datetime.fromtimestamp(int(trade.get('block', {}).get('timestamp', {}).get('unixtime'))),
                        'buyer': trade.get('buyAddress'),
                        'seller': trade.get('sellAddress'),
                        'price': float(trade.get('quotePrice')),
                        'amount': float(trade.get('tradeAmount')),
                        'amount_usd': float(trade.get('tradeAmount', {}).get('inUSD')),
                        'pool_address': trade.get('exchange', {}).get('address')
                    })
                
                return dex_trades
        except Exception as e:
            logger.error(f"Error fetching DEX trades from Bitquery: {e}")
        
        return None
    
    async def get_token_liquidity(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Liquiditätsdaten für einen Token"""
        try:
            query = self._build_liquidity_query(token_address, chain)
            
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            
            if data.get('data') and data['data'].get('ethereum'):
                pools = data['data']['ethereum']['dexTrades']
                
                # Aggregiere Liquiditätsdaten
                total_liquidity_usd = 0
                unique_traders = set()
                pool_data = {}
                
                for pool in pools:
                    pool_address = pool.get('exchange', {}).get('address')
                    if pool_address not in pool_data:
                        pool_data[pool_address] = {
                            'liquidity_usd': 0,
                            'trader_count': 0
                        }
                    
                    pool_data[pool_address]['liquidity_usd'] += float(pool.get('tradeAmount', {}).get('inUSD', 0))
                    unique_traders.add(pool.get('buyAddress'))
                    unique_traders.add(pool.get('sellAddress'))
                
                for pool_address in pool_data:
                    pool_data[pool_address]['trader_count'] = len(unique_traders)
                    total_liquidity_usd += pool_data[pool_address]['liquidity_usd']
                
                return {
                    'total_liquidity_usd': total_liquidity_usd,
                    'unique_traders_24h': len(unique_traders),
                    'pool_data': pool_data,
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching token liquidity from Bitquery: {e}")
        
        return None
    
    async def get_wallet_activity(self, wallet_address: str, chain: str, hours: int = 24) -> Optional[List[Dict[str, Any]]]:
        """Holt Wallet-Aktivitäten für eine bestimmte Adresse"""
        try:
            query = self._build_wallet_activity_query(wallet_address, chain, hours)
            
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            
            if data.get('data') and data['data'].get('ethereum'):
                transfers = data['data']['ethereum']['transfers']
                wallet_activity = []
                
                for transfer in transfers:
                    wallet_activity.append({
                        'transaction_hash': transfer.get('transaction', {}).get('hash'),
                        'timestamp': datetime.fromtimestamp(int(transfer.get('block', {}).get('timestamp', {}).get('unixtime'))),
                        'sender': transfer.get('sender', {}).get('address'),
                        'receiver': transfer.get('receiver', {}).get('address'),
                        'amount': float(transfer.get('amount')),
                        'amount_usd': float(transfer.get('amount', {}).get('inUSD', 0)),
                        'token_symbol': transfer.get('currency', {}).get('symbol')
                    })
                
                return wallet_activity
        except Exception as e:
            logger.error(f"Error fetching wallet activity from Bitquery: {e}")
        
        return None
    
    def _build_price_query(self, token_address: str, chain: str) -> str:
        """Erstellt eine GraphQL-Abfrage für den Token-Preis"""
        # Für dieses Beispiel verwenden wir eine einfache Ethereum-DEX-Abfrage
        return """
        {
          ethereum {
            dexTrades(
              options: {limit: 1, desc: "block.timestamp.timeInterval"}
              baseCurrency: {is: "%s"}
              quoteCurrency: {is: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"}
            ) {
              tradeAmount(in: USD)
              quotePrice
              block {
                timestamp {
                  timeInterval(minute: 24)
                }
              }
            }
          }
        }
        """ % token_address
    
    def _build_dex_trades_query(self, token_address: str, chain: str, hours: int) -> str:
        """Erstellt eine GraphQL-Abfrage für DEX-Trades"""
        return """
        {
          ethereum {
            dexTrades(
              options: {desc: "block.timestamp.unixtime", limit: 100}
              baseCurrency: {is: "%s"}
              time: {since: "%s"}
            ) {
              transaction {
                hash
              }
              block {
                timestamp {
                  unixtime
                }
              }
              buyAddress
              sellAddress
              quotePrice
              tradeAmount
              tradeAmount(in: USD)
              exchange {
                address
              }
            }
          }
        }
        """ % (token_address, f"utc_now-{hours}h")
    
    def _build_liquidity_query(self, token_address: str, chain: str) -> str:
        """Erstellt eine GraphQL-Abfrage für Liquiditätsdaten"""
        return """
        {
          ethereum {
            dexTrades(
              options: {desc: "block.timestamp.unixtime", limit: 1000}
              baseCurrency: {is: "%s"}
              time: {since: "utc_now-24h"}
            ) {
              tradeAmount(in: USD)
              buyAddress
              sellAddress
              exchange {
                address
              }
            }
          }
        }
        """ % token_address
    
    def _build_wallet_activity_query(self, wallet_address: str, chain: str, hours: int) -> str:
        """Erstellt eine GraphQL-Abfrage für Wallet-Aktivitäten"""
        return """
        {
          ethereum {
            transfers(
              options: {desc: "block.timestamp.unixtime", limit: 100}
              amount: {gt: 0}
              time: {since: "%s"}
            ) {
              transaction {
                hash
              }
              block {
                timestamp {
                  unixtime
                }
              }
              sender {
                address
              }
              receiver {
                address
              }
              amount
              amount(in: USD)
              currency {
                symbol
              }
            }
          }
        }
        """ % f"utc_now-{hours}h"
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 60, "requests_per_hour": 3600}

# ====== COINMARKETCAP (FREE PLAN) - Eingeschränkt ======
class CoinMarketCapProvider(BaseAPIProvider):
    """CoinMarketCap API-Anbieter - eingeschränkte kostenlose Version"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("CoinMarketCap", "https://pro-api.coinmarketcap.com/v1", api_key, "COINMARKETCAP_API_KEY")
        self.min_request_interval = 1.2  # Etwas länger für CoinMarketCap
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # CoinMarketCap benötigt zuerst eine Mapping von Contract-Adresse zu Coin-ID
            coin_id = await self._get_coin_id(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/cryptocurrency/quotes/latest"
            params = {
                'id': coin_id,
                'convert': 'USD'
            }
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data.get('data'):
                coin_data = data['data'][coin_id]
                quote = coin_data.get('quote', {}).get('USD', {})
                
                return TokenPriceData(
                    price=quote.get('price', 0),
                    market_cap=quote.get('market_cap', 0),
                    volume_24h=quote.get('volume_24h', 0),
                    price_change_percentage_24h=quote.get('percent_change_24h'),
                    circulating_supply=coin_data.get('circulating_supply'),
                    total_supply=coin_data.get('total_supply'),
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from CoinMarketCap: {e}")
        
        return None
    
    async def get_token_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Token-Metadaten"""
        try:
            coin_id = await self._get_coin_id(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/cryptocurrency/info"
            params = {'id': coin_id}
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data.get('data'):
                coin_data = data['data'][coin_id]
                return {
                    'name': coin_data.get('name'),
                    'symbol': coin_data.get('symbol'),
                    'description': coin_data.get('description'),
                    'website': coin_data.get('urls', {}).get('website', [None])[0],
                    'social_links': {
                        'twitter': coin_data.get('urls', {}).get('twitter'),
                        'telegram': coin_data.get('urls', {}).get('telegram'),
                        'github': coin_data.get('urls', {}).get('github', [None])[0]
                    },
                    'logo': coin_data.get('logo'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching token metadata from CoinMarketCap: {e}")
        
        return None
    
    async def get_global_metrics(self) -> Optional[Dict[str, Any]]:
        """Holt globale Marktmetriken"""
        try:
            url = f"{self.base_url}/global-metrics/quotes/latest"
            params = {'convert': 'USD'}
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data.get('data'):
                quote = data['data'].get('quote', {}).get('USD', {})
                return {
                    'total_market_cap_usd': quote.get('total_market_cap'),
                    'total_volume_24h_usd': quote.get('total_volume_24h'),
                    'btc_dominance': data['data'].get('btc_dominance'),
                    'active_cryptocurrencies': data['data'].get('active_cryptocurrencies'),
                    'active_market_pairs': data['data'].get('active_market_pairs'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching global metrics from CoinMarketCap: {e}")
        
        return None
    
    async def _get_coin_id(self, token_address: str, chain: str) -> Optional[str]:
        """Holt die Coin-ID für eine Contract-Adresse"""
        try:
            # Im Free Tier ist dieser Endpunkt möglicherweise nicht verfügbar
            url = f"{self.base_url}/cryptocurrency/map"
            params = {
                'address': token_address,
                'symbol': ''
            }
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data.get('data'):
                return data['data'][0].get('id')
        except Exception as e:
            logger.error(f"Error getting coin ID from CoinMarketCap: {e}")
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 333, "requests_per_day": 10000}

# ====== BITGET (PUBLIC API) - Mittelgroße Börse ======
class BitgetProvider(BaseAPIProvider):
    """Bitget API-Anbieter - mittelgroße Börse"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Bitget", "https://api.bitget.com/api/spot/v1", api_key, "BITGET_API_KEY")
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Bitget verwendet Symbolnamen statt Contract-Adressen
            symbol = self._get_symbol_from_address(token_address, chain)
            if not symbol:
                return None
            
            url = f"{self.base_url}/ticker"
            params = {'symbol': symbol}
            
            data = await self._make_request(url, params)
            
            if data.get('data') and len(data['data']) > 0:
                ticker = data['data'][0]
                return TokenPriceData(
                    price=float(ticker.get('lastPr', 0)),
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=float(ticker.get('baseVol', 0)),
                    price_change_percentage_24h=float(ticker.get('change24h', 0)),
                    high_24h=float(ticker.get('high24h', 0)),
                    low_24h=float(ticker.get('low24h', 0)),
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from Bitget: {e}")
        
        return None
    
    async def get_historical_candles(self, token_address: str, chain: str, interval: str = '1H', limit: int = 100) -> Optional[List[Dict[str, Any]]]:
        """Holt historische Candlestick-Daten"""
        try:
            symbol = self._get_symbol_from_address(token_address, chain)
            if not symbol:
                return None
            
            url = f"{self.base_url}/candles"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            data = await self._make_request(url, params)
            
            if data.get('data'):
                candles = []
                for candle in data['data']:
                    candles.append({
                        'timestamp': datetime.fromtimestamp(int(candle[0]) / 1000),
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5])
                    })
                
                return candles
        except Exception as e:
            logger.error(f"Error fetching historical candles from Bitget: {e}")
        
        return None
    
    def _get_symbol_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, das Trading-Symbol aus der Contract-Adresse abzuleiten"""
        known_tokens = {
            'ethereum': {
                '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': 'ETHUSDT',  # WETH
                '0x6B175474E89094C44Da98b954EedeAC495271d0F': 'DAIUSDT'   # DAI
            },
            'bsc': {
                '0x55d398326f99059fF775485246999027B3197955': 'USDTUSDT',  # USDT
                '0x2170Ed0880ac9A755fd29B2688956BD959F933F8': 'ETHUSDT'    # WETH
            }
        }
        
        if chain in known_tokens and token_address in known_tokens[chain]:
            return known_tokens[chain][token_address]
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 100, "requests_per_hour": 10000}

# ====== COINBASE (PUBLIC API) - Einfache Preise ======
class CoinbaseProvider(BaseAPIProvider):
    """Coinbase API-Anbieter - einfache Preise"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Coinbase", "https://api.coinbase.com/v2", api_key, "COINBASE_API_KEY")
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Coinbase verwendet Produkt-IDs statt Contract-Adressen
            product_id = self._get_product_id_from_address(token_address, chain)
            if not product_id:
                return None
            
            url = f"{self.base_url}/prices/{product_id}/spot"
            
            data = await self._make_request(url, {})
            
            if data.get('data'):
                price_data = data['data']
                return TokenPriceData(
                    price=float(price_data.get('amount', 0)),
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=0,  # Nicht verfügbar
                    price_change_percentage_24h=0,  # Nicht verfügbar
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from Coinbase: {e}")
        
        return None
    
    async def get_buy_sell_prices(self, token_address: str, chain: str) -> Optional[Dict[str, float]]:
        """Holt Kauf- und Verkaufspreise"""
        try:
            product_id = self._get_product_id_from_address(token_address, chain)
            if not product_id:
                return None
            
            # Kaufpreis
            buy_url = f"{self.base_url}/prices/{product_id}/buy"
            buy_data = await self._make_request(buy_url, {})
            
            # Verkaufspreis
            sell_url = f"{self.base_url}/prices/{product_id}/sell"
            sell_data = await self._make_request(sell_url, {})
            
            if buy_data.get('data') and sell_data.get('data'):
                return {
                    'buy_price': float(buy_data['data'].get('amount', 0)),
                    'sell_price': float(sell_data['data'].get('amount', 0)),
                    'spread': float(sell_data['data'].get('amount', 0)) - float(buy_data['data'].get('amount', 0)),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching buy/sell prices from Coinbase: {e}")
        
        return None
    
    async def get_exchange_rates(self) -> Optional[Dict[str, float]]:
        """Holt Wechselkurse zwischen Fiat und Crypto"""
        try:
            url = f"{self.base_url}/exchange-rates"
            
            data = await self._make_request(url, {})
            
            if data.get('data'):
                return {
                    'currency': data['data'].get('currency'),
                    'rates': data['data'].get('rates'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching exchange rates from Coinbase: {e}")
        
        return None
    
    def _get_product_id_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, die Produkt-ID aus der Contract-Adresse abzuleiten"""
        known_tokens = {
            'ethereum': {
                '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2': 'ETH-USD',  # WETH
                '0x6B175474E89094C44Da98b954EedeAC495271d0F': 'DAI-USD'   # DAI
            },
            'bsc': {
                '0x55d398326f99059fF775485246999027B3197955': 'USDT-USD',  # USDT
                '0x2170Ed0880ac9A755fd29B2688956BD959F933F8': 'ETH-USD'    # WETH
            }
        }
        
        if chain in known_tokens and token_address in known_tokens[chain]:
            return known_tokens[chain][token_address]
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 10, "requests_per_hour": 600}

class RateLimiter:
    """Einfacher Rate-Limiter für API-Anfragen"""
    
    def __init__(self):
        self.request_timestamps = {}
        self.limits = {}
    
    async def acquire(self, service_name: str, max_requests: int, time_window: int) -> bool:
        """Prüft, ob eine Anfrage gemacht werden kann"""
        current_time = time.time()
        
        if service_name not in self.request_timestamps:
            self.request_timestamps[service_name] = []
        
        # Alte Zeitstempel entfernen
        window_start = current_time - time_window
        self.request_timestamps[service_name] = [
            ts for ts in self.request_timestamps[service_name] if ts > window_start
        ]
        
        # Prüfen, ob das Limit erreicht ist
        if len(self.request_timestamps[service_name]) >= max_requests:
            return False
        
        # Neue Anfrage hinzufügen
        self.request_timestamps[service_name].append(current_time)
        return True
