# services/multichain/api_providers.py
import asyncio
import aiohttp
import logging
import time
import json
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
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
        if not await self.rate_limiter.acquire(self.name, 10, 60):  # 10 Anfragen pro Minute
            raise RateLimitExceededException(self.name, 10, "minute")
        
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
                    raise RateLimitExceededException(self.name, 10, "minute")
                
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"Network error for {self.name}: {e}")
            raise APIException(f"Network error: {str(e)}")
    
    def check_availability(self) -> bool:
        """Prüft, ob der Anbieter verfügbar ist"""
        return self.is_available
class CoinGeckoProvider(BaseAPIProvider):
    """CoinGecko API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        # Für Demo-API-Schlüssel öffentliche API verwenden
        if api_key and api_key.startswith('CG-'):
            super().__init__("CoinGecko", "https://api.coingecko.com/api/v3", api_key, "COINGECKO_API_KEY")
        else:
            super().__init__("CoinGecko", "https://api.coingecko.com/api/v3", None, "COINGECKO_API_KEY")
    
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
                    source=self.name
                )
        except Exception as e:
            logger.error(f"Error fetching from CoinGecko: {e}")
        
        return None
    
    def _get_platform_id(self, chain: str) -> str:
        mapping = {
            'ethereum': 'ethereum',
            'bsc': 'binance-smart-chain',
            'solana': 'solana'
        }
        return mapping.get(chain, 'ethereum')
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 10, "requests_per_hour": 600}
class CoinMarketCapProvider(BaseAPIProvider):
    """CoinMarketCap API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("CoinMarketCap", "https://pro-api.coinmarketcap.com/v1", api_key, "COINMARKETCAP_API_KEY")
        self.min_request_interval = 1.2  # Etwas länger für CoinMarketCap
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # CoinMarketCap benötigt zuerst eine Mapping von Contract-Adresse zu Coin-ID
            coin_id = await self._get_coin_id(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': '1',
                'limit': '1',
                'convert': 'USD'
            }
            
            if coin_id:
                params['id'] = coin_id
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data.get('data'):
                coin_data = data['data'][0]
                quote = coin_data.get('quote', {}).get('USD', {})
                
                return TokenPriceData(
                    price=quote.get('price', 0),
                    market_cap=quote.get('market_cap', 0),
                    volume_24h=quote.get('volume_24h', 0),
                    price_change_percentage_24h=quote.get('percent_change_24h'),
                    source=self.name
                )
        except Exception as e:
            logger.error(f"Error fetching from CoinMarketCap: {e}")
        
        return None
    
    async def _get_coin_id(self, token_address: str, chain: str) -> Optional[str]:
        """Holt die Coin-ID für eine Contract-Adresse"""
        try:
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
class CryptoCompareProvider(BaseAPIProvider):
    """CryptoCompare API-Anbieter"""
    
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
                source=self.name
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
                source=self.name
            )
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 100, "requests_per_hour": 10000}
class BinanceProvider(BaseAPIProvider):
    """Binance API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Binance", "https://api.binance.com/api/v3", api_key, "BINANCE_API_KEY")
    
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
    
    async def _get_ethereum_price(self, token_address: str) -> Optional[TokenPriceData]:
        # Für Ethereum-Tokens auf Binance
        url = f"{self.base_url}/ticker/price"
        params = {
            'symbol': f'{token_address}USDT'
        }
        
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
        url = f"{self.base_url}/ticker/price"
        params = {
            'symbol': f'{token_address}USDT'
        }
        
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
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 1200, "requests_per_day": 100000}
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
