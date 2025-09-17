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
    
    async def _make_post_request(self, url: str, json_data: Dict[str, Any], headers: Dict[str, str] = None) -> Dict[str, Any]:
        """Interne Methode für POST-Anfragen mit Rate-Limiting"""
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
            async with self.session.post(url, json=json_data, headers=headers) as response:
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
class BitgetProvider(BaseAPIProvider):
    """Bitget API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Bitget", "https://api.bitget.com/api/spot/v1", api_key, "BITGET_API_KEY")
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Bitget verwendet Symbolnamen statt Contract-Adressen
            # Wir versuchen, das Symbol aus der Adresse abzuleiten
            symbol = self._get_symbol_from_address(token_address, chain)
            if not symbol:
                return None
            
            url = f"{self.base_url}/ticker"
            params = {
                'symbol': symbol
            }
            
            data = await self._make_request(url, params)
            
            if data.get('data') and len(data['data']) > 0:
                ticker = data['data'][0]
                return TokenPriceData(
                    price=float(ticker.get('lastPr', 0)),
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=float(ticker.get('baseVol', 0)),
                    price_change_percentage_24h=float(ticker.get('change24h', 0)),
                    source=self.name
                )
        except Exception as e:
            logger.error(f"Error fetching from Bitget: {e}")
        
        return None
    
    def _get_symbol_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, das Trading-Symbol aus der Contract-Adresse abzuleiten"""
        # In einer echten Implementierung müsste hier eine Mapping-Logik oder Datenbankabfrage erfolgen
        # Für dieses Beispiel geben wir nur einige bekannte Symbole zurück
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
class CoinbaseProvider(BaseAPIProvider):
    """Coinbase API-Anbieter"""
    
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
                    source=self.name
                )
        except Exception as e:
            logger.error(f"Error fetching from Coinbase: {e}")
        
        return None
    
    def _get_product_id_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, die Produkt-ID aus der Contract-Adresse abzuleiten"""
        # In einer echten Implementierung müsste hier eine Mapping-Logik oder Datenbankabfrage erfolgen
        # Für dieses Beispiel geben wir nur einige bekannte Produkt-IDs zurück
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
class KrakenProvider(BaseAPIProvider):
    """Kraken API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Kraken", "https://api.kraken.com/0/public", api_key, "KRAKEN_API_KEY")
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Kraken verwendet Paar-Namen statt Contract-Adressen
            pair = self._get_pair_from_address(token_address, chain)
            if not pair:
                return None
            
            url = f"{self.base_url}/Ticker"
            params = {
                'pair': pair
            }
            
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
                    source=self.name
                )
        except Exception as e:
            logger.error(f"Error fetching from Kraken: {e}")
        
        return None
    
    def _get_pair_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, das Handelspaar aus der Contract-Adresse abzuleiten"""
        # In einer echten Implementierung müsste hier eine Mapping-Logik oder Datenbankabfrage erfolgen
        # Für dieses Beispiel geben wir nur einige bekannte Paare zurück
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
class BitqueryProvider(BaseAPIProvider):
    """Bitquery API-Anbieter (GraphQL)"""
    
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
                    source=self.name
                )
        except Exception as e:
            logger.error(f"Error fetching from Bitquery: {e}")
        
        return None
    
    def _build_price_query(self, token_address: str, chain: str) -> str:
        """Erstellt eine GraphQL-Abfrage für den Token-Preis"""
        # In einer echten Implementierung müsste die Abfrage an die jeweilige Blockchain angepasst werden
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
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 60, "requests_per_hour": 3600}
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
