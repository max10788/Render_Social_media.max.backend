"""
CoinMarketCap API provider implementation - angepasst für kostenlose Version.
"""

import json
import os
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class CoinMarketCapProvider(BaseAPIProvider):
    """CoinMarketCap API-Anbieter - angepasst für kostenlose Sandbox-Version"""
    
    def __init__(self):
        # Lese API-Schlüssel aus der Umgebungsvariable
        api_key = os.getenv("COINMARKETCAP_API_KEY")
        
        # ANPASSUNG FÜR KOSTENLOSE VERSION: Sandbox-URL
        if api_key:
            # Für kostenlose Version mit API-Schlüssel: Sandbox-API
            base_url = "https://sandbox-api.coinmarketcap.com/v1"
            logger.info("CoinMarketCapProvider: Using Sandbox API URL with API key")
        else:
            # Für kostenlose Version ohne API-Schlüssel: Public API (sehr eingeschränkt)
            base_url = "https://api.coinmarketcap.com/v1"
            logger.info("CoinMarketCapProvider: Using Public API URL without API key")
        
        # Initialisiere die Basisklasse
        super().__init__("CoinMarketCap", base_url, api_key, "COINMARKETCAP_API_KEY")
        
        # ANPASSUNG FÜR KOSTENLOSE VERSION: Strengere Rate-Limits
        if api_key:
            # Kostenlose Sandbox mit API-Schlüssel: 333 Anfragen/Tag, ~10/Minute
            self.min_request_interval = 6.0  # 6 Sekunden zwischen Anfragen (10 pro Minute)
        else:
            # Öffentliche API ohne Schlüssel: sehr eingeschränkt
            self.min_request_interval = 12.0  # 12 Sekunden zwischen Anfragen (5 pro Minute)
    
    def _sanitize_float_values(self, data):
        """
        Bereinigt ungültige Float-Werte (NaN, Infinity) in einem Dictionary oder einer Liste.
        Ersetzt sie durch 0.0, um JSON-Kompatibilität sicherzustellen.
        """
        if isinstance(data, dict):
            return {k: self._sanitize_float_values(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_float_values(v) for v in data]
        elif isinstance(data, float):
            if math.isnan(data) or math.isinf(data):
                return 0.0
            return data
        return data
    
    async def _make_request(self, url: str, params: Dict = None, headers: Dict = None) -> Dict:
        """
        Führt eine API-Anfrage durch und loggt die direkte Antwort.
        
        Args:
            url: Die URL für die API-Anfrage
            params: Die Parameter für die Anfrage
            headers: Die Header für die Anfrage
            
        Returns:
            Die JSON-Antwort als Dictionary
        """
        try:
            # Füge den API-Schlüssel zu den Headern hinzu, falls nicht bereits vorhanden
            if headers is None:
                headers = {}
            
            if self.api_key and 'X-CMC_PRO_API_KEY' not in headers:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            # Führe die eigentliche Anfrage durch
            response = await super()._make_request(url, params, headers)
            
            # Bereinige ungültige Float-Werte in der Antwort
            response = self._sanitize_float_values(response)
            
            # Logge die direkte API-Antwort
            logger.info(f"CoinMarketCap API Response - URL: {url}")
            logger.info(f"Parameters: {params}")
            logger.info(f"Headers: {headers}")
            logger.info(f"Response: {json.dumps(response, indent=2)}")
            
            return response
        except Exception as e:
            # Logge den Fehler bei der Anfrage
            logger.error(f"CoinMarketCap API Request Failed - URL: {url}")
            logger.error(f"Parameters: {params}")
            logger.error(f"Headers: {headers}")
            logger.error(f"Error: {str(e)}")
            
            # Spezielle Behandlung für Rate-Limit-Fehler
            if "429" in str(e) or "rate limit" in str(e).lower():
                logger.error("Rate limit exceeded. For the free version, consider reducing request frequency.")
            
            raise
    
    async def get_token_price_by_address(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """
        Holt den aktuellen Preis eines Tokens über die Adresse.
        Zuerst wird die Coin-ID über die Map-API gesucht, dann die Preisdaten abgerufen.
        """
        try:
            # Zuerst die Coin-ID von der Adresse holen (über die Map-API)
            coin_id = await self._get_coin_id_from_address(token_address, chain)
            if not coin_id:
                logger.warning(f"Could not find coin ID for address {token_address}")
                return None
            
            # Dann die Preisdaten über die Listings-API abrufen
            return await self._get_price_by_id(coin_id)
            
        except Exception as e:
            logger.error(f"Error fetching token price by address from CoinMarketCap: {e}")
        
        return None
    
    async def _get_price_by_id(self, coin_id: str) -> Optional[TokenPriceData]:
        """Holt Preisdaten für eine Coin-ID"""
        try:
            # ANPASSUNG FÜR KOSTENLOSE VERSION: Wir können nur alle Listings abrufen und filtern
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': '1',
                'limit': '100',  # Reduziert für kostenlose Version
                'convert': 'USD'
            }
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('data'):
                # Suche nach dem Token in der Liste
                for token in data['data']:
                    if str(token['id']) == str(coin_id):
                        quote = token.get('quote', {}).get('USD', {})
                        
                        return TokenPriceData(
                            price=float(quote.get('price', 0)),
                            market_cap=float(quote.get('market_cap', 0)),
                            volume_24h=float(quote.get('volume_24h', 0)),
                            price_change_percentage_24h=float(quote.get('percent_change_24h', 0)),
                            name=token.get('name'),
                            symbol=token.get('symbol'),
                            source=self.name,
                            last_updated=datetime.now()
                        )
            
            logger.warning(f"Token with ID {coin_id} not found in listings")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching price by ID: {e}")
            return None
    
    async def _get_coin_id_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """
        Korrigierte Version: Kein platform-Parameter für /map-Endpunkt.
        Stattdessen clientseitige Filterung nach Adresse und Chain.
        """
        try:
            # Mapping von Chain-Name zu CoinMarketCap-Platform-ID
            platform_mapping = {
                'ethereum': '1027',
                'bsc': '1839',
                'polygon': '3890',
                'avalanche': '5805',
                'solana': '5426'
            }
            expected_platform_id = platform_mapping.get(chain.lower())
            
            url = f"{self.base_url}/cryptocurrency/map"
            params = {
                'listing_status': 'active',
                'start': '1',
                'limit': '100'  # Reduziert für kostenlose Version
            }
            
            # ENTFERNT: Der platform-Parameter ist für diesen Endpunkt nicht erlaubt!
            # if platform_id and self.api_key:
            #     params['platform'] = platform_id
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                # Clientseitige Filterung nach Adresse und Chain
                for token in data['data']:
                    platform_info = token.get('platform')
                    if platform_info:
                        token_addr = platform_info.get('token_address', '').lower()
                        platform_id = str(platform_info.get('id', ''))
                        
                        # Prüfe, ob Adresse und Chain (via platform_id) passen
                        if (token_addr == token_address.lower() and 
                            platform_id == expected_platform_id):
                            logger.info(f"Found token {token.get('name')} with ID {token.get('id')} for address {token_address} on {chain}")
                            return str(token.get('id'))
            
            # Wenn wir in der ersten Suche nichts gefunden haben, versuche es mit mehr Tokens
            # Aber nur, wenn wir einen API-Schlüssel haben (Rate-Limit!)
            if self.api_key:
                logger.info(f"First search didn't find token, trying with more tokens")
                params['limit'] = '500'
                params['start'] = '101'  # Zweite Seite
                
                data = await self._make_request(url, params, headers)
                
                if data and data.get('data'):
                    for token in data['data']:
                        platform_info = token.get('platform')
                        if platform_info:
                            token_addr = platform_info.get('token_address', '').lower()
                            platform_id = str(platform_info.get('id', ''))
                            
                            if (token_addr == token_address.lower() and 
                                platform_id == expected_platform_id):
                                logger.info(f"Found token {token.get('name')} with ID {token.get('id')} on second search")
                                return str(token.get('id'))
            
            logger.warning(f"Could not find coin ID for address {token_address} on {chain}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting coin ID from address in CoinMarketCap: {e}")
            return None
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Holt den aktuellen Preis eines Tokens - verwendet die Adressmethode"""
        return await self.get_token_price_by_address(token_address, chain)
    
    async def get_token_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """
        ANPASSUNG FÜR KOSTENLOSE VERSION: 
        Die Info-API ist in der Sandbox-Version eingeschränkt.
        """
        try:
            # Zuerst die Coin-ID von der Adresse holen
            coin_id = await self._get_coin_id_from_address(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/cryptocurrency/info"
            params = {
                'id': coin_id
            }
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                token_data = data['data'].get(str(coin_id), {})
                
                return {
                    'name': token_data.get('name'),
                    'symbol': token_data.get('symbol'),
                    'description': token_data.get('description'),
                    'website': token_data.get('urls', {}).get('website', [None])[0],
                    'social_links': {
                        'twitter': token_data.get('urls', {}).get('twitter'),
                        'telegram': token_data.get('urls', {}).get('telegram'),
                        'github': token_data.get('urls', {}).get('source_code', [None])[0]
                    },
                    'logo': token_data.get('logo'),
                    'category': token_data.get('category'),
                    'tags': token_data.get('tags')
                }
            
            logger.warning(f"No metadata found for coin ID {coin_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching token metadata from CoinMarketCap: {e}")
            return None
    
    async def get_historical_prices(self, token_address: str, chain: str, days: int = 30) -> Optional[Dict[str, float]]:
        """
        ANPASSUNG FÜR KOSTENLOSE VERSION: 
        Die historische API ist in der Sandbox-Version nicht verfügbar.
        Gib eine leere Antwort zurück.
        """
        logger.warning("Historical price data is not available in the free version of CoinMarketCap")
        return None
    
    async def get_global_market_data(self) -> Optional[Dict[str, Any]]:
        """Holt globale Krypto-Marktdaten"""
        try:
            url = f"{self.base_url}/global-metrics/quotes/latest"
            params = {
                'convert': 'USD'
            }
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                quote = data['data'].get('quote', {}).get('USD', {})
                
                return {
                    'total_market_cap': float(quote.get('total_market_cap', 0)),
                    'total_volume_24h': float(quote.get('total_volume_24h', 0)),
                    'btc_dominance': float(quote.get('bitcoin_dominance', 0)),
                    'active_cryptocurrencies': data['data'].get('active_cryptocurrencies'),
                    'active_market_pairs': data['data'].get('active_market_pairs'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching global market data from CoinMarketCap: {e}")
        
        return None
    
    async def get_trending_tokens(self) -> Optional[List[Dict[str, Any]]]:
        """Holt Trending Tokens basierend auf CoinMarketCap"""
        try:
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': '1',
                'limit': '20',  # Reduziert für kostenlose Version
                'convert': 'USD',
                'sort': 'percent_change_24h',
                'sort_dir': 'desc'
            }
            
            headers = {}
            if self.api_key:
                headers['X-CMC_PRO_API_KEY'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                trending_tokens = []
                for token in data['data']:
                    trending_tokens.append({
                        'id': token.get('id'),
                        'name': token.get('name'),
                        'symbol': token.get('symbol'),
                        'price': float(token.get('quote', {}).get('USD', {}).get('price', 0)),
                        'percent_change_24h': float(token.get('quote', {}).get('USD', {}).get('percent_change_24h', 0))
                    })
                
                return trending_tokens
        except Exception as e:
            logger.error(f"Error fetching trending tokens from CoinMarketCap: {e}")
        
        return None
    
    async def get_token_holders(self, token_address: str, chain: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        CoinMarketCap bietet keine direkte API für Token-Halter.
        Diese Methode gibt eine leere Liste zurück.
        """
        logger.warning("CoinMarketCap does not provide token holder information")
        return []
    
    def get_rate_limits(self) -> Dict[str, int]:
        """
        ANPASSUNG FÜR KOSTENLOSE VERSION: 
        Strengere Rate-Limits für die kostenlose Version.
        """
        if self.api_key:
            # Kostenlose Sandbox mit API-Schlüssel: 333 Anfragen/Tag, ~10/Minute
            return {
                "requests_per_minute": 10, 
                "requests_per_hour": 60,
                "requests_per_day": 333
            }
        else:
            # Öffentliche API ohne Schlüssel: sehr eingeschränkt
            return {
                "requests_per_minute": 5,
                "requests_per_hour": 30,
                "requests_per_day": 100
            }
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Client-Sessions."""
        if hasattr(self, 'session') and self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
            logger.info("CoinMarketCapProvider client session closed successfully")
