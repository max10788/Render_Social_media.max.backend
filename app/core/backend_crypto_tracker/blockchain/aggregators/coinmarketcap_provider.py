"""
CoinMarketCap API provider implementation.
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
    """CoinMarketCap API-Anbieter - eine der führenden Krypto-Datenquellen"""
    
    def __init__(self):
        # Lese API-Schlüssel aus der Umgebungsvariable
        api_key = os.getenv("COINMARKETCAP_API_KEY")
        
        # Initialisiere die Basisklasse
        super().__init__("CoinMarketCap", "https://pro-api.coinmarketcap.com/v1", api_key, "COINMARKETCAP_API_KEY")
        
        # Setze das Min Request Interval basierend auf dem Plan
        if api_key:
            self.min_request_interval = 0.1  # Pro API: ca. 333 Anfragen/Minute
        else:
            self.min_request_interval = 10.0  # Free API: deutlich weniger Anfragen
    
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
                logger.error("Rate limit exceeded. Consider upgrading to a higher plan or reducing request frequency.")
            
            raise
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Holt den aktuellen Preis eines Tokens"""
        try:
            # Zuerst die Coin-ID von der Adresse holen
            coin_id = await self._get_coin_id_from_address(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/cryptocurrency/listings/latest"
            params = {
                'start': '1',
                'limit': '5000',
                'convert': 'USD'
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                # Suche nach dem Token in der Liste
                for token in data['data']:
                    if str(token['id']) == str(coin_id):
                        quote = token.get('quote', {}).get('USD', {})
                        
                        return TokenPriceData(
                            price=float(quote.get('price', 0)),
                            market_cap=float(quote.get('market_cap', 0)),
                            volume_24h=float(quote.get('volume_24h', 0)),
                            price_change_percentage_24h=float(quote.get('percent_change_24h', 0)),
                            source=self.name,
                            last_updated=datetime.now()
                        )
        except Exception as e:
            logger.error(f"Error fetching token price from CoinMarketCap: {e}")
        
        return None
    
    async def get_token_metadata(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Token-Metadaten wie Beschreibung, Website, Social Links"""
        try:
            # Zuerst die Coin-ID von der Adresse holen
            coin_id = await self._get_coin_id_from_address(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/cryptocurrency/info"
            params = {
                'id': coin_id
            }
            
            data = await self._make_request(url, params)
            
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
        except Exception as e:
            logger.error(f"Error fetching token metadata from CoinMarketCap: {e}")
        
        return None
    
    async def get_historical_prices(self, token_address: str, chain: str, days: int = 30) -> Optional[Dict[str, float]]:
        """Holt historische Preisdaten für einen bestimmten Zeitraum"""
        try:
            # Zuerst die Coin-ID von der Adresse holen
            coin_id = await self._get_coin_id_from_address(token_address, chain)
            if not coin_id:
                return None
            
            url = f"{self.base_url}/cryptocurrency/quotes/historical"
            params = {
                'id': coin_id,
                'count': days,
                'convert': 'USD'
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                # Konvertiere Zeitstempel in lesbare Daten
                historical_prices = {}
                quotes = data['data'].get('quotes', [])
                
                for quote in quotes:
                    timestamp = quote.get('timestamp')
                    price = quote.get('quote', {}).get('USD', {}).get('price', 0)
                    
                    if timestamp:
                        date = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d')
                        historical_prices[date] = float(price)
                
                return historical_prices
        except Exception as e:
            logger.error(f"Error fetching historical prices from CoinMarketCap: {e}")
        
        return None
    
    async def get_global_market_data(self) -> Optional[Dict[str, Any]]:
        """Holt globale Krypto-Marktdaten"""
        try:
            url = f"{self.base_url}/global-metrics/quotes/latest"
            params = {
                'convert': 'USD'
            }
            
            data = await self._make_request(url, params)
            
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
                'limit': '20',
                'convert': 'USD',
                'sort': 'percent_change_24h',
                'sort_dir': 'desc'
            }
            
            data = await self._make_request(url, params)
            
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
        
        Args:
            token_address: Die Token-Vertragsadresse
            chain: Die Blockchain (z.B. 'ethereum', 'bsc')
            limit: Maximale Anzahl an Haltern, die abgerufen werden sollen
            
        Returns:
            Eine leere Liste
        """
        logger.warning("CoinMarketCap does not provide token holder information")
        return []
    
    async def _get_coin_id_from_address(self, token_address: str, chain: str) -> Optional[str]:
    """Holt den aktuellen Preis eines Tokens direkt über die Adresse"""
        try:
            # Bestimme die Plattform-ID für CoinMarketCap
            platform_mapping = {
                'ethereum': '1027',
                'bsc': '1839',
                'polygon': '3890'
            }
            platform_id = platform_mapping.get(chain.lower(), '1027')  # Default: Ethereum
            
            url = f"{self.base_url}/cryptocurrency/quotes/latest"
            params = {
                'address': token_address,
                'convert': 'USD'
            }
            
            # Füge Plattform-Parameter hinzu, falls angegeben
            if platform_id:
                params['platform'] = platform_id
            
            headers = {
                'X-CMC_PRO_API_KEY': self.api_key
            }
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('data'):
                # Die Antwort sollte nur ein Token enthalten
                for token_id, token_data in data['data'].items():
                    quote = token_data.get('quote', {}).get('USD', {})
                    
                    return TokenPriceData(
                        price=float(quote.get('price', 0)),
                        market_cap=float(quote.get('market_cap', 0)),
                        volume_24h=float(quote.get('volume_24h', 0)),
                        price_change_percentage_24h=float(quote.get('percent_change_24h', 0)),
                        name=token_data.get('name'),
                        symbol=token_data.get('symbol'),
                        source=self.name,
                        last_updated=datetime.now()
                    )
        except Exception as e:
            logger.error(f"Error fetching token price by address from CoinMarketCap: {e}")
        
        return None

    async def get_token_price_by_address(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Holt den aktuellen Preis eines Tokens direkt über die Adresse"""
        try:
            # Bestimme die Plattform-ID für CoinMarketCap
            platform_mapping = {
                'ethereum': '1027',
                'bsc': '1839',
                'polygon': '3890'
            }
            platform_id = platform_mapping.get(chain.lower(), '1027')  # Default: Ethereum
            
            url = f"{self.base_url}/cryptocurrency/quotes/latest"
            params = {
                'address': token_address,
                'convert': 'USD'
            }
            
            # Füge Plattform-Parameter hinzu, falls angegeben
            if platform_id:
                params['platform'] = platform_id
            
            headers = {
                'X-CMC_PRO_API_KEY': self.api_key
            }
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('data'):
                # Die Antwort sollte nur ein Token enthalten
                for token_id, token_data in data['data'].items():
                    quote = token_data.get('quote', {}).get('USD', {})
                    
                    return TokenPriceData(
                        price=float(quote.get('price', 0)),
                        market_cap=float(quote.get('market_cap', 0)),
                        volume_24h=float(quote.get('volume_24h', 0)),
                        price_change_percentage_24h=float(quote.get('percent_change_24h', 0)),
                        name=token_data.get('name'),
                        symbol=token_data.get('symbol'),
                        source=self.name,
                        last_updated=datetime.now()
                    )
        except Exception as e:
            logger.error(f"Error fetching token price by address from CoinMarketCap: {e}")
        
        return None
    
    def get_rate_limits(self) -> Dict[str, int]:
        # Unterschiedliche Rate-Limits für Free vs Pro API
        if self.api_key:
            # Pro API hat höhere Limits
            return {"requests_per_minute": 333, "requests_per_hour": 10000}
        else:
            # Free API hat deutlich niedrigere Limits
            return {"requests_per_minute": 10, "requests_per_hour": 100}
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Client-Sessions."""
        if hasattr(self, 'session') and self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
            logger.info("CoinMarketCapProvider client session closed successfully")
