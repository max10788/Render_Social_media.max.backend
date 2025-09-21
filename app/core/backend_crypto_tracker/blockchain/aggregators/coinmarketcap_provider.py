"""
CoinMarketCap API provider implementation.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class CoinMarketCapProvider(BaseAPIProvider):
    """CoinMarketCap API-Anbieter - eingeschränkte kostenlose Version"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("CoinMarketCap", "https://pro-api.coinmarketcap.com/v1", api_key, "COINMARKETCAP_API_KEY")
        self.min_request_interval = 1.2  # Etwas länger für CoinMarketCap
    
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
            # Führe die eigentliche Anfrage durch
            response = await super()._make_request(url, params, headers)
            
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
            raise
    
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
