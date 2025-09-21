"""
CoinGecko API provider implementation.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class CoinGeckoProvider(BaseAPIProvider):
    """CoinGecko API-Anbieter - umfangreichste kostenlose API"""
    
    def __init__(self, api_key: Optional[str] = None):
        # Korrigierte Initialisierung mit allen erforderlichen Parametern
        super().__init__("CoinGecko", "https://api.coingecko.com/api/v3", api_key, "COINGECKO_API_KEY")
        self.min_request_interval = 0.5  # Höheres Rate-Limiting
    
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
            logger.info(f"CoinGecko API Response - URL: {url}")
            logger.info(f"Parameters: {params}")
            logger.info(f"Headers: {headers}")
            logger.info(f"Response: {json.dumps(response, indent=2)}")
            
            return response
        except Exception as e:
            # Logge den Fehler bei der Anfrage
            logger.error(f"CoinGecko API Request Failed - URL: {url}")
            logger.error(f"Parameters: {params}")
            logger.error(f"Headers: {headers}")
            logger.error(f"Error: {str(e)}")
            raise
    
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
