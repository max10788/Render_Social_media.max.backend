"""
CoinGecko API provider implementation.
"""

import json
import os
import re
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class CoinGeckoProvider(BaseAPIProvider):
    """CoinGecko API-Anbieter - umfangreichste kostenlose API"""
    
    def __init__(self):
        # Lese API-Schlüssel ausschließlich aus der Umgebungsvariable
        api_key_from_env = os.getenv("COINGECKO_API_KEY")
        
        # Bestimme die richtige Basis-URL basierend auf dem API-Schlüssel
        if api_key_from_env:
            # Prüfe, ob es ein Demo/Free API-Schlüssel ist
            if self._is_demo_api_key(api_key_from_env):
                base_url = "https://api.coingecko.com/api/v3"
                logger.info("CoinGeckoProvider: Using Demo/Free API URL (api.coingecko.com) with API key from COINGECKO_API_KEY")
            else:
                base_url = "https://pro-api.coingecko.com/api/v3"
                logger.info("CoinGeckoProvider: Using Pro API URL (pro-api.coingecko.com) with API key from COINGECKO_API_KEY")
        else:
            # Kein API-Schlüssel - Standard-URL verwenden
            base_url = "https://api.coingecko.com/api/v3"
            logger.info("CoinGeckoProvider: Using Standard API URL (api.coingecko.com) without API key")
        
        # Initialisiere die Basisklasse mit der Umgebungsvariable
        super().__init__("CoinGecko", base_url, None, "COINGECKO_API_KEY")
        
        # Passe das Min Request Interval basierend auf dem API-Schlüssel an
        if self.api_key and not self._is_demo_api_key(self.api_key):
            self.min_request_interval = 0.12  # Pro API: ca. 500 Anfragen/Minute
        else:
            self.min_request_interval = 6.5   # Free API: ca. 9 Anfragen/Minute (etwas unter dem Limit)
    
    def _is_demo_api_key(self, api_key: str) -> bool:
        """
        Prüft, ob es sich um einen Demo/Free API-Schlüssel handelt.
        
        CoinGecko Demo-API-Schlüssel beginnen normalerweise mit "CG-" 
        und haben eine bestimmte Länge/Struktur.
        """
        if not api_key:
            return True
        
        # Demo-API-Schlüssel beginnen mit "CG-" und sind kürzer als Pro-Schlüssel
        if api_key.startswith("CG-") and len(api_key) <= 34:
            return True
        
        # Wenn der Schlüssel "demo" in Kleinbuchstaben enthält
        if "demo" in api_key.lower():
            return True
        
        return False
    
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
            # Führe die eigentliche Anfrage durch
            response = await super()._make_request(url, params, headers)
            
            # Bereinige ungültige Float-Werte in der Antwort
            response = self._sanitize_float_values(response)
            
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
            
            # Spezielle Behandlung für Rate-Limit-Fehler
            if "429" in str(e) or "rate limit" in str(e).lower():
                logger.error("Rate limit exceeded. Consider upgrading to a Pro plan or reducing request frequency.")
            
            # Spezielle Behandlung für API-URL-Fehler
            error_msg = str(e)
            if "pro-api.coingecko.com" in error_msg and "demo" in error_msg.lower():
                logger.error("CoinGecko API URL mismatch detected. You might be using a demo API key with the pro-api URL.")
                logger.error("Please check your API key type or remove the API key to use the free tier.")
            elif "api.coingecko.com" in error_msg and "pro-api" in error_msg.lower():
                logger.error("CoinGecko API URL mismatch detected. You might be using a pro API key with the standard API URL.")
                logger.error("Please check your API key type or upgrade to a pro plan.")
            
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
                if self._is_demo_api_key(self.api_key):
                    # Für kostenlose API-Schlüssel: x-cg-api-key
                    headers['x-cg-api-key'] = self.api_key
                    logger.debug(f"Using CoinGecko Free API key for request to {url}")
                else:
                    # Für Pro-API-Schlüssel: x-cg-pro-api-key
                    headers['x-cg-pro-api-key'] = self.api_key
                    logger.debug(f"Using CoinGecko Pro API key for request to {url}")
            else:
                logger.debug(f"Making CoinGecko request without API key to {url}")
            
            data = await self._make_request(url, params, headers)
            
            # Überprüfen, ob Daten für den Token vorhanden sind
            if not data or token_address.lower() not in data:
                logger.warning(f"No data found for token {token_address} on {chain}")
                return None
            
            token_data = data.get(token_address.lower(), {})
            if token_data:
                # Bereinige alle Float-Werte vor der Erstellung des TokenPriceData-Objekts
                token_data = self._sanitize_float_values(token_data)
                
                return TokenPriceData(
                    price=float(token_data.get('usd', 0)),
                    market_cap=float(token_data.get('usd_market_cap', 0)),
                    volume_24h=float(token_data.get('usd_24h_vol', 0)),
                    price_change_percentage_24h=float(token_data.get('usd_24h_change', 0)),
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
                if self._is_demo_api_key(self.api_key):
                    headers['x-cg-api-key'] = self.api_key
                else:
                    headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data:
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
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
                if self._is_demo_api_key(self.api_key):
                    headers['x-cg-api-key'] = self.api_key
                else:
                    headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, params, headers)
            
            if data and data.get('prices'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                # Konvertiere Zeitstempel in lesbare Daten
                historical_prices = {}
                for timestamp, price in data['prices']:
                    date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                    historical_prices[date] = float(price)
                
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
                if self._is_demo_api_key(self.api_key):
                    headers['x-cg-api-key'] = self.api_key
                else:
                    headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, {}, headers)
            
            if data and data.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
                return {
                    'total_market_cap_usd': float(data['data'].get('total_market_cap', {}).get('usd', 0)),
                    'total_volume_24h_usd': float(data['data'].get('total_volume', {}).get('usd', 0)),
                    'btc_dominance': float(data['data'].get('market_cap_percentage', {}).get('btc', 0)),
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
                if self._is_demo_api_key(self.api_key):
                    headers['x-cg-api-key'] = self.api_key
                else:
                    headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, {}, headers)
            
            if data and data.get('coins'):
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
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
    
    async def get_token_holders(self, token_address: str, chain: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Holt die Top-Token-Halter für einen bestimmten Token.
        Nutzt CoinGecko Pro On-Chain API, falls verfügbar, sonst GeckoTerminal.
        
        Args:
            token_address: Die Token-Vertragsadresse
            chain: Die Blockchain (z.B. 'ethereum', 'bsc')
            limit: Maximale Anzahl an Haltern, die abgerufen werden sollen
            
        Returns:
            Eine Liste von Dictionaries mit Halter-Informationen
        """
        try:
            # Wenn wir einen CoinGecko Pro-API-Schlüssel haben, nutzen wir den On-Chain Endpunkt
            if self.api_key and not self._is_demo_api_key(self.api_key):
                return await self._get_holders_from_coingecko(token_address, chain, limit)
            else:
                # Ansonsten nutzen wir GeckoTerminal
                return await self._get_holders_from_geckoterminal(token_address, chain, limit)
                
        except Exception as e:
            logger.error(f"Error fetching token holders for {token_address} on {chain}: {e}")
            return []
    
    async def _get_holders_from_coingecko(self, token_address: str, chain: str, limit: int) -> List[Dict[str, Any]]:
        """Nutzt CoinGecko Pro On-Chain API für Token-Halter"""
        try:
            # Bestimme die Netzwerk-ID für CoinGecko
            network_mapping = {
                'ethereum': 'ethereum',
                'bsc': 'binance-smart-chain',
                'polygon': 'polygon-pos',
                'avalanche': 'avalanche',
                'arbitrum': 'arbitrum-one'
            }
            network_id = network_mapping.get(chain, chain)
            
            url = f"https://pro-api.coingecko.com/api/v3/onchain/addresses/{network_id}/token_holders_rankings_by_token"
            params = {
                'token_addresses': token_address,
                'limit': limit
            }
            headers = {
                'x-cg-pro-api-key': self.api_key
            }
            
            response = await self._make_request(url, params, headers)
            
            if response and response.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                response = self._sanitize_float_values(response)
                
                holders = []
                for holder_data in response['data']:
                    holders.append({
                        'address': holder_data.get('address'),
                        'amount': float(holder_data.get('token_balance', 0)),
                        'percentage': float(holder_data.get('percentage', 0)),
                        'rank': holder_data.get('rank')
                    })
                return holders
                
            return []
            
        except Exception as e:
            logger.error(f"Error fetching holders from CoinGecko: {e}")
            return []
    
    async def _get_holders_from_geckoterminal(self, token_address: str, chain: str, limit: int) -> List[Dict[str, Any]]:
        """Nutzt GeckoTerminal API für Token-Halter"""
        try:
            # Bestimme die Netzwerk-ID für GeckoTerminal
            network_mapping = {
                'ethereum': 'eth',
                'bsc': 'bsc',
                'polygon': 'polygon',
                'avalanche': 'avax',
                'arbitrum': 'arbitrum'
            }
            network_id = network_mapping.get(chain, chain)
            
            url = f"https://api.geckoterminal.com/api/v2/networks/{network_id}/tokens/{token_address}"
            params = {}
            
            response = await self._make_request(url, params)
            
            if response and response.get('data'):
                # Bereinige alle Float-Werte in der Antwort
                response = self._sanitize_float_values(response)
                
                token_data = response['data']
                if token_data.get('top_holders'):
                    holders = []
                    for holder_data in token_data['top_holders'][:limit]:
                        holders.append({
                            'address': holder_data.get('address'),
                            'amount': float(holder_data.get('amount', 0)),
                            'percentage': float(holder_data.get('percentage', 0))
                        })
                    return holders
                
            return []
            
        except Exception as e:
            logger.error(f"Error fetching holders from GeckoTerminal: {e}")
            return []
    
    async def _get_coin_id_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Holt die Coin-ID von einer Contract-Adresse"""
        try:
            platform_id = self._get_platform_id(chain)
            url = f"{self.base_url}/coins/{platform_id}/contract/{token_address}"
            
            headers = {}
            if self.api_key:
                if self._is_demo_api_key(self.api_key):
                    headers['x-cg-api-key'] = self.api_key
                else:
                    headers['x-cg-pro-api-key'] = self.api_key
            
            data = await self._make_request(url, {}, headers)
            
            if data:
                # Bereinige alle Float-Werte in der Antwort
                data = self._sanitize_float_values(data)
                
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
        # Unterschiedliche Rate-Limits für Free vs Pro API
        if self.api_key and not self._is_demo_api_key(self.api_key):
            # Pro API hat höhere Limits
            return {"requests_per_minute": 500, "requests_per_hour": 3000}
        else:
            # Free/Demo API hat deutlich niedrigere Limits
            return {"requests_per_minute": 10, "requests_per_hour": 100}
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Client-Sessions."""
        if hasattr(self, 'session') and self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
            logger.info("CoinGeckoProvider client session closed successfully")
