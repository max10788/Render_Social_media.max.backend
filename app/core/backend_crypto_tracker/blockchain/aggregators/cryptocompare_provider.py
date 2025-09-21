"""
CryptoCompare API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


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
