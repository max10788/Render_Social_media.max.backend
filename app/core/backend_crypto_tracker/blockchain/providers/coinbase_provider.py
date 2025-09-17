"""
Coinbase API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from .base_provider import BaseAPIProvider
from ..data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


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
