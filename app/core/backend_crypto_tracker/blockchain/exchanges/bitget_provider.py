"""
Bitget API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


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
