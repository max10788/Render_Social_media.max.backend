"""
Binance API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from .base_provider import BaseAPIProvider
from ..data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class BinanceProvider(BaseAPIProvider):
    """Binance API-Anbieter - höchstes Rate-Limit"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Binance", "https://api.binance.com/api/v3", api_key, "BINANCE_API_KEY")
        self.min_request_interval = 0.05  # Sehr niedrig für hohe Rate-Limits
    
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
    
    async def get_24h_statistics(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Holt vollständige 24h-Statistiken"""
        try:
            if chain == 'ethereum':
                symbol = self._get_symbol_from_address(token_address, 'ethereum')
            elif chain == 'bsc':
                symbol = self._get_symbol_from_address(token_address, 'bsc')
            else:
                return None
            
            if not symbol:
                return None
            
            url = f"{self.base_url}/ticker/24hr"
            params = {'symbol': symbol}
            
            data = await self._make_request(url, params)
            
            if data:
                return TokenPriceData(
                    price=float(data.get('lastPrice', 0)),
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=float(data.get('volume', 0)),
                    price_change_percentage_24h=float(data.get('priceChangePercent', 0)),
                    high_24h=float(data.get('highPrice', 0)),
                    low_24h=float(data.get('lowPrice', 0)),
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching 24h statistics from Binance: {e}")
        
        return None
    
    async def get_order_book(self, token_address: str, chain: str, limit: int = 100) -> Optional[Dict[str, Any]]:
        """Holt Orderbuchdaten"""
        try:
            if chain == 'ethereum':
                symbol = self._get_symbol_from_address(token_address, 'ethereum')
            elif chain == 'bsc':
                symbol = self._get_symbol_from_address(token_address, 'bsc')
            else:
                return None
            
            if not symbol:
                return None
            
            url = f"{self.base_url}/depth"
            params = {
                'symbol': symbol,
                'limit': limit
            }
            
            data = await self._make_request(url, params)
            
            if data:
                return {
                    'bids': data.get('bids', []),
                    'asks': data.get('asks', []),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching order book from Binance: {e}")
        
        return None
    
    async def get_historical_candles(self, token_address: str, chain: str, interval: str = '1h', limit: int = 100) -> Optional[List[Dict[str, Any]]]:
        """Holt historische Candlestick-Daten"""
        try:
            if chain == 'ethereum':
                symbol = self._get_symbol_from_address(token_address, 'ethereum')
            elif chain == 'bsc':
                symbol = self._get_symbol_from_address(token_address, 'bsc')
            else:
                return None
            
            if not symbol:
                return None
            
            url = f"{self.base_url}/klines"
            params = {
                'symbol': symbol,
                'interval': interval,
                'limit': limit
            }
            
            data = await self._make_request(url, params)
            
            if data:
                candles = []
                for candle in data:
                    candles.append({
                        'open_time': datetime.fromtimestamp(candle[0] / 1000),
                        'open': float(candle[1]),
                        'high': float(candle[2]),
                        'low': float(candle[3]),
                        'close': float(candle[4]),
                        'volume': float(candle[5]),
                        'close_time': datetime.fromtimestamp(candle[6] / 1000)
                    })
                
                return candles
        except Exception as e:
            logger.error(f"Error fetching historical candles from Binance: {e}")
        
        return None
    
    async def _get_ethereum_price(self, token_address: str) -> Optional[TokenPriceData]:
        # Für Ethereum-Tokens auf Binance
        symbol = self._get_symbol_from_address(token_address, 'ethereum')
        if not symbol:
            return None
            
        url = f"{self.base_url}/ticker/price"
        params = {'symbol': symbol}
        
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
        symbol = self._get_symbol_from_address(token_address, 'bsc')
        if not symbol:
            return None
            
        url = f"{self.base_url}/ticker/price"
        params = {'symbol': symbol}
        
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
        return {"requests_per_minute": 1200, "requests_per_day": 100000}
