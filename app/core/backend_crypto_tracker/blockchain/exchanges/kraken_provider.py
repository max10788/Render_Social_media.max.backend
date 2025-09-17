"""
Kraken API provider implementation.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from .base_provider import BaseAPIProvider
from ..data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class KrakenProvider(BaseAPIProvider):
    """Kraken API-Anbieter - gute Orderbuchdaten"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Kraken", "https://api.kraken.com/0/public", api_key, "KRAKEN_API_KEY")
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Kraken verwendet Paar-Namen statt Contract-Adressen
            pair = self._get_pair_from_address(token_address, chain)
            if not pair:
                return None
            
            url = f"{self.base_url}/Ticker"
            params = {'pair': pair}
            
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
                    high_24h=float(ticker.get('h', [0, 0])[1]),
                    low_24h=float(ticker.get('l', [0, 0])[1]),
                    bid_price=float(ticker.get('b', [0, 0])[0]),
                    ask_price=float(ticker.get('a', [0, 0])[0]),
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from Kraken: {e}")
        
        return None
    
    async def get_order_book(self, token_address: str, chain: str, count: int = 100) -> Optional[Dict[str, Any]]:
        """Holt Orderbuchdaten mit Tiefe"""
        try:
            pair = self._get_pair_from_address(token_address, chain)
            if not pair:
                return None
            
            url = f"{self.base_url}/Depth"
            params = {
                'pair': pair,
                'count': count
            }
            
            data = await self._make_request(url, params)
            
            if data.get('error') == [] and data.get('result'):
                result = data['result']
                # Das Ergebnis enthält den Pair-Namen als Schlüssel
                pair_key = list(result.keys())[0]
                order_book = result[pair_key]
                
                return {
                    'bids': order_book.get('bids', []),
                    'asks': order_book.get('asks', []),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching order book from Kraken: {e}")
        
        return None
    
    async def get_historical_ohlc(self, token_address: str, chain: str, interval: int = 1440, since: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """Holt historische OHLC-Daten"""
        try:
            pair = self._get_pair_from_address(token_address, chain)
            if not pair:
                return None
            
            url = f"{self.base_url}/OHLC"
            params = {
                'pair': pair,
                'interval': interval  # in Minuten (1, 5, 15, 30, 60, 240, 1440, 10080, 21600)
            }
            
            if since:
                params['since'] = since
            
            data = await self._make_request(url, params)
            
            if data.get('error') == [] and data.get('result'):
                result = data['result']
                # Das Ergebnis enthält den Pair-Namen als Schlüssel
                pair_key = list(result.keys())[0]
                ohlc_data = result[pair_key]
                
                ohlc_list = []
                for item in ohlc_data:
                    ohlc_list.append({
                        'time': datetime.fromtimestamp(item[0]),
                        'open': float(item[1]),
                        'high': float(item[2]),
                        'low': float(item[3]),
                        'close': float(item[4]),
                        'vwap': float(item[5]),
                        'volume': float(item[6]),
                        'count': int(item[7])
                    })
                
                return ohlc_list
        except Exception as e:
            logger.error(f"Error fetching historical OHLC from Kraken: {e}")
        
        return None
    
    def _get_pair_from_address(self, token_address: str, chain: str) -> Optional[str]:
        """Versucht, das Handelspaar aus der Contract-Adresse abzuleiten"""
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
