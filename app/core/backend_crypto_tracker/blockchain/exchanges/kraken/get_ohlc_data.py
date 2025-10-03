# blockchain/exchanges/kraken/get_ohlc_data.py
from typing import List, Optional
from datetime import datetime
from decimal import Decimal
from ...data_models import PricePoint
from ..base_provider import BaseProvider, ExchangeConfig

def get_ohlc_data(
    pair: str,
    interval: int = 60,  # minutes
    since: Optional[int] = None
) -> List[PricePoint]:
    """Get OHLC data from Kraken"""
    config = ExchangeConfig(
        base_url="https://api.kraken.com/0",
        rate_limit=1.0  # Kraken has strict rate limits
    )
    provider = KrakenProvider(config)
    
    params = {
        'pair': pair,
        'interval': interval
    }
    
    if since:
        params['since'] = since
    
    response = provider._make_request('/public/OHLC', params)
    
    if 'error' in response and response['error']:
        raise Exception(f"Kraken API error: {response['error']}")
    
    data = response['result'][pair]
    
    return [
        PricePoint(
            timestamp=datetime.fromtimestamp(ohlc[0]),
            open=Decimal(ohlc[1]),
            high=Decimal(ohlc[2]),
            low=Decimal(ohlc[3]),
            close=Decimal(ohlc[4]),
            volume=Decimal(ohlc[6])
        )
        for ohlc in data
    ]

class KrakenProvider(BaseProvider):
    def get_ticker(self, symbol: str):
        return self._make_request('/public/Ticker', {'pair': symbol})
    
    def get_orderbook(self, symbol: str, limit: int = 100):
        return self._make_request('/public/Depth', {'pair': symbol, 'count': limit})
