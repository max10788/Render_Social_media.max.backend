# blockchain/exchanges/coinbase/get_candles.py
from typing import List, Optional
from datetime import datetime
from decimal import Decimal
from ...data_models import PricePoint
from ..base_provider import BaseProvider, ExchangeConfig

def get_candles(
    product_id: str,
    granularity: int = 3600,  # 1 hour in seconds
    start: Optional[datetime] = None,
    end: Optional[datetime] = None
) -> List[PricePoint]:
    """Get candle data from Coinbase"""
    config = ExchangeConfig(
        base_url="https://api.exchange.coinbase.com",
        rate_limit=10.0
    )
    provider = CoinbaseProvider(config)
    
    params = {
        'granularity': granularity
    }
    
    if start:
        params['start'] = start.isoformat()
    if end:
        params['end'] = end.isoformat()
    
    data = provider._make_request(f'/products/{product_id}/candles', params)
    
    return [
        PricePoint(
            timestamp=datetime.fromtimestamp(candle[0]),
            open=Decimal(str(candle[3])),
            high=Decimal(str(candle[2])),
            low=Decimal(str(candle[1])),
            close=Decimal(str(candle[4])),
            volume=Decimal(str(candle[5]))
        )
        for candle in data
    ]

class CoinbaseProvider(BaseProvider):
    def get_ticker(self, symbol: str):
        return self._make_request(f'/products/{symbol}/ticker')
    
    def get_orderbook(self, symbol: str, limit: int = 100):
        return self._make_request(f'/products/{symbol}/book', {'level': 2})
