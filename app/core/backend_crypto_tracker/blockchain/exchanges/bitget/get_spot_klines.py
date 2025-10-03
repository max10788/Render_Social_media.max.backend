# blockchain/exchanges/bitget/get_spot_klines.py
from typing import List, Optional
from datetime import datetime
from decimal import Decimal
from ...data_models import PricePoint
from ..base_provider import BaseProvider, ExchangeConfig

def get_spot_klines(
    symbol: str,
    granularity: str = "1h",
    limit: int = 100,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> List[PricePoint]:
    """Get spot kline data from Bitget"""
    config = ExchangeConfig(
        base_url="https://api.bitget.com/api/v2/spot",
        rate_limit=10.0
    )
    provider = BitgetProvider(config)
    
    params = {
        'symbol': symbol.upper(),
        'granularity': granularity,
        'limit': limit
    }
    
    if start_time:
        params['startTime'] = int(start_time.timestamp() * 1000)
    if end_time:
        params['endTime'] = int(end_time.timestamp() * 1000)
    
    data = provider._make_request('/market/candles', params)
    
    return [
        PricePoint(
            timestamp=datetime.fromtimestamp(int(k[0]) / 1000),
            open=Decimal(k[1]),
            high=Decimal(k[2]),
            low=Decimal(k[3]),
            close=Decimal(k[4]),
            volume=Decimal(k[5])
        )
        for k in data['data']
    ]

class BitgetProvider(BaseProvider):
    def get_ticker(self, symbol: str):
        return self._make_request('/market/ticker', {'symbol': symbol})
    
    def get_orderbook(self, symbol: str, limit: int = 100):
        return self._make_request('/market/orderbook', {'symbol': symbol, 'limit': limit})
