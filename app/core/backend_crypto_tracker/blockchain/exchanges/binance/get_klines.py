# blockchain/exchanges/binance/get_klines.py
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal
from ...data_models import PricePoint
from ..base_provider import BaseProvider, ExchangeConfig

def get_klines(
    symbol: str,
    interval: str = "1h",
    limit: int = 100,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
) -> List[PricePoint]:
    """Get kline/candlestick data from Binance"""
    config = ExchangeConfig(
        base_url="https://api.binance.com/api/v3",
        rate_limit=20.0
    )
    provider = BinanceProvider(config)
    
    params = {
        'symbol': symbol.upper(),
        'interval': interval,
        'limit': limit
    }
    
    if start_time:
        params['startTime'] = int(start_time.timestamp() * 1000)
    if end_time:
        params['endTime'] = int(end_time.timestamp() * 1000)
    
    data = provider._make_request('/klines', params)
    
    return [
        PricePoint(
            timestamp=datetime.fromtimestamp(k[0] / 1000),
            open=Decimal(k[1]),
            high=Decimal(k[2]),
            low=Decimal(k[3]),
            close=Decimal(k[4]),
            volume=Decimal(k[5])
        )
        for k in data
    ]

class BinanceProvider(BaseProvider):
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._make_request('/ticker/24hr', {'symbol': symbol.upper()})
    
    def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        return self._make_request('/depth', {'symbol': symbol.upper(), 'limit': limit})
