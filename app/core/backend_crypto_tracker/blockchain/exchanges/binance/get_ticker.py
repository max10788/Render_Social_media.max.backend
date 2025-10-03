# blockchain/exchanges/binance/get_ticker.py
from typing import Dict, Any
from decimal import Decimal
from ..base_provider import BaseProvider, ExchangeConfig

def get_ticker(symbol: str) -> Dict[str, Any]:
    """Get 24hr ticker statistics from Binance"""
    config = ExchangeConfig(
        base_url="https://api.binance.com/api/v3",
        rate_limit=20.0
    )
    provider = BinanceProvider(config)
    data = provider.get_ticker(symbol)
    
    return {
        'symbol': data['symbol'],
        'price': Decimal(data['lastPrice']),
        'volume_24h': Decimal(data['volume']),
        'quote_volume_24h': Decimal(data['quoteVolume']),
        'price_change_24h': Decimal(data['priceChange']),
        'price_change_percent_24h': float(data['priceChangePercent']),
        'high_24h': Decimal(data['highPrice']),
        'low_24h': Decimal(data['lowPrice'])
    }

class BinanceProvider(BaseProvider):
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._make_request('/ticker/24hr', {'symbol': symbol.upper()})
    
    def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        return self._make_request('/depth', {'symbol': symbol.upper(), 'limit': limit})
