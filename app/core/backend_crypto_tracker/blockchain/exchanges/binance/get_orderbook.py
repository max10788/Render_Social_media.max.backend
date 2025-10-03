# blockchain/exchanges/binance/get_orderbook.py
from typing import Dict, Any, List, Tuple
from decimal import Decimal
from ..base_provider import BaseProvider, ExchangeConfig

def get_orderbook(symbol: str, limit: int = 100) -> Dict[str, Any]:
    """Get order book depth from Binance"""
    config = ExchangeConfig(
        base_url="https://api.binance.com/api/v3",
        rate_limit=20.0
    )
    provider = BinanceProvider(config)
    data = provider.get_orderbook(symbol, limit)
    
    def parse_orders(orders: List) -> List[Tuple[Decimal, Decimal]]:
        return [(Decimal(price), Decimal(qty)) for price, qty in orders]
    
    return {
        'bids': parse_orders(data['bids']),
        'asks': parse_orders(data['asks']),
        'last_update_id': data['lastUpdateId']
    }

class BinanceProvider(BaseProvider):
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._make_request('/ticker/24hr', {'symbol': symbol.upper()})
    
    def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        return self._make_request('/depth', {'symbol': symbol.upper(), 'limit': limit})
