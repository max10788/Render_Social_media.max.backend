"""
Base exchange class for iceberg order detection
Provides common interface for all exchanges
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import asyncio


class BaseExchange(ABC):
    """Abstract base class for exchange implementations"""
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.name = self.__class__.__name__.lower().replace('exchange', '')
        self._orderbook_cache = {}
        self._trades_cache = {}
        
    @abstractmethod
    async def fetch_orderbook(self, symbol: str, limit: int = 100) -> Dict:
        """
        Fetch order book for a symbol
        
        Returns:
            {
                'bids': [[price, volume], ...],
                'asks': [[price, volume], ...],
                'timestamp': int,
                'symbol': str
            }
        """
        pass
    
    @abstractmethod
    async def fetch_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        """
        Fetch recent trades for a symbol
        
        Returns:
            [
                {
                    'price': float,
                    'amount': float,
                    'side': 'buy' or 'sell',
                    'timestamp': int
                },
                ...
            ]
        """
        pass
    
    @abstractmethod
    async def get_available_symbols(self) -> List[str]:
        """Get list of available trading symbols"""
        pass
    
    @abstractmethod
    async def subscribe_orderbook(self, symbol: str, callback):
        """Subscribe to real-time orderbook updates via WebSocket"""
        pass
    
    @abstractmethod
    async def subscribe_trades(self, symbol: str, callback):
        """Subscribe to real-time trades via WebSocket"""
        pass
    
    def normalize_orderbook(self, raw_orderbook: Dict) -> Dict:
        """
        Normalize orderbook to standard format
        Override if exchange has different format
        """
        return {
            'bids': sorted(raw_orderbook.get('bids', []), key=lambda x: x[0], reverse=True),
            'asks': sorted(raw_orderbook.get('asks', []), key=lambda x: x[0]),
            'timestamp': raw_orderbook.get('timestamp', int(datetime.now().timestamp() * 1000)),
            'symbol': raw_orderbook.get('symbol', '')
        }
    
    def normalize_trade(self, raw_trade: Dict) -> Dict:
        """
        Normalize trade to standard format
        Override if exchange has different format
        """
        return {
            'price': float(raw_trade.get('price', 0)),
            'amount': float(raw_trade.get('amount', 0)),
            'side': raw_trade.get('side', 'buy'),
            'timestamp': raw_trade.get('timestamp', int(datetime.now().timestamp() * 1000)),
            'id': raw_trade.get('id', '')
        }
    
    async def get_orderbook_snapshot(self, symbol: str) -> Dict:
        """Get cached orderbook or fetch new one"""
        cache_key = f"{symbol}_orderbook"
        
        # Simple cache with 1 second TTL
        if cache_key in self._orderbook_cache:
            cached_data, cached_time = self._orderbook_cache[cache_key]
            if (datetime.now().timestamp() - cached_time) < 1:
                return cached_data
        
        orderbook = await self.fetch_orderbook(symbol)
        self._orderbook_cache[cache_key] = (orderbook, datetime.now().timestamp())
        return orderbook
    
    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Get cached trades or fetch new ones"""
        cache_key = f"{symbol}_trades_{limit}"
        
        if cache_key in self._trades_cache:
            cached_data, cached_time = self._trades_cache[cache_key]
            if (datetime.now().timestamp() - cached_time) < 1:
                return cached_data
        
        trades = await self.fetch_trades(symbol, limit)
        self._trades_cache[cache_key] = (trades, datetime.now().timestamp())
        return trades
    
    def calculate_spread(self, orderbook: Dict) -> Tuple[float, float]:
        """Calculate bid-ask spread"""
        if not orderbook.get('bids') or not orderbook.get('asks'):
            return 0.0, 0.0
        
        best_bid = orderbook['bids'][0][0]
        best_ask = orderbook['asks'][0][0]
        spread = best_ask - best_bid
        spread_percent = (spread / best_bid) * 100
        
        return spread, spread_percent
    
    def get_exchange_info(self) -> Dict:
        """Get exchange information"""
        return {
            'name': self.name,
            'type': 'cex',
            'supports_websocket': True,
            'rate_limit': self.get_rate_limit()
        }
    
    def get_rate_limit(self) -> int:
        """Get rate limit in requests per second"""
        return 10  # Default, override per exchange
    
    async def close(self):
        """Clean up resources"""
        pass
