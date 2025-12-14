"""
Coinbase exchange implementation for iceberg order detection
FIXED: Proper timestamp parsing for ISO format
"""
import aiohttp
from typing import Dict, List, Optional
from datetime import datetime
import time
from app.core.iceberg_orders.exchanges.base import BaseExchange


class CoinbaseExchange(BaseExchange):
    """Coinbase Advanced Trade API implementation"""
    
    BASE_URL = "https://api.exchange.coinbase.com"
    WS_URL = "wss://ws-feed.exchange.coinbase.com"
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        super().__init__(api_key, api_secret)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    def _parse_timestamp(self, timestamp_str: str) -> int:
        """
        Parse Coinbase timestamp (ISO format) to milliseconds
        
        Coinbase returns: "2025-12-14T20:25:43.426846Z"
        """
        try:
            # Remove 'Z' and parse ISO format
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            # Fallback to current time
            return int(time.time() * 1000)
    
    async def fetch_orderbook(self, symbol: str, limit: int = 100) -> Dict:
        session = await self._get_session()
        
        # Convert BTC/USDT -> BTC-USD (Coinbase format)
        product_id = symbol.replace('/', '-').replace('USDT', 'USD')
        
        url = f"{self.BASE_URL}/products/{product_id}/book"
        params = {'level': 2}
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            
            # Coinbase orderbook structure: [[price, size, num_orders], ...]
            bids = []
            for price, size, num_orders in data.get('bids', []):
                bids.append({
                    'price': float(price),
                    'volume': float(size),
                    'order_count': int(num_orders)  # Coinbase provides this!
                })
            
            asks = []
            for price, size, num_orders in data.get('asks', []):
                asks.append({
                    'price': float(price),
                    'volume': float(size),
                    'order_count': int(num_orders)
                })
            
            return {
                'bids': bids,
                'asks': asks,
                'timestamp': int(time.time() * 1000),
                'symbol': symbol,
                'exchange': 'coinbase'
            }
    
    async def fetch_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        session = await self._get_session()
        
        product_id = symbol.replace('/', '-').replace('USDT', 'USD')
        
        url = f"{self.BASE_URL}/products/{product_id}/trades"
        
        async with session.get(url) as response:
            data = await response.json()
            
            trades = []
            for trade in data[:limit]:
                # FIXED: Parse ISO timestamp
                timestamp = self._parse_timestamp(trade['time'])
                
                # Coinbase side: 'buy' or 'sell' (from taker perspective)
                side = trade['side']
                
                # For iceberg detection, we need maker side
                # In Coinbase: side is the TAKER side
                # So maker side is opposite
                maker_side = 'sell' if side == 'buy' else 'buy'
                
                trades.append({
                    'price': float(trade['price']),
                    'amount': float(trade['size']),
                    'side': side,  # Taker side
                    'maker_side': maker_side,  # Maker side (for iceberg detection)
                    'timestamp': timestamp,
                    'id': str(trade['trade_id']),
                    'is_buyer_maker': (side == 'sell')  # Compatibility
                })
            
            return trades
    
    async def get_available_symbols(self) -> List[str]:
        session = await self._get_session()
        
        url = f"{self.BASE_URL}/products"
        
        async with session.get(url) as response:
            data = await response.json()
            
            symbols = []
            for product in data:
                if product.get('status') == 'online':
                    base = product['base_currency']
                    quote = product['quote_currency']
                    symbols.append(f"{base}/{quote}")
            
            return sorted([s for s in symbols if s.endswith('/USD')])[:50]
    
    async def subscribe_orderbook(self, symbol: str, callback):
        product_id = symbol.replace('/', '-').replace('USDT', 'USD')
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.WS_URL) as ws:
                subscribe_msg = {
                    "type": "subscribe",
                    "product_ids": [product_id],
                    "channels": ["level2"]
                }
                await ws.send_json(subscribe_msg)
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        if data.get('type') == 'snapshot' or data.get('type') == 'l2update':
                            await callback(data)
    
    async def subscribe_trades(self, symbol: str, callback):
        product_id = symbol.replace('/', '-').replace('USDT', 'USD')
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.WS_URL) as ws:
                subscribe_msg = {
                    "type": "subscribe",
                    "product_ids": [product_id],
                    "channels": ["matches"]
                }
                await ws.send_json(subscribe_msg)
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        if data.get('type') == 'match':
                            await callback(data)
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
