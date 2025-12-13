"""
Coinbase exchange implementation for iceberg order detection
"""
import aiohttp
from typing import Dict, List, Optional
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
    
    async def fetch_orderbook(self, symbol: str, limit: int = 100) -> Dict:
        session = await self._get_session()
        
        # Convert BTC/USDT -> BTC-USD (Coinbase format)
        product_id = symbol.replace('/', '-').replace('USDT', 'USD')
        
        url = f"{self.BASE_URL}/products/{product_id}/book"
        params = {'level': 2}
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            
            return self.normalize_orderbook({
                'bids': [[float(price), float(size)] for price, size, _ in data.get('bids', [])],
                'asks': [[float(price), float(size)] for price, size, _ in data.get('asks', [])],
                'timestamp': int(response.headers.get('date', 0)),
                'symbol': symbol
            })
    
    async def fetch_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        session = await self._get_session()
        
        product_id = symbol.replace('/', '-').replace('USDT', 'USD')
        
        url = f"{self.BASE_URL}/products/{product_id}/trades"
        
        async with session.get(url) as response:
            data = await response.json()
            
            trades = []
            for trade in data[:limit]:
                trades.append(self.normalize_trade({
                    'price': float(trade['price']),
                    'amount': float(trade['size']),
                    'side': trade['side'],
                    'timestamp': int(trade['time']),
                    'id': trade['trade_id']
                }))
            
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
