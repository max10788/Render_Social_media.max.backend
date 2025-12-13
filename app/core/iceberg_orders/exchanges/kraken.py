"""
Kraken exchange implementation for iceberg order detection
"""
import aiohttp
from typing import Dict, List, Optional
import time
from app.core.iceberg_orders.exchanges.base import BaseExchange

class KrakenExchange(BaseExchange):
    """Kraken exchange implementation"""
    
    BASE_URL = "https://api.kraken.com"
    WS_URL = "wss://ws.kraken.com"
    
    # Kraken symbol mapping
    SYMBOL_MAP = {
        'BTC/USDT': 'XBTUSDT',
        'ETH/USDT': 'ETHUSDT',
        'SOL/USDT': 'SOLUSDT',
    }
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        super().__init__(api_key, api_secret)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    def _convert_symbol(self, symbol: str) -> str:
        """Convert standard symbol to Kraken format"""
        return self.SYMBOL_MAP.get(symbol, symbol.replace('/', ''))
    
    async def fetch_orderbook(self, symbol: str, limit: int = 100) -> Dict:
        session = await self._get_session()
        
        kraken_symbol = self._convert_symbol(symbol)
        
        url = f"{self.BASE_URL}/0/public/Depth"
        params = {
            'pair': kraken_symbol,
            'count': min(limit, 500)
        }
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            
            if data.get('error'):
                raise Exception(f"Kraken API error: {data['error']}")
            
            result = list(data['result'].values())[0]
            
            return self.normalize_orderbook({
                'bids': [[float(price), float(volume)] for price, volume, _ in result.get('bids', [])],
                'asks': [[float(price), float(volume)] for price, volume, _ in result.get('asks', [])],
                'timestamp': int(time.time() * 1000),
                'symbol': symbol
            })
    
    async def fetch_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        session = await self._get_session()
        
        kraken_symbol = self._convert_symbol(symbol)
        
        url = f"{self.BASE_URL}/0/public/Trades"
        params = {
            'pair': kraken_symbol,
            'count': min(limit, 1000)
        }
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            
            if data.get('error'):
                raise Exception(f"Kraken API error: {data['error']}")
            
            result = list(data['result'].values())[0]
            
            trades = []
            for trade in result:
                price, volume, timestamp, side, *_ = trade
                trades.append(self.normalize_trade({
                    'price': float(price),
                    'amount': float(volume),
                    'side': 'sell' if side == 's' else 'buy',
                    'timestamp': int(float(timestamp) * 1000),
                    'id': str(timestamp)
                }))
            
            return trades
    
    async def get_available_symbols(self) -> List[str]:
        session = await self._get_session()
        
        url = f"{self.BASE_URL}/0/public/AssetPairs"
        
        async with session.get(url) as response:
            data = await response.json()
            
            if data.get('error'):
                raise Exception(f"Kraken API error: {data['error']}")
            
            symbols = []
            for pair_name, pair_info in data['result'].items():
                if 'wsname' in pair_info:
                    base = pair_info['base']
                    quote = pair_info['quote']
                    
                    # Clean up Kraken's naming
                    if base.startswith('X'):
                        base = base[1:]
                    if quote.startswith('Z') or quote.startswith('X'):
                        quote = quote[1:]
                    
                    symbols.append(f"{base}/{quote}")
            
            # Filter to USDT pairs
            usdt_symbols = [s for s in symbols if 'USDT' in s or 'USD' in s]
            return sorted(set(usdt_symbols))[:50]
    
    async def subscribe_orderbook(self, symbol: str, callback):
        kraken_symbol = self._convert_symbol(symbol)
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.WS_URL) as ws:
                subscribe_msg = {
                    "event": "subscribe",
                    "pair": [kraken_symbol],
                    "subscription": {"name": "book", "depth": 100}
                }
                await ws.send_json(subscribe_msg)
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        if isinstance(data, list):
                            await callback(data)
    
    async def subscribe_trades(self, symbol: str, callback):
        kraken_symbol = self._convert_symbol(symbol)
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.WS_URL) as ws:
                subscribe_msg = {
                    "event": "subscribe",
                    "pair": [kraken_symbol],
                    "subscription": {"name": "trade"}
                }
                await ws.send_json(subscribe_msg)
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        if isinstance(data, list):
                            await callback(data)
    
    def get_rate_limit(self) -> int:
        return 15  # 15 requests per second
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
