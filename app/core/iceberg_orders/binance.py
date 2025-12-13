"""
Binance exchange implementation for iceberg order detection
"""
import asyncio
import aiohttp
import hmac
import hashlib
import time
from typing import Dict, List, Optional
from .base import BaseExchange


class BinanceExchange(BaseExchange):
    """Binance exchange implementation"""
    
    BASE_URL = "https://api.binance.com"
    WS_URL = "wss://stream.binance.com:9443/ws"
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        super().__init__(api_key, api_secret)
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_connections = {}
        
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def fetch_orderbook(self, symbol: str, limit: int = 100) -> Dict:
        """Fetch orderbook from Binance"""
        session = await self._get_session()
        
        # Convert symbol format (BTC/USDT -> BTCUSDT)
        binance_symbol = symbol.replace('/', '')
        
        url = f"{self.BASE_URL}/api/v3/depth"
        params = {
            'symbol': binance_symbol,
            'limit': min(limit, 5000)
        }
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            
            return self.normalize_orderbook({
                'bids': [[float(price), float(qty)] for price, qty in data.get('bids', [])],
                'asks': [[float(price), float(qty)] for price, qty in data.get('asks', [])],
                'timestamp': data.get('lastUpdateId', int(time.time() * 1000)),
                'symbol': symbol
            })
    
    async def fetch_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        """Fetch recent trades from Binance"""
        session = await self._get_session()
        
        binance_symbol = symbol.replace('/', '')
        
        url = f"{self.BASE_URL}/api/v3/trades"
        params = {
            'symbol': binance_symbol,
            'limit': min(limit, 1000)
        }
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            
            trades = []
            for trade in data:
                trades.append(self.normalize_trade({
                    'price': float(trade['price']),
                    'amount': float(trade['qty']),
                    'side': 'sell' if trade['isBuyerMaker'] else 'buy',
                    'timestamp': trade['time'],
                    'id': trade['id']
                }))
            
            return trades
    
    async def get_available_symbols(self) -> List[str]:
        """Get available trading pairs from Binance"""
        session = await self._get_session()
        
        url = f"{self.BASE_URL}/api/v3/exchangeInfo"
        
        async with session.get(url) as response:
            data = await response.json()
            
            symbols = []
            for symbol_info in data.get('symbols', []):
                if symbol_info['status'] == 'TRADING':
                    # Convert BTCUSDT -> BTC/USDT
                    base = symbol_info['baseAsset']
                    quote = symbol_info['quoteAsset']
                    symbols.append(f"{base}/{quote}")
            
            # Filter to major USDT pairs
            usdt_symbols = [s for s in symbols if s.endswith('/USDT')]
            return sorted(usdt_symbols)[:100]  # Limit to top 100
    
    async def subscribe_orderbook(self, symbol: str, callback):
        """Subscribe to orderbook updates via WebSocket"""
        binance_symbol = symbol.replace('/', '').lower()
        stream_name = f"{binance_symbol}@depth@100ms"
        
        ws_url = f"{self.WS_URL}/{stream_name}"
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url) as ws:
                self.ws_connections[f"orderbook_{symbol}"] = ws
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        
                        orderbook = self.normalize_orderbook({
                            'bids': [[float(price), float(qty)] for price, qty in data.get('b', [])],
                            'asks': [[float(price), float(qty)] for price, qty in data.get('a', [])],
                            'timestamp': data.get('E', int(time.time() * 1000)),
                            'symbol': symbol
                        })
                        
                        await callback(orderbook)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break
    
    async def subscribe_trades(self, symbol: str, callback):
        """Subscribe to trade updates via WebSocket"""
        binance_symbol = symbol.replace('/', '').lower()
        stream_name = f"{binance_symbol}@trade"
        
        ws_url = f"{self.WS_URL}/{stream_name}"
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url) as ws:
                self.ws_connections[f"trades_{symbol}"] = ws
                
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = msg.json()
                        
                        trade = self.normalize_trade({
                            'price': float(data['p']),
                            'amount': float(data['q']),
                            'side': 'sell' if data['m'] else 'buy',
                            'timestamp': data['T'],
                            'id': data['t']
                        })
                        
                        await callback(trade)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break
    
    async def fetch_klines(self, symbol: str, interval: str = '1m', limit: int = 100) -> List[Dict]:
        """Fetch candlestick data"""
        session = await self._get_session()
        
        binance_symbol = symbol.replace('/', '')
        
        url = f"{self.BASE_URL}/api/v3/klines"
        params = {
            'symbol': binance_symbol,
            'interval': interval,
            'limit': min(limit, 1000)
        }
        
        async with session.get(url, params=params) as response:
            data = await response.json()
            
            klines = []
            for kline in data:
                klines.append({
                    'timestamp': kline[0],
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                })
            
            return klines
    
    def get_rate_limit(self) -> int:
        """Binance rate limit"""
        return 20  # 20 requests per second
    
    async def close(self):
        """Close all connections"""
        for ws in self.ws_connections.values():
            await ws.close()
        
        if self.session and not self.session.closed:
            await self.session.close()
