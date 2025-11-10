"""
Realtime Trade Stream - WebSocket Trade Streaming

Für LIVE-Analyse (< 5 Minuten):
- Streamt ALLE Trades via WebSocket
- Keine Rate Limits
- Millisekunden-Präzision
"""

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable
import websockets


logger = logging.getLogger(__name__)


class RealtimeTradeStream:
    """
    WebSocket Stream für Echtzeit-Trades
    
    Sammelt ALLE Trades während einer Candle (nicht nur Sample)
    
    Unterstützt:
    - Bitget WebSocket
    - Binance WebSocket
    - (Kraken: TODO)
    """
    
    def __init__(self, exchange_name: str):
        """
        Args:
            exchange_name: 'bitget', 'binance', oder 'kraken'
        """
        self.exchange_name = exchange_name.lower()
        self.trades = []
        self.is_streaming = False
        
        logger.info(f"RealtimeTradeStream initialisiert für {self.exchange_name}")
    
    async def stream_candle_trades(
        self,
        symbol: str,
        duration_seconds: int = 300,  # 5 Minuten
        on_trade_callback: Optional[Callable] = None
    ) -> List[Dict[str, Any]]:
        """
        Streamt Trades für eine Candle-Duration
        
        Args:
            symbol: Trading Pair (z.B. 'BTC/USDT')
            duration_seconds: Wie lange streamen (default: 300s = 5min)
            on_trade_callback: Optional callback für jeden Trade
            
        Returns:
            Liste aller Trades
        """
        logger.info(
            f"Starte WebSocket Stream für {symbol} ({duration_seconds}s)"
        )
        
        self.trades = []
        self.is_streaming = True
        
        # Exchange-spezifische Implementation
        if self.exchange_name == 'bitget':
            await self._stream_bitget(symbol, duration_seconds, on_trade_callback)
        elif self.exchange_name == 'binance':
            await self._stream_binance(symbol, duration_seconds, on_trade_callback)
        elif self.exchange_name == 'kraken':
            await self._stream_kraken(symbol, duration_seconds, on_trade_callback)
        else:
            logger.error(f"WebSocket streaming not implemented for {self.exchange_name}")
            return []
        
        self.is_streaming = False
        
        logger.info(f"✅ Stream completed: {len(self.trades)} trades collected")
        
        return self.trades
    
    async def _stream_bitget(
        self,
        symbol: str,
        duration_seconds: int,
        callback: Optional[Callable]
    ):
        """Bitget WebSocket Stream"""
        
        # Konvertiere Symbol Format: BTC/USDT → BTCUSDT_SPBL
        symbol_formatted = symbol.replace('/', '') + '_SPBL'
        
        ws_url = "wss://ws.bitget.com/spot/v1/stream"
        
        try:
            async with websockets.connect(ws_url) as websocket:
                # Subscribe zu Trades
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [{
                        "instType": "sp",
                        "channel": "trade",
                        "instId": symbol_formatted
                    }]
                }
                
                await websocket.send(json.dumps(subscribe_msg))
                logger.info(f"Bitget: Subscribed to {symbol_formatted}")
                
                # Stream für duration_seconds
                end_time = datetime.now() + timedelta(seconds=duration_seconds)
                
                while datetime.now() < end_time and self.is_streaming:
                    try:
                        # Timeout damit wir end_time checken können
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=1.0
                        )
                        
                        data = json.loads(message)
                        
                        # Prüfe ob Trade-Data
                        if data.get('action') in ['snapshot', 'update']:
                            for trade_raw in data.get('data', []):
                                trade = self._parse_bitget_trade(trade_raw, symbol)
                                self.trades.append(trade)
                                
                                if callback:
                                    callback(trade)
                    
                    except asyncio.TimeoutError:
                        # Normal - weiter streamen
                        continue
                    except Exception as e:
                        logger.warning(f"Stream message error: {e}")
                        continue
                
        except Exception as e:
            logger.error(f"Bitget WebSocket error: {e}", exc_info=True)
    
    async def _stream_binance(
        self,
        symbol: str,
        duration_seconds: int,
        callback: Optional[Callable]
    ):
        """Binance WebSocket Stream"""
        
        # Konvertiere Symbol: BTC/USDT → btcusdt
        symbol_formatted = symbol.replace('/', '').lower()
        
        ws_url = f"wss://stream.binance.com:9443/ws/{symbol_formatted}@trade"
        
        try:
            async with websockets.connect(ws_url) as websocket:
                logger.info(f"Binance: Connected to {symbol_formatted}@trade")
                
                # Stream für duration_seconds
                end_time = datetime.now() + timedelta(seconds=duration_seconds)
                
                while datetime.now() < end_time and self.is_streaming:
                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=1.0
                        )
                        
                        data = json.loads(message)
                        
                        # Binance Trade Format
                        if data.get('e') == 'trade':
                            trade = self._parse_binance_trade(data, symbol)
                            self.trades.append(trade)
                            
                            if callback:
                                callback(trade)
                    
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.warning(f"Stream message error: {e}")
                        continue
                
        except Exception as e:
            logger.error(f"Binance WebSocket error: {e}", exc_info=True)
    
    async def _stream_kraken(
        self,
        symbol: str,
        duration_seconds: int,
        callback: Optional[Callable]
    ):
        """Kraken WebSocket Stream (TODO: Implementieren wenn gewünscht)"""
        logger.warning("Kraken WebSocket streaming not yet implemented")
        # Kraken nutzt anderes WebSocket Protokoll
        # Implementation analog zu Bitget/Binance möglich
    
    def _parse_bitget_trade(self, trade_raw: Dict, symbol: str) -> Dict[str, Any]:
        """Parsed Bitget Trade Format"""
        return {
            'id': trade_raw.get('tradeId'),
            'timestamp': datetime.fromtimestamp(int(trade_raw['ts']) / 1000),
            'trade_type': 'buy' if trade_raw['side'] == 'buy' else 'sell',
            'amount': float(trade_raw['size']),
            'price': float(trade_raw['price']),
            'value_usd': float(trade_raw['size']) * float(trade_raw['price']),
            'symbol': symbol,
            'source': 'websocket'
        }
    
    def _parse_binance_trade(self, trade_raw: Dict, symbol: str) -> Dict[str, Any]:
        """Parsed Binance Trade Format"""
        return {
            'id': str(trade_raw['t']),  # Trade ID
            'timestamp': datetime.fromtimestamp(trade_raw['T'] / 1000),  # Trade time
            'trade_type': 'sell' if trade_raw['m'] else 'buy',  # m = buyer is maker
            'amount': float(trade_raw['q']),  # Quantity
            'price': float(trade_raw['p']),   # Price
            'value_usd': float(trade_raw['q']) * float(trade_raw['p']),
            'symbol': symbol,
            'source': 'websocket'
        }
    
    def stop_streaming(self):
        """Stoppt den Stream vorzeitig"""
        self.is_streaming = False
        logger.info("Stream stopped manually")
