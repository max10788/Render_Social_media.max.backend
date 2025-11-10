# Neue Datei: app/core/price_movers/collectors/realtime_stream.py

class RealtimeTradeStream:
    """
    WebSocket Stream für Echtzeit-Trades
    
    Sammelt ALLE Trades während einer Candle
    """
    
    async def stream_candle_trades(
        self,
        symbol: str,
        duration_seconds: int = 300  # 5 Minuten
    ):
        """
        Streamt Trades für eine Candle-Duration
        """
        trades = []
        
        # Bitget WebSocket
        if self.exchange_name == 'bitget':
            import websocket
            
            ws_url = "wss://ws.bitget.com/spot/v1/stream"
            
            def on_message(ws, message):
                data = json.loads(message)
                if data['action'] == 'snapshot' or data['action'] == 'update':
                    for trade in data['data']:
                        trades.append({
                            'timestamp': datetime.fromtimestamp(trade['ts'] / 1000),
                            'price': float(trade['price']),
                            'amount': float(trade['size']),
                            'side': trade['side'],  # 'buy' oder 'sell'
                            'trade_id': trade['tradeId']
                        })
            
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message
            )
            
            # Subscribe zu Trades
            ws.send(json.dumps({
                "op": "subscribe",
                "args": [{"instType": "sp", "channel": "trade", "instId": symbol}]
            }))
            
            # Laufe für duration_seconds
            await asyncio.sleep(duration_seconds)
            ws.close()
        
        return trades
