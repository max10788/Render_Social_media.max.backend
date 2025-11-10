# Neue Datei: app/core/price_movers/collectors/orderbook_analyzer.py

class OrderbookAnalyzer:
    """
    Analysiert Orderbook-Änderungen während einer Candle
    
    Sammelt Snapshots alle 10 Sekunden und erkennt:
    - Große Orders (Walls)
    - Order Cancellations
    - Aggressive Taker
    """
    
    async def analyze_candle_orderbook(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime
    ):
        """
        Nimmt Orderbook-Snapshots während Candle
        """
        snapshots = []
        
        current_time = start_time
        while current_time < end_time:
            # Hole Orderbook
            orderbook = await self.collector.fetch_orderbook(symbol, limit=50)
            
            snapshots.append({
                'timestamp': current_time,
                'bids': orderbook['bids'][:20],  # Top 20
                'asks': orderbook['asks'][:20],
                'spread': orderbook['spread']
            })
            
            # Wait 10 seconds
            await asyncio.sleep(10)
            current_time = datetime.now()
        
        # Analysiere Änderungen
        large_orders = self._detect_large_orders(snapshots)
        order_cancellations = self._detect_cancellations(snapshots)
        aggressive_trades = self._detect_aggressive_trades(snapshots)
        
        return {
            'large_orders': large_orders,
            'cancellations': order_cancellations,
            'aggressive_trades': aggressive_trades
        }
    
    def _detect_large_orders(self, snapshots):
        """
        Findet Orders > 5x Average Size
        
        Diese sind wahrscheinlich von Whales/Institutions
        """
        all_orders = []
        for snapshot in snapshots:
            all_orders.extend([
                {'price': bid[0], 'size': bid[1], 'side': 'bid', 'timestamp': snapshot['timestamp']}
                for bid in snapshot['bids']
            ])
            all_orders.extend([
                {'price': ask[0], 'size': ask[1], 'side': 'ask', 'timestamp': snapshot['timestamp']}
                for ask in snapshot['asks']
            ])
        
        # Berechne Average
        avg_size = np.mean([o['size'] for o in all_orders])
        
        # Finde Large Orders
        large_orders = [
            o for o in all_orders
            if o['size'] > 5 * avg_size
        ]
        
        return large_orders
