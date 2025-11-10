"""
Orderbook Analyzer - Analysiert Orderbook-Veränderungen

Für LIVE-Daten (< 5 Minuten):
- Nimmt Snapshots alle 10 Sekunden
- Erkennt große Orders (Walls)
- Erkennt Order Cancellations
- Erkennt Aggressive Taker
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict
import numpy as np


logger = logging.getLogger(__name__)


class OrderbookAnalyzer:
    """
    Analysiert Orderbook-Dynamik während einer Candle
    
    Sammelt Orderbook-Snapshots und erkennt:
    - Large Orders (Whale Walls)
    - Order Cancellations
    - Aggressive Market Orders
    - Spread Changes
    """
    
    def __init__(self, exchange_collector):
        """
        Args:
            exchange_collector: ExchangeCollector instance
        """
        self.collector = exchange_collector
        self.snapshot_interval_seconds = 10
        logger.info("OrderbookAnalyzer initialisiert")
    
    async def analyze_candle_orderbook(
        self,
        symbol: str,
        duration_seconds: int = 300,  # 5 Minuten
        snapshot_limit: int = 50
    ) -> Dict[str, Any]:
        """
        Nimmt Orderbook-Snapshots während einer Candle-Duration
        
        Args:
            symbol: Trading Pair
            duration_seconds: Candle-Duration (default: 300s = 5min)
            snapshot_limit: Orderbook depth pro Snapshot
            
        Returns:
            Dictionary mit Orderbook-Analyse
        """
        logger.info(
            f"Starte Orderbook-Analyse für {symbol} ({duration_seconds}s)"
        )
        
        snapshots = []
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=duration_seconds)
        
        snapshot_count = 0
        
        # Sammle Snapshots
        while datetime.now() < end_time:
            try:
                # Hole Orderbook
                orderbook = await self.collector.fetch_orderbook(
                    symbol=symbol,
                    limit=snapshot_limit
                )
                
                snapshots.append({
                    'timestamp': datetime.now(),
                    'bids': orderbook['bids'][:20],  # Top 20
                    'asks': orderbook['asks'][:20],
                    'spread': orderbook.get('spread', 0),
                    'spread_pct': orderbook.get('spread_pct', 0),
                    'best_bid': orderbook.get('bid'),
                    'best_ask': orderbook.get('ask'),
                })
                
                snapshot_count += 1
                
                # Warte bis nächster Snapshot
                await asyncio.sleep(self.snapshot_interval_seconds)
                
            except Exception as e:
                logger.warning(f"Orderbook snapshot failed: {e}")
                await asyncio.sleep(self.snapshot_interval_seconds)
        
        logger.info(f"✓ {snapshot_count} Orderbook snapshots gesammelt")
        
        if len(snapshots) < 2:
            logger.warning("Nicht genug Snapshots für Analyse")
            return self._empty_analysis()
        
        # Analysiere Snapshots
        analysis = {
            'snapshot_count': len(snapshots),
            'duration_seconds': duration_seconds,
            'large_orders': self._detect_large_orders(snapshots),
            'order_cancellations': self._detect_cancellations(snapshots),
            'spread_changes': self._analyze_spread_changes(snapshots),
            'liquidity_profile': self._analyze_liquidity(snapshots),
        }
        
        logger.info(
            f"✅ Orderbook-Analyse abgeschlossen: "
            f"{len(analysis['large_orders'])} large orders, "
            f"{len(analysis['order_cancellations'])} cancellations"
        )
        
        return analysis
    
    def _detect_large_orders(self, snapshots: List[Dict]) -> List[Dict[str, Any]]:
        """
        Findet Orders die > 5x Average Size sind
        
        Diese sind wahrscheinlich von Whales/Institutions
        """
        all_orders = []
        
        # Sammle alle Orders aus allen Snapshots
        for snapshot in snapshots:
            timestamp = snapshot['timestamp']
            
            # Bids
            for bid in snapshot['bids']:
                all_orders.append({
                    'timestamp': timestamp,
                    'price': bid[0],
                    'size': bid[1],
                    'side': 'bid',
                    'value_usd': bid[0] * bid[1]
                })
            
            # Asks
            for ask in snapshot['asks']:
                all_orders.append({
                    'timestamp': timestamp,
                    'price': ask[0],
                    'size': ask[1],
                    'side': 'ask',
                    'value_usd': ask[0] * ask[1]
                })
        
        if not all_orders:
            return []
        
        # Berechne Average Size
        avg_size = np.mean([o['size'] for o in all_orders])
        avg_value = np.mean([o['value_usd'] for o in all_orders])
        
        # Finde Large Orders (> 5x Average)
        large_orders = [
            o for o in all_orders
            if o['size'] > 5 * avg_size or o['value_usd'] > 5 * avg_value
        ]
        
        # Sortiere nach Value
        large_orders.sort(key=lambda x: x['value_usd'], reverse=True)
        
        return large_orders[:50]  # Top 50
    
    def _detect_cancellations(self, snapshots: List[Dict]) -> List[Dict[str, Any]]:
        """
        Erkennt Order-Cancellations
        
        Eine Order "verschwindet" zwischen zwei Snapshots
        → Wahrscheinlich gecancelt (oder gefilled)
        """
        if len(snapshots) < 2:
            return []
        
        cancellations = []
        
        # Vergleiche aufeinanderfolgende Snapshots
        for i in range(len(snapshots) - 1):
            prev_snapshot = snapshots[i]
            curr_snapshot = snapshots[i + 1]
            
            # Erstelle Orderbook-State für Vergleich
            prev_bids = {bid[0]: bid[1] for bid in prev_snapshot['bids']}
            curr_bids = {bid[0]: bid[1] for bid in curr_snapshot['bids']}
            
            prev_asks = {ask[0]: ask[1] for ask in prev_snapshot['asks']}
            curr_asks = {ask[0]: ask[1] for ask in curr_snapshot['asks']}
            
            # Finde verschwundene Bids
            for price, size in prev_bids.items():
                if price not in curr_bids and size > 0:
                    # Order verschwunden
                    cancellations.append({
                        'timestamp': curr_snapshot['timestamp'],
                        'price': price,
                        'size': size,
                        'side': 'bid',
                        'value_usd': price * size
                    })
            
            # Finde verschwundene Asks
            for price, size in prev_asks.items():
                if price not in curr_asks and size > 0:
                    cancellations.append({
                        'timestamp': curr_snapshot['timestamp'],
                        'price': price,
                        'size': size,
                        'side': 'ask',
                        'value_usd': price * size
                    })
        
        # Sortiere nach Value
        cancellations.sort(key=lambda x: x['value_usd'], reverse=True)
        
        return cancellations[:30]  # Top 30
    
    def _analyze_spread_changes(self, snapshots: List[Dict]) -> Dict[str, Any]:
        """
        Analysiert Spread-Veränderungen
        
        Hohe Volatilität im Spread = Hohe Unsicherheit/Aktivität
        """
        spreads = [s['spread'] for s in snapshots if s.get('spread')]
        spread_pcts = [s['spread_pct'] for s in snapshots if s.get('spread_pct')]
        
        if not spreads:
            return {}
        
        return {
            'min_spread': min(spreads),
            'max_spread': max(spreads),
            'avg_spread': np.mean(spreads),
            'spread_volatility': np.std(spreads),
            'avg_spread_pct': np.mean(spread_pcts) if spread_pcts else 0,
        }
    
    def _analyze_liquidity(self, snapshots: List[Dict]) -> Dict[str, Any]:
        """
        Analysiert Liquiditäts-Profil
        
        Wie tief ist das Orderbook?
        """
        # Berechne durchschnittliche Liquidität pro Level
        bid_liquidity = []
        ask_liquidity = []
        
        for snapshot in snapshots:
            bid_liquidity.append(sum(bid[1] for bid in snapshot['bids']))
            ask_liquidity.append(sum(ask[1] for ask in snapshot['asks']))
        
        return {
            'avg_bid_liquidity': np.mean(bid_liquidity),
            'avg_ask_liquidity': np.mean(ask_liquidity),
            'liquidity_imbalance': np.mean(bid_liquidity) - np.mean(ask_liquidity),
            'liquidity_ratio': np.mean(bid_liquidity) / np.mean(ask_liquidity) if np.mean(ask_liquidity) > 0 else 0,
        }
    
    def _empty_analysis(self) -> Dict[str, Any]:
        """Leere Analyse wenn keine Daten"""
        return {
            'snapshot_count': 0,
            'duration_seconds': 0,
            'large_orders': [],
            'order_cancellations': [],
            'spread_changes': {},
            'liquidity_profile': {},
        }
