"""
Core iceberg order detection logic
Implements multiple detection algorithms
"""
import asyncio
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict, deque

from ..models.iceberg import (
    IcebergOrder, OrderSide, DetectionMethod, 
    TradeEvent, OrderBookSnapshot, OrderBookLevel,
    RefillPattern, VolumeAnomaly, IcebergDetectionResult
)


class IcebergDetector:
    """Main iceberg order detector"""
    
    def __init__(self, threshold: float = 0.05, lookback_window: int = 100):
        """
        Initialize detector
        
        Args:
            threshold: Detection threshold (e.g., 0.05 = 5%)
            lookback_window: Number of historical data points to analyze
        """
        self.threshold = threshold
        self.lookback_window = lookback_window
        
        # Historical data storage
        self.orderbook_history = deque(maxlen=lookback_window)
        self.trade_history = deque(maxlen=lookback_window * 2)
        
        # Detection state
        self.price_level_volumes = defaultdict(list)
        self.refill_timestamps = defaultdict(list)
    
    async def detect(
        self,
        orderbook: Dict,
        trades: List[Dict],
        exchange: str,
        symbol: str
    ) -> IcebergDetectionResult:
        """
        Main detection method - combines multiple algorithms
        
        Args:
            orderbook: Current order book snapshot
            trades: Recent trades
            exchange: Exchange name
            symbol: Trading symbol
            
        Returns:
            IcebergDetectionResult with detected icebergs
        """
        # Convert to model objects
        ob_snapshot = self._parse_orderbook(orderbook, exchange, symbol)
        trade_events = self._parse_trades(trades)
        
        # Store history
        self.orderbook_history.append(ob_snapshot)
        self.trade_history.extend(trade_events)
        
        # Run detection algorithms
        trade_flow_icebergs = await self._detect_via_trade_flow(ob_snapshot, trade_events)
        refill_icebergs = await self._detect_via_refill_pattern(ob_snapshot)
        volume_anomaly_icebergs = await self._detect_via_volume_anomaly(ob_snapshot, trade_events)
        
        # Combine and deduplicate results
        all_icebergs = self._merge_detections(
            trade_flow_icebergs,
            refill_icebergs,
            volume_anomaly_icebergs
        )
        
        # Create timeline
        timeline = self._create_timeline(all_icebergs, trade_events)
        
        # Metadata
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'detectionThreshold': self.threshold,
            'algorithmsUsed': ['trade_flow', 'refill_pattern', 'volume_anomaly']
        }
        
        return IcebergDetectionResult(
            icebergs=all_icebergs,
            timeline=timeline,
            metadata=metadata
        )
    
    async def _detect_via_trade_flow(
        self,
        orderbook: OrderBookSnapshot,
        trades: List[TradeEvent]
    ) -> List[IcebergOrder]:
        """
        Detect icebergs by analyzing trade flow vs visible volume
        
        Logic: If trades executed >> visible volume at price level,
        likely iceberg order present
        """
        icebergs = []
        
        # Analyze bids (buy side)
        for bid_level in orderbook.bids[:20]:  # Top 20 levels
            nearby_trades = self._get_trades_near_price(
                trades,
                bid_level.price,
                OrderSide.BUY,
                tolerance=0.001  # 0.1% price tolerance
            )
            
            total_trade_volume = sum(t.amount for t in nearby_trades)
            
            # Detection condition
            if total_trade_volume > bid_level.volume * (1 + self.threshold):
                hidden_volume = total_trade_volume - bid_level.volume
                confidence = min(hidden_volume / bid_level.volume, 1.0)
                
                iceberg = IcebergOrder(
                    side=OrderSide.BUY,
                    price=bid_level.price,
                    visible_volume=bid_level.volume,
                    hidden_volume=hidden_volume,
                    confidence=confidence,
                    timestamp=orderbook.timestamp,
                    exchange=orderbook.exchange,
                    symbol=orderbook.symbol,
                    detection_method=DetectionMethod.TRADE_FLOW_ANALYSIS
                )
                icebergs.append(iceberg)
        
        # Analyze asks (sell side)
        for ask_level in orderbook.asks[:20]:
            nearby_trades = self._get_trades_near_price(
                trades,
                ask_level.price,
                OrderSide.SELL,
                tolerance=0.001
            )
            
            total_trade_volume = sum(t.amount for t in nearby_trades)
            
            if total_trade_volume > ask_level.volume * (1 + self.threshold):
                hidden_volume = total_trade_volume - ask_level.volume
                confidence = min(hidden_volume / ask_level.volume, 1.0)
                
                iceberg = IcebergOrder(
                    side=OrderSide.SELL,
                    price=ask_level.price,
                    visible_volume=ask_level.volume,
                    hidden_volume=hidden_volume,
                    confidence=confidence,
                    timestamp=orderbook.timestamp,
                    exchange=orderbook.exchange,
                    symbol=orderbook.symbol,
                    detection_method=DetectionMethod.TRADE_FLOW_ANALYSIS
                )
                icebergs.append(iceberg)
        
        return icebergs
    
    async def _detect_via_refill_pattern(
        self,
        orderbook: OrderBookSnapshot
    ) -> List[IcebergOrder]:
        """
        Detect icebergs by identifying order refill patterns
        
        Logic: Orders that are repeatedly filled and refilled at same price
        level indicate iceberg behavior
        """
        icebergs = []
        
        if len(self.orderbook_history) < 5:
            return icebergs
        
        # Track volume changes at price levels
        refill_patterns = self._identify_refill_patterns()
        
        for pattern in refill_patterns:
            if pattern.refill_count >= 3 and pattern.confidence > 0.5:
                # Estimate hidden volume based on refill pattern
                hidden_volume = pattern.total_volume_refilled * 0.7  # Conservative estimate
                
                # Find current visible volume
                visible_volume = self._get_volume_at_price(
                    orderbook,
                    pattern.price_level,
                    pattern.side
                )
                
                if visible_volume > 0:
                    iceberg = IcebergOrder(
                        side=pattern.side,
                        price=pattern.price_level,
                        visible_volume=visible_volume,
                        hidden_volume=hidden_volume,
                        confidence=pattern.confidence,
                        timestamp=orderbook.timestamp,
                        exchange=orderbook.exchange,
                        symbol=orderbook.symbol,
                        detection_method=DetectionMethod.ORDER_REFILL_PATTERN
                    )
                    icebergs.append(iceberg)
        
        return icebergs
    
    async def _detect_via_volume_anomaly(
        self,
        orderbook: OrderBookSnapshot,
        trades: List[TradeEvent]
    ) -> List[IcebergOrder]:
        """
        Detect icebergs by identifying volume anomalies
        
        Logic: Unusual volume patterns compared to historical average
        """
        icebergs = []
        
        if len(self.trade_history) < 20:
            return icebergs
        
        # Calculate average volumes
        recent_trades = list(self.trade_history)[-50:]
        
        buy_volumes = [t.amount for t in recent_trades if t.side == OrderSide.BUY]
        sell_volumes = [t.amount for t in recent_trades if t.side == OrderSide.SELL]
        
        avg_buy_volume = np.mean(buy_volumes) if buy_volumes else 0
        avg_sell_volume = np.mean(sell_volumes) if sell_volumes else 0
        std_buy_volume = np.std(buy_volumes) if buy_volumes else 0
        std_sell_volume = np.std(sell_volumes) if sell_volumes else 0
        
        # Check for anomalies in recent trades
        for trade in trades[-10:]:  # Last 10 trades
            if trade.side == OrderSide.BUY:
                if trade.amount > avg_buy_volume + 2 * std_buy_volume:
                    # Anomalously large buy trade
                    visible_vol = self._get_volume_at_price(orderbook, trade.price, OrderSide.BUY)
                    
                    if visible_vol < trade.amount:
                        iceberg = IcebergOrder(
                            side=OrderSide.BUY,
                            price=trade.price,
                            visible_volume=visible_vol,
                            hidden_volume=trade.amount - visible_vol,
                            confidence=0.7,
                            timestamp=trade.timestamp,
                            exchange=orderbook.exchange,
                            symbol=orderbook.symbol,
                            detection_method=DetectionMethod.VOLUME_ANOMALY
                        )
                        icebergs.append(iceberg)
            
            else:  # SELL
                if trade.amount > avg_sell_volume + 2 * std_sell_volume:
                    visible_vol = self._get_volume_at_price(orderbook, trade.price, OrderSide.SELL)
                    
                    if visible_vol < trade.amount:
                        iceberg = IcebergOrder(
                            side=OrderSide.SELL,
                            price=trade.price,
                            visible_volume=visible_vol,
                            hidden_volume=trade.amount - visible_vol,
                            confidence=0.7,
                            timestamp=trade.timestamp,
                            exchange=orderbook.exchange,
                            symbol=orderbook.symbol,
                            detection_method=DetectionMethod.VOLUME_ANOMALY
                        )
                        icebergs.append(iceberg)
        
        return icebergs
    
    def _merge_detections(self, *detection_lists) -> List[IcebergOrder]:
        """Merge and deduplicate iceberg detections"""
        all_icebergs = []
        for detections in detection_lists:
            all_icebergs.extend(detections)
        
        # Deduplicate by price and side
        unique_icebergs = {}
        for iceberg in all_icebergs:
            key = (iceberg.price, iceberg.side)
            
            if key not in unique_icebergs:
                unique_icebergs[key] = iceberg
            else:
                # Keep the one with higher confidence
                if iceberg.confidence > unique_icebergs[key].confidence:
                    unique_icebergs[key] = iceberg
        
        return list(unique_icebergs.values())
    
    def _create_timeline(
        self,
        icebergs: List[IcebergOrder],
        trades: List[TradeEvent]
    ) -> List[Dict]:
        """Create timeline of detections"""
        timeline = []
        
        for iceberg in icebergs:
            timeline.append({
                'side': iceberg.side.value,
                'volume': iceberg.total_volume,
                'timestamp': iceberg.timestamp.isoformat(),
                'price': iceberg.price
            })
        
        return sorted(timeline, key=lambda x: x['timestamp'])
    
    def _get_trades_near_price(
        self,
        trades: List[TradeEvent],
        price: float,
        side: OrderSide,
        tolerance: float = 0.001
    ) -> List[TradeEvent]:
        """Get trades near a specific price level"""
        nearby_trades = []
        
        for trade in trades:
            if trade.side == side:
                price_diff = abs(trade.price - price)
                if price_diff <= price * tolerance:
                    nearby_trades.append(trade)
        
        return nearby_trades
    
    def _get_volume_at_price(
        self,
        orderbook: OrderBookSnapshot,
        price: float,
        side: OrderSide,
        tolerance: float = 0.001
    ) -> float:
        """Get visible volume at a price level"""
        levels = orderbook.bids if side == OrderSide.BUY else orderbook.asks
        
        for level in levels:
            if abs(level.price - price) <= price * tolerance:
                return level.volume
        
        return 0.0
    
    def _identify_refill_patterns(self) -> List[RefillPattern]:
        """Identify order refill patterns from history"""
        patterns = []
        
        # Track volume changes at specific price levels
        price_volumes = defaultdict(list)
        
        for snapshot in self.orderbook_history:
            for bid in snapshot.bids[:10]:
                key = round(bid.price, 2)
                price_volumes[(key, 'buy')].append((snapshot.timestamp, bid.volume))
            
            for ask in snapshot.asks[:10]:
                key = round(ask.price, 2)
                price_volumes[(key, 'sell')].append((snapshot.timestamp, ask.volume))
        
        # Detect refill patterns
        for (price, side_str), history in price_volumes.items():
            if len(history) < 3:
                continue
            
            refills = 0
            total_refilled = 0
            intervals = []
            
            for i in range(1, len(history)):
                prev_time, prev_vol = history[i-1]
                curr_time, curr_vol = history[i]
                
                # Refill detected if volume increased significantly
                if curr_vol > prev_vol * 1.2:
                    refills += 1
                    total_refilled += (curr_vol - prev_vol)
                    intervals.append((curr_time - prev_time).total_seconds())
            
            if refills >= 2:
                pattern = RefillPattern(
                    price_level=price,
                    refill_count=refills,
                    avg_refill_interval=np.mean(intervals) if intervals else 0,
                    total_volume_refilled=total_refilled,
                    side=OrderSide.BUY if side_str == 'buy' else OrderSide.SELL,
                    confidence=min(refills / 5, 1.0)
                )
                patterns.append(pattern)
        
        return patterns
    
    def _parse_orderbook(
        self,
        raw_orderbook: Dict,
        exchange: str,
        symbol: str
    ) -> OrderBookSnapshot:
        """Parse raw orderbook to model"""
        bids = [
            OrderBookLevel(price=float(p), volume=float(v))
            for p, v in raw_orderbook.get('bids', [])
        ]
        asks = [
            OrderBookLevel(price=float(p), volume=float(v))
            for p, v in raw_orderbook.get('asks', [])
        ]
        
        return OrderBookSnapshot(
            bids=bids,
            asks=asks,
            timestamp=datetime.fromtimestamp(raw_orderbook.get('timestamp', 0) / 1000),
            symbol=symbol,
            exchange=exchange
        )
    
    def _parse_trades(self, raw_trades: List[Dict]) -> List[TradeEvent]:
        """Parse raw trades to model"""
        trades = []
        
        for trade in raw_trades:
            trades.append(TradeEvent(
                price=float(trade.get('price', 0)),
                amount=float(trade.get('amount', 0)),
                side=OrderSide.BUY if trade.get('side') == 'buy' else OrderSide.SELL,
                timestamp=datetime.fromtimestamp(trade.get('timestamp', 0) / 1000),
                trade_id=str(trade.get('id', ''))
            ))
        
        return trades
