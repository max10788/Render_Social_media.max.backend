"""
IMPROVED Iceberg Order Detector

FIXES & IMPROVEMENTS:
- ✅ Dynamic price tolerance based on spread
- ✅ Improved statistical validation (checks for normality)
- ✅ Trading session awareness
- ✅ Uses maker_side from trades for better accuracy
- ✅ Enhanced confidence scoring
- ✅ Larger lookback window
"""
import asyncio
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict, deque
from scipy import stats


class OrderSide:
    BUY = "buy"
    SELL = "sell"


class DetectionMethod:
    TRADE_FLOW_ANALYSIS = "trade_flow_analysis"
    ORDER_REFILL_PATTERN = "order_refill_pattern"
    VOLUME_ANOMALY = "volume_anomaly"
    TIME_SERIES_ANALYSIS = "time_series_analysis"
    HYBRID = "hybrid"


class IcebergDetector:
    """Improved iceberg order detector"""
    
    def __init__(self, threshold: float = 0.05, lookback_window: int = 200):
        """
        Initialize improved detector
        
        Args:
            threshold: Detection threshold (default 5%)
            lookback_window: Increased to 200 for better statistics
        """
        self.threshold = threshold
        self.lookback_window = lookback_window
        
        # Historical data storage (larger)
        self.orderbook_history = deque(maxlen=lookback_window)
        self.trade_history = deque(maxlen=lookback_window * 3)  # More trade history
        
        # Detection state
        self.price_level_volumes = defaultdict(list)
        self.refill_timestamps = defaultdict(list)
        
        # OPTIMIZED BALANCE:
        self.min_confidence = 0.4  # ← Statt 0.3 (filtert Noise, behält kleine Icebergs)
        self.min_trades_for_stats = 25  # ← Statt 30 (mehr Flexibilität)
        
        # NEU: Größen-basierte Confidence-Anpassung
        self.small_iceberg_boost = 0.05  # Bonus für kleinere Icebergs
        self.large_iceberg_threshold = 0.7  # Strengere Kriterien für große
        
    def get_dynamic_tolerance(self, orderbook: Dict) -> float:
        """
        Calculate dynamic price tolerance based on market conditions
        
        IMPROVEMENT: Adapts to volatility instead of fixed 0.1%
        """
        try:
            if not orderbook.get('bids') or not orderbook.get('asks'):
                return 0.001  # Fallback to 0.1%
            
            # Get best bid/ask
            best_bid = orderbook['bids'][0]['price']
            best_ask = orderbook['asks'][0]['price']
            
            # Calculate spread percentage
            spread = best_ask - best_bid
            spread_pct = (spread / best_bid) * 100
            
            # Tolerance is 50% of spread, but at least 0.05% and max 0.5%
            tolerance = max(0.0005, min(spread_pct * 0.005, 0.005))
            
            return tolerance
            
        except Exception as e:
            return 0.001  # Fallback
    
    def is_active_trading_session(self, timestamp: datetime) -> bool:
        """
        Check if timestamp is during active trading session
        
        IMPROVEMENT: Accounts for trading hours
        Higher confidence during active hours
        """
        hour_utc = timestamp.hour
        
        # Major trading sessions (UTC):
        # Asian: 00:00 - 09:00
        # European: 07:00 - 16:00  
        # US: 13:00 - 22:00
        
        # Active hours: 07:00 - 22:00 UTC (overlap of EU + US)
        is_active = 7 <= hour_utc <= 22
        
        # Weekday check
        is_weekday = timestamp.weekday() < 5
        
        return is_active and is_weekday
    
    async def detect(
        self,
        orderbook: Dict,
        trades: List[Dict],
        exchange: str,
        symbol: str
    ) -> Dict:
        """
        Main detection method with improvements
        """
        # Store history
        self.orderbook_history.append(orderbook)
        self.trade_history.extend(trades)
        
        # Get dynamic tolerance
        tolerance = self.get_dynamic_tolerance(orderbook)
        
        # Run improved detection algorithms
        trade_flow_icebergs = await self._detect_via_trade_flow_improved(
            orderbook, trades, tolerance
        )
        
        refill_icebergs = await self._detect_via_refill_pattern_improved(
            orderbook, tolerance
        )
        
        volume_anomaly_icebergs = await self._detect_via_volume_anomaly_improved(
            orderbook, trades, tolerance
        )
        
        # Merge and deduplicate
        all_icebergs = self._merge_detections(
            trade_flow_icebergs,
            refill_icebergs,
            volume_anomaly_icebergs
        )
        
        # Filter by minimum confidence
        all_icebergs = [i for i in all_icebergs if i['confidence'] >= self.min_confidence]
        
        # Create timeline
        timeline = self._create_timeline(all_icebergs, trades)
        
        # Enhanced metadata
        metadata = {
            'exchange': exchange,
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'detectionThreshold': self.threshold,
            'dynamicTolerance': tolerance,
            'algorithmsUsed': ['trade_flow', 'refill_pattern', 'volume_anomaly'],
            'lookbackWindow': self.lookback_window,
            'tradesAnalyzed': len(trades),
            'orderbookDepth': len(orderbook.get('bids', [])) + len(orderbook.get('asks', []))
        }
        
        # Calculate statistics
        statistics = self._calculate_statistics(all_icebergs)
        
        return {
            'icebergs': all_icebergs,
            'timeline': timeline,
            'metadata': metadata,
            'statistics': statistics
        }
    
    async def _detect_via_trade_flow_improved(
        self,
        orderbook: Dict,
        trades: List[Dict],
        tolerance: float
    ) -> List[Dict]:
        """
        IMPROVED: Uses maker_side and dynamic tolerance
        """
        icebergs = []
        
        # Analyze bids (potential buy icebergs)
        for bid in orderbook.get('bids', [])[:30]:  # Top 30 levels
            bid_price = bid['price']
            bid_volume = bid['volume']
            
            # Get trades that HIT this bid (maker_side = buy)
            nearby_trades = self._get_trades_near_price_improved(
                trades,
                bid_price,
                maker_side='buy',  # Using maker side now!
                tolerance=tolerance
            )
            
            if not nearby_trades:
                continue
            
            total_trade_volume = sum(t['amount'] for t in nearby_trades)
            
            # Detection: trade volume significantly exceeds visible volume
            if total_trade_volume > bid_volume * (1 + self.threshold):
                hidden_volume = total_trade_volume - bid_volume
                
                # Enhanced confidence calculation
                volume_ratio = total_trade_volume / bid_volume if bid_volume > 0 else 0
                confidence = min(0.4 + (volume_ratio - 1) * 0.3, 0.95)
                
                # Session bonus
                if nearby_trades and self.is_active_trading_session(
                    datetime.fromtimestamp(nearby_trades[0]['timestamp'] / 1000)
                ):
                    confidence *= 1.1
                
                confidence = min(confidence, 1.0)
                
                iceberg = {
                    'side': 'buy',
                    'price': bid_price,
                    'visible_volume': bid_volume,
                    'hidden_volume': hidden_volume,
                    'total_volume': bid_volume + hidden_volume,
                    'confidence': confidence,
                    'timestamp': datetime.now().isoformat(),
                    'exchange': orderbook.get('exchange', ''),
                    'symbol': orderbook.get('symbol', ''),
                    'detection_method': DetectionMethod.TRADE_FLOW_ANALYSIS,
                    'supporting_trades': len(nearby_trades)
                }
                icebergs.append(iceberg)
        
        # Analyze asks (potential sell icebergs)
        for ask in orderbook.get('asks', [])[:30]:
            ask_price = ask['price']
            ask_volume = ask['volume']
            
            # Get trades that HIT this ask (maker_side = sell)
            nearby_trades = self._get_trades_near_price_improved(
                trades,
                ask_price,
                maker_side='sell',
                tolerance=tolerance
            )
            
            if not nearby_trades:
                continue
            
            total_trade_volume = sum(t['amount'] for t in nearby_trades)
            
            if total_trade_volume > ask_volume * (1 + self.threshold):
                hidden_volume = total_trade_volume - ask_volume
                
                volume_ratio = total_trade_volume / ask_volume if ask_volume > 0 else 0
                confidence = min(0.4 + (volume_ratio - 1) * 0.3, 0.95)
                
                if nearby_trades and self.is_active_trading_session(
                    datetime.fromtimestamp(nearby_trades[0]['timestamp'] / 1000)
                ):
                    confidence *= 1.1
                
                confidence = min(confidence, 1.0)
                
                iceberg = {
                    'side': 'sell',
                    'price': ask_price,
                    'visible_volume': ask_volume,
                    'hidden_volume': hidden_volume,
                    'total_volume': ask_volume + hidden_volume,
                    'confidence': confidence,
                    'timestamp': datetime.now().isoformat(),
                    'exchange': orderbook.get('exchange', ''),
                    'symbol': orderbook.get('symbol', ''),
                    'detection_method': DetectionMethod.TRADE_FLOW_ANALYSIS,
                    'supporting_trades': len(nearby_trades)
                }
                icebergs.append(iceberg)
        
        return icebergs
    
    async def _detect_via_refill_pattern_improved(
        self,
        orderbook: Dict,
        tolerance: float
    ) -> List[Dict]:
        """
        IMPROVED: Better refill pattern detection
        """
        icebergs = []
        
        if len(self.orderbook_history) < 10:  # Need more history
            return icebergs
        
        # Identify refill patterns
        refill_patterns = self._identify_refill_patterns_improved()
        
        for pattern in refill_patterns:
            if pattern['refill_count'] >= 3 and pattern['confidence'] > 0.4:
                # Estimate hidden volume
                hidden_volume = pattern['total_volume_refilled'] * 0.8
                
                # Find current visible volume
                visible_volume = self._get_volume_at_price(
                    orderbook,
                    pattern['price_level'],
                    pattern['side'],
                    tolerance
                )
                
                if visible_volume > 0:
                    # Enhanced confidence based on consistency
                    confidence = pattern['confidence']
                    
                    # Bonus for consistent refill intervals
                    if pattern['interval_std'] < pattern['avg_refill_interval'] * 0.3:
                        confidence *= 1.15
                    
                    confidence = min(confidence, 0.95)
                    
                    iceberg = {
                        'side': pattern['side'],
                        'price': pattern['price_level'],
                        'visible_volume': visible_volume,
                        'hidden_volume': hidden_volume,
                        'total_volume': visible_volume + hidden_volume,
                        'confidence': confidence,
                        'timestamp': datetime.now().isoformat(),
                        'exchange': orderbook.get('exchange', ''),
                        'symbol': orderbook.get('symbol', ''),
                        'detection_method': DetectionMethod.ORDER_REFILL_PATTERN,
                        'refill_count': pattern['refill_count']
                    }
                    icebergs.append(iceberg)
        
        return icebergs
    
    async def _detect_via_volume_anomaly_improved(
        self,
        orderbook: Dict,
        trades: List[Dict],
        tolerance: float
    ) -> List[Dict]:
        """
        IMPROVED: Statistical validation with normality check
        """
        icebergs = []
        
        if len(self.trade_history) < self.min_trades_for_stats:
            return icebergs
        
        # Separate by maker side
        recent_trades = list(self.trade_history)[-100:]
        
        buy_maker_volumes = [t['amount'] for t in recent_trades if t.get('maker_side') == 'buy']
        sell_maker_volumes = [t['amount'] for t in recent_trades if t.get('maker_side') == 'sell']
        
        # Calculate statistics with validation
        if len(buy_maker_volumes) >= 20:
            buy_stats = self._calculate_robust_statistics(buy_maker_volumes)
        else:
            buy_stats = None
        
        if len(sell_maker_volumes) >= 20:
            sell_stats = self._calculate_robust_statistics(sell_maker_volumes)
        else:
            sell_stats = None
        
        # Check recent trades for anomalies
        for trade in trades[-20:]:
            maker_side = trade.get('maker_side')
            
            if maker_side == 'buy' and buy_stats:
                if trade['amount'] > buy_stats['mean'] + 2.5 * buy_stats['std']:
                    # Anomalously large buy
                    visible_vol = self._get_volume_at_price(
                        orderbook, trade['price'], 'buy', tolerance
                    )
                    
                    if visible_vol < trade['amount']:
                        # Calculate z-score for confidence
                        z_score = (trade['amount'] - buy_stats['mean']) / buy_stats['std']
                        confidence = min(0.5 + (z_score - 2.5) * 0.1, 0.85)
                        
                        iceberg = {
                            'side': 'buy',
                            'price': trade['price'],
                            'visible_volume': visible_vol,
                            'hidden_volume': trade['amount'] - visible_vol,
                            'total_volume': trade['amount'],
                            'confidence': confidence,
                            'timestamp': datetime.fromtimestamp(trade['timestamp'] / 1000).isoformat(),
                            'exchange': orderbook.get('exchange', ''),
                            'symbol': orderbook.get('symbol', ''),
                            'detection_method': DetectionMethod.VOLUME_ANOMALY,
                            'z_score': z_score
                        }
                        icebergs.append(iceberg)
            
            elif maker_side == 'sell' and sell_stats:
                if trade['amount'] > sell_stats['mean'] + 2.5 * sell_stats['std']:
                    visible_vol = self._get_volume_at_price(
                        orderbook, trade['price'], 'sell', tolerance
                    )
                    
                    if visible_vol < trade['amount']:
                        if sell_stats['std'] > 0:
                            z_score = (trade['amount'] - sell_stats['mean']) / sell_stats['std']
                        else:
                            z_score = 0  # Fallback wenn keine Standardabweichung
                        confidence = min(0.5 + (z_score - 2.5) * 0.1, 0.85)
                        
                        iceberg = {
                            'side': 'sell',
                            'price': trade['price'],
                            'visible_volume': visible_vol,
                            'hidden_volume': trade['amount'] - visible_vol,
                            'total_volume': trade['amount'],
                            'confidence': confidence,
                            'timestamp': datetime.fromtimestamp(trade['timestamp'] / 1000).isoformat(),
                            'exchange': orderbook.get('exchange', ''),
                            'symbol': orderbook.get('symbol', ''),
                            'detection_method': DetectionMethod.VOLUME_ANOMALY,
                            'z_score': z_score
                        }
                        icebergs.append(iceberg)
        
        return icebergs
    
    def _calculate_robust_statistics(self, volumes: List[float]) -> Dict:
        """
        Calculate robust statistics with outlier detection
        """
        if not volumes:
            return {'mean': 0, 'std': 0, 'median': 0, 'is_normal': False}
        
        arr = np.array(volumes)
        
        # Basic stats
        mean = np.mean(arr)
        std = np.std(arr)
        median = np.median(arr)
        
        # Check for normality (Shapiro-Wilk test)
        is_normal = False
        if len(arr) >= 8:
            try:
                _, p_value = stats.shapiro(arr)
                is_normal = p_value > 0.05  # Not significantly non-normal
            except:
                pass
        
        # If not normal, use robust estimators
        if not is_normal and len(arr) >= 10:
            # Use median and MAD (Median Absolute Deviation)
            mad = np.median(np.abs(arr - median))
            std = mad * 1.4826  # MAD to std conversion factor
        
        return {
            'mean': mean,
            'std': std,
            'median': median,
            'is_normal': is_normal,
            'count': len(arr)
        }
    
    def _get_trades_near_price_improved(
        self,
        trades: List[Dict],
        price: float,
        maker_side: str,
        tolerance: float
    ) -> List[Dict]:
        """IMPROVED: Uses maker_side instead of taker side"""
        nearby_trades = []
        
        for trade in trades:
            if trade.get('maker_side') == maker_side:
                price_diff = abs(trade['price'] - price)
                if price_diff <= price * tolerance:
                    nearby_trades.append(trade)
        
        return nearby_trades
    
    def _get_volume_at_price(
        self,
        orderbook: Dict,
        price: float,
        side: str,
        tolerance: float
    ) -> float:
        """Get visible volume at price level"""
        levels = orderbook.get('bids', []) if side == 'buy' else orderbook.get('asks', [])
        
        for level in levels:
            if abs(level['price'] - price) <= price * tolerance:
                return level['volume']
        
        return 0.0
    
    def _identify_refill_patterns_improved(self) -> List[Dict]:
        """IMPROVED: Better refill detection with timing analysis"""
        patterns = []
        
        price_volumes = defaultdict(list)
        
        # Track volume changes
        for snapshot in self.orderbook_history:
            timestamp = snapshot.get('timestamp', 0)
            if isinstance(timestamp, int):
                ts = datetime.fromtimestamp(timestamp / 1000)
            else:
                ts = datetime.now()
            
            for bid in snapshot.get('bids', [])[:15]:
                key = (round(bid['price'], 4), 'buy')
                price_volumes[key].append((ts, bid['volume']))
            
            for ask in snapshot.get('asks', [])[:15]:
                key = (round(ask['price'], 4), 'sell')
                price_volumes[key].append((ts, ask['volume']))
        
        # Detect refills
        for (price, side), history in price_volumes.items():
            if len(history) < 5:
                continue
            
            refills = []
            intervals = []
            volumes_refilled = []
            
            for i in range(1, len(history)):
                prev_time, prev_vol = history[i-1]
                curr_time, curr_vol = history[i]
                
                # Refill detected
                if curr_vol > prev_vol * 1.15:  # 15% increase
                    refills.append(i)
                    volumes_refilled.append(curr_vol - prev_vol)
                    interval = (curr_time - prev_time).total_seconds()
                    if interval > 0:
                        intervals.append(interval)
            
            if len(refills) >= 3:
                avg_interval = np.mean(intervals) if intervals else 0
                interval_std = np.std(intervals) if len(intervals) > 1 else 0
                total_refilled = sum(volumes_refilled)
                
                # Confidence based on consistency
                confidence = min(len(refills) / 8, 0.9)
                
                pattern = {
                    'price_level': price,
                    'side': side,
                    'refill_count': len(refills),
                    'avg_refill_interval': avg_interval,
                    'interval_std': interval_std,
                    'total_volume_refilled': total_refilled,
                    'confidence': confidence
                }
                patterns.append(pattern)
        
        return patterns
    
    def _merge_detections(self, *detection_lists) -> List[Dict]:
        """Merge with improved deduplication"""
        all_icebergs = []
        for detections in detection_lists:
            all_icebergs.extend(detections)
        
        # Group by price and side
        unique_icebergs = {}
        for iceberg in all_icebergs:
            # Round price to avoid floating point issues
            key = (round(iceberg['price'], 4), iceberg['side'])
            
            if key not in unique_icebergs:
                unique_icebergs[key] = iceberg
            else:
                # Keep highest confidence, but average the volumes
                existing = unique_icebergs[key]
                if iceberg['confidence'] > existing['confidence']:
                    # Update but keep track of multiple detections
                    iceberg['detection_methods'] = [
                        existing.get('detection_method'),
                        iceberg.get('detection_method')
                    ]
                    iceberg['confidence'] = min(iceberg['confidence'] * 1.2, 0.98)  # Bonus for multiple methods
                    unique_icebergs[key] = iceberg
        
        return list(unique_icebergs.values())
    
    def _create_timeline(self, icebergs: List[Dict], trades: List[Dict]) -> List[Dict]:
        """Create detection timeline"""
        timeline = []
        
        for iceberg in icebergs:
            timeline.append({
                'side': iceberg['side'],
                'volume': iceberg['total_volume'],
                'timestamp': iceberg['timestamp'],
                'price': iceberg['price'],
                'confidence': iceberg['confidence']
            })
        
        return sorted(timeline, key=lambda x: x['timestamp'])
    
    def _calculate_statistics(self, icebergs: List[Dict]) -> Dict:
        """Calculate enhanced statistics"""
        if not icebergs:
            return {
                'totalDetected': 0,
                'buyOrders': 0,
                'sellOrders': 0,
                'totalHiddenVolume': 0,
                'averageConfidence': 0
            }
        
        buy_icebergs = [i for i in icebergs if i['side'] == 'buy']
        sell_icebergs = [i for i in icebergs if i['side'] == 'sell']
        
        total_hidden = sum(i['hidden_volume'] for i in icebergs)
        avg_confidence = sum(i['confidence'] for i in icebergs) / len(icebergs)
        
        largest = max(icebergs, key=lambda x: x['hidden_volume'])
        
        # Method distribution
        methods = {}
        for iceberg in icebergs:
            method = iceberg.get('detection_method', 'unknown')
            methods[method] = methods.get(method, 0) + 1
        
        return {
            'totalDetected': len(icebergs),
            'buyOrders': len(buy_icebergs),
            'sellOrders': len(sell_icebergs),
            'totalHiddenVolume': total_hidden,
            'averageConfidence': avg_confidence,
            'highConfidenceDetections': len([i for i in icebergs if i['confidence'] > 0.7]),
            'largestIceberg': {
                'side': largest['side'],
                'price': largest['price'],
                'hiddenVolume': largest['hidden_volume'],
                'confidence': largest['confidence']
            },
            'detectionMethods': methods
        }
