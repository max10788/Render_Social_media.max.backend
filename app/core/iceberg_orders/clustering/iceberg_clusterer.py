"""
Iceberg Order Clustering System
Groups related iceberg detections (refills) into parent orders
"""
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
from dataclasses import dataclass, field


@dataclass
class ParentIcebergOrder:
    """Represents a parent iceberg order composed of multiple refills"""
    
    # Identification
    id: str
    side: str  # 'buy' or 'sell'
    
    # Price information
    avg_price: float
    price_min: float
    price_max: float
    price_std: float
    
    # Volume information
    total_volume: float
    total_visible_volume: float
    total_hidden_volume: float
    avg_refill_size: float
    refill_size_std: float
    
    # Refill tracking
    refill_count: int
    
    # Time information
    first_seen: datetime
    last_seen: datetime
    duration_seconds: float
    avg_refill_interval: float
    refill_interval_std: float
    
    # Confidence metrics
    overall_confidence: float
    consistency_score: float
    
    # Detection metadata
    exchange: str
    symbol: str
    
    # Fields with defaults MUST come last
    refills: List[Dict] = field(default_factory=list)
    detection_methods: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'id': self.id,
            'side': self.side,
            'price': {
                'avg': self.avg_price,
                'min': self.price_min,
                'max': self.price_max,
                'std': self.price_std,
                'range_percent': ((self.price_max - self.price_min) / self.avg_price * 100) if self.avg_price > 0 else 0
            },
            'volume': {
                'total': self.total_volume,
                'visible': self.total_visible_volume,
                'hidden': self.total_hidden_volume,
                'hidden_ratio': self.total_hidden_volume / self.total_visible_volume if self.total_visible_volume > 0 else 0,
                'avg_refill_size': self.avg_refill_size,
                'refill_size_std': self.refill_size_std,
                'refill_size_consistency': 1 - (self.refill_size_std / self.avg_refill_size) if self.avg_refill_size > 0 else 0
            },
            'refills': {
                'count': self.refill_count,
                'details': self.refills
            },
            'timing': {
                'first_seen': self.first_seen.isoformat(),
                'last_seen': self.last_seen.isoformat(),
                'duration_seconds': self.duration_seconds,
                'duration_minutes': self.duration_seconds / 60,
                'avg_interval_seconds': self.avg_refill_interval,
                'interval_std': self.refill_interval_std,
                'interval_consistency': 1 - (self.refill_interval_std / self.avg_refill_interval) if self.avg_refill_interval > 0 else 0
            },
            'confidence': {
                'overall': self.overall_confidence,
                'consistency_score': self.consistency_score
            },
            'metadata': {
                'exchange': self.exchange,
                'symbol': self.symbol,
                'detection_methods': self.detection_methods
            }
        }


class IcebergClusterer:
    """
    Clusters individual iceberg detections into parent orders
    
    Clustering based on:
    - Temporal proximity (time window)
    - Price similarity (tolerance)
    - Side consistency (buy/sell)
    - Volume consistency (similar refill sizes)
    """
    
    def __init__(
        self,
        time_window_seconds: int = 300,  # 5 minutes
        price_tolerance_percent: float = 0.1,  # 0.1%
        volume_tolerance_percent: float = 50,  # 50%
        min_refills: int = 3,  # Minimum refills to be considered parent order
        min_consistency_score: float = 0.5
    ):
        self.time_window_seconds = time_window_seconds
        self.price_tolerance_percent = price_tolerance_percent / 100
        self.volume_tolerance_percent = volume_tolerance_percent / 100
        self.min_refills = min_refills
        self.min_consistency_score = min_consistency_score
        
        self._cluster_id_counter = 0
    
    def cluster(self, icebergs: List[Dict]) -> Dict[str, List]:
        """
        Main clustering method
        
        Returns:
            {
                'parent_orders': List[ParentIcebergOrder],
                'individual_icebergs': List[Dict],  # Unclustered
                'clustering_stats': Dict
            }
        """
        if not icebergs:
            return {
                'parent_orders': [],
                'individual_icebergs': [],
                'clustering_stats': {
                    'total_input_icebergs': 0,
                    'parent_orders_found': 0,
                    'clustered_icebergs': 0,
                    'unclustered_icebergs': 0,
                    'clustering_rate': 0
                }
            }
        
        # Sort by timestamp
        sorted_icebergs = sorted(
            icebergs,
            key=lambda x: self._parse_timestamp(x.get('timestamp'))
        )
        
        # Group by side first (buy/sell)
        buy_icebergs = [i for i in sorted_icebergs if i['side'] == 'buy']
        sell_icebergs = [i for i in sorted_icebergs if i['side'] == 'sell']
        
        # Cluster each side separately
        buy_clusters = self._cluster_by_side(buy_icebergs, 'buy')
        sell_clusters = self._cluster_by_side(sell_icebergs, 'sell')
        
        all_clusters = buy_clusters + sell_clusters
        
        # Create parent orders from clusters
        parent_orders = []
        clustered_iceberg_ids = set()
        
        for cluster in all_clusters:
            if len(cluster) >= self.min_refills:
                parent_order = self._create_parent_order(cluster)
                
                # Check consistency
                if parent_order.consistency_score >= self.min_consistency_score:
                    parent_orders.append(parent_order)
                    clustered_iceberg_ids.update(
                        self._get_iceberg_id(iceberg) for iceberg in cluster
                    )
        
        # Unclustered icebergs
        individual_icebergs = [
            iceberg for iceberg in sorted_icebergs
            if self._get_iceberg_id(iceberg) not in clustered_iceberg_ids
        ]
        
        # Calculate statistics
        clustering_stats = {
            'total_input_icebergs': len(icebergs),
            'parent_orders_found': len(parent_orders),
            'clustered_icebergs': sum(p.refill_count for p in parent_orders),
            'unclustered_icebergs': len(individual_icebergs),
            'clustering_rate': (sum(p.refill_count for p in parent_orders) / len(icebergs) * 100) if icebergs else 0,
            'avg_refills_per_parent': (sum(p.refill_count for p in parent_orders) / len(parent_orders)) if parent_orders else 0
        }
        
        return {
            'parent_orders': [p.to_dict() for p in parent_orders],
            'individual_icebergs': individual_icebergs,
            'clustering_stats': clustering_stats
        }
    
    def _cluster_by_side(self, icebergs: List[Dict], side: str) -> List[List[Dict]]:
        """Cluster icebergs of the same side"""
        if not icebergs:
            return []
        
        clusters = []
        current_cluster = [icebergs[0]]
        
        for i in range(1, len(icebergs)):
            current_iceberg = icebergs[i]
            last_in_cluster = current_cluster[-1]
            
            if self._should_merge(last_in_cluster, current_iceberg):
                current_cluster.append(current_iceberg)
            else:
                # Check if we should merge with entire cluster
                if self._should_merge_with_cluster(current_cluster, current_iceberg):
                    current_cluster.append(current_iceberg)
                else:
                    # Start new cluster
                    if len(current_cluster) >= 2:  # Keep clusters with at least 2
                        clusters.append(current_cluster)
                    current_cluster = [current_iceberg]
        
        # Add last cluster
        if len(current_cluster) >= 2:
            clusters.append(current_cluster)
        
        return clusters
    
    def _should_merge(self, iceberg1: Dict, iceberg2: Dict) -> bool:
        """Check if two icebergs should be merged"""
        # Time check
        time1 = self._parse_timestamp(iceberg1.get('timestamp'))
        time2 = self._parse_timestamp(iceberg2.get('timestamp'))
        time_diff = abs((time2 - time1).total_seconds())
        
        if time_diff > self.time_window_seconds:
            return False
        
        # Price check
        price1 = iceberg1.get('price', 0)
        price2 = iceberg2.get('price', 0)
        price_diff = abs(price2 - price1) / price1 if price1 > 0 else 999
        
        if price_diff > self.price_tolerance_percent:
            return False
        
        # Volume consistency check
        vol1 = iceberg1.get('hidden_volume', 0) or iceberg1.get('total_volume', 0)
        vol2 = iceberg2.get('hidden_volume', 0) or iceberg2.get('total_volume', 0)
        
        if vol1 > 0 and vol2 > 0:
            vol_ratio = max(vol1, vol2) / min(vol1, vol2)
            if vol_ratio > (1 + self.volume_tolerance_percent):
                return False
        
        return True
    
    def _should_merge_with_cluster(self, cluster: List[Dict], iceberg: Dict) -> bool:
        """Check if iceberg should be merged with entire cluster"""
        # Check against cluster average
        avg_price = np.mean([i.get('price', 0) for i in cluster])
        avg_volume = np.mean([i.get('hidden_volume', 0) or i.get('total_volume', 0) for i in cluster])
        
        iceberg_price = iceberg.get('price', 0)
        iceberg_volume = iceberg.get('hidden_volume', 0) or iceberg.get('total_volume', 0)
        
        # Price check
        price_diff = abs(iceberg_price - avg_price) / avg_price if avg_price > 0 else 999
        if price_diff > self.price_tolerance_percent:
            return False
        
        # Volume check
        if avg_volume > 0 and iceberg_volume > 0:
            vol_ratio = max(avg_volume, iceberg_volume) / min(avg_volume, iceberg_volume)
            if vol_ratio > (1 + self.volume_tolerance_percent):
                return False
        
        # Time check with first and last in cluster
        first_time = self._parse_timestamp(cluster[0].get('timestamp'))
        last_time = self._parse_timestamp(cluster[-1].get('timestamp'))
        iceberg_time = self._parse_timestamp(iceberg.get('timestamp'))
        
        time_to_first = abs((iceberg_time - first_time).total_seconds())
        time_to_last = abs((iceberg_time - last_time).total_seconds())
        
        if min(time_to_first, time_to_last) > self.time_window_seconds:
            return False
        
        return True
    
    def _create_parent_order(self, cluster: List[Dict]) -> ParentIcebergOrder:
        """Create a parent order from a cluster of icebergs"""
        # Extract data
        prices = [i.get('price', 0) for i in cluster]
        visible_volumes = [i.get('visible_volume', 0) for i in cluster]
        hidden_volumes = [i.get('hidden_volume', 0) for i in cluster]
        total_volumes = [i.get('total_volume', 0) for i in cluster]
        timestamps = [self._parse_timestamp(i.get('timestamp')) for i in cluster]
        confidences = [i.get('confidence', 0) for i in cluster]
        
        # Calculate intervals
        intervals = []
        for i in range(1, len(timestamps)):
            interval = (timestamps[i] - timestamps[i-1]).total_seconds()
            if interval > 0:
                intervals.append(interval)
        
        # Calculate consistency score
        consistency_score = self._calculate_consistency_score(
            prices, total_volumes, intervals
        )
        
        # Collect detection methods
        detection_methods = list(set(
            i.get('detection_method', 'unknown') for i in cluster
        ))
        
        # Generate ID
        self._cluster_id_counter += 1
        parent_id = f"PARENT_{self._cluster_id_counter}_{cluster[0]['side'].upper()}"
        
        # Create parent order
        parent_order = ParentIcebergOrder(
            id=parent_id,
            side=cluster[0]['side'],
            avg_price=np.mean(prices),
            price_min=min(prices),
            price_max=max(prices),
            price_std=np.std(prices),
            total_volume=sum(total_volumes),
            total_visible_volume=sum(visible_volumes),
            total_hidden_volume=sum(hidden_volumes),
            avg_refill_size=np.mean(total_volumes),
            refill_size_std=np.std(total_volumes),
            refill_count=len(cluster),
            refills=cluster,
            first_seen=min(timestamps),
            last_seen=max(timestamps),
            duration_seconds=(max(timestamps) - min(timestamps)).total_seconds(),
            avg_refill_interval=np.mean(intervals) if intervals else 0,
            refill_interval_std=np.std(intervals) if intervals else 0,
            overall_confidence=np.mean(confidences),
            consistency_score=consistency_score,
            exchange=cluster[0].get('exchange', ''),
            symbol=cluster[0].get('symbol', ''),
            detection_methods=detection_methods
        )
        
        return parent_order
    
    def _calculate_consistency_score(
        self,
        prices: List[float],
        volumes: List[float],
        intervals: List[float]
    ) -> float:
        """
        Calculate consistency score (0-1)
        
        High score = consistent prices, volumes, and intervals
        """
        scores = []
        
        # Price consistency (coefficient of variation)
        if prices and np.mean(prices) > 0:
            price_cv = np.std(prices) / np.mean(prices)
            price_score = max(0, 1 - price_cv * 10)  # Lower CV = higher score
            scores.append(price_score)
        
        # Volume consistency
        if volumes and np.mean(volumes) > 0:
            volume_cv = np.std(volumes) / np.mean(volumes)
            volume_score = max(0, 1 - volume_cv)  # Lower CV = higher score
            scores.append(volume_score)
        
        # Interval consistency
        if intervals and np.mean(intervals) > 0:
            interval_cv = np.std(intervals) / np.mean(intervals)
            interval_score = max(0, 1 - interval_cv)
            scores.append(interval_score)
        
        return np.mean(scores) if scores else 0.5
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime"""
        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except:
            return datetime.now()
    
    def _get_iceberg_id(self, iceberg: Dict) -> str:
        """Generate unique ID for iceberg"""
        return f"{iceberg.get('price', 0)}_{iceberg.get('side', '')}_{iceberg.get('timestamp', '')}"
    
    def get_parent_order_summary(self, parent_orders: List[ParentIcebergOrder]) -> Dict:
        """Get summary statistics for parent orders"""
        if not parent_orders:
            return {
                'total_parent_orders': 0,
                'total_refills': 0,
                'total_volume': 0,
                'total_hidden_volume': 0
            }
        
        buy_orders = [p for p in parent_orders if p.side == 'buy']
        sell_orders = [p for p in parent_orders if p.side == 'sell']
        
        return {
            'total_parent_orders': len(parent_orders),
            'buy_parent_orders': len(buy_orders),
            'sell_parent_orders': len(sell_orders),
            'total_refills': sum(p.refill_count for p in parent_orders),
            'avg_refills_per_order': np.mean([p.refill_count for p in parent_orders]),
            'total_volume': sum(p.total_volume for p in parent_orders),
            'total_hidden_volume': sum(p.total_hidden_volume for p in parent_orders),
            'avg_duration_minutes': np.mean([p.duration_seconds / 60 for p in parent_orders]),
            'avg_consistency_score': np.mean([p.consistency_score for p in parent_orders]),
            'avg_overall_confidence': np.mean([p.overall_confidence for p in parent_orders]),
            'largest_parent_order': {
                'id': max(parent_orders, key=lambda x: x.total_volume).id,
                'volume': max(parent_orders, key=lambda x: x.total_volume).total_volume,
                'refills': max(parent_orders, key=lambda x: x.total_volume).refill_count
            } if parent_orders else None
        }


class AdaptiveClusterer:
    """
    Adaptive clustering that adjusts parameters based on market conditions
    """
    
    def __init__(self):
        self.base_clusterer = IcebergClusterer()
        self.market_volatility = 0.0
        
    def cluster_adaptive(self, icebergs: List[Dict], orderbook: Dict) -> Dict:
        """Cluster with adaptive parameters based on market conditions"""
        # Calculate market volatility (spread)
        if orderbook.get('bids') and orderbook.get('asks'):
            if isinstance(orderbook['bids'][0], dict):
                best_bid = orderbook['bids'][0]['price']
                best_ask = orderbook['asks'][0]['price']
            else:
                best_bid = orderbook['bids'][0][0]
                best_ask = orderbook['asks'][0][0]
            
            spread_percent = (best_ask - best_bid) / best_bid * 100
            self.market_volatility = spread_percent
            
            # Adjust price tolerance based on spread
            # Wider spread = more tolerance
            price_tolerance = max(0.05, min(spread_percent * 50, 0.5))
            
            # Create adaptive clusterer
            adaptive_clusterer = IcebergClusterer(
                time_window_seconds=300,
                price_tolerance_percent=price_tolerance,
                volume_tolerance_percent=50,
                min_refills=3,
                min_consistency_score=0.5
            )
            
            result = adaptive_clusterer.cluster(icebergs)
            result['adaptive_params'] = {
                'market_volatility': self.market_volatility,
                'price_tolerance_used': price_tolerance
            }
            
            return result
        
        # Fallback to base clusterer
        return self.base_clusterer.cluster(icebergs)
