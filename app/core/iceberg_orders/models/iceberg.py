"""
Data models for iceberg order detection
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
from enum import Enum


class OrderSide(Enum):
    """Order side enumeration"""
    BUY = "buy"
    SELL = "sell"


class DetectionMethod(Enum):
    """Detection method enumeration"""
    TRADE_FLOW_ANALYSIS = "trade_flow_analysis"
    ORDER_REFILL_PATTERN = "order_refill_pattern"
    VOLUME_ANOMALY = "volume_anomaly"
    TIME_SERIES_ANALYSIS = "time_series_analysis"
    HYBRID = "hybrid"


@dataclass
class IcebergOrder:
    """Represents a detected iceberg order"""
    side: OrderSide
    price: float
    visible_volume: float
    hidden_volume: float
    confidence: float
    timestamp: datetime
    exchange: str
    symbol: str
    detection_method: DetectionMethod
    total_volume: float = field(init=False)
    hidden_ratio: float = field(init=False)
    
    def __post_init__(self):
        """Calculate derived fields"""
        self.total_volume = self.visible_volume + self.hidden_volume
        self.hidden_ratio = self.hidden_volume / self.visible_volume if self.visible_volume > 0 else 0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'side': self.side.value,
            'price': self.price,
            'visibleVolume': self.visible_volume,
            'hiddenVolume': self.hidden_volume,
            'totalVolume': self.total_volume,
            'hiddenRatio': self.hidden_ratio,
            'confidence': self.confidence,
            'timestamp': self.timestamp.isoformat(),
            'exchange': self.exchange,
            'symbol': self.symbol,
            'detectionMethod': self.detection_method.value
        }
    
    def get_size_category(self) -> str:
        """Categorize iceberg by size"""
        if self.hidden_ratio >= 3:
            return 'large'
        elif self.hidden_ratio >= 1:
            return 'medium'
        return 'small'


@dataclass
class TradeEvent:
    """Represents a trade event"""
    price: float
    amount: float
    side: OrderSide
    timestamp: datetime
    trade_id: str
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'price': self.price,
            'amount': self.amount,
            'side': self.side.value,
            'timestamp': self.timestamp.isoformat(),
            'tradeId': self.trade_id
        }


@dataclass
class OrderBookLevel:
    """Represents a single level in the order book"""
    price: float
    volume: float
    order_count: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'price': self.price,
            'volume': self.volume,
            'orderCount': self.order_count
        }


@dataclass
class OrderBookSnapshot:
    """Represents an order book snapshot"""
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    timestamp: datetime
    symbol: str
    exchange: str
    
    def get_best_bid(self) -> Optional[OrderBookLevel]:
        """Get best bid price"""
        return self.bids[0] if self.bids else None
    
    def get_best_ask(self) -> Optional[OrderBookLevel]:
        """Get best ask price"""
        return self.asks[0] if self.asks else None
    
    def get_spread(self) -> float:
        """Calculate bid-ask spread"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid and best_ask:
            return best_ask.price - best_bid.price
        return 0.0
    
    def get_spread_percent(self) -> float:
        """Calculate spread as percentage"""
        best_bid = self.get_best_bid()
        spread = self.get_spread()
        
        if best_bid and best_bid.price > 0:
            return (spread / best_bid.price) * 100
        return 0.0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'bids': [level.to_dict() for level in self.bids],
            'asks': [level.to_dict() for level in self.asks],
            'timestamp': self.timestamp.isoformat(),
            'symbol': self.symbol,
            'exchange': self.exchange,
            'spread': self.get_spread(),
            'spreadPercent': self.get_spread_percent()
        }


@dataclass
class IcebergDetectionResult:
    """Result of iceberg detection analysis"""
    icebergs: List[IcebergOrder]
    timeline: List[Dict]
    metadata: Dict
    statistics: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate statistics"""
        self.statistics = self._calculate_statistics()
    
    def _calculate_statistics(self) -> Dict:
        """Calculate detection statistics"""
        buy_icebergs = [i for i in self.icebergs if i.side == OrderSide.BUY]
        sell_icebergs = [i for i in self.icebergs if i.side == OrderSide.SELL]
        
        total_hidden_volume = sum(i.hidden_volume for i in self.icebergs)
        avg_confidence = sum(i.confidence for i in self.icebergs) / len(self.icebergs) if self.icebergs else 0
        
        return {
            'totalDetected': len(self.icebergs),
            'buyOrders': len(buy_icebergs),
            'sellOrders': len(sell_icebergs),
            'totalHiddenVolume': total_hidden_volume,
            'averageConfidence': avg_confidence,
            'largestIceberg': max(self.icebergs, key=lambda x: x.hidden_volume).to_dict() if self.icebergs else None,
            'detectionMethods': self._count_detection_methods()
        }
    
    def _count_detection_methods(self) -> Dict[str, int]:
        """Count icebergs by detection method"""
        methods = {}
        for iceberg in self.icebergs:
            method = iceberg.detection_method.value
            methods[method] = methods.get(method, 0) + 1
        return methods
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'icebergs': [iceberg.to_dict() for iceberg in self.icebergs],
            'timeline': self.timeline,
            'metadata': self.metadata,
            'statistics': self.statistics
        }


@dataclass
class RefillPattern:
    """Represents a detected order refill pattern"""
    price_level: float
    refill_count: int
    avg_refill_interval: float  # seconds
    total_volume_refilled: float
    side: OrderSide
    confidence: float
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'priceLevel': self.price_level,
            'refillCount': self.refill_count,
            'avgRefillInterval': self.avg_refill_interval,
            'totalVolumeRefilled': self.total_volume_refilled,
            'side': self.side.value,
            'confidence': self.confidence
        }


@dataclass
class VolumeAnomaly:
    """Represents a detected volume anomaly"""
    price: float
    actual_volume: float
    expected_volume: float
    deviation: float
    side: OrderSide
    timestamp: datetime
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'price': self.price,
            'actualVolume': self.actual_volume,
            'expectedVolume': self.expected_volume,
            'deviation': self.deviation,
            'side': self.side.value,
            'timestamp': self.timestamp.isoformat()
        }
