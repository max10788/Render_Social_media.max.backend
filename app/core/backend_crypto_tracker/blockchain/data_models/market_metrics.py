# blockchain/data_models/market_metrics.py
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime
from decimal import Decimal

@dataclass
class VolumeData:
    """Trading volume data"""
    volume_24h: Decimal
    volume_7d: Optional[Decimal] = None
    volume_30d: Optional[Decimal] = None
    volume_change_24h: Optional[float] = None

@dataclass
class GlobalMetrics:
    """Global market metrics"""
    total_market_cap: Decimal
    total_volume_24h: Decimal
    btc_dominance: float
    eth_dominance: float
    active_cryptocurrencies: int
    active_exchanges: int
    last_updated: datetime

@dataclass
class MarketMetrics:
    """Comprehensive market metrics"""
    symbol: str
    rank: int
    market_cap: Decimal
    fully_diluted_valuation: Optional[Decimal]
    volume: VolumeData
    price_change_1h: float
    price_change_24h: float
    price_change_7d: float
    all_time_high: Decimal
    all_time_high_date: datetime
    all_time_low: Decimal
    all_time_low_date: datetime
    metadata: Dict[str, Any]
