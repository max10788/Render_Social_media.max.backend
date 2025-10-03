# blockchain/data_models/token_price_data.py
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal

@dataclass
class PricePoint:
    """Single price point with timestamp"""
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    market_cap: Optional[Decimal] = None
    
@dataclass
class MarketData:
    """Current market data for a token"""
    symbol: str
    name: str
    current_price: Decimal
    market_cap: Decimal
    volume_24h: Decimal
    price_change_24h: Decimal
    price_change_percentage_24h: float
    circulating_supply: Optional[Decimal] = None
    total_supply: Optional[Decimal] = None
    last_updated: Optional[datetime] = None

@dataclass
class TokenPriceData:
    """Complete token price data"""
    token_address: str
    chain: str
    symbol: str
    name: str
    current_market: MarketData
    historical_prices: List[PricePoint]
    metadata: Dict[str, Any]
