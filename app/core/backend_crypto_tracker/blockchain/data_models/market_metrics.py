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
class TokenMarketData:
    """
    Current market data for a single token.
    Used by CoinGecko and other market data aggregators.
    """
    token_id: str
    symbol: str
    price: float
    market_cap: float
    volume_24h: float
    price_change_24h: float
    currency: str = "usd"
    source: str = "unknown"
    
    # Optional extended data
    price_change_1h: Optional[float] = None
    price_change_7d: Optional[float] = None
    price_change_30d: Optional[float] = None
    market_cap_rank: Optional[int] = None
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    max_supply: Optional[float] = None
    ath: Optional[float] = None  # All-time high
    ath_date: Optional[datetime] = None
    atl: Optional[float] = None  # All-time low
    atl_date: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'token_id': self.token_id,
            'symbol': self.symbol,
            'price': self.price,
            'market_cap': self.market_cap,
            'volume_24h': self.volume_24h,
            'price_change_24h': self.price_change_24h,
            'currency': self.currency,
            'source': self.source,
            'price_change_1h': self.price_change_1h,
            'price_change_7d': self.price_change_7d,
            'price_change_30d': self.price_change_30d,
            'market_cap_rank': self.market_cap_rank,
            'circulating_supply': self.circulating_supply,
            'total_supply': self.total_supply,
            'max_supply': self.max_supply,
            'ath': self.ath,
            'ath_date': self.ath_date.isoformat() if self.ath_date else None,
            'atl': self.atl,
            'atl_date': self.atl_date.isoformat() if self.atl_date else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TokenMarketData':
        """Create from dictionary"""
        # Handle datetime fields
        ath_date = None
        if data.get('ath_date'):
            ath_date = datetime.fromisoformat(data['ath_date']) if isinstance(data['ath_date'], str) else data['ath_date']
        
        atl_date = None
        if data.get('atl_date'):
            atl_date = datetime.fromisoformat(data['atl_date']) if isinstance(data['atl_date'], str) else data['atl_date']
        
        last_updated = None
        if data.get('last_updated'):
            last_updated = datetime.fromisoformat(data['last_updated']) if isinstance(data['last_updated'], str) else data['last_updated']
        
        return cls(
            token_id=data['token_id'],
            symbol=data['symbol'],
            price=data['price'],
            market_cap=data['market_cap'],
            volume_24h=data['volume_24h'],
            price_change_24h=data['price_change_24h'],
            currency=data.get('currency', 'usd'),
            source=data.get('source', 'unknown'),
            price_change_1h=data.get('price_change_1h'),
            price_change_7d=data.get('price_change_7d'),
            price_change_30d=data.get('price_change_30d'),
            market_cap_rank=data.get('market_cap_rank'),
            circulating_supply=data.get('circulating_supply'),
            total_supply=data.get('total_supply'),
            max_supply=data.get('max_supply'),
            ath=data.get('ath'),
            ath_date=ath_date,
            atl=data.get('atl'),
            atl_date=atl_date,
            last_updated=last_updated
        )


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
    """Comprehensive market metrics for detailed analysis"""
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
