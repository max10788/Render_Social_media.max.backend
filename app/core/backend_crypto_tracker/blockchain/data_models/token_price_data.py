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
    """
    Token price data model for unified price information across chains.
    """
    token_address: str
    chain: str
    price: float
    currency: str = "usd"
    
    # Optional USD price (for when currency is not USD)
    price_usd: Optional[float] = None
    
    # Market metrics
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    volume_7d: Optional[float] = None
    
    # Price changes
    price_change_1h: Optional[float] = None
    price_change_24h: Optional[float] = None
    price_change_7d: Optional[float] = None
    price_change_30d: Optional[float] = None
    
    # Liquidity information
    liquidity_usd: Optional[float] = None
    
    # Token metadata
    symbol: Optional[str] = None
    name: Optional[str] = None
    decimals: Optional[int] = None
    token_id: Optional[str] = None
    
    # Timestamps
    last_updated: Optional[datetime] = None
    
    # Source information
    source: str = "unknown"  # e.g., "coingecko", "sui_dex", "uniswap"
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'token_address': self.token_address,
            'chain': self.chain,
            'price': self.price,
            'currency': self.currency,
            'price_usd': self.price_usd,
            'market_cap': self.market_cap,
            'volume_24h': self.volume_24h,
            'volume_7d': self.volume_7d,
            'price_change_1h': self.price_change_1h,
            'price_change_24h': self.price_change_24h,
            'price_change_7d': self.price_change_7d,
            'price_change_30d': self.price_change_30d,
            'liquidity_usd': self.liquidity_usd,
            'symbol': self.symbol,
            'name': self.name,
            'decimals': self.decimals,
            'token_id': self.token_id,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'source': self.source
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TokenPriceData':
        """Create from dictionary"""
        last_updated = None
        if data.get('last_updated'):
            if isinstance(data['last_updated'], str):
                last_updated = datetime.fromisoformat(data['last_updated'])
            else:
                last_updated = data['last_updated']
        
        return cls(
            token_address=data['token_address'],
            chain=data['chain'],
            price=data['price'],
            currency=data.get('currency', 'usd'),
            price_usd=data.get('price_usd'),
            market_cap=data.get('market_cap'),
            volume_24h=data.get('volume_24h'),
            volume_7d=data.get('volume_7d'),
            price_change_1h=data.get('price_change_1h'),
            price_change_24h=data.get('price_change_24h'),
            price_change_7d=data.get('price_change_7d'),
            price_change_30d=data.get('price_change_30d'),
            liquidity_usd=data.get('liquidity_usd'),
            symbol=data.get('symbol'),
            name=data.get('name'),
            decimals=data.get('decimals'),
            token_id=data.get('token_id'),
            last_updated=last_updated,
            source=data.get('source', 'unknown')
        )
    
    def __str__(self) -> str:
        """String representation"""
        symbol_str = f" ({self.symbol})" if self.symbol else ""
        return f"TokenPriceData{symbol_str}: ${self.price} on {self.chain} from {self.source}"
    
    def is_stale(self, max_age_seconds: int = 300) -> bool:
        """
        Check if price data is stale (older than max_age_seconds).
        
        Args:
            max_age_seconds: Maximum age in seconds (default: 5 minutes)
            
        Returns:
            True if data is stale
        """
        if not self.last_updated:
            return True
        
        age = (datetime.now() - self.last_updated).total_seconds()
        return age > max_age_seconds
    
    def get_price_in_currency(self, target_currency: str = "usd") -> Optional[float]:
        """
        Get price in a specific currency.
        
        Args:
            target_currency: Target currency code
            
        Returns:
            Price in target currency or None
        """
        if target_currency.lower() == self.currency.lower():
            return self.price
        elif target_currency.lower() == "usd" and self.price_usd:
            return self.price_usd
        else:
            # Would need exchange rate conversion
            return None
