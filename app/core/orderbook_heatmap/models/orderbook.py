"""
Datenmodelle für Orderbuch-Daten von CEX und DEX
"""
from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel, Field
from enum import Enum


class ExchangeType(str, Enum):
    """Exchange-Typen"""
    CEX = "cex"
    DEX = "dex"


class Exchange(str, Enum):
    """Unterstützte Börsen"""
    BINANCE = "binance"
    BITGET = "bitget"
    KRAKEN = "kraken"
    UNISWAP_V3 = "uniswap_v3"
    RAYDIUM = "raydium"


class OrderbookLevel(BaseModel):
    """Einzelne Orderbuch-Ebene (Bid/Ask)"""
    price: float = Field(..., description="Preis")
    quantity: float = Field(..., description="Menge")
    total: float = Field(default=0.0, description="Kumulierte Menge")
    
    class Config:
        json_schema_extra = {
            "example": {
                "price": 50000.00,
                "quantity": 1.5,
                "total": 3.0
            }
        }


class OrderbookSide(BaseModel):
    """Eine Seite des Orderbuchs (Bids oder Asks)"""
    levels: List[OrderbookLevel] = Field(default_factory=list)
    
    def get_total_volume(self) -> float:
        """Berechnet Gesamtvolumen"""
        return sum(level.quantity for level in self.levels)
    
    def get_liquidity_at_price(self, price: float, range_pct: float = 0.001) -> float:
        """Holt Liquidität bei einem bestimmten Preis (±range_pct)"""
        lower = price * (1 - range_pct)
        upper = price * (1 + range_pct)
        return sum(
            level.quantity 
            for level in self.levels 
            if lower <= level.price <= upper
        )


class Orderbook(BaseModel):
    """Vollständiges Orderbuch"""
    exchange: Exchange
    exchange_type: ExchangeType
    symbol: str = Field(..., description="Trading Pair (z.B. BTC/USDT)")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    bids: OrderbookSide = Field(default_factory=OrderbookSide)
    asks: OrderbookSide = Field(default_factory=OrderbookSide)
    
    # Zusätzliche Metadaten
    sequence: Optional[int] = None
    is_snapshot: bool = False
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "binance",
                "exchange_type": "cex",
                "symbol": "BTC/USDT",
                "timestamp": "2024-01-01T12:00:00",
                "bids": {"levels": []},
                "asks": {"levels": []}
            }
        }
    
    def get_spread(self) -> Optional[float]:
        """Berechnet Spread"""
        if not self.bids.levels or not self.asks.levels:
            return None
        best_bid = max(self.bids.levels, key=lambda x: x.price).price
        best_ask = min(self.asks.levels, key=lambda x: x.price).price
        return best_ask - best_bid
    
    def get_mid_price(self) -> Optional[float]:
        """Berechnet Mid-Price"""
        if not self.bids.levels or not self.asks.levels:
            return None
        best_bid = max(self.bids.levels, key=lambda x: x.price).price
        best_ask = min(self.asks.levels, key=lambda x: x.price).price
        return (best_bid + best_ask) / 2


class DEXLiquidityTick(BaseModel):
    """DEX Liquiditäts-Tick (für Uniswap v3)"""
    tick_index: int
    liquidity: float
    price_lower: float
    price_upper: float
    
    def to_orderbook_level(self) -> OrderbookLevel:
        """Konvertiert DEX Tick zu Orderbuch-Level"""
        mid_price = (self.price_lower + self.price_upper) / 2
        return OrderbookLevel(
            price=mid_price,
            quantity=self.liquidity,
            total=self.liquidity
        )


class AggregatedOrderbook(BaseModel):
    """Aggregiertes Orderbuch von mehreren Börsen"""
    symbol: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    orderbooks: Dict[str, Orderbook] = Field(default_factory=dict)
    
    def get_all_price_levels(self, side: str = "both") -> List[float]:
        """Holt alle Preis-Levels von allen Börsen"""
        prices = []
        for ob in self.orderbooks.values():
            if side in ["bids", "both"]:
                prices.extend([level.price for level in ob.bids.levels])
            if side in ["asks", "both"]:
                prices.extend([level.price for level in ob.asks.levels])
        return sorted(set(prices))
    
    def get_liquidity_at_price(self, price: float, range_pct: float = 0.001) -> Dict[str, float]:
        """Holt Liquidität bei einem bestimmten Preis von allen Börsen"""
        liquidity = {}
        for exchange_name, ob in self.orderbooks.items():
            bid_liq = ob.bids.get_liquidity_at_price(price, range_pct)
            ask_liq = ob.asks.get_liquidity_at_price(price, range_pct)
            liquidity[exchange_name] = bid_liq + ask_liq
        return liquidity
