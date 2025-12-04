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


# ============================================================================
# DEX-SPECIFIC MODELS (Uniswap v3, etc.)
# ============================================================================

class LiquidityRange(BaseModel):
    """
    Liquiditätsbereich für DEX (z.B. Uniswap v3 Concentrated Liquidity)
    Repräsentiert Liquidität in einer Preisspanne
    """
    price_lower: float = Field(..., description="Untere Preisgrenze")
    price_upper: float = Field(..., description="Obere Preisgrenze")
    liquidity: float = Field(..., description="Gesamtliquidität in der Spanne (Token-Einheiten)")
    liquidity_usd: float = Field(..., description="USD-Wert der Liquidität")
    tick_lower: Optional[int] = Field(None, description="Uniswap Tick Index (untere Grenze)")
    tick_upper: Optional[int] = Field(None, description="Uniswap Tick Index (obere Grenze)")
    provider_count: int = Field(default=1, description="Anzahl LP-Positionen in dieser Spanne")
    effective_depth: float = Field(default=0.0, description="Effektive Tiefe (bereinigt um Slippage)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "price_lower": 92400.0,
                "price_upper": 92450.0,
                "liquidity": 0.523,
                "liquidity_usd": 48350.0,
                "tick_lower": 201000,
                "tick_upper": 201010,
                "provider_count": 12,
                "effective_depth": 0.498
            }
        }


class PoolState(BaseModel):
    """
    Aktueller Zustand eines DEX Pools (Uniswap v3, etc.)
    """
    pool_address: str = Field(..., description="Smart Contract Adresse des Pools")
    token0_address: str = Field(..., description="Token0 Contract Adresse")
    token1_address: str = Field(..., description="Token1 Contract Adresse")
    token0_symbol: str = Field(..., description="Token0 Symbol (z.B. WETH)")
    token1_symbol: str = Field(..., description="Token1 Symbol (z.B. USDC)")
    current_price: float = Field(..., description="Aktueller Wechselkurs")
    current_tick: int = Field(..., description="Aktueller aktiver Tick")
    sqrt_price_x96: int = Field(..., description="Uniswap Price Representation (sqrtPriceX96)")
    total_liquidity: float = Field(..., description="Summe aller Liquidität")
    tvl_usd: float = Field(..., description="Total Value Locked in USD")
    fee_tier: int = Field(..., description="Pool Fee (z.B. 3000 = 0.3%)")
    tick_spacing: int = Field(..., description="Abstand zwischen Ticks")
    liquidity_distribution: List[LiquidityRange] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_schema_extra = {
            "example": {
                "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "token0_address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "token1_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "token0_symbol": "WETH",
                "token1_symbol": "USDC",
                "current_price": 2850.45,
                "current_tick": 201234,
                "sqrt_price_x96": 1234567890,
                "total_liquidity": 125000.5,
                "tvl_usd": 285000000.0,
                "fee_tier": 500,
                "tick_spacing": 10
            }
        }


class DEXOrderbook(BaseModel):
    """
    Virtuelles Orderbook aus DEX Liquiditätskurve generiert
    Ermöglicht DEX-Daten im CEX-Orderbook-Format zu konsumieren
    """
    exchange: Exchange
    exchange_type: ExchangeType = Field(default=ExchangeType.DEX)
    symbol: str = Field(..., description="Trading Pair (z.B. WETH/USDC)")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    bids: OrderbookSide = Field(default_factory=OrderbookSide)
    asks: OrderbookSide = Field(default_factory=OrderbookSide)
    
    # DEX-spezifische Felder
    pool_address: str = Field(..., description="Pool Contract Adresse")
    liquidity_ranges: List[LiquidityRange] = Field(
        default_factory=list,
        description="Original DEX Liquiditätsbereiche"
    )
    is_virtual: bool = Field(
        default=True,
        description="Kennzeichen für virtuelles Orderbook"
    )
    pool_state: Optional[PoolState] = Field(
        None,
        description="Aktueller Pool-Zustand"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "uniswap_v3",
                "exchange_type": "dex",
                "symbol": "WETH/USDC",
                "pool_address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "is_virtual": True,
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
