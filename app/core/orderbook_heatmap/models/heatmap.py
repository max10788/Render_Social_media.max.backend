"""
Datenmodelle für Heatmap-Visualisierung
"""
from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel, Field
import numpy as np


class HeatmapCell(BaseModel):
    """Einzelne Zelle in der Heatmap"""
    price: float
    timestamp: datetime
    liquidity: float
    exchange: str
    
    class Config:
        arbitrary_types_allowed = True


class PriceLevel(BaseModel):
    """Preis-Level mit Liquidität von allen Börsen"""
    price: float
    liquidity_by_exchange: Dict[str, float] = Field(default_factory=dict)
    total_liquidity: float = 0.0
    
    def add_liquidity(self, exchange: str, amount: float):
        """Fügt Liquidität von einer Börse hinzu"""
        self.liquidity_by_exchange[exchange] = amount
        self.total_liquidity = sum(self.liquidity_by_exchange.values())


class HeatmapSnapshot(BaseModel):
    """Snapshot der Heatmap zu einem bestimmten Zeitpunkt"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    symbol: str
    price_levels: List[PriceLevel] = Field(default_factory=list)
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    
    def to_matrix(self, exchanges: List[str]) -> Dict:
        """
        Konvertiert zu Matrix-Format für Visualisierung
        Returns:
            Dict mit prices, exchanges, matrix (2D array)
        """
        if not self.price_levels:
            return {
                "prices": [],
                "exchanges": exchanges,
                "matrix": [],
                "timestamp": self.timestamp.isoformat()
            }
        
        prices = [pl.price for pl in self.price_levels]
        matrix = []
        
        for exchange in exchanges:
            exchange_data = [
                pl.liquidity_by_exchange.get(exchange, 0.0)
                for pl in self.price_levels
            ]
            matrix.append(exchange_data)
        
        return {
            "prices": prices,
            "exchanges": exchanges,
            "matrix": matrix,
            "timestamp": self.timestamp.isoformat()
        }


class HeatmapTimeSeries(BaseModel):
    """Zeit-Serie von Heatmap-Snapshots"""
    symbol: str
    snapshots: List[HeatmapSnapshot] = Field(default_factory=list)
    max_snapshots: int = 100  # Limit für Memory
    
    def add_snapshot(self, snapshot: HeatmapSnapshot):
        """Fügt einen Snapshot hinzu"""
        self.snapshots.append(snapshot)
        # Behalte nur die neuesten N Snapshots
        if len(self.snapshots) > self.max_snapshots:
            self.snapshots = self.snapshots[-self.max_snapshots:]
    
    def get_latest(self) -> Optional[HeatmapSnapshot]:
        """Holt den neuesten Snapshot"""
        return self.snapshots[-1] if self.snapshots else None
    
    def to_3d_matrix(self, exchanges: List[str]) -> Dict:
        """
        Konvertiert zu 3D-Matrix für Zeit-basierte Heatmap
        Returns:
            Dict mit times, prices, exchanges, matrix (3D array)
        """
        if not self.snapshots:
            return {
                "times": [],
                "prices": [],
                "exchanges": exchanges,
                "matrix": []
            }
        
        times = [s.timestamp.isoformat() for s in self.snapshots]
        
        # Sammle alle einzigartigen Preise
        all_prices = set()
        for snapshot in self.snapshots:
            all_prices.update(pl.price for pl in snapshot.price_levels)
        prices = sorted(all_prices)
        
        # Erstelle 3D-Matrix: [Zeit][Börse][Preis]
        matrix = []
        for snapshot in self.snapshots:
            snapshot_matrix = []
            for exchange in exchanges:
                price_data = []
                for price in prices:
                    # Finde PriceLevel für diesen Preis
                    level = next(
                        (pl for pl in snapshot.price_levels if pl.price == price),
                        None
                    )
                    liquidity = (
                        level.liquidity_by_exchange.get(exchange, 0.0)
                        if level else 0.0
                    )
                    price_data.append(liquidity)
                snapshot_matrix.append(price_data)
            matrix.append(snapshot_matrix)
        
        return {
            "times": times,
            "prices": prices,
            "exchanges": exchanges,
            "matrix": matrix
        }


class HeatmapConfig(BaseModel):
    """Konfiguration für Heatmap-Generierung"""
    price_bucket_size: float = Field(
        default=10.0,
        description="Größe der Preis-Buckets für Aggregation"
    )
    time_window_seconds: int = Field(
        default=60,
        description="Zeitfenster für Aggregation in Sekunden"
    )
    exchanges: List[str] = Field(
        default_factory=lambda: ["binance", "bitget", "kraken", "uniswap_v3"]
    )
    normalize_liquidity: bool = Field(
        default=True,
        description="Normalisiert Liquidität zwischen 0-1"
    )
    color_scale: str = Field(
        default="viridis",
        description="Farbskala für Heatmap (viridis, plasma, hot, etc.)"
    )


# ============================================================================
# DEX-SPECIFIC HEATMAP MODELS
# ============================================================================

class DEXHeatmapSnapshot(HeatmapSnapshot):
    """
    Erweiterter Heatmap-Snapshot für DEX mit zusätzlichen Metriken
    """
    pool_addresses: List[str] = Field(
        default_factory=list,
        description="Liste der Source Pool Contract Adressen"
    )
    total_tvl: float = Field(
        default=0.0,
        description="Kombinierter TVL über alle Pools in USD"
    )
    active_lp_count: int = Field(
        default=0,
        description="Anzahl aktiver Liquidity Provider"
    )
    fee_tier_distribution: Dict[int, float] = Field(
        default_factory=dict,
        description="TVL-Verteilung nach Fee Tier (z.B. {500: 123M, 3000: 456M})"
    )
    concentration_ratio: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Prozent der Liquidität innerhalb ±2% vom aktuellen Preis"
    )
    
    # Zusätzliche Konzentrations-Metriken
    concentration_metrics: Dict[str, float] = Field(
        default_factory=dict,
        description="Konzentrations-Metriken: within_1_percent, within_2_percent, within_5_percent"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "WETH/USDC",
                "pool_addresses": [
                    "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"
                ],
                "total_tvl": 410000000.0,
                "active_lp_count": 523,
                "fee_tier_distribution": {
                    500: 285000000.0,
                    3000: 125000000.0
                },
                "concentration_ratio": 0.582,
                "concentration_metrics": {
                    "within_1_percent": 35.5,
                    "within_2_percent": 58.2,
                    "within_5_percent": 82.4
                }
            }
        }
    
    def calculate_concentration_ratio(self, current_price: float, tolerance_pct: float = 2.0):
        """
        Berechnet Konzentrations-Ratio (% Liquidität nahe current_price)
        
        Args:
            current_price: Aktueller Preis
            tolerance_pct: Toleranz in Prozent (default: 2% = ±2%)
        """
        if not self.price_levels:
            self.concentration_ratio = 0.0
            return
        
        price_lower = current_price * (1 - tolerance_pct / 100)
        price_upper = current_price * (1 + tolerance_pct / 100)
        
        total_liquidity = sum(pl.total_liquidity for pl in self.price_levels)
        concentrated_liquidity = sum(
            pl.total_liquidity 
            for pl in self.price_levels 
            if price_lower <= pl.price <= price_upper
        )
        
        if total_liquidity > 0:
            self.concentration_ratio = concentrated_liquidity / total_liquidity
        else:
            self.concentration_ratio = 0.0
    
    def to_matrix(self, exchanges: List[str]) -> Dict:
        """
        Erweiterte Matrix-Konvertierung mit DEX-Metriken
        """
        matrix_data = super().to_matrix(exchanges)
        
        # Füge DEX-spezifische Felder hinzu
        matrix_data.update({
            "pool_addresses": self.pool_addresses,
            "total_tvl": self.total_tvl,
            "active_lp_count": self.active_lp_count,
            "fee_tier_distribution": self.fee_tier_distribution,
            "concentration_ratio": self.concentration_ratio,
            "concentration_metrics": self.concentration_metrics
        })
        
        return matrix_data
