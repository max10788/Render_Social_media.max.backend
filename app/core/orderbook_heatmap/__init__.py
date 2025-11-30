"""
Orderbook Heatmap Tool

Live-Orderbuch-Heatmap die gleichzeitig mehrere CEXs (Binance, Bitget, Kraken) 
und DEX-Orderflow in einer einzigen Visualisierung vergleicht.
"""

from .models import (
    Exchange,
    ExchangeType,
    Orderbook,
    AggregatedOrderbook,
    HeatmapSnapshot,
    HeatmapTimeSeries,
    HeatmapConfig
)

from .exchanges import (
    BinanceExchange,
    BitgetExchange,
    KrakenExchange,
    UniswapV3Exchange
)

from .aggregator import OrderbookAggregator
from .api import router
from .utils import PriceNormalizer, HeatmapGenerator

__version__ = "1.0.0"

__all__ = [
    # Models
    "Exchange",
    "ExchangeType",
    "Orderbook",
    "AggregatedOrderbook",
    "HeatmapSnapshot",
    "HeatmapTimeSeries",
    "HeatmapConfig",
    
    # Exchanges
    "BinanceExchange",
    "BitgetExchange",
    "KrakenExchange",
    "UniswapV3Exchange",
    
    # Core
    "OrderbookAggregator",
    
    # API
    "router",
    
    # Utils
    "PriceNormalizer",
    "HeatmapGenerator",
]
