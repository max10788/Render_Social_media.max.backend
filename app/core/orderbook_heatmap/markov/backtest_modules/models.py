"""
models.py — Kerndatenmodelle für das Orderbuch-Simulations-Framework.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


class Exchange(Enum):
    binance  = "binance"
    bitget   = "bitget"
    kraken   = "kraken"
    bybit    = "bybit"
    okx      = "okx"
    coinbase = "coinbase"
    deribit  = "deribit"


ExchangeType = "cex"


@dataclass
class OrderbookLevel:
    price: float
    quantity: float


@dataclass
class OrderbookSide:
    levels: List[OrderbookLevel] = field(default_factory=list)


@dataclass
class Orderbook:
    exchange: Exchange
    symbol: str
    timestamp: datetime
    bids: OrderbookSide
    asks: OrderbookSide
    exchange_type: str = "cex"
    sequence: Optional[int] = None
    is_snapshot: bool = True


@dataclass
class PriceLevel:
    price: float
    liquidity_by_exchange: Dict[str, float] = field(default_factory=dict)

    @property
    def total_liquidity(self) -> float:
        return sum(self.liquidity_by_exchange.values())


@dataclass
class HeatmapSnapshot:
    symbol: str
    timestamp: datetime
    price_levels: List[PriceLevel]   # sortiert nach Preis (aufsteigend)
    min_price: float
    max_price: float
    mid_price: float                  # (best_bid + best_ask) / 2
