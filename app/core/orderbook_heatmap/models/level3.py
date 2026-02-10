"""
Level 3 (L3) order book data models

L3 provides the most granular market view - individual orders with unique IDs,
prices, sizes, and lifecycle events (add/modify/cancel) rather than aggregated
price levels (L2).
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class L3EventType(str, Enum):
    """Level 3 order event types"""
    OPEN = "open"           # New order added
    DONE = "done"           # Order fully filled or canceled
    CHANGE = "change"       # Order size/price changed
    MATCH = "match"         # Order matched (trade executed)


class L3Side(str, Enum):
    """Order side"""
    BID = "bid"
    ASK = "ask"


class L3Order(BaseModel):
    """
    Individual Level 3 order

    Represents a single order in the order book with full lifecycle tracking.
    """
    exchange: str = Field(..., description="Exchange name (e.g. 'coinbase', 'bitfinex')")
    symbol: str = Field(..., description="Trading pair (e.g. 'BTC/USDT', 'BTC-USD')")
    order_id: str = Field(..., description="Exchange-specific unique order ID")
    sequence: Optional[int] = Field(None, description="Sequence number for ordering events")
    side: L3Side = Field(..., description="Order side (bid/ask)")
    price: float = Field(..., description="Order price", gt=0)
    size: float = Field(..., description="Order size/quantity", gt=0)
    event_type: L3EventType = Field(..., description="Event type (open/done/change/match)")
    timestamp: datetime = Field(..., description="Event timestamp")
    metadata: Optional[Dict[str, Any]] = Field(
        None,
        description="Exchange-specific metadata (maker_order_id, taker_order_id, trade_id, etc.)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
                "sequence": 50,
                "side": "bid",
                "price": 50000.00,
                "size": 0.5,
                "event_type": "open",
                "timestamp": "2026-02-10T12:00:00Z",
                "metadata": {"user_id": "abc123"}
            }
        }


class L3Orderbook(BaseModel):
    """
    Full Level 3 orderbook state

    Contains all individual orders on both sides of the book at a specific point in time.
    """
    exchange: str = Field(..., description="Exchange name")
    symbol: str = Field(..., description="Trading pair")
    sequence: int = Field(..., description="Sequence number of this snapshot")
    timestamp: datetime = Field(..., description="Snapshot timestamp")
    bids: List[L3Order] = Field(
        default_factory=list,
        description="All bid orders"
    )
    asks: List[L3Order] = Field(
        default_factory=list,
        description="All ask orders"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "sequence": 1000,
                "timestamp": "2026-02-10T12:00:00Z",
                "bids": [],
                "asks": []
            }
        }

    def get_total_orders(self) -> int:
        """Returns total number of orders in the book"""
        return len(self.bids) + len(self.asks)

    def get_total_volume(self, side: Optional[L3Side] = None) -> float:
        """
        Calculate total volume

        Args:
            side: If specified, return volume for that side only

        Returns:
            Total volume in base currency
        """
        if side == L3Side.BID:
            return sum(order.size for order in self.bids)
        elif side == L3Side.ASK:
            return sum(order.size for order in self.asks)
        else:
            return sum(order.size for order in self.bids) + sum(order.size for order in self.asks)

    def get_best_bid(self) -> Optional[float]:
        """Returns highest bid price"""
        if not self.bids:
            return None
        return max(order.price for order in self.bids)

    def get_best_ask(self) -> Optional[float]:
        """Returns lowest ask price"""
        if not self.asks:
            return None
        return min(order.price for order in self.asks)

    def get_spread(self) -> Optional[float]:
        """Calculate bid-ask spread"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid is None or best_ask is None:
            return None
        return best_ask - best_bid

    def get_mid_price(self) -> Optional[float]:
        """Calculate mid price"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid is None or best_ask is None:
            return None
        return (best_bid + best_ask) / 2


class L3Snapshot(BaseModel):
    """
    Compressed L3 snapshot for database storage

    Used for periodic snapshots and recovery purposes.
    """
    exchange: str
    symbol: str
    sequence: int
    timestamp: datetime
    bids: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Compressed bid orders [{order_id, price, size}, ...]"
    )
    asks: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Compressed ask orders [{order_id, price, size}, ...]"
    )
    total_bid_orders: int = Field(default=0)
    total_ask_orders: int = Field(default=0)
    total_bid_volume: float = Field(default=0.0)
    total_ask_volume: float = Field(default=0.0)

    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "coinbase",
                "symbol": "BTC-USD",
                "sequence": 1000,
                "timestamp": "2026-02-10T12:00:00Z",
                "bids": [
                    {"order_id": "abc", "price": 50000.0, "size": 0.5}
                ],
                "asks": [
                    {"order_id": "def", "price": 50100.0, "size": 0.3}
                ],
                "total_bid_orders": 1,
                "total_ask_orders": 1,
                "total_bid_volume": 0.5,
                "total_ask_volume": 0.3
            }
        }


class StartL3Request(BaseModel):
    """Request model for starting L3 data collection"""
    symbol: str = Field(..., description="Trading pair to monitor")
    exchanges: List[str] = Field(..., description="List of exchanges to connect", min_items=1)
    persist: bool = Field(default=True, description="Whether to persist to database")
    snapshot_interval_seconds: int = Field(
        default=60,
        description="Interval for full snapshots (seconds)",
        ge=10,
        le=3600
    )

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "BTC-USD",
                "exchanges": ["coinbase", "bitfinex"],
                "persist": True,
                "snapshot_interval_seconds": 60
            }
        }


class L3StreamStatus(BaseModel):
    """Status of L3 data stream"""
    symbol: str
    exchanges: List[str]
    is_active: bool
    orders_received: int = Field(default=0, description="Total orders received")
    orders_persisted: int = Field(default=0, description="Total orders saved to DB")
    snapshots_taken: int = Field(default=0, description="Number of snapshots saved")
    started_at: datetime
    last_update: Optional[datetime] = None
    errors: List[str] = Field(default_factory=list, description="Recent error messages")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "BTC-USD",
                "exchanges": ["coinbase"],
                "is_active": True,
                "orders_received": 15234,
                "orders_persisted": 15230,
                "snapshots_taken": 10,
                "started_at": "2026-02-10T12:00:00Z",
                "last_update": "2026-02-10T12:15:00Z",
                "errors": []
            }
        }
