"""
Snapshot manager for Level 3 orderbooks

Handles periodic snapshot creation and recovery logic.
"""
import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime

from app.core.orderbook_heatmap.models.level3 import L3Order, L3Orderbook, L3Side, L3EventType, L3Snapshot
from app.core.orderbook_heatmap.storage.l3_repository import L3Repository


logger = logging.getLogger(__name__)


class SnapshotManager:
    """
    Manages periodic snapshots and orderbook state reconstruction

    Maintains in-memory orderbook state and periodically saves snapshots
    to database for recovery purposes.
    """

    def __init__(self, snapshot_interval_seconds: int = 60):
        """
        Initialize snapshot manager

        Args:
            snapshot_interval_seconds: Interval between snapshots (default: 60s)
        """
        self.snapshot_interval = snapshot_interval_seconds
        self.orderbooks: Dict[str, L3Orderbook] = {}  # Key: "exchange:symbol"
        self.orders: Dict[str, Dict[str, L3Order]] = {}  # Key: "exchange:symbol", Value: {order_id: order}
        self.repository: Optional[L3Repository] = None
        self._snapshot_task: Optional[asyncio.Task] = None
        self._running = False

    def _get_key(self, exchange: str, symbol: str) -> str:
        """Get cache key for exchange/symbol pair"""
        return f"{exchange}:{symbol}"

    def initialize_orderbook(self, snapshot: L3Orderbook):
        """
        Initialize orderbook from snapshot

        Args:
            snapshot: L3Orderbook snapshot
        """
        key = self._get_key(snapshot.exchange, snapshot.symbol)

        self.orderbooks[key] = snapshot
        self.orders[key] = {}

        # Index all orders by order_id
        for order in snapshot.bids + snapshot.asks:
            self.orders[key][order.order_id] = order

        logger.info(f"Initialized orderbook for {key}: {len(snapshot.bids)} bids, {len(snapshot.asks)} asks")

    def apply_order_event(self, order: L3Order):
        """
        Apply order event to in-memory orderbook

        Updates the orderbook state based on the event type.

        Args:
            order: L3Order event
        """
        key = self._get_key(order.exchange, order.symbol)

        # Ensure orderbook exists
        if key not in self.orderbooks:
            self.orderbooks[key] = L3Orderbook(
                exchange=order.exchange,
                symbol=order.symbol,
                sequence=order.sequence or 0,
                timestamp=order.timestamp,
                bids=[],
                asks=[]
            )
            self.orders[key] = {}

        orderbook = self.orderbooks[key]
        orders_dict = self.orders[key]

        # Update sequence
        if order.sequence and order.sequence > orderbook.sequence:
            orderbook.sequence = order.sequence

        orderbook.timestamp = order.timestamp

        # Apply event
        if order.event_type == L3EventType.OPEN:
            # Add new order
            orders_dict[order.order_id] = order

            if order.side == L3Side.BID:
                orderbook.bids.append(order)
            else:
                orderbook.asks.append(order)

        elif order.event_type == L3EventType.DONE:
            # Remove order
            if order.order_id in orders_dict:
                old_order = orders_dict[order.order_id]

                if old_order.side == L3Side.BID:
                    orderbook.bids = [o for o in orderbook.bids if o.order_id != order.order_id]
                else:
                    orderbook.asks = [o for o in orderbook.asks if o.order_id != order.order_id]

                del orders_dict[order.order_id]

        elif order.event_type == L3EventType.CHANGE:
            # Update order size
            if order.order_id in orders_dict:
                orders_dict[order.order_id].size = order.size
                orders_dict[order.order_id].timestamp = order.timestamp

        elif order.event_type == L3EventType.MATCH:
            # Match events typically result in order removal or size reduction
            # For simplicity, we remove the order (full fill)
            # In production, you'd check remaining_size
            if order.order_id in orders_dict:
                old_order = orders_dict[order.order_id]

                if old_order.side == L3Side.BID:
                    orderbook.bids = [o for o in orderbook.bids if o.order_id != order.order_id]
                else:
                    orderbook.asks = [o for o in orderbook.asks if o.order_id != order.order_id]

                del orders_dict[order.order_id]

    def get_orderbook(self, exchange: str, symbol: str) -> Optional[L3Orderbook]:
        """
        Get current orderbook state

        Args:
            exchange: Exchange name
            symbol: Trading pair

        Returns:
            Current L3Orderbook or None
        """
        key = self._get_key(exchange, symbol)
        return self.orderbooks.get(key)

    def get_orderbook_stats(self, exchange: str, symbol: str) -> Dict[str, any]:
        """
        Get statistics about current orderbook

        Args:
            exchange: Exchange name
            symbol: Trading pair

        Returns:
            Statistics dictionary
        """
        orderbook = self.get_orderbook(exchange, symbol)

        if not orderbook:
            return {
                "exchange": exchange,
                "symbol": symbol,
                "exists": False
            }

        return {
            "exchange": exchange,
            "symbol": symbol,
            "exists": True,
            "sequence": orderbook.sequence,
            "timestamp": orderbook.timestamp,
            "bid_count": len(orderbook.bids),
            "ask_count": len(orderbook.asks),
            "total_orders": orderbook.get_total_orders(),
            "total_bid_volume": orderbook.get_total_volume(L3Side.BID),
            "total_ask_volume": orderbook.get_total_volume(L3Side.ASK),
            "best_bid": orderbook.get_best_bid(),
            "best_ask": orderbook.get_best_ask(),
            "spread": orderbook.get_spread(),
            "mid_price": orderbook.get_mid_price()
        }

    async def start_periodic_snapshots(self, repository: L3Repository):
        """
        Start periodic snapshot task

        Args:
            repository: L3Repository for database persistence
        """
        self.repository = repository
        self._running = True
        self._snapshot_task = asyncio.create_task(self._snapshot_loop())
        logger.info(f"Started periodic snapshots (interval: {self.snapshot_interval}s)")

    async def stop_periodic_snapshots(self):
        """Stop periodic snapshot task"""
        self._running = False

        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
            self._snapshot_task = None

        logger.info("Stopped periodic snapshots")

    async def _snapshot_loop(self):
        """Background task that periodically saves snapshots"""
        try:
            while self._running:
                await asyncio.sleep(self.snapshot_interval)

                # Save snapshot for each orderbook
                for key, orderbook in self.orderbooks.items():
                    try:
                        await self._save_snapshot(orderbook)
                    except Exception as e:
                        logger.error(f"Error saving snapshot for {key}: {e}")

        except asyncio.CancelledError:
            logger.info("Snapshot loop cancelled")
        except Exception as e:
            logger.error(f"Error in snapshot loop: {e}")

    async def _save_snapshot(self, orderbook: L3Orderbook):
        """
        Save orderbook snapshot to database

        Args:
            orderbook: L3Orderbook to save
        """
        if not self.repository:
            logger.warning("No repository configured for snapshots")
            return

        try:
            # Compress orders to simple dicts
            bids = [
                {
                    "order_id": order.order_id,
                    "price": order.price,
                    "size": order.size
                }
                for order in orderbook.bids
            ]

            asks = [
                {
                    "order_id": order.order_id,
                    "price": order.price,
                    "size": order.size
                }
                for order in orderbook.asks
            ]

            snapshot = L3Snapshot(
                exchange=orderbook.exchange,
                symbol=orderbook.symbol,
                sequence=orderbook.sequence,
                timestamp=orderbook.timestamp,
                bids=bids,
                asks=asks,
                total_bid_orders=len(bids),
                total_ask_orders=len(asks),
                total_bid_volume=orderbook.get_total_volume(L3Side.BID),
                total_ask_volume=orderbook.get_total_volume(L3Side.ASK)
            )

            success = await self.repository.save_snapshot(snapshot)

            if success:
                logger.info(
                    f"Saved snapshot for {orderbook.exchange} {orderbook.symbol}: "
                    f"{len(bids)} bids, {len(asks)} asks, seq={orderbook.sequence}"
                )

        except Exception as e:
            logger.error(f"Error creating snapshot: {e}")

    async def recover_from_snapshot(
        self,
        exchange: str,
        symbol: str,
        repository: L3Repository
    ) -> bool:
        """
        Recover orderbook from latest database snapshot

        Args:
            exchange: Exchange name
            symbol: Trading pair
            repository: L3Repository for database access

        Returns:
            True if recovery successful
        """
        try:
            snapshot = await repository.get_latest_snapshot(exchange, symbol)

            if not snapshot:
                logger.warning(f"No snapshot found for {exchange} {symbol}")
                return False

            # Reconstruct orderbook from snapshot
            orderbook = L3Orderbook(
                exchange=exchange,
                symbol=symbol,
                sequence=snapshot.sequence,
                timestamp=snapshot.timestamp,
                bids=[],
                asks=[]
            )

            # Convert compressed orders back to L3Order objects
            for bid in snapshot.bids:
                orderbook.bids.append(L3Order(
                    exchange=exchange,
                    symbol=symbol,
                    order_id=bid["order_id"],
                    side=L3Side.BID,
                    price=bid["price"],
                    size=bid["size"],
                    event_type=L3EventType.OPEN,
                    timestamp=snapshot.timestamp,
                    sequence=snapshot.sequence
                ))

            for ask in snapshot.asks:
                orderbook.asks.append(L3Order(
                    exchange=exchange,
                    symbol=symbol,
                    order_id=ask["order_id"],
                    side=L3Side.ASK,
                    price=ask["price"],
                    size=ask["size"],
                    event_type=L3EventType.OPEN,
                    timestamp=snapshot.timestamp,
                    sequence=snapshot.sequence
                ))

            self.initialize_orderbook(orderbook)

            logger.info(
                f"Recovered orderbook from snapshot: {exchange} {symbol}, "
                f"seq={snapshot.sequence}, {len(orderbook.bids)} bids, {len(orderbook.asks)} asks"
            )

            return True

        except Exception as e:
            logger.error(f"Error recovering from snapshot: {e}")
            return False
