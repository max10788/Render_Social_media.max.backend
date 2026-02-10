"""
Level 3 order book repository for database operations

Handles CRUD operations for L3 orders and snapshots.
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.orderbook_heatmap.models.level3 import L3Order, L3Orderbook, L3Snapshot
from app.core.backend_crypto_tracker.config.database import get_async_db, AsyncSessionLocal


logger = logging.getLogger(__name__)


class L3Repository:
    """Repository for Level 3 order book data persistence"""

    def __init__(self, session: Optional[AsyncSession] = None):
        """
        Initialize repository

        Args:
            session: Optional existing session, otherwise create new one
        """
        self.session = session
        self._own_session = session is None

    async def __aenter__(self):
        """Context manager entry"""
        if self._own_session:
            self.session = AsyncSessionLocal()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self._own_session and self.session:
            await self.session.close()

    async def save_order(self, order: L3Order) -> bool:
        """
        Save a single L3 order to database

        Args:
            order: L3Order instance

        Returns:
            True if successful
        """
        try:
            query = text("""
                INSERT INTO otc_analysis.level3_orders
                (exchange, symbol, order_id, sequence, side, price, size, event_type, timestamp, metadata)
                VALUES
                (:exchange, :symbol, :order_id, :sequence, :side, :price, :size, :event_type, :timestamp, :metadata)
            """)

            await self.session.execute(query, {
                "exchange": order.exchange,
                "symbol": order.symbol,
                "order_id": order.order_id,
                "sequence": order.sequence,
                "side": order.side.value,
                "price": order.price,
                "size": order.size,
                "event_type": order.event_type.value,
                "timestamp": order.timestamp,
                "metadata": order.metadata
            })

            await self.session.commit()
            return True

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error saving order {order.order_id}: {e}")
            return False

    async def save_orders_batch(self, orders: List[L3Order]) -> int:
        """
        Save multiple orders in a single transaction (bulk insert)

        Args:
            orders: List of L3Order instances

        Returns:
            Number of orders successfully saved
        """
        if not orders:
            return 0

        try:
            # Prepare batch data
            values = []
            for order in orders:
                values.append({
                    "exchange": order.exchange,
                    "symbol": order.symbol,
                    "order_id": order.order_id,
                    "sequence": order.sequence,
                    "side": order.side.value,
                    "price": float(order.price),
                    "size": float(order.size),
                    "event_type": order.event_type.value,
                    "timestamp": order.timestamp,
                    "metadata": order.metadata
                })

            # Execute batch insert
            query = text("""
                INSERT INTO otc_analysis.level3_orders
                (exchange, symbol, order_id, sequence, side, price, size, event_type, timestamp, metadata)
                VALUES
                (:exchange, :symbol, :order_id, :sequence, :side, :price, :size, :event_type, :timestamp, :metadata)
            """)

            await self.session.execute(query, values)
            await self.session.commit()

            logger.info(f"Saved {len(orders)} orders to database")
            return len(orders)

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error saving batch of {len(orders)} orders: {e}")
            return 0

    async def save_snapshot(self, snapshot: L3Snapshot) -> bool:
        """
        Save a full L3 orderbook snapshot

        Args:
            snapshot: L3Snapshot instance

        Returns:
            True if successful
        """
        try:
            query = text("""
                INSERT INTO otc_analysis.level3_snapshots
                (exchange, symbol, sequence, timestamp, bids, asks,
                 total_bid_orders, total_ask_orders, total_bid_volume, total_ask_volume)
                VALUES
                (:exchange, :symbol, :sequence, :timestamp, :bids, :asks,
                 :total_bid_orders, :total_ask_orders, :total_bid_volume, :total_ask_volume)
            """)

            await self.session.execute(query, {
                "exchange": snapshot.exchange,
                "symbol": snapshot.symbol,
                "sequence": snapshot.sequence,
                "timestamp": snapshot.timestamp,
                "bids": snapshot.bids,
                "asks": snapshot.asks,
                "total_bid_orders": snapshot.total_bid_orders,
                "total_ask_orders": snapshot.total_ask_orders,
                "total_bid_volume": float(snapshot.total_bid_volume),
                "total_ask_volume": float(snapshot.total_ask_volume)
            })

            await self.session.commit()
            logger.info(f"Saved snapshot for {snapshot.exchange} {snapshot.symbol} (seq: {snapshot.sequence})")
            return True

        except Exception as e:
            await self.session.rollback()
            logger.error(f"Error saving snapshot: {e}")
            return False

    async def get_orders(
        self,
        exchange: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Query historical L3 orders

        Args:
            exchange: Exchange name
            symbol: Trading pair
            start_time: Start timestamp
            end_time: End timestamp
            limit: Maximum number of orders to return
            offset: Pagination offset

        Returns:
            List of order dictionaries
        """
        try:
            query = text("""
                SELECT
                    id, exchange, symbol, order_id, sequence, side,
                    price, size, event_type, timestamp, metadata, created_at
                FROM otc_analysis.level3_orders
                WHERE exchange = :exchange
                AND symbol = :symbol
                AND timestamp >= :start_time
                AND timestamp <= :end_time
                ORDER BY timestamp DESC, sequence DESC
                LIMIT :limit OFFSET :offset
            """)

            result = await self.session.execute(query, {
                "exchange": exchange,
                "symbol": symbol,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
                "offset": offset
            })

            rows = result.fetchall()

            orders = []
            for row in rows:
                orders.append({
                    "id": row[0],
                    "exchange": row[1],
                    "symbol": row[2],
                    "order_id": row[3],
                    "sequence": row[4],
                    "side": row[5],
                    "price": float(row[6]),
                    "size": float(row[7]),
                    "event_type": row[8],
                    "timestamp": row[9],
                    "metadata": row[10],
                    "created_at": row[11]
                })

            return orders

        except Exception as e:
            logger.error(f"Error querying orders: {e}")
            return []

    async def get_latest_snapshot(
        self,
        exchange: str,
        symbol: str
    ) -> Optional[L3Snapshot]:
        """
        Get the most recent snapshot for recovery

        Args:
            exchange: Exchange name
            symbol: Trading pair

        Returns:
            L3Snapshot or None
        """
        try:
            query = text("""
                SELECT
                    exchange, symbol, sequence, timestamp, bids, asks,
                    total_bid_orders, total_ask_orders, total_bid_volume, total_ask_volume
                FROM otc_analysis.level3_snapshots
                WHERE exchange = :exchange AND symbol = :symbol
                ORDER BY sequence DESC
                LIMIT 1
            """)

            result = await self.session.execute(query, {
                "exchange": exchange,
                "symbol": symbol
            })

            row = result.fetchone()

            if not row:
                return None

            return L3Snapshot(
                exchange=row[0],
                symbol=row[1],
                sequence=row[2],
                timestamp=row[3],
                bids=row[4],
                asks=row[5],
                total_bid_orders=row[6],
                total_ask_orders=row[7],
                total_bid_volume=float(row[8]),
                total_ask_volume=float(row[9])
            )

        except Exception as e:
            logger.error(f"Error fetching latest snapshot: {e}")
            return None

    async def rebuild_orderbook(
        self,
        exchange: str,
        symbol: str,
        from_sequence: int
    ) -> Optional[L3Orderbook]:
        """
        Rebuild orderbook from events after a sequence number

        Reconstructs the full orderbook state by replaying all order events
        from a starting sequence number.

        Args:
            exchange: Exchange name
            symbol: Trading pair
            from_sequence: Starting sequence number

        Returns:
            L3Orderbook or None
        """
        try:
            # Get snapshot as starting point
            snapshot = await self.get_latest_snapshot(exchange, symbol)

            if not snapshot or snapshot.sequence < from_sequence:
                logger.warning(f"No suitable snapshot found for {exchange} {symbol}")
                return None

            # Build initial orderbook from snapshot
            orderbook = L3Orderbook(
                exchange=exchange,
                symbol=symbol,
                sequence=snapshot.sequence,
                timestamp=snapshot.timestamp,
                bids=[],
                asks=[]
            )

            # Convert snapshot data to L3Order objects
            from app.core.orderbook_heatmap.models.level3 import L3Side, L3EventType

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

            # Apply subsequent events
            query = text("""
                SELECT order_id, side, price, size, event_type, timestamp, sequence
                FROM otc_analysis.level3_orders
                WHERE exchange = :exchange
                AND symbol = :symbol
                AND sequence > :from_sequence
                ORDER BY sequence ASC
            """)

            result = await self.session.execute(query, {
                "exchange": exchange,
                "symbol": symbol,
                "from_sequence": from_sequence
            })

            orders_dict = {}  # Track orders by ID

            for row in result.fetchall():
                order_id = row[0]
                side = row[1]
                price = float(row[2])
                size = float(row[3])
                event_type = row[4]
                timestamp = row[5]
                sequence = row[6]

                if event_type == "open":
                    # Add new order
                    order = L3Order(
                        exchange=exchange,
                        symbol=symbol,
                        order_id=order_id,
                        side=L3Side(side),
                        price=price,
                        size=size,
                        event_type=L3EventType.OPEN,
                        timestamp=timestamp,
                        sequence=sequence
                    )
                    orders_dict[order_id] = order

                    if side == "bid":
                        orderbook.bids.append(order)
                    else:
                        orderbook.asks.append(order)

                elif event_type in ["done", "match"]:
                    # Remove order
                    if order_id in orders_dict:
                        order = orders_dict[order_id]
                        if order.side == L3Side.BID:
                            orderbook.bids = [o for o in orderbook.bids if o.order_id != order_id]
                        else:
                            orderbook.asks = [o for o in orderbook.asks if o.order_id != order_id]
                        del orders_dict[order_id]

                elif event_type == "change":
                    # Update order size
                    if order_id in orders_dict:
                        orders_dict[order_id].size = size

                orderbook.sequence = sequence
                orderbook.timestamp = timestamp

            logger.info(f"Rebuilt orderbook for {exchange} {symbol}: {len(orderbook.bids)} bids, {len(orderbook.asks)} asks")
            return orderbook

        except Exception as e:
            logger.error(f"Error rebuilding orderbook: {e}")
            return None

    async def get_statistics(self, exchange: str, symbol: str) -> Dict[str, Any]:
        """
        Get statistics for L3 data collection

        Args:
            exchange: Exchange name
            symbol: Trading pair

        Returns:
            Dictionary with statistics
        """
        try:
            query = text("""
                SELECT
                    COUNT(*) as total_orders,
                    MIN(timestamp) as first_order,
                    MAX(timestamp) as last_order,
                    COUNT(DISTINCT order_id) as unique_orders
                FROM otc_analysis.level3_orders
                WHERE exchange = :exchange AND symbol = :symbol
            """)

            result = await self.session.execute(query, {
                "exchange": exchange,
                "symbol": symbol
            })

            row = result.fetchone()

            snapshot_query = text("""
                SELECT COUNT(*) FROM otc_analysis.level3_snapshots
                WHERE exchange = :exchange AND symbol = :symbol
            """)

            snapshot_result = await self.session.execute(snapshot_query, {
                "exchange": exchange,
                "symbol": symbol
            })

            snapshot_count = snapshot_result.scalar()

            return {
                "exchange": exchange,
                "symbol": symbol,
                "total_orders": row[0] if row else 0,
                "first_order": row[1] if row else None,
                "last_order": row[2] if row else None,
                "unique_orders": row[3] if row else 0,
                "snapshots": snapshot_count or 0
            }

        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
