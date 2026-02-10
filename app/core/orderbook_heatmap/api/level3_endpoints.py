"""
Level 3 Order Book API Endpoints

Provides REST and WebSocket APIs for Level 3 order data.
"""
import asyncio
import logging
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Query
from pydantic import BaseModel

from app.core.orderbook_heatmap.models.level3 import (
    L3Order, L3Orderbook, StartL3Request, L3StreamStatus
)
from app.core.orderbook_heatmap.exchanges.level3 import CoinbaseL3, BitfinexL3
from app.core.orderbook_heatmap.storage.l3_repository import L3Repository
from app.core.orderbook_heatmap.storage.snapshot_manager import SnapshotManager
from app.core.orderbook_heatmap.websocket.manager import WebSocketManager


logger = logging.getLogger(__name__)


# Router
router = APIRouter(prefix="/api/v1/orderbook-heatmap/level3", tags=["Level 3 Order Book"])


# Global state management
class L3StreamManager:
    """Manages active L3 data streams"""

    def __init__(self):
        self.active_streams: Dict[str, Dict] = {}  # Key: "exchange:symbol"
        self.exchanges: Dict[str, any] = {}
        self.snapshot_managers: Dict[str, SnapshotManager] = {}
        self.ws_manager = WebSocketManager()
        self.repository: Optional[L3Repository] = None
        self.order_buffer: Dict[str, List[L3Order]] = {}
        self.buffer_size = 1000
        self._flush_task: Optional[asyncio.Task] = None

    def _get_key(self, exchange: str, symbol: str) -> str:
        return f"{exchange}:{symbol}"

    async def start_stream(self, request: StartL3Request) -> L3StreamStatus:
        """Start L3 data collection"""
        try:
            # Initialize repository
            if self.repository is None:
                self.repository = L3Repository()
                await self.repository.__aenter__()

            # Start flush task if not running
            if self._flush_task is None:
                self._flush_task = asyncio.create_task(self._flush_loop())

            status_list = []

            for exchange_name in request.exchanges:
                key = self._get_key(exchange_name, request.symbol)

                # Check if already running
                if key in self.active_streams:
                    logger.warning(f"Stream already active for {key}")
                    continue

                # Create exchange instance
                if exchange_name == "coinbase":
                    exchange = CoinbaseL3()
                elif exchange_name == "bitfinex":
                    exchange = BitfinexL3()
                else:
                    logger.error(f"Unsupported exchange: {exchange_name}")
                    continue

                # Set callbacks
                exchange.set_l3_callback(self._on_order_event)
                exchange.set_l3_snapshot_callback(self._on_snapshot_event)

                # Initialize snapshot manager
                snapshot_mgr = SnapshotManager(
                    snapshot_interval_seconds=request.snapshot_interval_seconds
                )
                self.snapshot_managers[key] = snapshot_mgr

                if request.persist and self.repository:
                    await snapshot_mgr.start_periodic_snapshots(self.repository)

                # Store exchange
                self.exchanges[key] = exchange

                # Initialize stream status
                self.active_streams[key] = {
                    "symbol": request.symbol,
                    "exchange": exchange_name,
                    "is_active": True,
                    "orders_received": 0,
                    "orders_persisted": 0,
                    "snapshots_taken": 0,
                    "started_at": datetime.utcnow(),
                    "last_update": None,
                    "errors": []
                }

                # Initialize order buffer
                self.order_buffer[key] = []

                # Start stream
                await exchange.start_l3_stream(request.symbol)

                logger.info(f"Started L3 stream for {key}")

            return L3StreamStatus(
                symbol=request.symbol,
                exchanges=request.exchanges,
                is_active=True,
                started_at=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"Error starting L3 stream: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def stop_stream(self, exchange: str, symbol: str):
        """Stop L3 data collection"""
        key = self._get_key(exchange, symbol)

        if key not in self.active_streams:
            raise HTTPException(status_code=404, detail="Stream not found")

        try:
            # Stop exchange stream
            if key in self.exchanges:
                await self.exchanges[key].stop_l3_stream()
                del self.exchanges[key]

            # Stop snapshot manager
            if key in self.snapshot_managers:
                await self.snapshot_managers[key].stop_periodic_snapshots()
                del self.snapshot_managers[key]

            # Flush remaining orders
            if key in self.order_buffer and self.order_buffer[key]:
                await self._flush_orders(key)

            # Remove from active streams
            del self.active_streams[key]

            logger.info(f"Stopped L3 stream for {key}")

        except Exception as e:
            logger.error(f"Error stopping L3 stream: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def _on_order_event(self, order: L3Order):
        """Callback for individual order events"""
        key = self._get_key(order.exchange, order.symbol)

        # Update stream status
        if key in self.active_streams:
            self.active_streams[key]["orders_received"] += 1
            self.active_streams[key]["last_update"] = datetime.utcnow()

        # Apply to snapshot manager
        if key in self.snapshot_managers:
            self.snapshot_managers[key].apply_order_event(order)

        # Buffer order for persistence
        if key not in self.order_buffer:
            self.order_buffer[key] = []

        self.order_buffer[key].append(order)

        # Flush if buffer full
        if len(self.order_buffer[key]) >= self.buffer_size:
            await self._flush_orders(key)

        # Broadcast to WebSocket clients
        await self.ws_manager.broadcast_l3_order(order)

    async def _on_snapshot_event(self, snapshot: L3Orderbook):
        """Callback for snapshot events"""
        key = self._get_key(snapshot.exchange, snapshot.symbol)

        # Initialize snapshot manager with snapshot
        if key in self.snapshot_managers:
            self.snapshot_managers[key].initialize_orderbook(snapshot)

        # Update stream status
        if key in self.active_streams:
            self.active_streams[key]["snapshots_taken"] += 1

        # Broadcast to WebSocket clients
        await self.ws_manager.broadcast_l3_snapshot(snapshot)

        logger.info(f"Processed snapshot for {key}: {snapshot.get_total_orders()} orders")

    async def _flush_orders(self, key: str):
        """Flush buffered orders to database"""
        if key not in self.order_buffer or not self.order_buffer[key]:
            return

        if not self.repository:
            return

        orders = self.order_buffer[key]
        self.order_buffer[key] = []

        count = await self.repository.save_orders_batch(orders)

        if key in self.active_streams:
            self.active_streams[key]["orders_persisted"] += count

    async def _flush_loop(self):
        """Background task to periodically flush orders"""
        try:
            while True:
                await asyncio.sleep(1)  # Flush every second

                for key in list(self.order_buffer.keys()):
                    try:
                        await self._flush_orders(key)
                    except Exception as e:
                        logger.error(f"Error flushing orders for {key}: {e}")

        except asyncio.CancelledError:
            logger.info("Flush loop cancelled")

    def get_stream_status(self, exchange: str, symbol: str) -> L3StreamStatus:
        """Get status of active stream"""
        key = self._get_key(exchange, symbol)

        if key not in self.active_streams:
            raise HTTPException(status_code=404, detail="Stream not found")

        status = self.active_streams[key]

        return L3StreamStatus(
            symbol=status["symbol"],
            exchanges=[status["exchange"]],
            is_active=status["is_active"],
            orders_received=status["orders_received"],
            orders_persisted=status["orders_persisted"],
            snapshots_taken=status["snapshots_taken"],
            started_at=status["started_at"],
            last_update=status["last_update"],
            errors=status["errors"]
        )


# Global stream manager instance
stream_manager = L3StreamManager()


# ============================================================================
# REST API Endpoints
# ============================================================================

@router.post("/start", response_model=L3StreamStatus)
async def start_l3_stream(request: StartL3Request):
    """
    Start Level 3 data collection

    Initiates L3 order streaming from specified exchanges.

    **Example Request:**
    ```json
    {
        "symbol": "BTC-USD",
        "exchanges": ["coinbase", "bitfinex"],
        "persist": true,
        "snapshot_interval_seconds": 60
    }
    ```
    """
    return await stream_manager.start_stream(request)


@router.post("/stop/{exchange}/{symbol}")
async def stop_l3_stream(exchange: str, symbol: str):
    """
    Stop Level 3 data collection

    **Example:** `POST /api/v1/orderbook-heatmap/level3/stop/coinbase/BTC-USD`
    """
    await stream_manager.stop_stream(exchange, symbol)
    return {"message": f"Stopped L3 stream for {exchange} {symbol}"}


@router.get("/status/{exchange}/{symbol}", response_model=L3StreamStatus)
async def get_l3_stream_status(exchange: str, symbol: str):
    """
    Get status of L3 data stream

    **Example:** `GET /api/v1/orderbook-heatmap/level3/status/coinbase/BTC-USD`
    """
    return stream_manager.get_stream_status(exchange, symbol)


@router.get("/orders/{exchange}/{symbol}")
async def get_l3_orders(
    exchange: str,
    symbol: str,
    start_time: Optional[datetime] = Query(None, description="Start timestamp"),
    end_time: Optional[datetime] = Query(None, description="End timestamp"),
    limit: int = Query(1000, ge=1, le=10000, description="Max orders to return"),
    offset: int = Query(0, ge=0, description="Pagination offset")
):
    """
    Query historical Level 3 orders

    Returns individual order events within the specified time range.

    **Example:**
    ```
    GET /api/v1/orderbook-heatmap/level3/orders/coinbase/BTC-USD?start_time=2026-02-09T00:00:00&end_time=2026-02-09T23:59:59&limit=100
    ```
    """
    # Default to last hour if no times specified
    if not end_time:
        end_time = datetime.utcnow()
    if not start_time:
        start_time = end_time - timedelta(hours=1)

    async with L3Repository() as repo:
        orders = await repo.get_orders(
            exchange=exchange,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset
        )

    return {
        "exchange": exchange,
        "symbol": symbol,
        "start_time": start_time,
        "end_time": end_time,
        "count": len(orders),
        "orders": orders
    }


@router.get("/snapshot/{exchange}/{symbol}")
async def get_l3_snapshot(exchange: str, symbol: str):
    """
    Get latest Level 3 orderbook snapshot

    Returns the current full orderbook state with all orders.

    **Example:** `GET /api/v1/orderbook-heatmap/level3/snapshot/coinbase/BTC-USD`
    """
    key = stream_manager._get_key(exchange, symbol)

    # Try to get from active stream first
    if key in stream_manager.snapshot_managers:
        orderbook = stream_manager.snapshot_managers[key].get_orderbook(exchange, symbol)
        if orderbook:
            return {
                "exchange": exchange,
                "symbol": symbol,
                "sequence": orderbook.sequence,
                "timestamp": orderbook.timestamp,
                "statistics": {
                    "total_orders": orderbook.get_total_orders(),
                    "bid_count": len(orderbook.bids),
                    "ask_count": len(orderbook.asks),
                    "total_bid_volume": orderbook.get_total_volume(),
                    "best_bid": orderbook.get_best_bid(),
                    "best_ask": orderbook.get_best_ask(),
                    "spread": orderbook.get_spread(),
                    "mid_price": orderbook.get_mid_price()
                },
                "bids": [
                    {"order_id": o.order_id, "price": o.price, "size": o.size}
                    for o in sorted(orderbook.bids, key=lambda x: x.price, reverse=True)[:100]
                ],
                "asks": [
                    {"order_id": o.order_id, "price": o.price, "size": o.size}
                    for o in sorted(orderbook.asks, key=lambda x: x.price)[:100]
                ]
            }

    # Otherwise, try database
    async with L3Repository() as repo:
        snapshot = await repo.get_latest_snapshot(exchange, symbol)

    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    return {
        "exchange": exchange,
        "symbol": symbol,
        "sequence": snapshot.sequence,
        "timestamp": snapshot.timestamp,
        "statistics": {
            "total_bid_orders": snapshot.total_bid_orders,
            "total_ask_orders": snapshot.total_ask_orders,
            "total_bid_volume": snapshot.total_bid_volume,
            "total_ask_volume": snapshot.total_ask_volume
        },
        "bids": snapshot.bids[:100],
        "asks": snapshot.asks[:100]
    }


@router.get("/statistics/{exchange}/{symbol}")
async def get_l3_statistics(exchange: str, symbol: str):
    """
    Get statistics about L3 data collection

    **Example:** `GET /api/v1/orderbook-heatmap/level3/statistics/coinbase/BTC-USD`
    """
    async with L3Repository() as repo:
        stats = await repo.get_statistics(exchange, symbol)

    # Add live stats if stream is active
    key = stream_manager._get_key(exchange, symbol)
    if key in stream_manager.snapshot_managers:
        live_stats = stream_manager.snapshot_managers[key].get_orderbook_stats(exchange, symbol)
        stats["live"] = live_stats

    return stats


# ============================================================================
# WebSocket Endpoint
# ============================================================================

@router.websocket("/ws/{symbol}")
async def l3_websocket(websocket: WebSocket, symbol: str):
    """
    Stream live Level 3 updates

    Connect to receive real-time L3 order events.

    **Connection:** `ws://backend/api/v1/orderbook-heatmap/level3/ws/BTC-USD`

    **Message Types:**
    - `l3_snapshot`: Full orderbook on connect
    - `l3_order`: Individual order events
    - `l3_statistics`: Periodic statistics updates
    """
    await stream_manager.ws_manager.connect(websocket, symbol)

    try:
        # Send initial snapshots for active streams
        for key, snapshot_mgr in stream_manager.snapshot_managers.items():
            if symbol in key:
                exchange, sym = key.split(":")
                orderbook = snapshot_mgr.get_orderbook(exchange, sym)
                if orderbook:
                    await stream_manager.ws_manager.broadcast_l3_snapshot(orderbook)

        # Keep connection alive
        while True:
            data = await websocket.receive_text()
            # Handle client messages if needed
            logger.debug(f"Received from client: {data}")

    except WebSocketDisconnect:
        stream_manager.ws_manager.disconnect(websocket)
        logger.info(f"WebSocket disconnected for {symbol}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        stream_manager.ws_manager.disconnect(websocket)
