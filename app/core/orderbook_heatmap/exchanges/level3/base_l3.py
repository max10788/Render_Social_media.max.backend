"""
Base class for Level 3 exchange implementations

Extends the standard CEXExchange with L3-specific methods.
"""
from abc import abstractmethod
from typing import Optional, Dict, Any, Callable
import asyncio
import logging

from app.core.orderbook_heatmap.exchanges.base import CEXExchange
from app.core.orderbook_heatmap.models.orderbook import Exchange
from app.core.orderbook_heatmap.models.level3 import L3Order, L3Orderbook


logger = logging.getLogger(__name__)


class L3Exchange(CEXExchange):
    """
    Base class for Level 3 exchange implementations

    Provides abstract methods for L3 snapshot fetching and WebSocket streaming.
    """

    def __init__(self, exchange: Exchange):
        super().__init__(exchange)
        self.l3_callback: Optional[Callable[[L3Order], None]] = None
        self.l3_snapshot_callback: Optional[Callable[[L3Orderbook], None]] = None
        self.current_sequence: int = 0
        self._l3_ws_task: Optional[asyncio.Task] = None

    @abstractmethod
    async def get_l3_snapshot(self, symbol: str) -> Optional[L3Orderbook]:
        """
        Fetch initial L3 orderbook snapshot via REST API

        Args:
            symbol: Trading pair (e.g. "BTC/USDT")

        Returns:
            L3Orderbook with full order book state, or None on error
        """
        pass

    @abstractmethod
    async def subscribe_l3_updates(self, symbol: str):
        """
        Subscribe to Level 3 WebSocket updates

        This method should:
        1. Connect to the exchange's L3 WebSocket feed
        2. Subscribe to the specified symbol
        3. Call parse_l3_event for each message
        4. Call _emit_l3_order for each parsed order

        Args:
            symbol: Trading pair to subscribe to
        """
        pass

    @abstractmethod
    def parse_l3_event(self, data: Dict[str, Any]) -> Optional[L3Order]:
        """
        Parse exchange-specific L3 message into L3Order

        Args:
            data: Raw WebSocket message data

        Returns:
            L3Order or None if message should be ignored
        """
        pass

    def set_l3_callback(self, callback: Callable[[L3Order], None]):
        """
        Set callback for individual L3 order events

        Args:
            callback: Function to call for each order update
        """
        self.l3_callback = callback

    def set_l3_snapshot_callback(self, callback: Callable[[L3Orderbook], None]):
        """
        Set callback for L3 snapshot events

        Args:
            callback: Function to call for initial snapshot
        """
        self.l3_snapshot_callback = callback

    async def _emit_l3_order(self, order: L3Order):
        """
        Emit L3 order to registered callback

        Args:
            order: L3Order instance
        """
        if self.l3_callback:
            try:
                if asyncio.iscoroutinefunction(self.l3_callback):
                    await self.l3_callback(order)
                else:
                    self.l3_callback(order)
            except Exception as e:
                logger.error(f"Error in L3 order callback for {self.exchange}: {e}")

    async def _emit_l3_snapshot(self, snapshot: L3Orderbook):
        """
        Emit L3 snapshot to registered callback

        Args:
            snapshot: L3Orderbook instance
        """
        if self.l3_snapshot_callback:
            try:
                if asyncio.iscoroutinefunction(self.l3_snapshot_callback):
                    await self.l3_snapshot_callback(snapshot)
                else:
                    self.l3_snapshot_callback(snapshot)
            except Exception as e:
                logger.error(f"Error in L3 snapshot callback for {self.exchange}: {e}")

    async def start_l3_stream(self, symbol: str):
        """
        Start L3 data streaming

        Fetches initial snapshot and starts WebSocket subscription.

        Args:
            symbol: Trading pair to stream
        """
        try:
            logger.info(f"Starting L3 stream for {self.exchange.value} {symbol}")

            # Fetch initial snapshot
            snapshot = await self.get_l3_snapshot(symbol)
            if snapshot:
                await self._emit_l3_snapshot(snapshot)
                self.current_sequence = snapshot.sequence
                logger.info(f"Received L3 snapshot: {snapshot.get_total_orders()} orders, seq={snapshot.sequence}")
            else:
                logger.warning(f"Failed to fetch L3 snapshot for {symbol}")

            # Start WebSocket stream
            self._l3_ws_task = asyncio.create_task(self.subscribe_l3_updates(symbol))

            self.is_connected = True

        except Exception as e:
            logger.error(f"Error starting L3 stream: {e}")
            self.is_connected = False

    async def stop_l3_stream(self):
        """Stop L3 data streaming"""
        try:
            if self._l3_ws_task:
                self._l3_ws_task.cancel()
                try:
                    await self._l3_ws_task
                except asyncio.CancelledError:
                    pass
                self._l3_ws_task = None

            await self.disconnect()
            self.is_connected = False

            logger.info(f"Stopped L3 stream for {self.exchange.value}")

        except Exception as e:
            logger.error(f"Error stopping L3 stream: {e}")

    def get_l3_status(self) -> Dict[str, Any]:
        """
        Get L3 stream status

        Returns:
            Status dictionary
        """
        status = self.get_status()
        status.update({
            "current_sequence": self.current_sequence,
            "l3_stream_active": self._l3_ws_task is not None and not self._l3_ws_task.done()
        })
        return status
