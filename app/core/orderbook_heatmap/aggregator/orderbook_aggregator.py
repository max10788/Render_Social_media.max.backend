"""
Orderbook Aggregator - Kombiniert Daten von mehreren Börsen
FIXED VERSION: Mit periodic snapshot generation
"""
import asyncio
import logging
from typing import Dict, List, Optional, Callable
from datetime import datetime
from collections import defaultdict

from app.core.orderbook_heatmap.models.orderbook import Orderbook, AggregatedOrderbook, Exchange
from app.core.orderbook_heatmap.models.heatmap import HeatmapSnapshot, PriceLevel, HeatmapTimeSeries, HeatmapConfig
from app.core.orderbook_heatmap.exchanges.base import BaseExchange 
from app.core.orderbook_heatmap.exchanges.binance import BinanceExchange
from app.core.orderbook_heatmap.exchanges.bitget import BitgetExchange
from app.core.orderbook_heatmap.exchanges.kraken import KrakenExchange
from app.core.orderbook_heatmap.exchanges.dex.uniswap_v3 import UniswapV3Exchange

logger = logging.getLogger(__name__)


class OrderbookAggregator:
    """
    Aggregiert Orderbuch-Daten von mehreren Börsen und generiert Heatmap-Daten
    """
    
    def __init__(self, config: Optional[HeatmapConfig] = None):
        self.config = config or HeatmapConfig()
        self.exchanges: Dict[str, BaseExchange] = {}
        self.current_orderbooks: Dict[str, Orderbook] = {}
        self.heatmap_timeseries: Dict[str, HeatmapTimeSeries] = {}
        self.update_callbacks: List[Callable] = []
        self.symbols: set = set()  # FIXED: Track active symbols
        
        # Locks für Thread-Safety
        self._orderbook_lock = asyncio.Lock()
        self._heatmap_lock = asyncio.Lock()
        
        # Tasks
        self._dex_poll_task: Optional[asyncio.Task] = None
        self._snapshot_task: Optional[asyncio.Task] = None  # FIXED: Periodic snapshot generation
        
    def add_exchange(self, exchange: BaseExchange):
        """Fügt eine Börse hinzu"""
        exchange_name = exchange.exchange.value
        self.exchanges[exchange_name] = exchange
        
        # Setze Callback für Orderbuch-Updates
        exchange.set_orderbook_callback(self._on_orderbook_update)
        
        logger.info(f"Added exchange: {exchange_name}")
    
    async def connect_all(self, symbol: str, dex_pool_addresses: Optional[Dict[str, str]] = None):
        """
        Verbindet alle Börsen
        
        Args:
            symbol: Trading Pair (z.B. "BTC/USDT")
            dex_pool_addresses: Dict mit DEX Namen und Pool Addresses
        """
        tasks = []
        
        for exchange_name, exchange in self.exchanges.items():
            # CEX nutzen normales Symbol
            if exchange.exchange_type.value == "cex":
                tasks.append(exchange.connect(symbol))
            # DEX nutzen Pool Address
            elif exchange.exchange_type.value == "dex":
                if dex_pool_addresses and exchange_name in dex_pool_addresses:
                    pool_address = dex_pool_addresses[exchange_name]
                    tasks.append(exchange.connect(pool_address))
                else:
                    logger.warning(f"No pool address provided for {exchange_name}")
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Logge Ergebnisse
        for exchange_name, result in zip(self.exchanges.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Failed to connect {exchange_name}: {result}")
            elif result:
                logger.info(f"Successfully connected to {exchange_name}")
            else:
                logger.warning(f"Failed to connect to {exchange_name}")
        
        # Starte DEX Polling
        if dex_pool_addresses:
            self._dex_poll_task = asyncio.create_task(
                self._poll_dex_orderbooks(symbol, dex_pool_addresses)
            )
        
        # FIXED: Track symbol und starte Snapshot Task
        self.symbols.add(symbol)
        
        if not self._snapshot_task or self._snapshot_task.done():
            self._snapshot_task = asyncio.create_task(
                self._generate_periodic_snapshots()
            )
            logger.info("🔄 Started periodic snapshot generation")
        
        logger.info(f"Connected to {sum(1 for r in results if r)} / {len(results)} exchanges")
    
    async def disconnect_all(self):
        """Trennt alle Börsen"""
        # FIXED: Stoppe Snapshot Task
        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass
        
        # Stoppe DEX Polling
        if self._dex_poll_task:
            self._dex_poll_task.cancel()
            try:
                await self._dex_poll_task
            except asyncio.CancelledError:
                pass
        
        # Clear symbols
        self.symbols.clear()
        
        tasks = [exchange.disconnect() for exchange in self.exchanges.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Disconnected from all exchanges")
    
    async def _poll_dex_orderbooks(self, symbol: str, pool_addresses: Dict[str, str]):
        """Pollt DEX Orderbooks regelmäßig"""
        while True:
            try:
                await asyncio.sleep(self.config.time_window_seconds)
                
                for exchange_name, pool_address in pool_addresses.items():
                    if exchange_name in self.exchanges:
                        exchange = self.exchanges[exchange_name]
                        
                        if exchange.exchange_type.value == "dex":
                            orderbook = await exchange.get_orderbook_snapshot(pool_address)
                            if orderbook:
                                await self._on_orderbook_update(orderbook)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error polling DEX orderbooks: {e}")
    
    async def _generate_periodic_snapshots(self):
        """
        FIXED: Generiert periodisch Snapshots (jede Sekunde)
        
        Diese Funktion läuft im Hintergrund und erstellt regelmäßig
        Snapshots, unabhängig davon ob Callbacks aufgerufen werden.
        """
        logger.info("🔄 Periodic snapshot generation started")
        
        while True:
            try:
                await asyncio.sleep(1)  # Jede Sekunde
                
                for symbol in list(self.symbols):
                    try:
                        # Prüfe ob Orderbücher vorhanden
                        if not self.current_orderbooks:
                            logger.debug(f"No orderbooks available yet for {symbol}")
                            continue
                        
                        # Generiere Snapshot
                        await self._update_heatmap(symbol)
                        
                        # Trigger Callbacks
                        await self._notify_callbacks()
                        
                        logger.debug(f"✅ Generated snapshot for {symbol}")
                        
                    except Exception as e:
                        logger.error(f"Error generating snapshot for {symbol}: {e}")
                
            except asyncio.CancelledError:
                logger.info("Periodic snapshot generation stopped")
                break
            except Exception as e:
                logger.error(f"Error in periodic snapshot generation: {e}")
    
    async def _on_orderbook_update(self, orderbook: Orderbook):
        """
        Callback für Orderbuch-Updates
        
        Args:
            orderbook: Neues Orderbuch
        """
        async with self._orderbook_lock:
            exchange_name = orderbook.exchange.value
            self.current_orderbooks[exchange_name] = orderbook
            
            logger.info(  # FIXED: Changed from debug to info
                f"✅ Orderbook update from {exchange_name}: "
                f"{len(orderbook.bids.levels)} bids, {len(orderbook.asks.levels)} asks"
            )
        
        # NOTE: Snapshot generation now handled by periodic task
        # No need to generate here anymore
    
    async def _update_heatmap(self, symbol: str):
        """
        Aktualisiert Heatmap-Daten
        
        Args:
            symbol: Trading Pair
        """
        async with self._heatmap_lock:
            # Erstelle Heatmap-Snapshot
            snapshot = await self._create_heatmap_snapshot(symbol)
            
            if snapshot:
                # Füge zu TimeSeries hinzu
                if symbol not in self.heatmap_timeseries:
                    self.heatmap_timeseries[symbol] = HeatmapTimeSeries(
                        symbol=symbol,
                        max_snapshots=self.config.time_window_seconds
                    )
                
                self.heatmap_timeseries[symbol].add_snapshot(snapshot)
    
    async def _create_heatmap_snapshot(self, symbol: str) -> Optional[HeatmapSnapshot]:
        """
        Erstellt Heatmap-Snapshot aus aktuellen Orderbüchern
        
        Args:
            symbol: Trading Pair
            
        Returns:
            HeatmapSnapshot oder None
        """
        if not self.current_orderbooks:
            return None
        
        # Sammle alle Preise
        all_prices = set()
        for ob in self.current_orderbooks.values():
            for level in ob.bids.levels:
                all_prices.add(level.price)
            for level in ob.asks.levels:
                all_prices.add(level.price)
        
        if not all_prices:
            return None
        
        # Erstelle Preis-Buckets
        min_price = min(all_prices)
        max_price = max(all_prices)
        
        # Quantisiere Preise
        price_levels_dict: Dict[float, PriceLevel] = {}
        
        for price in all_prices:
            # Runde auf Bucket-Größe
            bucket_price = self._quantize_price(price)
            
            if bucket_price not in price_levels_dict:
                price_levels_dict[bucket_price] = PriceLevel(price=bucket_price)
        
        # Füge Liquidität von allen Börsen hinzu
        for exchange_name, orderbook in self.current_orderbooks.items():
            # Bids
            for level in orderbook.bids.levels:
                bucket_price = self._quantize_price(level.price)
                if bucket_price in price_levels_dict:
                    current = price_levels_dict[bucket_price].liquidity_by_exchange.get(exchange_name, 0.0)
                    price_levels_dict[bucket_price].add_liquidity(
                        exchange_name, 
                        current + level.quantity
                    )
            
            # Asks
            for level in orderbook.asks.levels:
                bucket_price = self._quantize_price(level.price)
                if bucket_price in price_levels_dict:
                    current = price_levels_dict[bucket_price].liquidity_by_exchange.get(exchange_name, 0.0)
                    price_levels_dict[bucket_price].add_liquidity(
                        exchange_name, 
                        current + level.quantity
                    )
        
        # Sortiere PriceLevels
        price_levels = sorted(price_levels_dict.values(), key=lambda x: x.price)
        
        return HeatmapSnapshot(
            symbol=symbol,
            price_levels=price_levels,
            min_price=min_price,
            max_price=max_price
        )
    
    def _quantize_price(self, price: float) -> float:
        """
        Quantisiert Preis auf Bucket-Größe
        
        Args:
            price: Original-Preis
            
        Returns:
            Quantisierter Preis
        """
        bucket_size = self.config.price_bucket_size
        return round(price / bucket_size) * bucket_size
    
    async def get_aggregated_orderbook(self, symbol: str) -> AggregatedOrderbook:
        """
        Holt aggregiertes Orderbuch
        
        Args:
            symbol: Trading Pair
            
        Returns:
            AggregatedOrderbook
        """
        async with self._orderbook_lock:
            return AggregatedOrderbook(
                symbol=symbol,
                orderbooks=self.current_orderbooks.copy()
            )
    
    async def get_latest_heatmap(self, symbol: str) -> Optional[HeatmapSnapshot]:
        """
        Holt neuesten Heatmap-Snapshot
        
        Args:
            symbol: Trading Pair
            
        Returns:
            HeatmapSnapshot oder None
        """
        async with self._heatmap_lock:
            if symbol in self.heatmap_timeseries:
                return self.heatmap_timeseries[symbol].get_latest()
            return None
    
    async def get_heatmap_timeseries(self, symbol: str) -> Optional[HeatmapTimeSeries]:
        """
        Holt Heatmap-TimeSeries
        
        Args:
            symbol: Trading Pair
            
        Returns:
            HeatmapTimeSeries oder None
        """
        async with self._heatmap_lock:
            return self.heatmap_timeseries.get(symbol)
    
    def add_update_callback(self, callback: Callable):
        """Fügt Callback für Updates hinzu"""
        self.update_callbacks.append(callback)
    
    async def _notify_callbacks(self):
        """Ruft alle registrierten Callbacks auf"""
        for callback in self.update_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                logger.error(f"Error in update callback: {e}")
    
    def get_status(self) -> Dict:
        """Gibt Status aller Börsen zurück"""
        return {
            "exchanges": {
                name: exchange.get_status()
                for name, exchange in self.exchanges.items()
            },
            "orderbooks": {
                name: {
                    "bids": len(ob.bids.levels),
                    "asks": len(ob.asks.levels),
                    "spread": ob.get_spread(),
                    "mid_price": ob.get_mid_price()
                }
                for name, ob in self.current_orderbooks.items()
            },
            "config": {
                "price_bucket_size": self.config.price_bucket_size,
                "time_window_seconds": self.config.time_window_seconds,
                "exchanges": self.config.exchanges
            }
        }
