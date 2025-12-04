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
    
    async def connect_all(
        self, 
        symbol: str, 
        dex_pool_addresses: Optional[Dict[str, str]] = None
    ):
        """
        Verbindet alle Börsen (CEX und DEX)
        
        Args:
            symbol: Trading Pair (z.B. "BTC/USDT")
            dex_pool_addresses: Optional Dict mit DEX Namen und Pool Addresses
                               z.B. {"uniswap_v3": "0x88e6a0c..."}
        """
        tasks = []
        
        for exchange_name, exchange in self.exchanges.items():
            exchange_type = self._detect_exchange_type(exchange_name)
            
            # CEX nutzen normales Symbol
            if exchange_type == "CEX":
                tasks.append(exchange.connect(symbol))
                logger.info(f"📡 Connecting to CEX: {exchange_name}")
            
            # DEX nutzen Pool Address
            elif exchange_type == "DEX":
                if dex_pool_addresses and exchange_name in dex_pool_addresses:
                    pool_address = dex_pool_addresses[exchange_name]
                    tasks.append(exchange.connect(pool_address))
                    logger.info(f"🔗 Connecting to DEX: {exchange_name} (pool: {pool_address[:10]}...)")
                else:
                    logger.warning(f"⚠️ No pool address provided for DEX: {exchange_name}")
        
        # Führe alle Connections parallel aus
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Logge Ergebnisse
        success_count = 0
        for exchange_name, result in zip(self.exchanges.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"❌ Failed to connect {exchange_name}: {result}")
            elif result:
                logger.info(f"✅ Successfully connected to {exchange_name}")
                success_count += 1
            else:
                logger.warning(f"⚠️ Failed to connect to {exchange_name}")
        
        # Starte DEX Polling falls DEX vorhanden
        if dex_pool_addresses:
            self._dex_poll_task = asyncio.create_task(
                self._poll_dex_orderbooks(symbol, dex_pool_addresses)
            )
            logger.info("🔄 Started DEX polling task")
        
        # Track symbol und starte Snapshot Task
        self.symbols.add(symbol)
        
        if not self._snapshot_task or self._snapshot_task.done():
            self._snapshot_task = asyncio.create_task(
                self._generate_periodic_snapshots()
            )
            logger.info("🔄 Started periodic snapshot generation")
        
        logger.info(f"📊 Connected to {success_count} / {len(results)} exchanges")
        
        return success_count > 0

    
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


    async def _aggregate_dex_liquidity(
        self,
        dex_orderbooks: Dict[str, 'Orderbook']
    ) -> Dict[float, Dict[str, float]]:
        """
        Aggregiert Liquidität von mehreren DEX-Quellen
        
        Im Gegensatz zur CEX-Aggregation (die Orderbücher merged),
        kombiniert diese Methode Liquiditätskurven von verschiedenen Pools.
        
        Args:
            dex_orderbooks: Dict mit DEX-Namen -> Orderbook
            
        Returns:
            Dict mit Preis -> {dex_name: liquidity_amount}
        """
        from collections import defaultdict
        
        # Gruppiere nach Preis-Bucket
        price_buckets = defaultdict(lambda: {"total_liquidity": 0.0, "sources": {}})
        
        for dex_name, orderbook in dex_orderbooks.items():
            # Bids
            for level in orderbook.bids.levels:
                bucket_price = self._quantize_price(level.price)
                price_buckets[bucket_price]["total_liquidity"] += level.quantity
                price_buckets[bucket_price]["sources"][dex_name] = \
                    price_buckets[bucket_price]["sources"].get(dex_name, 0.0) + level.quantity
            
            # Asks
            for level in orderbook.asks.levels:
                bucket_price = self._quantize_price(level.price)
                price_buckets[bucket_price]["total_liquidity"] += level.quantity
                price_buckets[bucket_price]["sources"][dex_name] = \
                    price_buckets[bucket_price]["sources"].get(dex_name, 0.0) + level.quantity
        
        # Berechne effektive Tiefe (berücksichtigt Slippage)
        for bucket_data in price_buckets.values():
            bucket_data["effective_depth"] = self._calculate_effective_depth(
                bucket_data["total_liquidity"],
                list(bucket_data["sources"].values())
            )
        
        return price_buckets
    
    def _calculate_effective_depth(
        self,
        total_liquidity: float,
        source_liquidities: List[float],
        current_price: float = None,
        trade_size: float = None
    ) -> float:
        """
        Berechnet effektive Tiefe unter Berücksichtigung von AMM Slippage
        
        Verwendet die Constant Product Formula (x * y = k) um reale Trading-Tiefe
        zu berechnen. Bei AMMs ist Slippage nicht linear - große Trades haben
        exponentiell höheres Slippage.
        
        Args:
            total_liquidity: Gesamt-Liquidität im Pool
            source_liquidities: Liquidität pro Quelle (für Multi-Pool Adjustierung)
            current_price: Aktueller Preis (optional, für präzise Berechnung)
            trade_size: Erwartete Trade-Größe (optional, default: 1% des Pools)
            
        Returns:
            Effektive Tiefe (adjustiert für realistisches Slippage)
        """
        if total_liquidity == 0:
            return 0.0
        
        # ========================================================================
        # 1. POOL FRAGMENTATION ADJUSTIERUNG
        # ========================================================================
        # Wenn Liquidität über viele Quellen verteilt ist, ist effektive Tiefe niedriger
        # (man muss mehrere Pools nutzen = höhere Kosten)
        
        if len(source_liquidities) > 1:
            # Herfindahl-Hirschman Index (HHI) für Konzentration
            # HHI = sum(market_share^2), Range: [0, 1]
            # HHI = 1 → Monopol (eine Quelle)
            # HHI → 0 → perfekte Verteilung
            market_shares = [liq / total_liquidity for liq in source_liquidities if total_liquidity > 0]
            hhi = sum(share ** 2 for share in market_shares)
            
            # Fragmentation Penalty: 0-30% basierend auf HHI
            # Niedrige HHI = hohe Fragmentierung = höheres Penalty
            fragmentation_penalty = (1 - hhi) * 0.3  # 0% bis 30%
        else:
            fragmentation_penalty = 0.0
        
        # ========================================================================
        # 2. AMM SLIPPAGE BERECHNUNG (Constant Product Formula)
        # ========================================================================
        # Für Uniswap v3: Δy = L * (√P_1 - √P_0)
        # Vereinfacht: price_impact = trade_size / (2 * liquidity)
        
        if trade_size is None:
            # Default: Simuliere 1% Trade des Pool-Volumens
            trade_size = total_liquidity * 0.01
        
        if current_price is not None and current_price > 0:
            # Präzise AMM Formula
            # Für swap von amount_in zu amount_out:
            # amount_out = (amount_in * reserve_out) / (reserve_in + amount_in)
            
            # Approximiere Reserves aus Liquidität
            # L = sqrt(x * y), bei price = y/x
            # Dann: x = L / sqrt(P), y = L * sqrt(P)
            
            sqrt_price = current_price ** 0.5
            reserve_in = total_liquidity / sqrt_price
            reserve_out = total_liquidity * sqrt_price
            
            # Berechne amount_out für trade_size
            amount_out = (trade_size * reserve_out) / (reserve_in + trade_size)
            
            # Expected amount out ohne Slippage
            expected_out = trade_size * current_price
            
            # Slippage als Prozent
            if expected_out > 0:
                slippage_pct = 1 - (amount_out / expected_out)
                slippage_pct = max(0.0, min(1.0, slippage_pct))  # Clip [0, 1]
            else:
                slippage_pct = 0.0
        else:
            # Vereinfachte lineare Approximation
            # Slippage ≈ trade_size / (2 * liquidity)
            # Dies ist die First-Order Taylor Expansion der AMM Formula
            slippage_pct = min(1.0, trade_size / (2 * total_liquidity))
        
        # ========================================================================
        # 3. KOMBINIERTE ADJUSTIERUNG
        # ========================================================================
        # Effektive Tiefe = Liquidität * (1 - total_penalty)
        
        # Gesamtes Penalty: Slippage + Fragmentierung
        # Verwende Max um konservativ zu sein
        total_penalty = max(slippage_pct, fragmentation_penalty)
        
        # Zusätzlich: Gas Cost Adjustierung für DEX
        # Bei kleinen Pools sind Gas Costs relativ hoch
        if total_liquidity < 10000:  # Threshold: $10k
            gas_penalty = 0.05  # 5% extra für geringe Liquidität
            total_penalty = min(1.0, total_penalty + gas_penalty)
        
        # Berechne effektive Tiefe
        effective_depth = total_liquidity * (1 - total_penalty)
        
        # ========================================================================
        # 4. QUALITY ADJUSTIERUNG
        # ========================================================================
        # DEX Liquidität ist "weicher" als CEX (kein instant execution guarantee)
        # Reduziere effektive Tiefe um 10-20% für DEX vs CEX Vergleichbarkeit
        
        dex_discount = 0.15  # 15% Discount für DEX vs CEX
        effective_depth *= (1 - dex_discount)
        
        return max(0.0, effective_depth)
    
    def _detect_exchange_type(self, exchange_name: str) -> str:
        """
        Erkennt ob Exchange CEX oder DEX ist
        
        Args:
            exchange_name: Name der Exchange
            
        Returns:
            "CEX" oder "DEX"
        """
        dex_indicators = ["uniswap", "raydium", "curve", "balancer", "pancake", "sushi"]
        
        exchange_lower = exchange_name.lower()
        
        for indicator in dex_indicators:
            if indicator in exchange_lower:
                return "DEX"
        
        return "CEX"
    
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
