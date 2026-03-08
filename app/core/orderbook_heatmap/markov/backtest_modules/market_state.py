"""
market_state.py — Berechnet Markt-Features aus HeatmapSnapshots
und klassifiziert den aktuellen Preis-Zustand (PriceZone).
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np

from .models import HeatmapSnapshot, PriceLevel
from .wall_filter import WallFilter, WallState


class PriceZone(Enum):
    FREE_SPACE         = "free_space"          # Keine Wall in der Nähe
    WALL_APPROACH_BID  = "wall_approach_bid"   # Nähert sich Support von oben
    WALL_APPROACH_ASK  = "wall_approach_ask"   # Nähert sich Resistance von unten
    BETWEEN_WALLS      = "between_walls"        # Eingeklemmt zwischen Bid- und Ask-Wall
    BREAKTHROUGH       = "breakthrough"         # Hat gerade eine Wall durchbrochen


@dataclass
class MarketFeatures:
    mid_price: float
    bid_ask_imbalance: float          # Geglättet, Range [-1, +1]; positiv = mehr Ask-Liq
    volatility_proxy: float           # Relative Änderungsrate der Gesamtliquidität
    liquidity_gradient: float         # Steilheit der Orderbuch-Tiefe nahe Spread
    active_walls: List[WallState]
    nearest_bid_wall: Optional[float] # Preis der nächsten Support-Wall (< mid)
    nearest_ask_wall: Optional[float] # Preis der nächsten Resistance-Wall (>= mid)
    zone: PriceZone
    wall_distances: Dict[str, float]  # {"bid_wall_dist": ..., "ask_wall_dist": ...}


class MarketStateEstimator:
    """
    Berechnet alle relevanten Features für den MarkovSimulator.
    Hält internen Zustand für gleitende Durchschnitte.
    """

    _EPS = 1e-10

    def __init__(
        self,
        wall_filter: WallFilter,
        imbalance_ma_window: int = 10,
        gradient_depth_levels: int = 5,
        wall_proximity_threshold: float = 50.0,
    ) -> None:
        self.wall_filter               = wall_filter
        self.imbalance_ma_window       = imbalance_ma_window
        self.gradient_depth_levels     = gradient_depth_levels
        self.wall_proximity_threshold  = wall_proximity_threshold

        self._imbalance_history: deque       = deque(maxlen=imbalance_ma_window)
        self._prev_total_liquidity: Optional[float] = None

    # ------------------------------------------------------------------
    # Feature-Berechnungen
    # ------------------------------------------------------------------

    def _bid_ask_imbalance(self, snapshot: HeatmapSnapshot) -> float:
        mid = snapshot.mid_price
        bid_liq = sum(pl.total_liquidity for pl in snapshot.price_levels if pl.price < mid)
        ask_liq = sum(pl.total_liquidity for pl in snapshot.price_levels if pl.price >= mid)
        total   = bid_liq + ask_liq
        raw     = (ask_liq - bid_liq) / (total + self._EPS)
        self._imbalance_history.append(raw)
        return float(np.mean(self._imbalance_history))

    def _volatility_proxy(self, total_liq: float) -> float:
        if self._prev_total_liquidity is None:
            vol = 0.0
        else:
            vol = abs(total_liq - self._prev_total_liquidity) / (
                self._prev_total_liquidity + self._EPS
            )
        self._prev_total_liquidity = total_liq
        return vol

    def _liquidity_gradient(self, snapshot: HeatmapSnapshot) -> float:
        """
        Lineare Regression von |price - mid| gegen Liquidität für die
        gradient_depth_levels nächsten Levels beiderseits.
        Negativer Slope = steiles Orderbuch (Liquidität fällt schnell mit Distanz).
        """
        mid = snapshot.mid_price
        bids = sorted(
            [pl for pl in snapshot.price_levels if pl.price < mid],
            key=lambda pl: mid - pl.price,
        )[: self.gradient_depth_levels]
        asks = sorted(
            [pl for pl in snapshot.price_levels if pl.price >= mid],
            key=lambda pl: pl.price - mid,
        )[: self.gradient_depth_levels]

        near = bids + asks
        if len(near) < 2:
            return 0.0

        distances   = np.array([abs(pl.price - mid) for pl in near], dtype=float)
        liquidities = np.array([pl.total_liquidity for pl in near], dtype=float)

        if np.std(distances) < self._EPS:
            return 0.0

        slope = float(np.polyfit(distances, liquidities, 1)[0])
        return slope

    def _find_nearest_walls(
        self, walls: List[WallState], mid: float
    ) -> Tuple[Optional[float], Optional[float]]:
        bids = [w.price for w in walls if w.price < mid]
        asks = [w.price for w in walls if w.price >= mid]
        return (max(bids) if bids else None), (min(asks) if asks else None)

    def _classify_zone(
        self,
        bid_dist: Optional[float],
        ask_dist: Optional[float],
    ) -> PriceZone:
        thr      = self.wall_proximity_threshold
        near_bid = bid_dist is not None and bid_dist <= thr
        near_ask = ask_dist is not None and ask_dist <= thr

        if near_bid and near_ask:
            return PriceZone.BETWEEN_WALLS
        if near_bid:
            return PriceZone.WALL_APPROACH_BID
        if near_ask:
            return PriceZone.WALL_APPROACH_ASK
        return PriceZone.FREE_SPACE

    # ------------------------------------------------------------------
    # Öffentliche API
    # ------------------------------------------------------------------

    def compute(self, snapshot: HeatmapSnapshot) -> MarketFeatures:
        """Verarbeitet einen HeatmapSnapshot und gibt MarketFeatures zurück."""
        mid       = snapshot.mid_price
        total_liq = sum(pl.total_liquidity for pl in snapshot.price_levels)

        active_walls = self.wall_filter.update(snapshot)
        imbalance    = self._bid_ask_imbalance(snapshot)
        volatility   = self._volatility_proxy(total_liq)
        gradient     = self._liquidity_gradient(snapshot)

        nearest_bid, nearest_ask = self._find_nearest_walls(active_walls, mid)

        bid_dist: Optional[float] = (mid - nearest_bid)  if nearest_bid is not None else None
        ask_dist: Optional[float] = (nearest_ask - mid)  if nearest_ask is not None else None

        wall_distances: Dict[str, float] = {}
        if bid_dist is not None:
            wall_distances["bid_wall_dist"] = bid_dist
        if ask_dist is not None:
            wall_distances["ask_wall_dist"] = ask_dist

        zone = self._classify_zone(bid_dist, ask_dist)

        return MarketFeatures(
            mid_price         = mid,
            bid_ask_imbalance = imbalance,
            volatility_proxy  = volatility,
            liquidity_gradient= gradient,
            active_walls      = active_walls,
            nearest_bid_wall  = nearest_bid,
            nearest_ask_wall  = nearest_ask,
            zone              = zone,
            wall_distances    = wall_distances,
        )
