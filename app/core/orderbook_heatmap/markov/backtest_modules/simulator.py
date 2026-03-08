"""
simulator.py — Markov-Ketten-Simulation mit Liquiditätswänden als
semi-permeablen Barrieren.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .models import HeatmapSnapshot
from .wall_filter import WallFilter, WallState
from .market_state import MarketStateEstimator, MarketFeatures, PriceZone

# Kanonische Zustandsreihenfolge (fixiert Matrixindizes)
_STATES: List[PriceZone] = [
    PriceZone.FREE_SPACE,
    PriceZone.WALL_APPROACH_BID,
    PriceZone.WALL_APPROACH_ASK,
    PriceZone.BETWEEN_WALLS,
    PriceZone.BREAKTHROUGH,
]


@dataclass
class TransitionMatrix:
    matrix: np.ndarray              # (n_states, n_states), zeilenweise normiert
    state_index: Dict[PriceZone, int]
    counts: np.ndarray              # Rohzähler vor Normalisierung

    def probability(self, from_state: PriceZone, to_state: PriceZone) -> float:
        return float(self.matrix[self.state_index[from_state],
                                 self.state_index[to_state]])

    def to_dataframe(self) -> pd.DataFrame:
        idx_to_state = {v: k for k, v in self.state_index.items()}
        n      = len(self.state_index)
        labels = [idx_to_state[i].name for i in range(n)]
        return pd.DataFrame(self.matrix, index=labels, columns=labels)


@dataclass
class SimulationPath:
    prices: List[float]
    zones: List[PriceZone]
    wall_bounces: int
    wall_breakthroughs: int
    n_steps: int


class MarkovSimulator:
    """
    Lernt eine Übergangsmatrix aus historischen HeatmapSnapshots
    und führt Monte-Carlo-Simulationen durch.

    Liquiditätswände fungieren als semi-permeable Barrieren:
      - Bounce:       Preis reflektiert mit Wahrscheinlichkeit p_bounce
      - Breakthrough: Preis setzt sich mit erhöhtem Momentum durch
    """

    def __init__(
        self,
        wall_filter: WallFilter,
        state_estimator: MarketStateEstimator,
        price_step_std: float = 20.0,
        wall_bounce_factor: float = 0.7,
        breakthrough_momentum: float = 1.5,
        seed: Optional[int] = None,
    ) -> None:
        self.wall_filter            = wall_filter
        self.state_estimator        = state_estimator
        self.price_step_std         = price_step_std
        self.wall_bounce_factor     = wall_bounce_factor
        self.breakthrough_momentum  = breakthrough_momentum
        self.rng                    = np.random.default_rng(seed)

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(self, snapshots: List[HeatmapSnapshot]) -> TransitionMatrix:
        """
        Lernt Zustandsübergänge aus einer historischen Snapshot-Zeitreihe.
        Nutzt eine frische Kopie des Filters, um Zustandsverschmutzung
        durch evtl. vorherige Aufrufe zu vermeiden.
        """
        local_filter    = copy.deepcopy(self.wall_filter)
        local_estimator = copy.deepcopy(self.state_estimator)
        local_estimator.wall_filter = local_filter

        n_states    = len(_STATES)
        state_index = {s: i for i, s in enumerate(_STATES)}
        counts      = np.zeros((n_states, n_states), dtype=np.float64)

        prev_zone: Optional[PriceZone] = None

        for snap in snapshots:
            features     = local_estimator.compute(snap)
            current_zone = features.zone

            if prev_zone is not None:
                counts[state_index[prev_zone], state_index[current_zone]] += 1.0

            prev_zone = current_zone

        # Laplace-Smoothing (α = 0.1) → verhindert Nullwahrscheinlichkeiten
        alpha    = 0.1
        smoothed = counts + alpha
        matrix   = smoothed / smoothed.sum(axis=1, keepdims=True)

        return TransitionMatrix(matrix=matrix, state_index=state_index, counts=counts)

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------

    def simulate(
        self,
        transition_matrix: TransitionMatrix,
        initial_snapshot: HeatmapSnapshot,
        n_steps: int = 100,
        n_paths: int = 500,
        warmup_snapshots: Optional[List[HeatmapSnapshot]] = None,
    ) -> List[SimulationPath]:
        """
        Monte-Carlo-Simulation basierend auf der gelernten Übergangsmatrix.
        Walls werden als semi-permeable Barrieren behandelt.

        warmup_snapshots: Optionale Liste von Snapshots vor initial_snapshot,
            die genutzt werden um den Wall-Filter vorzuwärmen (Persistence-Buffer).
            Empfohlen: letzte persistence_window Snapshots vor dem Startzeitpunkt.
        """
        # Initiale Features — Filter mit Warmup-Daten vorwärmen
        local_filter    = copy.deepcopy(self.wall_filter)
        local_estimator = copy.deepcopy(self.state_estimator)
        local_estimator.wall_filter = local_filter

        if warmup_snapshots:
            for snap in warmup_snapshots:
                local_filter.update(snap)

        init_features   = local_estimator.compute(initial_snapshot)

        return [
            self._simulate_single(transition_matrix, init_features, n_steps)
            for _ in range(n_paths)
        ]

    def _simulate_single(
        self,
        tm: TransitionMatrix,
        init: MarketFeatures,
        n_steps: int,
    ) -> SimulationPath:
        n_states      = len(_STATES)
        idx_to_state  = {v: k for k, v in tm.state_index.items()}

        price  = init.mid_price
        zone   = init.zone
        imbal  = init.bid_ask_imbalance
        vol    = min(abs(imbal), 1.0)

        # Initiale Wall-Preise (per-Path kopiert → Durchbrüche unabhängig)
        bid_wall: Optional[float] = init.nearest_bid_wall
        ask_wall: Optional[float] = init.nearest_ask_wall

        prices  = [price]
        zones   = [zone]
        bounces = 0
        brkthr  = 0

        for _ in range(n_steps):
            # ── Nächsten Zustand samplen ──────────────────────────────
            row       = tm.matrix[tm.state_index[zone]]
            next_idx  = int(self.rng.choice(n_states, p=row))
            zone      = idx_to_state[next_idx]

            # ── Preisschritt berechnen ────────────────────────────────
            step = float(self.rng.normal(0.0, self.price_step_std))
            step -= imbal * self.price_step_std * 0.3   # Imbalance-Richtungskorrektur

            # Zonen-Skalierung
            if zone == PriceZone.BETWEEN_WALLS:
                step *= 0.60    # Konsolidierung: engere Bewegung
            elif zone == PriceZone.FREE_SPACE:
                step *= 1.20    # Momentum: größere Bewegung

            new_price = price + step

            # ── Bid-Wall Interaktion (Support) ───────────────────────
            if bid_wall is not None and new_price < bid_wall:
                overshoot   = bid_wall - new_price   # immer positiv
                p_bounce    = float(np.clip(
                    self.wall_bounce_factor * (1.0 - vol), 0.1, 0.95
                ))
                if self.rng.random() < p_bounce:
                    new_price = bid_wall + overshoot * 0.5
                    bounces  += 1
                    zone      = PriceZone.WALL_APPROACH_BID
                else:
                    new_price = bid_wall - abs(step) * self.breakthrough_momentum
                    bid_wall  = None
                    brkthr   += 1
                    zone      = PriceZone.BREAKTHROUGH

            # ── Ask-Wall Interaktion (Resistance) ────────────────────
            elif ask_wall is not None and new_price > ask_wall:
                overshoot   = new_price - ask_wall   # immer positiv
                p_bounce    = float(np.clip(
                    self.wall_bounce_factor * (1.0 - vol), 0.1, 0.95
                ))
                if self.rng.random() < p_bounce:
                    new_price = ask_wall - overshoot * 0.5
                    bounces  += 1
                    zone      = PriceZone.WALL_APPROACH_ASK
                else:
                    new_price = ask_wall + abs(step) * self.breakthrough_momentum
                    ask_wall  = None
                    brkthr   += 1
                    zone      = PriceZone.BREAKTHROUGH

            price = new_price
            prices.append(price)
            zones.append(zone)

        return SimulationPath(
            prices            = prices,
            zones             = zones,
            wall_bounces      = bounces,
            wall_breakthroughs= brkthr,
            n_steps           = n_steps,
        )

    # ------------------------------------------------------------------
    # Analyse
    # ------------------------------------------------------------------

    def analyze_paths(
        self, paths: List[SimulationPath], initial_price: float
    ) -> Dict:
        finals = np.array([p.prices[-1] for p in paths])
        mean_f = float(np.mean(finals))

        if mean_f > initial_price * 1.005:
            distribution = "bullish"
        elif mean_f < initial_price * 0.995:
            distribution = "bearish"
        else:
            distribution = "neutral"

        total_events = sum(p.wall_bounces + p.wall_breakthroughs for p in paths)

        return {
            "n_paths":                  len(paths),
            "initial_price":            initial_price,
            "mean_final":               mean_f,
            "std_final":                float(np.std(finals)),
            "pct_5":                    float(np.percentile(finals, 5)),
            "pct_25":                   float(np.percentile(finals, 25)),
            "pct_50":                   float(np.percentile(finals, 50)),
            "pct_75":                   float(np.percentile(finals, 75)),
            "pct_95":                   float(np.percentile(finals, 95)),
            "mean_bounces":             float(np.mean([p.wall_bounces for p in paths])),
            "mean_breakthroughs":       float(np.mean([p.wall_breakthroughs for p in paths])),
            "bounce_rate":              float(np.mean([p.wall_bounces for p in paths]))
                                        / max(1.0, float(np.mean([p.n_steps for p in paths]))),
            "price_distribution":       distribution,
            "pct_paths_above_initial":  float(np.mean(finals > initial_price)),
        }
