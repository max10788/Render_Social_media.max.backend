"""
l2_simulator.py — Wrapper für den Markov L2 Orderbuch-Simulator.

Verbindet den Backtest-Simulator mit Live-Bitget-Daten aus dem Backend.
Konvertiert Bitget-Orderbook-Snapshots (bids/asks) in HeatmapSnapshot-Objekte
und führt die Markov-Monte-Carlo-Simulation durch.
"""
from __future__ import annotations

import sys
import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# Backtest-Module in sys.path eintragen (relative Imports im Backtest-Code)
_HEATMAP_PROB_PATH = "/home/josua/Block_Intel/backtest/heatmap_probability"
if _HEATMAP_PROB_PATH not in sys.path:
    sys.path.insert(0, _HEATMAP_PROB_PATH)

try:
    from models import HeatmapSnapshot, PriceLevel          # noqa: E402
    from wall_filter import WallFilter                       # noqa: E402
    from market_state import MarketStateEstimator, PriceZone # noqa: E402
    from simulator import MarkovSimulator, TransitionMatrix  # noqa: E402
    _IMPORTS_OK = True
except ImportError as _import_err:
    _IMPORTS_OK = False
    _IMPORT_ERROR = str(_import_err)


# ---------------------------------------------------------------------------
# Konvertierung Bitget-Orderbook → HeatmapSnapshot
# ---------------------------------------------------------------------------

def _orderbook_to_snapshot(
    ob: Dict[str, Any],
    symbol: str,
    timestamp: Optional[datetime] = None,
) -> Optional[HeatmapSnapshot]:
    """
    Konvertiert ein Bitget-Orderbook-Dict (bids/asks als [[price_str, qty_str]])
    in ein HeatmapSnapshot-Objekt für den Markov-Simulator.

    Args:
        ob:        Bitget-Orderbook mit keys 'bids', 'asks', optional 'ts'
        symbol:    Handels-Symbol (z.B. "ARB/USDT")
        timestamp: Zeitstempel; falls None, wird utcnow() verwendet

    Returns:
        HeatmapSnapshot oder None wenn bids/asks leer sind
    """
    bids: List[List[str]] = ob.get("bids", [])
    asks: List[List[str]] = ob.get("asks", [])

    if not bids or not asks:
        return None

    ts = timestamp or datetime.now(timezone.utc)

    # Bids → PriceLevels
    price_levels: List[PriceLevel] = []
    for entry in bids:
        try:
            price = float(entry[0])
            qty   = float(entry[1])
            if price > 0 and qty > 0:
                price_levels.append(
                    PriceLevel(price=price, liquidity_by_exchange={"bitget": price * qty})
                )
        except (ValueError, IndexError):
            continue

    # Asks → PriceLevels
    for entry in asks:
        try:
            price = float(entry[0])
            qty   = float(entry[1])
            if price > 0 and qty > 0:
                price_levels.append(
                    PriceLevel(price=price, liquidity_by_exchange={"bitget": price * qty})
                )
        except (ValueError, IndexError):
            continue

    if not price_levels:
        return None

    price_levels.sort(key=lambda pl: pl.price)

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid_price = (best_bid + best_ask) / 2.0

    return HeatmapSnapshot(
        symbol=symbol,
        timestamp=ts,
        price_levels=price_levels,
        min_price=price_levels[0].price,
        max_price=price_levels[-1].price,
        mid_price=mid_price,
    )


# ---------------------------------------------------------------------------
# Preis-Fan berechnen (Perzentil-Pfade aus Monte-Carlo)
# ---------------------------------------------------------------------------

def _compute_price_fan(
    paths: list,
    percentiles: List[int] = [5, 25, 50, 75, 95],
) -> Dict[str, List[float]]:
    """
    Berechnet Perzentil-Pfade (Fan) aus allen Monte-Carlo-Simulationspfaden.

    Für jeden Schritt t werden die Preis-Werte aller Pfade genommen und
    das jeweilige Perzentil berechnet → ergibt einen "Fan" über die Zeit.
    """
    if not paths:
        return {f"p{p}": [] for p in percentiles}

    n_steps = len(paths[0].prices)
    # Matrix: (n_paths, n_steps)
    price_matrix = np.array([p.prices for p in paths], dtype=np.float64)

    fan: Dict[str, List[float]] = {}
    for pct in percentiles:
        fan[f"p{pct}"] = [
            float(np.percentile(price_matrix[:, t], pct))
            for t in range(n_steps)
        ]

    return fan


# ---------------------------------------------------------------------------
# Haupt-API
# ---------------------------------------------------------------------------

def check_imports() -> Tuple[bool, str]:
    """Gibt zurück ob die Backtest-Imports erfolgreich waren."""
    if _IMPORTS_OK:
        return True, "ok"
    return False, _IMPORT_ERROR


def run_l2_markov_simulation(
    snapshots: List[Dict[str, Any]],
    symbol: str,
    n_paths: int = 300,
    n_steps: int = 50,
    price_step_std: float = 25.0,
    wall_bounce_factor: float = 0.7,
    breakthrough_momentum: float = 1.5,
    persistence_window: int = 5,
    entry_percentile: float = 95.0,
    exit_percentile: float = 85.0,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Führt die vollständige Markov-L2-Simulation durch.

    Args:
        snapshots:             Liste von Bitget-Orderbook-Dicts (bids/asks)
        symbol:                Handels-Symbol (z.B. "ARB/USDT")
        n_paths:               Anzahl Monte-Carlo-Pfade
        n_steps:               Schritte pro Simulationspfad
        price_step_std:        Std der Normalverteilung für Preis-Schritte
        wall_bounce_factor:    Reflexionswahrscheinlichkeits-Faktor [0.1, 1.0]
        breakthrough_momentum: Momentum-Multiplikator beim Durchbruch
        persistence_window:    Min. Snapshots für Wall-Aktivierung
        entry_percentile:      Liquiditäts-Perzentil für Wall-Aktivierung
        exit_percentile:       Liquiditäts-Perzentil für Wall-Deaktivierung
        seed:                  Random Seed für Reproduzierbarkeit

    Returns:
        Dict mit transition_matrix, simulation-Statistiken, price_fan, active_walls
    """
    if not _IMPORTS_OK:
        raise ImportError(
            f"Backtest-Module nicht importierbar: {_IMPORT_ERROR}. "
            f"Pfad: {_HEATMAP_PROB_PATH}"
        )

    # Konvertiere Bitget-Orderbooks → HeatmapSnapshots
    heatmap_snapshots: List[HeatmapSnapshot] = []
    for ob in snapshots:
        snap = _orderbook_to_snapshot(ob, symbol)
        if snap is not None:
            heatmap_snapshots.append(snap)

    n_usable = len(heatmap_snapshots)
    if n_usable < persistence_window + 2:
        raise ValueError(
            f"Zu wenig verwertbare Snapshots: {n_usable} (Minimum: {persistence_window + 2}). "
            f"Erhöhe n_snapshots oder reduziere persistence_window."
        )

    # Komponenten aufbauen
    wall_filter = WallFilter(
        persistence_window=persistence_window,
        entry_percentile=entry_percentile,
        exit_percentile=exit_percentile,
        price_bucket_size=max(0.0001, heatmap_snapshots[-1].mid_price * 0.002),
    )
    estimator = MarketStateEstimator(
        wall_filter=wall_filter,
        wall_proximity_threshold=heatmap_snapshots[-1].mid_price * 0.005,
    )
    simulator = MarkovSimulator(
        wall_filter=wall_filter,
        state_estimator=estimator,
        price_step_std=price_step_std,
        wall_bounce_factor=wall_bounce_factor,
        breakthrough_momentum=breakthrough_momentum,
        seed=seed,
    )

    # Training auf allen Snapshots außer dem letzten
    train_snaps = heatmap_snapshots[:-1]
    initial_snap = heatmap_snapshots[-1]

    tm: TransitionMatrix = simulator.fit(train_snaps)

    # Monte-Carlo-Simulation
    warmup = train_snaps[-persistence_window:] if len(train_snaps) >= persistence_window else train_snaps
    paths = simulator.simulate(
        transition_matrix=tm,
        initial_snapshot=initial_snap,
        n_steps=n_steps,
        n_paths=n_paths,
        warmup_snapshots=warmup,
    )

    # Analyse + Preis-Fan
    analysis = simulator.analyze_paths(paths, initial_snap.mid_price)
    fan = _compute_price_fan(paths)

    # Übergangsmatrix serialisieren
    tm_df = tm.to_dataframe()
    labels = list(tm_df.columns)
    matrix_values = tm_df.values.tolist()

    # Aktive Walls aus letztem Snapshot extrahieren (nach Training)
    final_features = estimator.compute(initial_snap)
    active_walls = [
        {
            "price": round(w.price, 8),
            "side": "bid" if w.price < initial_snap.mid_price else "ask",
            "proximity_weight": round(w.proximity_weight, 4),
            "persistence_count": w.persistence_count,
        }
        for w in final_features.active_walls
    ]

    return {
        "n_snapshots_used": n_usable,
        "initial_price": round(initial_snap.mid_price, 8),
        "transition_matrix": {
            "labels": labels,
            "values": [[round(v, 6) for v in row] for row in matrix_values],
        },
        "simulation": {
            "n_paths": analysis["n_paths"],
            "n_steps": n_steps,
            "mean_final_price": round(analysis["mean_final"], 8),
            "std_final_price": round(analysis["std_final"], 8),
            "percentiles": {
                "p5":  round(analysis["pct_5"],  8),
                "p25": round(analysis["pct_25"], 8),
                "p50": round(analysis["pct_50"], 8),
                "p75": round(analysis["pct_75"], 8),
                "p95": round(analysis["pct_95"], 8),
            },
            "mean_bounces":             round(analysis["mean_bounces"], 4),
            "mean_breakthroughs":       round(analysis["mean_breakthroughs"], 4),
            "bounce_rate":              round(analysis["bounce_rate"], 6),
            "price_distribution":       analysis["price_distribution"],
            "pct_paths_above_initial":  round(analysis["pct_paths_above_initial"], 4),
        },
        "price_fan": {k: [round(v, 8) for v in vals] for k, vals in fan.items()},
        "active_walls": active_walls,
    }
