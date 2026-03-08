"""
wall_filter.py — Noise-Reduction für Liquiditätswände via drei Mechanismen:
  1. Persistence Filter   — Wall muss N Schritte stabil sein
  2. Proximity Weighting  — Wände zu weit vom Mid-Price werden ignoriert
  3. Hysterese-Thresholding — Zwei Schwellenwerte verhindern "Flapping"
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np

from .models import HeatmapSnapshot


@dataclass
class WallState:
    price: float
    active: bool = False
    persistence_count: int = 0
    last_liquidity: float = 0.0
    proximity_weight: float = 1.0   # 0 = ignoriert, 1 = volles Gewicht


class WallFilter:
    """
    Filtert kurzlebige Fake-Walls (Spoofing) aus dem Orderbuch heraus
    und liefert nur robuste, persistente Liquiditätszonen zurück.
    """

    def __init__(
        self,
        persistence_window: int = 5,
        proximity_std_threshold: float = 2.0,
        entry_percentile: float = 95.0,
        exit_percentile: float = 85.0,
        price_bucket_size: float = 10.0,
    ) -> None:
        self.persistence_window      = persistence_window
        self.proximity_std_threshold = proximity_std_threshold
        self.entry_percentile        = entry_percentile
        self.exit_percentile         = exit_percentile
        self.price_bucket_size       = price_bucket_size

        # Interner Zustand
        self._liquidity_history: Dict[float, deque] = {}
        self._active_walls: Dict[float, WallState]  = {}
        self._candidate_count: Dict[float, int]     = {}  # Schritte seit entry_threshold

    # ------------------------------------------------------------------
    # Hilfsmethoden
    # ------------------------------------------------------------------

    def _bucket(self, price: float) -> float:
        """Rundet einen Preis auf den nächsten Bucket-Wert."""
        return round(round(price / self.price_bucket_size) * self.price_bucket_size, 8)

    def _proximity_weight(
        self, price: float, mid: float, price_std: float
    ) -> float:
        """
        Berechnet Proximity-Gewicht [0, 1].
        Normalisiert über die Hälfte der Snapshot-Preisspanne (nicht Std der Levels),
        damit entfernte aber persistente Walls nicht fälschlicherweise gefiltert werden.
        Preise jenseits von proximity_std_threshold × half_range erhalten ein
        abnehmendes Gewicht.
        """
        if price_std < 1e-9:
            return 1.0
        # half_range = Hälfte der sichtbaren Preisspanne im Snapshot
        # price_std wird als half_range / 2 übergeben (siehe update())
        half_range = price_std * 2.0
        dist_ratio = abs(price - mid) / half_range
        threshold  = self.proximity_std_threshold / 4.0   # ~0.5 → obere Hälfte des Buchs
        if dist_ratio <= threshold:
            return 1.0
        return max(0.0, 1.0 - (dist_ratio - threshold) / threshold)

    # ------------------------------------------------------------------
    # Öffentliche API
    # ------------------------------------------------------------------

    def update(self, snapshot: HeatmapSnapshot) -> List[WallState]:
        """
        Verarbeitet einen neuen HeatmapSnapshot.
        Gibt die Liste aller aktuell aktiven WallStates zurück.
        """
        mid   = snapshot.mid_price
        pls   = snapshot.price_levels

        if not pls:
            return []

        # ── Rohdaten aufbereiten ──────────────────────────────────────
        current_liq: Dict[float, float] = {}
        for pl in pls:
            bp = self._bucket(pl.price)
            current_liq[bp] = current_liq.get(bp, 0.0) + pl.total_liquidity

        all_prices_raw = [pl.price for pl in pls]
        # Verwende halbe Preisspanne als Referenz statt Std (robuster für Randlagen)
        if len(all_prices_raw) > 1:
            price_std = (max(all_prices_raw) - min(all_prices_raw)) / 2.0
        else:
            price_std = 1.0

        # Perzentil-Schwellenwerte aus aktuellen Liquiditätswerten
        liq_vals = list(current_liq.values())
        entry_thr = float(np.percentile(liq_vals, self.entry_percentile))
        exit_thr  = float(np.percentile(liq_vals, self.exit_percentile))

        # ── Liquiditäts-History aktualisieren ────────────────────────
        all_bucketed = set(self._liquidity_history.keys()) | set(current_liq.keys())
        for bp in all_bucketed:
            if bp not in self._liquidity_history:
                self._liquidity_history[bp] = deque(maxlen=self.persistence_window)
            self._liquidity_history[bp].append(current_liq.get(bp, 0.0))

        # ── Wall-Status pro Bucket bewerten ──────────────────────────
        new_active: Dict[float, WallState] = {}

        for bp in all_bucketed:
            liq    = current_liq.get(bp, 0.0)
            prox_w = self._proximity_weight(bp, mid, price_std)

            # Proximity-Ausschluss: Gewicht zu gering → ignorieren
            if prox_w < 0.2:
                self._candidate_count.pop(bp, None)
                self._active_walls.pop(bp, None)
                continue

            eff_liq = liq * prox_w
            history = self._liquidity_history.get(bp, deque())

            # ── Hysterese-Logik ──────────────────────────────────────
            is_active = bp in self._active_walls and self._active_walls[bp].active

            if is_active:
                # Aktive Wall bleibt aktiv, bis sie unter exit_thr fällt
                survives = eff_liq >= exit_thr
            else:
                # Inaktive Wall muss entry_thr überschreiten → Kandidat
                if eff_liq >= entry_thr:
                    self._candidate_count[bp] = self._candidate_count.get(bp, 0) + 1
                else:
                    self._candidate_count.pop(bp, None)
                survives = False

            # ── Persistence-Filter ────────────────────────────────────
            # Ein Kandidat wird aktiviert, sobald er über persistence_window
            # Schritte konsistent über exit_thr lag.
            if not is_active and bp in self._candidate_count:
                persistent = (
                    len(history) >= self.persistence_window
                    and all(h >= exit_thr for h in history)
                )
                if persistent:
                    survives = True
                    self._candidate_count.pop(bp, None)

            # ── Zustand sichern ──────────────────────────────────────
            if survives:
                prev = self._active_walls.get(bp)
                ws = WallState(
                    price             = bp,
                    active            = True,
                    persistence_count = (prev.persistence_count + 1 if prev else 1),
                    last_liquidity    = liq,
                    proximity_weight  = prox_w,
                )
                new_active[bp] = ws

        self._active_walls = new_active
        return self.get_active_walls()

    def get_active_walls(self) -> List[WallState]:
        return sorted(
            [w for w in self._active_walls.values() if w.active],
            key=lambda w: w.price,
        )

    def reset(self) -> None:
        self._liquidity_history.clear()
        self._active_walls.clear()
        self._candidate_count.clear()
