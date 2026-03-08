"""
hmm_regime.py — Wrapper für den HMM Markov Regime Detector aus dem Backtest.

Importiert MarkovRegime aus dem lokalen backtest_modules-Paket,
konvertiert JSON-OHLCV-Daten in pandas DataFrames und gibt
strukturierte Regime-Signale zurück.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from app.core.orderbook_heatmap.markov.backtest_modules.markov_regime import MarkovRegime
    _IMPORTS_OK = True
    _IMPORT_ERROR = ""
except ImportError as _e:
    _IMPORTS_OK = False
    _IMPORT_ERROR = str(_e)


def check_imports() -> Tuple[bool, str]:
    """Gibt (ok, fehlermeldung) zurück."""
    return _IMPORTS_OK, _IMPORT_ERROR


def run_hmm_regime(
    ohlcv_data: List[Dict[str, Any]],
    params: Dict[str, Any],
    max_signal_bars: int = 1000,
) -> Dict[str, Any]:
    """
    Führt die HMM-Regime-Erkennung auf OHLCV-Daten durch.

    Args:
        ohlcv_data:      Liste von Dicts mit keys: timestamp, open, high, low, close, volume
        params:          MarkovRegime-Parameter (n_states, train_bars, use_volume, etc.)
        max_signal_bars: Maximale Anzahl zurückgegebener Signal-Bars (letzte N Bars)

    Returns:
        Dict mit Regime-Summary, Signalen, State-Verteilung und aktuellem Zustand
    """
    if not _IMPORTS_OK:
        raise ImportError(
            f"MarkovRegime nicht importierbar: {_IMPORT_ERROR}. "
            f"Stelle sicher dass 'hmmlearn' installiert ist: pip install hmmlearn"
        )

    import pandas as pd
    import numpy as np

    # OHLCV-Liste → DataFrame (MarkovRegime erwartet kapitalisierte Spaltennamen)
    df = pd.DataFrame(ohlcv_data)
    df = df.rename(columns={
        "timestamp": "Timestamp",
        "open":      "Open",
        "high":      "High",
        "low":       "Low",
        "close":     "Close",
        "volume":    "Volume",
    })

    # Index setzen (DatetimeIndex falls Timestamp parsebar, sonst RangeIndex)
    try:
        df.index = pd.DatetimeIndex(pd.to_datetime(df["Timestamp"]))
    except Exception:
        pass

    n_bars = len(df)

    # Strategie instanziieren und Signale berechnen
    strat = MarkovRegime(params)
    result_df = strat.generate_signals(df)

    # Regime-Summary serialisieren
    summary_df = strat.regime_summary()
    regime_summary: List[Dict] = []
    if summary_df is not None:
        for state_idx, row in summary_df.iterrows():
            regime_summary.append({
                "state":           int(state_idx),
                "label":           str(row["label"]),
                "mean_log_return": round(float(row["mean_log_return"]), 8),
                "bar_count":       int(row["bar_count"]),
            })

    # State-Verteilung (Anteil pro State)
    states_array = result_df["hmm_state"].values
    state_distribution: Dict[str, float] = {}
    for s in range(strat.n_states):
        state_distribution[str(s)] = round(float(np.mean(states_array == s)), 6)

    # Signale serialisieren (letzte max_signal_bars Einträge)
    signal_rows = result_df[["Close", "hmm_state", "position", "signal"]].tail(max_signal_bars)
    signals: List[Dict] = []
    for idx, row in signal_rows.iterrows():
        ts = idx.isoformat() if hasattr(idx, "isoformat") else str(idx)
        signals.append({
            "timestamp":  ts,
            "close":      round(float(row["Close"]), 8),
            "hmm_state":  int(row["hmm_state"]),
            "position":   int(row["position"]),
            "signal":     int(row["signal"]),
        })

    # Aktueller Zustand (letzter Bar)
    current_state    = int(result_df["hmm_state"].iloc[-1])
    current_position = int(result_df["position"].iloc[-1])
    last_signal      = int(result_df["signal"].iloc[-1])

    return {
        "n_bars":             n_bars,
        "n_states":           strat.n_states,
        "bull_state":         int(strat._bull_state) if strat._bull_state is not None else None,
        "model_means":        [round(float(m), 8) for m in strat.model_means] if strat.model_means is not None else [],
        "regime_summary":     regime_summary,
        "state_distribution": state_distribution,
        "current_state":      current_state,
        "current_position":   current_position,
        "last_signal":        last_signal,
        "training_completed": True,
        "signals":            signals,
    }
