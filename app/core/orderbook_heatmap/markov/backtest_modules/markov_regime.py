"""
Markov Regime Strategy
======================
Uses a Gaussian Hidden Markov Model (HMM) to classify the market into
N hidden regimes (e.g. bear / sideways / bull) from log-returns and
optionally normalised volume.  Only enters long positions when the
current inferred regime matches the configured ``bull_state``.

The model is fitted on a warm-up window (``train_bars``) of historical
data before signals are generated, preventing look-ahead bias.

Parameters (via ``params`` dict)
---------------------------------
n_states        : int   — number of hidden regimes             (default 3)
train_bars      : int   — minimum bars used to fit the model   (default 120)
use_volume      : bool  — include normalised volume as feature (default True)
retrain_every   : int   — re-fit every N bars (0 = fit once)   (default 0)
covariance_type : str   — 'full', 'diag', 'tied', 'spherical'  (default 'diag')
n_iter          : int   — EM iterations for HMM fitting        (default 200)
bull_state      : int   — regime index treated as bull;
                          None = auto-detect by highest mean return (default None)

How it works
------------
1. Compute log-returns (and optional volume z-score) as HMM features.
2. Fit GaussianHMM on the first ``train_bars`` bars.
3. For every subsequent bar, predict the current hidden state using
   the full history up to that bar (Viterbi decoding).
4. Identify the "bull" state: the hidden state whose Gaussian component
   has the highest mean log-return.
5. Signal = 1 (long) when current state == bull_state, else 0 (flat).

Wrap with VolatilityRegime to further filter by ATR if desired:
    >>> inner = MarkovRegime({'n_states': 3})
    >>> filtered = VolatilityRegime({'regime': 'low'}, inner_strategy=inner)
"""

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from .base_strategy import BaseStrategy


def _log_returns(close: np.ndarray) -> np.ndarray:
    """Compute log-returns; first element is 0."""
    lr = np.zeros(len(close))
    lr[1:] = np.log(close[1:] / close[:-1])
    return lr


def _volume_zscore(volume: np.ndarray, window: int = 20) -> np.ndarray:
    """Rolling z-score of volume; NaN-safe."""
    v = pd.Series(volume)
    mu = v.rolling(window, min_periods=1).mean()
    sigma = v.rolling(window, min_periods=1).std().replace(0, 1)
    return ((v - mu) / sigma).fillna(0).values


def _build_features(
    close: np.ndarray,
    volume: Optional[np.ndarray],
    use_volume: bool,
) -> np.ndarray:
    """Stack features into (n_bars, n_features) array."""
    lr = _log_returns(close).reshape(-1, 1)
    if use_volume and volume is not None:
        vz = _volume_zscore(volume).reshape(-1, 1)
        return np.hstack([lr, vz])
    return lr


def _fit_hmm(features: np.ndarray, n_states: int, cov_type: str, n_iter: int):
    """Fit a GaussianHMM and return the fitted model."""
    from hmmlearn.hmm import GaussianHMM

    model = GaussianHMM(
        n_components=n_states,
        covariance_type=cov_type,
        n_iter=n_iter,
        min_covar=1e-3,
        random_state=42,
    )
    model.fit(features)
    return model


def _detect_bull_state(model, n_states: int) -> int:
    """Return the index of the state with the highest mean log-return."""
    means = model.means_[:, 0]  # first feature = log-return
    return int(np.argmax(means))


class MarkovRegime(BaseStrategy):
    """
    HMM-based regime detection strategy compatible with the project's
    BaseStrategy / vectorized_engine interface.
    """

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        super().__init__(params)
        self.n_states = self.get_parameter("n_states", 3)
        self.train_bars = self.get_parameter("train_bars", 120)
        self.use_volume = self.get_parameter("use_volume", True)
        self.retrain_every = self.get_parameter("retrain_every", 0)
        self.covariance_type = self.get_parameter("covariance_type", "diag")
        self.n_iter = self.get_parameter("n_iter", 200)
        self._bull_state_param = self.get_parameter("bull_state", None)

        self._model = None
        self._bull_state: Optional[int] = None
        self.signals: Optional[pd.DataFrame] = None

        # Diagnostics exposed after generate_signals()
        self.regime_series: Optional[pd.Series] = None
        self.model_means: Optional[np.ndarray] = None

    def validate_params(self) -> bool:
        if self.n_states < 2:
            raise ValueError("n_states must be >= 2")
        if self.train_bars < self.n_states * 10:
            raise ValueError(
                f"train_bars ({self.train_bars}) too small for "
                f"{self.n_states} states; increase to >= {self.n_states * 10}"
            )
        valid_cov = ("full", "diag", "tied", "spherical")
        if self.covariance_type not in valid_cov:
            raise ValueError(f"covariance_type must be one of {valid_cov}")
        return True

    # ------------------------------------------------------------------
    # generate_signals — vectorised, walk-forward aware
    # ------------------------------------------------------------------
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        self.validate_params()
        df = data.copy()

        close = df["Close"].values
        volume = df["Volume"].values if "Volume" in df.columns else None

        n = len(df)
        if n < self.train_bars:
            raise ValueError(
                f"Not enough bars: got {n}, need at least {self.train_bars}"
            )

        features = _build_features(close, volume, self.use_volume)

        # --- Fit initial model on training window ---
        train_features = features[: self.train_bars]
        self._model = _fit_hmm(
            train_features, self.n_states, self.covariance_type, self.n_iter
        )
        self._bull_state = (
            self._bull_state_param
            if self._bull_state_param is not None
            else _detect_bull_state(self._model, self.n_states)
        )
        self.model_means = self._model.means_[:, 0].copy()

        # --- Predict regimes bar by bar (walk-forward) ---
        states = np.full(n, -1, dtype=int)

        # For the training window we use the fitted model directly
        states[: self.train_bars] = self._model.predict(train_features)

        for i in range(self.train_bars, n):
            # Optional periodic retraining
            if (
                self.retrain_every > 0
                and (i - self.train_bars) % self.retrain_every == 0
                and i > self.train_bars
            ):
                self._model = _fit_hmm(
                    features[:i], self.n_states, self.covariance_type, self.n_iter
                )
                self._bull_state = (
                    self._bull_state_param
                    if self._bull_state_param is not None
                    else _detect_bull_state(self._model, self.n_states)
                )
                self.model_means = self._model.means_[:, 0].copy()

            states[i] = self._model.predict(features[: i + 1])[-1]

        # --- Convert states to position signal ---
        position = (states == self._bull_state).astype(int)
        # Mask training window — no signals during warm-up
        position[: self.train_bars] = 0

        # Derive entry/exit signal from position changes
        sig = np.zeros(n, dtype=int)
        sig[1:] = np.diff(position)  # +1 = entry, -1 = exit
        if position[0] == 1:
            sig[0] = 1

        df["hmm_state"] = states
        df["position"] = position
        df["signal"] = sig

        self.regime_series = pd.Series(states, index=df.index, name="hmm_state")
        self.signals = df
        return df

    # ------------------------------------------------------------------
    # on_bar — event-driven hook (delegates to pre-computed signals)
    # ------------------------------------------------------------------
    def on_bar(self, bar: pd.Series) -> Optional[Dict[str, Any]]:
        if self.signals is None or bar.name not in self.signals.index:
            return None
        sig = self.signals.loc[bar.name, "signal"]
        if sig == 1:
            return {"action": "buy", "price": bar["Close"]}
        if sig == -1:
            return {"action": "sell", "price": bar["Close"]}
        return None

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    def regime_summary(self) -> Optional[pd.DataFrame]:
        """Return a DataFrame summarising each detected state's statistics."""
        if self._model is None or self.regime_series is None:
            return None
        rows = []
        for s in range(self.n_states):
            label = "BULL" if s == self._bull_state else f"state_{s}"
            rows.append(
                {
                    "state": s,
                    "label": label,
                    "mean_log_return": float(self._model.means_[s, 0]),
                    "bar_count": int((self.regime_series == s).sum()),
                }
            )
        return pd.DataFrame(rows).set_index("state")

    def __str__(self) -> str:
        return (
            f"MarkovRegime(n_states={self.n_states}, "
            f"train_bars={self.train_bars}, "
            f"use_volume={self.use_volume}, "
            f"bull_state={self._bull_state})"
        )
