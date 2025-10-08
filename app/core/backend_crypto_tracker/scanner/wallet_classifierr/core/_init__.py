# ============================================================================
# core/__init__.py
# ============================================================================
"""Core module for wallet classification."""

from .base_analyzer import BaseWalletAnalyzer
from .metric_definitions import (
    DUST_SWEEPER_METRICS,
    HODLER_METRICS,
    MIXER_METRICS,
    TRADER_METRICS,
    WHALE_METRICS
)
from .stages import Stage1_RawMetrics, Stage2_DerivedMetrics, Stage3_ContextAnalysis
from .utils import convert_to_usd, calculate_time_difference, normalize_score

__all__ = [
    'BaseWalletAnalyzer',
    'DUST_SWEEPER_METRICS',
    'HODLER_METRICS',
    'MIXER_METRICS',
    'TRADER_METRICS',
    'WHALE_METRICS',
    'Stage1_RawMetrics',
    'Stage2_DerivedMetrics',
    'Stage3_ContextAnalysis',
    'convert_to_usd',
    'calculate_time_difference',
    'normalize_score'
]
