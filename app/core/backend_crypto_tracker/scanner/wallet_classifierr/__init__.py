"""
Wallet Classifier - Rule-based Bitcoin wallet classification system.

This package provides a transparent, reproducible system for classifying
Bitcoin wallets into five categories:
- Dust Sweeper
- Hodler
- Mixer
- Trader
- Whale
"""

from .analyzer import WalletClassifier, classify_wallet
from .data_sources import GroundTruthDB
from .classes import (
    DustSweeperAnalyzer,
    HodlerAnalyzer,
    MixerAnalyzer,
    TraderAnalyzer,
    WhaleAnalyzer
)

__version__ = '1.0.0'
__all__ = [
    'WalletClassifier',
    'classify_wallet',
    'GroundTruthDB',
    'DustSweeperAnalyzer',
    'HodlerAnalyzer',
    'MixerAnalyzer',
    'TraderAnalyzer',
    'WhaleAnalyzer'
]
