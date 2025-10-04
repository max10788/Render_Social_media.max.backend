# ============================================================================
# wallet_classifier/__init__.py
# ============================================================================
"""
Wallet Classifier Package
Mehrstufige Klassifizierung von Blockchain-Wallets
"""

from .core.base_classifier import BaseClassifier
from .core.metrics import MetricCalculator, MetricResult
from .core.utils import TransactionUtils

from .trader.classifier import TraderClassifier
from .hodler.classifier import HodlerClassifier
from .whale.classifier import WhaleClassifier
from .miner.classifier import MinerClassifier
from .mixer.classifier import MixerClassifier
from .dust_sweeper.classifier import DustSweeperClassifier

__version__ = "1.0.0"
__all__ = [
    'BaseClassifier',
    'MetricCalculator',
    'MetricResult',
    'TransactionUtils',
    'TraderClassifier',
    'HodlerClassifier',
    'WhaleClassifier',
    'MinerClassifier',
    'MixerClassifier',
    'DustSweeperClassifier'
]
