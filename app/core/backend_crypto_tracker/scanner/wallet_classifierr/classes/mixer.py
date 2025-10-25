# ============================================================================
# classes/mixer.py
# ============================================================================
"""Mixer wallet analyzer."""

from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.metric_definitions import MIXER_METRICS
from typing import Dict, Any


class MixerAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Mixer wallets."""
    
    CLASS_NAME = "Mixer"
    METRICS = MIXER_METRICS
    THRESHOLD = 0.45
    WEIGHTS = {"primary": 0.5, "secondary": 0.2, "context": 0.3}
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """Compute Mixer score."""
        # Primary indicators
        equal_outputs = metrics.get('equal_output_proportion', 0)
        known_mixer = 1.0 if metrics.get('known_mixer_interaction', False) else 0
        
        # CoinJoin frequency (many inputs + uniform outputs)
        high_inputs = self._normalize(metrics.get('avg_inputs_per_tx', 0), 0, 20)
        coinjoin_freq = high_inputs * equal_outputs
        
        round_amounts = metrics.get('round_amounts_ratio', 0)
        
        primary_score = self._avg([
            equal_outputs,
            known_mixer,
            coinjoin_freq,
            round_amounts,
            self._normalize(metrics.get('avg_inputs_per_tx', 0), 0, 15)
        ])
        
        # Secondary indicators
        timing_entropy = self._normalize(metrics.get('timing_entropy', 0), 0, 5)
        output_uniformity = equal_outputs  # Same as equal outputs for simplicity
        
        # Path complexity (estimated by transaction count and network degree)
        path_complexity = self._normalize(
            metrics.get('tx_count', 0) * metrics.get('out_degree', 1),
            0,
            1000
        )
        
        secondary_score = self._avg([timing_entropy, output_uniformity, path_complexity])
        
        # Context indicators
        tornado_cash = 1.0 if metrics.get('tornado_cash_interaction', False) else 0
        high_betweenness = self._normalize(metrics.get('betweenness_centrality', 0), 0, 0.1)
        
        context_score = self._avg([
            tornado_cash,
            high_betweenness,
            0.5  # Placeholder for mixed output reuse and cluster fragmentation
        ])
        
        # Weighted combination
        final_score = (
            primary_score * self.WEIGHTS["primary"] +
            secondary_score * self.WEIGHTS["secondary"] +
            context_score * self.WEIGHTS["context"]
        )
        
        return final_score
