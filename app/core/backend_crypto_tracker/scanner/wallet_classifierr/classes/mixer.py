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
    WEIGHTS = {"primary": 0.5, "secondary": 0.3, "context": 0.2}
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """Compute Mixer score."""
        # Primary indicators - Output patterns and mixing behavior
        equal_outputs = metrics.get('equal_output_proportion', 0)
        known_mixer = 1.0 if metrics.get('known_mixer_interaction', False) else 0
        
        # CoinJoin frequency (many inputs + uniform outputs)
        high_inputs = self._normalize(metrics.get('avg_inputs_per_tx', 0), 0, 20)
        coinjoin_freq = high_inputs * equal_outputs
        
        round_amounts = metrics.get('round_amounts_ratio', 0)
        
        # NEW: Transaction size consistency (high for mixers)
        tx_size_consistency = metrics.get('tx_size_consistency', 0)
        
        # NEW: Fan-out score (mixers distribute to many addresses)
        fan_out = metrics.get('fan_out_score', 0)
        fan_out_score = self._normalize(fan_out, 0, 10)
        
        primary_score = self._avg([
            equal_outputs,
            known_mixer,
            coinjoin_freq,
            round_amounts,
            tx_size_consistency,
            fan_out_score,
            self._normalize(metrics.get('avg_inputs_per_tx', 0), 0, 15)
        ])
        
        # Secondary indicators - Timing and obfuscation patterns
        timing_entropy = self._normalize(metrics.get('timing_entropy', 0), 0, 5)
        output_uniformity = equal_outputs  # Same as equal outputs for simplicity
        
        # NEW: Night trading ratio (mixers operate 24/7, often at night)
        night_trading = metrics.get('night_trading_ratio', 0)
        
        # Path complexity (estimated by transaction count and network degree)
        path_complexity = self._normalize(
            metrics.get('tx_count', 0) * metrics.get('out_degree', 1),
            0,
            1000
        )
        
        # NEW: High out-degree indicates distribution pattern
        high_out_degree = self._normalize(metrics.get('out_degree', 0), 0, 50)
        
        secondary_score = self._avg([
            timing_entropy,
            output_uniformity,
            night_trading,
            path_complexity,
            high_out_degree
        ])
        
        # Context indicators - Known mixing services and network position
        tornado_cash = 1.0 if metrics.get('tornado_cash_interaction', False) else 0
        high_betweenness = self._normalize(metrics.get('betweenness_centrality', 0), 0, 0.1)
        
        # NEW: Consistent transaction size (indicates automated mixing)
        automated_pattern = tx_size_consistency * timing_entropy
        
        # Mixed output reuse (low is suspicious)
        low_output_reuse = 1.0 - self._normalize(
            metrics.get('output_values', []).count(metrics.get('avg_output_value', 0)) 
            if metrics.get('output_values') else 0,
            0,
            10
        )
        
        context_score = self._avg([
            tornado_cash,
            high_betweenness,
            automated_pattern,
            low_output_reuse
        ])
        
        # Weighted combination
        final_score = (
            primary_score * self.WEIGHTS["primary"] +
            secondary_score * self.WEIGHTS["secondary"] +
            context_score * self.WEIGHTS["context"]
        )
        
        return final_score
