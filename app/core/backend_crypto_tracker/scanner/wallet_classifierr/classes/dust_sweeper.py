# ============================================================================
# classes/dust_sweeper.py
# ============================================================================
"""Dust Sweeper wallet analyzer."""

from core.base_analyzer import BaseWalletAnalyzer
from core.metric_definitions import DUST_SWEEPER_METRICS
from typing import Dict, Any


class DustSweeperAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Dust Sweeper wallets."""
    
    CLASS_NAME = "Dust Sweeper"
    METRICS = DUST_SWEEPER_METRICS
    THRESHOLD = 0.65
    WEIGHTS = {"primary": 0.7, "secondary": 0.2, "context": 0.1}
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """Compute Dust Sweeper score."""
        # Primary indicators
        high_input_count = self._normalize(metrics.get('avg_inputs_per_tx', 0), 0, 10)
        low_input_value = 1.0 - self._normalize(metrics.get('avg_input_value_usd', 0), 0, 200)
        consolidation = metrics.get('consolidation_rate', 0)
        
        inputs_gte_5 = sum(
            1 for count in metrics.get('inputs_per_tx', {}).values() if count >= 5
        )
        inputs_gte_5_ratio = (
            inputs_gte_5 / len(metrics.get('inputs_per_tx', {}))
            if metrics.get('inputs_per_tx') else 0
        )
        
        single_output = sum(
            1 for count in metrics.get('outputs_per_tx', {}).values() if count == 1
        )
        single_output_ratio = (
            single_output / len(metrics.get('outputs_per_tx', {}))
            if metrics.get('outputs_per_tx') else 0
        )
        
        primary_score = self._avg([
            high_input_count,
            low_input_value,
            consolidation,
            inputs_gte_5_ratio,
            single_output_ratio
        ])
        
        # Secondary indicators
        dust_freq = self._normalize(consolidation * metrics.get('tx_count', 0), 0, 50)
        timing_regularity = 1.0 - self._normalize(metrics.get('timing_entropy', 0), 0, 5)
        
        secondary_score = self._avg([dust_freq, timing_regularity])
        
        # Context indicators
        context_score = 0.5
        if metrics.get('in_degree', 0) > 10:
            context_score += 0.3
        if metrics.get('cluster_size', 0) > 5:
            context_score += 0.2
        context_score = min(1.0, context_score)
        
        # Weighted combination
        final_score = (
            primary_score * self.WEIGHTS["primary"] +
            secondary_score * self.WEIGHTS["secondary"] +
            context_score * self.WEIGHTS["context"]
        )
        
        return final_score
