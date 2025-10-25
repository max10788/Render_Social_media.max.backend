# ============================================================================
# classes/dust_sweeper.py
# ============================================================================
"""Dust Sweeper wallet analyzer."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.metric_definitions import DUST_SWEEPER_METRICS
from typing import Dict, Any

class DustSweeperAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Dust Sweeper wallets."""
    
    CLASS_NAME = "Dust Sweeper"
    METRICS = DUST_SWEEPER_METRICS
    THRESHOLD = 0.45
    WEIGHTS = {"primary": 0.7, "secondary": 0.2, "context": 0.1}
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """Compute Dust Sweeper score."""
        # Primary indicators - Input aggregation patterns
        high_input_count = self._normalize(metrics.get('avg_inputs_per_tx', 0), 0, 10)
        low_input_value = 1.0 - self._normalize(metrics.get('avg_input_value_usd', 0), 0, 200)
        consolidation = metrics.get('consolidation_rate', 0)
        
        # NEW: Fan-in score (high for dust sweepers collecting from many sources)
        fan_in = metrics.get('fan_in_score', 0)
        fan_in_score = self._normalize(fan_in, 0, 10)
        
        # NEW: Micro transaction ratio (high proportion of tiny transactions)
        micro_tx_ratio = metrics.get('micro_tx_ratio', 0)
        
        # Transactions with >= 5 inputs
        inputs_gte_5 = sum(
            1 for count in metrics.get('inputs_per_tx', {}).values() if count >= 5
        )
        inputs_gte_5_ratio = (
            inputs_gte_5 / len(metrics.get('inputs_per_tx', {}))
            if metrics.get('inputs_per_tx') else 0
        )
        
        # Single output transactions (typical consolidation pattern)
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
            fan_in_score,
            micro_tx_ratio,
            inputs_gte_5_ratio,
            single_output_ratio
        ])
        
        # Secondary indicators - Frequency and timing patterns
        # Dust consolidation frequency
        dust_freq = self._normalize(consolidation * metrics.get('tx_count', 0), 0, 50)
        
        # Timing regularity (automated sweepers have regular patterns)
        timing_regularity = 1.0 - self._normalize(metrics.get('timing_entropy', 0), 0, 5)
        
        # NEW: High in-degree (receiving from many sources)
        high_in_degree = self._normalize(metrics.get('in_degree', 0), 0, 50)
        
        # NEW: Consolidation efficiency (many inputs, few outputs)
        input_output_ratio = (
            metrics.get('avg_inputs_per_tx', 0) / max(metrics.get('avg_outputs_per_tx', 1), 1)
        )
        consolidation_efficiency = self._normalize(input_output_ratio, 0, 10)
        
        secondary_score = self._avg([
            dust_freq,
            timing_regularity,
            high_in_degree,
            consolidation_efficiency
        ])
        
        # Context indicators - Network position and behavior
        context_score = 0.5
        
        # High in-degree (many incoming connections)
        if metrics.get('in_degree', 0) > 10:
            context_score += 0.2
        
        # Cluster size (dust sweepers often part of larger operations)
        if metrics.get('cluster_size', 0) > 5:
            context_score += 0.2
        
        # NEW: Low output value (dust sweepers consolidate small amounts)
        avg_output_value = metrics.get('avg_output_value_usd', 0)
        if avg_output_value < 500:
            context_score += 0.1
        
        context_score = min(1.0, context_score)
        
        # Weighted combination
        final_score = (
            primary_score * self.WEIGHTS["primary"] +
            secondary_score * self.WEIGHTS["secondary"] +
            context_score * self.WEIGHTS["context"]
        )
        
        return final_score
