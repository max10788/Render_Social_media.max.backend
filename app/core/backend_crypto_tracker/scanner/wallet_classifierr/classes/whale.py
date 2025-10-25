# ============================================================================
# classes/whale.py
# ============================================================================
"""Whale wallet analyzer."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.metric_definitions import WHALE_METRICS
from typing import Dict, Any

class WhaleAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Whale wallets."""
    
    CLASS_NAME = "Whale"
    METRICS = WHALE_METRICS
    THRESHOLD = 0.55
    WEIGHTS = {"primary": 0.7, "secondary": 0.2, "context": 0.1}
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """Compute Whale score."""
        # Primary indicators - Value and transaction size
        total_value = metrics.get('total_value_usd', 0)
        
        # Top 1% threshold (simplified: >$10M)
        value_score = 1.0 if total_value > 10_000_000 else self._normalize(total_value, 0, 10_000_000)
        
        # NEW: Large transaction ratio
        large_tx_ratio = metrics.get('large_tx_ratio', 0)
        
        # Count of large transactions (legacy metric)
        large_tx_count = sum(
            1 for val in metrics.get('output_values', [])
            if val * 50000 > 1_000_000  # Assuming BTC price
        )
        large_tx_score = self._normalize(large_tx_count, 0, 10)
        
        # Portfolio concentration
        concentration = metrics.get('portfolio_concentration', 0)
        
        # Net inflow
        net_inflow_score = self._normalize(abs(metrics.get('net_inflow_usd', 0)), 0, 5_000_000)
        
        # Address age (older is more established)
        age_score = self._normalize(metrics.get('age_days', 0), 0, 1825)  # 5 years
        
        primary_score = self._avg([
            value_score,
            large_tx_ratio,
            large_tx_score,
            concentration,
            net_inflow_score,
            age_score
        ])
        
        # Secondary indicators - Activity and connections
        # Low transaction frequency (whales don't trade frequently)
        low_tx_freq = 1.0 - self._normalize(metrics.get('tx_per_month', 0), 0, 20)
        
        # Whale cluster membership
        whale_cluster = 1.0 if metrics.get('whale_cluster_member', False) else 0
        
        # NEW: Holding pattern (whales tend to hold)
        holding_score = self._normalize(metrics.get('holding_period_days', 0), 0, 730)
        
        secondary_score = self._avg([
            low_tx_freq,
            whale_cluster,
            holding_score,
            value_score * 0.5
        ])
        
        # Context indicators - Institutional and network position
        institutional = 1.0 if metrics.get('institutional_wallet', False) else 0
        high_eigenvector = self._normalize(metrics.get('eigenvector_centrality', 0), 0, 0.1)
        
        # NEW: Strategic behavior (low activity but high value)
        strategic_behavior = value_score * (1.0 - self._normalize(metrics.get('tx_per_month', 0), 0, 10))
        
        context_score = self._avg([
            institutional,
            high_eigenvector,
            strategic_behavior
        ])
        
        # Weighted combination
        final_score = (
            primary_score * self.WEIGHTS["primary"] +
            secondary_score * self.WEIGHTS["secondary"] +
            context_score * self.WEIGHTS["context"]
        )
        
        return final_score
