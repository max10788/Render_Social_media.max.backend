# ============================================================================
# classes/trader.py
# ============================================================================
"""Trader wallet analyzer."""

from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.metric_definitions import TRADER_METRICS
from typing import Dict, Any


class TraderAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Trader wallets."""
    
    CLASS_NAME = "Trader"
    METRICS = TRADER_METRICS
    THRESHOLD = 0.6
    WEIGHTS = {"primary": 0.65, "secondary": 0.2, "context": 0.15}
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """Compute Trader score."""
        # Primary indicators
        high_tx_freq = self._normalize(metrics.get('tx_per_month', 0), 0, 50)
        
        # Bidirectional flow (both sending and receiving)
        in_out_ratio = min(
            metrics.get('total_sent', 1) / metrics.get('total_received', 1),
            metrics.get('total_received', 1) / metrics.get('total_sent', 1)
        ) if metrics.get('total_received', 0) > 0 else 0
        
        exchange_freq = self._normalize(metrics.get('exchange_interaction_count', 0), 0, 20)
        avg_tx_value = self._normalize(metrics.get('total_value_usd', 0) / max(metrics.get('tx_count', 1), 1), 0, 10000)
        
        # Short holding time
        short_holding = 1.0 - self._normalize(metrics.get('holding_period_days', 0), 0, 365)
        
        primary_score = self._avg([
            high_tx_freq,
            in_out_ratio,
            exchange_freq,
            avg_tx_value,
            short_holding
        ])
        
        # Secondary indicators
        volatility = self._normalize(metrics.get('turnover_rate', 0), 0, 5)
        turnover = self._normalize(metrics.get('turnover_rate', 0), 0, 10)
        
        secondary_score = self._avg([volatility, turnover])
        
        # Context indicators
        dex_cex = self._normalize(metrics.get('dex_cex_interactions', 0), 0, 50)
        high_out_degree = self._normalize(metrics.get('out_degree', 0), 0, 50)
        
        context_score = self._avg([dex_cex, high_out_degree])
        
        # Weighted combination
        final_score = (
            primary_score * self.WEIGHTS["primary"] +
            secondary_score * self.WEIGHTS["secondary"] +
            context_score * self.WEIGHTS["context"]
        )
        
        return final_score
