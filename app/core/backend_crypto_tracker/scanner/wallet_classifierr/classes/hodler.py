# ============================================================================
# classes/hodler.py
# ============================================================================
"""Hodler wallet analyzer."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.metric_definitions import HODLER_METRICS
from typing import Dict, Any

class HodlerAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Hodler wallets."""
    
    CLASS_NAME = "Hodler"
    METRICS = HODLER_METRICS
    THRESHOLD = 0.30
    WEIGHTS = {"primary": 0.7, "secondary": 0.2, "context": 0.1}
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """Compute Hodler score."""
        # Primary indicators - Core hodler behavior
        holding_period = self._normalize(metrics.get('holding_period_days', 0), 0, 730)
        balance_retention = metrics.get('balance_retention_ratio', 0)
        low_outgoing = 1.0 - metrics.get('outgoing_tx_ratio', 1.0)
        
        # NEW: Dormancy ratio (high for hodlers)
        dormancy = metrics.get('dormancy_ratio', 0)
        
        # NEW: Accumulation pattern (positive accumulation is typical for hodlers)
        accumulation = max(0, metrics.get('accumulation_pattern', 0))
        accumulation_score = self._normalize(accumulation, 0, 1)
        
        # UTXO age (longer is better for hodlers)
        utxo_age = self._normalize(metrics.get('holding_period_days', 0) * 0.8, 0, 600)
        
        # Last outgoing (longer since last tx is better)
        last_outgoing_age = self._normalize(
            metrics.get('age_days', 0) - metrics.get('last_seen', 0) / 86400,
            0,
            365
        )
        
        primary_score = self._avg([
            holding_period,
            balance_retention,
            low_outgoing,
            dormancy,
            accumulation_score,
            utxo_age,
            last_outgoing_age
        ])
        
        # Secondary indicators - Activity patterns
        balance_stable = 1.0 - self._normalize(metrics.get('turnover_rate', 0), 0, 2)
        inactive_ratio = 1.0 - self._normalize(metrics.get('tx_per_month', 0), 0, 10)
        
        # NEW: Balance utilization (high balance/value ratio for hodlers)
        balance_utilization = metrics.get('balance_utilization', 0)
        
        # NEW: Low weekend trading (hodlers don't actively trade)
        low_weekend = 1.0 - metrics.get('weekend_trading_ratio', 0)
        
        # NEW: Low business hours trading (hodlers don't day-trade)
        low_business_hours = 1.0 - metrics.get('business_hours_ratio', 1.0)
        
        secondary_score = self._avg([
            balance_stable,
            inactive_ratio,
            balance_utilization,
            low_weekend,
            low_business_hours
        ])
        
        # Context indicators (hodlers avoid exchanges and smart contracts)
        no_exchange = 1.0 - self._normalize(metrics.get('exchange_interaction_count', 0), 0, 10)
        no_smart_contracts = 1.0 if metrics.get('smart_contract_calls', 0) == 0 else 0.3
        low_out_degree = 1.0 - self._normalize(metrics.get('out_degree', 0), 0, 20)
        
        context_score = self._avg([no_exchange, no_smart_contracts, low_out_degree])
        
        # Weighted combination
        final_score = (
            primary_score * self.WEIGHTS["primary"] +
            secondary_score * self.WEIGHTS["secondary"] +
            context_score * self.WEIGHTS["context"]
        )
        
        return final_score
