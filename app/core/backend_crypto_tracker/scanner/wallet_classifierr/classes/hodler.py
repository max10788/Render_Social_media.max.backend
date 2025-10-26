# ============================================================================
# wallet_classifier/hodler.py - ADAPTIVE VERSION
# ============================================================================
"""Hodler wallet analyzer with adaptive classification."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from typing import Dict, Any


class HodlerAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Hodler wallets using adaptive feature-based classification."""
    
    CLASS_NAME = "Hodler"
    THRESHOLD = 0.50  # 50% probability threshold
    
    # Metrics für Dokumentation und Feature-Extraktion
    METRICS = {
        "primary": [
            "holding_period_days",      # Lange Halteperiode
            "balance_retention_ratio",  # Hohe Balance-Retention
            "dormancy_ratio",           # Hohe Dormancy
            "accumulation_pattern",     # Positive Accumulation
            "balance_utilization"       # Hohe Balance-Nutzung
        ],
        "secondary": [
            "outgoing_tx_ratio",        # Niedrige Outgoing-TX
            "tx_per_month",             # Niedrige Aktivität
            "weekend_trading_ratio",    # Niedrig (kein Trading)
            "turnover_rate"             # Niedrig (stabil)
        ],
        "context": [
            "exchange_interaction_count",  # Niedrig (keine Exchanges)
            "smart_contract_calls",        # Niedrig
            "out_degree"                   # Niedrig (isoliert)
        ]
    }
    
    def get_key_indicators(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gibt die wichtigsten Indikatoren für Hodler zurück.
        
        Returns:
            Dict mit Key Indicators und ihren Werten
        """
        return {
            "holding_period_days": {
                "value": metrics.get('holding_period_days', 0),
                "description": "Average holding period in days",
                "interpretation": "High = Long-term holder"
            },
            "balance_retention_ratio": {
                "value": metrics.get('balance_retention_ratio', 0),
                "description": "Ratio of balance retained",
                "interpretation": "High = Not selling, accumulating"
            },
            "dormancy_ratio": {
                "value": metrics.get('dormancy_ratio', 0),
                "description": "Ratio of inactive periods",
                "interpretation": "High = HODL behavior (buy and hold)"
            },
            "accumulation_pattern": {
                "value": metrics.get('accumulation_pattern', 0),
                "description": "Pattern of accumulation vs distribution",
                "interpretation": "Positive = Accumulating over time"
            },
            "tx_per_month": {
                "value": metrics.get('tx_per_month', 0),
                "description": "Transactions per month",
                "interpretation": "Low = Not actively trading"
            }
        }
