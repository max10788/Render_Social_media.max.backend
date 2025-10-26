# ============================================================================
# wallet_classifier/dust_sweeper.py - ADAPTIVE VERSION
# ============================================================================
"""Dust Sweeper wallet analyzer with adaptive classification."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from typing import Dict, Any


class DustSweeperAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Dust Sweeper wallets using adaptive feature-based classification."""
    
    CLASS_NAME = "Dust Sweeper"
    THRESHOLD = 0.45  # 45% probability threshold
    
    # Metrics für Dokumentation und Feature-Extraktion
    METRICS = {
        "primary": [
            "avg_inputs_per_tx",        # Hohe Anzahl Inputs pro TX
            "avg_input_value_usd",      # Niedrige Input-Werte (Dust)
            "consolidation_rate",       # Hohe Consolidation-Rate
            "fan_in_score",             # Viele Quellen (Fan-in)
            "micro_tx_ratio",           # Hoher Anteil Micro-Transaktionen
            "single_output_ratio"       # Viele Single-Output TXs
        ],
        "secondary": [
            "timing_entropy",           # Regelmäßige Timing-Muster
            "in_degree",                # Hoher In-Degree
            "avg_output_value_usd"      # Niedrige Output-Werte
        ],
        "context": [
            "cluster_size",             # Teil größerer Operations
            "known_dust_service_interaction"
        ]
    }
    
    # Keine compute_score() Methode mehr nötig - verwendet AdaptiveClassifier
    
    def get_key_indicators(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gibt die wichtigsten Indikatoren für Dust Sweeper zurück.
        
        Returns:
            Dict mit Key Indicators und ihren Werten
        """
        return {
            "avg_inputs_per_tx": {
                "value": metrics.get('avg_inputs_per_tx', 0),
                "description": "Average inputs per transaction",
                "interpretation": "High = Consolidating from many sources"
            },
            "consolidation_rate": {
                "value": metrics.get('consolidation_rate', 0),
                "description": "Rate of consolidation transactions",
                "interpretation": "High = Typical dust sweeping behavior"
            },
            "micro_tx_ratio": {
                "value": metrics.get('micro_tx_ratio', 0),
                "description": "Ratio of micro transactions",
                "interpretation": "High = Dealing with small amounts (dust)"
            },
            "fan_in_score": {
                "value": metrics.get('fan_in_score', 0),
                "description": "Fan-in network pattern",
                "interpretation": "High = Collecting from many addresses"
            },
            "single_output_ratio": {
                "value": metrics.get('single_output_ratio', 0),
                "description": "Ratio of single-output transactions",
                "interpretation": "High = Consolidation pattern"
            }
        }
