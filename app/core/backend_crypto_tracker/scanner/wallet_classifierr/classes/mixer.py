b# ============================================================================
# wallet_classifier/mixer.py - ADAPTIVE VERSION
# ============================================================================
"""Mixer wallet analyzer with adaptive classification."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from typing import Dict, Any


class MixerAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Mixer wallets using adaptive feature-based classification."""
    
    CLASS_NAME = "Mixer"
    THRESHOLD = 0.60  # 60% probability threshold (higher for privacy-sensitive classification)
    
    # Metrics für Dokumentation und Feature-Extraktion
    METRICS = {
        "primary": [
            "equal_output_proportion",  # Gleiche Output-Beträge
            "coinjoin_frequency",       # CoinJoin Aktivität
            "tx_size_consistency",      # Konsistente TX-Größe
            "fan_out_score",            # Verteilung an viele Adressen
            "round_amounts_ratio",      # Runde Beträge
            "known_mixer_interaction"   # Bekannte Mixer
        ],
        "secondary": [
            "timing_entropy",           # Hohe Timing-Entropie
            "night_trading_ratio",      # 24/7 Betrieb
            "out_degree",               # Hoher Out-Degree
            "avg_inputs_per_tx"         # Viele Inputs (CoinJoin)
        ],
        "context": [
            "tornado_cash_interaction", # Tornado Cash Nutzung
            "betweenness_centrality",   # Zentrale Position im Netzwerk
            "cluster_fragmentation"     # Fragmentierung
        ]
    }
    
    def get_key_indicators(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gibt die wichtigsten Indikatoren für Mixer zurück.
        
        Returns:
            Dict mit Key Indicators und ihren Werten
        """
        return {
            "equal_output_proportion": {
                "value": metrics.get('equal_output_proportion', 0),
                "description": "Proportion of equal-value outputs",
                "interpretation": "High = Typical mixing pattern"
            },
            "coinjoin_frequency": {
                "value": metrics.get('coinjoin_frequency', 0),
                "description": "Frequency of CoinJoin transactions",
                "interpretation": "High = Privacy-focused transactions"
            },
            "tx_size_consistency": {
                "value": metrics.get('tx_size_consistency', 0),
                "description": "Consistency in transaction sizes",
                "interpretation": "High = Automated mixing service"
            },
            "fan_out_score": {
                "value": metrics.get('fan_out_score', 0),
                "description": "Distribution pattern to many addresses",
                "interpretation": "High = Distributing mixed funds"
            },
            "known_mixer_interaction": {
                "value": metrics.get('known_mixer_interaction', False),
                "description": "Interaction with known mixing services",
                "interpretation": "True = Direct mixer connection"
            }
        }
