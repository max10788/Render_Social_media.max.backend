# ============================================================================
# wallet_classifier/whale.py - ADAPTIVE VERSION
# ============================================================================
"""Whale wallet analyzer with adaptive classification."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from typing import Dict, Any


class WhaleAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Whale wallets using adaptive feature-based classification."""
    
    CLASS_NAME = "Whale"
    THRESHOLD = 0.55  # 55% probability threshold
    
    # Metrics für Dokumentation und Feature-Extraktion
    METRICS = {
        "primary": [
            "total_value_usd",          # Sehr hoher Gesamtwert
            "large_tx_ratio",           # Hoher Anteil großer TXs
            "portfolio_concentration",  # Konzentriertes Portfolio
            "net_inflow_usd",           # Große Kapitalströme
            "age_days"                  # Etabliertes Alter
        ],
        "secondary": [
            "holding_period_days",      # Lange Halteperioden
            "tx_per_month",             # Niedrige Aktivität
            "whale_cluster_member",     # Teil von Whale-Cluster
            "institutional_wallet"      # Institutionelles Wallet
        ],
        "context": [
            "eigenvector_centrality",   # Wichtige Netzwerkposition
            "governance_participation", # Governance-Teilnahme
            "cross_chain_presence"      # Multi-Chain Präsenz
        ]
    }
    
    def get_key_indicators(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gibt die wichtigsten Indikatoren für Whale zurück.
        
        Returns:
            Dict mit Key Indicators und ihren Werten
        """
        total_value = metrics.get('total_value_usd', 0)
        
        return {
            "total_value_usd": {
                "value": total_value,
                "description": "Total wallet value in USD",
                "interpretation": f"${total_value:,.0f} - {'WHALE (>$10M)' if total_value > 10_000_000 else 'Large holder' if total_value > 1_000_000 else 'Regular holder'}"
            },
            "large_tx_ratio": {
                "value": metrics.get('large_tx_ratio', 0),
                "description": "Ratio of large transactions (>$1M)",
                "interpretation": "High = Moving significant capital"
            },
            "portfolio_concentration": {
                "value": metrics.get('portfolio_concentration', 0),
                "description": "Concentration of holdings",
                "interpretation": "High = Focused strategy"
            },
            "age_days": {
                "value": metrics.get('age_days', 0),
                "description": "Wallet age in days",
                "interpretation": "High = Established player"
            },
            "institutional_wallet": {
                "value": metrics.get('institutional_wallet', False),
                "description": "Identified as institutional",
                "interpretation": "True = Likely exchange/institution"
            },
            "eigenvector_centrality": {
                "value": metrics.get('eigenvector_centrality', 0),
                "description": "Network influence score",
                "interpretation": "High = Connected to important addresses"
            }
        }
