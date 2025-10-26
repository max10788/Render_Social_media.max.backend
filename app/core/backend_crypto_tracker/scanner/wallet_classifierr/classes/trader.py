# ============================================================================
# wallet_classifier/trader.py - ADAPTIVE VERSION
# ============================================================================
"""Trader wallet analyzer with adaptive classification."""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from typing import Dict, Any


class TraderAnalyzer(BaseWalletAnalyzer):
    """Analyzer for Trader wallets using adaptive feature-based classification."""
    
    CLASS_NAME = "Trader"
    THRESHOLD = 0.50  # 50% probability threshold
    
    # Metrics für Dokumentation und Feature-Extraktion
    METRICS = {
        "primary": [
            "tx_per_month",             # Hohe Transaktionsfrequenz
            "trading_regularity",       # Regelmäßiges Trading
            "activity_burst_ratio",     # Trading-Bursts
            "balance_volatility",       # Volatile Balance
            "turnover_rate",            # Hohe Turnover-Rate
            "exchange_interaction_count" # Exchange-Nutzung
        ],
        "secondary": [
            "business_hours_ratio",     # Trading während Geschäftszeiten
            "weekday_ratio",            # Wochentags-Trading
            "counterparty_diversity",   # Viele verschiedene Partner
            "smart_contract_ratio",     # DEX-Nutzung
            "dormancy_ratio"            # Niedrige Dormancy
        ],
        "context": [
            "out_degree",               # Viele Outgoing-Verbindungen
            "dex_cex_interactions",     # DEX/CEX Interaktionen
            "holding_period_days"       # Kurze Halteperioden
        ]
    }
    
    def get_key_indicators(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gibt die wichtigsten Indikatoren für Trader zurück.
        
        Returns:
            Dict mit Key Indicators und ihren Werten
        """
        return {
            "tx_per_month": {
                "value": metrics.get('tx_per_month', 0),
                "description": "Transactions per month",
                "interpretation": "High = Active trading"
            },
            "trading_regularity": {
                "value": metrics.get('trading_regularity', 0),
                "description": "Consistency of trading activity",
                "interpretation": "High = Regular, systematic trading"
            },
            "balance_volatility": {
                "value": metrics.get('balance_volatility', 0),
                "description": "Balance fluctuation over time",
                "interpretation": "High = Constantly changing positions"
            },
            "turnover_rate": {
                "value": metrics.get('turnover_rate', 0),
                "description": "Rate of capital turnover",
                "interpretation": "High = Fast in-and-out trading"
            },
            "exchange_interaction_count": {
                "value": metrics.get('exchange_interaction_count', 0),
                "description": "Interactions with exchanges",
                "interpretation": "High = Using exchanges for trading"
            },
            "business_hours_ratio": {
                "value": metrics.get('business_hours_ratio', 0),
                "description": "Trading during business hours",
                "interpretation": "High = Professional trading behavior"
            }
        }
