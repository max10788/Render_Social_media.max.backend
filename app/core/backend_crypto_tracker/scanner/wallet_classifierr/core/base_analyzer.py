# ============================================================================
# core/base_analyzer.py (UPDATED FOR ADAPTIVE CLASSIFICATION)
# ============================================================================
"""Base class for all wallet class analyzers with adaptive classification."""

from typing import Dict, Any, List, Optional
from .stages_blockchain import Stage1_RawMetrics
from .stages import Stage2_DerivedMetrics, Stage3_ContextAnalysis
from .adaptive_classifier import AdaptiveClassifier


class BaseWalletAnalyzer:
    """
    Base class for wallet classification with adaptive feature-based scoring.
    """
    
    CLASS_NAME = "Base"
    METRICS = {}
    THRESHOLD = 0.5  # Now interpreted as minimum probability
    
    def __init__(self):
        self.stage1 = Stage1_RawMetrics()
        self.stage2 = Stage2_DerivedMetrics()
        self.stage3 = Stage3_ContextAnalysis()
    
    def analyze(
        self,
        address: str,
        blockchain_data: Dict[str, Any],
        context_db: Any = None,
        config: Dict[str, Any] = None
    ) -> float:
        """
        Perform complete analysis through all stages.
        
        Args:
            address: Wallet address
            blockchain_data: Raw blockchain transaction data
            context_db: Optional context database
            config: Optional configuration
            
        Returns:
            Classification probability [0, 1] for this specific class
        """
        # Stage 1: Raw metrics
        raw_metrics = self.stage1.execute(blockchain_data, config)
        
        # Stage 2: Derived metrics
        derived_metrics = self.stage2.execute(raw_metrics, config)
        
        # Stage 3: Context
        context_metrics = self.stage3.execute(derived_metrics, address, context_db)
        
        # Combine all metrics
        all_metrics = {**raw_metrics, **derived_metrics, **context_metrics}
        
        # Compute class-specific score using adaptive classifier
        score = self.compute_score(all_metrics)
        
        return score
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """
        Compute classification probability using adaptive feature-based classifier.
        
        Args:
            metrics: Combined metrics from all stages
            
        Returns:
            Probability [0, 1] for this specific class
        """
        probabilities = AdaptiveClassifier.classify(metrics)
        return probabilities.get(self.CLASS_NAME, 0.0)
    
    def analyze_with_explanation(
        self,
        address: str,
        blockchain_data: Dict[str, Any],
        context_db: Any = None,
        config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Perform analysis with detailed explanation.
        
        Returns:
            {
                'score': float,
                'is_class': bool,
                'all_probabilities': {...},
                'features': {...},
                'reasoning': [...],
                'confidence': float
            }
        """
        # Get all metrics
        raw_metrics = self.stage1.execute(blockchain_data, config)
        derived_metrics = self.stage2.execute(raw_metrics, config)
        context_metrics = self.stage3.execute(derived_metrics, address, context_db)
        all_metrics = {**raw_metrics, **derived_metrics, **context_metrics}
        
        # Get detailed explanation
        result = AdaptiveClassifier.classify_with_explanation(all_metrics)
        
        return {
            'score': result['probabilities'][self.CLASS_NAME],
            'is_class': self.is_class(result['probabilities'][self.CLASS_NAME]),
            'all_probabilities': result['probabilities'],
            'features': result['features'],
            'reasoning': result['reasoning'][self.CLASS_NAME],
            'confidence': result['confidence'],
            'top_class': result['top_class'],
            'metrics': all_metrics
        }
    
    def is_class(self, score: float) -> bool:
        """
        Determine if probability indicates membership in this class.
        
        Args:
            score: Classification probability
            
        Returns:
            True if wallet belongs to this class
        """
        return score >= self.THRESHOLD
    
    def _avg(self, values: List[float]) -> float:
        """Calculate average, handling empty lists."""
        return sum(values) / len(values) if values else 0
    
    def _normalize(self, value: float, min_val: float, max_val: float) -> float:
        """Normalize value to [0, 1]."""
        if max_val == min_val:
            return 0.5
        return max(0, min(1, (value - min_val) / (max_val - min_val)))


# ============================================================================
# Example: Updated Analyzer Classes
# ============================================================================

class DustSweeperAnalyzer(BaseWalletAnalyzer):
    """Dust Sweeper classifier using adaptive features."""
    
    CLASS_NAME = "Dust Sweeper"
    THRESHOLD = 0.5  # 50% probability threshold
    
    # Metrics still defined for reference/documentation
    METRICS = {
        "primary": [
            "input_count_per_tx",
            "avg_input_value_usd",
            "tx_consolidation_rate",
            "inputs_gte_5_ratio",
            "single_output_ratio"
        ],
        "secondary": [
            "dust_aggregation_frequency",
            "time_between_inputs_avg",
            "input_source_diversity"
        ],
        "context": [
            "known_dust_service_interaction",
            "change_address_reuse_ratio",
            "cluster_size",
            "in_degree_centrality"
        ]
    }
    
    # No need to override compute_score - uses adaptive classifier


class HodlerAnalyzer(BaseWalletAnalyzer):
    """Hodler classifier using adaptive features."""
    
    CLASS_NAME = "Hodler"
    THRESHOLD = 0.5
    
    METRICS = {
        "primary": [
            "holding_period_days",
            "balance_retention_ratio",
            "outgoing_tx_ratio",
            "utxo_age_avg",
            "last_outgoing_tx_age_days"
        ],
        "secondary": [
            "balance_stability_index",
            "inactive_days_ratio",
            "value_growth_vs_market"
        ],
        "context": [
            "exchange_interaction_count",
            "smart_contract_calls",
            "out_degree",
            "isolation_score"
        ]
    }


class MixerAnalyzer(BaseWalletAnalyzer):
    """Mixer classifier using adaptive features."""
    
    CLASS_NAME = "Mixer"
    THRESHOLD = 0.6  # Higher threshold for privacy-sensitive classification
    
    METRICS = {
        "primary": [
            "equal_output_proportion",
            "known_mixer_interaction",
            "coinjoin_frequency",
            "round_amounts_ratio",
            "high_input_count_ratio"
        ],
        "secondary": [
            "timing_entropy",
            "output_uniformity_score",
            "path_complexity"
        ],
        "context": [
            "tornado_cash_interaction",
            "betweenness_centrality",
            "mixed_output_reuse_ratio",
            "cluster_fragmentation"
        ]
    }


class TraderAnalyzer(BaseWalletAnalyzer):
    """Trader classifier using adaptive features."""
    
    CLASS_NAME = "Trader"
    THRESHOLD = 0.5
    
    METRICS = {
        "primary": [
            "tx_count_per_month",
            "bidirectional_flow_ratio",
            "exchange_interaction_freq",
            "avg_tx_value_usd",
            "short_holding_time_ratio"
        ],
        "secondary": [
            "volatility_exposure",
            "turnover_rate",
            "profit_loss_cycles"
        ],
        "context": [
            "dex_cex_smart_contract_calls",
            "out_degree",
            "token_diversity",
            "bridge_usage_count"
        ]
    }


class WhaleAnalyzer(BaseWalletAnalyzer):
    """Whale classifier using adaptive features."""
    
    CLASS_NAME = "Whale"
    THRESHOLD = 0.5
    
    METRICS = {
        "primary": [
            "total_value_usd",
            "single_tx_over_1m_count",
            "portfolio_concentration",
            "net_inflow_usd",
            "address_age_days"
        ],
        "secondary": [
            "market_impact_estimate",
            "liquidity_absorption_ratio",
            "whale_cluster_membership"
        ],
        "context": [
            "institutional_wallet_interaction",
            "governance_participation_count",
            "cross_chain_presence",
            "eigenvector_centrality"
        ]
    }


# ============================================================================
# Backward Compatibility Layer
# ============================================================================

class LegacyScoreAdapter:
    """
    Adapter for systems that expect old-style [0, 1] scores.
    Converts probabilities to binary scores using thresholds.
    """
    
    @staticmethod
    def adapt_result(
        probabilities: Dict[str, float],
        thresholds: Dict[str, float] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Convert probability distribution to legacy score format.
        
        Args:
            probabilities: Class probabilities from adaptive classifier
            thresholds: Optional custom thresholds per class
            
        Returns:
            Legacy format: {class_name: {'score': float, 'is_class': bool}}
        """
        if thresholds is None:
            thresholds = {
                "Dust Sweeper": 0.5,
                "Hodler": 0.5,
                "Mixer": 0.6,
                "Trader": 0.5,
                "Whale": 0.5
            }
        
        result = {}
        for class_name, probability in probabilities.items():
            threshold = thresholds.get(class_name, 0.5)
            result[class_name] = {
                'score': probability,
                'is_class': probability >= threshold,
                'threshold': threshold
            }
        
        return result
