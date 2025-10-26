# ============================================================================
# core/adaptive_classifier.py
# ============================================================================
"""
Adaptive Feature-Based Wallet Classification System
Converts raw metrics into interpretable features and computes class probabilities.
"""

from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
import math


@dataclass
class FeatureDefinition:
    """Definition of a single feature with its thresholds."""
    name: str
    metric_key: str  # Key from metrics dict
    levels: Dict[str, Tuple[float, float]]  # level_name -> (min, max) or callable
    description: str


class FeatureExtractor:
    """Extracts interpretable features from raw metrics."""
    
    FEATURES = {
        # ==================== TRANSACTION ACTIVITY ====================
        "transaction_frequency": FeatureDefinition(
            name="transaction_frequency",
            metric_key="tx_count_per_month",
            levels={
                "very_high": (50, float('inf')),
                "high": (10, 50),
                "medium": (1, 10),
                "low": (0, 1)
            },
            description="Monthly transaction frequency"
        ),
        
        "bidirectional_activity": FeatureDefinition(
            name="bidirectional_activity",
            metric_key="bidirectional_flow_ratio",
            levels={
                "high": (0.7, 1.0),
                "medium": (0.3, 0.7),
                "low": (0, 0.3)
            },
            description="Ratio of bidirectional flows"
        ),
        
        # ==================== HOLDING BEHAVIOR ====================
        "holding_duration": FeatureDefinition(
            name="holding_duration",
            metric_key="holding_period_days",
            levels={
                "very_long": (730, float('inf')),  # 2+ years
                "long": (365, 730),
                "medium": (90, 365),
                "short": (30, 90),
                "very_short": (0, 30)
            },
            description="Average holding period"
        ),
        
        "balance_retention": FeatureDefinition(
            name="balance_retention",
            metric_key="balance_retention_ratio",
            levels={
                "very_high": (0.9, 1.0),
                "high": (0.7, 0.9),
                "medium": (0.4, 0.7),
                "low": (0, 0.4)
            },
            description="Current balance vs total received"
        ),
        
        "utxo_age": FeatureDefinition(
            name="utxo_age",
            metric_key="utxo_age_avg",
            levels={
                "ancient": (730, float('inf')),
                "old": (365, 730),
                "mature": (90, 365),
                "fresh": (0, 90)
            },
            description="Average age of unspent outputs"
        ),
        
        # ==================== VALUE PROFILE ====================
        "total_value": FeatureDefinition(
            name="total_value",
            metric_key="total_value_usd",
            levels={
                "mega_whale": (100_000_000, float('inf')),
                "whale": (10_000_000, 100_000_000),
                "large": (1_000_000, 10_000_000),
                "medium": (100_000, 1_000_000),
                "small": (10_000, 100_000),
                "tiny": (0, 10_000)
            },
            description="Total portfolio value in USD"
        ),
        
        "single_tx_size": FeatureDefinition(
            name="single_tx_size",
            metric_key="single_tx_over_1m_count",
            levels={
                "frequent_large": (10, float('inf')),
                "occasional_large": (1, 10),
                "none": (0, 1)
            },
            description="Transactions over $1M"
        ),
        
        "avg_transaction_value": FeatureDefinition(
            name="avg_transaction_value",
            metric_key="avg_tx_value_usd",
            levels={
                "very_high": (100_000, float('inf')),
                "high": (10_000, 100_000),
                "medium": (1_000, 10_000),
                "low": (100, 1_000),
                "very_low": (0, 100)
            },
            description="Average transaction size"
        ),
        
        # ==================== DUST SWEEPING BEHAVIOR ====================
        "input_consolidation": FeatureDefinition(
            name="input_consolidation",
            metric_key="input_count_per_tx",
            levels={
                "extreme": (20, float('inf')),
                "high": (10, 20),
                "medium": (5, 10),
                "low": (0, 5)
            },
            description="Average inputs per transaction"
        ),
        
        "input_value_size": FeatureDefinition(
            name="input_value_size",
            metric_key="avg_input_value_usd",
            levels={
                "dust": (0, 10),
                "small": (10, 100),
                "medium": (100, 1000),
                "large": (1000, float('inf'))
            },
            description="Average input value"
        ),
        
        "consolidation_pattern": FeatureDefinition(
            name="consolidation_pattern",
            metric_key="tx_consolidation_rate",
            levels={
                "constant": (0.8, 1.0),
                "frequent": (0.5, 0.8),
                "occasional": (0.2, 0.5),
                "rare": (0, 0.2)
            },
            description="Ratio of consolidation transactions"
        ),
        
        "multi_input_txs": FeatureDefinition(
            name="multi_input_txs",
            metric_key="inputs_gte_5_ratio",
            levels={
                "dominant": (0.7, 1.0),
                "frequent": (0.4, 0.7),
                "occasional": (0.1, 0.4),
                "rare": (0, 0.1)
            },
            description="Transactions with 5+ inputs"
        ),
        
        # ==================== MIXING BEHAVIOR ====================
        "equal_outputs": FeatureDefinition(
            name="equal_outputs",
            metric_key="equal_output_proportion",
            levels={
                "dominant": (0.7, 1.0),
                "frequent": (0.4, 0.7),
                "occasional": (0.1, 0.4),
                "rare": (0, 0.1)
            },
            description="Proportion of equal-value outputs"
        ),
        
        "mixer_interaction": FeatureDefinition(
            name="mixer_interaction",
            metric_key="known_mixer_interaction",
            levels={
                "confirmed": (1, 2),
                "none": (0, 1)
            },
            description="Interaction with known mixers"
        ),
        
        "coinjoin_activity": FeatureDefinition(
            name="coinjoin_activity",
            metric_key="coinjoin_frequency",
            levels={
                "frequent": (0.5, float('inf')),
                "occasional": (0.1, 0.5),
                "rare": (0, 0.1)
            },
            description="CoinJoin-like pattern frequency"
        ),
        
        "round_amounts": FeatureDefinition(
            name="round_amounts",
            metric_key="round_amounts_ratio",
            levels={
                "dominant": (0.7, 1.0),
                "frequent": (0.4, 0.7),
                "occasional": (0.1, 0.4),
                "rare": (0, 0.1)
            },
            description="Ratio of round-number amounts"
        ),
        
        # ==================== EXCHANGE INTERACTION ====================
        "exchange_frequency": FeatureDefinition(
            name="exchange_frequency",
            metric_key="exchange_interaction_freq",
            levels={
                "very_high": (50, float('inf')),
                "high": (20, 50),
                "medium": (5, 20),
                "low": (1, 5),
                "none": (0, 1)
            },
            description="Frequency of exchange interactions"
        ),
        
        "dex_cex_activity": FeatureDefinition(
            name="dex_cex_activity",
            metric_key="dex_cex_smart_contract_calls",
            levels={
                "very_high": (100, float('inf')),
                "high": (20, 100),
                "medium": (5, 20),
                "low": (0, 5)
            },
            description="DEX/CEX smart contract calls"
        ),
        
        # ==================== PORTFOLIO BEHAVIOR ====================
        "portfolio_concentration": FeatureDefinition(
            name="portfolio_concentration",
            metric_key="portfolio_concentration",
            levels={
                "highly_concentrated": (0.8, 1.0),
                "concentrated": (0.6, 0.8),
                "diversified": (0.3, 0.6),
                "highly_diversified": (0, 0.3)
            },
            description="Gini coefficient of holdings"
        ),
        
        "token_diversity": FeatureDefinition(
            name="token_diversity",
            metric_key="token_diversity",
            levels={
                "very_high": (20, float('inf')),
                "high": (10, 20),
                "medium": (5, 10),
                "low": (2, 5),
                "single": (0, 2)
            },
            description="Number of different tokens"
        ),
        
        # ==================== NETWORK CONTEXT ====================
        "network_connectivity": FeatureDefinition(
            name="network_connectivity",
            metric_key="out_degree",
            levels={
                "hub": (100, float('inf')),
                "highly_connected": (50, 100),
                "connected": (10, 50),
                "isolated": (0, 10)
            },
            description="Network out-degree"
        ),
        
        "centrality": FeatureDefinition(
            name="centrality",
            metric_key="eigenvector_centrality",
            levels={
                "central": (0.1, float('inf')),
                "moderate": (0.05, 0.1),
                "peripheral": (0, 0.05)
            },
            description="Network eigenvector centrality"
        ),
        
        # ==================== TEMPORAL PATTERNS ====================
        "activity_recency": FeatureDefinition(
            name="activity_recency",
            metric_key="last_outgoing_tx_age_days",
            levels={
                "dormant": (365, float('inf')),
                "inactive": (90, 365),
                "moderate": (30, 90),
                "active": (7, 30),
                "very_active": (0, 7)
            },
            description="Days since last outgoing transaction"
        ),
        
        "holding_stability": FeatureDefinition(
            name="holding_stability",
            metric_key="balance_stability_index",
            levels={
                "very_stable": (0, 0.1),
                "stable": (0.1, 0.3),
                "moderate": (0.3, 0.6),
                "volatile": (0.6, float('inf'))
            },
            description="Balance variance over time"
        ),
    }
    
    @staticmethod
    def extract_features(metrics: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract all features from metrics.
        
        Args:
            metrics: Combined metrics from all stages
            
        Returns:
            Dictionary of feature_name -> feature_level
        """
        features = {}
        
        for feature_name, feature_def in FeatureExtractor.FEATURES.items():
            metric_value = metrics.get(feature_def.metric_key)
            
            if metric_value is None:
                features[feature_name] = "unknown"
                continue
            
            # Find matching level
            level = FeatureExtractor._find_level(metric_value, feature_def.levels)
            features[feature_name] = level
        
        return features
    
    @staticmethod
    def _find_level(value: float, levels: Dict[str, Tuple[float, float]]) -> str:
        """Find which level a value belongs to."""
        for level_name, (min_val, max_val) in levels.items():
            if min_val <= value < max_val:
                return level_name
        return "unknown"


class FeatureAffinityMatrix:
    """
    Defines how strongly each feature level contributes to each class.
    Values range from 0 (no affinity) to 1 (strong affinity).
    """
    
    AFFINITIES = {
        "Dust Sweeper": {
            # Strong indicators
            "input_consolidation": {"extreme": 1.0, "high": 0.9, "medium": 0.5, "low": 0.1},
            "input_value_size": {"dust": 1.0, "small": 0.7, "medium": 0.2, "large": 0.0},
            "consolidation_pattern": {"constant": 1.0, "frequent": 0.8, "occasional": 0.3, "rare": 0.0},
            "multi_input_txs": {"dominant": 1.0, "frequent": 0.8, "occasional": 0.3, "rare": 0.0},
            
            # Supporting indicators
            "total_value": {"tiny": 0.8, "small": 0.5, "medium": 0.2, "large": 0.0, "whale": 0.0, "mega_whale": 0.0},
            "transaction_frequency": {"low": 0.6, "medium": 0.8, "high": 0.4, "very_high": 0.2},
            "network_connectivity": {"isolated": 0.7, "connected": 0.4, "highly_connected": 0.1, "hub": 0.0},
            
            # Negative indicators
            "holding_duration": {"very_short": 0.8, "short": 0.6, "medium": 0.2, "long": 0.0, "very_long": 0.0},
            "mixer_interaction": {"confirmed": 0.0, "none": 0.9},
        },
        
        "Hodler": {
            # Strong indicators
            "holding_duration": {"very_long": 1.0, "long": 0.9, "medium": 0.5, "short": 0.1, "very_short": 0.0},
            "balance_retention": {"very_high": 1.0, "high": 0.8, "medium": 0.4, "low": 0.0},
            "utxo_age": {"ancient": 1.0, "old": 0.9, "mature": 0.6, "fresh": 0.1},
            "activity_recency": {"dormant": 0.9, "inactive": 1.0, "moderate": 0.5, "active": 0.2, "very_active": 0.0},
            
            # Supporting indicators
            "holding_stability": {"very_stable": 1.0, "stable": 0.8, "moderate": 0.4, "volatile": 0.0},
            "transaction_frequency": {"low": 1.0, "medium": 0.4, "high": 0.1, "very_high": 0.0},
            "exchange_frequency": {"none": 0.9, "low": 0.7, "medium": 0.3, "high": 0.1, "very_high": 0.0},
            
            # Can be any value (neutral)
            "total_value": {"tiny": 0.5, "small": 0.5, "medium": 0.5, "large": 0.5, "whale": 0.5, "mega_whale": 0.5},
            "network_connectivity": {"isolated": 0.8, "connected": 0.5, "highly_connected": 0.2, "hub": 0.0},
            
            # Negative indicators
            "bidirectional_activity": {"high": 0.0, "medium": 0.3, "low": 0.9},
            "mixer_interaction": {"confirmed": 0.0, "none": 0.9},
        },
        
        "Mixer": {
            # Strong indicators
            "mixer_interaction": {"confirmed": 1.0, "none": 0.0},
            "equal_outputs": {"dominant": 1.0, "frequent": 0.9, "occasional": 0.5, "rare": 0.1},
            "coinjoin_activity": {"frequent": 1.0, "occasional": 0.7, "rare": 0.2},
            "round_amounts": {"dominant": 0.9, "frequent": 0.7, "occasional": 0.4, "rare": 0.1},
            
            # Supporting indicators
            "multi_input_txs": {"dominant": 0.8, "frequent": 0.6, "occasional": 0.3, "rare": 0.1},
            "network_connectivity": {"hub": 0.8, "highly_connected": 0.6, "connected": 0.4, "isolated": 0.1},
            "centrality": {"central": 0.9, "moderate": 0.6, "peripheral": 0.2},
            
            # Activity patterns
            "transaction_frequency": {"very_high": 0.7, "high": 0.8, "medium": 0.5, "low": 0.2},
            "bidirectional_activity": {"high": 0.8, "medium": 0.6, "low": 0.2},
            
            # Can be any value
            "total_value": {"tiny": 0.5, "small": 0.5, "medium": 0.5, "large": 0.5, "whale": 0.5, "mega_whale": 0.5},
            "holding_duration": {"very_short": 0.6, "short": 0.5, "medium": 0.4, "long": 0.3, "very_long": 0.2},
        },
        
        "Trader": {
            # Strong indicators
            "transaction_frequency": {"very_high": 1.0, "high": 0.9, "medium": 0.5, "low": 0.1},
            "bidirectional_activity": {"high": 1.0, "medium": 0.7, "low": 0.2},
            "exchange_frequency": {"very_high": 1.0, "high": 0.9, "medium": 0.6, "low": 0.2, "none": 0.0},
            "dex_cex_activity": {"very_high": 1.0, "high": 0.9, "medium": 0.6, "low": 0.2},
            
            # Supporting indicators
            "holding_duration": {"very_short": 1.0, "short": 0.9, "medium": 0.5, "long": 0.1, "very_long": 0.0},
            "activity_recency": {"very_active": 1.0, "active": 0.9, "moderate": 0.5, "inactive": 0.1, "dormant": 0.0},
            "token_diversity": {"very_high": 0.9, "high": 0.8, "medium": 0.6, "low": 0.3, "single": 0.1},
            "network_connectivity": {"hub": 0.8, "highly_connected": 0.7, "connected": 0.5, "isolated": 0.1},
            
            # Value profile (traders can be any size)
            "total_value": {"tiny": 0.3, "small": 0.5, "medium": 0.7, "large": 0.8, "whale": 0.6, "mega_whale": 0.4},
            "avg_transaction_value": {"very_high": 0.5, "high": 0.7, "medium": 0.8, "low": 0.6, "very_low": 0.3},
            
            # Negative indicators
            "balance_retention": {"very_high": 0.0, "high": 0.2, "medium": 0.6, "low": 0.9},
            "holding_stability": {"very_stable": 0.1, "stable": 0.3, "moderate": 0.7, "volatile": 1.0},
            "mixer_interaction": {"confirmed": 0.3, "none": 0.7},
        },
        
        "Whale": {
            # Strong indicators
            "total_value": {"mega_whale": 1.0, "whale": 1.0, "large": 0.7, "medium": 0.2, "small": 0.0, "tiny": 0.0},
            "single_tx_size": {"frequent_large": 1.0, "occasional_large": 0.8, "none": 0.0},
            "avg_transaction_value": {"very_high": 1.0, "high": 0.8, "medium": 0.4, "low": 0.1, "very_low": 0.0},
            
            # Supporting indicators
            "portfolio_concentration": {"highly_concentrated": 0.8, "concentrated": 0.7, "diversified": 0.5, "highly_diversified": 0.6},
            "network_connectivity": {"hub": 0.9, "highly_connected": 0.8, "connected": 0.5, "isolated": 0.6},
            "centrality": {"central": 1.0, "moderate": 0.7, "peripheral": 0.3},
            
            # Behavioral patterns (whales can be traders OR hodlers)
            "holding_duration": {"very_long": 0.8, "long": 0.7, "medium": 0.6, "short": 0.5, "very_short": 0.4},
            "transaction_frequency": {"very_high": 0.6, "high": 0.6, "medium": 0.5, "low": 0.7},
            "balance_retention": {"very_high": 0.8, "high": 0.7, "medium": 0.5, "low": 0.4},
            
            # Exchange interaction (institutional whales use exchanges)
            "exchange_frequency": {"very_high": 0.7, "high": 0.7, "medium": 0.6, "low": 0.5, "none": 0.4},
            
            # Negative indicators
            "input_value_size": {"dust": 0.0, "small": 0.1, "medium": 0.5, "large": 1.0},
            "mixer_interaction": {"confirmed": 0.2, "none": 0.8},
        }
    }
    
    @staticmethod
    def get_affinity(class_name: str, feature: str, level: str) -> float:
        """
        Get affinity score for a specific feature level and class.
        
        Args:
            class_name: Target class
            feature: Feature name
            level: Feature level
            
        Returns:
            Affinity score [0, 1], default 0.5 if not defined
        """
        if class_name not in FeatureAffinityMatrix.AFFINITIES:
            return 0.5
        
        class_affinities = FeatureAffinityMatrix.AFFINITIES[class_name]
        
        if feature not in class_affinities:
            return 0.5  # Neutral if feature not defined for this class
        
        feature_affinities = class_affinities[feature]
        
        return feature_affinities.get(level, 0.5)  # Neutral if level not defined


class AdaptiveClassifier:
    """
    Main adaptive classifier that computes class probabilities from features.
    """
    
    CLASSES = ["Dust Sweeper", "Hodler", "Mixer", "Trader", "Whale"]
    
    # Feature importance weights (how much each feature matters)
    FEATURE_WEIGHTS = {
        # Critical features (high weight)
        "mixer_interaction": 2.0,
        "total_value": 1.8,
        "holding_duration": 1.6,
        "transaction_frequency": 1.5,
        "input_consolidation": 1.5,
        "equal_outputs": 1.4,
        
        # Important features (medium-high weight)
        "balance_retention": 1.3,
        "exchange_frequency": 1.2,
        "bidirectional_activity": 1.2,
        "input_value_size": 1.2,
        "coinjoin_activity": 1.2,
        "single_tx_size": 1.1,
        
        # Supporting features (medium weight)
        "consolidation_pattern": 1.0,
        "utxo_age": 1.0,
        "activity_recency": 1.0,
        "dex_cex_activity": 1.0,
        "multi_input_txs": 1.0,
        "avg_transaction_value": 1.0,
        
        # Context features (lower weight)
        "token_diversity": 0.8,
        "network_connectivity": 0.8,
        "portfolio_concentration": 0.8,
        "holding_stability": 0.8,
        "round_amounts": 0.8,
        "centrality": 0.7,
    }
    
    @staticmethod
    def classify(
        metrics: Dict[str, Any],
        feature_overrides: Dict[str, str] = None
    ) -> Dict[str, float]:
        """
        Classify wallet based on metrics.
        
        Args:
            metrics: Raw metrics from analysis stages
            feature_overrides: Optional manual feature specifications
            
        Returns:
            Dictionary of class_name -> probability
        """
        # Extract features
        features = FeatureExtractor.extract_features(metrics)
        
        # Apply overrides if provided
        if feature_overrides:
            features.update(feature_overrides)
        
        # Compute raw scores for each class
        raw_scores = {}
        for class_name in AdaptiveClassifier.CLASSES:
            raw_scores[class_name] = AdaptiveClassifier._compute_class_score(
                class_name, features
            )
        
        # Normalize to probabilities
        probabilities = AdaptiveClassifier._normalize_scores(raw_scores)
        
        return probabilities
    
    @staticmethod
    def _compute_class_score(class_name: str, features: Dict[str, str]) -> float:
        """Compute weighted affinity score for a class."""
        total_weight = 0.0
        weighted_sum = 0.0
        
        for feature_name, feature_level in features.items():
            if feature_level == "unknown":
                continue
            
            affinity = FeatureAffinityMatrix.get_affinity(
                class_name, feature_name, feature_level
            )
            
            weight = AdaptiveClassifier.FEATURE_WEIGHTS.get(feature_name, 1.0)
            
            weighted_sum += affinity * weight
            total_weight += weight
        
        if total_weight == 0:
            return 0.5
        
        return weighted_sum / total_weight
    
    @staticmethod
    def _normalize_scores(scores: Dict[str, float]) -> Dict[str, float]:
        """
        Normalize scores to probabilities using softmax.
        
        Args:
            scores: Raw scores for each class
            
        Returns:
            Normalized probabilities that sum to 1.0
        """
        # Apply softmax with temperature
        temperature = 2.0  # Lower = more confident, higher = more distributed
        exp_scores = {
            class_name: math.exp(score / temperature)
            for class_name, score in scores.items()
        }
        
        total = sum(exp_scores.values())
        
        return {
            class_name: exp_score / total
            for class_name, exp_score in exp_scores.items()
        }
    
    @staticmethod
    def classify_with_explanation(
        metrics: Dict[str, Any],
        feature_overrides: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Classify with detailed explanation of reasoning.
        
        Returns:
            {
                'probabilities': {...},
                'features': {...},
                'top_class': str,
                'confidence': float,
                'reasoning': {class_name: [feature explanations]}
            }
        """
        features = FeatureExtractor.extract_features(metrics)
        if feature_overrides:
            features.update(feature_overrides)
        
        probabilities = AdaptiveClassifier.classify(metrics, feature_overrides)
        
        # Find top class
        top_class = max(probabilities.items(), key=lambda x: x[1])
        
        # Compute confidence (how much better top class is than second)
        sorted_probs = sorted(probabilities.values(), reverse=True)
        confidence = sorted_probs[0] - sorted_probs[1] if len(sorted_probs) > 1 else sorted_probs[0]
        
        # Generate reasoning
        reasoning = {}
        for class_name in AdaptiveClassifier.CLASSES:
            class_reasoning = []
            for feature_name, feature_level in features.items():
                if feature_level == "unknown":
                    continue
                
                affinity = FeatureAffinityMatrix.get_affinity(
                    class_name, feature_name, feature_level
                )
                weight = AdaptiveClassifier.FEATURE_WEIGHTS.get(feature_name, 1.0)
                
                # Only include significant features
                if affinity >= 0.7:
                    class_reasoning.append({
                        'feature': feature_name,
                        'level': feature_level,
                        'affinity': affinity,
                        'weight': weight,
                        'impact': 'strong_positive'
                    })
                elif affinity <= 0.3:
                    class_reasoning.append({
                        'feature': feature_name,
                        'level': feature_level,
                        'affinity': affinity,
                        'weight': weight,
                        'impact': 'strong_negative'
                    })
            
            # Sort by weighted impact
            class_reasoning.sort(
                key=lambda x: abs(x['affinity'] - 0.5) * x['weight'],
                reverse=True
            )
            
            reasoning[class_name] = class_reasoning[:5]  # Top 5 reasons
        
        return {
            'probabilities': probabilities,
            'features': features,
            'top_class': top_class[0],
            'confidence': confidence,
            'reasoning': reasoning,
            'metrics': metrics
        }


# ============================================================================
# Integration with existing system
# ============================================================================

def integrate_adaptive_classifier():
    """
    Example of how to integrate with existing BaseWalletAnalyzer.
    Replace the compute_score() method in each analyzer.
    """
    
    example_code = '''
# In wallet_classifier/base_analyzer.py - UPDATED compute_score()

def compute_score(self, metrics: Dict[str, Any]) -> float:
    """
    Compute classification score using adaptive classifier.
    
    Args:
        metrics: Combined metrics from all stages
        
    Returns:
        Probability score [0, 1] for this specific class
    """
    from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.adaptive_classifier import AdaptiveClassifier
    
    probabilities = AdaptiveClassifier.classify(metrics)
    return probabilities.get(self.CLASS_NAME, 0.0)
'''
    
    return example_code


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example: Manual feature specification
    manual_features = {
        "transaction_frequency": "high",
        "holding_duration": "short",
        "exchange_frequency": "very_high",
        "bidirectional_activity": "high",
        "total_value": "medium",
        "balance_retention": "low",
        "token_diversity": "high"
    }
    
    # Simulate metrics (normally from Stage 1-3)
    mock_metrics = {
        "tx_count_per_month": 25,
        "holding_period_days": 45,
        "exchange_interaction_freq": 60,
        "bidirectional_flow_ratio": 0.8,
        "total_value_usd": 250000,
        "balance_retention_ratio": 0.3,
        "token_diversity": 15,
        "avg_tx_value_usd": 5000,
        "known_mixer_interaction": 0,
        "input_count_per_tx": 2.5,
        "avg_input_value_usd": 1000,
        "equal_output_proportion": 0.1
    }
    
    # Method 1: Classify from metrics
    print("=" * 80)
    print("METHOD 1: Classification from Metrics")
    print("=" * 80)
    probabilities = AdaptiveClassifier.classify(mock_metrics)
    for class_name, prob in sorted(probabilities.items(), key=lambda x: x[1], reverse=True):
        print(f"{class_name:20s}: {prob:6.2%}")
    
    print("\n" + "=" * 80)
    print("METHOD 2: Classification with Manual Features")
    print("=" * 80)
    probabilities2 = AdaptiveClassifier.classify(mock_metrics, manual_features)
    for class_name, prob in sorted(probabilities2.items(), key=lambda x: x[1], reverse=True):
        print(f"{class_name:20s}: {prob:6.2%}")
    
    print("\n" + "=" * 80)
    print("METHOD 3: Classification with Detailed Explanation")
    print("=" * 80)
    result = AdaptiveClassifier.classify_with_explanation(mock_metrics)
    
    print(f"\nTop Class: {result['top_class']}")
    print(f"Confidence: {result['confidence']:.2%}")
    print(f"\nProbabilities:")
    for class_name, prob in sorted(result['probabilities'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {class_name:20s}: {prob:6.2%}")
    
    print(f"\nExtracted Features:")
    for feature, level in sorted(result['features'].items()):
        print(f"  {feature:30s}: {level}")
    
    print(f"\nTop Reasoning for '{result['top_class']}':")
    for reason in result['reasoning'][result['top_class']][:5]:
        impact_symbol = "✓" if reason['impact'] == 'strong_positive' else "✗"
        print(f"  {impact_symbol} {reason['feature']:30s} = {reason['level']:15s} "
              f"(affinity: {reason['affinity']:.2f}, weight: {reason['weight']:.1f})")
    
    print("\n" + "=" * 80)
    print("EXAMPLE: Whale Detection")
    print("=" * 80)
    whale_metrics = {
        "total_value_usd": 50_000_000,
        "single_tx_over_1m_count": 15,
        "avg_tx_value_usd": 500_000,
        "holding_period_days": 800,
        "balance_retention_ratio": 0.95,
        "tx_count_per_month": 5,
        "exchange_interaction_freq": 10,
        "portfolio_concentration": 0.7,
        "eigenvector_centrality": 0.15,
        "out_degree": 80
    }
    
    whale_result = AdaptiveClassifier.classify_with_explanation(whale_metrics)
    print(f"\nDetected: {whale_result['top_class']} ({whale_result['probabilities'][whale_result['top_class']]:.1%})")
    print("\nKey Indicators:")
    for reason in whale_result['reasoning'][whale_result['top_class']][:3]:
        print(f"  • {reason['feature']}: {reason['level']} (affinity: {reason['affinity']:.0%})")
    
    print("\n" + "=" * 80)
    print("EXAMPLE: Mixer Detection")
    print("=" * 80)
    mixer_metrics = {
        "known_mixer_interaction": 1,
        "equal_output_proportion": 0.85,
        "coinjoin_frequency": 0.6,
        "round_amounts_ratio": 0.75,
        "input_count_per_tx": 15,
        "tx_count_per_month": 40,
        "total_value_usd": 100_000,
        "holding_period_days": 20,
        "betweenness_centrality": 0.12
    }
    
    mixer_result = AdaptiveClassifier.classify_with_explanation(mixer_metrics)
    print(f"\nDetected: {mixer_result['top_class']} ({mixer_result['probabilities'][mixer_result['top_class']]:.1%})")
    print("\nKey Indicators:")
    for reason in mixer_result['reasoning'][mixer_result['top_class']][:3]:
        print(f"  • {reason['feature']}: {reason['level']} (affinity: {reason['affinity']:.0%})")
    
    print("\n" + "=" * 80)
    print("EXAMPLE: Dust Sweeper Detection")
    print("=" * 80)
    dust_metrics = {
        "input_count_per_tx": 25,
        "avg_input_value_usd": 5,
        "tx_consolidation_rate": 0.9,
        "inputs_gte_5_ratio": 0.95,
        "single_output_ratio": 0.85,
        "total_value_usd": 500,
        "tx_count_per_month": 8,
        "holding_period_days": 10
    }
    
    dust_result = AdaptiveClassifier.classify_with_explanation(dust_metrics)
    print(f"\nDetected: {dust_result['top_class']} ({dust_result['probabilities'][dust_result['top_class']]:.1%})")
    print("\nKey Indicators:")
    for reason in dust_result['reasoning'][dust_result['top_class']][:3]:
        print(f"  • {reason['feature']}: {reason['level']} (affinity: {reason['affinity']:.0%})")
