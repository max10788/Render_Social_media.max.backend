# ============================================================================
# core/adaptive_classifier.py - WITH EXTENSIVE LOGGING
# ============================================================================
"""
Adaptive Classifier with detailed logging to debug metrics and classification.

✅ Logs all features used
✅ Logs missing features
✅ Logs classification scores
✅ Detects when using default metrics
"""

from typing import Dict, Any, List, Tuple
import logging
import math

logger = logging.getLogger(__name__)


class AdaptiveClassifier:
    """
    Adaptive classification with comprehensive logging.
    """
    
    FEATURE_WEIGHTS = {
        "Dust Sweeper": {
            "avg_inputs_per_tx": 0.15,
            "consolidation_rate": 0.15,
            "fan_in_score": 0.12,
            "micro_tx_ratio": 0.10,
            "single_output_ratio": 0.10,
            "in_degree": 0.10,
            "avg_input_value_usd": -0.08,
            "timing_entropy": -0.06,
            "avg_output_value_usd": -0.08,
            "automated_pattern_score": 0.06,
        },
        
        "Hodler": {
            "holding_period_days": 0.18,
            "balance_retention_ratio": 0.15,
            "dormancy_ratio": 0.12,
            "accumulation_pattern": 0.10,
            "balance_utilization": 0.08,
            "outgoing_tx_ratio": -0.10,
            "tx_per_month": -0.10,
            "weekend_trading_ratio": -0.04,
            "exchange_interaction_count": -0.04,
            "token_diversity_score": 0.08,
            "stablecoin_ratio": -0.05,
            "dex_swap_count": -0.10,
            "dex_trading_ratio": -0.06,
        },
        
        "Mixer": {
            "equal_output_proportion": 0.18,
            "coinjoin_frequency": 0.15,
            "tx_size_consistency": 0.12,
            "fan_out_score": 0.12,
            "timing_entropy": 0.10,
            "night_trading_ratio": 0.08,
            "out_degree": 0.10,
            "known_mixer_interaction": 0.10,
            "round_amounts_ratio": 0.05,
            "automated_pattern_score": 0.08,
        },
        
        "Trader": {
            "tx_per_month": 0.10,
            "trading_regularity": 0.08,
            "activity_burst_ratio": 0.06,
            "business_hours_ratio": 0.06,
            "weekday_ratio": 0.06,
            "balance_volatility": 0.08,
            "turnover_rate": 0.08,
            "counterparty_diversity": 0.06,
            "smart_contract_ratio": 0.06,
            "exchange_interaction_count": 0.08,
            "dormancy_ratio": -0.06,
            "dex_swap_count": 0.15,
            "dex_protocols_used": 0.08,
            "dex_volume_usd": 0.10,
            "dex_trading_ratio": 0.12,
            "unique_tokens_held": 0.08,
            "portfolio_complexity": 0.06,
            "automated_pattern_score": 0.05,
        },
        
        "Whale": {
            "total_value_usd": 0.25,
            "large_tx_ratio": 0.15,
            "portfolio_concentration": 0.10,
            "age_days": 0.10,
            "holding_period_days": 0.10,
            "net_inflow_usd": 0.08,
            "eigenvector_centrality": 0.08,
            "institutional_wallet": 0.07,
            "tx_per_month": -0.05,
            "token_concentration_ratio": 0.08,
            "stablecoin_ratio": 0.05,
            "dex_volume_usd": 0.06,
        }
    }
    
    FEATURE_NORMALIZATION = {
        "avg_inputs_per_tx": [0, 20],
        "avg_input_value_usd": [0, 500],
        "consolidation_rate": [0, 1],
        "fan_in_score": [0, 10],
        "micro_tx_ratio": [0, 1],
        "single_output_ratio": [0, 1],
        "in_degree": [0, 100],
        "timing_entropy": [0, 5],
        "avg_output_value_usd": [0, 5000],
        "holding_period_days": [0, 730],
        "balance_retention_ratio": [0, 1],
        "dormancy_ratio": [0, 1],
        "accumulation_pattern": [-1, 1],
        "balance_utilization": [0, 1],
        "outgoing_tx_ratio": [0, 1],
        "tx_per_month": [0, 50],
        "weekend_trading_ratio": [0, 1],
        "exchange_interaction_count": [0, 20],
        "equal_output_proportion": [0, 1],
        "coinjoin_frequency": [0, 1],
        "tx_size_consistency": [0, 1],
        "fan_out_score": [0, 10],
        "night_trading_ratio": [0, 1],
        "out_degree": [0, 100],
        "known_mixer_interaction": [0, 1],
        "round_amounts_ratio": [0, 1],
        "trading_regularity": [0, 1],
        "activity_burst_ratio": [0, 1],
        "business_hours_ratio": [0, 1],
        "weekday_ratio": [0, 1],
        "balance_volatility": [0, 1],
        "turnover_rate": [0, 10],
        "counterparty_diversity": [0, 1],
        "smart_contract_ratio": [0, 1],
        "total_value_usd": [0, 50_000_000],
        "large_tx_ratio": [0, 1],
        "portfolio_concentration": [0, 1],
        "age_days": [0, 1825],
        "net_inflow_usd": [-10_000_000, 10_000_000],
        "eigenvector_centrality": [0, 0.1],
        "institutional_wallet": [0, 1],
        "unique_tokens_held": [0, 50],
        "token_diversity_score": [0, 1],
        "stablecoin_ratio": [0, 1],
        "token_concentration_ratio": [0, 1],
        "dex_swap_count": [0, 100],
        "dex_protocols_used": [0, 10],
        "dex_volume_usd": [0, 1_000_000],
        "dex_trading_ratio": [0, 1],
        "tx_timing_precision_score": [0, 1],
        "gas_price_optimization_score": [0, 1],
        "automated_pattern_score": [0, 1],
        "portfolio_complexity": [0, 1],
        "bot_likelihood_score": [0, 1],
    }
    
    @classmethod
    def normalize_feature(cls, feature_name: str, value: float) -> float:
        """Normalize feature value to [0, 1]."""
        if feature_name not in cls.FEATURE_NORMALIZATION:
            logger.debug(f"⚠️ Feature {feature_name} not in normalization table, using 0.5")
            return 0.5
        
        min_val, max_val = cls.FEATURE_NORMALIZATION[feature_name]
        
        if max_val == min_val:
            return 0.5
        
        normalized = (value - min_val) / (max_val - min_val)
        return max(0.0, min(1.0, normalized))
    
    @classmethod
    def extract_features(cls, metrics: Dict[str, Any]) -> Dict[str, float]:
        """
        Extract and normalize features from metrics.
        
        WITH LOGGING: Shows which features are found/missing
        """
        logger.info("="*70)
        logger.info("🔍 FEATURE EXTRACTION START")
        logger.info("="*70)
        
        features = {}
        
        # All possible features
        all_features = set()
        for class_features in cls.FEATURE_WEIGHTS.values():
            all_features.update(class_features.keys())
        
        logger.info(f"📊 Total features expected: {len(all_features)}")
        logger.info(f"📊 Total metrics available: {len(metrics)}")
        
        found_features = []
        missing_features = []
        
        for feature_name in sorted(all_features):
            raw_value = metrics.get(feature_name, None)
            
            if raw_value is None:
                missing_features.append(feature_name)
                logger.warning(f"❌ Feature '{feature_name}' NOT FOUND in metrics")
                features[feature_name] = 0.5  # Default
            else:
                found_features.append(feature_name)
                
                # Convert boolean to float
                if isinstance(raw_value, bool):
                    raw_value = 1.0 if raw_value else 0.0
                
                # Normalize
                normalized_value = cls.normalize_feature(feature_name, raw_value)
                features[feature_name] = normalized_value
                
                logger.info(f"✅ {feature_name}: {raw_value} → {normalized_value:.4f}")
        
        # ===== SUMMARY LOGGING =====
        logger.info("="*70)
        logger.info("📈 FEATURE EXTRACTION SUMMARY")
        logger.info("="*70)
        logger.info(f"✅ Found: {len(found_features)}/{len(all_features)} features")
        logger.info(f"❌ Missing: {len(missing_features)}/{len(all_features)} features")
        
        if missing_features:
            logger.warning("❌ Missing features list:")
            for feature in missing_features:
                logger.warning(f"   - {feature}")
        
        # ===== CHECK IF USING DEFAULTS =====
        is_all_defaults = cls._check_if_defaults(metrics)
        if is_all_defaults:
            logger.error("🚨 WARNING: ALL METRICS ARE DEFAULTS! No real transaction data!")
            logger.error("🚨 This means Stage1 returned default values (no transactions parsed)")
        
        logger.info("="*70)
        
        return features
    
    @classmethod
    def _check_if_defaults(cls, metrics: Dict[str, Any]) -> bool:
        """Check if metrics are all defaults (indicates no transaction data)."""
        # Key indicators of default metrics
        indicators = [
            metrics.get('tx_count', 1) == 0,
            metrics.get('total_tx_count', 1) == 0,
            metrics.get('total_received', 1) == 0,
            metrics.get('total_sent', 1) == 0,
            len(metrics.get('timestamps', [1])) == 0,
        ]
        
        return sum(indicators) >= 3  # If 3+ indicators are true, likely all defaults
    
    @classmethod
    def compute_class_score(
        cls,
        class_name: str,
        features: Dict[str, float]
    ) -> float:
        """
        Compute score for a specific class.
        
        WITH LOGGING: Shows contribution of each feature
        """
        if class_name not in cls.FEATURE_WEIGHTS:
            return 0.0
        
        logger.info(f"\n{'='*70}")
        logger.info(f"🎯 COMPUTING SCORE FOR: {class_name}")
        logger.info(f"{'='*70}")
        
        weights = cls.FEATURE_WEIGHTS[class_name]
        total_weight = sum(abs(w) for w in weights.values())
        
        if total_weight == 0:
            return 0.0
        
        score = 0.0
        contributions = []
        
        for feature_name, weight in weights.items():
            feature_value = features.get(feature_name, 0.5)
            
            # Invert for negative weights
            if weight < 0:
                effective_value = 1.0 - feature_value
                effective_weight = abs(weight)
            else:
                effective_value = feature_value
                effective_weight = weight
            
            contribution = effective_value * effective_weight
            score += contribution
            
            contributions.append((feature_name, feature_value, weight, contribution))
        
        # Sort by contribution
        contributions.sort(key=lambda x: abs(x[3]), reverse=True)
        
        # Log top 5 contributors
        logger.info(f"📊 Top 5 contributors to {class_name} score:")
        for i, (fname, fval, fweight, contrib) in enumerate(contributions[:5], 1):
            logger.info(f"  {i}. {fname:30s}: value={fval:.3f}, weight={fweight:+.3f} → contrib={contrib:.4f}")
        
        # Normalize to [0, 1]
        normalized_score = score / total_weight
        logger.info(f"📈 Raw score: {score:.4f}, Normalized: {normalized_score:.4f}")
        
        return normalized_score
    
    @classmethod
    def classify(cls, metrics: Dict[str, Any]) -> Dict[str, float]:
        """
        Classify wallet and return probabilities.
        
        WITH LOGGING: Shows full classification process
        """
        logger.info("\n" + "="*70)
        logger.info("🚀 STARTING CLASSIFICATION")
        logger.info("="*70)
        
        # Extract features (with logging)
        features = cls.extract_features(metrics)
        
        # Compute scores for all classes (with logging)
        raw_scores = {}
        for class_name in cls.FEATURE_WEIGHTS.keys():
            raw_scores[class_name] = cls.compute_class_score(class_name, features)
        
        # Apply softmax
        probabilities = cls._softmax(raw_scores)
        
        # ===== FINAL SUMMARY =====
        logger.info("\n" + "="*70)
        logger.info("🏆 CLASSIFICATION RESULTS")
        logger.info("="*70)
        
        sorted_probs = sorted(probabilities.items(), key=lambda x: -x[1])
        for i, (class_name, prob) in enumerate(sorted_probs, 1):
            emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            logger.info(f"{emoji} {i}. {class_name:15s}: {prob:.4f} ({prob*100:.2f}%)")
        
        # Check confidence
        top_2 = sorted(probabilities.values(), reverse=True)[:2]
        confidence = top_2[0] - top_2[1] if len(top_2) > 1 else top_2[0]
        logger.info(f"\n📊 Confidence (gap between top 2): {confidence:.4f} ({confidence*100:.2f}%)")
        
        if confidence < 0.1:
            logger.warning("⚠️ LOW CONFIDENCE! Top classes are very close.")
        
        logger.info("="*70 + "\n")
        
        return probabilities
    
    @classmethod
    def classify_with_explanation(
        cls,
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Classification with detailed explanation."""
        # Extract features
        features = cls.extract_features(metrics)
        
        # Classify
        probabilities = cls.classify(metrics)
        
        # Top class
        top_class = max(probabilities.items(), key=lambda x: x[1])
        
        # Reasoning for each class
        reasoning = {}
        for class_name in cls.FEATURE_WEIGHTS.keys():
            reasoning[class_name] = cls._generate_reasoning(
                class_name,
                features,
                metrics,
                probabilities[class_name]
            )
        
        # Confidence
        sorted_probs = sorted(probabilities.values(), reverse=True)
        confidence = sorted_probs[0] - sorted_probs[1] if len(sorted_probs) > 1 else sorted_probs[0]
        
        return {
            'probabilities': probabilities,
            'top_class': top_class[0],
            'top_probability': top_class[1],
            'confidence': confidence,
            'features': features,
            'reasoning': reasoning
        }
    
    @classmethod
    def _generate_reasoning(
        cls,
        class_name: str,
        features: Dict[str, float],
        metrics: Dict[str, Any],
        probability: float
    ) -> List[str]:
        """Generate explanations for classification."""
        reasoning = []
        weights = cls.FEATURE_WEIGHTS.get(class_name, {})
        
        # Top 3 features
        sorted_features = sorted(
            weights.items(),
            key=lambda x: abs(x[1]),
            reverse=True
        )[:3]
        
        for feature_name, weight in sorted_features:
            feature_value = features.get(feature_name, 0)
            raw_value = metrics.get(feature_name, 0)
            
            if abs(weight) < 0.05:
                continue
            
            # Check if Phase 1 feature
            is_new_feature = feature_name in [
                'unique_tokens_held', 'token_diversity_score', 'stablecoin_ratio',
                'token_concentration_ratio', 'dex_swap_count', 'dex_protocols_used',
                'dex_volume_usd', 'dex_trading_ratio', 'tx_timing_precision_score',
                'gas_price_optimization_score', 'automated_pattern_score',
                'portfolio_complexity', 'bot_likelihood_score'
            ]
            marker = "🆕" if is_new_feature else ""
            
            if weight > 0:
                if feature_value > 0.6:
                    reasoning.append(
                        f"✓{marker} High {feature_name}: {raw_value:.2f} (normalized: {feature_value:.2f})"
                    )
                elif feature_value < 0.3:
                    reasoning.append(
                        f"✗{marker} Low {feature_name}: {raw_value:.2f} (normalized: {feature_value:.2f})"
                    )
            else:
                if feature_value < 0.4:
                    reasoning.append(
                        f"✓{marker} Low {feature_name}: {raw_value:.2f} (good for {class_name})"
                    )
                elif feature_value > 0.7:
                    reasoning.append(
                        f"✗{marker} High {feature_name}: {raw_value:.2f} (bad for {class_name})"
                    )
        
        if not reasoning:
            reasoning.append(f"Probability: {probability:.2%}")
        
        return reasoning
    
    @classmethod
    def _softmax(cls, scores: Dict[str, float]) -> Dict[str, float]:
        """Convert scores to probability distribution."""
        # Exponential
        exp_scores = {k: math.exp(v * 5) for k, v in scores.items()}
        
        # Sum
        total = sum(exp_scores.values())
        
        if total == 0:
            return {k: 1.0 / len(scores) for k in scores.keys()}
        
        # Normalize
        probabilities = {k: v / total for k, v in exp_scores.items()}
        
        return probabilities


class FeatureImportanceAnalyzer:
    """Analyze feature importance."""
    
    @staticmethod
    def analyze_feature_contribution(
        metrics: Dict[str, Any],
        class_name: str
    ) -> List[Tuple[str, float, float]]:
        """Analyze contribution of each feature."""
        features = AdaptiveClassifier.extract_features(metrics)
        weights = AdaptiveClassifier.FEATURE_WEIGHTS.get(class_name, {})
        
        contributions = []
        for feature_name, weight in weights.items():
            feature_value = features.get(feature_name, 0.5)
            
            if weight < 0:
                feature_value = 1.0 - feature_value
                weight = abs(weight)
            
            contribution = feature_value * weight
            contributions.append((feature_name, contribution, weight))
        
        contributions.sort(key=lambda x: x[1], reverse=True)
        
        return contributions
    
    @staticmethod
    def get_top_features(
        metrics: Dict[str, Any],
        class_name: str,
        n: int = 5
    ) -> List[Dict[str, Any]]:
        """Get top N features for a class."""
        contributions = FeatureImportanceAnalyzer.analyze_feature_contribution(
            metrics,
            class_name
        )
        
        top_features = []
        for feature_name, contribution, weight in contributions[:n]:
            raw_value = metrics.get(feature_name, 0)
            normalized_value = AdaptiveClassifier.extract_features(metrics).get(feature_name, 0)
            
            is_new = feature_name in [
                'unique_tokens_held', 'token_diversity_score', 'stablecoin_ratio',
                'token_concentration_ratio', 'dex_swap_count', 'dex_protocols_used',
                'dex_volume_usd', 'dex_trading_ratio', 'tx_timing_precision_score',
                'gas_price_optimization_score', 'automated_pattern_score',
                'portfolio_complexity', 'bot_likelihood_score'
            ]
            
            top_features.append({
                'feature': feature_name,
                'raw_value': raw_value,
                'normalized_value': normalized_value,
                'weight': weight,
                'contribution': contribution,
                'is_phase1_feature': is_new
            })
        
        return top_features
    
    @staticmethod
    def get_phase1_features_impact(
        metrics: Dict[str, Any],
        class_name: str
    ) -> Dict[str, Any]:
        """
        Analyze impact of Phase 1 features specifically.
        
        Returns:
            {
                'total_impact': float,
                'feature_breakdown': {...},
                'top_phase1_feature': str
            }
        """
        phase1_features = [
            'unique_tokens_held', 'token_diversity_score', 'stablecoin_ratio',
            'token_concentration_ratio', 'dex_swap_count', 'dex_protocols_used',
            'dex_volume_usd', 'dex_trading_ratio', 'tx_timing_precision_score',
            'gas_price_optimization_score', 'automated_pattern_score',
            'portfolio_complexity', 'bot_likelihood_score'
        ]
        
        all_contributions = FeatureImportanceAnalyzer.analyze_feature_contribution(
            metrics, class_name
        )
        
        phase1_contributions = [
            (name, contrib, weight) 
            for name, contrib, weight in all_contributions 
            if name in phase1_features
        ]
        
        total_phase1_impact = sum(c[1] for c in phase1_contributions)
        total_overall_impact = sum(c[1] for c in all_contributions)
        
        impact_percentage = (total_phase1_impact / total_overall_impact * 100) if total_overall_impact > 0 else 0
        
        return {
            'total_impact': total_phase1_impact,
            'impact_percentage': impact_percentage,
            'feature_breakdown': {
                name: {'contribution': contrib, 'weight': weight}
                for name, contrib, weight in phase1_contributions
            },
            'top_phase1_feature': phase1_contributions[0][0] if phase1_contributions else None
        }

