# ============================================================================
# core/adaptive_classifier.py
# ============================================================================
"""
Adaptive Feature-Based Wallet Classifier
Verwendet Merkmale statt hart codierter Regeln für Klassifizierung
"""

from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


class AdaptiveClassifier:
    """
    Adaptive Klassifizierung basierend auf Feature-Vektoren.
    Berechnet Wahrscheinlichkeiten für jede Wallet-Klasse.
    """
    
    # Feature-Gewichtungen pro Klasse [0-1]
    FEATURE_WEIGHTS = {
        "Dust Sweeper": {
            "avg_inputs_per_tx": 0.15,
            "consolidation_rate": 0.15,
            "fan_in_score": 0.12,
            "micro_tx_ratio": 0.12,
            "single_output_ratio": 0.10,
            "in_degree": 0.10,
            "avg_input_value_usd": -0.08,  # Negativ: niedrig ist gut
            "timing_entropy": -0.08,
            "avg_output_value_usd": -0.10
        },
        "Hodler": {
            "holding_period_days": 0.18,
            "balance_retention_ratio": 0.15,
            "dormancy_ratio": 0.15,
            "accumulation_pattern": 0.12,
            "balance_utilization": 0.10,
            "outgoing_tx_ratio": -0.10,
            "tx_per_month": -0.10,
            "weekend_trading_ratio": -0.05,
            "exchange_interaction_count": -0.05
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
            "round_amounts_ratio": 0.05
        },
        "Trader": {
            "tx_per_month": 0.12,
            "trading_regularity": 0.10,
            "activity_burst_ratio": 0.08,
            "business_hours_ratio": 0.08,
            "weekday_ratio": 0.08,
            "balance_volatility": 0.10,
            "turnover_rate": 0.10,
            "counterparty_diversity": 0.08,
            "smart_contract_ratio": 0.08,
            "exchange_interaction_count": 0.10,
            "dormancy_ratio": -0.08
        },
        "Whale": {
            "total_value_usd": 0.25,
            "large_tx_ratio": 0.15,
            "portfolio_concentration": 0.12,
            "age_days": 0.10,
            "holding_period_days": 0.10,
            "net_inflow_usd": 0.08,
            "eigenvector_centrality": 0.08,
            "institutional_wallet": 0.07,
            "tx_per_month": -0.05
        }
    }
    
    # Normalisierungsparameter [min, max] für Features
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
        "institutional_wallet": [0, 1]
    }
    
    @classmethod
    def normalize_feature(cls, feature_name: str, value: float) -> float:
        """Normalisiert einen Feature-Wert auf [0, 1]."""
        if feature_name not in cls.FEATURE_NORMALIZATION:
            return 0.5  # Default für unbekannte Features
        
        min_val, max_val = cls.FEATURE_NORMALIZATION[feature_name]
        
        if max_val == min_val:
            return 0.5
        
        normalized = (value - min_val) / (max_val - min_val)
        return max(0.0, min(1.0, normalized))
    
    @classmethod
    def extract_features(cls, metrics: Dict[str, Any]) -> Dict[str, float]:
        """
        Extrahiert und normalisiert Features aus den Metriken.
        
        Returns:
            Dict mit normalisierten Feature-Werten [0, 1]
        """
        features = {}
        
        # Alle möglichen Features durchgehen
        all_features = set()
        for class_features in cls.FEATURE_WEIGHTS.values():
            all_features.update(class_features.keys())
        
        for feature_name in all_features:
            raw_value = metrics.get(feature_name, 0)
            
            # Boolean zu Float konvertieren
            if isinstance(raw_value, bool):
                raw_value = 1.0 if raw_value else 0.0
            
            # Normalisieren
            normalized_value = cls.normalize_feature(feature_name, raw_value)
            features[feature_name] = normalized_value
        
        return features
    
    @classmethod
    def compute_class_score(
        cls,
        class_name: str,
        features: Dict[str, float]
    ) -> float:
        """
        Berechnet den Score für eine spezifische Klasse.
        
        Args:
            class_name: Name der Wallet-Klasse
            features: Normalisierte Features [0, 1]
            
        Returns:
            Weighted score [0, 1]
        """
        if class_name not in cls.FEATURE_WEIGHTS:
            return 0.0
        
        weights = cls.FEATURE_WEIGHTS[class_name]
        total_weight = sum(abs(w) for w in weights.values())
        
        if total_weight == 0:
            return 0.0
        
        score = 0.0
        for feature_name, weight in weights.items():
            feature_value = features.get(feature_name, 0.5)
            
            # Bei negativen Gewichten Feature invertieren
            if weight < 0:
                feature_value = 1.0 - feature_value
                weight = abs(weight)
            
            score += feature_value * weight
        
        # Normalisieren auf [0, 1]
        return score / total_weight
    
    @classmethod
    def classify(cls, metrics: Dict[str, Any]) -> Dict[str, float]:
        """
        Klassifiziert eine Wallet und gibt Wahrscheinlichkeiten zurück.
        
        Args:
            metrics: Alle Metriken aus den 3 Stages
            
        Returns:
            Dict mit Wahrscheinlichkeiten pro Klasse
        """
        # Features extrahieren
        features = cls.extract_features(metrics)
        
        # Scores für alle Klassen berechnen
        raw_scores = {}
        for class_name in cls.FEATURE_WEIGHTS.keys():
            raw_scores[class_name] = cls.compute_class_score(class_name, features)
        
        # Softmax zur Wahrscheinlichkeitsverteilung
        probabilities = cls._softmax(raw_scores)
        
        return probabilities
    
    @classmethod
    def classify_with_explanation(
        cls,
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Klassifizierung mit detaillierter Erklärung.
        
        Returns:
            {
                'probabilities': Dict[str, float],
                'top_class': str,
                'confidence': float,
                'features': Dict[str, float],
                'reasoning': Dict[str, List[str]]
            }
        """
        # Features extrahieren
        features = cls.extract_features(metrics)
        
        # Klassifizieren
        probabilities = cls.classify(metrics)
        
        # Top-Klasse bestimmen
        top_class = max(probabilities.items(), key=lambda x: x[1])
        
        # Reasoning für jede Klasse
        reasoning = {}
        for class_name in cls.FEATURE_WEIGHTS.keys():
            reasoning[class_name] = cls._generate_reasoning(
                class_name,
                features,
                metrics,
                probabilities[class_name]
            )
        
        # Confidence berechnen (Abstand zwischen Top 2)
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
        """Generiert Erklärungen für eine Klassifizierung."""
        reasoning = []
        weights = cls.FEATURE_WEIGHTS.get(class_name, {})
        
        # Top 3 Features für diese Klasse
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
            
            if weight > 0:
                if feature_value > 0.6:
                    reasoning.append(
                        f"✓ High {feature_name}: {raw_value:.2f} (normalized: {feature_value:.2f})"
                    )
                elif feature_value < 0.3:
                    reasoning.append(
                        f"✗ Low {feature_name}: {raw_value:.2f} (normalized: {feature_value:.2f})"
                    )
            else:  # Negative weight
                if feature_value < 0.4:
                    reasoning.append(
                        f"✓ Low {feature_name}: {raw_value:.2f} (good for {class_name})"
                    )
                elif feature_value > 0.7:
                    reasoning.append(
                        f"✗ High {feature_name}: {raw_value:.2f} (bad for {class_name})"
                    )
        
        if not reasoning:
            reasoning.append(f"Probability: {probability:.2%}")
        
        return reasoning
    
    @classmethod
    def _softmax(cls, scores: Dict[str, float]) -> Dict[str, float]:
        """
        Konvertiert Scores zu Wahrscheinlichkeitsverteilung.
        
        Args:
            scores: Raw scores pro Klasse
            
        Returns:
            Normalisierte Wahrscheinlichkeiten (Summe = 1.0)
        """
        import math
        
        # Exponential der Scores
        exp_scores = {k: math.exp(v * 5) for k, v in scores.items()}  # *5 für stärkere Unterschiede
        
        # Summe
        total = sum(exp_scores.values())
        
        if total == 0:
            # Gleichverteilung bei Fehler
            return {k: 1.0 / len(scores) for k in scores.keys()}
        
        # Normalisieren
        probabilities = {k: v / total for k, v in exp_scores.items()}
        
        return probabilities


class FeatureImportanceAnalyzer:
    """Analysiert Feature-Wichtigkeit für Debugging."""
    
    @staticmethod
    def analyze_feature_contribution(
        metrics: Dict[str, Any],
        class_name: str
    ) -> List[Tuple[str, float, float]]:
        """
        Analysiert Beitrag jedes Features zum Score.
        
        Returns:
            List[(feature_name, contribution, weight)]
        """
        features = AdaptiveClassifier.extract_features(metrics)
        weights = AdaptiveClassifier.FEATURE_WEIGHTS.get(class_name, {})
        
        contributions = []
        for feature_name, weight in weights.items():
            feature_value = features.get(feature_name, 0.5)
            
            # Bei negativen Gewichten invertieren
            if weight < 0:
                feature_value = 1.0 - feature_value
                weight = abs(weight)
            
            contribution = feature_value * weight
            contributions.append((feature_name, contribution, weight))
        
        # Sortieren nach Beitrag
        contributions.sort(key=lambda x: x[1], reverse=True)
        
        return contributions
    
    @staticmethod
    def get_top_features(
        metrics: Dict[str, Any],
        class_name: str,
        n: int = 5
    ) -> List[Dict[str, Any]]:
        """Gibt die Top N Features für eine Klasse zurück."""
        contributions = FeatureImportanceAnalyzer.analyze_feature_contribution(
            metrics,
            class_name
        )
        
        top_features = []
        for feature_name, contribution, weight in contributions[:n]:
            raw_value = metrics.get(feature_name, 0)
            normalized_value = AdaptiveClassifier.extract_features(metrics).get(feature_name, 0)
            
            top_features.append({
                'feature': feature_name,
                'raw_value': raw_value,
                'normalized_value': normalized_value,
                'weight': weight,
                'contribution': contribution
            })
        
        return top_features
