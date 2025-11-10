# ============================================================================
# analyzer.py - ENHANCED WITH FEATURE DISPLAY
# ============================================================================
"""Main interface for wallet classification with feature explanations."""

from typing import Dict, Any, List, Optional
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.classes import (
    DustSweeperAnalyzer,
    HodlerAnalyzer,
    MixerAnalyzer,
    TraderAnalyzer,
    WhaleAnalyzer
)
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.data_sources.ground_truth import GroundTruthDB
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.stages_blockchain import Stage1_RawMetrics
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.stages import Stage2_DerivedMetrics, Stage3_ContextAnalysis
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.adaptive_classifier import (
    AdaptiveClassifier,
    FeatureImportanceAnalyzer
)
from app.core.price_movers.services.lightweight_entity_identifier import (
    LightweightEntityIdentifier,
    TradingEntity
)


class WalletClassifier:
    """
    Enhanced wallet classification system with feature-based explanations.
    """
    
    def __init__(self, context_db: Optional[GroundTruthDB] = None):
        """
        Initialize classifier with all analyzers.
        
        Args:
            context_db: Optional database for context information
        """
        self.context_db = context_db or GroundTruthDB()
        
        self.analyzers = {
            'Dust Sweeper': DustSweeperAnalyzer(),
            'Hodler': HodlerAnalyzer(),
            'Mixer': MixerAnalyzer(),
            'Trader': TraderAnalyzer(),
            'Whale': WhaleAnalyzer()
        }
    
    def classify(
        self,
        address: str,
        blockchain_data: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None,
        blockchain: str = 'ethereum',
        include_features: bool = True
    ) -> Dict[str, Any]:
        """
        Classify a wallet with detailed feature explanations.
        
        Args:
            address: The wallet address to classify
            blockchain_data: Raw blockchain transaction data
            config: Optional configuration
            blockchain: Blockchain name
            include_features: Include detailed feature analysis
            
        Returns:
            {
                'address': str,
                'blockchain': str,
                'primary_class': str,
                'primary_probability': float,
                'confidence': float,
                'all_probabilities': {...},
                'features': {...},  # if include_features=True
                'key_indicators': {...},  # if include_features=True
                'reasoning': {...},  # if include_features=True
                'classification_details': {...}
            }
        """
        # Add address to blockchain_data
        blockchain_data['address'] = address
        
        # Stage 1: Raw metrics (blockchain-specific)
        raw_metrics = Stage1_RawMetrics.execute(blockchain_data, config, blockchain)
        
        # Stage 2: Derived metrics
        derived_metrics = Stage2_DerivedMetrics().execute(raw_metrics, config)
        
        # Stage 3: Context
        context_metrics = Stage3_ContextAnalysis().execute(derived_metrics, address, self.context_db)
        
        # Combine all metrics
        all_metrics = {**raw_metrics, **derived_metrics, **context_metrics}
        
        # Get adaptive classification with explanation
        classification_result = AdaptiveClassifier.classify_with_explanation(all_metrics)
        
        # Build result
        result = {
            'address': address,
            'blockchain': blockchain,
            'primary_class': classification_result['top_class'],
            'primary_probability': classification_result['top_probability'],
            'confidence': classification_result['confidence'],
            'all_probabilities': classification_result['probabilities'],
            'classification_details': {}
        }
        
        # Add detailed analysis per class
        for class_name, analyzer in self.analyzers.items():
            probability = classification_result['probabilities'][class_name]
            is_class = probability >= analyzer.THRESHOLD
            
            class_details = {
                'probability': probability,
                'is_class': is_class,
                'threshold': analyzer.THRESHOLD
            }
            
            # Add key indicators if available
            if hasattr(analyzer, 'get_key_indicators'):
                class_details['key_indicators'] = analyzer.get_key_indicators(all_metrics)
            
            result['classification_details'][class_name] = class_details
        
        # Add feature analysis if requested
        if include_features:
            result['features'] = classification_result['features']
            result['reasoning'] = classification_result['reasoning']
            
            # Add top features for primary class
            result['top_features'] = FeatureImportanceAnalyzer.get_top_features(
                all_metrics,
                result['primary_class'],
                n=5
            )
        
        # Apply hybrid logic
        result = self._apply_hybrid_rules(result, all_metrics)
        
        # Add risk flags
        result['risk_flags'] = self._generate_risk_flags(result)
        
        return result
    
    def classify_simple(
        self,
        address: str,
        blockchain_data: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None,
        blockchain: str = 'ethereum'
    ) -> Dict[str, Any]:
        """
        Simplified classification without feature details (backwards compatible).
        
        Returns:
            {
                'address': str,
                'primary_class': str,
                'probability': float,
                'all_classes': {...}
            }
        """
        full_result = self.classify(
            address,
            blockchain_data,
            config,
            blockchain,
            include_features=False
        )
        
        return {
            'address': address,
            'blockchain': blockchain,
            'primary_class': full_result['primary_class'],
            'probability': full_result['primary_probability'],
            'confidence': full_result['confidence'],
            'all_classes': {
                name: {
                    'probability': details['probability'],
                    'is_class': details['is_class']
                }
                for name, details in full_result['classification_details'].items()
            }
        }
    
    def _apply_hybrid_rules(
        self,
        result: Dict[str, Any],
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply hybrid classification rules for overlapping classes."""
        
        details = result['classification_details']
        
        # Rule 1: Whale + Trader → Whale dominates if value > $10M
        if details['Whale']['is_class'] and details['Trader']['is_class']:
            total_value = metrics.get('total_value_usd', 0)
            if total_value > 10_000_000:
                details['Trader']['is_class'] = False
                result['hybrid_note'] = "Whale dominates over Trader (high value)"
        
        # Rule 2: Hodler + Trader → Mutual exclusive based on activity
        if details['Hodler']['is_class'] and details['Trader']['is_class']:
            if details['Trader']['probability'] > details['Hodler']['probability']:
                details['Hodler']['is_class'] = False
                result['hybrid_note'] = "Trader dominates over Hodler (high activity)"
            else:
                details['Trader']['is_class'] = False
                result['hybrid_note'] = "Hodler dominates over Trader (low activity)"
        
        return result
    
    def _generate_risk_flags(self, result: Dict[str, Any]) -> List[str]:
        """Generate risk flags based on classification."""
        flags = []
        
        details = result['classification_details']
        
        # Mixer detection
        if details['Mixer']['probability'] > 0.6:
            flags.append("HIGH RISK: Mixer activity detected")
        elif details['Mixer']['probability'] > 0.4:
            flags.append("MEDIUM RISK: Possible mixer-like behavior")
        
        # Dust sweeper as service
        if details['Dust Sweeper']['is_class']:
            flags.append("INFO: Dust consolidation service detected")
        
        # Whale movement
        if details['Whale']['is_class']:
            flags.append("INFO: Large capital holder (Whale)")
        
        return flags
    
    def classify_batch(
        self,
        addresses: List[str],
        blockchain_data_dict: Dict[str, Dict[str, Any]],
        config: Optional[Dict[str, Any]] = None,
        blockchain: str = 'ethereum',
        include_features: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """Classify multiple addresses in batch."""
        return {
            address: self.classify(
                address,
                blockchain_data_dict.get(address, {}),
                config,
                blockchain,
                include_features
            )
            for address in addresses
        }


def classify_wallet(
    address: str,
    blockchain_data: Dict[str, Any],
    context_db: Optional[GroundTruthDB] = None,
    config: Optional[Dict[str, Any]] = None,
    blockchain: str = 'ethereum',
    detailed: bool = False
) -> Dict[str, Any]:
    """
    Convenience function to classify a single wallet.
    
    Args:
        address: Wallet address
        blockchain_data: Transaction data
        context_db: Optional context database
        config: Optional configuration
        blockchain: Blockchain name
        detailed: If True, include feature analysis
        
    Returns:
        Classification result with probabilities and features
    """
    classifier = WalletClassifier(context_db)
    
    if detailed:
        return classifier.classify(address, blockchain_data, config, blockchain, include_features=True)
    else:
        return classifier.classify_simple(address, blockchain_data, config, blockchain)


def get_wallet_summary(classification_result: Dict[str, Any]) -> str:
    """
    Generate a human-readable summary of classification results.
    
    Args:
        classification_result: Result from classify() or classify_simple()
        
    Returns:
        Human-readable summary string
    """
    primary = classification_result['primary_class']
    prob = classification_result['primary_probability']
    confidence = classification_result.get('confidence', 0)
    
    summary = f"Classification: {primary} ({prob:.1%} probability)\n"
    summary += f"Confidence: {confidence:.1%}\n"
    
    if 'top_features' in classification_result:
        summary += "\nTop Features:\n"
        for feature in classification_result['top_features'][:3]:
            summary += f"  • {feature['feature']}: {feature['raw_value']:.2f}\n"
    
    if 'risk_flags' in classification_result and classification_result['risk_flags']:
        summary += "\nRisk Flags:\n"
        for flag in classification_result['risk_flags']:
            summary += f"  ⚠ {flag}\n"
    
    return summary
