# ============================================================================
# analyzer.py (UPDATED)
# ============================================================================
"""Main interface for wallet classification with blockchain support."""

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


class WalletClassifier:
    """
    Main wallet classification system with blockchain support.
    Orchestrates all analyzers and applies hybrid logic.
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
        blockchain: str = 'ethereum'
    ) -> Dict[str, Any]:
        """
        Classify a wallet address with blockchain-specific logic.
        
        Args:
            address: The wallet address to classify
            blockchain_data: Raw blockchain transaction data
            config: Optional configuration (e.g., BTC price, thresholds)
            blockchain: Blockchain name ('ethereum', 'bitcoin', 'solana', etc.)
            
        Returns:
            Dictionary with classification results for each class
        """
        # Add address to blockchain_data for Stage 1 processing
        blockchain_data['address'] = address
        
        results = {}
        
        # Stage 1: Raw metrics (blockchain-specific)
        raw_metrics = Stage1_RawMetrics.execute(blockchain_data, config, blockchain)
        
        # Stage 2: Derived metrics
        derived_metrics = Stage2_DerivedMetrics().execute(raw_metrics, config)
        
        # Stage 3: Context
        context_metrics = Stage3_ContextAnalysis().execute(derived_metrics, address, self.context_db)
        
        # Combine all metrics
        all_metrics = {**raw_metrics, **derived_metrics, **context_metrics}
        
        # Run all analyzers
        for class_name, analyzer in self.analyzers.items():
            score = analyzer.analyze(address, blockchain_data, self.context_db, config)
            is_member = analyzer.is_class(score)
            
            results[class_name] = {
                'score': score,
                'is_class': is_member,
                'threshold': analyzer.THRESHOLD,
                'metrics': all_metrics
            }
        
        # Apply hybrid logic
        results = self._apply_hybrid_rules(results, blockchain_data)
        
        # Determine primary class
        results['primary_class'] = self._determine_primary_class(results)
        
        # Add metadata
        results['blockchain'] = blockchain
        results['address'] = address
        
        return results
    
    def _apply_hybrid_rules(
        self,
        results: Dict[str, Any],
        blockchain_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply hybrid classification rules for overlapping classes."""
        
        # Rule 1: Whale + Trader → Whale dominates if value > $10M
        if results['Whale']['is_class'] and results['Trader']['is_class']:
            total_value = blockchain_data.get('balance', 0) * 50000
            if total_value > 10_000_000:
                results['Trader']['is_class'] = False
                results['hybrid_note'] = "Whale dominates over Trader (high value)"
        
        # Rule 2: Hodler + Trader → Mutual exclusive based on activity
        if results['Hodler']['is_class'] and results['Trader']['is_class']:
            if results['Trader']['score'] > results['Hodler']['score']:
                results['Hodler']['is_class'] = False
                results['hybrid_note'] = "Trader dominates over Hodler (high activity)"
            else:
                results['Trader']['is_class'] = False
                results['hybrid_note'] = "Hodler dominates over Trader (low activity)"
        
        # Rule 3: Mixer detected → Flag as high risk
        if results['Mixer']['is_class']:
            results['risk_flag'] = "HIGH - Mixer activity detected"
        
        # Rule 4: Dust Sweeper + Low Value → Likely consolidation service
        if results['Dust Sweeper']['is_class']:
            total_value = blockchain_data.get('balance', 0) * 50000
            if total_value < 1000:
                results['service_type'] = "Dust Consolidation Service"
        
        return results
    
    def _determine_primary_class(self, results: Dict[str, Any]) -> str:
        """Determine the primary class from classification results."""
        active_classes = [
            (name, data['score'])
            for name, data in results.items()
            if isinstance(data, dict) and data.get('is_class', False)
        ]
        
        if not active_classes:
            return "Unknown"
        
        return max(active_classes, key=lambda x: x[1])[0]
    
    def classify_batch(
        self,
        addresses: List[str],
        blockchain_data_dict: Dict[str, Dict[str, Any]],
        config: Optional[Dict[str, Any]] = None,
        blockchain: str = 'ethereum'
    ) -> Dict[str, Dict[str, Any]]:
        """Classify multiple addresses in batch."""
        return {
            address: self.classify(
                address,
                blockchain_data_dict.get(address, {}),
                config,
                blockchain
            )
            for address in addresses
        }


def classify_wallet(
    address: str,
    blockchain_data: Dict[str, Any],
    context_db: Optional[GroundTruthDB] = None,
    config: Optional[Dict[str, Any]] = None,
    blockchain: str = 'ethereum'
) -> Dict[str, Any]:
    """Convenience function to classify a single wallet."""
    classifier = WalletClassifier(context_db)
    return classifier.classify(address, blockchain_data, config, blockchain)
