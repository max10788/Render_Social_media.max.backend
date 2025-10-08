# ============================================================================
# analyzer.py
# ============================================================================
"""Main interface for wallet classification."""

from typing import Dict, Any, List, Optional
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.classes import (
    DustSweeperAnalyzer,
    HodlerAnalyzer,
    MixerAnalyzer,
    TraderAnalyzer,
    WhaleAnalyzer
)
from data_sources import GroundTruthDB


class WalletClassifier:
    """
    Main wallet classification system.
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
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Classify a wallet address.
        
        Args:
            address: The wallet address to classify
            blockchain_data: Raw blockchain transaction data
            config: Optional configuration (e.g., BTC price, thresholds)
            
        Returns:
            Dictionary with classification results for each class
        """
        results = {}
        
        # Run all analyzers
        for class_name, analyzer in self.analyzers.items():
            score = analyzer.analyze(address, blockchain_data, self.context_db, config)
            is_member = analyzer.is_class(score)
            
            results[class_name] = {
                'score': score,
                'is_class': is_member,
                'threshold': analyzer.THRESHOLD
            }
        
        # Apply hybrid logic
        results = self._apply_hybrid_rules(results, blockchain_data)
        
        # Determine primary class
        results['primary_class'] = self._determine_primary_class(results)
        
        return results
    
    def _apply_hybrid_rules(
        self,
        results: Dict[str, Any],
        blockchain_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Apply hybrid classification rules for overlapping classes.
        
        Args:
            results: Initial classification results
            blockchain_data: Raw blockchain data
            
        Returns:
            Updated results with hybrid rules applied
        """
        # Rule 1: Whale + Trader → Whale dominates if value > $10M
        if results['Whale']['is_class'] and results['Trader']['is_class']:
            total_value = blockchain_data.get('balance', 0) * 50000  # Estimate USD
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
        """
        Determine the primary class from classification results.
        
        Args:
            results: Classification results
            
        Returns:
            Primary class name
        """
        # Find all classes where is_class is True
        active_classes = [
            (name, data['score'])
            for name, data in results.items()
            if isinstance(data, dict) and data.get('is_class', False)
        ]
        
        if not active_classes:
            return "Unknown"
        
        # Return class with highest score
        return max(active_classes, key=lambda x: x[1])[0]
    
    def classify_batch(
        self,
        addresses: List[str],
        blockchain_data_dict: Dict[str, Dict[str, Any]],
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Classify multiple addresses in batch.
        
        Args:
            addresses: List of addresses to classify
            blockchain_data_dict: Dictionary mapping addresses to their blockchain data
            config: Optional configuration
            
        Returns:
            Dictionary mapping addresses to classification results
        """
        return {
            address: self.classify(address, blockchain_data_dict.get(address, {}), config)
            for address in addresses
        }


def classify_wallet(
    address: str,
    blockchain_data: Dict[str, Any],
    context_db: Optional[GroundTruthDB] = None,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Convenience function to classify a single wallet.
    
    Args:
        address: Wallet address
        blockchain_data: Raw blockchain transaction data
        context_db: Optional context database
        config: Optional configuration
        
    Returns:
        Classification results
    """
    classifier = WalletClassifier(context_db)
    return classifier.classify(address, blockchain_data, config)
