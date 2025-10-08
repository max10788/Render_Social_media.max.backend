# wallet_classifier/dust_sweeper/stage3/features.py

from typing import Dict, List, Any, Set
import numpy as np

class Stage3Features:
    """Feature extraction for Stage 3 Dust Sweeper analysis"""
    
    # Known patterns for entity detection
    EXCHANGE_TAGS = {'exchange', 'binance', 'coinbase', 'kraken', 'huobi', 'okex'}
    MINING_TAGS = {'pool', 'mining', 'f2pool', 'antpool', 'slush'}
    MIXER_TAGS = {'mixer', 'tornado', 'wasabi', 'samourai'}
    
    @staticmethod
    def extract_entity_interactions(transactions: List[Dict]) -> Dict[str, float]:
        """Extract patterns of interaction with known entities"""
        features = {}
        
        exchange_interactions = 0
        mining_interactions = 0
        mixer_interactions = 0
        
        for tx in transactions:
            # Check transaction tags or metadata
            tags = set(str(tx.get('tag', '')).lower().split())
            label = str(tx.get('label', '')).lower()
            
            # Check counterparty addresses
            to_addr = str(tx.get('to', '')).lower()
            from_addr = str(tx.get('from', '')).lower()
            
            # Count interactions by type
            if any(tag in Stage3Features.EXCHANGE_TAGS for tag in tags) or \
               any(tag in to_addr for tag in Stage3Features.EXCHANGE_TAGS) or \
               any(tag in label for tag in Stage3Features.EXCHANGE_TAGS):
                exchange_interactions += 1
            
            if any(tag in Stage3Features.MINING_TAGS for tag in tags) or \
               any(tag in from_addr for tag in Stage3Features.MINING_TAGS):
                mining_interactions += 1
            
            if any(tag in Stage3Features.MIXER_TAGS for tag in tags):
                mixer_interactions += 1
        
        total_txs = len(transactions) if transactions else 1
        
        features['exchange_interaction_rate'] = exchange_interactions / total_txs
        features['mining_source_rate'] = mining_interactions / total_txs
        features['mixer_interaction_rate'] = mixer_interactions / total_txs
        
        return features
    
    @staticmethod
    def extract_optimization_patterns(transactions: List[Dict]) -> Dict[str, float]:
        """Extract fee and transaction optimization patterns"""
        features = {}
        
        fee_ratios = []
        confirmation_times = []
        
        for tx in transactions:
            value = tx.get('value', 0)
            fee = tx.get('fee', 0)
            
            if value > 0 and fee > 0:
                fee_ratios.append(fee / value)
            
            # Confirmation time if available
            conf_time = tx.get('confirmation_time', tx.get('conf_time', 0))
            if conf_time > 0:
                confirmation_times.append(conf_time)
        
        # Fee optimization score
        if fee_ratios:
            # Lower and more consistent fees indicate optimization
            avg_fee_ratio = np.mean(fee_ratios)
            fee_consistency = 1.0 - (np.std(fee_ratios) / np.mean(fee_ratios) if np.mean(fee_ratios) > 0 else 0)
            features['fee_optimization'] = (1.0 - min(1.0, avg_fee_ratio * 100)) * fee_consistency
        else:
            features['fee_optimization'] = 0
        
        # Transaction batching efficiency
        batch_score = Stage3Features._calculate_batching_efficiency(transactions)
        features['batching_efficiency'] = batch_score
        
        return features
    
    @staticmethod
    def _calculate_batching_efficiency(transactions: List[Dict]) -> float:
        """Calculate how efficiently transactions are batched"""
        if len(transactions) < 2:
            return 0
        
        batch_txs = 0
        for tx in transactions:
            outputs = tx.get('output_count', len(tx.get('outputs', [])))
            if outputs > 2:  # Multiple outputs indicate batching
                batch_txs += 1
        
        return batch_txs / len(transactions)
