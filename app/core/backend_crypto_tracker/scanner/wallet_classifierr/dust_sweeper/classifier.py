# wallet_classifier/dust_sweeper/classifier.py

from typing import Dict, List, Any
import numpy as np
from ..core.base_classifier import BaseClassifier, WalletClass, WalletData
from ..core.metrics import MetricCalculator

class DustSweeperClassifier(BaseClassifier):
    """Classifier for identifying dust sweeper wallets"""
    
    def get_thresholds(self) -> Dict[str, float]:
        return {
            'basic': 0.60,
            'intermediate': 0.65,
            'advanced': 0.70
        }
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'primary': 0.70,
            'secondary': 0.30,
            'context': 0.20
        }
    
    def get_wallet_class(self) -> WalletClass:
        return WalletClass.DUST_SWEEPER
    
    def calculate_stage1_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """
        Stage 1 - Primary Metrics (5):
        1. Input count per transaction
        2. Average input value
        3. Transaction consolidation rate
        4. Small value transaction ratio
        5. Output reduction ratio
        """
        metrics = {}
        transactions = wallet_data.transactions
        
        # 1. Input count per transaction (5-50 expected)
        input_counts = []
        for tx in transactions:
            input_count = tx.get('input_count', len(tx.get('inputs', [])))
            if input_count > 0:
                input_counts.append(input_count)
        
        avg_input_count = np.mean(input_counts) if input_counts else 0
        # Normalize: optimal range 5-50 inputs
        metrics['primary_input_count'] = min(1.0, max(0, (avg_input_count - 1) / 49))
        
        # 2. Average input value (<100 USD expected)
        input_values = []
        for tx in transactions:
            inputs = tx.get('inputs', [])
            for inp in inputs:
                value = inp.get('value', 0)
                if value > 0:
                    input_values.append(value)
        
        avg_input_value = np.mean(input_values) if input_values else 0
        # Score higher for smaller values (dust)
        metrics['primary_avg_input_value'] = max(0, 1.0 - (avg_input_value / 1000))
        
        # 3. Transaction consolidation rate (0.7-0.95 expected)
        consolidation_rate = MetricCalculator.calculate_consolidation_ratio(transactions)
        metrics['primary_consolidation_rate'] = consolidation_rate
        
        # 4. Small value transaction ratio
        small_tx_count = sum(1 for tx in transactions if 0 < tx.get('value', 0) < 100)
        metrics['primary_small_tx_ratio'] = small_tx_count / len(transactions) if transactions else 0
        
        # 5. Output reduction ratio
        output_reduction_count = 0
        for tx in transactions:
            inputs = tx.get('input_count', len(tx.get('inputs', [])))
            outputs = tx.get('output_count', len(tx.get('outputs', [])))
            if inputs > outputs and inputs > 0:
                output_reduction_count += 1
        
        metrics['primary_output_reduction'] = output_reduction_count / len(transactions) if transactions else 0
        
        return metrics
    
    def calculate_stage2_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """
        Stage 2 - Secondary Metrics (3):
        1. Sweep frequency pattern
        2. Address reuse rate
        3. Time clustering coefficient
        """
        metrics = {}
        transactions = wallet_data.transactions
        
        # 1. Sweep frequency pattern
        if len(transactions) > 1:
            timestamps = sorted([tx.get('timestamp', 0) for tx in transactions])
            intervals = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
            
            # Look for regular sweeping patterns (e.g., daily, weekly)
            if intervals:
                # Check for consistency in intervals (lower variance = more regular)
                interval_std = np.std(intervals)
                interval_mean = np.mean(intervals)
                consistency = 1.0 - min(1.0, interval_std / interval_mean) if interval_mean > 0 else 0
                metrics['secondary_sweep_frequency'] = consistency
            else:
                metrics['secondary_sweep_frequency'] = 0
        else:
            metrics['secondary_sweep_frequency'] = 0
        
        # 2. Address reuse rate
        input_addresses = []
        for tx in transactions:
            inputs = tx.get('inputs', [])
            for inp in inputs:
                addr = inp.get('address', inp.get('from', ''))
                if addr:
                    input_addresses.append(addr)
        
        if input_addresses:
            unique_addresses = len(set(input_addresses))
            total_addresses = len(input_addresses)
            # High reuse rate indicates sweeping from same sources
            reuse_rate = 1.0 - (unique_addresses / total_addresses)
            metrics['secondary_address_reuse'] = reuse_rate
        else:
            metrics['secondary_address_reuse'] = 0
        
        # 3. Time clustering coefficient
        # Measure how clustered transactions are in time (bursts of activity)
        if len(transactions) > 2:
            timestamps = [tx.get('timestamp', 0) for tx in transactions]
            time_diffs = np.diff(sorted(timestamps))
            
            if len(time_diffs) > 0:
                # Calculate clustering using coefficient of variation
                cv = np.std(time_diffs) / np.mean(time_diffs) if np.mean(time_diffs) > 0 else 0
                # High CV indicates clustering (bursts)
                metrics['secondary_time_clustering'] = min(1.0, cv / 2)
            else:
                metrics['secondary_time_clustering'] = 0
        else:
            metrics['secondary_time_clustering'] = 0
        
        return metrics
    
    def calculate_stage3_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """
        Stage 3 - Context Metrics (4):
        1. Exchange interaction frequency
        2. Mining pool source detection
        3. Fee optimization behavior
        4. UTXO fragmentation level
        """
        metrics = {}
        transactions = wallet_data.transactions
        
        # 1. Exchange interaction frequency
        # Known exchange addresses patterns (simplified)
        exchange_patterns = ['exchange', 'binance', 'coinbase', 'kraken']
        exchange_interactions = 0
        
        for tx in transactions:
            # Check if interacting with known exchange patterns
            to_addr = str(tx.get('to', '')).lower()
            from_addr = str(tx.get('from', '')).lower()
            
            if any(pattern in to_addr for pattern in exchange_patterns):
                exchange_interactions += 1
            if any(pattern in from_addr for pattern in exchange_patterns):
                exchange_interactions += 1
        
        metrics['context_exchange_interaction'] = min(1.0, exchange_interactions / (len(transactions) * 2)) if transactions else 0
        
        # 2. Mining pool source detection
        mining_pool_sources = 0
        for tx in transactions:
            # Check for mining pool characteristics
            # Large number of inputs with similar small values
            inputs = tx.get('inputs', [])
            if len(inputs) > 10:
                values = [inp.get('value', 0) for inp in inputs]
                if values:
                    cv = np.std(values) / np.mean(values) if np.mean(values) > 0 else 1
                    if cv < 0.3:  # Similar values
                        mining_pool_sources += 1
        
        metrics['context_mining_pool_source'] = min(1.0, mining_pool_sources / len(transactions)) if transactions else 0
        
        # 3. Fee optimization behavior
        fees = []
        for tx in transactions:
            fee = tx.get('fee', 0)
            value = tx.get('value', 0)
            if value > 0 and fee > 0:
                fee_ratio = fee / value
                fees.append(fee_ratio)
        
        if fees:
            # Low and consistent fees indicate optimization
            avg_fee_ratio = np.mean(fees)
            fee_variance = np.std(fees)
            # Score higher for lower fees with low variance
            optimization_score = max(0, 1.0 - avg_fee_ratio * 10) * (1.0 - min(1.0, fee_variance * 10))
            metrics['context_fee_optimization'] = optimization_score
        else:
            metrics['context_fee_optimization'] = 0
        
        # 4. UTXO fragmentation level
        # High fragmentation = many small UTXOs
        if wallet_data.chain in ['bitcoin', 'litecoin']:
            utxo_count = 0
            utxo_values = []
            
            for tx in transactions:
                outputs = tx.get('outputs', [])
                for output in outputs:
                    if not output.get('spent', False):
                        utxo_count += 1
                        utxo_values.append(output.get('value', 0))
            
            if utxo_values:
                # Many small UTXOs indicate high fragmentation
                small_utxos = sum(1 for v in utxo_values if v < 100)
                fragmentation = small_utxos / len(utxo_values)
                metrics['context_utxo_fragmentation'] = fragmentation
            else:
                metrics['context_utxo_fragmentation'] = 0
        else:
            # For account-based chains, use transaction fragmentation
            small_txs = sum(1 for tx in transactions if 0 < tx.get('value', 0) < 100)
            metrics['context_utxo_fragmentation'] = small_txs / len(transactions) if transactions else 0
        
        return metrics
