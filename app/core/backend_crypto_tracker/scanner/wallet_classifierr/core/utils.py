# wallet_classifier/core/utils.py

from typing import Dict, List, Any, Optional
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class DataValidator:
    """Validates and normalizes blockchain data"""
    
    @staticmethod
    def validate_wallet_data(data: Dict[str, Any]) -> bool:
        """Validate required fields in wallet data"""
        required_fields = ['address', 'transactions', 'balance']
        
        for field in required_fields:
            if field not in data:
                logging.error(f"Missing required field: {field}")
                return False
        
        if not isinstance(data['transactions'], list):
            logging.error("Transactions must be a list")
            return False
        
        return True
    
    @staticmethod
    def normalize_transaction_data(transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize transaction data across different chains"""
        normalized = {
            'hash': transaction.get('hash', transaction.get('tx_hash', '')),
            'timestamp': transaction.get('timestamp', transaction.get('time', 0)),
            'value': float(transaction.get('value', transaction.get('amount', 0))),
            'from': transaction.get('from', transaction.get('sender', '')),
            'to': transaction.get('to', transaction.get('recipient', '')),
            'fee': float(transaction.get('fee', transaction.get('gas_price', 0))),
            'type': 'send' if transaction.get('from') else 'receive'
        }
        
        # Handle UTXO-based chains
        if 'inputs' in transaction:
            normalized['inputs'] = transaction['inputs']
            normalized['input_count'] = len(transaction['inputs'])
        
        if 'outputs' in transaction:
            normalized['outputs'] = transaction['outputs']
            normalized['output_count'] = len(transaction['outputs'])
        
        return normalized

class HybridResolver:
    """Resolves conflicts when wallet shows multiple class characteristics"""
    
    PRIORITY_ORDER = [
        'MIXER',  # Privacy concern takes precedence
        'WHALE',  # Large holdings are significant
        'TRADER', # Active trading behavior
        'DUST_SWEEPER',  # Consolidation patterns
        'HODLER'  # Default passive behavior
    ]
    
    @staticmethod
    def resolve_classifications(scores: List[Dict[str, Any]]) -> str:
        """
        Resolve multiple classifications based on priority and confidence
        
        Args:
            scores: List of classification results with confidence scores
            
        Returns:
            Final classification string
        """
        if not scores:
            return 'UNKNOWN'
        
        # Filter scores above threshold
        valid_scores = [s for s in scores if s['confidence'] > 0.5]
        
        if not valid_scores:
            return 'UNKNOWN'
        
        # Special case resolutions
        classes = [s['class'] for s in valid_scores]
        
        # Trader-Whale: Check balance
        if 'TRADER' in classes and 'WHALE' in classes:
            whale_score = next(s for s in valid_scores if s['class'] == 'WHALE')
            if whale_score.get('balance', 0) > 10_000_000:  # $10M threshold
                return 'WHALE'
            return 'TRADER'
        
        # Mixer always takes precedence due to privacy implications
        if 'MIXER' in classes:
            return 'MIXER'
        
        # Return highest confidence otherwise
        return max(valid_scores, key=lambda x: x['confidence'])['class']
