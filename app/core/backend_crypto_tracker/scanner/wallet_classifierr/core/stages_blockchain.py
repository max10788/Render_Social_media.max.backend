from typing import Dict, Any, List
from datetime import datetime
from abc import ABC, abstractmethod


class Stage1_BlockchainAdapter(ABC):
    """Abstract base for blockchain-specific Stage 1 implementations."""
    
    BLOCKCHAIN_TYPE = None  # 'utxo' or 'account'
    
    @abstractmethod
    def execute(self, address_data: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute blockchain-specific Stage 1 analysis."""
        pass
    
    def _empty_metrics(self) -> Dict[str, Any]:
        """Return empty metrics for addresses with no data."""
        return {
            'tx_count': 0,
            'total_received': 0,
            'total_sent': 0,
            'current_balance': 0,
            'first_seen': 0,
            'last_seen': 0,
            'age_days': 0,
            'avg_inputs_per_tx': 0,
            'avg_outputs_per_tx': 0,
            'inputs_per_tx': {},
            'outputs_per_tx': {},
            'input_values': [],
            'output_values': [],
            'timestamps': []
        }


class Stage1_UTXO(Stage1_BlockchainAdapter):
    """Stage 1 for UTXO-based blockchains (Bitcoin, Litecoin, etc.)."""
    
    BLOCKCHAIN_TYPE = 'utxo'
    
    def execute(self, address_data: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Extract raw metrics from UTXO blockchain data."""
        txs = address_data.get('txs', [])
        inputs = address_data.get('inputs', [])
        outputs = address_data.get('outputs', [])
        
        if not txs:
            return self._empty_metrics()
        
        tx_count = len(txs)
        total_received = sum(inp.get('value', 0) for inp in inputs)
        total_sent = sum(out.get('value', 0) for out in outputs)
        current_balance = address_data.get('balance', 0)
        
        timestamps = [tx.get('timestamp', 0) for tx in txs if tx.get('timestamp')]
        first_seen = min(timestamps) if timestamps else 0
        last_seen = max(timestamps) if timestamps else 0
        current_time = int(datetime.now().timestamp())
        
        # Input/Output analysis
        inputs_per_tx = {}
        outputs_per_tx = {}
        for tx in txs:
            tx_hash = tx.get('hash')
            tx_inputs = [inp for inp in inputs if inp.get('tx_hash') == tx_hash]
            tx_outputs = [out for out in outputs if out.get('tx_hash') == tx_hash]
            inputs_per_tx[tx_hash] = len(tx_inputs)
            outputs_per_tx[tx_hash] = len(tx_outputs)
        
        avg_inputs = sum(inputs_per_tx.values()) / len(inputs_per_tx) if inputs_per_tx else 0
        avg_outputs = sum(outputs_per_tx.values()) / len(outputs_per_tx) if outputs_per_tx else 0
        
        return {
            'tx_count': tx_count,
            'total_received': total_received,
            'total_sent': total_sent,
            'current_balance': current_balance,
            'first_seen': first_seen,
            'last_seen': last_seen,
            'age_days': (current_time - first_seen) / 86400 if first_seen else 0,
            'avg_inputs_per_tx': avg_inputs,
            'avg_outputs_per_tx': avg_outputs,
            'inputs_per_tx': inputs_per_tx,
            'outputs_per_tx': outputs_per_tx,
            'input_values': [inp.get('value', 0) for inp in inputs],
            'output_values': [out.get('value', 0) for out in outputs],
            'timestamps': timestamps,
            'blockchain_type': 'utxo'
        }


class Stage1_AccountBased(Stage1_BlockchainAdapter):
    """Stage 1 for Account-based blockchains (Ethereum, Solana, etc.)."""
    
    BLOCKCHAIN_TYPE = 'account'
    
    def execute(self, address_data: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Extract raw metrics from Account-based blockchain data."""
        txs = address_data.get('txs', [])
        
        if not txs:
            return self._empty_metrics()
        
        current_time = int(datetime.now().timestamp())
        
        # Transaction analysis
        tx_count = len(txs)
        total_received = 0
        total_sent = 0
        
        incoming_txs = []  # Txs where address is 'to'
        outgoing_txs = []  # Txs where address is 'from'
        
        address_lower = address_data.get('address', '').lower()
        
        for tx in txs:
            tx_to = tx.get('to_address', tx.get('to', '')).lower()
            tx_from = tx.get('from_address', tx.get('from', '')).lower()
            tx_value = tx.get('value', 0)
            
            if tx_to == address_lower:
                incoming_txs.append(tx)
                total_received += tx_value
            
            if tx_from == address_lower:
                outgoing_txs.append(tx)
                total_sent += tx_value
        
        # ✅ FIX: Temporal metrics - Konvertiere datetime zu timestamp
        timestamps = []
        for tx in txs:
            ts = tx.get('timestamp')
            if ts:
                # Konvertiere datetime zu Unix timestamp wenn nötig
                if isinstance(ts, datetime):
                    timestamps.append(int(ts.timestamp()))
                elif isinstance(ts, (int, float)):
                    timestamps.append(int(ts))
        
        first_seen = min(timestamps) if timestamps else 0
        last_seen = max(timestamps) if timestamps else 0
        
        # Current balance from address_data or estimate
        current_balance = address_data.get('balance', total_received - total_sent)
        
        # Value statistics
        incoming_values = [tx.get('value', 0) for tx in incoming_txs]
        outgoing_values = [tx.get('value', 0) for tx in outgoing_txs]
        
        avg_incoming_value = sum(incoming_values) / len(incoming_values) if incoming_values else 0
        avg_outgoing_value = sum(outgoing_values) / len(outgoing_values) if outgoing_values else 0
        
        # ✅ FIX: Erstelle inputs_per_tx und outputs_per_tx für Account-Based
        inputs_per_tx = {}
        outputs_per_tx = {}
        
        for tx in txs:
            tx_hash = tx.get('hash', tx.get('tx_hash', tx.get('signature', '')))
            
            # Für Account-Based: Input = 1 (der Sender), Output = 1+ (Empfänger + optional Contract)
            tx_to = tx.get('to_address', tx.get('to', '')).lower()
            tx_from = tx.get('from_address', tx.get('from', '')).lower()
            
            # Input: 1 wenn diese Adresse der Sender ist
            if tx_from == address_lower:
                inputs_per_tx[tx_hash] = 1
            else:
                inputs_per_tx[tx_hash] = 0
            
            # Output: 1 wenn diese Adresse der Empfänger ist
            if tx_to == address_lower:
                outputs_per_tx[tx_hash] = 1
            else:
                outputs_per_tx[tx_hash] = 0
        
        # Fallback: Stelle sicher dass jede TX mindestens 1 Output hat
        for tx_hash in inputs_per_tx:
            if outputs_per_tx.get(tx_hash, 0) == 0:
                outputs_per_tx[tx_hash] = 1
        
        avg_inputs = sum(inputs_per_tx.values()) / len(inputs_per_tx) if inputs_per_tx else 0
        avg_outputs = sum(outputs_per_tx.values()) / len(outputs_per_tx) if outputs_per_tx else 0
        
        return {
            'tx_count': tx_count,
            'total_received': total_received,
            'total_sent': total_sent,
            'current_balance': current_balance,
            'first_seen': first_seen,
            'last_seen': last_seen,
            'age_days': (current_time - first_seen) / 86400 if first_seen else 0,
            'incoming_tx_count': len(incoming_txs),
            'outgoing_tx_count': len(outgoing_txs),
            'avg_incoming_value': avg_incoming_value,
            'avg_outgoing_value': avg_outgoing_value,
            'avg_inputs_per_tx': avg_inputs,
            'avg_outputs_per_tx': avg_outputs,
            'incoming_values': incoming_values,
            'outgoing_values': outgoing_values,
            'input_values': incoming_values,
            'output_values': outgoing_values,
            'inputs_per_tx': inputs_per_tx,
            'outputs_per_tx': outputs_per_tx,
            'timestamps': timestamps,
            'blockchain_type': 'account'
        }

class Stage1_RawMetrics:
    """Adapter that selects the right Stage 1 implementation."""
    
    @staticmethod
    def execute(
        address_data: Dict[str, Any],
        config: Dict[str, Any] = None,
        blockchain: str = 'ethereum'
    ) -> Dict[str, Any]:
        """
        Execute Stage 1 with blockchain-specific logic.
        
        Args:
            address_data: Raw blockchain data
            config: Optional configuration
            blockchain: Blockchain type ('ethereum', 'bitcoin', 'solana', etc.)
            
        Returns:
            Stage 1 metrics
        """
        # Determine blockchain type
        utxo_blockchains = ['bitcoin', 'btc', 'litecoin', 'ltc', 'dogecoin', 'doge']
        account_blockchains = ['ethereum', 'eth', 'solana', 'sol', 'sui', 'polygon', 'avalanche']
        
        blockchain_lower = blockchain.lower()
        
        if blockchain_lower in utxo_blockchains:
            executor = Stage1_UTXO()
        elif blockchain_lower in account_blockchains:
            executor = Stage1_AccountBased()
        else:
            # Default to account-based for unknown blockchains
            executor = Stage1_AccountBased()
        
        result = executor.execute(address_data, config)
        result['blockchain'] = blockchain
        return result
