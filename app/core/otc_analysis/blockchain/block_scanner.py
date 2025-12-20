from typing import List, Dict, Optional, Callable
import time
from datetime import datetime
from otc_analysis.blockchain.node_provider import NodeProvider
from otc_analysis.models.transaction import Transaction

class BlockScanner:
    """
    Continuous block scanner for indexing blockchain transactions.
    Part of background workers - scans new blocks and extracts transactions.
    """
    
    def __init__(self, node_provider: NodeProvider, chain_id: int = 1):
        self.node_provider = node_provider
        self.chain_id = chain_id
        self.is_running = False
        self.current_block = None
        self.scan_delay = 12  # Seconds between scans (Ethereum block time ~12s)
    
    def start_from_block(self, start_block: int):
        """Initialize scanner from specific block."""
        self.current_block = start_block
        print(f"Scanner initialized at block {start_block}")
    
    def start_from_latest(self):
        """Initialize scanner from latest block."""
        latest = self.node_provider.get_latest_block_number()
        self.current_block = latest
        print(f"Scanner initialized at latest block {latest}")
    
    def scan_range(
        self,
        from_block: int,
        to_block: int,
        callback: Optional[Callable] = None
    ) -> List[Dict]:
        """
        Scan a specific range of blocks.
        
        Args:
            from_block: Start block
            to_block: End block
            callback: Optional function to call for each transaction
        
        Returns:
            List of transactions
        """
        all_transactions = []
        
        print(f"Scanning blocks {from_block} to {to_block}...")
        
        for block_num in range(from_block, to_block + 1):
            try:
                block_data = self.node_provider.get_block(block_num)
                
                if not block_data:
                    print(f"⚠ Failed to fetch block {block_num}, skipping...")
                    continue
                
                transactions = self._extract_block_transactions(block_data)
                
                # Process each transaction
                for tx in transactions:
                    if callback:
                        callback(tx)
                    all_transactions.append(tx)
                
                if block_num % 100 == 0:
                    print(f"Processed block {block_num}, found {len(transactions)} transactions")
                
            except Exception as e:
                print(f"Error scanning block {block_num}: {e}")
                continue
        
        print(f"✓ Scan complete. Total transactions: {len(all_transactions)}")
        return all_transactions
    
    def scan_continuous(
        self,
        callback: Callable,
        error_callback: Optional[Callable] = None
    ):
        """
        Continuously scan new blocks as they appear.
        Used in background worker.
        
        Args:
            callback: Function to call for each new transaction
            error_callback: Optional function to call on errors
        """
        self.is_running = True
        
        if self.current_block is None:
            self.start_from_latest()
        
        print(f"🔄 Starting continuous scan from block {self.current_block}")
        
        while self.is_running:
            try:
                latest_block = self.node_provider.get_latest_block_number()
                
                # Check if there are new blocks to process
                if latest_block > self.current_block:
                    # Process all blocks we've missed
                    for block_num in range(self.current_block + 1, latest_block + 1):
                        block_data = self.node_provider.get_block(block_num)
                        
                        if block_data:
                            transactions = self._extract_block_transactions(block_data)
                            
                            for tx in transactions:
                                try:
                                    callback(tx)
                                except Exception as e:
                                    print(f"Callback error for tx {tx.get('hash')}: {e}")
                                    if error_callback:
                                        error_callback(e, tx)
                            
                            print(f"✓ Block {block_num}: {len(transactions)} transactions")
                        
                        self.current_block = block_num
                
                # Wait before checking for new blocks
                time.sleep(self.scan_delay)
                
            except KeyboardInterrupt:
                print("\n⚠ Scan interrupted by user")
                self.stop()
                break
            except Exception as e:
                print(f"Scanner error: {e}")
                if error_callback:
                    error_callback(e, None)
                time.sleep(self.scan_delay * 2)  # Wait longer on error
    
    def stop(self):
        """Stop continuous scanning."""
        self.is_running = False
        print(f"Scanner stopped at block {self.current_block}")
    
    def _extract_block_transactions(self, block_data: Dict) -> List[Dict]:
        """
        Extract and format transactions from block data.
        
        Returns list of transaction dicts ready for further processing.
        """
        transactions = []
        block_timestamp = datetime.fromtimestamp(block_data['timestamp'])
        
        for tx in block_data.get('transactions', []):
            # Skip if no value transferred (contract deployments, pure function calls)
            if tx.get('value', 0) == 0:
                continue
            
            tx_formatted = {
                'tx_hash': tx['hash'].hex() if isinstance(tx['hash'], bytes) else tx['hash'],
                'block_number': block_data['number'],
                'timestamp': block_timestamp,
                'from_address': tx['from'],
                'to_address': tx['to'] if tx['to'] else None,
                'value': str(tx['value']),  # Keep as string to avoid overflow
                'value_decimal': self.node_provider.from_wei(tx['value'], 'ether'),
                'gas_used': tx.get('gas'),
                'gas_price': tx.get('gasPrice'),
                'input_data': tx.get('input', '0x'),
                'chain_id': self.chain_id
            }
            
            # Determine if contract interaction
            if tx['to'] and self.node_provider.is_contract(tx['to']):
                tx_formatted['is_contract_interaction'] = True
                # Extract method ID (first 4 bytes of input data)
                if tx_formatted['input_data'] and len(tx_formatted['input_data']) >= 10:
                    tx_formatted['method_id'] = tx_formatted['input_data'][:10]
            else:
                tx_formatted['is_contract_interaction'] = False
            
            transactions.append(tx_formatted)
        
        return transactions
    
    def get_current_block(self) -> int:
        """Get current block being processed."""
        return self.current_block if self.current_block else 0
    
    def get_blocks_behind(self) -> int:
        """Get number of blocks behind latest."""
        latest = self.node_provider.get_latest_block_number()
        current = self.current_block if self.current_block else latest
        return latest - current
