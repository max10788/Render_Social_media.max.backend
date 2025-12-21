from typing import List, Dict, Optional
from datetime import datetime
from otc_analysis.blockchain.etherscan import EtherscanAPI
from otc_analysis.blockchain.node_provider import NodeProvider
import logging

logger = logging.getLogger(__name__)  # ← HINZUFÜGEN

class TransactionExtractor:
    """
    Extracts and enriches transaction data from various sources.
    Combines node data with Etherscan API for comprehensive transaction info.
    """
    
    def __init__(self, node_provider: NodeProvider, etherscan: EtherscanAPI):
        self.node_provider = node_provider
        self.etherscan = etherscan
    
    def extract_wallet_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
        include_internal: bool = True,
        include_tokens: bool = True
    ) -> List[Dict]:
        """
        Extract all transactions for a wallet address.
        
        Args:
            address: Wallet address
            start_block: Starting block
            end_block: Ending block
            include_internal: Include internal (contract) transactions
            include_tokens: Include ERC20 token transfers
        
        Returns:
            List of enriched transaction dicts
        """
        all_txs = []
        
        # Get normal transactions
        print(f"Fetching normal transactions for {address[:10]}...")
        normal_txs = self.etherscan.get_normal_transactions(
            address, start_block, end_block
        )
        all_txs.extend(self._format_normal_transactions(normal_txs))
        
        # Get internal transactions
        if include_internal:
            print(f"Fetching internal transactions...")
            internal_txs = self.etherscan.get_internal_transactions(
                address, start_block, end_block
            )
            all_txs.extend(self._format_internal_transactions(internal_txs))
        
        # Get ERC20 token transfers
        if include_tokens:
            print(f"Fetching token transfers...")
            token_txs = self.etherscan.get_erc20_transfers(
                address=address,
                start_block=start_block,
                end_block=end_block
            )
            all_txs.extend(self._format_token_transactions(token_txs))
        
        # Sort by timestamp
        all_txs.sort(key=lambda x: x['timestamp'], reverse=True)
        
        print(f"✓ Extracted {len(all_txs)} transactions for {address[:10]}...")
        return all_txs
    
    def _format_normal_transactions(self, txs: List[Dict]) -> List[Dict]:
        """Format normal transactions from Etherscan."""
        formatted = []
        
        for tx in txs:
            try:
                formatted.append({
                    'tx_hash': tx['hash'],
                    'block_number': int(tx['blockNumber']),
                    'timestamp': datetime.fromtimestamp(int(tx['timeStamp'])),
                    'from_address': tx['from'],
                    'to_address': tx['to'],
                    'value': tx['value'],
                    'value_decimal': float(tx['value']) / 1e18,  # Wei to ETH
                    'gas_used': int(tx['gasUsed']),
                    'gas_price': int(tx['gasPrice']),
                    'is_contract_interaction': tx.get('input', '0x') != '0x',
                    'method_id': tx.get('input', '')[:10] if len(tx.get('input', '')) >= 10 else None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,  # Native ETH
                    'tx_type': 'normal'
                })
            except Exception as e:
                print(f"Error formatting transaction {tx.get('hash')}: {e}")
                continue
        
        return formatted
    
    def _format_internal_transactions(self, txs: List[Dict]) -> List[Dict]:
        """Format internal transactions from Etherscan."""
        formatted = []
        
        for tx in txs:
            try:
                # Skip if no value
                if int(tx.get('value', 0)) == 0:
                    continue
                
                formatted.append({
                    'tx_hash': tx['hash'],
                    'block_number': int(tx['blockNumber']),
                    'timestamp': datetime.fromtimestamp(int(tx['timeStamp'])),
                    'from_address': tx['from'],
                    'to_address': tx['to'],
                    'value': tx['value'],
                    'value_decimal': float(tx['value']) / 1e18,
                    'gas_used': int(tx.get('gas', 0)),
                    'gas_price': 0,  # Not available for internal txs
                    'is_contract_interaction': True,
                    'method_id': None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,
                    'tx_type': 'internal'
                })
            except Exception as e:
                print(f"Error formatting internal transaction: {e}")
                continue
        
        return formatted
    
    def _format_token_transactions(self, txs: List[Dict]) -> List[Dict]:
        """Format ERC20 token transactions from Etherscan."""
        formatted = []
        
        for tx in txs:
            try:
                # Calculate decimal value
                decimals = int(tx.get('tokenDecimal', 18))
                value_decimal = float(tx['value']) / (10 ** decimals)
                
                formatted.append({
                    'tx_hash': tx['hash'],
                    'block_number': int(tx['blockNumber']),
                    'timestamp': datetime.fromtimestamp(int(tx['timeStamp'])),
                    'from_address': tx['from'],
                    'to_address': tx['to'],
                    'value': tx['value'],
                    'value_decimal': value_decimal,
                    'gas_used': int(tx.get('gasUsed', 0)),
                    'gas_price': int(tx.get('gasPrice', 0)),
                    'is_contract_interaction': True,
                    'method_id': None,
                    'is_error': False,
                    'token_address': tx['contractAddress'],
                    'token_name': tx.get('tokenName'),
                    'token_symbol': tx.get('tokenSymbol'),
                    'token_decimals': decimals,
                    'tx_type': 'erc20'
                })
            except Exception as e:
                print(f"Error formatting token transaction: {e}")
                continue
        
        return formatted
    
    def enrich_with_usd_value(
        self,
        transactions: List[Dict],
        price_oracle
    ) -> List[Dict]:
        """
        Add USD values to transactions using price oracle.
        
        OPTIMIZED: Caches prices per day/token to avoid excessive API calls.
        
        Args:
            transactions: List of transaction dicts
            price_oracle: PriceOracle instance
        
        Returns:
            Enriched transactions with usd_value field
        """
        # Group transactions by date and token for batching
        price_cache = {}  # {(token, date): price}
        
        logger.info(f"💰 Enriching {len(transactions)} transactions with USD values...")
        
        enriched_count = 0
        cached_count = 0
        failed_count = 0
        
        for tx in transactions:
            try:
                token_address = tx.get('token_address')  # None for ETH
                timestamp = tx['timestamp']
                amount = tx['value_decimal']
                
                # Create cache key (token + date)
                date_key = timestamp.strftime('%Y-%m-%d') if hasattr(timestamp, 'strftime') else None
                cache_key = (token_address or 'ETH', date_key)
                
                # Check cache first
                if cache_key in price_cache:
                    price_usd = price_cache[cache_key]
                    cached_count += 1
                else:
                    # Fetch price (only once per token per day)
                    price_usd = price_oracle.get_historical_price(
                        token_address,
                        timestamp
                    )
                    price_cache[cache_key] = price_usd
                
                if price_usd:
                    tx['usd_value'] = amount * price_usd
                    enriched_count += 1
                else:
                    tx['usd_value'] = None
                    failed_count += 1
                    
            except Exception as e:
                logger.debug(f"Error enriching transaction {tx.get('tx_hash')}: {e}")
                tx['usd_value'] = None
                failed_count += 1
        
        logger.info(f"✅ Enriched {enriched_count} transactions ({cached_count} from cache, {failed_count} failed)")
        logger.info(f"📊 Made {len(price_cache)} unique price API calls instead of {len(transactions)}")
        
        return transactions
    
    def filter_by_value(
        self,
        transactions: List[Dict],
        min_usd_value: float = 100000
    ) -> List[Dict]:
        """
        Filter transactions by minimum USD value.
        Used for OTC detection (typically >$100K).
        """
        return [
            tx for tx in transactions
            if tx.get('usd_value') and tx['usd_value'] >= min_usd_value
        ]
    
    def filter_by_counterparty(
        self,
        transactions: List[Dict],
        address: str,
        direction: str = 'both'
    ) -> List[Dict]:
        """
        Filter transactions by counterparty address.
        
        Args:
            direction: 'from', 'to', or 'both'
        """
        if direction == 'from':
            return [tx for tx in transactions if tx['from_address'].lower() == address.lower()]
        elif direction == 'to':
            return [tx for tx in transactions if tx['to_address'].lower() == address.lower()]
        else:  # both
            return [
                tx for tx in transactions
                if tx['from_address'].lower() == address.lower() or
                   tx['to_address'].lower() == address.lower()
            ]
    
    def get_unique_counterparties(
        self,
        transactions: List[Dict],
        address: str
    ) -> set:
        """Get set of unique counterparty addresses for a given address."""
        counterparties = set()
        
        for tx in transactions:
            if tx['from_address'].lower() == address.lower():
                counterparties.add(tx['to_address'])
            elif tx['to_address'].lower() == address.lower():
                counterparties.add(tx['from_address'])
        
        return counterparties
