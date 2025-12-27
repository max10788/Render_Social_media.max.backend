from typing import List, Dict, Optional
from datetime import datetime
from app.core.otc_analysis.blockchain.etherscan import EtherscanAPI
from app.core.otc_analysis.blockchain.node_provider import NodeProvider
import logging

logger = logging.getLogger(__name__)

class TransactionExtractor:
    """
    Extracts and enriches transaction data from various sources.
    
    ✅ FIXED: Proper Wei-to-ETH conversion with sanity checks
    ✅ FIXED: Uses int() instead of float() for precision
    ✅ FIXED: Validates USD values before returning
    """
    
    def __init__(self, node_provider: NodeProvider, etherscan: EtherscanAPI):
        self.node_provider = node_provider
        self.etherscan = etherscan
    
    @staticmethod
    def wei_to_eth(wei_value: str | int) -> float:
        """
        Convert Wei to ETH with proper precision.
        
        ✅ FIX: Uses int() instead of float() to avoid precision loss
        
        Args:
            wei_value: Value in Wei (string or int)
        
        Returns:
            Value in ETH (float)
        """
        try:
            # ✅ Use int() for exact conversion, then divide
            wei_int = int(wei_value) if isinstance(wei_value, str) else wei_value
            eth_value = wei_int / 1e18
            
            # ✅ Sanity check: No wallet should have > 1 million ETH
            if eth_value > 1_000_000:
                logger.warning(f"⚠️ Suspicious ETH value: {eth_value:.2f} ETH from {wei_value} Wei")
                return 0.0
            
            return eth_value
        except (ValueError, TypeError, OverflowError) as e:
            logger.error(f"❌ Failed to convert Wei value: {wei_value} - {e}")
            return 0.0
    
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
        logger.info(f"Fetching normal transactions for {address[:10]}...")
        normal_txs = self.etherscan.get_normal_transactions(
            address, start_block, end_block
        )
        all_txs.extend(self._format_normal_transactions(normal_txs))
        
        # Get internal transactions
        if include_internal:
            logger.info(f"Fetching internal transactions...")
            internal_txs = self.etherscan.get_internal_transactions(
                address, start_block, end_block
            )
            all_txs.extend(self._format_internal_transactions(internal_txs))
        
        # Get ERC20 token transfers
        if include_tokens:
            logger.info(f"Fetching token transfers...")
            token_txs = self.etherscan.get_erc20_transfers(
                address=address,
                start_block=start_block,
                end_block=end_block
            )
            all_txs.extend(self._format_token_transactions(token_txs))
        
        # Sort by timestamp
        all_txs.sort(key=lambda x: x['timestamp'], reverse=True)
        
        logger.info(f"✓ Extracted {len(all_txs)} transactions for {address[:10]}...")
        return all_txs
    
    def _format_normal_transactions(self, txs: List[Dict]) -> List[Dict]:
        """
        Format normal transactions from Etherscan.
        
        ✅ FIXED: Uses wei_to_eth() for proper conversion
        """
        formatted = []
        
        for tx in txs:
            try:
                # ✅ FIX: Use helper function for conversion
                value_wei = tx.get('value', '0')
                value_eth = self.wei_to_eth(value_wei)
                
                formatted.append({
                    'tx_hash': tx['hash'],
                    'block_number': int(tx['blockNumber']),
                    'timestamp': datetime.fromtimestamp(int(tx['timeStamp'])),
                    'from_address': tx['from'],
                    'to_address': tx['to'],
                    'value': value_wei,  # Keep original Wei string
                    'value_decimal': value_eth,  # ✅ Already in ETH!
                    'gas_used': int(tx['gasUsed']),
                    'gas_price': int(tx['gasPrice']),
                    'is_contract_interaction': tx.get('input', '0x') != '0x',
                    'method_id': tx.get('input', '')[:10] if len(tx.get('input', '')) >= 10 else None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,  # Native ETH
                    'tx_type': 'normal'
                })
            except Exception as e:
                logger.debug(f"Error formatting transaction {tx.get('hash')}: {e}")
                continue
        
        return formatted
    
    def _format_internal_transactions(self, txs: List[Dict]) -> List[Dict]:
        """
        Format internal transactions from Etherscan.
        
        ✅ FIXED: Uses wei_to_eth() for proper conversion
        """
        formatted = []
        
        for tx in txs:
            try:
                # ✅ FIX: Convert Wei to ETH properly
                value_wei = tx.get('value', '0')
                value_eth = self.wei_to_eth(value_wei)
                
                # Skip if no value
                if value_eth == 0:
                    continue
                
                formatted.append({
                    'tx_hash': tx['hash'],
                    'block_number': int(tx['blockNumber']),
                    'timestamp': datetime.fromtimestamp(int(tx['timeStamp'])),
                    'from_address': tx['from'],
                    'to_address': tx['to'],
                    'value': value_wei,
                    'value_decimal': value_eth,  # ✅ Already in ETH!
                    'gas_used': int(tx.get('gas', 0)),
                    'gas_price': 0,  # Not available for internal txs
                    'is_contract_interaction': True,
                    'method_id': None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,
                    'tx_type': 'internal'
                })
            except Exception as e:
                logger.debug(f"Error formatting internal transaction: {e}")
                continue
        
        return formatted
    
    def _format_token_transactions(self, txs: List[Dict]) -> List[Dict]:
        """
        Format ERC20 token transactions from Etherscan.
        
        ✅ FIXED: Proper decimal handling
        """
        formatted = []
        
        for tx in txs:
            try:
                # Calculate decimal value
                decimals = int(tx.get('tokenDecimal', 18))
                value_raw = int(tx.get('value', 0))
                value_decimal = value_raw / (10 ** decimals)
                
                # ✅ Sanity check for token values
                if value_decimal > 1_000_000_000:  # > 1 billion tokens
                    logger.warning(f"⚠️ Suspicious token value: {value_decimal}")
                    continue
                
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
                logger.debug(f"Error formatting token transaction: {e}")
                continue
        
        return formatted
    
    def enrich_with_usd_value(
        self,
        transactions: List[Dict],
        price_oracle,
        max_transactions: int = 100
    ) -> List[Dict]:
        """
        Add USD values to transactions using price oracle.
        
        ✅ FIXED: Validates that value_decimal exists and is in ETH
        ✅ FIXED: Adds sanity checks for USD values
        
        OPTIMIZED: 
        - Only enriches most recent N transactions to avoid timeout
        - Caches prices per day/token to avoid excessive API calls
        
        Args:
            transactions: List of transaction dicts (should be sorted by timestamp DESC)
            price_oracle: PriceOracle instance
            max_transactions: Maximum number of transactions to enrich (default 100)
        
        Returns:
            Enriched transactions with usd_value field
        """
        if not transactions:
            return []
        
        # Split into transactions to enrich and skip
        txs_to_enrich = transactions[:max_transactions]
        remaining_txs = transactions[max_transactions:]
        
        logger.info(f"💰 Enriching top {len(txs_to_enrich)} of {len(transactions)} transactions...")
        if remaining_txs:
            logger.info(f"⏭️  Skipping {len(remaining_txs)} older transactions (set max_transactions higher to include)")
        
        # Group transactions by date and token for batching
        price_cache = {}  # {(token, date): price}
        
        enriched_count = 0
        cached_count = 0
        failed_count = 0
        suspicious_count = 0
        
        for tx in txs_to_enrich:
            try:
                token_address = tx.get('token_address')  # None for ETH
                timestamp = tx['timestamp']
                
                # ✅ CRITICAL FIX: Ensure we're using ETH value, not Wei!
                amount_eth = tx.get('value_decimal')
                
                if amount_eth is None:
                    # Fallback: Try to convert from Wei
                    value_wei = tx.get('value', '0')
                    amount_eth = self.wei_to_eth(value_wei)
                    logger.warning(f"⚠️ value_decimal missing for {tx.get('tx_hash', 'unknown')}, converted from Wei")
                
                # ✅ Skip if zero value
                if amount_eth <= 0:
                    tx['usd_value'] = 0.0
                    continue
                
                # ✅ Sanity check: No single transaction should be > 100,000 ETH
                if amount_eth > 100_000:
                    logger.warning(
                        f"⚠️ SUSPICIOUS: Transaction {tx.get('tx_hash', 'unknown')[:16]}... "
                        f"has {amount_eth:.2f} ETH - SKIPPING to avoid bad data"
                    )
                    tx['usd_value'] = None
                    suspicious_count += 1
                    continue
                
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
                    # ✅ Calculate USD value (amount is ALREADY in ETH!)
                    usd_value = amount_eth * price_usd
                    
                    # ✅ SANITY CHECK: Flag unrealistic values
                    if usd_value > 1_000_000_000:  # > $1 Billion
                        logger.error(
                            f"🚨 UNREALISTIC USD VALUE DETECTED:\n"
                            f"   TX: {tx.get('tx_hash', 'unknown')[:16]}...\n"
                            f"   Amount ETH: {amount_eth:.4f}\n"
                            f"   Price USD: ${price_usd:,.2f}\n"
                            f"   = USD Value: ${usd_value:,.2f}\n"
                            f"   ❌ REJECTING (>$1B is unrealistic for single tx)"
                        )
                        tx['usd_value'] = None
                        suspicious_count += 1
                    else:
                        tx['usd_value'] = usd_value
                        enriched_count += 1
                        
                        # Log successful enrichment for debugging
                        if enriched_count <= 3:  # Log first 3
                            logger.info(
                                f"✅ Enriched TX {tx.get('tx_hash', 'unknown')[:16]}...: "
                                f"{amount_eth:.4f} ETH * ${price_usd:,.2f} = ${usd_value:,.2f}"
                            )
                else:
                    tx['usd_value'] = None
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error enriching transaction {tx.get('tx_hash')}: {e}", exc_info=True)
                tx['usd_value'] = None
                failed_count += 1
        
        # Set usd_value to None for remaining (non-enriched) transactions
        for tx in remaining_txs:
            tx['usd_value'] = None
        
        # ✅ Enhanced logging
        logger.info(f"✅ Enrichment complete:")
        logger.info(f"   • Enriched: {enriched_count} transactions")
        logger.info(f"   • Cached: {cached_count} (saved API calls)")
        logger.info(f"   • Failed: {failed_count}")
        logger.info(f"   • Suspicious (rejected): {suspicious_count}")
        logger.info(f"📊 Made {len(price_cache)} unique price API calls instead of {len(txs_to_enrich)}")
        
        # ✅ Calculate and log total volume for verification
        total_usd = sum(tx.get('usd_value', 0) for tx in txs_to_enrich if tx.get('usd_value'))
        avg_usd = total_usd / enriched_count if enriched_count > 0 else 0
        logger.info(f"💵 Enriched Volume: ${total_usd:,.2f} total, ${avg_usd:,.2f} average")
        
        # Combine back together in original order
        return txs_to_enrich + remaining_txs
    
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
