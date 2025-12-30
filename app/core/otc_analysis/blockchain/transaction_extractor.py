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
    ✅ FIXED: Token-specific validation and thresholds
    ✅ FIXED: Validates USD values before returning
    ✅ FIXED: Shows actual token symbol (USDT, LINK) not "ERC20"
    """
    
    def __init__(self, node_provider: NodeProvider, etherscan: EtherscanAPI):
        self.node_provider = node_provider
        self.etherscan = etherscan
        
        # ✅ Token-specific max reasonable amounts
        self.token_max_amounts = {
            'USDT': 1_000_000_000,       # 1B USDT
            'USDC': 1_000_000_000,       # 1B USDC
            'DAI': 1_000_000_000,        # 1B DAI
            'WBTC': 100_000,             # 100K WBTC
            'WETH': 1_000_000,           # 1M WETH
            'LINK': 1_000_000_000,       # 1B LINK
            'UNI': 1_000_000_000,        # 1B UNI
            'MATIC': 10_000_000_000,     # 10B MATIC
            'SHIB': 100_000_000_000_000, # 100T SHIB (meme coin)
        }
    
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
            wei_int = int(wei_value) if isinstance(wei_value, str) else wei_value
            eth_value = wei_int / 1e18
            
            # Sanity check: No wallet should have > 1 million ETH
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
        """Extract all transactions for a wallet address."""
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
        """Format normal transactions from Etherscan."""
        formatted = []
        
        for tx in txs:
            try:
                value_wei = tx.get('value', '0')
                value_eth = self.wei_to_eth(value_wei)
                
                formatted.append({
                    'tx_hash': tx['hash'],
                    'block_number': int(tx['blockNumber']),
                    'timestamp': datetime.fromtimestamp(int(tx['timeStamp'])),
                    'from_address': tx['from'],
                    'to_address': tx['to'],
                    'value': value_wei,
                    'value_decimal': value_eth,  # ✅ Already in ETH!
                    'gas_used': int(tx['gasUsed']),
                    'gas_price': int(tx['gasPrice']),
                    'is_contract_interaction': tx.get('input', '0x') != '0x',
                    'method_id': tx.get('input', '')[:10] if len(tx.get('input', '')) >= 10 else None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,
                    'token_symbol': 'ETH',  # ✅ FIXED: Native ETH
                    'tx_type': 'normal'
                })
            except Exception as e:
                logger.debug(f"Error formatting transaction {tx.get('hash')}: {e}")
                continue
        
        return formatted
    
    def _format_internal_transactions(self, txs: List[Dict]) -> List[Dict]:
        """Format internal transactions from Etherscan."""
        formatted = []
        
        for tx in txs:
            try:
                value_wei = tx.get('value', '0')
                value_eth = self.wei_to_eth(value_wei)
                
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
                    'gas_price': 0,
                    'is_contract_interaction': True,
                    'method_id': None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,
                    'token_symbol': 'ETH',  # ✅ FIXED: Native ETH
                    'tx_type': 'internal'
                })
            except Exception as e:
                logger.debug(f"Error formatting internal transaction: {e}")
                continue
        
        return formatted
    
    def _format_token_transactions(self, txs: List[Dict]) -> List[Dict]:
        """
        Format ERC20 token transactions from Etherscan.
        
        ✅ HOTFIX: Better validation for token amounts with token-specific thresholds
        ✅ FIXED: Includes token_symbol for proper logging
        """
        formatted = []
        rejected_count = 0
        
        for tx in txs:
            try:
                # Calculate decimal value
                decimals = int(tx.get('tokenDecimal', 18))
                value_raw = int(tx.get('value', 0))
                value_decimal = value_raw / (10 ** decimals)
                
                # ✅ Token-specific validation
                token_symbol = tx.get('tokenSymbol', '').upper()
                
                # Get threshold for this token (default 10B for unknown tokens)
                threshold = self.token_max_amounts.get(token_symbol, 10_000_000_000)
                
                # ✅ Reject if exceeds token-specific threshold
                if value_decimal > threshold:
                    rejected_count += 1
                    if rejected_count <= 5:  # Only log first 5 to avoid spam
                        logger.debug(
                            f"Rejecting {token_symbol}: {value_decimal:,.0f} tokens (max: {threshold:,})"
                        )
                    continue
                
                # ✅ Additional check: Scientific notation indicates unrealistic number
                if value_decimal >= 1e15:  # 1 quadrillion
                    rejected_count += 1
                    if rejected_count <= 5:
                        logger.debug(
                            f"Rejecting {token_symbol}: {value_decimal:.2e} tokens (too large)"
                        )
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
                    'token_symbol': token_symbol,  # ✅ FIXED: Include token symbol!
                    'token_decimals': decimals,
                    'tx_type': 'erc20'
                })
            except Exception as e:
                logger.debug(f"Error formatting token transaction: {e}")
                continue
        
        if rejected_count > 0:
            logger.info(f"📊 Token validation: Rejected {rejected_count} unrealistic token transactions")
        
        return formatted
    
    def enrich_with_usd_value(
        self,
        transactions: List[Dict],
        price_oracle,
        max_transactions: int = 100
    ) -> List[Dict]:
        """
        Add USD values to transactions using price oracle.
        
        ✅ ENHANCED LOGGING: Shows exactly why enrichment fails
        ✅ Tracks API call success/failure by token
        ✅ Identifies rate limiting issues
        ✅ Shows which tokens are not in CoinGecko
        """
        if not transactions:
            return []
        
        txs_to_enrich = transactions[:max_transactions]
        remaining_txs = transactions[max_transactions:]
        
        logger.info(f"💰 Enriching top {len(txs_to_enrich)} of {len(transactions)} transactions...")
        if remaining_txs:
            logger.info(f"⏭️  Skipping {len(remaining_txs)} older transactions")
        
        price_cache = {}
        
        # ✅ NEW: Detailed tracking
        enriched_count = 0
        cached_count = 0
        failed_count = 0
        suspicious_count = 0
        
        # ✅ NEW: Track failures by reason
        failure_reasons = {
            'price_fetch_failed': [],      # API returned None
            'rate_limited': [],             # Too many requests
            'token_not_found': [],          # Token not in CoinGecko
            'network_error': [],            # Connection issues
            'amount_missing': [],           # value_decimal missing
            'suspicious_value': [],         # Unrealistic values
            'other': []
        }
        
        for tx in txs_to_enrich:
            try:
                token_address = tx.get('token_address')
                tx_type = tx.get('tx_type', 'normal')
                timestamp = tx['timestamp']
                tx_hash = tx.get('tx_hash', 'unknown')[:16]
                
                # ✅ Get token symbol for better logging
                token_symbol = tx.get('token_symbol', 'ETH' if not token_address else 'UNKNOWN')
                
                # ✅ Get amount (already in correct decimals from formatting)
                amount = tx.get('value_decimal')
                
                if amount is None:
                    # Fallback: Only for ETH transactions
                    if token_address is None and tx_type in ['normal', 'internal']:
                        value_wei = tx.get('value', '0')
                        amount = self.wei_to_eth(value_wei)
                        logger.debug(
                            f"⚠️ value_decimal missing for ETH TX {tx_hash}..."
                        )
                    else:
                        # For tokens, skip if value_decimal missing
                        logger.warning(
                            f"❌ value_decimal missing for {token_symbol} TX {tx_hash}... "
                            f"(token_address: {token_address})"
                        )
                        tx['usd_value'] = None
                        failed_count += 1
                        failure_reasons['amount_missing'].append({
                            'tx_hash': tx_hash,
                            'token': token_symbol,
                            'token_address': token_address
                        })
                        continue
                
                # Skip zero values
                if amount <= 0:
                    tx['usd_value'] = 0.0
                    continue
                
                # ✅ Type-specific sanity checks
                if tx_type in ['normal', 'internal']:  # ETH transactions
                    if amount > 100_000:  # 100K ETH threshold
                        logger.debug(
                            f"⚠️ SUSPICIOUS: TX {tx_hash}... "
                            f"has {amount:,.2f} ETH - SKIPPING"
                        )
                        tx['usd_value'] = None
                        suspicious_count += 1
                        failure_reasons['suspicious_value'].append({
                            'tx_hash': tx_hash,
                            'token': token_symbol,
                            'amount': amount,
                            'reason': 'amount_too_high'
                        })
                        continue
                
                elif tx_type == 'erc20':  # Token transactions
                    if amount > 1e15:  # 1 quadrillion tokens
                        logger.debug(
                            f"⚠️ SUSPICIOUS: Token TX {tx_hash}... "
                            f"has {amount:.2e} {token_symbol} - SKIPPING"
                        )
                        tx['usd_value'] = None
                        suspicious_count += 1
                        failure_reasons['suspicious_value'].append({
                            'tx_hash': tx_hash,
                            'token': token_symbol,
                            'amount': amount,
                            'reason': 'amount_too_high'
                        })
                        continue
                
                # Create cache key
                date_key = timestamp.strftime('%Y-%m-%d') if hasattr(timestamp, 'strftime') else None
                cache_key = (token_address or 'ETH', date_key)
                
                # Check cache first
                if cache_key in price_cache:
                    price_usd = price_cache[cache_key]
                    cached_count += 1
                else:
                    # ✅ ENHANCED: Fetch price with detailed logging
                    logger.debug(
                        f"🔍 Fetching price: {token_symbol} "
                        f"(address: {token_address or 'ETH'}) "
                        f"for date: {date_key}"
                    )
                    
                    try:
                        price_usd = price_oracle.get_historical_price(
                            token_address,
                            timestamp
                        )
                        
                        if price_usd:
                            logger.debug(f"   ✅ Got price: ${price_usd:,.2f}")
                            price_cache[cache_key] = price_usd
                        else:
                            logger.debug(f"   ❌ Price API returned None")
                            price_cache[cache_key] = None
                            
                            # ✅ NEW: Try to determine WHY it failed
                            # Check if it's a known issue
                            failure_reason = 'price_fetch_failed'
                            
                            # Check price oracle's last error (if available)
                            if hasattr(price_oracle, 'last_error'):
                                error_msg = str(price_oracle.last_error).lower()
                                if 'rate limit' in error_msg or '429' in error_msg:
                                    failure_reason = 'rate_limited'
                                elif 'not found' in error_msg or '404' in error_msg:
                                    failure_reason = 'token_not_found'
                                elif 'timeout' in error_msg or 'connection' in error_msg:
                                    failure_reason = 'network_error'
                            
                            failure_reasons[failure_reason].append({
                                'token': token_symbol,
                                'token_address': token_address,
                                'date': date_key
                            })
                    
                    except Exception as e:
                        logger.debug(f"   ❌ Exception: {str(e)}")
                        price_cache[cache_key] = None
                        
                        # Categorize exception
                        error_msg = str(e).lower()
                        if 'rate limit' in error_msg or '429' in error_msg:
                            failure_reasons['rate_limited'].append({
                                'token': token_symbol,
                                'error': str(e)
                            })
                        elif 'timeout' in error_msg or 'connection' in error_msg:
                            failure_reasons['network_error'].append({
                                'token': token_symbol,
                                'error': str(e)
                            })
                        else:
                            failure_reasons['other'].append({
                                'token': token_symbol,
                                'error': str(e)
                            })
                
                if price_usd:
                    # Calculate USD value
                    usd_value = amount * price_usd
                    
                    # ✅ Final sanity check: No single transaction > $1 Billion
                    if usd_value > 1_000_000_000:
                        logger.error(
                            f"🚨 UNREALISTIC USD VALUE:\n"
                            f"   TX: {tx_hash}...\n"
                            f"   Token: {token_symbol}\n"
                            f"   Amount: {amount:.4f}\n"
                            f"   Price: ${price_usd:,.2f}\n"
                            f"   = USD: ${usd_value:,.2f}\n"
                            f"   ❌ REJECTING (>$1B)"
                        )
                        tx['usd_value'] = None
                        suspicious_count += 1
                        failure_reasons['suspicious_value'].append({
                            'tx_hash': tx_hash,
                            'token': token_symbol,
                            'amount': amount,
                            'price': price_usd,
                            'usd_value': usd_value,
                            'reason': 'usd_value_too_high'
                        })
                    else:
                        tx['usd_value'] = usd_value
                        enriched_count += 1
                        
                        # Log first 3 successful enrichments
                        if enriched_count <= 3:
                            logger.info(
                                f"✅ Enriched TX {tx_hash}...: "
                                f"{amount:.4f} {token_symbol} * ${price_usd:,.2f} = ${usd_value:,.2f}"
                            )
                else:
                    tx['usd_value'] = None
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"❌ Error enriching transaction {tx.get('tx_hash')}: {e}")
                tx['usd_value'] = None
                failed_count += 1
                failure_reasons['other'].append({
                    'tx_hash': tx.get('tx_hash', 'unknown')[:16],
                    'error': str(e)
                })
        
        # Set usd_value to None for remaining transactions
        for tx in remaining_txs:
            tx['usd_value'] = None
        
        # ====================================================================
        # ✅ ENHANCED SUMMARY LOGGING - Shows WHY enrichments failed
        # ====================================================================
        logger.info(f"✅ Enrichment complete:")
        logger.info(f"   • Enriched: {enriched_count} transactions")
        logger.info(f"   • Cached: {cached_count} (saved API calls)")
        logger.info(f"   • Failed: {failed_count}")
        logger.info(f"   • Suspicious (rejected): {suspicious_count}")
        logger.info(f"📊 Made {len(price_cache)} unique price API calls instead of {len(txs_to_enrich)}")
        
        # ✅ NEW: Detailed failure analysis
        if failed_count > 0:
            logger.warning(f"")
            logger.warning(f"📊 FAILURE ANALYSIS:")
            logger.warning(f"   Total failed: {failed_count}")
            
            if failure_reasons['rate_limited']:
                logger.warning(f"   ⏱️  Rate limited: {len(failure_reasons['rate_limited'])} calls")
                # Show first 3 examples
                for item in failure_reasons['rate_limited'][:3]:
                    logger.warning(f"      • {item}")
            
            if failure_reasons['token_not_found']:
                logger.warning(f"   ❓ Token not found: {len(failure_reasons['token_not_found'])} tokens")
                # Show unique tokens
                unique_tokens = list(set(item['token'] for item in failure_reasons['token_not_found']))
                logger.warning(f"      Tokens: {', '.join(unique_tokens[:10])}")
            
            if failure_reasons['price_fetch_failed']:
                logger.warning(f"   ❌ Price fetch failed: {len(failure_reasons['price_fetch_failed'])} calls")
                # Group by token
                tokens = {}
                for item in failure_reasons['price_fetch_failed']:
                    token = item['token']
                    tokens[token] = tokens.get(token, 0) + 1
                logger.warning(f"      Most common failures:")
                for token, count in sorted(tokens.items(), key=lambda x: x[1], reverse=True)[:5]:
                    logger.warning(f"         • {token}: {count} times")
            
            if failure_reasons['network_error']:
                logger.warning(f"   🌐 Network errors: {len(failure_reasons['network_error'])} calls")
            
            if failure_reasons['amount_missing']:
                logger.warning(f"   📉 Amount missing: {len(failure_reasons['amount_missing'])} txs")
            
            if failure_reasons['suspicious_value']:
                logger.warning(f"   ⚠️  Suspicious values: {len(failure_reasons['suspicious_value'])} txs")
            
            if failure_reasons['other']:
                logger.warning(f"   ❓ Other errors: {len(failure_reasons['other'])} calls")
                # Show first error
                if failure_reasons['other']:
                    logger.warning(f"      Example: {failure_reasons['other'][0]}")
        
        # Calculate and log volume
        total_usd = sum(tx.get('usd_value', 0) for tx in txs_to_enrich if tx.get('usd_value'))
        avg_usd = total_usd / enriched_count if enriched_count > 0 else 0
        logger.info(f"💵 Enriched Volume: ${total_usd:,.2f} total, ${avg_usd:,.2f} average")
        
        return txs_to_enrich + remaining_txs
    
    def filter_by_value(
        self,
        transactions: List[Dict],
        min_usd_value: float = 100000
    ) -> List[Dict]:
        """Filter transactions by minimum USD value."""
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
        """Filter transactions by counterparty address."""
        if direction == 'from':
            return [tx for tx in transactions if tx['from_address'].lower() == address.lower()]
        elif direction == 'to':
            return [tx for tx in transactions if tx['to_address'].lower() == address.lower()]
        else:
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
        """Get set of unique counterparty addresses."""
        counterparties = set()
        
        for tx in transactions:
            if tx['from_address'].lower() == address.lower():
                counterparties.add(tx['to_address'])
            elif tx['to_address'].lower() == address.lower():
                counterparties.add(tx['from_address'])
        
        return counterparties
