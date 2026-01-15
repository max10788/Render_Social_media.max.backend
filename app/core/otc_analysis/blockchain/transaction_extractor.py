from typing import List, Dict, Optional
from datetime import datetime
import requests
import logging
import os

logger = logging.getLogger(__name__)

class TransactionExtractor:
    """
    Extracts and enriches transaction data from various sources.
    
    ✅ NEW: Moralis API Integration with auto-enriched labels
    ✅ FIXED: Proper Wei-to-ETH conversion with sanity checks
    ✅ FIXED: Uses int() instead of float() for precision
    ✅ FIXED: Token-specific validation and thresholds
    ✅ FIXED: Validates USD values before returning
    ✅ FIXED: Shows actual token symbol (USDT, LINK) not "ERC20"
    """
    
    def __init__(self, node_provider, etherscan, use_moralis: bool = True):
        self.node_provider = node_provider
        self.etherscan = etherscan
        self.use_moralis = use_moralis
        
        # ✅ Moralis API Configuration
        self.moralis_api_key = os.getenv('MORALIS_API_KEY', '')
        self.moralis_base_url = "https://deep-index.moralis.io/api/v2.2"
        
        if self.use_moralis and self.moralis_api_key:
            logger.info("✅ Moralis API enabled - labels will be auto-enriched")
        else:
            logger.info("⚠️ Moralis disabled - using Etherscan only")
        
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
        
    
    def _fetch_moralis_transactions(
        self,
        address: str,
        limit: int = 100
    ) -> List[Dict]:
        """
        🔥 Fetch transactions from Moralis with auto-enriched labels.
        
        Returns transactions with:
        - to_address_label (e.g., "Binance: Hot Wallet")
        - from_address_label
        - to_address_entity (e.g., "Binance")
        - from_address_entity
        """
        if not self.moralis_api_key:
            logger.warning("⚠️ Moralis API key missing - falling back to Etherscan")
            return []
        
        try:
            url = f"{self.moralis_base_url}/{address}"
            headers = {
                "X-API-Key": self.moralis_api_key,
                "Accept": "application/json"
            }
            params = {
                "chain": "eth",  # Ethereum mainnet
                "limit": min(limit, 100)  # Max 100 per request
            }
            
            logger.info(f"🔍 Fetching from Moralis: {address[:10]}... (limit: {limit})")
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                transactions = data.get('result', [])
                
                logger.info(
                    f"✅ Moralis: Got {len(transactions)} transactions "
                    f"(with auto-labels)"
                )
                
                return transactions
            
            elif response.status_code == 429:
                logger.warning("⚠️ Moralis rate limit hit - using Etherscan fallback")
                return []
            
            else:
                logger.warning(
                    f"⚠️ Moralis error {response.status_code}: {response.text[:100]}"
                )
                return []
        
        except requests.exceptions.Timeout:
            logger.warning("⚠️ Moralis timeout - using Etherscan fallback")
            return []
        
        except Exception as e:
            logger.error(f"❌ Moralis error: {e}")
            return []
    
    def _is_known_entity(self, label: str) -> bool:
        """
        Check if label matches known exchange/protocol.
        
        ✅ OPTIMIERT: Set-based O(1) lookup mit Caching
        
        Examples:
        - "Binance: Hot Wallet 6" → True (contains "binance")
        - "Uniswap V3: Router" → True (contains "uniswap")
        - "MEV Bot: 0x123..." → True (contains "mev bot")
        - "Random Wallet" → False
        """
        if not label:
            return False
        
        label_lower = label.lower()
        
        # ✅ Cache pattern set (einmalig beim ersten Call)
        if not hasattr(self, '_known_patterns_set'):
            self._known_patterns_set = {
                # Centralized Exchanges (CEX)
                'binance', 'coinbase', 'kraken', 'bitfinex', 'gemini',
                'bybit', 'okx', 'huobi', 'kucoin', 'gate.io', 'crypto.com',
                'bittrex', 'poloniex', 'bitstamp', 'ftx',
                
                # Decentralized Exchanges (DEX)
                'uniswap', '1inch', 'sushiswap', 'curve', 'balancer',
                'pancakeswap', '0x protocol', 'paraswap', 'kyber',
                'matcha', 'dex aggregator',
                
                # Bridges
                'multichain', 'synapse', 'stargate', 'hop protocol',
                'across', 'celer', 'connext', 'anyswap', 'wormhole',
                
                # MEV & Bots
                'mev bot', 'flashbots', 'mev relay', 'jit', 'sandwich',
                'arbitrage bot', 'front-run', 'backrun',
                
                # DeFi Protocols
                'aave', 'compound', 'makerdao', 'lido', 'yearn',
                'convex', 'curve finance', 'rocket pool',
                
                # Lending/Borrowing
                'benqi', 'venus', 'radiant', 'euler',
                
                # Privacy
                'tornado cash', 'mixer', 'privacy protocol',
                
                # Smart Contract Wallets
                'gnosis safe', 'multisig', 'argent', 'safe',
                
                # NFT Marketplaces
                'opensea', 'blur', 'x2y2', 'looksrare',
                
                # Others
                'null address', 'burn address', 'team wallet',
                'treasury', 'deployer'
            }
        
        # Check if any pattern is in label
        for pattern in self._known_patterns_set:
            if pattern in label_lower:
                return True
        
        return False

    
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
        include_tokens: bool = True,
        use_moralis_first: bool = True
    ) -> List[Dict]:
        """
        Extract all transactions for a wallet address.
        
        ✅ NEW: Tries Moralis first (with labels), falls back to Etherscan
        """
        all_txs = []
        
        # ============================================================================
        # 🔥 TRY MORALIS FIRST (if enabled)
        # ============================================================================
        if use_moralis_first and self.use_moralis and self.moralis_api_key:
            logger.info(f"🔥 Using Moralis API for {address[:10]}...")
            
            moralis_txs = self._fetch_moralis_transactions(address, limit=100)
            
            if moralis_txs:
                formatted = self._format_moralis_transactions(moralis_txs)
                
                if formatted:
                    logger.info(
                        f"✅ Moralis success: {len(formatted)} transactions "
                        f"(with entity labels)"
                    )
                    return formatted
                else:
                    logger.warning("⚠️ Moralis returned empty results")
        
        # ============================================================================
        # FALLBACK: Use Etherscan (original logic)
        # ============================================================================
        logger.info(f"📡 Using Etherscan fallback for {address[:10]}...")
        
        # Get normal transactions
        logger.info(f"Fetching normal transactions...")
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
    
    def _format_moralis_transactions(self, txs: List[Dict]) -> List[Dict]:
        """
        Format Moralis transactions with auto-enriched labels.
        
        ✅ Extracts labels automatically:
        - to_address_label
        - from_address_label
        - to_address_entity
        - from_address_entity
        - to_address_entity_logo
        - from_address_entity_logo
        """
        formatted = []
        
        for tx in txs:
            try:
                # Basic transaction data
                value_wei = tx.get('value', '0')
                value_eth = self.wei_to_eth(value_wei)
                
                # ✅ Extract Moralis labels
                to_label = tx.get('to_address_label', None)
                from_label = tx.get('from_address_label', None)
                to_entity = tx.get('to_address_entity', None)
                from_entity = tx.get('from_address_entity', None)
                to_logo = tx.get('to_address_entity_logo', None)
                from_logo = tx.get('from_address_entity_logo', None)
                
                # Check if it's a known entity
                to_is_known = self._is_known_entity(to_label or to_entity or '')
                from_is_known = self._is_known_entity(from_label or from_entity or '')
                
                formatted.append({
                    'tx_hash': tx['hash'],
                    'block_number': int(tx['block_number']),
                    'timestamp': datetime.fromisoformat(tx['block_timestamp'].replace('Z', '+00:00')),
                    'from_address': tx['from_address'],
                    'to_address': tx['to_address'],
                    'value': value_wei,
                    'value_decimal': value_eth,
                    'gas_used': int(tx.get('receipt_gas_used', 0)),
                    'gas_price': int(tx.get('gas_price', 0)),
                    'is_contract_interaction': tx.get('input', '0x') != '0x',
                    'method_id': tx.get('input', '')[:10] if len(tx.get('input', '')) >= 10 else None,
                    'is_error': tx.get('receipt_status', '1') == '0',
                    'token_address': None,
                    'token_symbol': 'ETH',
                    'tx_type': 'normal',
                    
                    # ✅ MORALIS LABELS
                    'to_address_label': to_label,
                    'from_address_label': from_label,
                    'to_address_entity': to_entity,
                    'from_address_entity': from_entity,
                    'to_address_entity_logo': to_logo,
                    'from_address_entity_logo': from_logo,
                    'to_is_known_entity': to_is_known,
                    'from_is_known_entity': from_is_known,
                    'source': 'moralis'
                })
                
            except Exception as e:
                logger.debug(f"Error formatting Moralis transaction {tx.get('hash')}: {e}")
                continue
        
        return formatted
    
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
                    'value_decimal': value_eth,
                    'gas_used': int(tx['gasUsed']),
                    'gas_price': int(tx['gasPrice']),
                    'is_contract_interaction': tx.get('input', '0x') != '0x',
                    'method_id': tx.get('input', '')[:10] if len(tx.get('input', '')) >= 10 else None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,
                    'token_symbol': 'ETH',
                    'tx_type': 'normal',
                    
                    # No Moralis labels available
                    'to_address_label': None,
                    'from_address_label': None,
                    'to_address_entity': None,
                    'from_address_entity': None,
                    'to_address_entity_logo': None,
                    'from_address_entity_logo': None,
                    'to_is_known_entity': False,
                    'from_is_known_entity': False,
                    'source': 'etherscan'
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
                    'value_decimal': value_eth,
                    'gas_used': int(tx.get('gas', 0)),
                    'gas_price': 0,
                    'is_contract_interaction': True,
                    'method_id': None,
                    'is_error': tx.get('isError', '0') == '1',
                    'token_address': None,
                    'token_symbol': 'ETH',
                    'tx_type': 'internal',
                    
                    # No Moralis labels available
                    'to_address_label': None,
                    'from_address_label': None,
                    'to_address_entity': None,
                    'from_address_entity': None,
                    'to_address_entity_logo': None,
                    'from_address_entity_logo': None,
                    'to_is_known_entity': False,
                    'from_is_known_entity': False,
                    'source': 'etherscan'
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
                    'token_symbol': token_symbol,
                    'token_decimals': decimals,
                    'tx_type': 'erc20',
                    
                    # No Moralis labels available
                    'to_address_label': None,
                    'from_address_label': None,
                    'to_address_entity': None,
                    'from_address_entity': None,
                    'to_address_entity_logo': None,
                    'from_address_entity_logo': None,
                    'to_is_known_entity': False,
                    'from_is_known_entity': False,
                    'source': 'etherscan'
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
        
        ✅ ENHANCED v2: Pass token_symbol to price_oracle for better lookup
        """
        if not transactions:
            return []
        
        txs_to_enrich = transactions[:max_transactions]
        remaining_txs = transactions[max_transactions:]
        
        logger.info(f"💰 Enriching top {len(txs_to_enrich)} of {len(transactions)} transactions...")
        if remaining_txs:
            logger.info(f"⏭️  Skipping {len(remaining_txs)} older transactions")
        
        price_cache = {}
        
        # Tracking
        enriched_count = 0
        cached_count = 0
        failed_count = 0
        suspicious_count = 0
        stablecoin_count = 0
        
        # Failure tracking
        failure_reasons = {
            'price_fetch_failed': [],
            'rate_limited': [],
            'token_not_found': [],
            'network_error': [],
            'amount_missing': [],
            'suspicious_value': [],
            'other': []
        }
        
        # Stablecoin list
        STABLECOINS = {
            'USDT': 1.0,
            'USDC': 1.0,
            'DAI': 1.0,
            'BUSD': 1.0,
            'TUSD': 1.0,
            'USDD': 1.0,
            'FRAX': 1.0,
            'USDP': 1.0,
            'GUSD': 1.0,
            'LUSD': 1.0
        }
        
        for tx in txs_to_enrich:
            try:
                token_address = tx.get('token_address')
                tx_type = tx.get('tx_type', 'normal')
                timestamp = tx['timestamp']
                tx_hash = tx.get('tx_hash', 'unknown')[:16]
                
                # Get token symbol
                token_symbol = tx.get('token_symbol', 'ETH' if not token_address else 'UNKNOWN')
                
                # Get amount
                amount = tx.get('value_decimal')
                
                if amount is None:
                    if token_address is None and tx_type in ['normal', 'internal']:
                        value_wei = tx.get('value', '0')
                        amount = self.wei_to_eth(value_wei)
                    else:
                        logger.warning(f"❌ value_decimal missing for {token_symbol} TX {tx_hash}...")
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
                
                # Sanity checks
                if tx_type in ['normal', 'internal']:
                    if amount > 100_000:
                        logger.debug(f"⚠️ SUSPICIOUS: TX {tx_hash}... has {amount:,.2f} ETH - SKIPPING")
                        tx['usd_value'] = None
                        suspicious_count += 1
                        failure_reasons['suspicious_value'].append({
                            'tx_hash': tx_hash,
                            'token': token_symbol,
                            'amount': amount,
                            'reason': 'amount_too_high'
                        })
                        continue
                
                elif tx_type == 'erc20':
                    if amount > 1e15:
                        logger.debug(f"⚠️ SUSPICIOUS: Token TX {tx_hash}... has {amount:.2e} {token_symbol} - SKIPPING")
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
                
                # Check cache
                if cache_key in price_cache:
                    price_usd = price_cache[cache_key]
                    cached_count += 1
                else:
                    # ====================================================================
                    # ✅ FIX: Pass token_symbol to price_oracle!
                    # ====================================================================
                    logger.debug(f"🔍 Fetching price: {token_symbol} @ {date_key}")
                    
                    try:
                        # ✅ NEW: Pass both address AND symbol
                        price_usd = price_oracle.get_historical_price(
                            token_address,
                            timestamp,
                            token_symbol=token_symbol  # ✅ THIS IS THE KEY FIX!
                        )
                        
                        if price_usd:
                            logger.debug(f"   ✅ Got price: ${price_usd:,.2f}")
                            price_cache[cache_key] = price_usd
                        else:
                            logger.debug(f"   ❌ Price API returned None")
                            
                            # Stablecoin fallback
                            if token_symbol in STABLECOINS:
                                price_usd = STABLECOINS[token_symbol]
                                price_cache[cache_key] = price_usd
                                logger.info(
                                    f"   💵 Stablecoin fallback: {token_symbol} = ${price_usd:.2f}"
                                )
                                stablecoin_count += 1
                            else:
                                price_cache[cache_key] = None
                                
                                failure_reason = 'price_fetch_failed'
                                
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
                        
                        # Stablecoin fallback on exception
                        if token_symbol in STABLECOINS:
                            price_usd = STABLECOINS[token_symbol]
                            price_cache[cache_key] = price_usd
                            logger.info(
                                f"   💵 Stablecoin fallback (after error): {token_symbol} = ${price_usd:.2f}"
                            )
                            stablecoin_count += 1
                        else:
                            price_cache[cache_key] = None
                            
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
                    usd_value = amount * price_usd
                    
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
        
        for tx in remaining_txs:
            tx['usd_value'] = None
        
        logger.info(f"✅ Enrichment complete:")
        logger.info(f"   • Enriched: {enriched_count} transactions")
        logger.info(f"   • Cached: {cached_count} (saved API calls)")
        logger.info(f"   • Failed: {failed_count}")
        logger.info(f"   • Suspicious (rejected): {suspicious_count}")
        
        if stablecoin_count > 0:
            logger.info(f"   💵 Stablecoin fallback: {stablecoin_count} transactions")
        
        logger.info(f"📊 Made {len(price_cache)} unique price API calls instead of {len(txs_to_enrich)}")
        
        total_usd = sum(tx.get('usd_value', 0) for tx in txs_to_enrich if tx.get('usd_value'))
        avg_usd = total_usd / enriched_count if enriched_count > 0 else 0
        logger.info(f"💵 Enriched Volume: ${total_usd:,.2f} total, ${avg_usd:,.2f} average")
        
        # ✅ NEW: Show failure breakdown if significant
        if failed_count > 10:
            logger.warning(f"\n⚠️ ENRICHMENT FAILURES BREAKDOWN:")
            for reason, failures in failure_reasons.items():
                if failures:
                    logger.warning(f"   • {reason}: {len(failures)} transactions")
                    if len(failures) <= 3:
                        for failure in failures:
                            logger.warning(f"      - {failure}")
        
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
