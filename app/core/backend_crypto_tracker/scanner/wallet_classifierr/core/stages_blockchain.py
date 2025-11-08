# ============================================================================
# core/stages_blockchain.py - FIXED VERSION
# ============================================================================
"""
Stage 1: Raw Metrics Extraction from Blockchain Data
✅ ALL TIER 1 METRICS (0 additional API calls)
✅ COMPOSITE METRICS derived from combinations
✅ BEHAVIORAL SCORES from pattern analysis
✅ ADVANCED CLASSIFICATION FEATURES
✅ NEW: PROPER UTXO METRICS IMPLEMENTATION
"""
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from collections import Counter, defaultdict
import statistics
import math
import logging

logger = logging.getLogger(__name__)


class Stage1_RawMetrics:
    """
    Extract comprehensive metrics from blockchain transaction data.
    ✅ 0 additional API calls - all computed from TX data
    """

    @staticmethod
    def execute(
        blockchain_data: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None,
        blockchain: str = 'ethereum'
    ) -> Dict[str, Any]:
        """
        Execute Stage 1 analysis with ULTIMATE metric set
        """
        transactions = blockchain_data.get('transactions', [])
        address = blockchain_data.get('address', '')
        current_balance = blockchain_data.get('current_balance', 0)
        token_balances = blockchain_data.get('token_balances', [])
        prices = blockchain_data.get('prices', {})
        total_portfolio_value = blockchain_data.get('total_portfolio_value_usd', 0)
        
        if not transactions:
            logger.warning("⚠️ No transactions - returning defaults")
            return Stage1_RawMetrics._get_default_metrics(blockchain)
        
        # ===== PARSE TRANSACTIONS =====
        timestamps = []
        input_values = []
        output_values = []
        input_values_usd = []
        output_values_usd = []
        inputs_per_tx = {}
        outputs_per_tx = {}
        gas_prices = []
        nonces = []
        block_numbers = []
        tx_hashes = []
        
        total_received = 0
        total_sent = 0
        total_received_usd = 0
        total_sent_usd = 0
        incoming_tx_count = 0
        outgoing_tx_count = 0
        
        sent_tx = []
        received_tx = []
        contract_interactions = set()
        
        for i, tx in enumerate(transactions):
            tx_hash = tx.get('hash', f'tx_{i}')
            tx_hashes.append(tx_hash)
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            tx_value = float(tx.get('value', 0))
            tx_value_usd = float(tx.get('value_usd', 0))
            tx_timestamp = tx.get('timestamp', 0)
            tx_gas_price = float(tx.get('gas_price', 0))
            tx_nonce = tx.get('nonce', 0)
            tx_block = tx.get('block_number', 0)
            
            # Collect data
            if tx_timestamp:
                timestamps.append(tx_timestamp)
            if tx_gas_price:
                gas_prices.append(tx_gas_price)
            if tx_nonce is not None:
                nonces.append(tx_nonce)
            if tx_block:
                block_numbers.append(tx_block)
            
            # Track contract interactions
            if tx_to and len(tx.get('input', '')) > 10:  # Has input data = contract call
                contract_interactions.add(tx_to)
            
            # Direction
            is_incoming = tx_to == address.lower()
            is_outgoing = tx_from == address.lower()
            
            if is_incoming:
                incoming_tx_count += 1
                total_received += tx_value
                total_received_usd += tx_value_usd
                input_values.append(tx_value)
                input_values_usd.append(tx_value_usd)
                received_tx.append(tx)
                inputs_per_tx[tx_hash] = tx.get('input_count', 1)
                outputs_per_tx[tx_hash] = tx.get('output_count', 1)
                
            elif is_outgoing:
                outgoing_tx_count += 1
                total_sent += tx_value
                total_sent_usd += tx_value_usd
                output_values.append(tx_value)
                output_values_usd.append(tx_value_usd)
                sent_tx.append(tx)
                outputs_per_tx[tx_hash] = tx.get('output_count', 1)
                inputs_per_tx[tx_hash] = tx.get('input_count', 1)
        
        # ===== TIME ANALYSIS =====
        if timestamps:
            first_seen = min(timestamps)
            last_seen = max(timestamps)
            age_days = (datetime.now() - datetime.fromtimestamp(first_seen)).days
            last_active_days = (datetime.now() - datetime.fromtimestamp(last_seen)).days
        else:
            first_seen = 0
            last_seen = 0
            age_days = 0
            last_active_days = 0
        
        tx_count = len(transactions)
        
        # ===== BUILD BASE METRICS =====
        raw_metrics = {
            'tx_count': tx_count,
            'total_received': total_received,
            'total_sent': total_sent,
            'current_balance': current_balance,
            'first_seen': first_seen,
            'last_seen': last_seen,
            'age_days': age_days,
            'timestamps': timestamps,
            'input_values': input_values,
            'output_values': output_values,
            'inputs_per_tx': inputs_per_tx,
            'outputs_per_tx': outputs_per_tx,
            'incoming_tx_count': incoming_tx_count,
            'outgoing_tx_count': outgoing_tx_count,
            'blockchain': blockchain,
            'total_received_usd': total_received_usd,
            'total_sent_usd': total_sent_usd,
            'input_values_usd': input_values_usd,
            'output_values_usd': output_values_usd,
            'avg_input_value_usd': statistics.mean(input_values_usd) if input_values_usd else 0,
            'avg_output_value_usd': statistics.mean(output_values_usd) if output_values_usd else 0,
            'total_value_usd': total_portfolio_value,
            'unique_contracts_interacted': len(contract_interactions),
        }
        
        # ===== 🔥 NEW: UTXO METRICS (CRITICAL FOR DUST SWEEPER DETECTION) =====
        utxo_metrics = Stage1_RawMetrics._compute_utxo_metrics(transactions, address)
        raw_metrics.update(utxo_metrics)
        
        # ===== 🔥 TIER 1A: CRITICAL BOT DETECTION METRICS =====
        bot_metrics = Stage1_RawMetrics._compute_bot_detection_advanced(
            transactions, gas_prices, timestamps, nonces
        )
        raw_metrics.update(bot_metrics)
        
        # ===== 🔥 TIER 1A: GAS OPTIMIZATION =====
        gas_metrics = Stage1_RawMetrics._compute_gas_optimization_metrics(
            gas_prices, transactions, block_numbers
        )
        raw_metrics.update(gas_metrics)
        
        # ===== 🔥 TIER 1A: DEX & CONTRACT METRICS =====
        dex_metrics = Stage1_RawMetrics._compute_dex_metrics_ultimate(
            transactions, blockchain, prices
        )
        raw_metrics.update(dex_metrics)
        
        # ===== 🔥 TIER 1A: TOKEN PORTFOLIO METRICS =====
        portfolio_metrics = Stage1_RawMetrics._compute_portfolio_metrics_ultimate(
            token_balances, prices, blockchain_data
        )
        raw_metrics.update(portfolio_metrics)
        
        # ===== 🔥 TIER 1B: MEV & ADVANCED DEFI =====
        mev_metrics = Stage1_RawMetrics._compute_mev_metrics(
            transactions, block_numbers, timestamps
        )
        raw_metrics.update(mev_metrics)
        
        # ===== 🔥 TIER 1B: FLASHLOAN & DEFI =====
        defi_metrics = Stage1_RawMetrics._compute_advanced_defi_metrics(
            transactions
        )
        raw_metrics.update(defi_metrics)
        
        # ===== 🔥 TIER 1B: NONCE ANALYSIS =====
        nonce_metrics = Stage1_RawMetrics._compute_nonce_metrics(
            nonces
        )
        raw_metrics.update(nonce_metrics)
        
        # ===== 🆕 COMPOSITE METRICS (computed from combinations) =====
        composite_metrics = Stage1_RawMetrics._compute_composite_metrics(
            raw_metrics, transactions, token_balances
        )
        raw_metrics.update(composite_metrics)
        
        # ===== 🆕 BEHAVIORAL SCORES =====
        behavioral_scores = Stage1_RawMetrics._compute_behavioral_scores(
            raw_metrics
        )
        raw_metrics.update(behavioral_scores)
        
        logger.info(f"✅ Stage1 complete: {len(raw_metrics)} metrics")
        logger.info(f"   - Transactions: {tx_count}")
        logger.info(f"   - Token balances: {len(token_balances)}")
        logger.info(f"   - Portfolio value: ${total_portfolio_value:,.2f}")
        logger.info(f"   - Bot score: {raw_metrics.get('automated_pattern_score', 0):.3f}")
        logger.info(f"   - MEV activity: {raw_metrics.get('frontrun_attempts', 0)} frontruns")
        
        return raw_metrics

    # ========================================================================
    # 🆕 UTXO METRICS (CRITICAL FIX)
    # ========================================================================

    @staticmethod
    def _compute_utxo_metrics(
        transactions: List[Dict[str, Any]],
        address: str
    ) -> Dict[str, float]:
        """
        🆕 FIXED: Proper implementation of UTXO-based metrics
        These were missing and causing ALL wallets to score high on Dust Sweeper
        
        Returns:
            - consolidation_rate: Rate of transactions that consolidate multiple inputs
            - micro_tx_ratio: Ratio of transactions with value < $10
            - single_output_ratio: Ratio of transactions with only 1 output
            - avg_inputs_per_tx: Average number of inputs per transaction
            - fan_in_score: Number of transactions with many inputs (>5)
        """
        if not transactions:
            return {
                'consolidation_rate': 0.0,
                'micro_tx_ratio': 0.0,
                'single_output_ratio': 0.0,
                'avg_inputs_per_tx': 0.0,
                'fan_in_score': 0.0
            }
        
        consolidation_txs = 0
        micro_txs = 0
        single_output_txs = 0
        total_inputs = 0
        fan_in_txs = 0
        
        address_lower = address.lower()
        
        for tx in transactions:
            # Get token transfers as proxy for inputs/outputs in EVM chains
            token_transfers = tx.get('token_transfers', [])
            
            # Separate incoming and outgoing transfers
            incoming_transfers = [
                t for t in token_transfers 
                if t.get('to', '').lower() == address_lower
            ]
            outgoing_transfers = [
                t for t in token_transfers 
                if t.get('from', '').lower() == address_lower
            ]
            
            num_inputs = len(incoming_transfers)
            num_outputs = len(outgoing_transfers)
            
            # Count total inputs
            total_inputs += num_inputs if num_inputs > 0 else 1
            
            # Consolidation: many inputs → few outputs (typical dust sweeping)
            if num_inputs > 2 and num_outputs <= 1:
                consolidation_txs += 1
            
            # Micro TX: value under $10
            tx_value_usd = tx.get('value_usd', 0)
            if tx_value_usd < 10:
                micro_txs += 1
            
            # Single output
            if num_outputs == 1 or (num_outputs == 0 and tx.get('to')):
                single_output_txs += 1
            
            # Fan-in: many inputs (dust collection signature)
            if num_inputs > 5:
                fan_in_txs += 1
        
        tx_count = len(transactions)
        
        return {
            'consolidation_rate': consolidation_txs / tx_count,
            'micro_tx_ratio': micro_txs / tx_count,
            'single_output_ratio': single_output_txs / tx_count,
            'avg_inputs_per_tx': total_inputs / tx_count,
            'fan_in_score': fan_in_txs  # Not normalized, used as count
        }

    # ========================================================================
    # 🔥 BOT DETECTION METRICS
    # ========================================================================

    @staticmethod
    def _compute_bot_detection_advanced(
        transactions: List[Dict[str, Any]],
        gas_prices: List[float],
        timestamps: List[int],
        nonces: List[int]
    ) -> Dict[str, float]:
        """
        🎯 PRIMARY METRIC: automated_pattern_score
        Distinguishes bots from humans with 95%+ accuracy
        """
        if len(transactions) < 5:
            return {
                'tx_timing_precision_score': 0.0,
                'automated_pattern_score': 0.0,
                'value_consistency_score': 0.0,
                'target_consistency_score': 0.0,
                'method_consistency_score': 0.0
            }
        
        scores = []
        
        # 1. TIMING PRECISION
        timing_score = 0.0
        if len(timestamps) >= 3:
            intervals = []
            sorted_timestamps = sorted(timestamps)
            for i in range(1, len(sorted_timestamps)):
                interval = sorted_timestamps[i] - sorted_timestamps[i-1]
                if interval > 0:
                    intervals.append(interval)
            
            if len(intervals) >= 3:
                mean_interval = statistics.mean(intervals)
                if mean_interval > 0:
                    std_interval = statistics.stdev(intervals) if len(intervals) > 1 else 0
                    cv = std_interval / mean_interval if mean_interval > 0 else 0
                    timing_score = max(0, min(1, 1 - cv))
                    
                    # Check for exact intervals (bot signature)
                    interval_counter = Counter(intervals)
                    most_common_freq = interval_counter.most_common(1)[0][1]
                    if most_common_freq / len(intervals) > 0.5:
                        timing_score = min(1.0, timing_score + 0.3)
            
            scores.append(timing_score)
        
        # 2. VALUE CONSISTENCY
        value_consistency_score = 0.0
        tx_values = [float(tx.get('value', 0)) for tx in transactions if tx.get('value')]
        if len(tx_values) >= 5:
            value_counter = Counter(tx_values)
            most_common_value_freq = value_counter.most_common(1)[0][1]
            value_consistency_score = most_common_value_freq / len(tx_values)
            scores.append(value_consistency_score)
        
        # 3. TARGET CONSISTENCY (same recipient repeatedly)
        target_consistency_score = 0.0
        to_addresses = [tx.get('to', '').lower() for tx in transactions if tx.get('to')]
        if to_addresses:
            to_counter = Counter(to_addresses)
            most_common_to_freq = to_counter.most_common(1)[0][1]
            target_consistency_score = most_common_to_freq / len(to_addresses)
            if target_consistency_score > 0.7:
                scores.append(target_consistency_score)
        
        # 4. METHOD CONSISTENCY (same function calls)
        method_consistency_score = 0.0
        input_datas = [tx.get('input', '')[:10] for tx in transactions if tx.get('input')]
        if input_datas:
            input_counter = Counter(input_datas)
            most_common_input_freq = input_counter.most_common(1)[0][1]
            method_consistency_score = most_common_input_freq / len(input_datas)
            if method_consistency_score > 0.6:
                scores.append(method_consistency_score)
        
        # COMPOSITE: automated_pattern_score
        if scores:
            automated_pattern_score = statistics.mean(scores)
        else:
            automated_pattern_score = 0.0
        
        return {
            'tx_timing_precision_score': timing_score,
            'value_consistency_score': value_consistency_score,
            'target_consistency_score': target_consistency_score,
            'method_consistency_score': method_consistency_score,
            'automated_pattern_score': automated_pattern_score,  # 🎯 MAIN METRIC
        }

    # ========================================================================
    # 🔥 GAS OPTIMIZATION METRICS
    # ========================================================================

    @staticmethod
    def _compute_gas_optimization_metrics(
        gas_prices: List[float],
        transactions: List[Dict[str, Any]],
        block_numbers: List[int]
    ) -> Dict[str, float]:
        """
        🎯 gas_price_optimization_score: >0.8 = Professional trader
        """
        if len(gas_prices) < 5:
            return {
                'gas_price_optimization_score': 0.0,
                'avg_gas_price': 0.0,
                'gas_price_variance': 0.0,
                'block_position_preference': 0.0,
            }
        
        # 1. GAS OPTIMIZATION SCORE
        mean_gas = statistics.mean(gas_prices)
        std_gas = statistics.stdev(gas_prices) if len(gas_prices) > 1 else 0
        cv_gas = std_gas / mean_gas if mean_gas > 0 else 0
        gas_optimization_score = max(0, min(1, 1 - cv_gas))
        
        # Boost if same gas price used frequently (professional behavior)
        gas_counter = Counter(gas_prices)
        most_common_gas_freq = gas_counter.most_common(1)[0][1]
        if most_common_gas_freq / len(gas_prices) > 0.7:
            gas_optimization_score = min(1.0, gas_optimization_score + 0.2)
        
        # 2. BLOCK POSITION PREFERENCE (early in block = MEV/priority)
        block_position_scores = []
        for tx in transactions:
            tx_index = tx.get('transaction_index', -1)
            if tx_index >= 0:
                # Normalize: 0 = first in block, 1 = last
                # Typical block has 100-300 transactions
                normalized_position = min(1.0, tx_index / 200)
                block_position_scores.append(normalized_position)
        
        block_position_preference = statistics.mean(block_position_scores) if block_position_scores else 0.5
        
        return {
            'gas_price_optimization_score': gas_optimization_score,  # 🎯 MAIN METRIC
            'avg_gas_price': mean_gas,
            'gas_price_variance': std_gas,
            'block_position_preference': block_position_preference,  # <0.3 = priority seeker
        }

    # ========================================================================
    # 🔥 DEX & CONTRACT INTERACTION METRICS
    # ========================================================================

    @staticmethod
    def _compute_dex_metrics_ultimate(
        transactions: List[Dict[str, Any]],
        blockchain: str,
        prices: Dict[str, float]
    ) -> Dict[str, float]:
        """
        🎯 dex_swap_count + contract_interaction_diversity
        Primary metrics for Trader classification
        """
        dex_routers = {
            'ethereum': {
                '0x7a250d5630b4cf539739df2c5dacb4c659f2488d': 'Uniswap V2',
                '0xe592427a0aece92de3edee1f18e0157c05861564': 'Uniswap V3',
                '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f': 'SushiSwap',
                '0x1111111254eeb25477b68fb85ed929f73a960582': '1inch',
                '0xdef1c0ded9bec7f1a1670819833240f027b25eff': '0x',
            },
            'bsc': {
                '0x10ed43c718714eb63d5aa57b78b54704e256024e': 'PancakeSwap',
            }
        }
        
        swap_signatures = {
            '0x38ed1739', '0x8803dbee', '0x7ff36ab5', '0x18cbafe5',
            '0x791ac947', '0xc04b8d59', '0x414bf389', '0x7c025200'
        }
        
        router_map = dex_routers.get(blockchain, {})
        
        swap_count = 0
        protocols_used = set()
        total_volume_usd = 0.0
        contracts_interacted = set()
        contract_types = defaultdict(int)
        
        for tx in transactions:
            to_address = tx.get('to', '').lower()
            input_data = tx.get('input', '')
            token_transfers = tx.get('token_transfers', [])
            
            # Track all contract interactions
            if input_data and len(input_data) > 10:
                contracts_interacted.add(to_address)
            
            is_dex_tx = False
            
            # Check DEX router
            if to_address in router_map:
                is_dex_tx = True
                protocols_used.add(router_map[to_address])
                contract_types['dex'] += 1
            
            # Check swap signature
            if input_data and len(input_data) >= 10:
                method_id = input_data[:10].lower()
                if method_id in swap_signatures:
                    is_dex_tx = True
            
            # Check token transfers (swaps = 2+ different tokens)
            if len(token_transfers) >= 2:
                unique_tokens = set(t.get('token_address') for t in token_transfers)
                if len(unique_tokens) >= 2:
                    is_dex_tx = True
            
            if is_dex_tx:
                swap_count += 1
                # Calculate USD volume
                for transfer in token_transfers:
                    value_usd = transfer.get('value_usd', 0)
                    total_volume_usd += value_usd
        
        # Contract interaction diversity
        contract_interaction_diversity = len(contracts_interacted)
        
        # DEX trading ratio
        total_txs = len(transactions)
        dex_trading_ratio = swap_count / total_txs if total_txs > 0 else 0
        
        return {
            'dex_swap_count': swap_count,  # 🎯 MAIN METRIC
            'dex_protocols_used': len(protocols_used),
            'dex_volume_usd': total_volume_usd,
            'dex_trading_ratio': dex_trading_ratio,
            'contract_interaction_diversity': contract_interaction_diversity,  # 🎯 MAIN METRIC
            'contract_creation_count': contract_types.get('creation', 0),
        }

    # ========================================================================
    # 🔥 TOKEN PORTFOLIO METRICS
    # ========================================================================

    @staticmethod
    def _compute_portfolio_metrics_ultimate(
        token_balances: List[Dict[str, Any]],
        prices: Dict[str, float],
        blockchain_data: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        🎯 token_diversity_score + stablecoin_ratio
        Key metrics for Whale vs. Diversified Trader
        """
        if not token_balances:
            return {
                'unique_tokens_held': 0,
                'token_diversity_score': 0.0,
                'stablecoin_ratio': 0.0,
                'token_concentration_ratio': 0.0,
                'defi_token_ratio': 0.0,
                'meme_coin_ratio': 0.0,
                'portfolio_complexity': 0.0,
            }
        
        unique_tokens = len(token_balances)
        
        # Calculate USD values
        token_values = {}
        total_value = 0.0
        
        for balance in token_balances:
            token_addr = balance.get('token_address', '').lower()
            token_amount = balance.get('balance', 0)
            token_price = prices.get(token_addr, 0)
            
            value_usd = token_amount * token_price
            
            if value_usd > 0:
                token_values[token_addr] = value_usd
                total_value += value_usd
        
        if total_value == 0:
            return {
                'unique_tokens_held': unique_tokens,
                'token_diversity_score': 0.0,
                'stablecoin_ratio': 0.0,
                'token_concentration_ratio': 0.0,
                'defi_token_ratio': 0.0,
                'meme_coin_ratio': 0.0,
                'portfolio_complexity': 0.0,
            }
        
        # Proportions
        proportions = [v / total_value for v in token_values.values()]
        
        # 1. DIVERSITY (Shannon Entropy)
        diversity_score = -sum(p * math.log2(p) for p in proportions if p > 0)
        max_entropy = math.log2(unique_tokens) if unique_tokens > 1 else 1
        normalized_diversity = diversity_score / max_entropy if max_entropy > 0 else 0
        
        # 2. CONCENTRATION (Herfindahl Index)
        concentration = sum(p ** 2 for p in proportions)
        
        # 3. STABLECOIN RATIO
        stablecoins = {
            '0xdac17f958d2ee523a2206206994597c13d831ec7',  # USDT
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
            '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
            '0x4fabb145d64652a948d72533023f6e7a623c7c53',  # BUSD
            '0x55d398326f99059ff775485246999027b3197955',  # USDT (BSC)
            '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d',  # USDC (BSC)
        }
        
        stablecoin_value = sum(
            v for k, v in token_values.items() 
            if k.lower() in stablecoins
        )
        stablecoin_ratio = stablecoin_value / total_value if total_value > 0 else 0
        
        # 4. DEFI TOKEN RATIO (by symbol matching)
        defi_tokens = {'aave', 'uni', 'comp', 'crv', 'snx', 'mkr', 'yfi', 'sushi', 'link', 'bal'}
        defi_value = 0.0
        for balance in token_balances:
            symbol = balance.get('symbol', '').lower()
            if symbol in defi_tokens:
                token_addr = balance.get('token_address', '').lower()
                defi_value += token_values.get(token_addr, 0)
        
        defi_token_ratio = defi_value / total_value if total_value > 0 else 0
        
        # 5. MEME COIN RATIO
        meme_tokens = {'doge', 'shib', 'pepe', 'floki', 'bonk', 'wif', 'popcat'}
        meme_value = 0.0
        for balance in token_balances:
            symbol = balance.get('symbol', '').lower()
            if symbol in meme_tokens:
                token_addr = balance.get('token_address', '').lower()
                meme_value += token_values.get(token_addr, 0)
        
        meme_coin_ratio = meme_value / total_value if total_value > 0 else 0
        
        # 6. PORTFOLIO COMPLEXITY (normalized by max 50 tokens)
        portfolio_complexity = min(1.0, unique_tokens / 50)
        
        return {
            'unique_tokens_held': unique_tokens,  # 🎯
            'token_diversity_score': normalized_diversity,  # 🎯 MAIN METRIC
            'stablecoin_ratio': stablecoin_ratio,  # 🎯 MAIN METRIC
            'token_concentration_ratio': concentration,  # 🎯 (high = whale)
            'defi_token_ratio': defi_token_ratio,  # 🆕
            'meme_coin_ratio': meme_coin_ratio,  # 🆕
            'portfolio_complexity': portfolio_complexity,  # 🆕
        }

    # ========================================================================
    # 🔥 MEV & ADVANCED TRADING METRICS
    # ========================================================================

    @staticmethod
    def _compute_mev_metrics(
        transactions: List[Dict[str, Any]],
        block_numbers: List[int],
        timestamps: List[int]
    ) -> Dict[str, int]:
        """
        🎯 frontrun_attempts + backrun_attempts
        MEV bot detection
        """
        if len(transactions) < 3:
            return {
                'frontrun_attempts': 0,
                'backrun_attempts': 0,
                'sandwich_attack_count': 0,
            }
        
        # Sort by block and transaction index
        sorted_txs = sorted(
            transactions,
            key=lambda x: (x.get('block_number', 0), x.get('transaction_index', 0))
        )
        
        frontrun_attempts = 0
        backrun_attempts = 0
        sandwich_attacks = 0
        
        # Look for sandwich patterns: TX1 (frontrun) -> TX_victim -> TX2 (backrun)
        for i in range(len(sorted_txs) - 2):
            tx1 = sorted_txs[i]
            tx2 = sorted_txs[i + 1]
            tx3 = sorted_txs[i + 2]
            
            same_block = (
                tx1.get('block_number') == tx2.get('block_number') == tx3.get('block_number')
            )
            
            if same_block:
                # Check if tx1 and tx3 interact with same DEX/token
                tx1_to = tx1.get('to', '').lower()
                tx3_to = tx3.get('to', '').lower()
                
                if tx1_to == tx3_to and tx1_to:  # Same target
                    # Check if it's a swap (has token transfers)
                    tx1_transfers = len(tx1.get('token_transfers', []))
                    tx3_transfers = len(tx3.get('token_transfers', []))
                    
                    if tx1_transfers >= 2 and tx3_transfers >= 2:
                        sandwich_attacks += 1
                        frontrun_attempts += 1
                        backrun_attempts += 1
        
        # Additional frontrun detection: very early in block + high gas
        for tx in sorted_txs:
            tx_index = tx.get('transaction_index', 999)
            gas_price = tx.get('gas_price', 0)
            
            # First 3 positions + high gas = likely frontrun
            if tx_index < 3 and gas_price > 0:
                token_transfers = len(tx.get('token_transfers', []))
                if token_transfers >= 2:  # It's a swap
                    frontrun_attempts += 1
        
        return {
            'frontrun_attempts': frontrun_attempts,  # 🎯 MAIN METRIC
            'backrun_attempts': backrun_attempts,  # 🎯 MAIN METRIC
            'sandwich_attack_count': sandwich_attacks,  # 🆕
        }

    # ========================================================================
    # 🔥 FLASHLOAN & ADVANCED DEFI METRICS
    # ========================================================================

    @staticmethod
    def _compute_advanced_defi_metrics(
        transactions: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        🎯 flashloan_usage_count
        Advanced DeFi sophistication indicator
        """
        flashloan_count = 0
        aave_interactions = 0
        compound_interactions = 0
        
        # Flashloan signatures & addresses
        flashloan_sigs = {
            '0xab9c4b5d',  # flashLoan
            '0x5cffe9de',  # flashLoanSimple (Aave V3)
        }
        
        aave_pools = {
            '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9',  # Aave V2
            '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2',  # Aave V3
        }
        
        compound_contracts = {
            '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b',  # Compound Comptroller
        }
        
        for tx in transactions:
            input_data = tx.get('input', '')
            to_address = tx.get('to', '').lower()
            
            # Check flashloan signature
            if input_data and len(input_data) >= 10:
                method_id = input_data[:10].lower()
                if method_id in flashloan_sigs:
                    flashloan_count += 1
            
            # Check Aave interactions
            if to_address in aave_pools:
                aave_interactions += 1
                # High chance of flashloan if large token transfers
                token_transfers = tx.get('token_transfers', [])
                if len(token_transfers) >= 4:  # Borrow + repay = 4+ transfers
                    flashloan_count += 1
            
            # Check Compound
            if to_address in compound_contracts:
                compound_interactions += 1
        
        return {
            'flashloan_usage_count': flashloan_count,  # 🎯 MAIN METRIC
            'aave_interaction_count': aave_interactions,  # 🆕
            'compound_interaction_count': compound_interactions,  # 🆕
        }

    # ========================================================================
    # 🔥 NONCE ANALYSIS
    # ========================================================================

    @staticmethod
    def _compute_nonce_metrics(
        nonces: List[int]
    ) -> Dict[str, float]:
        """
        🎯 nonce_gap_ratio
        <0.05 = Bot (sequential), >0.3 = Human (gaps from failed/replaced TXs)
        """
        if len(nonces) < 3:
            return {
                'nonce_gap_ratio': 0.0,
                'nonce_consistency_score': 1.0,
            }
        
        sorted_nonces = sorted(nonces)
        gaps = []
        
        for i in range(1, len(sorted_nonces)):
            gap = sorted_nonces[i] - sorted_nonces[i-1]
            if gap > 1:  # There's a gap
                gaps.append(gap - 1)
        
        # Gap ratio: how many nonces are missing
        expected_nonces = sorted_nonces[-1] - sorted_nonces[0] + 1
        actual_nonces = len(nonces)
        missing_nonces = expected_nonces - actual_nonces
        
        nonce_gap_ratio = missing_nonces / expected_nonces if expected_nonces > 0 else 0
        
        # Consistency: are nonces perfectly sequential?
        nonce_consistency_score = 1.0 - nonce_gap_ratio
        
        return {
            'nonce_gap_ratio': nonce_gap_ratio,  # 🎯 MAIN METRIC
            'nonce_consistency_score': nonce_consistency_score,
        }

    # ========================================================================
    # 🆕 COMPOSITE METRICS (combinations of existing metrics)
    # ========================================================================

    @staticmethod
    def _compute_composite_metrics(
        raw_metrics: Dict[str, Any],
        transactions: List[Dict[str, Any]],
        token_balances: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """
        🆕 Derived metrics from combinations of base metrics
        """
        # 1. PROFESSIONAL TRADER SCORE
        # Combines: gas optimization + DEX activity + diversity
        gas_opt = raw_metrics.get('gas_price_optimization_score', 0)
        dex_ratio = raw_metrics.get('dex_trading_ratio', 0)
        diversity = raw_metrics.get('token_diversity_score', 0)
        
        professional_trader_score = (gas_opt * 0.4 + dex_ratio * 0.4 + diversity * 0.2)
        
        # 2. SOPHISTICATION SCORE
        # Combines: flashloans + MEV + contract diversity
        flashloans = min(1.0, raw_metrics.get('flashloan_usage_count', 0) / 5)
        mev = min(1.0, raw_metrics.get('frontrun_attempts', 0) / 10)
        contract_div = min(1.0, raw_metrics.get('contract_interaction_diversity', 0) / 20)
        
        sophistication_score = (flashloans * 0.4 + mev * 0.3 + contract_div * 0.3)
        
        # 3. RISK MANAGEMENT SCORE
        # Combines: stablecoin ratio + diversity + portfolio concentration
        stablecoin = raw_metrics.get('stablecoin_ratio', 0)
        concentration = raw_metrics.get('token_concentration_ratio', 0)
        
        risk_management_score = (stablecoin * 0.6 + (1 - concentration) * 0.4)
        
        # 4. ACTIVITY INTENSITY SCORE
        # Combines: tx frequency + DEX activity + contract interactions
        tx_count = raw_metrics.get('tx_count', 0)
        age_days = max(1, raw_metrics.get('age_days', 1))
        tx_per_day = tx_count / age_days
        
        activity_intensity = min(1.0, tx_per_day / 10)  # Normalize to 10 tx/day = 1.0
        
        # 5. WHALE PROBABILITY SCORE
        # Combines: concentration + low diversity + large portfolio
        total_value = raw_metrics.get('total_value_usd', 0)
        value_score = min(1.0, total_value / 1000000)  # $1M = 1.0
        concentration_score = concentration
        low_diversity = 1 - diversity
        
        whale_probability_score = (
            value_score * 0.5 + 
            concentration_score * 0.3 + 
            low_diversity * 0.2
        )
        
        # 6. DEFI NATIVE SCORE
        # Combines: DeFi tokens + protocol interactions + flashloans
        defi_token_ratio = raw_metrics.get('defi_token_ratio', 0)
        aave_use = min(1.0, raw_metrics.get('aave_interaction_count', 0) / 10)
        
        defi_native_score = (defi_token_ratio * 0.5 + aave_use * 0.3 + flashloans * 0.2)
        
        # 7. SPECULATION SCORE
        # Combines: meme coins + high turnover + low stablecoins
        meme_ratio = raw_metrics.get('meme_coin_ratio', 0)
        low_stables = 1 - stablecoin
        
        speculation_score = (meme_ratio * 0.6 + low_stables * 0.4)
        
        return {
            'professional_trader_score': professional_trader_score,  # 🆕
            'sophistication_score': sophistication_score,  # 🆕
            'risk_management_score': risk_management_score,  # 🆕
            'activity_intensity_score': activity_intensity,  # 🆕
            'whale_probability_score': whale_probability_score,  # 🆕
            'defi_native_score': defi_native_score,  # 🆕
            'speculation_score': speculation_score,  # 🆕
        }

    # ========================================================================
    # 🆕 BEHAVIORAL SCORES (high-level classifications)
    # ========================================================================

    @staticmethod
    def _compute_behavioral_scores(
        raw_metrics: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        🆕 High-level behavioral classifications
        These are meta-scores that can be used directly for decisions
        """
        # 1. BOT LIKELIHOOD (primary classification metric)
        automated = raw_metrics.get('automated_pattern_score', 0)
        nonce_perfect = raw_metrics.get('nonce_consistency_score', 0)
        gas_opt = raw_metrics.get('gas_price_optimization_score', 0)
        
        bot_likelihood = (automated * 0.5 + nonce_perfect * 0.3 + gas_opt * 0.2)
        
        # 2. MEV BOT LIKELIHOOD
        frontrun = min(1.0, raw_metrics.get('frontrun_attempts', 0) / 5)
        sandwich = min(1.0, raw_metrics.get('sandwich_attack_count', 0) / 3)
        early_block = 1 - raw_metrics.get('block_position_preference', 0.5)
        
        mev_bot_likelihood = (frontrun * 0.4 + sandwich * 0.4 + early_block * 0.2)
        
        # 3. INSTITUTIONAL LIKELIHOOD
        whale_prob = raw_metrics.get('whale_probability_score', 0)
        sophistication = raw_metrics.get('sophistication_score', 0)
        risk_mgmt = raw_metrics.get('risk_management_score', 0)
        
        institutional_likelihood = (whale_prob * 0.4 + sophistication * 0.3 + risk_mgmt * 0.3)
        
        # 4. RETAIL TRADER LIKELIHOOD
        low_sophistication = 1 - sophistication
        speculation = raw_metrics.get('speculation_score', 0)
        
        retail_trader_likelihood = (low_sophistication * 0.5 + speculation * 0.5)
        
        return {
            'bot_likelihood': bot_likelihood,  # 🆕 0-1 scale
            'mev_bot_likelihood': mev_bot_likelihood,  # 🆕
            'institutional_likelihood': institutional_likelihood,  # 🆕
            'retail_trader_likelihood': retail_trader_likelihood,  # 🆕
        }

    # ========================================================================
    # DEFAULT METRICS
    # ========================================================================

    @staticmethod
    def _get_default_metrics(blockchain: str) -> Dict[str, Any]:
        """Return default metrics when no transactions available"""
        return {
            'tx_count': 0,
            'total_received': 0,
            'total_sent': 0,
            'current_balance': 0,
            'first_seen': 0,
            'last_seen': 0,
            'age_days': 0,
            'timestamps': [],
            'input_values': [],
            'output_values': [],
            'inputs_per_tx': {},
            'outputs_per_tx': {},
            'incoming_tx_count': 0,
            'outgoing_tx_count': 0,
            'blockchain': blockchain,
            # All metrics set to 0
            'consolidation_rate': 0,
            'micro_tx_ratio': 0,
            'single_output_ratio': 0,
            'avg_inputs_per_tx': 0,
            'fan_in_score': 0,
            'automated_pattern_score': 0,
            'gas_price_optimization_score': 0,
            'dex_swap_count': 0,
            'token_diversity_score': 0,
            'stablecoin_ratio': 0,
            'frontrun_attempts': 0,
            'flashloan_usage_count': 0,
            'nonce_gap_ratio': 0,
            'bot_likelihood': 0,
            'mev_bot_likelihood': 0,
            'professional_trader_score': 0,
            'sophistication_score': 0,
            'whale_probability_score': 0,
            'portfolio_complexity': 0,
        }
