# ============================================================================
# core/stages_blockchain.py - ENHANCED WITH PHASE 1 METRICS
# ============================================================================
"""
Stage 1: Raw Metrics Extraction from Blockchain Data
Enhanced with 10 New Phase 1 Metrics

NEW FEATURES:
✅ Portfolio Metrics (4): Token diversity, concentration, stablecoin ratio
✅ DEX Metrics (3): Swap count, protocols used, volume
✅ Bot Detection (3): Timing precision, gas optimization, automated patterns

⚡ Zero external APIs - uses existing blockchain data only
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from collections import Counter, defaultdict
import statistics
import math
import logging

logger = logging.getLogger(__name__)


class Stage1_RawMetrics:
    """
    Extract raw metrics from blockchain transaction data.
    
    Input: Raw blockchain data (transactions, balances, etc.)
    Output: Basic numerical metrics including 10 new Phase 1 metrics
    """
    
    @staticmethod
    def execute(blockchain_data: Dict[str, Any], config: Optional[Dict[str, Any]] = None, blockchain: str = 'ethereum') -> Dict[str, Any]:
        txs = blockchain_data.get('txs', [])
        logger.info(f"🔗 Processing {len(txs)} transactions for raw metrics")
        
        if not txs:
            logger.warning("⚠️ No transactions found - all metrics will be default values")
            return self._get_default_metrics()
        
        # Log sample transaction data
        sample_tx = txs[0]
        logger.info(f"📝 Sample transaction keys: {list(sample_tx.keys())}")
        logger.info(f"📝 Sample transaction: {sample_tx}")
        
        # Log which metrics will be calculated
        metrics_to_calculate = [
            'total_tx_count', 'sent_tx_count', 'received_tx_count',
            'total_value_sent', 'total_value_received', 'current_balance',
            'unique_senders', 'unique_receivers', 'age_days',
            'last_active_days', 'avg_inputs_per_tx', 'avg_outputs_per_tx',
            'avg_gas_price', 'total_gas_used'
        ]
        
        logger.info(f"🧮 Will calculate {len(metrics_to_calculate)} raw metrics: {metrics_to_calculate}")
        """
        Execute Stage 1 analysis with enhanced Phase 1 metrics.
        
        Args:
            blockchain_data: Raw transaction data from blockchain
            config: Optional configuration parameters
            blockchain: Blockchain name for chain-specific logic
            
        Returns:
            Dictionary of raw metrics (existing + 10 new Phase 1 metrics)
        """
        transactions = blockchain_data.get('transactions', [])
        balances = blockchain_data.get('balances', {})
        current_balance = blockchain_data.get('current_balance', 0)
        address = blockchain_data.get('address', '')
        
        # ===== EXISTING METRICS =====
        
        # Basic counts
        total_tx = len(transactions)
        sent_tx = [tx for tx in transactions if tx.get('from', '').lower() == address.lower()]
        received_tx = [tx for tx in transactions if tx.get('to', '').lower() == address.lower()]
        
        # Transaction values
        tx_values = [float(tx.get('value', 0)) for tx in transactions]
        sent_values = [float(tx.get('value', 0)) for tx in sent_tx]
        received_values = [float(tx.get('value', 0)) for tx in received_tx]
        
        # Timestamps
        timestamps = [tx.get('timestamp') for tx in transactions if tx.get('timestamp')]
        
        # Age calculation
        if timestamps:
            first_tx = min(timestamps)
            last_tx = max(timestamps)
            age_days = (datetime.now() - datetime.fromtimestamp(first_tx)).days
            last_active_days = (datetime.now() - datetime.fromtimestamp(last_tx)).days
        else:
            age_days = 0
            last_active_days = 0
        
        # Input/Output analysis
        total_inputs = sum(tx.get('input_count', 1) for tx in transactions)
        total_outputs = sum(tx.get('output_count', 1) for tx in transactions)
        
        # Gas analysis
        gas_prices = [float(tx.get('gas_price', 0)) for tx in transactions if tx.get('gas_price')]
        
        # Unique addresses
        unique_senders = len(set(tx.get('from', '') for tx in transactions if tx.get('from')))
        unique_receivers = len(set(tx.get('to', '') for tx in transactions if tx.get('to')))
        
        raw_metrics = {
            # Basic
            'total_tx_count': total_tx,
            'sent_tx_count': len(sent_tx),
            'received_tx_count': len(received_tx),
            'age_days': age_days,
            'last_active_days': last_active_days,
            
            # Values
            'total_value_transacted': sum(tx_values),
            'total_value_sent': sum(sent_values),
            'total_value_received': sum(received_values),
            'avg_tx_value': statistics.mean(tx_values) if tx_values else 0,
            'median_tx_value': statistics.median(tx_values) if tx_values else 0,
            'current_balance': current_balance,
            
            # Input/Output
            'total_inputs': total_inputs,
            'total_outputs': total_outputs,
            'avg_inputs_per_tx': total_inputs / total_tx if total_tx > 0 else 0,
            'avg_outputs_per_tx': total_outputs / total_tx if total_tx > 0 else 0,
            
            # Gas
            'avg_gas_price': statistics.mean(gas_prices) if gas_prices else 0,
            'median_gas_price': statistics.median(gas_prices) if gas_prices else 0,
            
            # Network
            'unique_senders': unique_senders,
            'unique_receivers': unique_receivers,
            'in_degree': unique_senders,
            'out_degree': unique_receivers,
            
            # Timestamps
            'first_tx_timestamp': min(timestamps) if timestamps else 0,
            'last_tx_timestamp': max(timestamps) if timestamps else 0,
        }
        
        # ===== 🆕 PHASE 1 METRICS (10 NEW) =====
        
        # 📦 PORTFOLIO METRICS (4 new)
        portfolio_metrics = Stage1_RawMetrics._compute_portfolio_metrics(
            blockchain_data, transactions
        )
        raw_metrics.update(portfolio_metrics)
        
        # 🔄 DEX METRICS (3 new) - KILLER FEATURE
        dex_metrics = Stage1_RawMetrics._compute_dex_metrics(
            transactions, blockchain
        )
        raw_metrics.update(dex_metrics)
        
        # 🤖 BOT DETECTION METRICS (3 new)
        bot_metrics = Stage1_RawMetrics._compute_bot_detection_metrics(
            transactions, gas_prices, timestamps
        )
        raw_metrics.update(bot_metrics)
        logger.info(f"✅ Raw metrics calculated: {list(raw_metrics.keys())}")
        for key, value in raw_metrics.items():
            logger.debug(f"  {key}: {value}")
    
    return raw_metrics
    
    # ========================================================================
    # 🆕 NEW: PORTFOLIO METRICS
    # ========================================================================
    
    @staticmethod
    def _compute_portfolio_metrics(
        blockchain_data: Dict[str, Any],
        transactions: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """
        Compute token portfolio diversity metrics.
        
        Uses: token_transfers from blockchain_data
        
        Returns:
            - unique_tokens_held: Count of different tokens in portfolio
            - token_diversity_score: Shannon entropy of token distribution (0-1)
            - stablecoin_ratio: Percentage of portfolio in stablecoins (0-1)
            - token_concentration_ratio: Herfindahl index (0=diverse, 1=concentrated)
        """
        token_transfers = blockchain_data.get('token_transfers', [])
        balances = blockchain_data.get('token_balances', {})
        
        if not balances and not token_transfers:
            return {
                'unique_tokens_held': 0,
                'token_diversity_score': 0.0,
                'stablecoin_ratio': 0.0,
                'token_concentration_ratio': 0.0
            }
        
        # Extract unique tokens
        if balances:
            # Use actual balances if available
            token_holdings = {
                token: float(balance) 
                for token, balance in balances.items() 
                if float(balance) > 0
            }
        else:
            # Estimate from transfers
            token_holdings = defaultdict(float)
            for transfer in token_transfers:
                token = transfer.get('token_address', '').lower()
                amount = float(transfer.get('value', 0))
                if transfer.get('to', '').lower() == blockchain_data.get('address', '').lower():
                    token_holdings[token] += amount
                elif transfer.get('from', '').lower() == blockchain_data.get('address', '').lower():
                    token_holdings[token] -= amount
            
            # Remove negative balances
            token_holdings = {k: v for k, v in token_holdings.items() if v > 0}
        
        # Count unique tokens
        unique_tokens = len(token_holdings)
        
        if unique_tokens == 0:
            return {
                'unique_tokens_held': 0,
                'token_diversity_score': 0.0,
                'stablecoin_ratio': 0.0,
                'token_concentration_ratio': 0.0
            }
        
        # Calculate total value
        total_value = sum(token_holdings.values())
        
        if total_value == 0:
            return {
                'unique_tokens_held': unique_tokens,
                'token_diversity_score': 0.0,
                'stablecoin_ratio': 0.0,
                'token_concentration_ratio': 0.0
            }
        
        # Calculate proportions
        proportions = [v / total_value for v in token_holdings.values()]
        
        # Diversity Score (Shannon Entropy)
        # Higher = more diverse portfolio
        diversity_score = -sum(p * math.log2(p) for p in proportions if p > 0)
        
        # Normalize to [0, 1]
        max_entropy = math.log2(unique_tokens) if unique_tokens > 1 else 1
        normalized_diversity = diversity_score / max_entropy if max_entropy > 0 else 0
        
        # Concentration Ratio (Herfindahl Index)
        # Lower = more diverse, Higher = more concentrated
        concentration = sum(p ** 2 for p in proportions)
        
        # Stablecoin Ratio
        # Known stablecoin addresses (lowercase)
        stablecoins = {
            # Ethereum
            '0xdac17f958d2ee523a2206206994597c13d831ec7',  # USDT
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
            '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
            '0x4fabb145d64652a948d72533023f6e7a623c7c53',  # BUSD
            '0x0000000000085d4780b73119b644ae5ecd22b376',  # TUSD
            # BSC
            '0x55d398326f99059ff775485246999027b3197955',  # USDT
            '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d',  # USDC
            '0xe9e7cea3dedca5984780bafc599bd69add087d56',  # BUSD
            '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3',  # DAI
        }
        
        stablecoin_value = sum(
            v for k, v in token_holdings.items() 
            if k.lower() in stablecoins
        )
        stablecoin_ratio = stablecoin_value / total_value if total_value > 0 else 0
        
        return {
            'unique_tokens_held': unique_tokens,
            'token_diversity_score': normalized_diversity,
            'stablecoin_ratio': stablecoin_ratio,
            'token_concentration_ratio': concentration
        }
    
    # ========================================================================
    # 🆕 NEW: DEX METRICS - KILLER FEATURE 🔥
    # ========================================================================
    
    @staticmethod
    def _compute_dex_metrics(
        transactions: List[Dict[str, Any]],
        blockchain: str
    ) -> Dict[str, float]:
        """
        Compute DEX (Decentralized Exchange) trading metrics.
        
        ⭐ KILLER FEATURE: dex_swap_count provides 38 vs 0 distinction
        
        Identifies DEX swaps by analyzing:
        - Contract interactions with known DEX routers
        - Swap function signatures
        - Token transfer patterns
        
        Returns:
            - dex_swap_count: Number of DEX swap transactions ⭐⭐⭐
            - dex_protocols_used: Count of different DEX protocols used
            - dex_volume_usd: Estimated total swap volume
        """
        # Known DEX router addresses (lowercase)
        dex_routers = {
            'ethereum': {
                '0x7a250d5630b4cf539739df2c5dacb4c659f2488d': 'Uniswap V2',
                '0xe592427a0aece92de3edee1f18e0157c05861564': 'Uniswap V3',
                '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f': 'SushiSwap',
                '0x1111111254eeb25477b68fb85ed929f73a960582': '1inch V5',
                '0x1111111254fb6c44bac0bed2854e76f90643097d': '1inch V4',
                '0xdef1c0ded9bec7f1a1670819833240f027b25eff': '0x',
            },
            'bsc': {
                '0x10ed43c718714eb63d5aa57b78b54704e256024e': 'PancakeSwap V2',
                '0x13f4ea83d0bd40e75c8222255bc855a974568dd4': 'PancakeSwap V3',
                '0x1b02da8cb0d097eb8d57a175b88c7d8b47997506': 'SushiSwap',
            },
            'solana': {
                # Solana uses program IDs
                'jupiterv6': 'Jupiter',
                'orca': 'Orca',
                'raydium': 'Raydium',
            }
        }
        
        # DEX function signatures (method IDs)
        swap_signatures = {
            '0x38ed1739',  # swapExactTokensForTokens
            '0x8803dbee',  # swapTokensForExactTokens
            '0x7ff36ab5',  # swapExactETHForTokens
            '0x18cbafe5',  # swapExactTokensForETH
            '0x791ac947',  # swapExactTokensForTokensSupportingFeeOnTransferTokens
            '0xfb3bdb41',  # swapETHForExactTokens
            '0x4a25d94a',  # swapTokensForExactETH
            '0xc04b8d59',  # exactInputSingle (Uniswap V3)
            '0x414bf389',  # exactInput (Uniswap V3)
        }
        
        router_map = dex_routers.get(blockchain, {})
        
        swap_count = 0
        protocols_used = set()
        total_volume = 0.0
        
        for tx in transactions:
            to_address = tx.get('to', '').lower()
            input_data = tx.get('input', '')
            
            # Check if transaction is to a DEX router
            is_dex_tx = False
            
            # Method 1: Known router address
            if to_address in router_map:
                is_dex_tx = True
                protocols_used.add(router_map[to_address])
            
            # Method 2: Function signature
            if input_data and len(input_data) >= 10:
                method_id = input_data[:10].lower()
                if method_id in swap_signatures:
                    is_dex_tx = True
            
            # Method 3: Pattern detection - multiple token transfers in single tx
            token_transfers = tx.get('token_transfers', [])
            if len(token_transfers) >= 2:
                # Check if tokens are different
                tokens = set(t.get('token_address') for t in token_transfers)
                if len(tokens) >= 2:
                    is_dex_tx = True
            
            if is_dex_tx:
                swap_count += 1
                
                # Estimate volume
                tx_value = float(tx.get('value', 0))
                if tx_value > 0:
                    total_volume += tx_value
                elif token_transfers:
                    volumes = [float(t.get('value_usd', 0)) for t in token_transfers]
                    if volumes:
                        total_volume += max(volumes)
        
        return {
            'dex_swap_count': swap_count,
            'dex_protocols_used': len(protocols_used),
            'dex_volume_usd': total_volume
        }
    
    # ========================================================================
    # 🆕 NEW: BOT DETECTION METRICS
    # ========================================================================
    
    @staticmethod
    def _compute_bot_detection_metrics(
        transactions: List[Dict[str, Any]],
        gas_prices: List[float],
        timestamps: List[int]
    ) -> Dict[str, float]:
        """
        Compute bot detection metrics.
        
        Bots typically show:
        - Very precise timing (transactions at exact intervals)
        - Optimized gas prices (minimal variation)
        - Automated patterns (consistent behavior)
        
        Returns:
            - tx_timing_precision_score: How precisely timed transactions are (0-1)
            - gas_price_optimization_score: Gas price consistency (0-1)
            - automated_pattern_score: Overall automation likelihood (0-1)
        """
        if len(transactions) < 5:
            # Need minimum transactions for pattern detection
            return {
                'tx_timing_precision_score': 0.0,
                'gas_price_optimization_score': 0.0,
                'automated_pattern_score': 0.0
            }
        
        # ===== TIMING PRECISION =====
        
        timing_score = 0.0
        if len(timestamps) >= 2:
            intervals = []
            sorted_timestamps = sorted(timestamps)
            for i in range(1, len(sorted_timestamps)):
                interval = sorted_timestamps[i] - sorted_timestamps[i-1]
                if interval > 0:
                    intervals.append(interval)
            
            if len(intervals) >= 3:
                mean_interval = statistics.mean(intervals)
                
                if mean_interval > 0:
                    std_interval = statistics.stdev(intervals)
                    cv = std_interval / mean_interval
                    timing_score = max(0, min(1, 1 - cv))
                    
                    # Check for exact intervals
                    interval_counter = Counter(intervals)
                    most_common_freq = interval_counter.most_common(1)[0][1]
                    if most_common_freq / len(intervals) > 0.5:
                        timing_score = min(1.0, timing_score + 0.3)
        
        # ===== GAS PRICE OPTIMIZATION =====
        
        gas_optimization_score = 0.0
        if len(gas_prices) >= 5:
            mean_gas = statistics.mean(gas_prices)
            
            if mean_gas > 0:
                std_gas = statistics.stdev(gas_prices)
                cv_gas = std_gas / mean_gas
                gas_optimization_score = max(0, min(1, 1 - cv_gas))
                
                # Check for identical gas prices
                gas_counter = Counter(gas_prices)
                most_common_gas_freq = gas_counter.most_common(1)[0][1]
                if most_common_gas_freq / len(gas_prices) > 0.7:
                    gas_optimization_score = min(1.0, gas_optimization_score + 0.2)
        
        # ===== AUTOMATED PATTERN SCORE =====
        
        pattern_indicators = []
        
        # Transaction value consistency
        tx_values = [float(tx.get('value', 0)) for tx in transactions if tx.get('value')]
        if len(tx_values) >= 5:
            value_counter = Counter(tx_values)
            most_common_value_freq = value_counter.most_common(1)[0][1]
            value_consistency = most_common_value_freq / len(tx_values)
            pattern_indicators.append(value_consistency)
        
        # Same contract interaction
        to_addresses = [tx.get('to', '').lower() for tx in transactions if tx.get('to')]
        if to_addresses:
            to_counter = Counter(to_addresses)
            most_common_to_freq = to_counter.most_common(1)[0][1]
            target_consistency = most_common_to_freq / len(to_addresses)
            if target_consistency > 0.8:
                pattern_indicators.append(0.8)
        
        # Input data consistency
        input_datas = [tx.get('input', '')[:10] for tx in transactions if tx.get('input')]
        if input_datas:
            input_counter = Counter(input_datas)
            most_common_input_freq = input_counter.most_common(1)[0][1]
            input_consistency = most_common_input_freq / len(input_datas)
            if input_consistency > 0.7:
                pattern_indicators.append(0.7)
        
        # Combine all indicators
        if pattern_indicators:
            automated_pattern_score = statistics.mean([
                timing_score,
                gas_optimization_score,
                *pattern_indicators
            ])
        else:
            automated_pattern_score = (timing_score + gas_optimization_score) / 2
        
        return {
            'tx_timing_precision_score': timing_score,
            'gas_price_optimization_score': gas_optimization_score,
            'automated_pattern_score': automated_pattern_score
        }


# ============================================================================
# VALIDATION & TESTING
# ============================================================================

def validate_phase1_metrics(metrics: Dict[str, Any]) -> Dict[str, bool]:
    """
    Validate that all 10 Phase 1 metrics are present.
    
    Returns:
        Dictionary showing which metrics are present
    """
    required_metrics = [
        # Portfolio (4)
        'unique_tokens_held',
        'token_diversity_score',
        'stablecoin_ratio',
        'token_concentration_ratio',
        
        # DEX (3)
        'dex_swap_count',
        'dex_protocols_used',
        'dex_volume_usd',
        
        # Bot Detection (3)
        'tx_timing_precision_score',
        'gas_price_optimization_score',
        'automated_pattern_score',
    ]
    
    validation = {}
    for metric in required_metrics:
        validation[metric] = metric in metrics
    
    return validation
