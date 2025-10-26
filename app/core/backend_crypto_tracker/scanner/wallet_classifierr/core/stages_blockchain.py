# ============================================================================
# core/stages_blockchain.py - COMPLETE WITH ALL METRICS
# ============================================================================
"""
Stage 1: Raw Metrics Extraction from Blockchain Data

✅ ALL REQUIRED OUTPUTS for Stage2
✅ PLUS 10 New Phase 1 Metrics
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
    
    Output Format (for Stage2 compatibility):
    - tx_count
    - total_received, total_sent, current_balance
    - first_seen, last_seen, age_days
    - timestamps[]
    - input_values[], output_values[]
    - inputs_per_tx{}, outputs_per_tx{}
    - incoming_tx_count, outgoing_tx_count
    - blockchain
    + 10 new Phase 1 metrics
    """
    
    @staticmethod
    def execute(
        blockchain_data: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None,
        blockchain: str = 'ethereum'
    ) -> Dict[str, Any]:
        """
        Execute Stage 1 analysis.
        
        Args:
            blockchain_data: Raw transaction data
            config: Optional configuration
            blockchain: Blockchain name
            
        Returns:
            Complete dictionary with ALL metrics Stage2 needs
        """
        transactions = blockchain_data.get('transactions', [])
        address = blockchain_data.get('address', '')
        current_balance = blockchain_data.get('current_balance', 0)
        
        if not transactions:
            logger.warning("⚠️ No transactions - returning defaults")
            return Stage1_RawMetrics._get_default_metrics(blockchain)
        
        # ===== PARSE TRANSACTIONS =====
        
        timestamps = []
        input_values = []
        output_values = []
        inputs_per_tx = {}
        outputs_per_tx = {}
        
        total_received = 0
        total_sent = 0
        incoming_tx_count = 0
        outgoing_tx_count = 0
        
        sent_tx = []
        received_tx = []
        
        for i, tx in enumerate(transactions):
            tx_hash = tx.get('hash', f'tx_{i}')
            tx_from = tx.get('from', '').lower()
            tx_to = tx.get('to', '').lower()
            tx_value = float(tx.get('value', 0))
            tx_timestamp = tx.get('timestamp', 0)
            
            # Timestamps
            if tx_timestamp:
                timestamps.append(tx_timestamp)
            
            # Determine direction
            is_incoming = tx_to == address.lower()
            is_outgoing = tx_from == address.lower()
            
            if is_incoming:
                incoming_tx_count += 1
                total_received += tx_value
                input_values.append(tx_value)
                received_tx.append(tx)
                
                # Input count (for consolidation detection)
                input_count = tx.get('input_count', 1)
                inputs_per_tx[tx_hash] = input_count
                outputs_per_tx[tx_hash] = tx.get('output_count', 1)
                
            elif is_outgoing:
                outgoing_tx_count += 1
                total_sent += tx_value
                output_values.append(tx_value)
                sent_tx.append(tx)
                
                # Output count
                output_count = tx.get('output_count', 1)
                outputs_per_tx[tx_hash] = output_count
                inputs_per_tx[tx_hash] = tx.get('input_count', 1)
        
        # ===== CALCULATE AGE =====
        
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
        
        # ===== BUILD STAGE1 OUTPUT (Required by Stage2) =====
        
        tx_count = len(transactions)
        
        raw_metrics = {
            # === REQUIRED BY STAGE2 ===
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
            
            # === ADDITIONAL BASIC METRICS ===
            'total_tx_count': tx_count,
            'sent_tx_count': len(sent_tx),
            'received_tx_count': len(received_tx),
            'last_active_days': last_active_days,
            'total_value_transacted': total_received + total_sent,
            'total_value_sent': total_sent,
            'total_value_received': total_received,
            'avg_tx_value': statistics.mean([*input_values, *output_values]) if (input_values or output_values) else 0,
            'median_tx_value': statistics.median([*input_values, *output_values]) if (input_values or output_values) else 0,
            'total_inputs': sum(inputs_per_tx.values()),
            'total_outputs': sum(outputs_per_tx.values()),
            'avg_inputs_per_tx': sum(inputs_per_tx.values()) / len(inputs_per_tx) if inputs_per_tx else 0,
            'avg_outputs_per_tx': sum(outputs_per_tx.values()) / len(outputs_per_tx) if outputs_per_tx else 0,
            'first_tx_timestamp': min(timestamps) if timestamps else 0,
            'last_tx_timestamp': max(timestamps) if timestamps else 0,
        }
        
        # === GAS METRICS ===
        gas_prices = [float(tx.get('gas_price', 0)) for tx in transactions if tx.get('gas_price')]
        raw_metrics['avg_gas_price'] = statistics.mean(gas_prices) if gas_prices else 0
        raw_metrics['median_gas_price'] = statistics.median(gas_prices) if gas_prices else 0
        
        # === NETWORK METRICS ===
        unique_senders = len(set(tx.get('from', '') for tx in transactions if tx.get('from')))
        unique_receivers = len(set(tx.get('to', '') for tx in transactions if tx.get('to')))
        raw_metrics['unique_senders'] = unique_senders
        raw_metrics['unique_receivers'] = unique_receivers
        raw_metrics['in_degree'] = unique_senders
        raw_metrics['out_degree'] = unique_receivers
        
        # ===== 🆕 PHASE 1 METRICS (10 NEW) =====
        
        # Portfolio Metrics (4)
        portfolio_metrics = Stage1_RawMetrics._compute_portfolio_metrics(
            blockchain_data, transactions
        )
        raw_metrics.update(portfolio_metrics)
        
        # DEX Metrics (3) - KILLER FEATURE
        dex_metrics = Stage1_RawMetrics._compute_dex_metrics(
            transactions, blockchain
        )
        raw_metrics.update(dex_metrics)
        
        # Bot Detection (3)
        bot_metrics = Stage1_RawMetrics._compute_bot_detection_metrics(
            transactions, gas_prices, timestamps
        )
        raw_metrics.update(bot_metrics)
        
        logger.info(f"✅ Stage1 complete: {len(raw_metrics)} metrics")
        
        return raw_metrics
    
    @staticmethod
    def _get_default_metrics(blockchain: str = 'ethereum') -> Dict[str, Any]:
        """Return default metrics when no transactions."""
        return {
            # Required by Stage2
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
            # Additional
            'total_tx_count': 0,
            'sent_tx_count': 0,
            'received_tx_count': 0,
            'last_active_days': 0,
            'total_value_transacted': 0,
            'total_value_sent': 0,
            'total_value_received': 0,
            'avg_tx_value': 0,
            'median_tx_value': 0,
            'total_inputs': 0,
            'total_outputs': 0,
            'avg_inputs_per_tx': 0,
            'avg_outputs_per_tx': 0,
            'avg_gas_price': 0,
            'median_gas_price': 0,
            'unique_senders': 0,
            'unique_receivers': 0,
            'in_degree': 0,
            'out_degree': 0,
            'first_tx_timestamp': 0,
            'last_tx_timestamp': 0,
            # Phase 1
            'unique_tokens_held': 0,
            'token_diversity_score': 0,
            'stablecoin_ratio': 0,
            'token_concentration_ratio': 0,
            'dex_swap_count': 0,
            'dex_protocols_used': 0,
            'dex_volume_usd': 0,
            'tx_timing_precision_score': 0,
            'gas_price_optimization_score': 0,
            'automated_pattern_score': 0,
        }
    
    # ========================================================================
    # PHASE 1 METRICS
    # ========================================================================
    
    @staticmethod
    def _compute_portfolio_metrics(
        blockchain_data: Dict[str, Any],
        transactions: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """Compute token portfolio diversity metrics."""
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
            token_holdings = {
                token: float(balance) 
                for token, balance in balances.items() 
                if float(balance) > 0
            }
        else:
            token_holdings = defaultdict(float)
            for transfer in token_transfers:
                token = transfer.get('token_address', '').lower()
                amount = float(transfer.get('value', 0))
                if transfer.get('to', '').lower() == blockchain_data.get('address', '').lower():
                    token_holdings[token] += amount
                elif transfer.get('from', '').lower() == blockchain_data.get('address', '').lower():
                    token_holdings[token] -= amount
            
            token_holdings = {k: v for k, v in token_holdings.items() if v > 0}
        
        unique_tokens = len(token_holdings)
        
        if unique_tokens == 0:
            return {
                'unique_tokens_held': 0,
                'token_diversity_score': 0.0,
                'stablecoin_ratio': 0.0,
                'token_concentration_ratio': 0.0
            }
        
        total_value = sum(token_holdings.values())
        
        if total_value == 0:
            return {
                'unique_tokens_held': unique_tokens,
                'token_diversity_score': 0.0,
                'stablecoin_ratio': 0.0,
                'token_concentration_ratio': 0.0
            }
        
        # Proportions
        proportions = [v / total_value for v in token_holdings.values()]
        
        # Diversity (Shannon Entropy)
        diversity_score = -sum(p * math.log2(p) for p in proportions if p > 0)
        max_entropy = math.log2(unique_tokens) if unique_tokens > 1 else 1
        normalized_diversity = diversity_score / max_entropy if max_entropy > 0 else 0
        
        # Concentration (Herfindahl)
        concentration = sum(p ** 2 for p in proportions)
        
        # Stablecoin Ratio
        stablecoins = {
            '0xdac17f958d2ee523a2206206994597c13d831ec7',  # USDT
            '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',  # USDC
            '0x6b175474e89094c44da98b954eedeac495271d0f',  # DAI
            '0x4fabb145d64652a948d72533023f6e7a623c7c53',  # BUSD
            '0x55d398326f99059ff775485246999027b3197955',  # USDT (BSC)
            '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d',  # USDC (BSC)
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
    
    @staticmethod
    def _compute_dex_metrics(
        transactions: List[Dict[str, Any]],
        blockchain: str
    ) -> Dict[str, float]:
        """Compute DEX trading metrics - KILLER FEATURE."""
        dex_routers = {
            'ethereum': {
                '0x7a250d5630b4cf539739df2c5dacb4c659f2488d': 'Uniswap V2',
                '0xe592427a0aece92de3edee1f18e0157c05861564': 'Uniswap V3',
                '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f': 'SushiSwap',
                '0x1111111254eeb25477b68fb85ed929f73a960582': '1inch',
            },
            'bsc': {
                '0x10ed43c718714eb63d5aa57b78b54704e256024e': 'PancakeSwap',
            }
        }
        
        swap_signatures = {
            '0x38ed1739', '0x8803dbee', '0x7ff36ab5', '0x18cbafe5',
            '0x791ac947', '0xc04b8d59', '0x414bf389'
        }
        
        router_map = dex_routers.get(blockchain, {})
        
        swap_count = 0
        protocols_used = set()
        total_volume = 0.0
        
        for tx in transactions:
            to_address = tx.get('to', '').lower()
            input_data = tx.get('input', '')
            
            is_dex_tx = False
            
            # Check router
            if to_address in router_map:
                is_dex_tx = True
                protocols_used.add(router_map[to_address])
            
            # Check signature
            if input_data and len(input_data) >= 10:
                method_id = input_data[:10].lower()
                if method_id in swap_signatures:
                    is_dex_tx = True
            
            # Check token transfers
            token_transfers = tx.get('token_transfers', [])
            if len(token_transfers) >= 2:
                tokens = set(t.get('token_address') for t in token_transfers)
                if len(tokens) >= 2:
                    is_dex_tx = True
            
            if is_dex_tx:
                swap_count += 1
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
    
    @staticmethod
    def _compute_bot_detection_metrics(
        transactions: List[Dict[str, Any]],
        gas_prices: List[float],
        timestamps: List[int]
    ) -> Dict[str, float]:
        """Compute bot detection metrics."""
        if len(transactions) < 5:
            return {
                'tx_timing_precision_score': 0.0,
                'gas_price_optimization_score': 0.0,
                'automated_pattern_score': 0.0
            }
        
        # Timing Precision
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
                    
                    # Check exact intervals
                    interval_counter = Counter(intervals)
                    most_common_freq = interval_counter.most_common(1)[0][1]
                    if most_common_freq / len(intervals) > 0.5:
                        timing_score = min(1.0, timing_score + 0.3)
        
        # Gas Optimization
        gas_optimization_score = 0.0
        if len(gas_prices) >= 5:
            mean_gas = statistics.mean(gas_prices)
            if mean_gas > 0:
                std_gas = statistics.stdev(gas_prices)
                cv_gas = std_gas / mean_gas
                gas_optimization_score = max(0, min(1, 1 - cv_gas))
                
                gas_counter = Counter(gas_prices)
                most_common_gas_freq = gas_counter.most_common(1)[0][1]
                if most_common_gas_freq / len(gas_prices) > 0.7:
                    gas_optimization_score = min(1.0, gas_optimization_score + 0.2)
        
        # Automated Pattern
        pattern_indicators = []
        
        # Value consistency
        tx_values = [float(tx.get('value', 0)) for tx in transactions if tx.get('value')]
        if len(tx_values) >= 5:
            value_counter = Counter(tx_values)
            most_common_value_freq = value_counter.most_common(1)[0][1]
            value_consistency = most_common_value_freq / len(tx_values)
            pattern_indicators.append(value_consistency)
        
        # Target consistency
        to_addresses = [tx.get('to', '').lower() for tx in transactions if tx.get('to')]
        if to_addresses:
            to_counter = Counter(to_addresses)
            most_common_to_freq = to_counter.most_common(1)[0][1]
            target_consistency = most_common_to_freq / len(to_addresses)
            if target_consistency > 0.8:
                pattern_indicators.append(0.8)
        
        # Input consistency
        input_datas = [tx.get('input', '')[:10] for tx in transactions if tx.get('input')]
        if input_datas:
            input_counter = Counter(input_datas)
            most_common_input_freq = input_counter.most_common(1)[0][1]
            input_consistency = most_common_input_freq / len(input_datas)
            if input_consistency > 0.7:
                pattern_indicators.append(0.7)
        
        # Combine
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
