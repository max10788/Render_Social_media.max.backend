# ============================================================================
# core/stages.py - COMPLETE VERSION (All Metrics + Phase 1)
# ============================================================================
"""
Logic for the 3 analysis stages.
✅ ALL EXISTING METRICS (50+)
✅ PLUS 10 NEW PHASE 1 METRICS
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from collections import Counter, defaultdict
import math
import statistics


# ============================================================================
# UTILITY FUNCTIONS (Inline to avoid dependencies)
# ============================================================================

def convert_to_usd(amount: float, price: float) -> float:
    """Convert crypto amount to USD."""
    return amount * price


def calculate_entropy(timestamps: List[int]) -> float:
    """Calculate Shannon entropy of time intervals."""
    if len(timestamps) < 2:
        return 0
    
    intervals = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
    if not intervals:
        return 0
    
    # Group into hour buckets
    hours = [int(interval / 3600) for interval in intervals]
    hour_counts = Counter(hours)
    total = len(hours)
    
    entropy = 0
    for count in hour_counts.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    
    return entropy


def calculate_gini_coefficient(values: List[float]) -> float:
    """Calculate Gini coefficient for value distribution."""
    if not values or len(values) < 2:
        return 0
    
    sorted_values = sorted(values)
    n = len(sorted_values)
    cumsum = 0
    
    for i, val in enumerate(sorted_values):
        cumsum += (2 * (i + 1) - n - 1) * val
    
    return cumsum / (n * sum(sorted_values)) if sum(sorted_values) > 0 else 0


def is_round_amount(value: float) -> bool:
    """Check if value is a round number."""
    if value == 0:
        return False
    
    # Check if value ends in multiple zeros
    str_val = f"{value:.8f}".rstrip('0').rstrip('.')
    
    # Round numbers: 1.0, 10.0, 100.0, 0.1, 0.01, etc.
    return value in [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0]


def normalize_score(value: float, min_val: float, max_val: float) -> float:
    """Normalize value to [0, 1]."""
    if max_val == min_val:
        return 0.5
    return max(0, min(1, (value - min_val) / (max_val - min_val)))


def calculate_consolidation_ratio(inputs_per_tx: Dict, outputs_per_tx: Dict) -> float:
    """Calculate consolidation ratio from inputs/outputs."""
    if not inputs_per_tx or not outputs_per_tx:
        return 1.0
    
    avg_inputs = sum(inputs_per_tx.values()) / len(inputs_per_tx)
    avg_outputs = sum(outputs_per_tx.values()) / len(outputs_per_tx)
    
    if avg_outputs == 0:
        return 10.0
    
    return min(avg_inputs / avg_outputs, 10.0)


def calculate_balance_retention(current_balance: float, total_received: float) -> float:
    """Calculate balance retention ratio."""
    if total_received == 0:
        return 0
    return min(current_balance / total_received, 1.0)


def calculate_turnover_rate(total_sent: float, current_balance: float) -> float:
    """Calculate turnover rate."""
    if current_balance == 0:
        return 10.0
    return min(total_sent / current_balance, 10.0)


def detect_equal_outputs(output_values: List[float]) -> float:
    """Detect proportion of equal output values (mixer detection)."""
    if len(output_values) < 2:
        return 0
    
    # Round to 8 decimals and count
    rounded = [round(v, 8) for v in output_values]
    counter = Counter(rounded)
    
    # Find most common value
    most_common_count = counter.most_common(1)[0][1] if counter else 0
    
    return most_common_count / len(output_values)


# ============================================================================
# STAGE 2: DERIVED METRICS
# ============================================================================

class Stage2_DerivedMetrics:
    """
    Stage 2: Calculate derived metrics from raw data.
    ✅ COMPLETE: All existing + Phase 1 new metrics
    """
    
    def execute(self, stage1_output: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Calculate derived metrics from Stage 1 output.
        
        Args:
            stage1_output: Output from Stage 1
            config: Optional configuration (e.g., BTC price, thresholds)
            
        Returns:
            Dictionary of derived metrics
        """
        config = config or {}
        btc_price = config.get('btc_price', 50000)
        eth_price = config.get('eth_price', 3000)
        
        current_time = int(datetime.now().timestamp())
        
        # Get raw metrics
        tx_count = stage1_output.get('tx_count', 0)
        total_received = stage1_output.get('total_received', 0)
        total_sent = stage1_output.get('total_sent', 0)
        current_balance = stage1_output.get('current_balance', 0)
        first_seen = stage1_output.get('first_seen', 0)
        last_seen = stage1_output.get('last_seen', 0)
        age_days = stage1_output.get('age_days', 0)
        timestamps = stage1_output.get('timestamps', [])
        input_values = stage1_output.get('input_values', [])
        output_values = stage1_output.get('output_values', [])
        inputs_per_tx = stage1_output.get('inputs_per_tx', {})
        outputs_per_tx = stage1_output.get('outputs_per_tx', {})
        incoming_tx_count = stage1_output.get('incoming_tx_count', 0)
        outgoing_tx_count = stage1_output.get('outgoing_tx_count', 0)
        
        # Determine price based on blockchain
        blockchain = stage1_output.get('blockchain', 'ethereum').lower()
        if blockchain in ['bitcoin', 'btc']:
            crypto_price = btc_price
        elif blockchain in ['ethereum', 'eth', 'polygon', 'avalanche', 'bsc']:
            crypto_price = eth_price
        else:
            crypto_price = 100  # Default
        
        derived = {}
        
        # ===================================================================
        # EXISTING METRICS (Keep ALL original metrics)
        # ===================================================================
        
        # === HOLDING METRICS ===
        derived['holding_period_days'] = (current_time - first_seen) / 86400 if first_seen else age_days
        derived['balance_retention_ratio'] = calculate_balance_retention(current_balance, total_received)
        
        # === ACTIVITY METRICS ===
        months_active = max(age_days / 30, 0.1)
        derived['tx_per_month'] = tx_count / months_active if months_active > 0 else 0
        
        if outgoing_tx_count == 0 and tx_count > 0:
            outgoing_tx_count = sum(1 for count in outputs_per_tx.values() if count > 0)
        derived['outgoing_tx_ratio'] = outgoing_tx_count / tx_count if tx_count > 0 else 0
        
        # === CONSOLIDATION ===
        derived['consolidation_rate'] = calculate_consolidation_ratio(inputs_per_tx, outputs_per_tx)
        
        # === VALUE ANALYSIS ===
        if input_values:
            avg_input_value = sum(input_values) / len(input_values)
            derived['avg_input_value_usd'] = convert_to_usd(avg_input_value, crypto_price)
        else:
            derived['avg_input_value_usd'] = 0
        
        if output_values:
            avg_output_value = sum(output_values) / len(output_values)
            derived['avg_output_value_usd'] = convert_to_usd(avg_output_value, crypto_price)
        else:
            derived['avg_output_value_usd'] = 0
        
        derived['total_value_usd'] = convert_to_usd(current_balance, crypto_price)
        net_inflow = total_received - total_sent
        derived['net_inflow_usd'] = convert_to_usd(net_inflow, crypto_price)
        
        # === MIXER DETECTION ===
        derived['equal_output_proportion'] = detect_equal_outputs(output_values)
        
        if output_values:
            round_count = sum(1 for v in output_values if is_round_amount(v))
            derived['round_amounts_ratio'] = round_count / len(output_values)
        else:
            derived['round_amounts_ratio'] = 0
        
        # Single output ratio
        if outputs_per_tx:
            single_output_txs = sum(1 for count in outputs_per_tx.values() if count == 1)
            derived['single_output_ratio'] = single_output_txs / len(outputs_per_tx)
        else:
            derived['single_output_ratio'] = 0
        
        # === TIMING ANALYSIS ===
        derived['timing_entropy'] = calculate_entropy(timestamps)
        
        # === PORTFOLIO METRICS ===
        derived['portfolio_concentration'] = calculate_gini_coefficient(output_values)
        
        # === TRADER METRICS ===
        derived['turnover_rate'] = calculate_turnover_rate(total_sent, current_balance)
        
        # === EXCHANGE INTERACTIONS ===
        if derived['tx_per_month'] > 10:
            derived['exchange_interaction_count'] = int(derived['tx_per_month'] * 0.5)
        elif derived['tx_per_month'] > 5:
            derived['exchange_interaction_count'] = int(derived['tx_per_month'] * 0.3)
        else:
            derived['exchange_interaction_count'] = 0
        
        derived['dex_cex_interactions'] = derived['exchange_interaction_count']
        
        # === SMART CONTRACT CALLS ===
        if blockchain in ['ethereum', 'eth', 'polygon', 'avalanche', 'bsc', 'solana', 'sui']:
            derived['smart_contract_calls'] = int(tx_count * 0.25)
        else:
            derived['smart_contract_calls'] = 0
        
        # === BOOLEAN FLAGS ===
        derived['whale_cluster_member'] = derived['total_value_usd'] > 5_000_000
        derived['institutional_wallet'] = (
            derived['total_value_usd'] > 10_000_000 and 
            derived['tx_per_month'] < 2
        )
        derived['known_mixer_interaction'] = False
        derived['tornado_cash_interaction'] = False
        derived['coinjoin_frequency'] = derived['equal_output_proportion']
        
        # ===================================================================
        # TIME-BASED PATTERNS
        # ===================================================================
        
        # Business Hours Trading Ratio
        if timestamps:
            hours = [datetime.fromtimestamp(ts).hour for ts in timestamps]
            business_hours_txs = len([h for h in hours if 8 <= h <= 20])
            derived['business_hours_ratio'] = business_hours_txs / len(timestamps)
        else:
            derived['business_hours_ratio'] = 0.5
        
        # Weekday Trading Ratio
        if timestamps:
            weekdays = [datetime.fromtimestamp(ts).weekday() for ts in timestamps]
            weekday_txs = len([d for d in weekdays if d < 5])
            derived['weekday_ratio'] = weekday_txs / len(timestamps)
        else:
            derived['weekday_ratio'] = 0.7
        
        # Trading Regularity
        if len(timestamps) > 1:
            time_diffs = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
            if time_diffs:
                std_dev = statistics.stdev(time_diffs)
                max_std = 86400 * 7  # 7 days
                derived['trading_regularity'] = 1.0 - min(std_dev / max_std, 1.0)
            else:
                derived['trading_regularity'] = 0
        else:
            derived['trading_regularity'] = 0
        
        # Night Trading Ratio
        if timestamps:
            hours = [datetime.fromtimestamp(ts).hour for ts in timestamps]
            night_txs = len([h for h in hours if h < 6 or h > 22])
            derived['night_trading_ratio'] = night_txs / len(timestamps)
        else:
            derived['night_trading_ratio'] = 0
        
        # Weekend Trading Ratio
        if timestamps:
            weekdays = [datetime.fromtimestamp(ts).weekday() for ts in timestamps]
            weekend_txs = len([d for d in weekdays if d >= 5])
            derived['weekend_trading_ratio'] = weekend_txs / len(timestamps)
        else:
            derived['weekend_trading_ratio'] = 0.15
        
        # ===================================================================
        # VALUE PATTERNS
        # ===================================================================
        
        # Large Transaction Ratio
        if output_values:
            large_txs = len([v for v in output_values if convert_to_usd(v, crypto_price) > 10000])
            derived['large_tx_ratio'] = large_txs / len(output_values)
        else:
            derived['large_tx_ratio'] = 0
        
        # Micro Transaction Ratio
        if output_values:
            micro_txs = len([v for v in output_values if convert_to_usd(v, crypto_price) < 100])
            derived['micro_tx_ratio'] = micro_txs / len(output_values)
        else:
            derived['micro_tx_ratio'] = 0
        
        # ===================================================================
        # BALANCE BEHAVIOR
        # ===================================================================
        
        # Balance Volatility
        if total_received > 0:
            max_possible_balance = total_received
            derived['balance_volatility'] = 1.0 - (current_balance / max_possible_balance)
        else:
            derived['balance_volatility'] = 0
        
        # Balance Utilization
        if total_received > 0:
            derived['balance_utilization'] = current_balance / total_received
        else:
            derived['balance_utilization'] = 1.0
        
        # ===================================================================
        # NETWORK PATTERNS
        # ===================================================================
        
        # Counterparty Diversity
        if outputs_per_tx:
            avg_outputs = sum(outputs_per_tx.values()) / len(outputs_per_tx)
            derived['counterparty_diversity'] = min(avg_outputs / 10, 1.0)
        else:
            derived['counterparty_diversity'] = 0
        
        # Smart Contract Ratio
        derived['smart_contract_ratio'] = derived['smart_contract_calls'] / max(tx_count, 1)
        
        # Fan-out Score
        if outputs_per_tx:
            high_output_txs = sum(1 for count in outputs_per_tx.values() if count > 3)
            derived['fan_out_score'] = high_output_txs / len(outputs_per_tx)
        else:
            derived['fan_out_score'] = 0
        
        # Fan-in Score
        if inputs_per_tx:
            high_input_txs = sum(1 for count in inputs_per_tx.values() if count > 3)
            derived['fan_in_score'] = high_input_txs / len(inputs_per_tx)
        else:
            derived['fan_in_score'] = 0
        
        # ===================================================================
        # BEHAVIORAL PATTERNS
        # ===================================================================
        
        # Activity Burst Ratio
        if len(timestamps) > 2:
            time_diffs = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
            if time_diffs:
                avg_diff = sum(time_diffs) / len(time_diffs)
                short_intervals = len([d for d in time_diffs if d < avg_diff * 0.5])
                derived['activity_burst_ratio'] = short_intervals / len(time_diffs)
            else:
                derived['activity_burst_ratio'] = 0
        else:
            derived['activity_burst_ratio'] = 0
        
        # Dormancy Ratio
        if age_days > 0 and tx_count > 0:
            expected_interval = age_days / tx_count
            derived['dormancy_ratio'] = min(expected_interval / 30, 1.0)
        else:
            derived['dormancy_ratio'] = 0
        
        # Accumulation Pattern
        if len(input_values) > 3:
            first_half_avg = sum(input_values[:len(input_values)//2]) / max(len(input_values)//2, 1)
            second_half_avg = sum(input_values[len(input_values)//2:]) / max(len(input_values) - len(input_values)//2, 1)
            if first_half_avg > 0:
                derived['accumulation_pattern'] = max(0, (second_half_avg - first_half_avg) / first_half_avg)
            else:
                derived['accumulation_pattern'] = 0
        else:
            derived['accumulation_pattern'] = 0
        
        # Transaction Size Consistency
        all_values = input_values + output_values
        if len(all_values) > 1:
            avg_val = sum(all_values) / len(all_values)
            variance = sum((x - avg_val)**2 for x in all_values) / len(all_values)
            std_val = math.sqrt(variance)
            coef_var = std_val / (avg_val + 1)
            derived['tx_size_consistency'] = 1.0 - min(coef_var, 1.0)
        else:
            derived['tx_size_consistency'] = 0
        
        # ===================================================================
        # 🆕 PHASE 1 METRICS - Derived from Stage 1
        # ===================================================================
        
        # DEX trading intensity (uses dex_swap_count from Stage 1)
        dex_swaps = stage1_output.get('dex_swap_count', 0)
        if tx_count > 0:
            derived['dex_trading_ratio'] = dex_swaps / tx_count
        else:
            derived['dex_trading_ratio'] = 0
        
        # Portfolio complexity (uses unique_tokens_held from Stage 1)
        tokens_held = stage1_output.get('unique_tokens_held', 0)
        derived['portfolio_complexity'] = min(1.0, tokens_held / 20)
        
        # Bot likelihood score (uses bot metrics from Stage 1)
        timing_score = stage1_output.get('tx_timing_precision_score', 0)
        gas_score = stage1_output.get('gas_price_optimization_score', 0)
        pattern_score = stage1_output.get('automated_pattern_score', 0)
        derived['bot_likelihood_score'] = (timing_score + gas_score + pattern_score) / 3
        
        return derived


# ============================================================================
# STAGE 3: CONTEXT ANALYSIS
# ============================================================================

class Stage3_ContextAnalysis:
    """
    Stage 3: Add external context data.
    ✅ Works with or without context_db
    """
    
    def execute(
        self,
        stage2_output: Dict[str, Any],
        address: str,
        context_db: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        Add context from external databases and network analysis.
        
        Args:
            stage2_output: Output from Stage 2
            address: The wallet address being analyzed
            context_db: Optional database/service providing context
            
        Returns:
            Dictionary of context metrics
        """
        context = {}
        
        # Try to use context_db if provided
        if context_db:
            try:
                context['is_exchange'] = context_db.is_exchange(address)
                context['exchange_interaction_count'] = context_db.count_exchange_interactions(address)
                context['in_mixer_cluster'] = context_db.in_mixer_cluster(address)
                context['known_mixer_interaction'] = context_db.interacts_with_mixer(address)
                context['tornado_cash_interaction'] = context_db.tornado_cash_interaction(address)
                context['in_degree'] = context_db.get_in_degree(address)
                context['out_degree'] = context_db.get_out_degree(address)
                context['betweenness_centrality'] = context_db.get_betweenness(address)
                context['eigenvector_centrality'] = context_db.get_eigenvector(address)
                context['smart_contract_calls'] = context_db.count_smart_contract_calls(address)
                context['dex_cex_interactions'] = context_db.count_dex_cex_interactions(address)
                context['institutional_wallet'] = context_db.is_institutional(address)
                context['whale_cluster_member'] = context_db.in_whale_cluster(address)
                context['cluster_size'] = context_db.get_cluster_size(address)
                
                return context
                
            except (AttributeError, Exception):
                pass
        
        # === ESTIMATE CONTEXT METRICS ===
        
        tx_per_month = stage2_output.get('tx_per_month', 0)
        consolidation_rate = stage2_output.get('consolidation_rate', 0)
        equal_output_prop = stage2_output.get('equal_output_proportion', 0)
        total_value_usd = stage2_output.get('total_value_usd', 0)
        fan_in = stage2_output.get('fan_in_score', 0)
        fan_out = stage2_output.get('fan_out_score', 0)
        
        # Network degree metrics
        context['in_degree'] = int(tx_per_month * fan_in * 5)
        context['out_degree'] = int(tx_per_month * fan_out * 5)
        context['cluster_size'] = int(10 * (fan_in + fan_out)) if (fan_in + fan_out) > 0.5 else 1
        
        # Centrality metrics
        context['betweenness_centrality'] = equal_output_prop * 0.05
        
        if total_value_usd > 10_000_000:
            context['eigenvector_centrality'] = 0.08
        elif total_value_usd > 1_000_000:
            context['eigenvector_centrality'] = 0.03
        else:
            context['eigenvector_centrality'] = 0.01
        
        # Use Stage 2 values
        context['exchange_interaction_count'] = stage2_output.get('exchange_interaction_count', 0)
        context['dex_cex_interactions'] = stage2_output.get('dex_cex_interactions', 0)
        context['smart_contract_calls'] = stage2_output.get('smart_contract_calls', 0)
        context['whale_cluster_member'] = stage2_output.get('whale_cluster_member', False)
        context['institutional_wallet'] = stage2_output.get('institutional_wallet', False)
        context['known_mixer_interaction'] = stage2_output.get('known_mixer_interaction', False)
        context['tornado_cash_interaction'] = stage2_output.get('tornado_cash_interaction', False)
        
        # Additional flags
        context['is_exchange'] = False
        context['in_mixer_cluster'] = equal_output_prop > 0.6
        
        # 🆕 Phase 1 context enhancements
        bot_score = stage2_output.get('bot_likelihood_score', 0)
        if bot_score > 0.7:
            context['likely_bot'] = True
            context['bot_confidence'] = 'High'
        elif bot_score > 0.5:
            context['likely_bot'] = True
            context['bot_confidence'] = 'Medium'
        else:
            context['likely_bot'] = False
            context['bot_confidence'] = 'Low'
        
        # DEX trader classification
        dex_ratio = stage2_output.get('dex_trading_ratio', 0)
        if dex_ratio > 0.5:
            context['primary_dex_trader'] = True
        else:
            context['primary_dex_trader'] = False
        
        # Portfolio classification
        portfolio_complexity = stage2_output.get('portfolio_complexity', 0)
        if portfolio_complexity > 0.5:
            context['diversified_portfolio'] = True
        else:
            context['diversified_portfolio'] = False
        
        return context
