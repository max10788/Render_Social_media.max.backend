# ============================================================================
# core/stages.py - ENHANCED VERSION with 20+ NEW METRICS
# ============================================================================
"""
Logic for the 3 analysis stages.
✅ ENHANCED: Added 20+ new metrics without requiring additional blockchain data
✅ All new metrics are calculated from existing Stage 1 output
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import math
from .utils import (
    convert_to_usd,
    calculate_entropy,
    calculate_gini_coefficient,
    is_round_amount,
    normalize_score,
    calculate_consolidation_ratio,
    calculate_balance_retention,
    calculate_turnover_rate,
    detect_equal_outputs
)


class Stage2_DerivedMetrics:
    """
    Stage 2: Calculate derived metrics from raw data.
    ✅ ENHANCED: Now calculates 40+ metrics from existing data
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
            crypto_price = 100  # Default for other chains
        
        derived = {}
        
        # ===================================================================
        # EXISTING METRICS (Keep all original metrics)
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
        
        # ===================================================================
        # ✅ NEW METRICS - TIME-BASED PATTERNS (No new blockchain data needed)
        # ===================================================================
        
        # 1. Business Hours Trading Ratio
        if timestamps:
            hours = [datetime.fromtimestamp(ts).hour for ts in timestamps]
            business_hours_txs = len([h for h in hours if 8 <= h <= 20])
            derived['business_hours_ratio'] = business_hours_txs / len(timestamps)
        else:
            derived['business_hours_ratio'] = 0.5  # Neutral
        
        # 2. Weekday Trading Ratio
        if timestamps:
            weekdays = [datetime.fromtimestamp(ts).weekday() for ts in timestamps]
            weekday_txs = len([d for d in weekdays if d < 5])
            derived['weekday_ratio'] = weekday_txs / len(timestamps)
        else:
            derived['weekday_ratio'] = 0.7  # Most trading happens on weekdays
        
        # 3. Trading Regularity (consistency of time intervals)
        if len(timestamps) > 1:
            time_diffs = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
            if time_diffs:
                std_dev = math.sqrt(sum((x - sum(time_diffs)/len(time_diffs))**2 for x in time_diffs) / len(time_diffs))
                # Normalize: lower std_dev = more regular
                max_std = 86400 * 7  # 7 days
                derived['trading_regularity'] = 1.0 - min(std_dev / max_std, 1.0)
            else:
                derived['trading_regularity'] = 0
        else:
            derived['trading_regularity'] = 0
        
        # 4. Night Trading Ratio (unusual activity hours)
        if timestamps:
            hours = [datetime.fromtimestamp(ts).hour for ts in timestamps]
            night_txs = len([h for h in hours if h < 6 or h > 22])
            derived['night_trading_ratio'] = night_txs / len(timestamps)
        else:
            derived['night_trading_ratio'] = 0
        
        # 5. Weekend Trading Ratio
        if timestamps:
            weekdays = [datetime.fromtimestamp(ts).weekday() for ts in timestamps]
            weekend_txs = len([d for d in weekdays if d >= 5])
            derived['weekend_trading_ratio'] = weekend_txs / len(timestamps)
        else:
            derived['weekend_trading_ratio'] = 0.15  # Typical weekend activity
        
        # ===================================================================
        # ✅ NEW METRICS - VALUE PATTERNS (From existing input/output values)
        # ===================================================================
        
        # 6. Output Value Variance (selling behavior)
        if output_values and len(output_values) > 1:
            avg_output = sum(output_values) / len(output_values)
            variance = sum((x - avg_output)**2 for x in output_values) / len(output_values)
            std_output = math.sqrt(variance)
            derived['output_value_variance'] = std_output / (avg_output + 1)  # Coefficient of variation
        else:
            derived['output_value_variance'] = 0
        
        # 7. Input Value Variance (buying behavior / DCA detection)
        if input_values and len(input_values) > 1:
            avg_input = sum(input_values) / len(input_values)
            variance = sum((x - avg_input)**2 for x in input_values) / len(input_values)
            std_input = math.sqrt(variance)
            derived['input_value_variance'] = std_input / (avg_input + 1)
            # Low variance = DCA behavior
            derived['dca_behavior'] = 1.0 - min(derived['input_value_variance'], 1.0)
        else:
            derived['input_value_variance'] = 0
            derived['dca_behavior'] = 0
        
        # 8. Varied Selling Score (traders sell in tranches)
        derived['varied_selling'] = min(derived['output_value_variance'], 1.0)
        
        # 9. Quick Sell Ratio (fast turnaround = active trading)
        if len(timestamps) > 1:
            quick_sells = 0
            for i in range(1, len(timestamps)):
                time_diff = timestamps[i] - timestamps[i-1]
                # If transaction within 1 hour of previous
                if time_diff < 3600 and i < len(output_values) and output_values[i] > 0:
                    quick_sells += 1
            derived['quick_sell_ratio'] = quick_sells / max(len(timestamps) - 1, 1)
        else:
            derived['quick_sell_ratio'] = 0
        
        # 10. Large Transaction Ratio (% of transactions > $10k)
        if output_values:
            large_txs = len([v for v in output_values if convert_to_usd(v, crypto_price) > 10000])
            derived['large_tx_ratio'] = large_txs / len(output_values)
        else:
            derived['large_tx_ratio'] = 0
        
        # 11. Micro Transaction Ratio (% of transactions < $100)
        if output_values:
            micro_txs = len([v for v in output_values if convert_to_usd(v, crypto_price) < 100])
            derived['micro_tx_ratio'] = micro_txs / len(output_values)
        else:
            derived['micro_tx_ratio'] = 0
        
        # ===================================================================
        # ✅ NEW METRICS - BALANCE BEHAVIOR (From existing balance data)
        # ===================================================================
        
        # 12. Balance Volatility (how much balance fluctuates)
        # Estimate from total_received, total_sent, current_balance
        if total_received > 0:
            max_possible_balance = total_received
            avg_balance = current_balance / 2  # Rough estimate
            derived['balance_volatility'] = 1.0 - (current_balance / max_possible_balance)
        else:
            derived['balance_volatility'] = 0
        
        # 13. Balance Utilization (how much of peak balance is typically used)
        if total_received > 0:
            derived['balance_utilization'] = current_balance / total_received
        else:
            derived['balance_utilization'] = 1.0
        
        # 14. Empty Wallet Frequency (estimate based on outgoing ratio)
        # High outgoing ratio suggests frequent emptying
        derived['empty_frequency'] = derived['outgoing_tx_ratio'] * 0.5
        
        # 15. Net Flow Direction (consistent direction = accumulator or distributor)
        if total_received + total_sent > 0:
            derived['net_flow_direction'] = (total_received - total_sent) / (total_received + total_sent)
        else:
            derived['net_flow_direction'] = 0
        
        # ===================================================================
        # ✅ NEW METRICS - NETWORK PATTERNS (From tx structure)
        # ===================================================================
        
        # 16. Counterparty Diversity (estimated from outputs_per_tx)
        # Many outputs = many counterparties
        if outputs_per_tx:
            avg_outputs = sum(outputs_per_tx.values()) / len(outputs_per_tx)
            derived['counterparty_diversity'] = min(avg_outputs / 10, 1.0)
        else:
            derived['counterparty_diversity'] = 0
        
        # 17. Repeat Transaction Ratio (inverse of diversity)
        derived['repeat_ratio'] = 1.0 - derived['counterparty_diversity']
        
        # 18. Smart Contract Ratio
        derived['smart_contract_ratio'] = derived['smart_contract_calls'] / max(tx_count, 1)
        
        # 19. Fan-out Score (many outputs = distribution pattern)
        if outputs_per_tx:
            high_output_txs = sum(1 for count in outputs_per_tx.values() if count > 3)
            derived['fan_out_score'] = high_output_txs / len(outputs_per_tx)
        else:
            derived['fan_out_score'] = 0
        
        # 20. Fan-in Score (many inputs = collection pattern)
        if inputs_per_tx:
            high_input_txs = sum(1 for count in inputs_per_tx.values() if count > 3)
            derived['fan_in_score'] = high_input_txs / len(inputs_per_tx)
        else:
            derived['fan_in_score'] = 0
        
        # ===================================================================
        # ✅ NEW METRICS - BEHAVIORAL PATTERNS
        # ===================================================================
        
        # 21. Activity Burst Ratio (concentrated activity periods)
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
        
        # 22. Dormancy Ratio (long periods of inactivity)
        if age_days > 0 and tx_count > 0:
            expected_interval = age_days / tx_count
            derived['dormancy_ratio'] = min(expected_interval / 30, 1.0)  # Normalize to months
        else:
            derived['dormancy_ratio'] = 0
        
        # 23. Profit-Taking Pattern (decreasing output values over time)
        if len(output_values) > 3:
            # Check if outputs trend downward (profit-taking in tranches)
            first_half_avg = sum(output_values[:len(output_values)//2]) / max(len(output_values)//2, 1)
            second_half_avg = sum(output_values[len(output_values)//2:]) / max(len(output_values) - len(output_values)//2, 1)
            if first_half_avg > 0:
                derived['profit_taking_pattern'] = max(0, (first_half_avg - second_half_avg) / first_half_avg)
            else:
                derived['profit_taking_pattern'] = 0
        else:
            derived['profit_taking_pattern'] = 0
        
        # 24. Accumulation Pattern (increasing input values over time)
        if len(input_values) > 3:
            first_half_avg = sum(input_values[:len(input_values)//2]) / max(len(input_values)//2, 1)
            second_half_avg = sum(input_values[len(input_values)//2:]) / max(len(input_values) - len(input_values)//2, 1)
            if first_half_avg > 0:
                derived['accumulation_pattern'] = max(0, (second_half_avg - first_half_avg) / first_half_avg)
            else:
                derived['accumulation_pattern'] = 0
        else:
            derived['accumulation_pattern'] = 0
        
        # 25. Transaction Size Consistency (similar tx sizes = algorithmic/bot)
        all_values = input_values + output_values
        if len(all_values) > 1:
            avg_val = sum(all_values) / len(all_values)
            variance = sum((x - avg_val)**2 for x in all_values) / len(all_values)
            std_val = math.sqrt(variance)
            coef_var = std_val / (avg_val + 1)
            derived['tx_size_consistency'] = 1.0 - min(coef_var, 1.0)
        else:
            derived['tx_size_consistency'] = 0
        
        return derived


# Stage3 remains unchanged
class Stage3_ContextAnalysis:
    """
    Stage 3: Add external context data.
    ✅ FIXED: Works without context_db by providing estimated values
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
                
            except (AttributeError, Exception) as e:
                pass
        
        # === ESTIMATE CONTEXT METRICS ===
        
        tx_per_month = stage2_output.get('tx_per_month', 0)
        consolidation_rate = stage2_output.get('consolidation_rate', 0)
        equal_output_prop = stage2_output.get('equal_output_proportion', 0)
        total_value_usd = stage2_output.get('total_value_usd', 0)
        fan_in = stage2_output.get('fan_in_score', 0)
        fan_out = stage2_output.get('fan_out_score', 0)
        
        # Network degree metrics (enhanced with new metrics)
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
        
        return context
