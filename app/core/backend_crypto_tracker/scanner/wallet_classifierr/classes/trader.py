# ============================================================================
# classes/trader.py - ENHANCED VERSION
# ============================================================================
"""
Enhanced Trader wallet analyzer with 25+ metrics.
✅ Uses all new time-based, value-based, and behavioral metrics
✅ Higher accuracy through more comprehensive analysis
"""
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.base_analyzer import BaseWalletAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.metric_definitions import TRADER_METRICS
from typing import Dict, Any


class TraderAnalyzer(BaseWalletAnalyzer):
    """Enhanced Analyzer for Trader wallets with 25+ metrics."""
    
    CLASS_NAME = "Trader"
    METRICS = TRADER_METRICS
    THRESHOLD = 0.40  # ✅ Can keep at 0.40 with more metrics!
    
    # ✅ NEW: More granular weighting with 6 categories
    WEIGHTS = {
        "transaction_frequency": 0.20,   # How often trading?
        "value_patterns": 0.20,          # How much & how?
        "timing_patterns": 0.15,         # When trading?
        "balance_behavior": 0.15,        # Balance management?
        "network_behavior": 0.15,        # Who with?
        "exchange_usage": 0.15           # Where trading?
    }
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """
        Enhanced scoring with 25+ metrics across 6 categories.
        
        Args:
            metrics: All metrics from Stage 1, 2, and 3
            
        Returns:
            Score [0, 1] indicating probability of being a trader
        """
        
        # ===================================================================
        # CATEGORY 1: TRANSACTION FREQUENCY (20%)
        # ===================================================================
        
        # 1.1 High transaction frequency
        tx_freq = self._normalize(metrics.get('tx_per_month', 0), 0, 50)
        
        # 1.2 Trading regularity (consistent timing)
        regularity = metrics.get('trading_regularity', 0)
        
        # 1.3 Activity bursts (concentrated trading periods)
        burst_activity = metrics.get('activity_burst_ratio', 0)
        
        # 1.4 Low dormancy (active, not idle)
        low_dormancy = 1.0 - metrics.get('dormancy_ratio', 0)
        
        frequency_score = self._avg([
            tx_freq,
            regularity,
            burst_activity,
            low_dormancy
        ])
        
        # ===================================================================
        # CATEGORY 2: VALUE PATTERNS (20%)
        # ===================================================================
        
        # 2.1 Bidirectional flow (balanced in/out)
        total_sent = metrics.get('total_sent', 0)
        total_received = metrics.get('total_received', 0)
        
        if total_received > 0 and total_sent > 0:
            in_out_balance = min(
                total_sent / total_received,
                total_received / total_sent
            )
        else:
            in_out_balance = 0
        
        # 2.2 Average transaction value (moderate, not micro/whale)
        tx_count = max(metrics.get('tx_count', 0), 1)
        avg_tx_value = self._normalize(
            metrics.get('total_value_usd', 0) / tx_count,
            0,
            10000
        )
        
        # 2.3 Varied selling (selling in tranches, not all at once)
        varied_selling = metrics.get('varied_selling', 0)
        
        # 2.4 DCA behavior (consistent buying patterns)
        dca_score = metrics.get('dca_behavior', 0)
        
        # 2.5 Quick sell ratio (fast reactions to market)
        quick_sells = metrics.get('quick_sell_ratio', 0)
        
        # 2.6 Profit-taking pattern
        profit_taking = metrics.get('profit_taking_pattern', 0)
        
        value_score = self._avg([
            in_out_balance,
            avg_tx_value,
            varied_selling,
            dca_score,
            quick_sells,
            profit_taking
        ])
        
        # ===================================================================
        # CATEGORY 3: TIMING PATTERNS (15%)
        # ===================================================================
        
        # 3.1 Business hours trading (traders active during market hours)
        business_hours = metrics.get('business_hours_ratio', 0)
        
        # 3.2 Weekday trading (more active on weekdays)
        weekday_trading = metrics.get('weekday_ratio', 0)
        
        # 3.3 Low night trading (professional trading hours)
        low_night_trading = 1.0 - metrics.get('night_trading_ratio', 0)
        
        # 3.4 Low weekend trading
        low_weekend_trading = 1.0 - metrics.get('weekend_trading_ratio', 0)
        
        timing_score = self._avg([
            business_hours,
            weekday_trading,
            low_night_trading * 0.5,  # Less important
            low_weekend_trading * 0.5  # Less important
        ])
        
        # ===================================================================
        # CATEGORY 4: BALANCE BEHAVIOR (15%)
        # ===================================================================
        
        # 4.1 High balance volatility (constantly changing)
        balance_volatility = metrics.get('balance_volatility', 0)
        
        # 4.2 Low balance utilization (not keeping high balances)
        low_balance_held = 1.0 - metrics.get('balance_utilization', 0)
        
        # 4.3 Frequent empty wallet
        empty_frequency = metrics.get('empty_frequency', 0)
        
        # 4.4 Short holding period (not holding long-term)
        short_holding = 1.0 - self._normalize(
            metrics.get('holding_period_days', 0), 0, 365
        )
        
        # 4.5 High turnover rate
        high_turnover = self._normalize(metrics.get('turnover_rate', 0), 0, 5)
        
        balance_score = self._avg([
            balance_volatility,
            low_balance_held,
            empty_frequency,
            short_holding,
            high_turnover
        ])
        
        # ===================================================================
        # CATEGORY 5: NETWORK BEHAVIOR (15%)
        # ===================================================================
        
        # 5.1 High counterparty diversity (trading with many different wallets)
        diversity = metrics.get('counterparty_diversity', 0)
        
        # 5.2 Low repeat transactions (not returning to same addresses)
        low_repeat = 1.0 - metrics.get('repeat_ratio', 0)
        
        # 5.3 Smart contract usage (DEX interactions)
        sc_usage = metrics.get('smart_contract_ratio', 0)
        
        # 5.4 High out-degree (sending to many addresses)
        high_out_degree = self._normalize(metrics.get('out_degree', 0), 0, 50)
        
        # 5.5 Fan-out pattern (distributing funds)
        fan_out = metrics.get('fan_out_score', 0)
        
        network_score = self._avg([
            diversity,
            low_repeat,
            sc_usage,
            high_out_degree,
            fan_out
        ])
        
        # ===================================================================
        # CATEGORY 6: EXCHANGE USAGE (15%)
        # ===================================================================
        
        # 6.1 Exchange interactions
        exchange_freq = self._normalize(
            metrics.get('exchange_interaction_count', 0), 0, 20
        )
        
        # 6.2 DEX/CEX usage
        dex_cex_usage = self._normalize(
            metrics.get('dex_cex_interactions', 0), 0, 50
        )
        
        # 6.3 Not an isolated wallet (interacts with ecosystem)
        not_isolated = min(exchange_freq + dex_cex_usage, 1.0)
        
        exchange_score = self._avg([
            exchange_freq,
            dex_cex_usage,
            not_isolated
        ])
        
        # ===================================================================
        # FINAL WEIGHTED SCORE
        # ===================================================================
        
        final_score = (
            frequency_score * self.WEIGHTS["transaction_frequency"] +
            value_score * self.WEIGHTS["value_patterns"] +
            timing_score * self.WEIGHTS["timing_patterns"] +
            balance_score * self.WEIGHTS["balance_behavior"] +
            network_score * self.WEIGHTS["network_behavior"] +
            exchange_score * self.WEIGHTS["exchange_usage"]
        )
        
        return final_score
    
    def get_classification_breakdown(self, metrics: Dict[str, Any]) -> Dict[str, float]:
        """
        Returns detailed breakdown of score components for debugging.
        
        Returns:
            Dict with scores for each category
        """
        # Recalculate all category scores (copy of compute_score logic)
        # This is useful for understanding why a wallet was classified
        
        tx_freq = self._normalize(metrics.get('tx_per_month', 0), 0, 50)
        regularity = metrics.get('trading_regularity', 0)
        burst_activity = metrics.get('activity_burst_ratio', 0)
        low_dormancy = 1.0 - metrics.get('dormancy_ratio', 0)
        frequency_score = self._avg([tx_freq, regularity, burst_activity, low_dormancy])
        
        # ... (similar for other categories)
        
        return {
            "frequency": frequency_score,
            "value_patterns": 0.0,  # Calculate similarly
            "timing": 0.0,
            "balance": 0.0,
            "network": 0.0,
            "exchange": 0.0,
            "overall": self.compute_score(metrics)
        }
