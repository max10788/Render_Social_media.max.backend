from typing import Dict, List, Optional, Tuple
from datetime import datetime, time
from app.core.otc_analysis.utils.calculations import (
    calculate_z_score,
    rolling_statistics,
    is_round_number,
    percentile_rank
)

class HeuristicAnalyzer:
    """
    Implements heuristic indicators for OTC activity detection.
    
    Based on doc methodology:
    1. Transfer Size Anomaly
    2. Wallet Behavior Profiles
    3. Timing Analysis
    4. Round Numbers & Price Fixation
    """
    
    def __init__(self):
        # Thresholds from doc
        self.min_otc_value_usd = 100000  # $100K minimum
        self.large_transfer_threshold = 1000000  # $1M for high confidence
        
        # Off-hours definition (22:00-06:00 UTC)
        self.off_hours_start = time(22, 0)
        self.off_hours_end = time(6, 0)
    
    def analyze_transfer_size(
        self,
        transaction: Dict,
        historical_transactions: List[Dict]
    ) -> Dict:
        """
        Transfer Size Anomaly Detection.
        
        Theory: OTC trades are typically >$100K, often >$1M
        Implementation: Z-score against rolling 30-day window
        
        Returns:
            {
                'is_anomaly': bool,
                'z_score': float,
                'percentile': float,
                'score': float (0-100)
            }
        """
        usd_value = transaction.get('usd_value', 0)
        
        if not usd_value or usd_value < self.min_otc_value_usd:
            return {
                'is_anomaly': False,
                'z_score': 0,
                'percentile': 0,
                'score': 0
            }
        
        # Get historical values for rolling window
        historical_values = [tx['usd_value'] for tx in historical_transactions if tx.get('usd_value')]
        
        if len(historical_values) < 10:  # Not enough data
            # Use simple threshold instead
            score = min(100, (usd_value / self.large_transfer_threshold) * 100)
            return {
                'is_anomaly': usd_value >= self.large_transfer_threshold,
                'z_score': None,
                'percentile': None,
                'score': score
            }
        
        # Calculate statistics
        stats = rolling_statistics(historical_values)
        z_score = calculate_z_score(usd_value, stats['mean'], stats['std'])
        percentile = percentile_rank(usd_value, historical_values)
        
        # Anomaly if Z-score > 2 (2 standard deviations)
        is_anomaly = z_score > 2.0
        
        # Score based on percentile and Z-score
        score = min(100, (percentile * 0.7 + min(z_score * 10, 30)))
        
        return {
            'is_anomaly': is_anomaly,
            'z_score': z_score,
            'percentile': percentile,
            'score': score
        }
    
    def analyze_wallet_behavior(self, wallet_data: Dict) -> Dict:
        """
        Wallet Behavior Profile Analysis.
        
        Theory: OTC wallets show specific patterns:
        - Low transaction frequency (<10/month)
        - High average transaction value
        - Direct P2P transfers (no smart contract interaction)
        - No DEX swaps or DeFi protocol usage
        
        Returns:
            {
                'is_otc_profile': bool,
                'score': float (0-100),
                'indicators': dict
            }
        """
        indicators = {
            'low_frequency': False,
            'high_avg_value': False,
            'no_defi': False,
            'p2p_transfers': False
        }
        
        score = 0
        
        # 1. Transaction Frequency Check (<10 per month = 0.33 per day)
        tx_frequency = wallet_data.get('transaction_frequency', 0)
        if tx_frequency < 0.33:
            indicators['low_frequency'] = True
            score += 25
        
        # 2. High Average Transaction Value (>$50K)
        avg_tx_value = wallet_data.get('avg_transaction_usd', 0)
        if avg_tx_value > 50000:
            indicators['high_avg_value'] = True
            score += 25
        
        # 3. No DeFi Interactions
        has_defi = wallet_data.get('has_defi_interactions', False)
        has_dex = wallet_data.get('has_dex_swaps', False)
        if not has_defi and not has_dex:
            indicators['no_defi'] = True
            score += 25
        
        # 4. Primarily P2P Transfers
        # If most transactions are NOT contract interactions
        if not wallet_data.get('has_defi_interactions', True):
            indicators['p2p_transfers'] = True
            score += 25
        
        is_otc_profile = score >= 50  # At least 2 out of 4 indicators
        
        return {
            'is_otc_profile': is_otc_profile,
            'score': score,
            'indicators': indicators
        }
    
    def analyze_timing(self, transaction: Dict) -> Dict:
        """
        Timing Analysis.
        
        Theory: OTC trades often occur:
        - Outside regular trading hours (22:00-06:00 UTC)
        - On weekends
        - No correlation with exchange volume spikes
        
        Returns:
            {
                'is_off_hours': bool,
                'is_weekend': bool,
                'hour': int,
                'day_of_week': int,
                'score': float (0-100)
            }
        """
        timestamp = transaction.get('timestamp')
        
        if not timestamp:
            return {
                'is_off_hours': False,
                'is_weekend': False,
                'hour': None,
                'day_of_week': None,
                'score': 0
            }
        
        hour = timestamp.hour
        day_of_week = timestamp.weekday()  # 0=Monday, 6=Sunday
        
        # Check if off-hours (22:00-06:00 UTC)
        is_off_hours = (
            hour >= self.off_hours_start.hour or 
            hour < self.off_hours_end.hour
        )
        
        # Check if weekend (Saturday=5, Sunday=6)
        is_weekend = day_of_week >= 5
        
        # Calculate score
        score = 0
        if is_off_hours:
            score += 50
        if is_weekend:
            score += 50
        
        return {
            'is_off_hours': is_off_hours,
            'is_weekend': is_weekend,
            'hour': hour,
            'day_of_week': day_of_week,
            'score': min(100, score)
        }
    
    def analyze_round_numbers(self, transaction: Dict) -> Dict:
        """
        Round Number & Price Fixation Detection.
        
        Theory: OTC deals often negotiated at psychologically round amounts
        Pattern: Transfers of exactly $1M, $5M, $10M USD equivalent
        
        Returns:
            {
                'is_round': bool,
                'level': str,
                'score': float (0-100)
            }
        """
        usd_value = transaction.get('usd_value', 0)
        
        if not usd_value:
            return {
                'is_round': False,
                'level': None,
                'score': 0
            }
        
        is_round, level = is_round_number(usd_value, tolerance=0.01)
        
        # Higher score for rounder numbers
        score_map = {
            'ten_million': 100,
            'five_million': 90,
            'million': 80,
            'half_million': 70,
            'hundred_k': 60
        }
        
        score = score_map.get(level, 0) if is_round else 0
        
        return {
            'is_round': is_round,
            'level': level,
            'score': score
        }
    
    def comprehensive_heuristic_analysis(
        self,
        transaction: Dict,
        wallet_data: Dict,
        historical_transactions: List[Dict]
    ) -> Dict:
        """
        Run all heuristic analyses and combine results.
        
        Returns comprehensive analysis report.
        """
        # Run all analyses
        size_analysis = self.analyze_transfer_size(transaction, historical_transactions)
        behavior_analysis = self.analyze_wallet_behavior(wallet_data)
        timing_analysis = self.analyze_timing(transaction)
        round_number_analysis = self.analyze_round_numbers(transaction)
        
        # Collect matched patterns
        patterns = []
        
        if size_analysis['is_anomaly']:
            patterns.append('large_transfer')
        if behavior_analysis['is_otc_profile']:
            patterns.append('otc_wallet_profile')
        if timing_analysis['is_off_hours']:
            patterns.append('off_hours')
        if timing_analysis['is_weekend']:
            patterns.append('weekend_activity')
        if round_number_analysis['is_round']:
            patterns.append('round_number')
        
        # Combined score (weighted average)
        combined_score = (
            size_analysis['score'] * 0.35 +
            behavior_analysis['score'] * 0.25 +
            timing_analysis['score'] * 0.20 +
            round_number_analysis['score'] * 0.20
        )
        
        return {
            'combined_score': combined_score,
            'patterns_matched': patterns,
            'pattern_count': len(patterns),
            'analyses': {
                'transfer_size': size_analysis,
                'wallet_behavior': behavior_analysis,
                'timing': timing_analysis,
                'round_number': round_number_analysis
            },
            'is_suspected_otc': combined_score >= 60  # Threshold for suspicion
        }
    
    def batch_analyze(
        self,
        transactions: List[Dict],
        wallet_profiles: Dict[str, Dict]
    ) -> List[Dict]:
        """
        Analyze multiple transactions in batch.
        
        Args:
            transactions: List of transactions to analyze
            wallet_profiles: Dict mapping addresses to wallet data
        
        Returns:
            List of analysis results
        """
        results = []
        
        for tx in transactions:
            from_addr = tx.get('from_address')
            to_addr = tx.get('to_address')
            
            # Get wallet data (use from_address as primary)
            wallet_data = wallet_profiles.get(from_addr, {})
            
            # Get historical transactions for this wallet
            # In practice, this would come from database
            historical = []
            
            analysis = self.comprehensive_heuristic_analysis(
                tx, wallet_data, historical
            )
            
            results.append({
                'tx_hash': tx.get('tx_hash'),
                'analysis': analysis
            })
        
        return results
