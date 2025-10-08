# wallet_classifier/hodler/classifier.py

from typing import Dict, List, Any
import numpy as np
from datetime import datetime, timedelta
from ..core.base_classifier import BaseClassifier, WalletClass, WalletData
from ..core.metrics import MetricCalculator

class HodlerClassifier(BaseClassifier):
    """Classifier for identifying hodler wallets (long-term holders)"""
    
    def get_thresholds(self) -> Dict[str, float]:
        return {
            'basic': 0.70,
            'intermediate': 0.75,
            'advanced': 0.80
        }
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'primary': 0.80,
            'secondary': 0.20,
            'context': 0.15
        }
    
    def get_wallet_class(self) -> WalletClass:
        return WalletClass.HODLER
    
    def calculate_stage1_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """
        Stage 1 - Primary Metrics (5):
        1. Holding period (365-5000+ days expected)
        2. UTXO age distribution (500+ days average)
        3. Balance retention ratio (0.9-1.0)
        4. Transaction dormancy score
        5. Accumulation pattern strength
        """
        metrics = {}
        transactions = wallet_data.transactions
        
        # 1. Holding period
        holding_period = MetricCalculator.calculate_holding_period(
            transactions, wallet_data.balance
        )
        # Normalize: 365 days = 0.5, 1000+ days = 1.0
        metrics['primary_holding_period'] = min(1.0, max(0, (holding_period - 180) / 820))
        
        # 2. UTXO age distribution (for UTXO chains) or transaction age
        transaction_ages = []
        current_time = datetime.now()
        
        for tx in transactions:
            if tx.get('type') == 'receive' or tx.get('value', 0) > 0:
                tx_time = datetime.fromtimestamp(tx.get('timestamp', 0))
                age_days = (current_time - tx_time).days
                transaction_ages.append(age_days)
        
        if transaction_ages:
            avg_age = np.mean(transaction_ages)
            # Normalize: 500 days average = 0.5, 1000+ = 1.0
            metrics['primary_utxo_age'] = min(1.0, avg_age / 1000)
        else:
            metrics['primary_utxo_age'] = 0
        
        # 3. Balance retention ratio
        if wallet_data.total_received > 0:
            retention = wallet_data.balance / wallet_data.total_received
            metrics['primary_balance_retention'] = min(1.0, retention)
        else:
            metrics['primary_balance_retention'] = 0
        
        # 4. Transaction dormancy score
        if transactions:
            last_tx_time = max(tx.get('timestamp', 0) for tx in transactions)
            last_tx_date = datetime.fromtimestamp(last_tx_time)
            dormancy_days = (current_time - last_tx_date).days
            
            # Score higher for longer dormancy (no recent transactions)
            metrics['primary_dormancy_score'] = min(1.0, dormancy_days / 365)
        else:
            metrics['primary_dormancy_score'] = 0
        
        # 5. Accumulation pattern strength
        accumulation_score = self._calculate_accumulation_pattern(transactions)
        metrics['primary_accumulation_pattern'] = accumulation_score
        
        return metrics
    
    def calculate_stage2_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """
        Stage 2 - Secondary Metrics (3):
        1. Withdrawal frequency (should be very low)
        2. DCA pattern detection
        3. Panic event resistance
        """
        metrics = {}
        transactions = wallet_data.transactions
        
        # 1. Withdrawal frequency
        withdrawal_count = sum(1 for tx in transactions 
                              if tx.get('type') == 'send' or 
                              (tx.get('from') == wallet_data.address and tx.get('value', 0) > 0))
        
        total_days = self._calculate_wallet_age_days(transactions)
        if total_days > 0:
            # Withdrawals per year (lower is better for hodlers)
            withdrawals_per_year = (withdrawal_count / total_days) * 365
            # Invert: fewer withdrawals = higher score
            metrics['secondary_withdrawal_frequency'] = max(0, 1.0 - (withdrawals_per_year / 12))
        else:
            metrics['secondary_withdrawal_frequency'] = 1.0
        
        # 2. DCA (Dollar Cost Averaging) pattern detection
        dca_score = self._detect_dca_pattern(transactions)
        metrics['secondary_dca_pattern'] = dca_score
        
        # 3. Panic event resistance (held through volatility)
        panic_resistance = self._calculate_panic_resistance(transactions, wallet_data.balance)
        metrics['secondary_panic_resistance'] = panic_resistance
        
        return metrics
    
    def calculate_stage3_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """
        Stage 3 - Context Metrics (4):
        1. Staking participation
        2. Cold storage indicators
        3. Round number accumulation
        4. Diamond hands score
        """
        metrics = {}
        transactions = wallet_data.transactions
        
        # 1. Staking participation
        staking_score = self._detect_staking_activity(transactions)
        metrics['context_staking_participation'] = staking_score
        
        # 2. Cold storage indicators
        cold_storage_score = self._detect_cold_storage_patterns(transactions, wallet_data)
        metrics['context_cold_storage'] = cold_storage_score
        
        # 3. Round number accumulation (psychological levels)
        round_number_score = self._detect_round_number_targets(wallet_data.balance, transactions)
        metrics['context_round_numbers'] = round_number_score
        
        # 4. Diamond hands score (never sold despite opportunities)
        diamond_hands = self._calculate_diamond_hands_score(transactions, wallet_data)
        metrics['context_diamond_hands'] = diamond_hands
        
        return metrics
    
    def _calculate_accumulation_pattern(self, transactions: List[Dict]) -> float:
        """Detect consistent accumulation behavior"""
        receive_txs = [tx for tx in transactions 
                      if tx.get('type') == 'receive' or 
                      (tx.get('to') and tx.get('value', 0) > 0)]
        
        send_txs = [tx for tx in transactions 
                   if tx.get('type') == 'send' or 
                   (tx.get('from') and tx.get('value', 0) > 0)]
        
        if len(receive_txs) == 0:
            return 0
        
        # High receive to send ratio indicates accumulation
        accumulation_ratio = len(receive_txs) / (len(send_txs) + 1)
        
        # Check for consistent buying over time
        if len(receive_txs) > 5:
            timestamps = sorted([tx.get('timestamp', 0) for tx in receive_txs])
            intervals = np.diff(timestamps)
            
            if len(intervals) > 0:
                # Regular intervals suggest systematic accumulation
                cv = np.std(intervals) / np.mean(intervals) if np.mean(intervals) > 0 else 1
                consistency = 1.0 - min(1.0, cv)
                return min(1.0, accumulation_ratio / 10) * consistency
        
        return min(1.0, accumulation_ratio / 10)
    
    def _calculate_wallet_age_days(self, transactions: List[Dict]) -> float:
        """Calculate wallet age in days"""
        if not transactions:
            return 0
        
        first_tx = min(transactions, key=lambda x: x.get('timestamp', float('inf')))
        first_time = datetime.fromtimestamp(first_tx.get('timestamp', 0))
        age_days = (datetime.now() - first_time).days
        
        return max(0, age_days)
    
    def _detect_dca_pattern(self, transactions: List[Dict]) -> float:
        """Detect Dollar Cost Averaging pattern"""
        receive_txs = sorted(
            [tx for tx in transactions if tx.get('type') == 'receive'],
            key=lambda x: x.get('timestamp', 0)
        )
        
        if len(receive_txs) < 5:
            return 0
        
        # Check for regular intervals
        timestamps = [tx.get('timestamp', 0) for tx in receive_txs]
        intervals = np.diff(timestamps)
        
        if len(intervals) == 0:
            return 0
        
        # Look for weekly/monthly patterns
        avg_interval = np.mean(intervals)
        week_seconds = 7 * 24 * 3600
        month_seconds = 30 * 24 * 3600
        
        # Check if close to weekly or monthly
        weekly_score = max(0, 1.0 - abs(avg_interval - week_seconds) / week_seconds)
        monthly_score = max(0, 1.0 - abs(avg_interval - month_seconds) / month_seconds)
        
        # Also check consistency
        cv = np.std(intervals) / np.mean(intervals) if np.mean(intervals) > 0 else 1
        consistency = max(0, 1.0 - cv)
        
        return max(weekly_score, monthly_score) * consistency
    
    def _calculate_panic_resistance(self, transactions: List[Dict], current_balance: float) -> float:
        """Calculate resistance to panic selling during market downturns"""
        if current_balance <= 0:
            return 0
        
        # Look for periods of high activity (potential panic events)
        if len(transactions) < 10:
            return 0.5  # Neutral score for insufficient data
        
        # Group transactions by time windows
        timestamps = [tx.get('timestamp', 0) for tx in transactions]
        
        # Identify high-activity periods (potential market events)
        time_windows = {}
        window_size = 7 * 24 * 3600  # 7 days
        
        for tx in transactions:
            window = tx.get('timestamp', 0) // window_size
            if window not in time_windows:
                time_windows[window] = {'sends': 0, 'receives': 0}
            
            if tx.get('type') == 'send':
                time_windows[window]['sends'] += 1
            else:
                time_windows[window]['receives'] += 1
        
        # Check if held through high-activity periods
        panic_periods = [w for w in time_windows.values() 
                        if w['sends'] + w['receives'] > 5]
        
        if not panic_periods:
            return 0.8  # Good score if no high activity
        
        # Calculate hold ratio during panic periods
        total_panic_sends = sum(p['sends'] for p in panic_periods)
        total_panic_receives = sum(p['receives'] for p in panic_periods)
        
        if total_panic_receives > 0:
            hold_ratio = 1.0 - (total_panic_sends / (total_panic_receives + total_panic_sends))
            return hold_ratio
        
        return 0.5
    
    def _detect_staking_activity(self, transactions: List[Dict]) -> float:
        """Detect staking or yield farming activity"""
        staking_indicators = 0
        total_transactions = len(transactions)
        
        if total_transactions == 0:
            return 0
        
        for tx in transactions:
            # Look for staking-related patterns
            to_addr = str(tx.get('to', '')).lower()
            memo = str(tx.get('memo', '')).lower()
            
            # Check for staking keywords
            staking_keywords = ['stake', 'validator', 'delegate', 'reward', 'compound']
            
            if any(keyword in to_addr for keyword in staking_keywords):
                staking_indicators += 1
            if any(keyword in memo for keyword in staking_keywords):
                staking_indicators += 1
            
            # Check for regular small incoming transactions (rewards)
            if tx.get('type') == 'receive':
                value = tx.get('value', 0)
                # Small regular payments might be staking rewards
                if 0 < value < 100:
                    staking_indicators += 0.1
        
        return min(1.0, staking_indicators / (total_transactions * 0.2))
    
    def _detect_cold_storage_patterns(self, transactions: List[Dict], wallet_data: WalletData) -> float:
        """Detect patterns indicating cold storage usage"""
        indicators = []
        
        # 1. Very low transaction frequency
        tx_per_year = (len(transactions) / max(1, self._calculate_wallet_age_days(transactions))) * 365
        if tx_per_year < 12:  # Less than monthly
            indicators.append(1.0)
        else:
            indicators.append(max(0, 1.0 - (tx_per_year - 12) / 100))
        
        # 2. Large single deposits followed by long dormancy
        if transactions:
            sorted_txs = sorted(transactions, key=lambda x: x.get('timestamp', 0))
            
            for i, tx in enumerate(sorted_txs[:-1]):
                if tx.get('value', 0) > wallet_data.balance * 0.5:  # Large deposit
                    next_tx = sorted_txs[i + 1]
                    time_gap = next_tx.get('timestamp', 0) - tx.get('timestamp', 0)
                    if time_gap > 30 * 24 * 3600:  # 30+ days dormancy
                        indicators.append(1.0)
                        break
        
        # 3. No complex interactions (simple receives only)
        simple_tx_ratio = sum(1 for tx in transactions 
                             if tx.get('type') == 'receive' and 
                             not tx.get('smart_contract_interaction', False))
        
        if transactions:
            indicators.append(simple_tx_ratio / len(transactions))
        
        return np.mean(indicators) if indicators else 0
    
    def _detect_round_number_targets(self, balance: float, transactions: List[Dict]) -> float:
        """Detect accumulation to round psychological numbers"""
        # Common round number targets in crypto
        round_targets = [1, 10, 21, 50, 100, 500, 1000, 10000]
        
        # Check if current balance is near a round number
        balance_score = 0
        for target in round_targets:
            if 0.95 * target <= balance <= 1.05 * target:
                balance_score = 0.8
                break
        
        # Check if accumulation stopped at round numbers
        accumulation_scores = []
        running_balance = 0
        
        for tx in sorted(transactions, key=lambda x: x.get('timestamp', 0)):
            if tx.get('type') == 'receive':
                running_balance += tx.get('value', 0)
                
                for target in round_targets:
                    if 0.95 * target <= running_balance <= 1.05 * target:
                        # Check if accumulation slowed after reaching target
                        accumulation_scores.append(1.0)
                        break
        
        if accumulation_scores:
            return max(balance_score, np.mean(accumulation_scores))
        
        return balance_score
    
    def _calculate_diamond_hands_score(self, transactions: List[Dict], wallet_data: WalletData) -> float:
        """Calculate 'diamond hands' score - holding despite price movements"""
        if wallet_data.balance <= 0 or not transactions:
            return 0
        
        # Calculate unrealized holding time
        holding_days = self._calculate_wallet_age_days(transactions)
        
        if holding_days < 365:
            return 0  # Need at least a year for diamond hands
        
        # Check sell pressure resistance
        sell_transactions = sum(1 for tx in transactions if tx.get('type') == 'send')
        buy_transactions = sum(1 for tx in transactions if tx.get('type') == 'receive')
        
        if buy_transactions == 0:
            return 0
        
        # Lower sell ratio = stronger hands
        sell_ratio = sell_transactions / (buy_transactions + sell_transactions)
        
        # Combine holding time and sell resistance
        time_score = min(1.0, holding_days / 1095)  # 3 years = max
        behavior_score = 1.0 - sell_ratio
        
        return time_score * behavior_score
