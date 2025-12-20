from typing import List, Dict, Optional
from datetime import datetime, timedelta
from collections import Counter
from app.core.otc_analysis.utils.calculations import (
    shannon_entropy,
    calculate_transaction_velocity,
    rolling_statistics
)

class WalletProfiler:
    """
    Creates detailed behavioral profiles for wallet addresses.
    
    Profiles include:
    - Activity metrics (frequency, volume, velocity)
    - Behavioral patterns (DeFi usage, counterparty diversity)
    - Timing patterns (active hours, weekend activity)
    - Network position
    """
    
    def __init__(self):
        self.min_transactions_for_profile = 5
    
    def create_profile(
        self,
        address: str,
        transactions: List[Dict],
        labels: Optional[Dict] = None
    ) -> Dict:
        """
        Create comprehensive wallet profile.
        
        Args:
            address: Wallet address to profile
            transactions: All transactions involving this address
            labels: Optional external labels
        
        Returns:
            Complete wallet profile
        """
        if len(transactions) < self.min_transactions_for_profile:
            return self._create_minimal_profile(address, transactions, labels)
        
        # Separate incoming and outgoing
        incoming = [tx for tx in transactions if tx.get('to_address') == address]
        outgoing = [tx for tx in transactions if tx.get('from_address') == address]
        
        # Activity metrics
        activity_metrics = self._calculate_activity_metrics(address, transactions)
        
        # Volume metrics
        volume_metrics = self._calculate_volume_metrics(incoming, outgoing)
        
        # Behavioral analysis
        behavioral = self._analyze_behavior(transactions)
        
        # Timing patterns
        timing = self._analyze_timing_patterns(transactions)
        
        # Counterparty analysis
        counterparty = self._analyze_counterparties(address, transactions)
        
        # Build complete profile
        profile = {
            'address': address,
            'entity_type': labels.get('entity_type', 'unknown') if labels else 'unknown',
            'entity_name': labels.get('entity_name') if labels else None,
            
            # Activity
            'first_seen': activity_metrics['first_seen'],
            'last_seen': activity_metrics['last_seen'],
            'total_transactions': activity_metrics['total_transactions'],
            'transaction_frequency': activity_metrics['transaction_frequency'],
            
            # Volume
            'total_volume_usd': volume_metrics['total_volume'],
            'avg_transaction_usd': volume_metrics['avg_transaction'],
            'median_transaction_usd': volume_metrics['median_transaction'],
            
            # Behavior
            'has_defi_interactions': behavioral['has_defi'],
            'has_dex_swaps': behavioral['has_dex'],
            'has_contract_deployments': behavioral['has_deployments'],
            
            # Counterparties
            'unique_counterparties': counterparty['unique_count'],
            'counterparty_entropy': counterparty['entropy'],
            
            # Timing
            'active_hours': timing['active_hours'],
            'active_days': timing['active_days'],
            'weekend_activity_ratio': timing['weekend_ratio'],
            
            # Labels
            'labels': labels.get('labels', []) if labels else [],
            
            # Metadata
            'last_analyzed': datetime.utcnow(),
            'confidence_score': self._calculate_profile_confidence(transactions)
        }
        
        return profile
    
    def _create_minimal_profile(
        self,
        address: str,
        transactions: List[Dict],
        labels: Optional[Dict]
    ) -> Dict:
        """Create minimal profile for addresses with few transactions."""
        return {
            'address': address,
            'entity_type': labels.get('entity_type', 'unknown') if labels else 'unknown',
            'total_transactions': len(transactions),
            'confidence_score': 0.3,  # Low confidence
            'last_analyzed': datetime.utcnow()
        }
    
    def _calculate_activity_metrics(self, address: str, transactions: List[Dict]) -> Dict:
        """Calculate wallet activity metrics."""
        if not transactions:
            return {
                'first_seen': None,
                'last_seen': None,
                'total_transactions': 0,
                'transaction_frequency': 0
            }
        
        timestamps = [tx['timestamp'] for tx in transactions if tx.get('timestamp')]
        
        first_seen = min(timestamps) if timestamps else None
        last_seen = max(timestamps) if timestamps else None
        
        # Calculate transaction frequency (tx per day)
        tx_frequency = calculate_transaction_velocity(timestamps) if timestamps else 0
        
        return {
            'first_seen': first_seen,
            'last_seen': last_seen,
            'total_transactions': len(transactions),
            'transaction_frequency': tx_frequency
        }
    
    def _calculate_volume_metrics(self, incoming: List[Dict], outgoing: List[Dict]) -> Dict:
        """Calculate volume-related metrics."""
        all_txs = incoming + outgoing
        values = [tx.get('usd_value', 0) for tx in all_txs if tx.get('usd_value')]
        
        if not values:
            return {
                'total_volume': 0,
                'avg_transaction': 0,
                'median_transaction': 0
            }
        
        stats = rolling_statistics(values)
        
        return {
            'total_volume': sum(values),
            'avg_transaction': stats['mean'],
            'median_transaction': stats['median']
        }
    
    def _analyze_behavior(self, transactions: List[Dict]) -> Dict:
        """Analyze behavioral patterns."""
        has_defi = any(tx.get('is_contract_interaction') for tx in transactions)
        
        # Check for DEX swaps (simplified - would need more sophisticated detection)
        has_dex = any(
            tx.get('is_contract_interaction') and tx.get('method_id') in [
                '0x38ed1739',  # swapExactTokensForTokens (Uniswap)
                '0x7ff36ab5',  # swapExactETHForTokens
                '0x18cbafe5',  # swapExactTokensForETH
            ]
            for tx in transactions
        )
        
        # Check for contract deployments (to_address is None)
        has_deployments = any(tx.get('to_address') is None for tx in transactions)
        
        return {
            'has_defi': has_defi,
            'has_dex': has_dex,
            'has_deployments': has_deployments
        }
    
    def _analyze_timing_patterns(self, transactions: List[Dict]) -> Dict:
        """Analyze when the wallet is active."""
        timestamps = [tx['timestamp'] for tx in transactions if tx.get('timestamp')]
        
        if not timestamps:
            return {
                'active_hours': [],
                'active_days': [],
                'weekend_ratio': 0
            }
        
        # Active hours (UTC)
        hours = [ts.hour for ts in timestamps]
        active_hours = list(set(hours))
        
        # Active days of week
        days = [ts.weekday() for ts in timestamps]
        active_days = list(set(days))
        
        # Weekend activity ratio
        weekend_txs = sum(1 for day in days if day >= 5)  # Saturday=5, Sunday=6
        weekend_ratio = weekend_txs / len(days) if days else 0
        
        return {
            'active_hours': sorted(active_hours),
            'active_days': sorted(active_days),
            'weekend_ratio': weekend_ratio
        }
    
    def _analyze_counterparties(self, address: str, transactions: List[Dict]) -> Dict:
        """Analyze counterparty diversity."""
        counterparties = []
        
        for tx in transactions:
            if tx.get('from_address') == address:
                counterparties.append(tx.get('to_address'))
            elif tx.get('to_address') == address:
                counterparties.append(tx.get('from_address'))
        
        # Remove None values
        counterparties = [cp for cp in counterparties if cp]
        
        unique_count = len(set(counterparties))
        
        # Calculate Shannon entropy for diversity
        entropy = shannon_entropy(counterparties) if counterparties else 0
        
        return {
            'unique_count': unique_count,
            'entropy': entropy,
            'counterparties': list(set(counterparties))
        }
    
    def _calculate_profile_confidence(self, transactions: List[Dict]) -> float:
        """
        Calculate confidence score for profile accuracy.
        
        More transactions = higher confidence
        """
        tx_count = len(transactions)
        
        if tx_count >= 100:
            return 1.0
        elif tx_count >= 50:
            return 0.9
        elif tx_count >= 20:
            return 0.7
        elif tx_count >= 10:
            return 0.5
        else:
            return 0.3
    
    def update_profile(
        self,
        existing_profile: Dict,
        new_transactions: List[Dict]
    ) -> Dict:
        """
        Update existing profile with new transaction data.
        
        More efficient than rebuilding from scratch.
        """
        address = existing_profile['address']
        
        # Combine with existing transactions count
        total_txs = existing_profile.get('total_transactions', 0) + len(new_transactions)
        
        # Update last seen
        new_timestamps = [tx['timestamp'] for tx in new_transactions if tx.get('timestamp')]
        if new_timestamps:
            existing_profile['last_seen'] = max(new_timestamps)
        
        # Recalculate metrics (simplified - in production would be incremental)
        existing_profile['total_transactions'] = total_txs
        existing_profile['last_analyzed'] = datetime.utcnow()
        
        return existing_profile
    
    def batch_profile(
        self,
        addresses: List[str],
        transactions_by_address: Dict[str, List[Dict]],
        labels_by_address: Optional[Dict[str, Dict]] = None
    ) -> Dict[str, Dict]:
        """
        Create profiles for multiple addresses in batch.
        
        Returns:
            Dict mapping addresses to profiles
        """
        profiles = {}
        
        for address in addresses:
            txs = transactions_by_address.get(address, [])
            labels = labels_by_address.get(address) if labels_by_address else None
            
            profile = self.create_profile(address, txs, labels)
            profiles[address] = profile
        
        return profiles
    
    def calculate_otc_probability(self, profile: Dict) -> float:
        """
        Calculate OTC probability score based on profile.
        
        Returns: 0-1 probability
        """
        score = 0
        
        # Low transaction frequency
        if profile.get('transaction_frequency', 0) < 0.5:  # <0.5 tx/day
            score += 0.25
        
        # High average value
        if profile.get('avg_transaction_usd', 0) > 100000:
            score += 0.30
        
        # No DeFi interactions
        if not profile.get('has_defi_interactions', True):
            score += 0.25
        
        # Low counterparty entropy (repeated partners)
        if profile.get('counterparty_entropy', 10) < 2.0:
            score += 0.20
        
        return min(1.0, score)
