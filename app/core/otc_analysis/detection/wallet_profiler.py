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
        
        ✨ IMPROVED ALGORITHM - Multi-factor OTC detection:
        
        Scoring Categories:
        1. Entity Labels (40 points) - Known OTC desks get high scores
        2. Volume Metrics (30 points) - Large transaction values indicate OTC
        3. Transaction Patterns (20 points) - Behavior analysis
        4. Network Characteristics (10 points) - Counterparty patterns
        
        Returns: 0-1 probability (normalized to 100)
        """
        score = 0
        max_score = 100  # Total possible points
        
        # ============================================================================
        # 1. ENTITY LABELS (40 points max)
        # ============================================================================
        entity_type = profile.get('entity_type', 'unknown')
        entity_name = profile.get('entity_name', '').lower()
        
        # Known OTC desk types
        if entity_type in ['otc_desk', 'market_maker', 'institutional']:
            score += 40
        elif entity_type in ['exchange', 'cex']:
            score += 35
        elif entity_type in ['whale', 'institutional_investor']:
            score += 30
        elif entity_type == 'dex':
            score += 10  # DEXes can also do OTC
        
        # Known OTC desk names (boost score)
        otc_desk_keywords = [
            'jump', 'wintermute', 'cumberland', 'b2c2', 'galaxy',
            'coinbase', 'binance', 'kraken', 'ftx', 'alameda',
            'flowtraders', 'dwf', 'gsr', 'falconx', 'sfox'
        ]
        
        if entity_name and any(keyword in entity_name for keyword in otc_desk_keywords):
            score += 10  # Bonus for known desks
        
        # ============================================================================
        # 2. VOLUME METRICS (30 points max)
        # ============================================================================
        total_volume = profile.get('total_volume_usd', 0)
        avg_transaction = profile.get('avg_transaction_usd', 0)
        
        # Total volume scoring (logarithmic scale)
        if total_volume >= 1_000_000_000:  # $1B+
            score += 15
        elif total_volume >= 100_000_000:  # $100M+
            score += 12
        elif total_volume >= 10_000_000:   # $10M+
            score += 10
        elif total_volume >= 1_000_000:    # $1M+
            score += 7
        elif total_volume >= 100_000:      # $100K+
            score += 5
        
        # Average transaction size (OTC = large transactions)
        if avg_transaction >= 10_000_000:  # $10M+ avg
            score += 15
        elif avg_transaction >= 1_000_000:  # $1M+ avg
            score += 12
        elif avg_transaction >= 500_000:    # $500K+ avg
            score += 10
        elif avg_transaction >= 100_000:    # $100K+ avg
            score += 7
        elif avg_transaction >= 50_000:     # $50K+ avg
            score += 5
        
        # ============================================================================
        # 3. TRANSACTION PATTERNS (20 points max)
        # ============================================================================
        tx_frequency = profile.get('transaction_frequency', 0)
        total_txs = profile.get('total_transactions', 0)
        
        # Low frequency = OTC (institutional, not retail)
        if tx_frequency < 0.1:  # <1 tx per 10 days
            score += 10
        elif tx_frequency < 0.5:  # <0.5 tx/day
            score += 7
        elif tx_frequency < 2:    # <2 tx/day
            score += 5
        
        # High transaction count = established entity
        if total_txs >= 10000:
            score += 5
        elif total_txs >= 1000:
            score += 4
        elif total_txs >= 100:
            score += 3
        
        # DeFi interactions (OTC desks DO use DeFi)
        has_defi = profile.get('has_defi_interactions', False)
        if has_defi:
            score += 5  # Modern OTC desks use DeFi too
        
        # ============================================================================
        # 4. NETWORK CHARACTERISTICS (10 points max)
        # ============================================================================
        unique_counterparties = profile.get('unique_counterparties', 0)
        counterparty_entropy = profile.get('counterparty_entropy', 0)
        
        # Many unique counterparties = active trading desk
        if unique_counterparties >= 1000:
            score += 5
        elif unique_counterparties >= 500:
            score += 4
        elif unique_counterparties >= 100:
            score += 3
        elif unique_counterparties >= 50:
            score += 2
        
        # High entropy = diverse trading (good for OTC)
        if counterparty_entropy >= 5.0:
            score += 5
        elif counterparty_entropy >= 3.0:
            score += 3
        elif counterparty_entropy >= 2.0:
            score += 2
        
        # ============================================================================
        # NORMALIZE & RETURN
        # ============================================================================
        # Normalize to 0-1 range
        probability = min(1.0, score / max_score)
        
        # Apply confidence modifier
        confidence = profile.get('confidence_score', 1.0)
        adjusted_probability = probability * confidence
        
        return adjusted_probability


class WalletDetailsService:
    """Service for fetching and calculating wallet details with live data."""
    
    @staticmethod
    async def get_wallet_details(
        address: str,
        wallet,  # OTCWallet model
        db  # Database session
    ) -> Dict:
        """
        Get comprehensive wallet details with live Etherscan data.
        
        Args:
            address: Wallet address
            wallet: OTCWallet model from database
            db: Database session
        
        Returns:
            Dict with all wallet details
        """
        from ..blockchain.etherscan import EtherscanAPI
        from ..utils.calculations import (
            calculate_wallet_metrics,
            generate_activity_chart,
            generate_transfer_size_chart
        )
        import logging
        
        logger = logging.getLogger(__name__)
        
        is_verified = (wallet.confidence_score or 0) >= 80
        
        logger.info(f"🔍 Wallet verified: {is_verified} (confidence: {wallet.confidence_score}%)")
        
        # Initialize Etherscan client
        etherscan = EtherscanAPI(chain_id=1)
        
        # Fetch live data
        balance_data = etherscan.get_balance(address)
        transactions = etherscan.get_recent_transactions(address, limit=100)
        
        if not balance_data or not transactions:
            logger.warning(f"⚠️ Etherscan failed, using DB data only")
            return WalletDetailsService._create_fallback_response(wallet)
        
        # Calculate metrics
        metrics = calculate_wallet_metrics(
            balance_eth=balance_data["balance_eth"],
            transactions=transactions
        )
        
        # Update DB if verified
        if is_verified:
            logger.info(f"💾 VERIFIED wallet - Updating DB with live data")
            
            wallet.total_volume = metrics["balance_usd"]
            wallet.transaction_count = metrics["transaction_count"]
            wallet.last_active = datetime.fromtimestamp(metrics["last_tx_timestamp"])
            wallet.is_active = metrics["is_active"]
            
            if not isinstance(wallet.tags, list):
                wallet.tags = []
            if "verified_live" not in wallet.tags:
                wallet.tags.append("verified_live")
            
            db.commit()
            db.refresh(wallet)
            
            logger.info(f"✅ DB updated: ${wallet.total_volume:,.2f}, {wallet.transaction_count} txs")
        else:
            logger.info(f"⚠️ UNVERIFIED wallet - Only showing data, NOT updating DB")
        
        # Generate chart data
        activity_data = generate_activity_chart(
            total_volume=metrics["total_volume"],
            transaction_count=metrics["transaction_count"],
            transactions=transactions
        )
        
        transfer_size_data = generate_transfer_size_chart(
            avg_transfer=metrics["avg_transfer_size"],
            transactions=transactions
        )
        
        # Return complete response
        return {
            "address": wallet.address,
            "label": wallet.label,
            "entity_type": wallet.entity_type,
            "entity_name": wallet.entity_name,
            "confidence_score": float(wallet.confidence_score or 0),
            "is_active": bool(metrics["is_active"]),
            "is_verified": bool(is_verified),
            
            "balance_eth": float(metrics["balance_eth"]),
            "balance_usd": float(metrics["balance_usd"]),
            "lifetime_volume": float(metrics["balance_usd"]),
            "volume_30d": float(metrics["recent_volume_30d"]),
            "volume_7d": float(metrics["recent_volume_7d"]),
            "avg_transfer": float(metrics["avg_transfer_size"]),
            "transaction_count": int(metrics["transaction_count"]),
            "last_activity": str(metrics["last_activity_text"]),
            
            "activity_data": activity_data,
            "transfer_size_data": transfer_size_data,
            "tags": wallet.tags if isinstance(wallet.tags, list) else [],
            
            "data_source": "etherscan_live" if is_verified else "etherscan_display",
            "last_updated": datetime.now().isoformat()
        }
    
    @staticmethod
    def _create_fallback_response(wallet) -> Dict:
        """Create fallback response when Etherscan unavailable."""
        from ..utils.calculations import (
            generate_activity_chart,
            generate_transfer_size_chart
        )
        
        activity_data = generate_activity_chart(
            total_volume=wallet.total_volume or 0,
            transaction_count=wallet.transaction_count or 0
        )
        
        transfer_size_data = generate_transfer_size_chart(
            avg_transfer=(wallet.total_volume or 0) / max(1, wallet.transaction_count or 1)
        )
        
        return {
            "address": wallet.address,
            "label": wallet.label,
            "entity_type": wallet.entity_type,
            "entity_name": wallet.entity_name,
            "confidence_score": float(wallet.confidence_score or 0),
            "is_active": bool(wallet.is_active),
            "is_verified": False,
            
            "lifetime_volume": float(wallet.total_volume or 0),
            "volume_30d": float(wallet.total_volume or 0) * 0.6,
            "volume_7d": float(wallet.total_volume or 0) * 0.2,
            "avg_transfer": float(wallet.total_volume or 0) / max(1, wallet.transaction_count or 1),
            "transaction_count": wallet.transaction_count or 0,
            "last_activity": "Unknown",
            
            "activity_data": activity_data,
            "transfer_size_data": transfer_size_data,
            "tags": wallet.tags if isinstance(wallet.tags, list) else [],
            
            "data_source": "database",
            "last_updated": datetime.now().isoformat()
        }
