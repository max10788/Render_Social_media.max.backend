from typing import List, Dict, Optional
from datetime import datetime, timedelta
from app.core.otc_analysis.analysis.heuristics import HeuristicAnalyzer
from app.core.otc_analysis.analysis.scoring import OTCScoringSystem
from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService
from app.core.otc_analysis.data_sources.otc_desks import OTCDeskRegistry
from app.core.otc_analysis.data_sources.wallet_labels import WalletLabelingService
from app.core.otc_analysis.utils.cache import CacheManager

class OTCDetector:
    """
    Main OTC Detection Service.
    
    Combines all analysis components to detect OTC activity:
    - Heuristic analysis
    - Scoring system
    - Network analysis
    - Known entity matching
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        otc_registry: Optional[OTCDeskRegistry] = None,
        labeling_service: Optional[WalletLabelingService] = None
    ):
        self.cache = cache_manager
        self.otc_registry = otc_registry or OTCDeskRegistry(cache_manager)
        self.labeling_service = labeling_service or WalletLabelingService(cache_manager)
        
        # Initialize analysis components
        self.heuristic_analyzer = HeuristicAnalyzer()
        self.scoring_system = OTCScoringSystem()
        self.network_analyzer = NetworkAnalysisService()
        
        # Detection thresholds
        self.min_usd_value = 100000  # $100K minimum
        self.high_confidence_threshold = 80
        self.medium_confidence_threshold = 60
    
    def detect_otc_transaction(
        self,
        transaction: Dict,
        wallet_profile: Dict,
        historical_transactions: List[Dict],
        network_metrics: Optional[Dict] = None
    ) -> Dict:
        """
        Detect if a single transaction is OTC activity.
        
        This is the main detection method that combines all analyses.
        
        Args:
            transaction: Transaction to analyze
            wallet_profile: Profile of the from_address wallet
            historical_transactions: Recent transactions for context
            network_metrics: Pre-calculated network metrics (optional)
        
        Returns:
            Complete OTC detection result with score and breakdown
        """
        tx_hash = transaction.get('tx_hash')
        from_address = transaction.get('from_address')
        to_address = transaction.get('to_address')
        usd_value = transaction.get('usd_value', 0)
        
        # Quick filters
        if not usd_value or usd_value < self.min_usd_value:
            return self._create_negative_result(tx_hash, 'below_threshold')
        
        # Check cache first
        if self.cache:
            cached = self.cache.get(f"otc_detection:{tx_hash}", prefix='detection')
            if cached:
                return cached
        
        # Step 1: Check if involves known OTC desk
        from_labels = self.labeling_service.get_wallet_labels(from_address)
        to_labels = self.labeling_service.get_wallet_labels(to_address)
        
        involves_known_desk = (
            self.otc_registry.is_otc_desk(from_address) or
            self.otc_registry.is_otc_desk(to_address)
        )
        
        # Get desk info if applicable
        desk_info = None
        if involves_known_desk:
            desk_info = (
                self.otc_registry.get_desk_info(from_address) or
                self.otc_registry.get_desk_info(to_address)
            )
        
        # Step 2: Heuristic Analysis
        heuristic_result = self.heuristic_analyzer.comprehensive_heuristic_analysis(
            transaction,
            wallet_profile,
            historical_transactions
        )
        
        # Step 3: Network Analysis (if metrics not provided)
        if network_metrics is None:
            # Build graph from historical transactions
            all_txs = historical_transactions + [transaction]
            self.network_analyzer.build_graph(all_txs)
            network_metrics = self.network_analyzer.analyze_wallet_centrality(from_address)
        
        # Step 4: Timing Analysis
        timing_data = self.heuristic_analyzer.analyze_timing(transaction)
        
        # Step 5: Calculate OTC Score
        score_result = self.scoring_system.calculate_otc_score(
            transaction,
            wallet_profile,
            network_metrics,
            timing_data,
            from_labels,
            to_labels
        )
        
        # Step 6: Determine classification
        total_score = score_result['total_score']
        
        if total_score >= self.high_confidence_threshold:
            classification = 'high_confidence'
        elif total_score >= self.medium_confidence_threshold:
            classification = 'medium'
        elif total_score >= 40:
            classification = 'low'
        else:
            classification = 'not_otc'
        
        # Build result
        result = {
            'tx_hash': tx_hash,
            'is_suspected_otc': total_score >= self.medium_confidence_threshold,
            'classification': classification,
            'confidence_score': total_score,
            'involves_known_desk': involves_known_desk,
            'otc_desk_info': desk_info,
            'score_breakdown': score_result,
            'heuristic_analysis': heuristic_result,
            'network_metrics': network_metrics,
            'timing_analysis': timing_data,
            'from_labels': from_labels,
            'to_labels': to_labels,
            'detected_at': datetime.utcnow().isoformat()
        }
        
        # Cache result
        if self.cache:
            self.cache.set(
                f"otc_detection:{tx_hash}",
                result,
                ttl=7200,  # 2 hours
                prefix='detection'
            )
        
        return result
    
    def _create_negative_result(self, tx_hash: str, reason: str) -> Dict:
        """Create negative detection result."""
        return {
            'tx_hash': tx_hash,
            'is_suspected_otc': False,
            'classification': 'not_otc',
            'confidence_score': 0,
            'reason': reason,
            'detected_at': datetime.utcnow().isoformat()
        }
    
    def batch_detect(
        self,
        transactions: List[Dict],
        wallet_profiles: Dict[str, Dict],
        historical_data: Dict[str, List[Dict]]
    ) -> List[Dict]:
        """
        Detect OTC activity in batch of transactions.
        
        More efficient than single detection for multiple transactions.
        
        Args:
            transactions: List of transactions to analyze
            wallet_profiles: Dict mapping addresses to profile data
            historical_data: Dict mapping addresses to historical transactions
        
        Returns:
            List of detection results
        """
        # Build network graph once for all transactions
        all_txs = transactions.copy()
        for hist_txs in historical_data.values():
            all_txs.extend(hist_txs)
        
        self.network_analyzer.build_graph(all_txs)
        
        results = []
        
        for tx in transactions:
            from_address = tx.get('from_address')
            
            # Get data for this transaction
            wallet_profile = wallet_profiles.get(from_address, {})
            historical = historical_data.get(from_address, [])
            
            # Get network metrics (already calculated from graph)
            network_metrics = self.network_analyzer.analyze_wallet_centrality(from_address)
            
            # Detect
            result = self.detect_otc_transaction(
                tx,
                wallet_profile,
                historical,
                network_metrics
            )
            
            results.append(result)
        
        return results
    
    def scan_block_range(
        self,
        transactions: List[Dict],
        min_usd_value: Optional[float] = None
    ) -> Dict:
        """
        Scan a range of blocks for OTC activity.
        
        This is used by the /api/otc/scan/range endpoint.
        
        Args:
            transactions: All transactions in the block range
            min_usd_value: Optional minimum value filter
        
        Returns:
            Summary of OTC activity found
        """
        min_value = min_usd_value or self.min_usd_value
        
        # Filter by value
        large_txs = [
            tx for tx in transactions
            if tx.get('usd_value', 0) >= min_value
        ]
        
        if not large_txs:
            return {
                'total_suspected_otc': 0,
                'total_volume_usd': 0,
                'transactions': [],
                'wallet_clusters': [],
                'confidence_distribution': {}
            }
        
        # Build wallet profiles (simplified for scanning)
        wallet_profiles = self._build_quick_profiles(large_txs)
        
        # Historical data for each wallet (last 30 days worth)
        historical_data = self._gather_historical_data(large_txs)
        
        # Detect OTC activity
        detection_results = self.batch_detect(large_txs, wallet_profiles, historical_data)
        
        # Filter to suspected OTC only
        suspected_otc = [
            result for result in detection_results
            if result['is_suspected_otc']
        ]
        
        # Calculate statistics
        total_volume = sum(
            tx.get('usd_value', 0)
            for tx in large_txs
            if any(r['tx_hash'] == tx['tx_hash'] for r in suspected_otc)
        )
        
        # Confidence distribution
        confidence_dist = {
            'high_confidence': len([r for r in suspected_otc if r['classification'] == 'high_confidence']),
            'medium': len([r for r in suspected_otc if r['classification'] == 'medium']),
            'low': len([r for r in suspected_otc if r['classification'] == 'low'])
        }
        
        # Identify wallet clusters (addresses that appear multiple times)
        wallet_activity = {}
        for result in suspected_otc:
            tx_hash = result['tx_hash']
            tx = next((t for t in large_txs if t['tx_hash'] == tx_hash), None)
            if tx:
                from_addr = tx['from_address']
                to_addr = tx['to_address']
                
                wallet_activity[from_addr] = wallet_activity.get(from_addr, 0) + 1
                wallet_activity[to_addr] = wallet_activity.get(to_addr, 0) + 1
        
        # Active wallets (>1 transaction)
        active_wallets = [
            {'address': addr, 'transaction_count': count}
            for addr, count in wallet_activity.items()
            if count > 1
        ]
        
        return {
            'total_suspected_otc': len(suspected_otc),
            'total_volume_usd': total_volume,
            'transactions': suspected_otc,
            'wallet_clusters': active_wallets,
            'confidence_distribution': confidence_dist,
            'scan_date': datetime.utcnow().isoformat()
        }
    
    def _build_quick_profiles(self, transactions: List[Dict]) -> Dict[str, Dict]:
        """Build quick wallet profiles from transaction data."""
        profiles = {}
        
        # Group by address
        for tx in transactions:
            from_addr = tx['from_address']
            
            if from_addr not in profiles:
                profiles[from_addr] = {
                    'address': from_addr,
                    'transaction_frequency': 0,
                    'avg_transaction_usd': 0,
                    'has_defi_interactions': False,
                    'has_dex_swaps': False,
                    'counterparty_entropy': 0,
                    'active_hours': [],
                    'counterparties': []
                }
            
            # Update profile
            profile = profiles[from_addr]
            profile['transaction_frequency'] += 1
            
            if tx.get('is_contract_interaction'):
                profile['has_defi_interactions'] = True
            
            if tx.get('timestamp'):
                hour = tx['timestamp'].hour
                if hour not in profile['active_hours']:
                    profile['active_hours'].append(hour)
            
            to_addr = tx.get('to_address')
            if to_addr and to_addr not in profile['counterparties']:
                profile['counterparties'].append(to_addr)
        
        # Calculate averages
        for addr, profile in profiles.items():
            addr_txs = [tx for tx in transactions if tx['from_address'] == addr]
            if addr_txs:
                total_value = sum(tx.get('usd_value', 0) for tx in addr_txs)
                profile['avg_transaction_usd'] = total_value / len(addr_txs)
        
        return profiles
    
    def _gather_historical_data(self, transactions: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Gather historical data for addresses.
        
        In production, this would query database.
        For now, just return empty dict.
        """
        return {}
    
    def get_detection_stats(self) -> Dict:
        """Get overall detection statistics."""
        # This would pull from database in production
        return {
            'total_transactions_analyzed': 0,
            'total_otc_detected': 0,
            'high_confidence_count': 0,
            'medium_confidence_count': 0,
            'known_desk_interactions': 0
        }
