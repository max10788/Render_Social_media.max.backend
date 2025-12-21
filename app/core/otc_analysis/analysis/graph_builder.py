from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
import logging
from collections import defaultdict

from app.core.otc_analysis.models.wallet import Wallet
from app.core.otc_analysis.analysis.network_graph import NetworkAnalysisService
from app.core.otc_analysis.utils.cache import CacheManager

logger = logging.getLogger(__name__)

class GraphBuilderService:
    """
    Service for building network graph data with all Phase 2 visualizations.
    
    Used by GET /api/otc/network/graph endpoint.
    Returns:
    - nodes, edges (for NetworkGraph)
    - sankey_data (for SankeyFlow)
    - time_heatmap (for TimeHeatmap)
    - timeline_data (for TransferTimeline)
    - distributions (for DistributionCharts)
    """
    
    def __init__(self, cache_manager: Optional[CacheManager] = None):
        self.cache = cache_manager
        self.network_analyzer = NetworkAnalysisService()
    
    def build_complete_graph(
        self,
        db: Session,
        from_date: datetime,
        to_date: datetime,
        min_confidence: float = 0,
        min_transfer_size: float = 0,
        entity_types: Optional[List[str]] = None,
        tokens: Optional[List[str]] = None,
        max_nodes: int = 500
    ) -> Dict:
        """
        Build complete graph with all Phase 2 data.
        
        Returns comprehensive data structure for all frontend components.
        """
        logger.info(f"🔨 Building graph: {from_date.date()} to {to_date.date()}")
        
        # Get wallets matching filters
        wallets = self._get_filtered_wallets(
            db, from_date, to_date, min_confidence, entity_types, max_nodes
        )
        
        if not wallets:
            return self._empty_graph()
        
        logger.info(f"📦 Found {len(wallets)} wallets")
        
        # Build core graph (nodes + edges)
        nodes, edges = self._build_nodes_and_edges(wallets, db)
        
        # Build Phase 2 data structures
        sankey_data = self._build_sankey_data(wallets, edges)
        time_heatmap = self._build_time_heatmap(wallets)
        timeline_data = self._build_timeline_data(wallets, from_date, to_date)
        distributions = self._build_distributions(wallets)
        
        result = {
            'nodes': nodes,
            'edges': edges,
            'sankey_data': sankey_data,
            'time_heatmap': time_heatmap,
            'timeline_data': timeline_data,
            'distributions': distributions,
            'metadata': {
                'generated_at': datetime.utcnow().isoformat(),
                'node_count': len(nodes),
                'edge_count': len(edges),
                'date_range': {
                    'from': from_date.isoformat(),
                    'to': to_date.isoformat()
                }
            }
        }
        
        logger.info(f"✅ Graph built: {len(nodes)} nodes, {len(edges)} edges")
        
        return result
    
    def _get_filtered_wallets(
        self,
        db: Session,
        from_date: datetime,
        to_date: datetime,
        min_confidence: float,
        entity_types: Optional[List[str]],
        max_nodes: int
    ) -> List[Wallet]:
        """Get wallets matching filters."""
        query = db.query(Wallet).filter(
            and_(
                Wallet.last_seen >= from_date,
                Wallet.last_seen <= to_date,
                Wallet.confidence_score >= min_confidence
            )
        )
        
        if entity_types:
            query = query.filter(Wallet.entity_type.in_(entity_types))
        
        # Order by OTC probability (highest first)
        query = query.order_by(Wallet.otc_probability.desc())
        
        # Limit to max_nodes
        query = query.limit(max_nodes)
        
        return query.all()
    
    def _build_nodes_and_edges(
        self,
        wallets: List[Wallet],
        db: Session
    ) -> tuple[List[Dict], List[Dict]]:
        """Build nodes and edges for NetworkGraph."""
        nodes = []
        edges = []
        
        wallet_addresses = {w.address for w in wallets}
        
        # Build nodes
        for wallet in wallets:
            nodes.append({
                'address': wallet.address,
                'label': wallet.entity_name,
                'entity_type': wallet.entity_type or 'unknown',
                'total_volume_usd': wallet.total_volume_usd,
                'confidence_score': wallet.confidence_score,
                'is_active': (datetime.utcnow() - wallet.last_seen).days < 1 if wallet.last_seen else False,
                'transaction_count': wallet.total_transactions
            })
        
        # Build edges (simplified - in production would query actual transactions)
        # For demo, create edges based on common patterns in OTC activity
        edges_dict = {}
        
        for wallet in wallets:
            # Simulate edges based on wallet metrics
            # In production: query actual transaction table
            # For now: create synthetic edges for high-probability OTC wallets
            
            if wallet.otc_probability > 0.7 and wallet.unique_counterparties > 0:
                # Create edges to other wallets
                for other in wallets[:10]:  # Connect to top 10
                    if other.address != wallet.address:
                        edge_key = f"{wallet.address}-{other.address}"
                        if edge_key not in edges_dict:
                            edges_dict[edge_key] = {
                                'source': wallet.address,
                                'target': other.address,
                                'transfer_amount_usd': wallet.avg_transaction_usd,
                                'is_suspected_otc': True,
                                'edge_count': 1,
                                'transaction_count': 1
                            }
        
        edges = list(edges_dict.values())
        
        return nodes, edges
    
    def _build_sankey_data(
        self,
        wallets: List[Wallet],
        edges: List[Dict]
    ) -> Dict:
        """Build Sankey flow diagram data."""
        # Group wallets by category
        categories = {
            'otc_desk': 'OTC Desks',
            'institutional': 'Institutional',
            'exchange': 'Exchanges',
            'unknown': 'Unknown'
        }
        
        # Build nodes (grouped by entity type)
        sankey_nodes = []
        node_names = set()
        
        for wallet in wallets:
            # Use entity name if available, otherwise category
            if wallet.entity_name and wallet.entity_name not in node_names:
                sankey_nodes.append({
                    'name': wallet.entity_name,
                    'category': categories.get(wallet.entity_type, 'Unknown')
                })
                node_names.add(wallet.entity_name)
        
        # If no named entities, use categories
        if not sankey_nodes:
            for entity_type, category in categories.items():
                sankey_nodes.append({
                    'name': category,
                    'category': category
                })
        
        # Build links
        sankey_links = []
        
        for edge in edges[:50]:  # Limit to top 50 flows
            # Map addresses to node names
            source_wallet = next((w for w in wallets if w.address == edge['source']), None)
            target_wallet = next((w for w in wallets if w.address == edge['target']), None)
            
            if source_wallet and target_wallet:
                source_name = source_wallet.entity_name or categories.get(source_wallet.entity_type, 'Unknown')
                target_name = target_wallet.entity_name or categories.get(target_wallet.entity_type, 'Unknown')
                
                sankey_links.append({
                    'source': source_name,
                    'target': target_name,
                    'value': edge['transfer_amount_usd'],
                    'transaction_count': edge.get('transaction_count', 1)
                })
        
        return {
            'nodes': sankey_nodes,
            'links': sankey_links
        }
    
    def _build_time_heatmap(self, wallets: List[Wallet]) -> Dict:
        """Build time heatmap data (7x24 matrix)."""
        # Initialize 7x24 matrix (days x hours)
        heatmap = [[0 for _ in range(24)] for _ in range(7)]
        transaction_counts = [[0 for _ in range(24)] for _ in range(7)]
        
        # Aggregate activity from wallets
        for wallet in wallets:
            if wallet.active_hours and wallet.active_days:
                for hour in wallet.active_hours:
                    for day in wallet.active_days:
                        if 0 <= day < 7 and 0 <= hour < 24:
                            heatmap[day][hour] += wallet.avg_transaction_usd
                            transaction_counts[day][hour] += 1
        
        # Find peak hours (top 3)
        peaks = []
        for day in range(7):
            for hour in range(24):
                if heatmap[day][hour] > 0:
                    peaks.append({
                        'day': day,
                        'hour': hour,
                        'value': heatmap[day][hour]
                    })
        
        peaks.sort(key=lambda x: x['value'], reverse=True)
        peak_hours = peaks[:3]
        
        # Detect patterns
        patterns = self._detect_time_patterns(heatmap, wallets)
        
        return {
            'heatmap': heatmap,
            'transaction_counts': transaction_counts,
            'peak_hours': peak_hours,
            'patterns': patterns
        }
    
    def _detect_time_patterns(
        self,
        heatmap: List[List[float]],
        wallets: List[Wallet]
    ) -> List[Dict]:
        """Detect timing patterns in activity."""
        patterns = []
        
        # Pattern 1: Weekend vs Weekday
        weekday_activity = sum(sum(heatmap[i]) for i in range(5))  # Mon-Fri
        weekend_activity = sum(sum(heatmap[i]) for i in range(5, 7))  # Sat-Sun
        
        if weekend_activity > weekday_activity * 0.3:
            patterns.append({
                'icon': '📅',
                'description': f'Significant weekend activity detected ({weekend_activity / (weekday_activity + weekend_activity) * 100:.0f}% of total)'
            })
        
        # Pattern 2: Off-hours activity
        business_hours_activity = sum(sum(heatmap[i][9:18]) for i in range(5))  # 9-18 weekdays
        total_activity = sum(sum(row) for row in heatmap)
        
        if business_hours_activity < total_activity * 0.5:
            patterns.append({
                'icon': '🌙',
                'description': 'Majority of activity occurs outside standard business hours'
            })
        
        # Pattern 3: OTC desk specific patterns
        otc_desks = [w for w in wallets if w.entity_type == 'otc_desk']
        if otc_desks:
            patterns.append({
                'icon': '📊',
                'description': f'{len(otc_desks)} OTC desk(s) identified with concentrated activity patterns'
            })
        
        return patterns
    
    def _build_timeline_data(
        self,
        wallets: List[Wallet],
        from_date: datetime,
        to_date: datetime
    ) -> Dict:
        """Build timeline data for TransferTimeline."""
        transfers = []
        
        # Generate synthetic transfers from wallet data
        # In production: query actual transaction table
        for wallet in wallets[:200]:  # Limit to 200 for performance
            if wallet.last_seen and wallet.last_seen >= from_date:
                # Create a representative transfer
                transfers.append({
                    'id': f'tx_{wallet.address[:10]}',
                    'timestamp': wallet.last_seen.isoformat(),
                    'from_address': wallet.address,
                    'to_address': '0x' + '0' * 40,  # Placeholder
                    'from_label': wallet.entity_name,
                    'to_label': None,
                    'usd_value': wallet.avg_transaction_usd,
                    'token': 'USDT',  # Default
                    'confidence_score': wallet.confidence_score,
                    'cluster_id': wallet.cluster_id
                })
        
        # Sort by timestamp
        transfers.sort(key=lambda x: x['timestamp'], reverse=True)
        
        # Calculate statistics
        total_volume = sum(t['usd_value'] for t in transfers)
        avg_confidence = sum(t['confidence_score'] for t in transfers) / len(transfers) if transfers else 0
        
        return {
            'transfers': transfers[:200],  # Limit to 200
            'statistics': {
                'total_count': len(transfers),
                'total_volume': total_volume,
                'avg_confidence': avg_confidence
            }
        }
    
    def _build_distributions(self, wallets: List[Wallet]) -> Dict:
        """Build distribution data for DistributionCharts."""
        # Size distribution (Normal vs OTC)
        size_buckets = {
            '$1K-$10K': {'normal': 0, 'otc': 0},
            '$10K-$100K': {'normal': 0, 'otc': 0},
            '$100K-$500K': {'normal': 0, 'otc': 0},
            '$500K-$1M': {'normal': 0, 'otc': 0},
            '$1M-$5M': {'normal': 0, 'otc': 0},
            '$5M+': {'normal': 0, 'otc': 0}
        }
        
        for wallet in wallets:
            avg_size = wallet.avg_transaction_usd
            is_otc = wallet.otc_probability > 0.6
            
            # Determine bucket
            if avg_size < 10000:
                bucket = '$1K-$10K'
            elif avg_size < 100000:
                bucket = '$10K-$100K'
            elif avg_size < 500000:
                bucket = '$100K-$500K'
            elif avg_size < 1000000:
                bucket = '$500K-$1M'
            elif avg_size < 5000000:
                bucket = '$1M-$5M'
            else:
                bucket = '$5M+'
            
            if is_otc:
                size_buckets[bucket]['otc'] += 1
            else:
                size_buckets[bucket]['normal'] += 1
        
        size_distribution = [
            {'range': k, 'normal': v['normal'], 'otc': v['otc']}
            for k, v in size_buckets.items()
        ]
        
        # Confidence distribution by entity type
        confidence_distribution = self._build_confidence_distribution(wallets)
        
        # Activity patterns (for Radar chart)
        activity_patterns = self._build_activity_patterns(wallets)
        
        return {
            'size_distribution': size_distribution,
            'confidence_distribution': confidence_distribution,
            'activity_patterns': activity_patterns
        }
    
    def _build_confidence_distribution(self, wallets: List[Wallet]) -> Dict:
        """Build confidence score distribution by entity type."""
        score_buckets = ['0-20', '20-40', '40-60', '60-80', '80-100']
        
        distributions = {
            'otc_desk': [],
            'institutional': [],
            'exchange': []
        }
        
        for entity_type in distributions.keys():
            type_wallets = [w for w in wallets if w.entity_type == entity_type]
            
            for bucket in score_buckets:
                low, high = map(int, bucket.split('-'))
                count = len([w for w in type_wallets if low <= w.confidence_score < high])
                
                distributions[entity_type].append({
                    'score': bucket,
                    'count': count
                })
        
        return distributions
    
    def _build_activity_patterns(self, wallets: List[Wallet]) -> Dict:
        """Build activity patterns for Radar chart."""
        patterns = {
            'otc_desk': [],
            'institutional': [],
            'exchange': []
        }
        
        metrics = [
            'Tx Frequency',
            'Avg Size',
            'Centrality',
            'Off-hours',
            'DeFi Level',
            'Diversity'
        ]
        
        for entity_type in patterns.keys():
            type_wallets = [w for w in wallets if w.entity_type == entity_type]
            
            if not type_wallets:
                # Default values
                patterns[entity_type] = [
                    {'metric': m, 'value': 0} for m in metrics
                ]
                continue
            
            # Calculate average metrics
            avg_freq = sum(w.transaction_frequency for w in type_wallets) / len(type_wallets)
            avg_size = sum(w.avg_transaction_usd for w in type_wallets) / len(type_wallets)
            avg_centrality = sum(w.betweenness_centrality for w in type_wallets) / len(type_wallets)
            avg_weekend = sum(w.weekend_activity_ratio for w in type_wallets) / len(type_wallets)
            avg_defi = sum(1 for w in type_wallets if w.has_defi_interactions) / len(type_wallets)
            avg_diversity = sum(w.unique_counterparties for w in type_wallets) / len(type_wallets)
            
            # Normalize to 0-100 scale
            patterns[entity_type] = [
                {'metric': 'Tx Frequency', 'value': min(100, avg_freq * 10)},
                {'metric': 'Avg Size', 'value': min(100, avg_size / 10000)},
                {'metric': 'Centrality', 'value': avg_centrality * 100},
                {'metric': 'Off-hours', 'value': avg_weekend * 100},
                {'metric': 'DeFi Level', 'value': avg_defi * 100},
                {'metric': 'Diversity', 'value': min(100, avg_diversity)}
            ]
        
        return patterns
    
    def _empty_graph(self) -> Dict:
        """Return empty graph structure."""
        return {
            'nodes': [],
            'edges': [],
            'sankey_data': {'nodes': [], 'links': []},
            'time_heatmap': {
                'heatmap': [[0] * 24 for _ in range(7)],
                'transaction_counts': [[0] * 24 for _ in range(7)],
                'peak_hours': [],
                'patterns': []
            },
            'timeline_data': {
                'transfers': [],
                'statistics': {'total_count': 0, 'total_volume': 0, 'avg_confidence': 0}
            },
            'distributions': {
                'size_distribution': [],
                'confidence_distribution': {},
                'activity_patterns': {}
            },
            'metadata': {
                'generated_at': datetime.utcnow().isoformat(),
                'node_count': 0,
                'edge_count': 0
            }
        }
