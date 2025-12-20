from typing import List, Dict, Optional, Tuple
from app.core.otc_analysis.utils.graph_utils import (
    create_transaction_graph,
    find_shortest_path,
    find_all_paths
)
import networkx as nx

class FlowTracer:
    """
    Money flow tracing from point A to point B.
    
    From doc:
    - Modified Dijkstra with confidence-weighted paths
    - Multi-path analysis
    - Hop distance calculation to known OTC desks
    """
    
    def __init__(self):
        self.graph = None
        self.max_paths_to_return = 10
    
    def trace_flow(
        self,
        source_address: str,
        target_address: str,
        transactions: List[Dict],
        max_hops: int = 5,
        min_confidence: float = 0.0
    ) -> Dict:
        """
        Trace money flow from source to target address.
        
        This implements the /api/otc/flow/trace endpoint functionality.
        
        Args:
            source_address: Starting address
            target_address: Destination address
            transactions: All transactions to build graph from
            max_hops: Maximum path length
            min_confidence: Minimum confidence for path segments
        
        Returns:
            Flow analysis with paths and confidence scores
        """
        # Build graph
        self.graph = create_transaction_graph(transactions)
        
        # Check if both addresses exist in graph
        if source_address not in self.graph.nodes():
            return {
                'source': source_address,
                'target': target_address,
                'path_exists': False,
                'reason': 'source_not_found',
                'paths': []
            }
        
        if target_address not in self.graph.nodes():
            return {
                'source': source_address,
                'target': target_address,
                'path_exists': False,
                'reason': 'target_not_found',
                'paths': []
            }
        
        # Find all possible paths
        all_paths = find_all_paths(
            self.graph,
            source_address,
            target_address,
            max_hops=max_hops,
            max_paths=self.max_paths_to_return
        )
        
        if not all_paths:
            return {
                'source': source_address,
                'target': target_address,
                'path_exists': False,
                'reason': 'no_path_within_hops',
                'max_hops': max_hops,
                'paths': []
            }
        
        # Calculate confidence for each path
        paths_with_confidence = []
        
        for path in all_paths:
            confidence_data = self._calculate_path_confidence(path, transactions)
            
            # Filter by minimum confidence
            if confidence_data['overall_confidence'] >= min_confidence:
                paths_with_confidence.append({
                    'path': path,
                    'hop_count': len(path) - 1,
                    'intermediaries': path[1:-1],
                    **confidence_data
                })
        
        # Sort by confidence (highest first)
        paths_with_confidence.sort(
            key=lambda x: (x['overall_confidence'], -x['hop_count']),
            reverse=True
        )
        
        # Get the best path
        best_path = paths_with_confidence[0] if paths_with_confidence else None
        
        return {
            'source': source_address,
            'target': target_address,
            'path_exists': len(paths_with_confidence) > 0,
            'path_count': len(paths_with_confidence),
            'best_path': best_path,
            'all_paths': paths_with_confidence,
            'max_hops': max_hops
        }
    
    def _calculate_path_confidence(
        self,
        path: List[str],
        transactions: List[Dict]
    ) -> Dict:
        """
        Calculate confidence score for a path.
        
        Confidence based on:
        - Transaction values (higher = more confident)
        - Recency (recent = more confident)
        - Number of transactions between nodes
        
        Returns confidence data
        """
        segment_confidences = []
        total_value = 0
        segment_details = []
        
        for i in range(len(path) - 1):
            from_addr = path[i]
            to_addr = path[i + 1]
            
            # Find transactions between these addresses
            segment_txs = [
                tx for tx in transactions
                if tx['from_address'] == from_addr and tx['to_address'] == to_addr
            ]
            
            if not segment_txs:
                # No direct transactions (shouldn't happen if path exists)
                segment_confidences.append(0)
                continue
            
            # Calculate segment confidence
            segment_value = sum(tx.get('usd_value', 0) for tx in segment_txs)
            segment_count = len(segment_txs)
            
            # Most recent transaction
            most_recent = max(tx['timestamp'] for tx in segment_txs)
            
            # Confidence factors:
            # 1. Value (normalized to 0-1, capped at $10M)
            value_confidence = min(segment_value / 10_000_000, 1.0)
            
            # 2. Count (more transactions = more confident, capped at 10)
            count_confidence = min(segment_count / 10, 1.0)
            
            # 3. Recency (transactions within 30 days = higher confidence)
            # This is simplified - would need current date
            recency_confidence = 0.5  # Placeholder
            
            # Combined confidence
            segment_conf = (
                value_confidence * 0.5 +
                count_confidence * 0.3 +
                recency_confidence * 0.2
            )
            
            segment_confidences.append(segment_conf)
            total_value += segment_value
            
            segment_details.append({
                'from': from_addr,
                'to': to_addr,
                'transaction_count': segment_count,
                'total_value': segment_value,
                'confidence': segment_conf
            })
        
        # Overall confidence is the minimum of all segments
        # (chain is only as strong as weakest link)
        overall_confidence = min(segment_confidences) if segment_confidences else 0
        
        return {
            'overall_confidence': overall_confidence,
            'avg_segment_confidence': sum(segment_confidences) / len(segment_confidences) if segment_confidences else 0,
            'total_value': total_value,
            'segment_details': segment_details
        }
    
    def calculate_hop_distance_to_desks(
        self,
        address: str,
        otc_desk_addresses: List[str],
        transactions: List[Dict],
        max_hops: int = 5
    ) -> Dict:
        """
        Calculate hop distance from address to nearest known OTC desk.
        
        Used in OTC detection to see how close a wallet is to known desks.
        
        Returns:
            {
                'nearest_desk': str,
                'hop_distance': int,
                'path': List[str],
                'desk_distances': Dict[str, int]
            }
        """
        if not self.graph:
            self.graph = create_transaction_graph(transactions)
        
        if address not in self.graph.nodes():
            return {
                'nearest_desk': None,
                'hop_distance': None,
                'path': [],
                'desk_distances': {}
            }
        
        desk_distances = {}
        nearest_desk = None
        min_distance = float('inf')
        best_path = []
        
        for desk_addr in otc_desk_addresses:
            if desk_addr not in self.graph.nodes():
                continue
            
            # Find shortest path
            path = find_shortest_path(self.graph, address, desk_addr, max_hops)
            
            if path:
                distance = len(path) - 1  # Number of hops
                desk_distances[desk_addr] = distance
                
                if distance < min_distance:
                    min_distance = distance
                    nearest_desk = desk_addr
                    best_path = path
        
        return {
            'address': address,
            'nearest_desk': nearest_desk,
            'hop_distance': min_distance if nearest_desk else None,
            'path': best_path,
            'desk_distances': desk_distances,
            'total_desks_analyzed': len(otc_desk_addresses)
        }
    
    def analyze_flow_pattern(
        self,
        address: str,
        transactions: List[Dict],
        depth: int = 2
    ) -> Dict:
        """
        Analyze the flow pattern around an address.
        
        Detects patterns like:
        - Fan-out (one source to many destinations)
        - Fan-in (many sources to one destination)
        - Hub (both fan-in and fan-out)
        
        Returns pattern analysis
        """
        if not self.graph:
            self.graph = create_transaction_graph(transactions)
        
        if address not in self.graph.nodes():
            return {
                'pattern': 'unknown',
                'reason': 'address_not_found'
            }
        
        # Count incoming and outgoing edges
        in_degree = self.graph.in_degree(address)
        out_degree = self.graph.out_degree(address)
        
        # Determine pattern
        if in_degree > 10 and out_degree > 10:
            pattern = 'hub'
        elif out_degree > in_degree * 2 and out_degree > 5:
            pattern = 'fan_out'
        elif in_degree > out_degree * 2 and in_degree > 5:
            pattern = 'fan_in'
        elif in_degree < 3 and out_degree < 3:
            pattern = 'isolated'
        else:
            pattern = 'balanced'
        
        # Get neighbors
        predecessors = list(self.graph.predecessors(address))[:20]  # Limit for performance
        successors = list(self.graph.successors(address))[:20]
        
        return {
            'address': address,
            'pattern': pattern,
            'in_degree': in_degree,
            'out_degree': out_degree,
            'unique_sources': len(predecessors),
            'unique_destinations': len(successors),
            'sample_sources': predecessors[:5],
            'sample_destinations': successors[:5]
        }
