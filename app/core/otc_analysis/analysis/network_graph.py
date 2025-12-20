from typing import List, Dict, Set, Optional, Tuple
import networkx as nx
from app.core.otc_analysis.utils.graph_utils import (
    create_transaction_graph,
    calculate_betweenness_centrality,
    calculate_degree_centrality,
    calculate_clustering_coefficient,
    get_k_hop_neighbors,
    find_shortest_path,
    find_all_paths
)

class NetworkAnalysisService:
    """
    Network Topology Analysis.
    
    From doc:
    - Betweenness Centrality: OTC desks have high values
    - Clustering Coefficient: Low for OTC hubs (star topology)
    - Degree Centrality: Many unique connections
    - Hub-and-Spoke detection
    """
    
    def __init__(self):
        self.graph = None
        self.centrality_cache = {}
    
    def build_graph(self, transactions: List[Dict]) -> nx.DiGraph:
        """Build directed graph from transactions."""
        self.graph = create_transaction_graph(transactions)
        return self.graph
    
    def analyze_wallet_centrality(self, address: str) -> Dict:
        """
        Calculate all centrality metrics for a wallet.
        
        Returns:
            {
                'betweenness': float,
                'degree': float,
                'clustering': float,
                'is_hub': bool
            }
        """
        if not self.graph:
            raise ValueError("Graph not built. Call build_graph() first.")
        
        # Check cache
        if address in self.centrality_cache:
            return self.centrality_cache[address]
        
        betweenness = calculate_betweenness_centrality(self.graph, address)
        degree = calculate_degree_centrality(self.graph, address)
        clustering = calculate_clustering_coefficient(self.graph, address)
        
        # Determine if this is a hub
        # Hub criteria: High betweenness AND high degree AND low clustering
        is_hub = (
            betweenness > 0.1 and
            degree > 0.05 and
            clustering < 0.3
        )
        
        result = {
            'betweenness_centrality': betweenness,
            'degree_centrality': degree,
            'clustering_coefficient': clustering,
            'is_hub': is_hub,
            'hub_score': (betweenness + degree) * (1 - clustering)  # Combined metric
        }
        
        # Cache result
        self.centrality_cache[address] = result
        
        return result
    
    def identify_otc_hubs(self, min_hub_score: float = 0.1) -> List[Dict]:
        """
        Identify potential OTC hubs in the network.
        
        OTC hubs have:
        - High betweenness centrality (bridge many transactions)
        - High degree centrality (many connections)
        - Low clustering coefficient (star topology)
        
        Returns:
            List of potential OTC hub addresses with scores
        """
        if not self.graph:
            raise ValueError("Graph not built. Call build_graph() first.")
        
        hubs = []
        
        for node in self.graph.nodes():
            metrics = self.analyze_wallet_centrality(node)
            
            if metrics['is_hub'] and metrics['hub_score'] >= min_hub_score:
                hubs.append({
                    'address': node,
                    'hub_score': metrics['hub_score'],
                    'betweenness': metrics['betweenness_centrality'],
                    'degree': metrics['degree_centrality'],
                    'clustering': metrics['clustering_coefficient']
                })
        
        # Sort by hub score
        hubs.sort(key=lambda x: x['hub_score'], reverse=True)
        
        return hubs
    
    def analyze_neighborhood(
        self,
        address: str,
        hops: int = 2,
        direction: str = 'both'
    ) -> Dict:
        """
        Analyze the neighborhood around an address.
        
        Args:
            address: Center address
            hops: Number of hops to explore
            direction: 'in', 'out', or 'both'
        
        Returns:
            Neighborhood analysis with statistics
        """
        if not self.graph:
            raise ValueError("Graph not built. Call build_graph() first.")
        
        neighbors = get_k_hop_neighbors(self.graph, address, k=hops, direction=direction)
        
        if not neighbors:
            return {
                'address': address,
                'neighbor_count': 0,
                'total_volume': 0,
                'avg_degree': 0
            }
        
        # Calculate statistics
        total_volume = 0
        degrees = []
        
        for neighbor in neighbors:
            # Get edge data if exists
            if self.graph.has_edge(address, neighbor):
                total_volume += self.graph[address][neighbor].get('total_value', 0)
            if self.graph.has_edge(neighbor, address):
                total_volume += self.graph[neighbor][address].get('total_value', 0)
            
            # Get degree
            degrees.append(self.graph.degree(neighbor))
        
        avg_degree = sum(degrees) / len(degrees) if degrees else 0
        
        return {
            'address': address,
            'neighbor_count': len(neighbors),
            'neighbors': list(neighbors),
            'total_volume': total_volume,
            'avg_degree': avg_degree,
            'hops': hops
        }
    
    def trace_flow_path(
        self,
        source: str,
        target: str,
        max_hops: int = 5
    ) -> Optional[Dict]:
        """
        Trace transaction flow from source to target.
        
        Used in flow tracing feature.
        
        Returns:
            {
                'path': List[str],
                'hop_count': int,
                'total_value': float,
                'path_exists': bool
            }
        """
        if not self.graph:
            raise ValueError("Graph not built. Call build_graph() first.")
        
        # Find shortest path
        path = find_shortest_path(self.graph, source, target, max_hops)
        
        if not path:
            return {
                'path': [],
                'hop_count': 0,
                'total_value': 0,
                'path_exists': False
            }
        
        # Calculate total value along path
        total_value = 0
        for i in range(len(path) - 1):
            if self.graph.has_edge(path[i], path[i + 1]):
                total_value += self.graph[path[i]][path[i + 1]].get('total_value', 0)
        
        return {
            'path': path,
            'hop_count': len(path) - 1,
            'total_value': total_value,
            'path_exists': True
        }
    
    def find_all_flow_paths(
        self,
        source: str,
        target: str,
        max_hops: int = 5,
        max_paths: int = 10
    ) -> List[Dict]:
        """
        Find all possible flow paths between source and target.
        
        Returns list of paths with metadata.
        """
        if not self.graph:
            raise ValueError("Graph not built. Call build_graph() first.")
        
        paths = find_all_paths(self.graph, source, target, max_hops, max_paths)
        
        result = []
        for path in paths:
            # Calculate metrics for this path
            total_value = 0
            intermediaries = path[1:-1]  # Exclude source and target
            
            for i in range(len(path) - 1):
                if self.graph.has_edge(path[i], path[i + 1]):
                    total_value += self.graph[path[i]][path[i + 1]].get('total_value', 0)
            
            result.append({
                'path': path,
                'hop_count': len(path) - 1,
                'intermediaries': intermediaries,
                'total_value': total_value
            })
        
        # Sort by hop count (prefer shorter paths)
        result.sort(key=lambda x: x['hop_count'])
        
        return result
    
    def calculate_network_position_score(self, address: str) -> float:
        """
        Calculate network position score for OTC scoring system.
        
        This is the NetworkPosition_Score component.
        
        Returns: 0-100
        """
        metrics = self.analyze_wallet_centrality(address)
        
        betweenness = metrics['betweenness_centrality']
        degree = metrics['degree_centrality']
        clustering = metrics['clustering_coefficient']
        
        score = 0
        
        # Betweenness (50% weight)
        score += betweenness * 100 * 0.5
        
        # Degree (30% weight)
        score += degree * 100 * 0.3
        
        # Inverse clustering (20% weight) - low clustering is good for OTC
        score += (1 - clustering) * 100 * 0.2
        
        return min(100, score)
    
    def detect_suspicious_patterns(self) -> List[Dict]:
        """
        Detect suspicious network patterns that might indicate OTC activity.
        
        Patterns:
        1. High-value star topologies (hub with many spokes)
        2. Circular flows (potential wash trading)
        3. Rapid multi-hop transfers
        
        Returns:
            List of suspicious patterns found
        """
        if not self.graph:
            raise ValueError("Graph not built. Call build_graph() first.")
        
        suspicious = []
        
        # Pattern 1: Star topologies with high value
        hubs = self.identify_otc_hubs(min_hub_score=0.15)
        for hub in hubs:
            neighbors = list(self.graph.neighbors(hub['address']))
            if len(neighbors) >= 5:  # At least 5 connections
                suspicious.append({
                    'pattern_type': 'star_topology',
                    'hub_address': hub['address'],
                    'spoke_count': len(neighbors),
                    'hub_score': hub['hub_score']
                })
        
        # Pattern 2: Circular flows (cycles)
        try:
            cycles = list(nx.simple_cycles(self.graph))
            for cycle in cycles[:50]:  # Limit to first 50
                if len(cycle) >= 3 and len(cycle) <= 6:  # Meaningful cycles
                    # Calculate total value in cycle
                    cycle_value = 0
                    for i in range(len(cycle)):
                        from_addr = cycle[i]
                        to_addr = cycle[(i + 1) % len(cycle)]
                        if self.graph.has_edge(from_addr, to_addr):
                            cycle_value += self.graph[from_addr][to_addr].get('total_value', 0)
                    
                    if cycle_value > 100000:  # Significant value
                        suspicious.append({
                            'pattern_type': 'circular_flow',
                            'cycle': cycle,
                            'cycle_length': len(cycle),
                            'total_value': cycle_value
                        })
        except:
            pass  # Cycle detection can be expensive
        
        return suspicious
    
    def get_graph_statistics(self) -> Dict:
        """Get overall graph statistics."""
        if not self.graph:
            raise ValueError("Graph not built. Call build_graph() first.")
        
        return {
            'node_count': self.graph.number_of_nodes(),
            'edge_count': self.graph.number_of_edges(),
            'density': nx.density(self.graph),
            'is_connected': nx.is_weakly_connected(self.graph),
            'component_count': nx.number_weakly_connected_components(self.graph)
        }
