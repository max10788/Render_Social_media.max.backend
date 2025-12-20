import networkx as nx
from typing import List, Dict, Set, Tuple, Optional
from collections import deque, defaultdict

def create_transaction_graph(transactions: List[Dict]) -> nx.DiGraph:
    """
    Create directed graph from transactions.
    Nodes are wallet addresses, edges are transactions.
    """
    G = nx.DiGraph()
    
    for tx in transactions:
        from_addr = tx['from_address']
        to_addr = tx['to_address']
        
        # Add nodes if they don't exist
        if not G.has_node(from_addr):
            G.add_node(from_addr, address=from_addr)
        if not G.has_node(to_addr):
            G.add_node(to_addr, address=to_addr)
        
        # Add or update edge
        if G.has_edge(from_addr, to_addr):
            G[from_addr][to_addr]['weight'] += 1
            G[from_addr][to_addr]['total_value'] += tx.get('usd_value', 0)
        else:
            G.add_edge(
                from_addr,
                to_addr,
                weight=1,
                total_value=tx.get('usd_value', 0),
                first_tx=tx.get('timestamp'),
                last_tx=tx.get('timestamp')
            )
    
    return G


def calculate_betweenness_centrality(G: nx.Graph, address: str) -> float:
    """
    Calculate betweenness centrality for a specific address.
    High values indicate the address acts as a bridge/hub.
    
    OTC desks typically have high betweenness centrality.
    """
    try:
        centrality = nx.betweenness_centrality(G, normalized=True)
        return centrality.get(address, 0.0)
    except:
        return 0.0


def calculate_degree_centrality(G: nx.Graph, address: str) -> float:
    """
    Calculate degree centrality - measures number of unique connections.
    """
    try:
        centrality = nx.degree_centrality(G)
        return centrality.get(address, 0.0)
    except:
        return 0.0


def calculate_clustering_coefficient(G: nx.Graph, address: str) -> float:
    """
    Calculate clustering coefficient - measures how connected neighbors are.
    
    Low clustering coefficient indicates hub-and-spoke topology (typical for OTC).
    """
    try:
        # Convert to undirected for clustering coefficient
        G_undirected = G.to_undirected()
        clustering = nx.clustering(G_undirected, address)
        return clustering
    except:
        return 0.0


def breadth_first_search(
    G: nx.DiGraph,
    seed_address: str,
    max_hops: int = 3,
    min_transaction_value: float = 0
) -> Set[str]:
    """
    Perform BFS from seed address to find connected wallets.
    Used in multi-hop analysis for clustering.
    
    Returns: Set of connected addresses within max_hops
    """
    visited = set()
    queue = deque([(seed_address, 0)])  # (address, hop_count)
    visited.add(seed_address)
    
    while queue:
        current_addr, hops = queue.popleft()
        
        if hops >= max_hops:
            continue
        
        # Get neighbors (both incoming and outgoing)
        for neighbor in G.neighbors(current_addr):
            edge_data = G[current_addr][neighbor]
            
            # Filter by transaction value if specified
            if edge_data.get('total_value', 0) < min_transaction_value:
                continue
            
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, hops + 1))
        
        # Also check incoming edges
        for predecessor in G.predecessors(current_addr):
            edge_data = G[predecessor][current_addr]
            
            if edge_data.get('total_value', 0) < min_transaction_value:
                continue
            
            if predecessor not in visited:
                visited.add(predecessor)
                queue.append((predecessor, hops + 1))
    
    return visited


def find_shortest_path(
    G: nx.DiGraph,
    source: str,
    target: str,
    max_hops: int = 5
) -> Optional[List[str]]:
    """
    Find shortest path between two addresses.
    Used in flow tracing.
    
    Returns: List of addresses in path, or None if no path found
    """
    try:
        path = nx.shortest_path(G, source, target)
        if len(path) - 1 <= max_hops:  # path length = nodes - 1
            return path
        return None
    except nx.NetworkXNoPath:
        return None
    except nx.NodeNotFound:
        return None


def find_all_paths(
    G: nx.DiGraph,
    source: str,
    target: str,
    max_hops: int = 5,
    max_paths: int = 10
) -> List[List[str]]:
    """
    Find all simple paths between source and target within max_hops.
    
    Returns: List of paths (each path is a list of addresses)
    """
    try:
        paths = []
        for path in nx.all_simple_paths(G, source, target, cutoff=max_hops):
            paths.append(path)
            if len(paths) >= max_paths:
                break
        return paths
    except (nx.NodeNotFound, nx.NetworkXNoPath):
        return []


def detect_topology_type(G: nx.Graph, nodes: List[str]) -> str:
    """
    Detect network topology type for a cluster.
    
    Returns: 'hub_spoke', 'mesh', or 'chain'
    """
    if len(nodes) < 2:
        return 'isolated'
    
    subgraph = G.subgraph(nodes)
    
    # Calculate average clustering coefficient
    try:
        G_undirected = subgraph.to_undirected()
        avg_clustering = nx.average_clustering(G_undirected)
    except:
        avg_clustering = 0
    
    # Calculate degree distribution
    degrees = [subgraph.degree(node) for node in nodes]
    max_degree = max(degrees) if degrees else 0
    avg_degree = sum(degrees) / len(degrees) if degrees else 0
    
    # Hub-spoke: One node with high degree, low clustering
    if max_degree > avg_degree * 3 and avg_clustering < 0.3:
        return 'hub_spoke'
    
    # Mesh: High clustering, relatively uniform degrees
    elif avg_clustering > 0.5:
        return 'mesh'
    
    # Chain: Low clustering, low average degree
    elif avg_degree < 2.5 and avg_clustering < 0.3:
        return 'chain'
    
    return 'mixed'


def find_hub_nodes(G: nx.Graph, nodes: List[str], top_n: int = 3) -> List[str]:
    """
    Identify hub nodes in a cluster based on degree centrality.
    
    Returns: List of top N hub addresses
    """
    subgraph = G.subgraph(nodes)
    
    try:
        centrality = nx.degree_centrality(subgraph)
        sorted_nodes = sorted(centrality.items(), key=lambda x: x[1], reverse=True)
        return [node for node, _ in sorted_nodes[:top_n]]
    except:
        return []


def calculate_modularity(G: nx.Graph, communities: List[Set[str]]) -> float:
    """
    Calculate modularity score for community detection quality.
    Higher modularity = better defined communities.
    """
    try:
        # Convert communities to proper format for networkx
        community_dict = {}
        for i, community in enumerate(communities):
            for node in community:
                community_dict[node] = i
        
        return nx.algorithms.community.modularity(G, communities)
    except:
        return 0.0


def get_connected_components(G: nx.Graph) -> List[Set[str]]:
    """
    Find all connected components in the graph.
    """
    if G.is_directed():
        G = G.to_undirected()
    
    return [set(component) for component in nx.connected_components(G)]


def calculate_graph_density(G: nx.Graph, nodes: List[str]) -> float:
    """
    Calculate density of a subgraph.
    Density = actual_edges / possible_edges
    
    Used in cluster analysis.
    """
    subgraph = G.subgraph(nodes)
    try:
        return nx.density(subgraph)
    except:
        return 0.0


def get_k_hop_neighbors(
    G: nx.DiGraph,
    address: str,
    k: int = 2,
    direction: str = 'both'
) -> Set[str]:
    """
    Get all neighbors within k hops.
    
    Args:
        direction: 'in', 'out', or 'both'
    """
    neighbors = set()
    current_level = {address}
    
    for _ in range(k):
        next_level = set()
        
        for node in current_level:
            if direction in ['out', 'both']:
                next_level.update(G.successors(node))
            if direction in ['in', 'both']:
                next_level.update(G.predecessors(node))
        
        neighbors.update(next_level)
        current_level = next_level
    
    neighbors.discard(address)  # Remove the seed address itself
    return neighbors
