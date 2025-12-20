from typing import List, Dict, Set, Optional
import hashlib
from collections import defaultdict
from datetime import datetime
from app.core.otc_analysis.utils.calculations import calculate_similarity_score
from app.core.otc_analysis.utils.graph_utils import (
    breadth_first_search,
    create_transaction_graph,
    detect_topology_type,
    find_hub_nodes,
    calculate_graph_density,
    calculate_modularity
)
import networkx as nx

class WalletClusteringService:
    """
    Multi-Hop Analysis & Clustering Algorithm.
    
    From doc:
    - Graph traversal from seed addresses
    - Similarity scoring for wallet grouping
    - Entity resolution (identifying wallets belonging to same entity)
    """
    
    def __init__(self, similarity_threshold: float = 0.7):
        self.similarity_threshold = similarity_threshold
        self.clusters = {}  # cluster_id -> cluster_data
        
        # Similarity weights from doc
        self.similarity_weights = {
            'transaction_frequency': 0.25,
            'temporal_proximity': 0.30,
            'amount_correlation': 0.25,
            'shared_counterparties': 0.20
        }
    
    def _generate_cluster_id(self, seed_addresses: List[str]) -> str:
        """Generate unique cluster ID from seed addresses."""
        combined = ''.join(sorted(seed_addresses))
        return hashlib.sha256(combined.encode()).hexdigest()
    
    def multi_hop_analysis(
        self,
        seed_address: str,
        transactions: List[Dict],
        max_hops: int = 3,
        min_transaction_value: float = 100000
    ) -> Set[str]:
        """
        Perform multi-hop graph traversal from seed address.
        
        Uses Breadth-First Search to find connected wallets.
        
        Args:
            seed_address: Starting wallet address
            transactions: All transactions to build graph from
            max_hops: Maximum depth to traverse
            min_transaction_value: Minimum USD value to consider edge
        
        Returns:
            Set of connected wallet addresses
        """
        # Build transaction graph
        G = create_transaction_graph(transactions)
        
        # Perform BFS
        connected_wallets = breadth_first_search(
            G,
            seed_address,
            max_hops=max_hops,
            min_transaction_value=min_transaction_value
        )
        
        return connected_wallets
    
    def calculate_wallet_similarity(
        self,
        wallet_a_data: Dict,
        wallet_b_data: Dict
    ) -> float:
        """
        Calculate similarity score between two wallets.
        
        Uses weighted similarity from doc:
        Similarity(A, B) = w1 * TransactionFrequency + 
                           w2 * TemporalProximity + 
                           w3 * AmountCorrelation +
                           w4 * SharedCounterparties
        
        Returns: Similarity score 0-1
        """
        return calculate_similarity_score(
            wallet_a_data,
            wallet_b_data,
            weights=self.similarity_weights
        )
    
    def cluster_similar_wallets(
        self,
        wallet_addresses: Set[str],
        wallet_profiles: Dict[str, Dict]
    ) -> List[Set[str]]:
        """
        Cluster wallets based on similarity scores.
        
        Uses agglomerative clustering approach:
        1. Calculate pairwise similarities
        2. Group wallets with similarity > threshold
        
        Args:
            wallet_addresses: Set of wallet addresses to cluster
            wallet_profiles: Dict mapping addresses to profile data
        
        Returns:
            List of wallet clusters (each cluster is a set of addresses)
        """
        addresses_list = list(wallet_addresses)
        n = len(addresses_list)
        
        # Initialize each wallet in its own cluster
        clusters = [{addr} for addr in addresses_list]
        
        # Calculate similarity matrix
        similarity_matrix = {}
        for i in range(n):
            for j in range(i + 1, n):
                addr_i = addresses_list[i]
                addr_j = addresses_list[j]
                
                profile_i = wallet_profiles.get(addr_i, {})
                profile_j = wallet_profiles.get(addr_j, {})
                
                similarity = self.calculate_wallet_similarity(profile_i, profile_j)
                similarity_matrix[(i, j)] = similarity
        
        # Merge clusters with high similarity
        merged = True
        while merged:
            merged = False
            best_merge = None
            best_similarity = self.similarity_threshold
            
            for i in range(len(clusters)):
                for j in range(i + 1, len(clusters)):
                    # Calculate average similarity between clusters
                    similarities = []
                    for addr_i in clusters[i]:
                        for addr_j in clusters[j]:
                            idx_i = addresses_list.index(addr_i)
                            idx_j = addresses_list.index(addr_j)
                            key = (min(idx_i, idx_j), max(idx_i, idx_j))
                            if key in similarity_matrix:
                                similarities.append(similarity_matrix[key])
                    
                    if similarities:
                        avg_similarity = sum(similarities) / len(similarities)
                        if avg_similarity > best_similarity:
                            best_similarity = avg_similarity
                            best_merge = (i, j)
            
            # Perform best merge
            if best_merge:
                i, j = best_merge
                clusters[i] = clusters[i].union(clusters[j])
                del clusters[j]
                merged = True
        
        return clusters
    
    def entity_resolution(
        self,
        transactions: List[Dict],
        wallet_profiles: Dict[str, Dict]
    ) -> Dict[str, Set[str]]:
        """
        Identify that multiple wallets belong to the same entity.
        
        Techniques:
        1. Peel Chain Analysis: Following change outputs
        2. Timing Correlation: Wallets active in same time windows
        3. Common Input Ownership Heuristic
        
        Returns:
            Dict mapping entity_id to set of wallet addresses
        """
        entities = defaultdict(set)
        entity_counter = 0
        
        # Build graph
        G = create_transaction_graph(transactions)
        
        # 1. Peel Chain Analysis
        # Look for patterns: A -> B (large) and B -> C (small, change)
        peel_chains = self._detect_peel_chains(transactions)
        
        for chain in peel_chains:
            entity_id = f"entity_{entity_counter}"
            entities[entity_id].update(chain)
            entity_counter += 1
        
        # 2. Timing Correlation
        # Group wallets that are always active at the same time
        timing_groups = self._detect_timing_correlation(wallet_profiles)
        
        for group in timing_groups:
            # Check if already in an entity
            existing_entity = None
            for entity_id, addresses in entities.items():
                if any(addr in addresses for addr in group):
                    existing_entity = entity_id
                    break
            
            if existing_entity:
                entities[existing_entity].update(group)
            else:
                entity_id = f"entity_{entity_counter}"
                entities[entity_id].update(group)
                entity_counter += 1
        
        # 3. Common Input Ownership
        # If multiple addresses appear as inputs in same transaction
        common_input_groups = self._detect_common_inputs(transactions)
        
        for group in common_input_groups:
            # Merge with existing entities if overlap
            existing_entity = None
            for entity_id, addresses in entities.items():
                if any(addr in addresses for addr in group):
                    existing_entity = entity_id
                    break
            
            if existing_entity:
                entities[existing_entity].update(group)
            else:
                entity_id = f"entity_{entity_counter}"
                entities[entity_id].update(group)
                entity_counter += 1
        
        return dict(entities)
    
    def _detect_peel_chains(self, transactions: List[Dict]) -> List[Set[str]]:
        """
        Detect peel chain patterns.
        
        A peel chain is: Large amount -> Address A -> Small amount (change)
        Indicates A is controlled by same entity.
        """
        chains = []
        
        # Group transactions by address
        addr_txs = defaultdict(list)
        for tx in transactions:
            addr_txs[tx['to_address']].append(tx)
        
        for address, incoming_txs in addr_txs.items():
            for in_tx in incoming_txs:
                # Look for outgoing tx shortly after
                outgoing = [
                    tx for tx in transactions
                    if tx['from_address'] == address
                    and tx['timestamp'] > in_tx['timestamp']
                    and (tx['timestamp'] - in_tx['timestamp']).total_seconds() < 3600  # Within 1 hour
                ]
                
                for out_tx in outgoing:
                    # Check if it's a peel (much smaller amount)
                    if out_tx.get('usd_value', 0) < in_tx.get('usd_value', 1) * 0.1:
                        # Likely a peel chain
                        chain = {in_tx['from_address'], address, out_tx['to_address']}
                        chains.append(chain)
        
        return chains
    
    def _detect_timing_correlation(
        self,
        wallet_profiles: Dict[str, Dict],
        correlation_threshold: float = 0.8
    ) -> List[Set[str]]:
        """
        Detect wallets with highly correlated activity times.
        """
        groups = []
        addresses = list(wallet_profiles.keys())
        
        for i in range(len(addresses)):
            for j in range(i + 1, len(addresses)):
                addr_i = addresses[i]
                addr_j = addresses[j]
                
                hours_i = set(wallet_profiles[addr_i].get('active_hours', []))
                hours_j = set(wallet_profiles[addr_j].get('active_hours', []))
                
                if not hours_i or not hours_j:
                    continue
                
                # Calculate Jaccard similarity
                intersection = len(hours_i & hours_j)
                union = len(hours_i | hours_j)
                
                if union > 0:
                    similarity = intersection / union
                    
                    if similarity >= correlation_threshold:
                        # Check if already in a group
                        found = False
                        for group in groups:
                            if addr_i in group or addr_j in group:
                                group.add(addr_i)
                                group.add(addr_j)
                                found = True
                                break
                        
                        if not found:
                            groups.append({addr_i, addr_j})
        
        return groups
    
    def _detect_common_inputs(self, transactions: List[Dict]) -> List[Set[str]]:
        """
        Detect transactions with multiple input addresses.
        These likely belong to same entity.
        
        Note: This is simplified - real implementation would need access
        to raw transaction inputs, not just from_address.
        """
        # This is a placeholder - in reality you'd need to parse tx inputs
        # For now, we'll use a heuristic: transactions from same address
        # within short time frame likely same entity
        
        groups = []
        # Implementation would go here
        return groups
    
    def create_cluster(
        self,
        seed_addresses: List[str],
        transactions: List[Dict],
        wallet_profiles: Dict[str, Dict],
        max_hops: int = 3
    ) -> Dict:
        """
        Create a complete cluster from seed addresses.
        
        Process:
        1. Multi-hop analysis to find connected wallets
        2. Calculate similarity scores
        3. Cluster similar wallets
        4. Analyze network topology
        5. Identify hub nodes
        
        Returns:
            Complete cluster data
        """
        # Step 1: Find all connected wallets
        all_connected = set(seed_addresses)
        for seed in seed_addresses:
            connected = self.multi_hop_analysis(seed, transactions, max_hops)
            all_connected.update(connected)
        
        # Step 2: Cluster by similarity
        wallet_clusters = self.cluster_similar_wallets(all_connected, wallet_profiles)
        
        # Find the largest cluster (or merge all if highly connected)
        main_cluster = max(wallet_clusters, key=len) if wallet_clusters else set(seed_addresses)
        
        # Step 3: Build graph for cluster
        cluster_txs = [
            tx for tx in transactions
            if tx['from_address'] in main_cluster or tx['to_address'] in main_cluster
        ]
        G = create_transaction_graph(cluster_txs)
        
        # Step 4: Analyze topology
        topology_type = detect_topology_type(G, list(main_cluster))
        hub_addresses = find_hub_nodes(G, list(main_cluster), top_n=3)
        density = calculate_graph_density(G, list(main_cluster))
        
        # Step 5: Calculate metrics
        first_activity = min(tx['timestamp'] for tx in cluster_txs) if cluster_txs else None
        last_activity = max(tx['timestamp'] for tx in cluster_txs) if cluster_txs else None
        total_volume = sum(tx.get('usd_value', 0) for tx in cluster_txs)
        
        # Generate cluster ID
        cluster_id = self._generate_cluster_id(seed_addresses)
        
        cluster_data = {
            'cluster_id': cluster_id,
            'cluster_type': 'otc_network',  # Could be determined by analysis
            'wallet_count': len(main_cluster),
            'seed_addresses': seed_addresses,
            'member_addresses': list(main_cluster),
            'first_activity': first_activity,
            'last_activity': last_activity,
            'total_transactions': len(cluster_txs),
            'total_volume_usd': total_volume,
            'topology_type': topology_type,
            'hub_addresses': hub_addresses,
            'cluster_density': density,
            'created_at': datetime.utcnow()
        }
        
        # Store cluster
        self.clusters[cluster_id] = cluster_data
        
        return cluster_data
    
    def update_cluster(
        self,
        cluster_id: str,
        new_transactions: List[Dict],
        wallet_profiles: Dict[str, Dict]
    ) -> Dict:
        """
        Incrementally update existing cluster with new data.
        
        Used by clustering worker that runs hourly.
        """
        if cluster_id not in self.clusters:
            raise ValueError(f"Cluster {cluster_id} not found")
        
        cluster = self.clusters[cluster_id]
        existing_members = set(cluster['member_addresses'])
        
        # Find new potential members from transactions
        new_addresses = set()
        for tx in new_transactions:
            if tx['from_address'] in existing_members:
                new_addresses.add(tx['to_address'])
            if tx['to_address'] in existing_members:
                new_addresses.add(tx['from_address'])
        
        # Check similarity of new addresses
        for new_addr in new_addresses:
            if new_addr in existing_members:
                continue
            
            new_profile = wallet_profiles.get(new_addr, {})
            
            # Check similarity with cluster members
            similarities = []
            for member in list(existing_members)[:10]:  # Sample to avoid O(n²)
                member_profile = wallet_profiles.get(member, {})
                sim = self.calculate_wallet_similarity(new_profile, member_profile)
                similarities.append(sim)
            
            if similarities and sum(similarities) / len(similarities) >= self.similarity_threshold:
                existing_members.add(new_addr)
        
        # Update cluster data
        cluster['member_addresses'] = list(existing_members)
        cluster['wallet_count'] = len(existing_members)
        cluster['last_updated'] = datetime.utcnow()
        
        return cluster
    
    def get_cluster_metrics(self, cluster_id: str) -> Dict:
        """Get detailed metrics for a cluster."""
        if cluster_id not in self.clusters:
            return None
        
        cluster = self.clusters[cluster_id]
        
        return {
            'cluster_id': cluster_id,
            'wallet_count': cluster['wallet_count'],
            'total_volume': cluster['total_volume_usd'],
            'topology': cluster['topology_type'],
            'density': cluster['cluster_density'],
            'hub_count': len(cluster['hub_addresses']),
            'age_days': (datetime.utcnow() - cluster['first_activity']).days if cluster['first_activity'] else 0
        }
