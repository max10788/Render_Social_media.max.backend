# ============================================================================
# data_sources/ground_truth.py
# ============================================================================
"""Access to ground truth labels and context databases."""

from typing import Dict, Set, List, Optional


class GroundTruthDB:
    """
    Mock database for ground truth labels and context information.
    In production, this would connect to real databases like Elliptic, WalletExplorer, etc.
    """
    
    def __init__(self):
        # Mock data - in production, load from actual databases
        self.exchanges = {
            '1BitcoinAddress1', '1CoinbaseAddr1', '1BinanceAddr1'
        }
        
        self.mixers = {
            '1WasabiMixer1', '1TornadoCash1', '1CoinJoinAddr1'
        }
        
        self.institutions = {
            '1MicroStrategy1', '1TeslaWallet1', '1GrayscaleAddr1'
        }
        
        self.whale_clusters = {
            '1WhaleCluster1', '1WhaleCluster2'
        }
        
        # Network graph (mock data)
        self.network_graph = {}
        
        # Labels from Elliptic, WalletExplorer, etc.
        self.labels = {}
    
    def is_exchange(self, address: str) -> bool:
        """Check if address belongs to an exchange."""
        return address in self.exchanges
    
    def count_exchange_interactions(self, address: str) -> int:
        """Count interactions with known exchanges."""
        # Mock implementation
        return len([addr for addr in self.exchanges if self._has_interaction(address, addr)])
    
    def in_mixer_cluster(self, address: str) -> bool:
        """Check if address is in a known mixer cluster."""
        return address in self.mixers
    
    def interacts_with_mixer(self, address: str) -> bool:
        """Check if address interacts with known mixers."""
        return any(self._has_interaction(address, mixer) for mixer in self.mixers)
    
    def tornado_cash_interaction(self, address: str) -> bool:
        """Check for Tornado Cash interaction."""
        return '1TornadoCash1' in self.network_graph.get(address, [])
    
    def is_institutional(self, address: str) -> bool:
        """Check if address belongs to an institution."""
        return address in self.institutions
    
    def in_whale_cluster(self, address: str) -> bool:
        """Check if address is in a whale cluster."""
        return address in self.whale_clusters
    
    def get_in_degree(self, address: str) -> int:
        """Get network in-degree."""
        return len([n for n, neighbors in self.network_graph.items() if address in neighbors])
    
    def get_out_degree(self, address: str) -> int:
        """Get network out-degree."""
        return len(self.network_graph.get(address, []))
    
    def get_betweenness(self, address: str) -> float:
        """Get betweenness centrality (mock)."""
        # In production, calculate from actual network graph
        return 0.05 if address in self.mixers else 0.01
    
    def get_eigenvector(self, address: str) -> float:
        """Get eigenvector centrality (mock)."""
        # In production, calculate from actual network graph
        return 0.08 if address in self.whale_clusters else 0.02
    
    def count_smart_contract_calls(self, address: str) -> int:
        """Count smart contract interactions."""
        # Mock implementation
        return 0
    
    def count_dex_cex_interactions(self, address: str) -> int:
        """Count DEX/CEX interactions."""
        return self.count_exchange_interactions(address)
    
    def get_cluster_size(self, address: str) -> int:
        """Get size of address cluster."""
        # Mock: return 1 for singleton, higher for clusters
        return 1
    
    def get_label(self, address: str) -> Optional[str]:
        """Get ground truth label if available."""
        return self.labels.get(address)
    
    def _has_interaction(self, addr1: str, addr2: str) -> bool:
        """Check if two addresses have interacted (mock)."""
        return addr2 in self.network_graph.get(addr1, [])
