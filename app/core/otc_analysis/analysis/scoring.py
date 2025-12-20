from typing import Dict, Optional
from app.core.otc_analysis.utils.calculations import (
    sigmoid,
    weighted_average,
    normalize_score
)

class OTCScoringSystem:
    """
    Multi-Factor Confidence Score (0-100) implementation.
    
    From doc:
    OTC_Score = (
        TransferSize_Score * 0.30 +
        WalletProfile_Score * 0.25 +
        NetworkPosition_Score * 0.20 +
        Timing_Score * 0.15 +
        KnownEntity_Score * 0.10
    )
    """
    
    def __init__(self):
        # Weights from doc
        self.weights = {
            'transfer_size': 0.30,
            'wallet_profile': 0.25,
            'network_position': 0.20,
            'timing': 0.15,
            'known_entity': 0.10
        }
    
    def calculate_transfer_size_score(self, usd_value: float) -> float:
        """
        TransferSize_Score: Sigmoid function based on USD value.
        
        Sigmoid with midpoint at $500K.
        Returns: 0-100
        """
        if not usd_value:
            return 0.0
        
        # Sigmoid function centered at $500K
        normalized = sigmoid(usd_value, midpoint=500000, steepness=0.000002)
        return normalized * 100
    
    def calculate_wallet_profile_score(self, wallet_data: Dict) -> float:
        """
        WalletProfile_Score: Composite from Tx frequency, DeFi interactions.
        
        Factors:
        - Low transaction frequency (higher score)
        - High average transaction value (higher score)
        - No DeFi interactions (higher score)
        - Low diversity of counterparties (higher score)
        
        Returns: 0-100
        """
        score = 0
        
        # 1. Transaction Frequency (inverse - lower is better for OTC)
        tx_freq = wallet_data.get('transaction_frequency', 0)
        if tx_freq > 0:
            # Score decreases as frequency increases
            freq_score = max(0, 100 - (tx_freq * 30))
            score += freq_score * 0.25
        else:
            score += 25
        
        # 2. Average Transaction Value
        avg_value = wallet_data.get('avg_transaction_usd', 0)
        if avg_value >= 1000000:  # $1M+
            score += 30
        elif avg_value >= 500000:  # $500K+
            score += 20
        elif avg_value >= 100000:  # $100K+
            score += 10
        
        # 3. DeFi Interactions (absence is positive)
        has_defi = wallet_data.get('has_defi_interactions', False)
        has_dex = wallet_data.get('has_dex_swaps', False)
        
        if not has_defi and not has_dex:
            score += 25
        elif not has_dex:
            score += 15
        
        # 4. Counterparty Diversity (low entropy = high score)
        entropy = wallet_data.get('counterparty_entropy', 5.0)
        # Lower entropy is better for OTC (repeated counterparties)
        entropy_score = max(0, 100 - (entropy * 20))
        score += entropy_score * 0.20
        
        return min(100, score)
    
    def calculate_network_position_score(self, network_metrics: Dict) -> float:
        """
        NetworkPosition_Score: Betweenness & Degree Centrality.
        
        High betweenness = hub behavior = higher OTC probability
        
        Returns: 0-100
        """
        betweenness = network_metrics.get('betweenness_centrality', 0)
        degree = network_metrics.get('degree_centrality', 0)
        clustering = network_metrics.get('clustering_coefficient', 1.0)
        
        score = 0
        
        # 1. Betweenness Centrality (normalized 0-1)
        # High betweenness indicates hub/bridge role
        betweenness_score = betweenness * 100
        score += betweenness_score * 0.5
        
        # 2. Degree Centrality
        # High degree = many connections
        degree_score = degree * 100
        score += degree_score * 0.3
        
        # 3. Clustering Coefficient (inverse - low is better for hub-spoke)
        # OTC desks typically have low clustering (star topology)
        clustering_score = (1 - clustering) * 100
        score += clustering_score * 0.2
        
        return min(100, score)
    
    def calculate_timing_score(self, timing_data: Dict) -> float:
        """
        Timing_Score: Off-hours bonus.
        
        Returns: 0-100
        """
        is_off_hours = timing_data.get('is_off_hours', False)
        is_weekend = timing_data.get('is_weekend', False)
        
        score = 0
        
        if is_off_hours:
            score += 60
        if is_weekend:
            score += 40
        
        return min(100, score)
    
    def calculate_known_entity_score(
        self,
        from_labels: Optional[Dict],
        to_labels: Optional[Dict]
    ) -> float:
        """
        KnownEntity_Score: Binary (50 if OTC desk, 0 otherwise).
        
        Actually can be more nuanced based on confidence.
        
        Returns: 0-100
        """
        score = 0
        
        # Check from_address
        if from_labels and from_labels.get('entity_type') == 'otc_desk':
            confidence = from_labels.get('confidence', 1.0)
            score = max(score, 100 * confidence)
        
        # Check to_address
        if to_labels and to_labels.get('entity_type') == 'otc_desk':
            confidence = to_labels.get('confidence', 1.0)
            score = max(score, 100 * confidence)
        
        return min(100, score)
    
    def calculate_otc_score(
        self,
        transaction: Dict,
        wallet_data: Dict,
        network_metrics: Dict,
        timing_data: Dict,
        from_labels: Optional[Dict] = None,
        to_labels: Optional[Dict] = None
    ) -> Dict:
        """
        Calculate comprehensive OTC score with breakdown.
        
        Returns:
            {
                'total_score': float (0-100),
                'confidence_level': str,
                'component_scores': dict,
                'weighted_components': dict
            }
        """
        # Calculate individual component scores
        transfer_size_score = self.calculate_transfer_size_score(
            transaction.get('usd_value', 0)
        )
        
        wallet_profile_score = self.calculate_wallet_profile_score(wallet_data)
        
        network_position_score = self.calculate_network_position_score(network_metrics)
        
        timing_score = self.calculate_timing_score(timing_data)
        
        known_entity_score = self.calculate_known_entity_score(
            from_labels, to_labels
        )
        
        # Store component scores
        component_scores = {
            'transfer_size': transfer_size_score,
            'wallet_profile': wallet_profile_score,
            'network_position': network_position_score,
            'timing': timing_score,
            'known_entity': known_entity_score
        }
        
        # Calculate weighted components
        weighted_components = {
            'transfer_size': transfer_size_score * self.weights['transfer_size'],
            'wallet_profile': wallet_profile_score * self.weights['wallet_profile'],
            'network_position': network_position_score * self.weights['network_position'],
            'timing': timing_score * self.weights['timing'],
            'known_entity': known_entity_score * self.weights['known_entity']
        }
        
        # Calculate total score
        total_score = sum(weighted_components.values())
        
        # Determine confidence level
        if total_score >= 80:
            confidence_level = 'high_confidence'
        elif total_score >= 60:
            confidence_level = 'medium'
        elif total_score >= 40:
            confidence_level = 'low'
        else:
            confidence_level = 'suspected'
        
        return {
            'total_score': round(total_score, 2),
            'confidence_level': confidence_level,
            'component_scores': component_scores,
            'weighted_components': weighted_components
        }
    
    def batch_score(
        self,
        transactions: List[Dict],
        wallet_profiles: Dict[str, Dict],
        network_data: Dict[str, Dict],
        labels_data: Dict[str, Dict]
    ) -> List[Dict]:
        """
        Score multiple transactions in batch.
        
        Returns list of scoring results.
        """
        results = []
        
        for tx in transactions:
            from_addr = tx.get('from_address')
            to_addr = tx.get('to_address')
            
            wallet_data = wallet_profiles.get(from_addr, {})
            network_metrics = network_data.get(from_addr, {})
            from_labels = labels_data.get(from_addr)
            to_labels = labels_data.get(to_addr)
            
            # Timing data from transaction
            timing_data = {
                'is_off_hours': tx.get('is_off_hours', False),
                'is_weekend': tx.get('is_weekend', False)
            }
            
            score_result = self.calculate_otc_score(
                tx, wallet_data, network_metrics, timing_data,
                from_labels, to_labels
            )
            
            results.append({
                'tx_hash': tx.get('tx_hash'),
                **score_result
            })
        
        return results
