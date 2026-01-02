"""
Discovery-Specific Scoring System

Scores wallets based on OTC interactions and patterns,
NOT dependent on USD enrichment.
"""

from typing import Dict, List, Set
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DiscoveryScorer:
    """
    Präzises Scoring für discovered wallets.
    
    Funktioniert auch ohne USD enrichment durch:
    - OTC Desk Interaction Analysis
    - Transaction Pattern Recognition
    - Network Position Metrics
    """
    
    def __init__(self, known_otc_addresses: List[str]):
        self.known_otc_addresses = [addr.lower() for addr in known_otc_addresses]
    
    def score_discovered_wallet(
        self,
        address: str,
        transactions: List[Dict],
        counterparty_data: Dict,
        profile: Dict
    ) -> Dict:
        """
        Berechne präzisen Discovery Score.
        
        Args:
            address: Wallet-Adresse
            transactions: Alle Transaktionen
            counterparty_data: Daten aus simple_analyzer
            profile: Wallet-Profil vom profiler
            
        Returns:
            Dict mit score und breakdown
        """
        logger.info(f"📊 Discovery Scoring for {address[:10]}...")
        
        score = 0
        breakdown = {}
        
        # 1. OTC Desk Interactions (0-40 Punkte)
        otc_score, otc_details = self._score_otc_interactions(
            transactions,
            counterparty_data
        )
        score += otc_score
        breakdown['otc_interactions'] = otc_details
        
        # 2. Transaction Activity (0-25 Punkte)
        activity_score, activity_details = self._score_activity(
            transactions,
            profile
        )
        score += activity_score
        breakdown['activity'] = activity_details
        
        # 3. Network Position (0-20 Punkte)
        network_score, network_details = self._score_network_position(
            transactions,
            profile
        )
        score += network_score
        breakdown['network'] = network_details
        
        # 4. Volume Signals (0-15 Punkte) - auch ohne USD!
        volume_score, volume_details = self._score_volume_signals(
            transactions,
            profile
        )
        score += volume_score
        breakdown['volume'] = volume_details
        
        logger.info(f"✅ Discovery Score: {score}/100")
        for category, details in breakdown.items():
            logger.info(f"   • {category}: {details['score']}/{ details['max']} - {details['reason']}")
        
        return {
            'score': score,
            'breakdown': breakdown,
            'is_high_confidence': score >= 60,
            'is_medium_confidence': 40 <= score < 60,
            'recommendation': self._get_recommendation(score, breakdown)
        }
    
    def _score_otc_interactions(
        self,
        transactions: List[Dict],
        counterparty_data: Dict
    ) -> tuple:
        """
        Score basierend auf OTC Desk Interactions.
        MAX: 40 Punkte
        """
        score = 0
        
        # Zähle Transaktionen mit bekannten OTC Desks
        otc_interactions = 0
        otc_desks_interacted = set()
        otc_tx_volume = 0  # ETH/Token count
        
        for tx in transactions:
            from_addr = str(tx.get('from_address', '')).lower()
            to_addr = str(tx.get('to_address', '')).lower()
            
            # Check ob OTC Desk involviert
            for otc_addr in self.known_otc_addresses:
                if otc_addr in [from_addr, to_addr]:
                    otc_interactions += 1
                    otc_desks_interacted.add(otc_addr)
                    
                    # Zähle volume (auch ohne USD!)
                    value = float(tx.get('value_decimal', 0))
                    otc_tx_volume += value
                    break
        
        # Scoring
        unique_otc_count = len(otc_desks_interacted)
        
        if unique_otc_count >= 3:
            score = 40  # Interagiert mit 3+ OTC Desks = TOP!
            reason = f"Interacts with {unique_otc_count} OTC desks"
        elif unique_otc_count == 2:
            score = 30  # 2 OTC Desks = Sehr gut
            reason = f"Interacts with {unique_otc_count} OTC desks"
        elif unique_otc_count == 1:
            if otc_interactions >= 10:
                score = 25  # 1 OTC Desk aber viele TXs
                reason = f"{otc_interactions} txs with 1 OTC desk"
            elif otc_interactions >= 5:
                score = 20
                reason = f"{otc_interactions} txs with 1 OTC desk"
            else:
                score = 10
                reason = f"{otc_interactions} txs with 1 OTC desk"
        else:
            score = 0
            reason = "No OTC desk interactions"
        
        return score, {
            'score': score,
            'max': 40,
            'reason': reason,
            'otc_interactions': otc_interactions,
            'unique_otc_desks': unique_otc_count,
            'otc_tx_volume': otc_tx_volume
        }
    
    def _score_activity(
        self,
        transactions: List[Dict],
        profile: Dict
    ) -> tuple:
        """
        Score basierend auf Transaction Activity.
        MAX: 25 Punkte
        """
        score = 0
        
        tx_count = len(transactions)
        counterparties = profile.get('unique_counterparties', 0)
        
        # TX Count (0-12 Punkte)
        if tx_count >= 2000:
            tx_score = 12
        elif tx_count >= 1000:
            tx_score = 10
        elif tx_count >= 500:
            tx_score = 7
        elif tx_count >= 100:
            tx_score = 4
        else:
            tx_score = 2
        
        score += tx_score
        
        # Counterparty Count (0-8 Punkte)
        if counterparties >= 1000:
            cp_score = 8
        elif counterparties >= 500:
            cp_score = 6
        elif counterparties >= 100:
            cp_score = 4
        elif counterparties >= 50:
            cp_score = 2
        else:
            cp_score = 1
        
        score += cp_score
        
        # Frequency (0-5 Punkte)
        if tx_count > 0 and transactions:
            # Berechne Zeitspanne
            sorted_tx = sorted(transactions, key=lambda x: x.get('timestamp', datetime.min))
            if len(sorted_tx) >= 2:
                first = sorted_tx[0].get('timestamp')
                last = sorted_tx[-1].get('timestamp')
                if first and last:
                    days = (last - first).days
                    if days > 0:
                        txs_per_day = tx_count / days
                        if txs_per_day >= 10:
                            freq_score = 5
                        elif txs_per_day >= 5:
                            freq_score = 4
                        elif txs_per_day >= 1:
                            freq_score = 3
                        else:
                            freq_score = 2
                    else:
                        freq_score = 2
                else:
                    freq_score = 2
            else:
                freq_score = 1
        else:
            freq_score = 0
        
        score += freq_score
        
        reason = f"{tx_count} TXs, {counterparties} counterparties"
        
        return score, {
            'score': score,
            'max': 25,
            'reason': reason,
            'tx_count': tx_count,
            'counterparties': counterparties
        }
    
    def _score_network_position(
        self,
        transactions: List[Dict],
        profile: Dict
    ) -> tuple:
        """
        Score basierend auf Network Position.
        MAX: 20 Punkte
        """
        score = 0
        
        # Entropy (0-10 Punkte)
        entropy = profile.get('counterparty_entropy', 0)
        if entropy >= 8:
            entropy_score = 10
        elif entropy >= 6:
            entropy_score = 8
        elif entropy >= 4:
            entropy_score = 5
        elif entropy >= 2:
            entropy_score = 3
        else:
            entropy_score = 1
        
        score += entropy_score
        
        # DeFi Interactions (0-5 Punkte)
        has_defi = profile.get('has_defi_interactions', False)
        defi_score = 5 if has_defi else 0
        score += defi_score
        
        # Contract Interactions (0-5 Punkte)
        contract_interactions = sum(
            1 for tx in transactions 
            if tx.get('is_contract_interaction', False)
        )
        if contract_interactions >= 100:
            contract_score = 5
        elif contract_interactions >= 50:
            contract_score = 3
        elif contract_interactions >= 10:
            contract_score = 2
        else:
            contract_score = 0
        
        score += contract_score
        
        reason = f"Entropy {entropy:.1f}, DeFi: {has_defi}"
        
        return score, {
            'score': score,
            'max': 20,
            'reason': reason,
            'entropy': entropy,
            'has_defi': has_defi
        }
    
    def _score_volume_signals(
        self,
        transactions: List[Dict],
        profile: Dict
    ) -> tuple:
        """
        Score basierend auf Volume Signals (auch ohne USD!).
        MAX: 15 Punkte
        """
        score = 0
        
        # Wenn USD verfügbar, nutze es
        usd_volume = profile.get('total_volume_usd', 0)
        if usd_volume > 0:
            if usd_volume >= 10_000_000:
                score = 15
            elif usd_volume >= 1_000_000:
                score = 12
            elif usd_volume >= 100_000:
                score = 8
            else:
                score = 5
            
            reason = f"${usd_volume:,.0f} volume"
        else:
            # Fallback: ETH/Token Amounts
            total_eth = 0
            large_txs = 0
            
            for tx in transactions:
                value = float(tx.get('value_decimal', 0))
                total_eth += value
                
                # Zähle "große" Transaktionen (>0.1 ETH oder Token)
                if value > 0.1:
                    large_txs += 1
            
            # Score basierend auf ETH amount + large TX count
            if total_eth >= 100 or large_txs >= 500:
                score = 12
            elif total_eth >= 50 or large_txs >= 200:
                score = 10
            elif total_eth >= 10 or large_txs >= 100:
                score = 7
            elif total_eth >= 1 or large_txs >= 50:
                score = 4
            else:
                score = 2
            
            reason = f"{total_eth:.1f} ETH total, {large_txs} large TXs"
        
        return score, {
            'score': score,
            'max': 15,
            'reason': reason,
            'usd_volume': usd_volume
        }
    
    def _get_recommendation(self, score: int, breakdown: Dict) -> str:
        """Get recommendation based on score."""
        if score >= 70:
            return "HIGH_CONFIDENCE_OTC"
        elif score >= 60:
            return "LIKELY_OTC"
        elif score >= 40:
            return "POSSIBLE_OTC"
        else:
            return "UNLIKELY_OTC"
