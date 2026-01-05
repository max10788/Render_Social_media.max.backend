"""
Discovery-Specific Scoring System - IMPROVED

✅ FIXES:
- OTC Interactions: Berücksichtigt Source OTC Desk (mindestens 1 Interaction)
- Volume Weight: 25 Punkte (hoch von 15)
- OTC Weight: 30 Punkte (runter von 40)
- Context-aware Entropy Scoring (hohe TX-Werte = OK)
- Gestaffelte Recommendations (40%+ = interessant)
"""

from typing import Dict, List, Set, Optional
import logging
import math
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DiscoveryScorer:
    """
    Präzises Scoring für discovered wallets.
    
    ✅ IMPROVED:
    - Bessere Gewichtung (30/25/25/20 statt 40/15/25/20)
    - Source OTC Desk wird berücksichtigt
    - Context-aware Network Scoring
    - Gestaffelte Thresholds
    """
    
    def __init__(self, known_otc_addresses: List[str]):
        """
        Initialize scorer with known OTC desk addresses.
        
        Args:
            known_otc_addresses: List of known OTC desk addresses (lowercase)
        """
        self.known_otc_addresses = [addr.lower() for addr in known_otc_addresses]
        logger.debug(f"📊 DiscoveryScorer initialized with {len(self.known_otc_addresses)} known OTC desks")
    
    def score_discovered_wallet(
        self,
        address: str,
        transactions: List[Dict],
        counterparty_data: Dict,
        profile: Dict,
        source_otc_desk: Optional[str] = None  # ✅ NEW!
    ) -> Dict:
        """
        Berechne präzisen Discovery Score.
        
        ✅ IMPROVED: Berücksichtigt Source OTC Desk für garantierte OTC Interaction
        
        Args:
            address: Wallet-Adresse
            transactions: Alle Transaktionen
            counterparty_data: Daten aus simple_analyzer
            profile: Wallet-Profil vom profiler
            source_otc_desk: Source OTC Desk Adresse (von dem aus discovered wurde)
            
        Returns:
            Dict mit score und breakdown
        """
        logger.info(f"📊 Discovery Scoring for {address[:10]}...")
        
        score = 0
        breakdown = {}
        
        # ✅ Add source OTC desk to known addresses (temporary)
        temp_known_otc = self.known_otc_addresses.copy()
        if source_otc_desk:
            temp_known_otc.append(source_otc_desk.lower())
            logger.debug(f"   ✅ Added source OTC desk {source_otc_desk[:10]}... to known OTC list")
        
        # ====================================================================
        # 1. OTC INTERACTIONS (30 Punkte, runter von 40)
        # ====================================================================
        otc_score, otc_details = self._score_otc_interactions(
            transactions,
            counterparty_data,
            temp_known_otc  # ✅ Use temporary list
        )
        score += otc_score
        breakdown['otc_interactions'] = otc_details
        
        # ====================================================================
        # 2. VOLUME (25 Punkte, hoch von 15!)
        # ====================================================================
        volume_score, volume_details = self._score_volume_signals(
            transactions,
            profile
        )
        score += volume_score
        breakdown['volume'] = volume_details
        
        # ====================================================================
        # 3. ACTIVITY (25 Punkte)
        # ====================================================================
        activity_score, activity_details = self._score_activity(
            transactions,
            profile
        )
        score += activity_score
        breakdown['activity'] = activity_details
        
        # ====================================================================
        # 4. NETWORK (20 Punkte) - Context-aware!
        # ====================================================================
        network_score, network_details = self._score_network_position(
            transactions,
            profile
        )
        score += network_score
        breakdown['network'] = network_details
        
        # ====================================================================
        # RECOMMENDATION
        # ====================================================================
        recommendation = self._get_recommendation(score, breakdown)
        
        logger.info(f"✅ Discovery Score: {score}/100")
        for category, details in breakdown.items():
            logger.info(f"   • {category}: {details['score']}/{details['max']} - {details['reason']}")
        
        return {
            'score': score,
            'breakdown': breakdown,
            'recommendation': recommendation
        }
    
    def _score_otc_interactions(
        self,
        transactions: List[Dict],
        counterparty_data: Dict,
        known_otc_addresses: List[str]
    ) -> tuple:
        """
        Score basierend auf OTC Desk Interactions.
        
        ✅ IMPROVED: MAX 30 Punkte (runter von 40)
        ✅ FIXED: Garantiert mindestens 1 OTC Interaction bei Discovery
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
            for otc_addr in known_otc_addresses:
                if otc_addr in [from_addr, to_addr]:
                    otc_interactions += 1
                    otc_desks_interacted.add(otc_addr)
                    
                    # Zähle volume (auch ohne USD!)
                    value = float(tx.get('value_decimal', 0))
                    otc_tx_volume += value
                    break
        
        # ✅ WICHTIG: Bei Discovery gibt es MINDESTENS 1 OTC Interaction
        # (Source OTC Desk ist bereits in known_otc_addresses)
        otc_interactions = max(otc_interactions, 1)
        
        # Scoring
        unique_otc_count = len(otc_desks_interacted)
        
        if unique_otc_count >= 3:
            score = 30  # Interagiert mit 3+ OTC Desks = TOP!
            reason = f"{otc_interactions} txs with {unique_otc_count} OTC desks"
        elif unique_otc_count == 2:
            score = 25  # 2 OTC Desks = Sehr gut
            reason = f"{otc_interactions} txs with {unique_otc_count} OTC desks"
        elif unique_otc_count == 1:
            if otc_interactions >= 10:
                score = 20  # 1 OTC Desk aber viele TXs
                reason = f"{otc_interactions} txs with 1 OTC desk"
            elif otc_interactions >= 5:
                score = 17
                reason = f"{otc_interactions} txs with 1 OTC desk"
            elif otc_interactions >= 1:
                score = 15  # ✅ Mindestens 15 Punkte bei Discovery!
                reason = f"{otc_interactions} txs with 1 OTC desk"
            else:
                score = 10  # Fallback (sollte nicht vorkommen bei Discovery)
                reason = "Minimal OTC interaction"
        else:
            # ✅ Bei Discovery sollte das nie passieren
            score = 5
            reason = "No OTC desk interactions detected"
        
        return score, {
            'score': score,
            'max': 30,  # ✅ Runter von 40
            'reason': reason,
            'otc_interactions': otc_interactions,
            'unique_otc_desks': unique_otc_count,
            'otc_tx_volume': otc_tx_volume
        }
    
    def _score_volume_signals(
        self,
        transactions: List[Dict],
        profile: Dict
    ) -> tuple:
        """
        Score basierend auf Volume Signals.
        
        ✅ IMPROVED: MAX 25 Punkte (hoch von 15!)
        ✅ Bessere Thresholds für große Volumes
        """
        score = 0
        
        # Wenn USD verfügbar, nutze es
        usd_volume = profile.get('total_volume_usd', 0)
        
        if usd_volume > 0:
            # ✅ Neue Thresholds mit höherem Max
            if usd_volume >= 100_000_000:  # $100M+
                score = 25
                reason = f"${usd_volume:,.0f} volume (very high)"
            elif usd_volume >= 50_000_000:  # $50M+
                score = 22
                reason = f"${usd_volume:,.0f} volume (high)"
            elif usd_volume >= 10_000_000:  # $10M+
                score = 20
                reason = f"${usd_volume:,.0f} volume (significant)"
            elif usd_volume >= 5_000_000:  # $5M+
                score = 18
                reason = f"${usd_volume:,.0f} volume"
            elif usd_volume >= 1_000_000:  # $1M+
                score = 15
                reason = f"${usd_volume:,.0f} volume"
            elif usd_volume >= 500_000:  # $500K+
                score = 12
                reason = f"${usd_volume:,.0f} volume"
            elif usd_volume >= 100_000:  # $100K+
                score = 8
                reason = f"${usd_volume:,.0f} volume"
            else:
                score = 3
                reason = f"${usd_volume:,.0f} volume (low)"
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
                score = 20
            elif total_eth >= 50 or large_txs >= 200:
                score = 17
            elif total_eth >= 10 or large_txs >= 100:
                score = 12
            elif total_eth >= 1 or large_txs >= 50:
                score = 7
            else:
                score = 3
            
            reason = f"{total_eth:.1f} ETH total, {large_txs} large TXs"
        
        return score, {
            'score': score,
            'max': 25,  # ✅ Hoch von 15!
            'reason': reason,
            'usd_volume': usd_volume
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
            tx_score = 8
        elif tx_count >= 100:
            tx_score = 6
        elif tx_count >= 50:
            tx_score = 4
        else:
            tx_score = 2
        
        score += tx_score
        
        # Counterparty Count (0-13 Punkte)
        if counterparties >= 1000:
            cp_score = 13
        elif counterparties >= 500:
            cp_score = 10
        elif counterparties >= 100:
            cp_score = 7
        elif counterparties >= 50:
            cp_score = 5
        elif counterparties >= 20:
            cp_score = 3
        else:
            cp_score = 1
        
        score += cp_score
        
        score = min(score, 25)
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
        
        ✅ IMPROVED: Context-aware Entropy Scoring
        - Niedrige Entropy ist OK bei hohen TX-Werten
        - OTC Desks haben oft wenige große Partner = niedrige Entropy
        
        MAX: 20 Punkte
        """
        score = 0
        
        # Calculate entropy
        counterparties = {}
        address = profile.get('address', '').lower()
        
        for tx in transactions:
            from_addr = tx.get('from_address', '').lower()
            to_addr = tx.get('to_address', '').lower()
            
            # Bestimme Counterparty
            cp = to_addr if from_addr == address else from_addr
            counterparties[cp] = counterparties.get(cp, 0) + 1
        
        if counterparties:
            total_txs = sum(counterparties.values())
            probs = [count / total_txs for count in counterparties.values()]
            entropy = -sum(p * math.log2(p) for p in probs if p > 0)
        else:
            entropy = 0.0
        
        # ✅ Context-aware Entropy Scoring
        total_volume = profile.get('total_volume_usd', 0)
        avg_tx_value = total_volume / len(transactions) if len(transactions) > 0 else 0
        
        # Entropy base score (0-10 Punkte)
        if entropy >= 8:
            entropy_score = 10
        elif entropy >= 6:
            entropy_score = 8
        elif entropy >= 4:
            entropy_score = 6
        elif entropy >= 2:
            entropy_score = 4
        elif entropy >= 1:
            entropy_score = 3
        else:
            # ✅ Niedrige Entropy ist OK bei hohen Werten!
            if avg_tx_value > 100_000:  # High-value = professional
                entropy_score = 5  # ✅ Nicht bestrafen!
            elif avg_tx_value > 10_000:
                entropy_score = 3
            else:
                entropy_score = 1
        
        score += entropy_score
        
        # Counterparty count bonus (0-10 Punkte)
        num_counterparties = len(counterparties)
        if num_counterparties >= 100:
            cp_score = 10
        elif num_counterparties >= 50:
            cp_score = 8
        elif num_counterparties >= 20:
            cp_score = 6
        elif num_counterparties >= 10:
            cp_score = 4
        elif num_counterparties >= 5:
            cp_score = 2
        else:
            cp_score = 1
        
        score += cp_score
        
        score = min(score, 20)
        
        reason = f"Entropy {entropy:.1f}, {num_counterparties} counterparties, Avg ${avg_tx_value:,.0f}"
        
        return score, {
            'score': score,
            'max': 20,
            'reason': reason,
            'entropy': entropy,
            'num_counterparties': num_counterparties,
            'avg_tx_value': avg_tx_value
        }
    
    def _get_recommendation(self, score: int, breakdown: Dict) -> str:
        """
        Get recommendation based on score.
        
        ✅ IMPROVED: Gestaffelte Recommendations
        """
        if score >= 70:
            return "AUTO_SAVE"  # Sehr hohe Confidence
        elif score >= 55:
            return "LIKELY_OTC"  # Hohe Confidence
        elif score >= 40:
            return "REVIEW_RECOMMENDED"  # Mittel, interessant
        elif score >= 30:
            return "INTERESTING_FLAG"  # Niedrig aber erwähnenswert
        else:
            return "UNLIKELY_OTC"  # Zu niedrig
