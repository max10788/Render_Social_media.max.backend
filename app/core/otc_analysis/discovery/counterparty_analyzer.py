"""
Counterparty Analyzer - Discovers OTC desks through transaction analysis.
"""

from typing import List, Dict, Set, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class CounterpartyAnalyzer:
    """
    Discovers potential OTC desks by analyzing counterparties.
    
    Logic:
    1. Analyze known OTC desk transactions
    2. Extract all counterparties (sender/receiver)
    3. Score based on:
       - OTC desk interaction count
       - Transaction volumes
       - Transaction patterns
    """
    
    def __init__(self, db, transaction_extractor, wallet_profiler):
        self.db = db
        self.transaction_extractor = transaction_extractor
        self.wallet_profiler = wallet_profiler
    
    def discover_counterparties(
        self,
        known_otc_addresses: List[str],
        min_interactions: int = 2,
        min_volume: float = 1_000_000,
        max_candidates: int = 10
    ) -> List[Dict]:
        """Discover potential OTC desks."""
        logger.info(f"🔍 Analyzing {len(known_otc_addresses)} OTC desks...")
        
        counterparty_scores = {}
        
        # Analyze each known OTC desk
        for otc_address in known_otc_addresses:
            logger.info(f"   📊 {otc_address[:10]}...")
            
            try:
                # Get transactions (MIT LIMIT)
                transactions = transaction_extractor.extract_wallet_transactions(
                    otc_address,
                    include_internal=True,
                    include_tokens=True
                )[:num_transactions * 2]  # ← Begrenze SOFORT nach Fetch ✅
                
                if not transactions:
                    logger.info("ℹ️ No transactions found")
                    return []
                
                # Sort and take recent N
                recent_txs = sorted(
                    transactions, 
                    key=lambda x: x.get('timestamp', datetime.min), 
                    reverse=True
                )[:num_transactions]
                
                # Extract counterparties
                for tx in recent_txs:
                    counterparty = self._extract_counterparty(tx, otc_address)
                    
                    if not counterparty or counterparty.lower() == otc_address.lower():
                        continue
                    
                    # Skip known OTC desks
                    if counterparty.lower() in [a.lower() for a in known_otc_addresses]:
                        continue
                    
                    # Initialize tracking
                    if counterparty not in counterparty_scores:
                        counterparty_scores[counterparty] = {
                            'address': counterparty,
                            'otc_interactions': set(),
                            'total_volume': 0,
                            'transaction_count': 0,
                            'avg_transaction': 0,
                            'first_seen': tx.get('timestamp'),
                            'last_seen': tx.get('timestamp')
                        }
                    
                    # Update metrics
                    data = counterparty_scores[counterparty]
                    data['otc_interactions'].add(otc_address.lower())
                    
                    if tx.get('usd_value'):
                        data['total_volume'] += tx['usd_value']
                    
                    data['transaction_count'] += 1
                    
                    tx_time = tx.get('timestamp')
                    if tx_time:
                        if tx_time < data['first_seen']:
                            data['first_seen'] = tx_time
                        if tx_time > data['last_seen']:
                            data['last_seen'] = tx_time
                
            except Exception as e:
                logger.error(f"❌ Error analyzing {otc_address[:10]}: {e}")
                continue
        
        # Filter and score
        candidates = []
        
        for address, data in counterparty_scores.items():
            otc_count = len(data['otc_interactions'])
            
            # Calculate average
            if data['transaction_count'] > 0:
                data['avg_transaction'] = data['total_volume'] / data['transaction_count']
            
            # Apply filters
            if otc_count < min_interactions or data['total_volume'] < min_volume:
                continue
            
            # Calculate score
            score = self._calculate_score(data, otc_count)
            
            candidates.append({
                'address': address,
                'otc_interaction_count': otc_count,
                'interacted_with': list(data['otc_interactions']),
                'total_volume': data['total_volume'],
                'transaction_count': data['transaction_count'],
                'avg_transaction': data['avg_transaction'],
                'discovery_score': score,
                'first_seen': data['first_seen'],
                'last_seen': data['last_seen']
            })
        
        candidates.sort(key=lambda x: x['discovery_score'], reverse=True)
        
        logger.info(f"✅ Found {len(candidates)} candidates")
        
        return candidates[:max_candidates]
    
    def _extract_counterparty(self, tx: Dict, wallet_address: str) -> Optional[str]:
        """Extract counterparty from transaction."""
        from_addr = tx.get('from', '').lower()
        to_addr = tx.get('to', '').lower()
        wallet_lower = wallet_address.lower()
        
        if from_addr == wallet_lower:
            return to_addr
        if to_addr == wallet_lower:
            return from_addr
        
        return None
    
    def _calculate_score(self, data: Dict, otc_count: int) -> float:
        """Calculate discovery score (0-100)."""
        score = 0
        
        # OTC interactions (0-40)
        if otc_count >= 3:
            score += 40
        elif otc_count == 2:
            score += 20
        
        # Volume (0-30)
        volume = data['total_volume']
        if volume >= 100_000_000:
            score += 30
        elif volume >= 50_000_000:
            score += 25
        elif volume >= 10_000_000:
            score += 20
        elif volume >= 5_000_000:
            score += 15
        elif volume >= 1_000_000:
            score += 10
        
        # Avg transaction (0-20)
        avg = data['avg_transaction']
        if avg >= 1_000_000:
            score += 20
        elif avg >= 500_000:
            score += 15
        elif avg >= 100_000:
            score += 10
        elif avg >= 50_000:
            score += 5
        
        # Frequency (0-10)
        tx_count = data['transaction_count']
        if tx_count >= 100:
            score += 10
        elif tx_count >= 50:
            score += 7
        elif tx_count >= 20:
            score += 5
        elif tx_count >= 10:
            score += 3
        
        return score
