"""
Volume Scorer - Scores wallets based on transaction volume and patterns
========================================================================

✅ VOLUME-FOCUSED SCORING (not OTC-specific):
- Total volume thresholds
- Transaction size patterns
- Token diversity
- Counterparty count
- Activity frequency

Version: 1.0
Date: 2025-01-12
"""

from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class VolumeScorer:
    """
    Scores wallets based on volume and transaction patterns.
    
    Similar to DiscoveryScorer but focused on:
    - Total USD volume
    - Average transaction size
    - Transaction frequency
    - Token diversity
    - Unique counterparties
    """
    
    def __init__(self, min_volume_threshold: float = 1_000_000):
        """
        Initialize VolumeScorer.
        
        Args:
            min_volume_threshold: Minimum USD volume to save (default: $1M)
        """
        self.min_volume_threshold = min_volume_threshold
        
        logger.info(
            f"✅ VolumeScorer initialized "
            f"(min threshold: ${min_volume_threshold:,.0f})"
        )
    
    def score_high_volume_wallet(
        self,
        address: str,
        transactions: List[Dict],
        counterparty_data: Dict,
        profile: Dict
    ) -> Dict:
        """
        Score a wallet for high-volume characteristics.
        
        Args:
            address: Wallet address
            transactions: List of wallet transactions
            counterparty_data: Data from counterparty extraction
            profile: Wallet profile from analyzer
            
        Returns:
            Dict with:
            - score: Volume score (0-100)
            - classification: Wallet classification
            - meets_threshold: Whether meets volume threshold
            - breakdown: Scoring breakdown
        """
        logger.info(f"📊 Scoring high-volume wallet {address[:10]}...")
        
        try:
            # Extract metrics
            total_volume = profile.get('total_volume_usd', 0)
            tx_count = profile.get('transfer_count', 0) or len(transactions)
            avg_transaction = profile.get('avg_transfer_usd', 0) or (
                total_volume / tx_count if tx_count > 0 else 0
            )
            token_diversity = profile.get('token_diversity', 0)
            unique_counterparties = profile.get('unique_counterparties', 0)
            large_transfer_count = profile.get('large_transfer_count', 0)
            
            # Initialize scoring breakdown
            breakdown = {
                'total_volume_score': 0,
                'transaction_size_score': 0,
                'frequency_score': 0,
                'diversity_score': 0,
                'large_transfer_score': 0
            }
            
            # ================================================================
            # SCORING CRITERIA
            # ================================================================
            
            # 1. TOTAL VOLUME (0-30 points)
            if total_volume >= 100_000_000:  # $100M+
                breakdown['total_volume_score'] = 30
            elif total_volume >= 50_000_000:  # $50M+
                breakdown['total_volume_score'] = 25
            elif total_volume >= 10_000_000:  # $10M+
                breakdown['total_volume_score'] = 20
            elif total_volume >= 5_000_000:   # $5M+
                breakdown['total_volume_score'] = 15
            elif total_volume >= 1_000_000:   # $1M+
                breakdown['total_volume_score'] = 10
            else:
                breakdown['total_volume_score'] = 5
            
            # 2. AVERAGE TRANSACTION SIZE (0-25 points)
            if avg_transaction >= 1_000_000:  # $1M+ avg
                breakdown['transaction_size_score'] = 25
            elif avg_transaction >= 500_000:  # $500K+ avg
                breakdown['transaction_size_score'] = 20
            elif avg_transaction >= 100_000:  # $100K+ avg
                breakdown['transaction_size_score'] = 15
            elif avg_transaction >= 50_000:   # $50K+ avg
                breakdown['transaction_size_score'] = 10
            elif avg_transaction >= 10_000:   # $10K+ avg
                breakdown['transaction_size_score'] = 5
            
            # 3. TRANSACTION FREQUENCY (0-20 points)
            if tx_count >= 500:
                breakdown['frequency_score'] = 20
            elif tx_count >= 200:
                breakdown['frequency_score'] = 17
            elif tx_count >= 100:
                breakdown['frequency_score'] = 15
            elif tx_count >= 50:
                breakdown['frequency_score'] = 12
            elif tx_count >= 20:
                breakdown['frequency_score'] = 8
            elif tx_count >= 10:
                breakdown['frequency_score'] = 5
            
            # 4. TOKEN DIVERSITY & COUNTERPARTIES (0-15 points)
            diversity_score = 0
            
            # Token diversity (0-8 points)
            if token_diversity >= 10:
                diversity_score += 8
            elif token_diversity >= 5:
                diversity_score += 6
            elif token_diversity >= 3:
                diversity_score += 4
            elif token_diversity >= 2:
                diversity_score += 2
            
            # Counterparty count (0-7 points)
            if unique_counterparties >= 50:
                diversity_score += 7
            elif unique_counterparties >= 20:
                diversity_score += 5
            elif unique_counterparties >= 10:
                diversity_score += 3
            elif unique_counterparties >= 5:
                diversity_score += 2
            
            breakdown['diversity_score'] = diversity_score
            
            # 5. LARGE TRANSFERS (0-10 points)
            # $100K+ transfers indicate institutional activity
            if large_transfer_count >= 20:
                breakdown['large_transfer_score'] = 10
            elif large_transfer_count >= 10:
                breakdown['large_transfer_score'] = 8
            elif large_transfer_count >= 5:
                breakdown['large_transfer_score'] = 6
            elif large_transfer_count >= 2:
                breakdown['large_transfer_score'] = 4
            elif large_transfer_count >= 1:
                breakdown['large_transfer_score'] = 2
            
            # ================================================================
            # CALCULATE FINAL SCORE
            # ================================================================
            
            final_score = sum(breakdown.values())
            
            # ================================================================
            # CLASSIFICATION
            # ================================================================
            
            classification = self._classify_wallet(
                total_volume,
                avg_transaction,
                tx_count,
                token_diversity,
                large_transfer_count
            )
            
            # ================================================================
            # THRESHOLD CHECK
            # ================================================================
            
            meets_threshold = total_volume >= self.min_volume_threshold
            
            # Log result
            logger.info(f"   📊 Volume Score: {final_score}/100")
            logger.info(f"   💰 Total Volume: ${total_volume:,.0f}")
            logger.info(f"   📈 Avg Transaction: ${avg_transaction:,.0f}")
            logger.info(f"   🔢 TX Count: {tx_count}")
            logger.info(f"   🏷️  Classification: {classification['classification']}")
            logger.info(f"   ✅ Meets threshold: {meets_threshold}")
            
            return {
                'score': final_score,
                'classification': classification,
                'meets_threshold': meets_threshold,
                'breakdown': breakdown,
                'metrics': {
                    'total_volume': total_volume,
                    'avg_transaction': avg_transaction,
                    'tx_count': tx_count,
                    'token_diversity': token_diversity,
                    'unique_counterparties': unique_counterparties,
                    'large_transfer_count': large_transfer_count
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error scoring wallet: {e}", exc_info=True)
            
            return {
                'score': 0,
                'classification': {'classification': 'error', 'tags': ['error']},
                'meets_threshold': False,
                'breakdown': {},
                'error': str(e)
            }
    
    def _classify_wallet(
        self,
        total_volume: float,
        avg_transaction: float,
        tx_count: int,
        token_diversity: int,
        large_transfer_count: int
    ) -> Dict:
        """
        Classify wallet based on volume patterns.
        
        Classifications:
        - mega_whale: $100M+ volume, $1M+ avg
        - whale: $10M+ volume, $500K+ avg
        - high_volume_trader: High frequency + volume
        - institutional: Large avg + moderate frequency
        - active_trader: High frequency, moderate volume
        - moderate_volume: Meets threshold but not exceptional
        """
        tags = []
        
        # Mega Whale
        if total_volume >= 100_000_000 and avg_transaction >= 1_000_000:
            classification = 'mega_whale'
            tags = ['mega_whale', 'ultra_high_volume', 'institutional_grade']
        
        # Whale
        elif total_volume >= 10_000_000 and avg_transaction >= 500_000:
            classification = 'whale'
            tags = ['whale', 'very_high_volume', 'large_transactions']
        
        # High Volume Trader
        elif total_volume >= 5_000_000 and tx_count >= 100:
            classification = 'high_volume_trader'
            tags = ['high_volume_trader', 'active', 'frequent_transactions']
        
        # Institutional
        elif avg_transaction >= 1_000_000 and large_transfer_count >= 5:
            classification = 'institutional'
            tags = ['institutional', 'large_avg_transaction', 'selective_trading']
        
        # Active Trader
        elif tx_count >= 200 and total_volume >= 2_000_000:
            classification = 'active_trader'
            tags = ['active_trader', 'high_frequency', 'moderate_volume']
        
        # Moderate Volume
        elif total_volume >= 1_000_000:
            classification = 'moderate_volume'
            tags = ['moderate_volume', 'meets_threshold']
        
        # Low Volume
        else:
            classification = 'low_volume'
            tags = ['low_volume', 'below_threshold']
        
        # Add token diversity tags
        if token_diversity >= 10:
            tags.append('highly_diversified')
        elif token_diversity >= 5:
            tags.append('diversified')
        elif token_diversity >= 2:
            tags.append('multi_token')
        
        # Add large transfer tags
        if large_transfer_count >= 10:
            tags.append('frequent_large_transfers')
        elif large_transfer_count >= 5:
            tags.append('regular_large_transfers')
        elif large_transfer_count >= 1:
            tags.append('occasional_large_transfers')
        
        return {
            'classification': classification,
            'tags': tags
        }


# Export
__all__ = ['VolumeScorer']
