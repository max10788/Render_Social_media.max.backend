"""
Volume Scorer - Scores wallets based on transaction volume and patterns
========================================================================

✅ FIXED v2.1 - Division by Zero Protection:
- Safe division checks in all balance ratio calculations
- Prevents crashes when total_volume = 0

✅ ENHANCED v2.0 - Balance + Activity Integration:
- Original volume-based scoring
- **NEW**: Balance-aware adjustments
- **NEW**: Activity pattern modifiers
- **NEW**: Enhanced classifications (active vs dormant)
- **NEW**: Risk-based score penalties

✅ VOLUME-FOCUSED SCORING (not OTC-specific):
- Total volume thresholds
- Transaction size patterns
- Token diversity
- Counterparty count
- Activity frequency
- **NEW**: Current balance ratio
- **NEW**: Temporal activity patterns

Version: 2.1 (FIXED)
Date: 2025-01-16
"""

from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class VolumeScorer:
    """
    Scores wallets based on volume and transaction patterns.
    
    ✅ FIXED v2.1: Division by zero protection added.
    ✨ ENHANCED v2.0: Considers current balance and activity patterns for accurate scoring.
    Prevents over-scoring of dormant/depleted wallets.
    """
    
    def __init__(self, min_volume_threshold: float = 1_000_000):
        """
        Initialize VolumeScorer.
        
        Args:
            min_volume_threshold: Minimum USD volume to save (default: $1M)
        """
        self.min_volume_threshold = min_volume_threshold
        
        logger.info(
            f"✅ VolumeScorer v2.0 initialized "
            f"(min threshold: ${min_volume_threshold:,.0f})"
        )
    
    def score_high_volume_wallet(
        self,
        address: str,
        transactions: List[Dict],
        counterparty_data: Dict,
        profile: Dict,
        balance_analysis: Optional[Dict] = None,  # ✨ NEW
        activity_analysis: Optional[Dict] = None,  # ✨ NEW
        combined_scoring: Optional[Dict] = None  # ✨ NEW
    ) -> Dict:
        """
        Score a wallet for high-volume characteristics.
        
        ✅ FIXED v2.1: Protected against division by zero.
        ✨ ENHANCED v2.0: Accepts balance and activity data for intelligent scoring.
        
        Args:
            address: Wallet address
            transactions: List of wallet transactions
            counterparty_data: Data from counterparty extraction
            profile: Wallet profile from analyzer
            balance_analysis: Current balance data (optional)
            activity_analysis: Temporal activity analysis (optional)
            combined_scoring: Pre-calculated combined score (optional)
            
        Returns:
            Dict with:
            - score: Volume score (0-100) with balance/activity modifiers
            - base_score: Original volume score before modifiers
            - classification: Wallet classification (with _active/_dormant suffix)
            - meets_threshold: Whether meets volume threshold
            - breakdown: Scoring breakdown
            - modifiers: Applied score modifiers
        """
        logger.info(f"📊 Scoring high-volume wallet {address[:10]}...")
        
        try:
            # ================================================================
            # STEP 1: CALCULATE BASE VOLUME SCORE
            # ================================================================
            
            base_score_result = self._calculate_base_volume_score(
                profile,
                transactions
            )
            
            base_score = base_score_result['score']
            breakdown = base_score_result['breakdown']
            metrics = base_score_result['metrics']
            
            logger.info(f"   📊 Base volume score: {base_score}/100")
            
            # ================================================================
            # ✨ STEP 2: APPLY BALANCE & ACTIVITY MODIFIERS (NEW!)
            # ================================================================
            
            modifiers = {
                'balance_modifier': 0,
                'activity_modifier': 0,
                'combined_modifier': 0,
                'total_adjustment': 0
            }
            
            final_score = base_score
            
            # If we have combined scoring, use it directly
            if combined_scoring:
                logger.info(f"   🎯 Using pre-calculated combined score")
                
                # Combined score already factors in balance + activity
                final_score = combined_scoring['combined_score']
                
                modifiers['combined_modifier'] = final_score - base_score
                modifiers['total_adjustment'] = modifiers['combined_modifier']
                
                logger.info(
                    f"   ✅ Combined score: {final_score:.1f}/100 "
                    f"(adjustment: {modifiers['combined_modifier']:+.1f})"
                )
            
            else:
                # Apply individual modifiers
                if balance_analysis:
                    balance_mod = self._calculate_balance_modifier(
                        balance_analysis,
                        metrics['total_volume']
                    )
                    modifiers['balance_modifier'] = balance_mod
                    final_score += balance_mod
                    
                    logger.info(f"   💰 Balance modifier: {balance_mod:+.1f}")
                
                if activity_analysis:
                    activity_mod = self._calculate_activity_modifier(
                        activity_analysis
                    )
                    modifiers['activity_modifier'] = activity_mod
                    final_score += activity_mod
                    
                    logger.info(f"   📅 Activity modifier: {activity_mod:+.1f}")
                
                modifiers['total_adjustment'] = (
                    modifiers['balance_modifier'] + 
                    modifiers['activity_modifier']
                )
            
            # Ensure score is within bounds
            final_score = max(0, min(100, final_score))
            
            # ================================================================
            # ✨ STEP 3: ENHANCED CLASSIFICATION (NEW!)
            # ================================================================
            
            classification = self._classify_wallet_enhanced(
                metrics=metrics,
                balance_analysis=balance_analysis,
                activity_analysis=activity_analysis,
                combined_scoring=combined_scoring
            )
            
            # ================================================================
            # STEP 4: THRESHOLD CHECK
            # ================================================================
            
            meets_threshold = metrics['total_volume'] >= self.min_volume_threshold
            
            # ================================================================
            # LOG RESULTS (WITH SAFE DIVISION)
            # ================================================================
            
            logger.info(f"   📊 Final Score: {final_score:.1f}/100 (base: {base_score}/100)")
            logger.info(f"   💰 Total Volume: ${metrics['total_volume']:,.0f}")
            logger.info(f"   📈 Avg Transaction: ${metrics['avg_transaction']:,.0f}")
            logger.info(f"   🔢 TX Count: {metrics['tx_count']}")
            logger.info(f"   🏷️  Classification: {classification['classification']}")
            logger.info(f"   ✅ Meets threshold: {meets_threshold}")
            
            # ✅ SAFE DIVISION FIX: Check if volume > 0 before calculating ratio
            if balance_analysis:
                balance_ratio = (
                    balance_analysis['total_balance_usd'] / metrics['total_volume'] 
                    if metrics['total_volume'] > 0 else 0
                )
                logger.info(
                    f"   💵 Balance: ${balance_analysis['total_balance_usd']:,.2f} "
                    f"(ratio: {balance_ratio:.2%})"
                )
            
            if activity_analysis:
                pattern = activity_analysis['pattern']['pattern']
                logger.info(f"   📅 Activity: {pattern} pattern")
            
            # ================================================================
            # RETURN ENHANCED RESULT
            # ================================================================
            
            return {
                'score': round(final_score, 1),
                'base_score': base_score,
                'classification': classification,
                'meets_threshold': meets_threshold,
                'breakdown': breakdown,
                'modifiers': modifiers,
                'metrics': metrics,
                # ✨ NEW: Include analysis references
                'has_balance_data': balance_analysis is not None,
                'has_activity_data': activity_analysis is not None,
                'scoring_method': 'combined' if combined_scoring else 'volume_with_modifiers'
            }
            
        except Exception as e:
            logger.error(f"❌ Error scoring wallet: {e}", exc_info=True)
            
            return {
                'score': 0,
                'base_score': 0,
                'classification': {'classification': 'error', 'tags': ['error']},
                'meets_threshold': False,
                'breakdown': {},
                'modifiers': {},
                'error': str(e)
            }
    
    # ========================================================================
    # ✨ NEW METHODS
    # ========================================================================
    
    def _calculate_base_volume_score(
        self,
        profile: Dict,
        transactions: List[Dict]
    ) -> Dict:
        """
        Calculate base volume score (original logic).
        
        Extracted for cleaner structure.
        """
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
        # SCORING CRITERIA (Original)
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
        
        # Calculate total
        final_score = sum(breakdown.values())
        
        return {
            'score': final_score,
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
    
    def _calculate_balance_modifier(
        self,
        balance_analysis: Dict,
        historical_volume: float
    ) -> float:
        """
        Calculate score modifier based on current balance.
        
        ✨ NEW METHOD:
        Adjusts score based on balance status.
        
        Returns:
            Modifier value (-50 to +20)
        """
        current_balance = balance_analysis.get('total_balance_usd', 0)
        
        # ✅ SAFE: Already protected
        if historical_volume <= 0:
            return 0
        
        balance_ratio = current_balance / historical_volume
        
        # ====================================================================
        # BALANCE STATUS PENALTIES/BONUSES
        # ====================================================================
        
        # Depleted (< 1%)
        if balance_ratio < 0.01:
            modifier = -50  # Major penalty
            status = 'depleted'
        
        # Minimal (1-10%)
        elif balance_ratio < 0.10:
            modifier = -30  # Significant penalty
            status = 'minimal'
        
        # Active (10-50%)
        elif balance_ratio < 0.50:
            if current_balance >= 10_000:
                modifier = 0  # Neutral
                status = 'active'
            else:
                modifier = -10  # Small penalty
                status = 'active_low'
        
        # Growing (50-100%)
        elif balance_ratio < 1.00:
            modifier = +10  # Small bonus
            status = 'growing'
        
        # Accumulating (> 100%)
        else:
            modifier = +20  # Bonus
            status = 'accumulating'
        
        logger.debug(
            f"   💰 Balance status: {status} "
            f"(ratio: {balance_ratio:.2%}, modifier: {modifier:+.1f})"
        )
        
        return modifier
    
    def _calculate_activity_modifier(
        self,
        activity_analysis: Dict
    ) -> float:
        """
        Calculate score modifier based on activity pattern.
        
        ✨ NEW METHOD:
        Adjusts score based on temporal activity.
        
        Returns:
            Modifier value (-40 to +10)
        """
        pattern = activity_analysis.get('pattern', {})
        pattern_type = pattern.get('pattern', 'unknown')
        activity_score = activity_analysis.get('activity_score', 0)
        
        # ====================================================================
        # ACTIVITY PATTERN PENALTIES/BONUSES
        # ====================================================================
        
        # Dormant
        if pattern_type == 'dormant':
            modifier = -40  # Major penalty
        
        # Declining
        elif pattern_type == 'declining':
            modifier = -20  # Significant penalty
        
        # Sporadic
        elif pattern_type == 'sporadic':
            modifier = -10  # Small penalty
        
        # Burst (short-term)
        elif pattern_type == 'burst':
            modifier = -5  # Slight penalty (transient)
        
        # Sustained (long-term)
        elif pattern_type == 'sustained':
            modifier = +10  # Bonus
        
        # Active
        elif pattern_type in ['active', 'early_stage']:
            modifier = +5  # Small bonus
        
        # Unknown
        else:
            modifier = 0  # Neutral
        
        # ====================================================================
        # ADJUST BASED ON ACTIVITY SCORE
        # ====================================================================
        
        # Further reduce modifier if activity score is very low
        if activity_score < 20 and modifier > -30:
            modifier -= 10
        
        logger.debug(
            f"   📅 Activity pattern: {pattern_type} "
            f"(score: {activity_score:.1f}, modifier: {modifier:+.1f})"
        )
        
        return modifier
    
    def _classify_wallet_enhanced(
        self,
        metrics: Dict,
        balance_analysis: Optional[Dict] = None,
        activity_analysis: Optional[Dict] = None,
        combined_scoring: Optional[Dict] = None
    ) -> Dict:
        """
        Enhanced classification with balance and activity.
        
        ✨ NEW METHOD:
        Extends base classification with _active/_dormant suffixes.
        """
        total_volume = metrics['total_volume']
        avg_transaction = metrics['avg_transaction']
        tx_count = metrics['tx_count']
        token_diversity = metrics['token_diversity']
        large_transfer_count = metrics['large_transfer_count']
        
        # ====================================================================
        # If we have combined scoring, use its classification
        # ====================================================================
        
        if combined_scoring:
            classification = combined_scoring.get('final_classification', 'unknown')
            tags = ['combined_scored']
            
            # Add volume-based tags
            tags.extend(self._get_volume_tags(
                total_volume, avg_transaction, tx_count, 
                token_diversity, large_transfer_count
            ))
            
            return {
                'classification': classification,
                'tags': tags,
                'method': 'combined_scoring'
            }
        
        # ====================================================================
        # Base classification (original logic)
        # ====================================================================
        
        base_classification = self._classify_wallet_base(
            total_volume,
            avg_transaction,
            tx_count,
            token_diversity,
            large_transfer_count
        )
        
        classification = base_classification['classification']
        tags = base_classification['tags']
        
        # ====================================================================
        # ✨ APPLY ACTIVITY SUFFIX (NEW!)
        # ====================================================================
        
        if activity_analysis:
            pattern = activity_analysis.get('pattern', {}).get('pattern', 'unknown')
            
            # Add suffix for dormant/active status
            if pattern == 'dormant':
                classification += '_dormant'
                tags.append('dormant')
            elif pattern in ['sustained', 'active']:
                classification += '_active'
                tags.append('active')
            elif pattern == 'declining':
                tags.append('declining')
            elif pattern == 'burst':
                tags.append('burst')
        
        # ====================================================================
        # ✨ ADD BALANCE TAGS (NEW!)
        # ====================================================================
        
        if balance_analysis:
            current_balance = balance_analysis.get('total_balance_usd', 0)
            # ✅ SAFE: Already protected with ternary operator
            balance_ratio = current_balance / total_volume if total_volume > 0 else 0
            
            if balance_ratio < 0.01:
                tags.append('depleted')
            elif balance_ratio < 0.10:
                tags.append('low_balance')
            elif balance_ratio >= 1.0:
                tags.append('accumulating')
        
        return {
            'classification': classification,
            'tags': tags,
            'method': 'volume_with_modifiers'
        }
    
    def _classify_wallet_base(
        self,
        total_volume: float,
        avg_transaction: float,
        tx_count: int,
        token_diversity: int,
        large_transfer_count: int
    ) -> Dict:
        """
        Base classification (original logic).
        
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
            tags = ['high_volume_trader', 'frequent_transactions']
        
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
        
        # Add diversity tags
        tags.extend(self._get_diversity_tags(token_diversity, large_transfer_count))
        
        return {
            'classification': classification,
            'tags': tags
        }
    
    def _get_volume_tags(
        self,
        total_volume: float,
        avg_transaction: float,
        tx_count: int,
        token_diversity: int,
        large_transfer_count: int
    ) -> List[str]:
        """Get volume-based tags."""
        tags = []
        
        # Volume tags
        if total_volume >= 100_000_000:
            tags.append('ultra_high_volume')
        elif total_volume >= 10_000_000:
            tags.append('very_high_volume')
        elif total_volume >= 1_000_000:
            tags.append('high_volume')
        
        # Transaction size tags
        if avg_transaction >= 1_000_000:
            tags.append('large_avg_transaction')
        
        # Frequency tags
        if tx_count >= 200:
            tags.append('high_frequency')
        
        # Diversity tags
        tags.extend(self._get_diversity_tags(token_diversity, large_transfer_count))
        
        return tags
    
    def _get_diversity_tags(
        self,
        token_diversity: int,
        large_transfer_count: int
    ) -> List[str]:
        """Get diversity-related tags."""
        tags = []
        
        # Token diversity tags
        if token_diversity >= 10:
            tags.append('highly_diversified')
        elif token_diversity >= 5:
            tags.append('diversified')
        elif token_diversity >= 2:
            tags.append('multi_token')
        
        # Large transfer tags
        if large_transfer_count >= 10:
            tags.append('frequent_large_transfers')
        elif large_transfer_count >= 5:
            tags.append('regular_large_transfers')
        elif large_transfer_count >= 1:
            tags.append('occasional_large_transfers')
        
        return tags


# Export
__all__ = ['VolumeScorer']
