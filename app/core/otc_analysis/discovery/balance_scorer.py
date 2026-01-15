"""
Balance Scorer - Combined Balance & Activity Scoring
====================================================

Scores wallets based on:
1. Current balance vs. historical volume
2. Activity patterns (from ActivityAnalyzer)
3. Balance status (depleted, active, growing)

Prevents misclassification of:
- Dormant wallets with empty balances
- Transient traders who moved on
- Historical whales now inactive

Version: 1.0
Date: 2025-01-15
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class BalanceScorer:
    """
    Scores wallets based on current balance and activity patterns.
    
    Key Concepts:
    - Balance Ratio: current_balance / historical_volume
    - Balance Status: depleted | minimal | active | growing | accumulating
    - Combined Score: balance + activity + historical volume
    """
    
    def __init__(
        self,
        min_active_balance_usd: float = 10_000,
        depletion_threshold: float = 0.01  # 1% of historical
    ):
        """
        Initialize Balance Scorer.
        
        Args:
            min_active_balance_usd: Minimum balance to consider "active" (default: $10k)
            depletion_threshold: Ratio below which wallet is "depleted" (default: 0.01)
        """
        self.min_active_balance = min_active_balance_usd
        self.depletion_threshold = depletion_threshold
        
        logger.info(
            f"✅ BalanceScorer initialized "
            f"(min_balance: ${min_active_balance_usd:,.0f}, "
            f"depletion: {depletion_threshold:.1%})"
        )
    
    
    def calculate_balance_ratio(
        self,
        current_balance_usd: float,
        historical_volume_usd: float
    ) -> float:
        """
        Calculate ratio of current balance to historical volume.
        
        Ratio interpretations:
        - 0.00 - 0.01: Depleted (< 1% remains)
        - 0.01 - 0.10: Minimal (1-10% remains)
        - 0.10 - 0.50: Active (10-50% remains)
        - 0.50 - 1.00: Growing (50-100% remains)
        - > 1.00: Accumulating (more than historical peak)
        
        Args:
            current_balance_usd: Current balance in USD
            historical_volume_usd: Historical transaction volume in USD
            
        Returns:
            Ratio as float (0.0 to infinity)
        """
        if historical_volume_usd <= 0:
            # No historical data
            if current_balance_usd > 0:
                return 1.0  # Has balance but no history
            return 0.0
        
        ratio = current_balance_usd / historical_volume_usd
        
        logger.debug(
            f"💰 Balance ratio: {ratio:.4f} "
            f"(${current_balance_usd:,.0f} / ${historical_volume_usd:,.0f})"
        )
        
        return ratio
    
    
    def classify_balance_status(
        self,
        current_balance_usd: float,
        historical_volume_usd: float,
        balance_ratio: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Classify wallet balance status.
        
        ✅ FIXED: Reduced penalties to prevent destroying scores
        
        Status types:
        - depleted: < 1% of historical volume remains
        - minimal: 1-10% remains, below active threshold
        - active: 10-50% remains, above active threshold
        - growing: 50-100% remains
        - accumulating: > 100% (more than historical peak)
        - unknown: No data available
        
        Args:
            current_balance_usd: Current balance in USD
            historical_volume_usd: Historical volume in USD
            balance_ratio: Pre-calculated ratio (optional)
            
        Returns:
            {
                'status': str,
                'confidence': float,
                'description': str,
                'risk_level': str,
                'balance_ratio': float,
                'tags': List[str]
            }
        """
        if balance_ratio is None:
            balance_ratio = self.calculate_balance_ratio(
                current_balance_usd,
                historical_volume_usd
            )
        
        # ====================================================================
        # ✅ FIX: REDUCED PENALTIES (was -50, now -30 max)
        # ====================================================================
        
        # STATUS 1: Depleted (< 1%)
        if balance_ratio < self.depletion_threshold:
            # ✅ FIX: -30 statt -50
            return {
                'status': 'depleted',
                'confidence': 0.95,
                'description': f'Wallet depleted: only {balance_ratio:.2%} of historical volume remains',
                'risk_level': 'high',
                'balance_ratio': balance_ratio,
                'tags': ['empty', 'depleted', 'high_risk'],
                'score_penalty': -30  # ✅ WAS: -50
            }
        
        # STATUS 2: Minimal (1-10%)
        if balance_ratio < 0.10:
            risk = 'high' if current_balance_usd < self.min_active_balance else 'medium'
            # ✅ FIX: -15 statt -30
            return {
                'status': 'minimal',
                'confidence': 0.9,
                'description': f'Low balance: {balance_ratio:.2%} of historical volume',
                'risk_level': risk,
                'balance_ratio': balance_ratio,
                'tags': ['low_balance', 'potentially_inactive'],
                'score_penalty': -15  # ✅ WAS: -30
            }
        
        # STATUS 3: Active (10-50%)
        if balance_ratio < 0.50:
            if current_balance_usd >= self.min_active_balance:
                return {
                    'status': 'active',
                    'confidence': 0.85,
                    'description': f'Active balance: {balance_ratio:.2%} of historical volume',
                    'risk_level': 'low',
                    'balance_ratio': balance_ratio,
                    'tags': ['active', 'operational'],
                    'score_penalty': 0  # ✅ No penalty for active
                }
            else:
                # ✅ FIX: -5 statt -10
                return {
                    'status': 'active_low',
                    'confidence': 0.75,
                    'description': f'Active but low: {balance_ratio:.2%}, ${current_balance_usd:,.0f}',
                    'risk_level': 'medium',
                    'balance_ratio': balance_ratio,
                    'tags': ['active', 'low_funds'],
                    'score_penalty': -5  # ✅ WAS: -10
                }
        
        # STATUS 4: Growing (50-100%)
        if balance_ratio < 1.00:
            return {
                'status': 'growing',
                'confidence': 0.9,
                'description': f'Growing balance: {balance_ratio:.2%} of historical volume',
                'risk_level': 'low',
                'balance_ratio': balance_ratio,
                'tags': ['growing', 'healthy', 'active'],
                'score_penalty': 0,
                'score_bonus': 10
            }
        
        # STATUS 5: Accumulating (> 100%)
        return {
            'status': 'accumulating',
            'confidence': 0.95,
            'description': f'Accumulating: {balance_ratio:.2%} (exceeds historical)',
            'risk_level': 'low',
            'balance_ratio': balance_ratio,
            'tags': ['accumulating', 'whale', 'growing'],
            'score_penalty': 0,
            'score_bonus': 20
        }
    
    
    def score_balance_health(
        self,
        balance_status: Dict[str, Any],
        current_balance_usd: float
    ) -> float:
        """
        Calculate balance health score (0-100).
        
        ✅ FIXED: Softer absolute balance penalties
        
        Factors:
        - Balance status (depleted to accumulating)
        - Absolute balance amount
        - Balance ratio
        
        Args:
            balance_status: Classification from classify_balance_status()
            current_balance_usd: Current balance amount
            
        Returns:
            Score 0-100
        """
        base_score = 50.0  # Start neutral
        
        # Apply status penalty/bonus
        penalty = balance_status.get('score_penalty', 0)
        bonus = balance_status.get('score_bonus', 0)
        
        base_score += penalty + bonus
        
        # ====================================================================
        # ✅ FIX: SOFTER ABSOLUTE BALANCE PENALTIES
        # ====================================================================
        
        if current_balance_usd >= 10_000_000:  # $10M+
            amount_score = 20
        elif current_balance_usd >= 1_000_000:  # $1M+
            amount_score = 15
        elif current_balance_usd >= 100_000:  # $100K+
            amount_score = 10
        elif current_balance_usd >= 10_000:  # $10K+
            amount_score = 5
        elif current_balance_usd >= 1_000:  # $1K+
            amount_score = 0
        elif current_balance_usd >= 100:  # $100+
            amount_score = -5  # ✅ WAS: -10 (softer)
        else:  # < $100
            amount_score = -10  # ✅ WAS: -20 (softer)
        
        base_score += amount_score
        
        # ====================================================================
        # ✅ FIX: SOFTER RATIO PENALTIES
        # ====================================================================
        
        balance_ratio = balance_status.get('balance_ratio', 0)
        
        if balance_ratio >= 1.0:
            # Accumulating - bonus
            ratio_score = 10
        elif balance_ratio >= 0.5:
            # Growing - small bonus
            ratio_score = 5
        elif balance_ratio >= 0.1:
            # Active - neutral
            ratio_score = 0
        elif balance_ratio >= 0.01:
            # Minimal - small penalty
            ratio_score = -10  # ✅ WAS: -15 (softer)
        else:
            # Depleted - penalty
            ratio_score = -15  # ✅ WAS: -25 (softer)
        
        base_score += ratio_score
        
        # Ensure within bounds
        final_score = max(0.0, min(100.0, base_score))
        
        logger.debug(
            f"💯 Balance health: {final_score:.1f}/100 "
            f"(base: {50 + penalty + bonus}, amount: {amount_score}, ratio: {ratio_score})"
        )
        
        return final_score
    
    def combine_balance_and_activity(
        self,
        balance_health_score: float,
        activity_score: float,
        activity_pattern: Dict[str, Any],
        balance_status: Dict[str, Any],
        historical_volume_usd: float
    ) -> Dict[str, Any]:
        """
        Combine balance and activity scores with intelligent weighting.
        
        Weighting Logic:
        - Depleted + Dormant: Major penalties, balance dominant
        - Active + High Balance: Equal weighting
        - Historical Whale + Depleted: Warn about status change
        
        Args:
            balance_health_score: Score from score_balance_health()
            activity_score: Score from ActivityAnalyzer
            activity_pattern: Pattern from ActivityAnalyzer
            balance_status: Status from classify_balance_status()
            historical_volume_usd: Historical volume
            
        Returns:
            {
                'combined_score': float,
                'balance_weight': float,
                'activity_weight': float,
                'final_classification': str,
                'risk_assessment': Dict,
                'recommendations': List[str]
            }
        """
        pattern_type = activity_pattern.get('pattern', 'unknown')
        status_type = balance_status.get('status', 'unknown')
        
        # ====================================================================
        # DETERMINE WEIGHTING STRATEGY
        # ====================================================================
        
        # Default: Equal weighting
        balance_weight = 0.5
        activity_weight = 0.5
        
        # Case 1: Depleted + Dormant = BAD (balance dominant)
        if status_type in ['depleted', 'minimal'] and pattern_type == 'dormant':
            balance_weight = 0.7
            activity_weight = 0.3
            logger.info("⚠️ Depleted + Dormant: Balance-weighted scoring")
        
        # Case 2: Active + Good Balance = GOOD (equal)
        elif status_type in ['active', 'growing', 'accumulating'] and pattern_type in ['sustained', 'active']:
            balance_weight = 0.5
            activity_weight = 0.5
            logger.info("✅ Active + Good Balance: Equal weighting")
        
        # Case 3: High Historical + Depleted = WARNING (balanced but flagged)
        elif historical_volume_usd > 10_000_000 and status_type == 'depleted':
            balance_weight = 0.6
            activity_weight = 0.4
            logger.warning("⚠️ Historical Whale Depleted: Balance-weighted")
        
        # Case 4: Burst Activity + Low Balance = TRANSIENT (activity dominant)
        elif pattern_type == 'burst' and status_type in ['minimal', 'depleted']:
            balance_weight = 0.4
            activity_weight = 0.6
            logger.info("⚡ Burst + Low Balance: Activity-weighted")
        
        # Case 5: Sustained + Minimal Balance = ACTIVE TRADER (activity important)
        elif pattern_type == 'sustained' and status_type == 'minimal':
            balance_weight = 0.45
            activity_weight = 0.55
            logger.info("🔄 Sustained + Minimal: Slight activity bias")
        
        # ====================================================================
        # CALCULATE COMBINED SCORE
        # ====================================================================
        
        combined_score = (
            (balance_health_score * balance_weight) +
            (activity_score * activity_weight)
        )
        
        # ====================================================================
        # DETERMINE FINAL CLASSIFICATION
        # ====================================================================
        
        final_classification = self._determine_classification(
            combined_score,
            balance_status,
            activity_pattern,
            historical_volume_usd
        )
        
        # ====================================================================
        # RISK ASSESSMENT
        # ====================================================================
        
        risk_assessment = self._assess_risk(
            balance_status,
            activity_pattern,
            combined_score,
            historical_volume_usd
        )
        
        # ====================================================================
        # GENERATE RECOMMENDATIONS
        # ====================================================================
        
        recommendations = self._generate_combined_recommendations(
            balance_status,
            activity_pattern,
            combined_score,
            historical_volume_usd
        )
        
        result = {
            'combined_score': round(combined_score, 1),
            'balance_weight': balance_weight,
            'activity_weight': activity_weight,
            'balance_health_score': round(balance_health_score, 1),
            'activity_score': round(activity_score, 1),
            'final_classification': final_classification,
            'risk_assessment': risk_assessment,
            'recommendations': recommendations
        }
        
        logger.info(
            f"🎯 Combined Score: {combined_score:.1f}/100 "
            f"(balance: {balance_health_score:.1f}, activity: {activity_score:.1f})"
        )
        logger.info(f"   Classification: {final_classification}")
        
        return result
    
    
    def _determine_classification(
        self,
        combined_score: float,
        balance_status: Dict[str, Any],
        activity_pattern: Dict[str, Any],
        historical_volume_usd: float
    ) -> str:
        """
        Determine final wallet classification.
        
        Classifications:
        - mega_whale_active: $100M+ history, active, good balance
        - whale_active: $10M+ history, active
        - whale_dormant: $10M+ history, dormant/depleted
        - high_volume_active: $1M+ history, active
        - high_volume_dormant: $1M+ history, dormant
        - transient_trader: Burst activity, now depleted
        - active_trader: Sustained activity, minimal balance
        - inactive: Low score, dormant
        """
        status_type = balance_status.get('status', 'unknown')
        pattern_type = activity_pattern.get('pattern', 'unknown')
        
        # Mega Whale Active
        if historical_volume_usd >= 100_000_000:
            if status_type in ['active', 'growing', 'accumulating'] and pattern_type in ['sustained', 'active']:
                return 'mega_whale_active'
            elif status_type == 'depleted' or pattern_type == 'dormant':
                return 'mega_whale_dormant'
            else:
                return 'mega_whale'
        
        # Whale
        if historical_volume_usd >= 10_000_000:
            if status_type in ['active', 'growing', 'accumulating'] and pattern_type in ['sustained', 'active']:
                return 'whale_active'
            elif status_type == 'depleted' or pattern_type == 'dormant':
                return 'whale_dormant'
            else:
                return 'whale'
        
        # High Volume
        if historical_volume_usd >= 1_000_000:
            if pattern_type == 'dormant' or status_type == 'depleted':
                return 'high_volume_dormant'
            elif pattern_type in ['sustained', 'active']:
                return 'high_volume_active'
            elif pattern_type == 'burst':
                return 'transient_trader'
            else:
                return 'high_volume_trader'
        
        # Medium Volume
        if historical_volume_usd >= 100_000:
            if pattern_type in ['sustained', 'active']:
                return 'active_trader'
            elif pattern_type == 'burst' and status_type == 'depleted':
                return 'transient_trader'
            else:
                return 'moderate_volume'
        
        # Low Activity
        if combined_score < 30:
            return 'inactive'
        
        return 'moderate_volume'
    
    
    def _assess_risk(
        self,
        balance_status: Dict[str, Any],
        activity_pattern: Dict[str, Any],
        combined_score: float,
        historical_volume_usd: float
    ) -> Dict[str, Any]:
        """Assess classification risk level."""
        risk_level = 'low'
        risk_factors = []
        
        status_type = balance_status.get('status', 'unknown')
        pattern_type = activity_pattern.get('pattern', 'unknown')
        
        # Check depleted status
        if status_type == 'depleted':
            risk_level = 'high'
            risk_factors.append('Wallet is depleted')
        
        # Check dormancy
        if pattern_type == 'dormant':
            risk_level = 'high' if risk_level != 'high' else 'high'
            risk_factors.append('No recent activity')
        
        # Check score
        if combined_score < 30:
            risk_level = 'high'
            risk_factors.append('Low combined score')
        elif combined_score < 50:
            risk_level = 'medium' if risk_level == 'low' else risk_level
            risk_factors.append('Moderate score')
        
        # Check historical vs current mismatch
        balance_ratio = balance_status.get('balance_ratio', 0)
        if historical_volume_usd > 10_000_000 and balance_ratio < 0.01:
            risk_level = 'high'
            risk_factors.append('Historical whale now empty')
        
        return {
            'level': risk_level,
            'factors': risk_factors,
            'confidence': balance_status.get('confidence', 0.5)
        }
    
    
    def _generate_combined_recommendations(
        self,
        balance_status: Dict[str, Any],
        activity_pattern: Dict[str, Any],
        combined_score: float,
        historical_volume_usd: float
    ) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        status_type = balance_status.get('status', 'unknown')
        pattern_type = activity_pattern.get('pattern', 'unknown')
        balance_ratio = balance_status.get('balance_ratio', 0)
        
        # Critical issues
        if status_type == 'depleted' and pattern_type == 'dormant':
            recommendations.append("❌ CRITICAL: Wallet is depleted and dormant")
            recommendations.append("⚠️ Do not classify as active/high-volume")
            recommendations.append("🔍 Consider removing from active tracking")
        
        # Historical whale issues
        if historical_volume_usd > 10_000_000 and balance_ratio < 0.05:
            recommendations.append("⚠️ Historical whale with depleted balance")
            recommendations.append("📊 Use 'dormant_whale' classification")
            recommendations.append("💡 Adjust confidence score downward (-30%)")
        
        # Transient trader
        if pattern_type == 'burst' and status_type in ['depleted', 'minimal']:
            recommendations.append("⚡ Transient trader pattern detected")
            recommendations.append("⏳ Short-term activity, now inactive")
            recommendations.append("📉 Lower classification confidence")
        
        # Active good status
        if status_type in ['active', 'growing', 'accumulating'] and pattern_type in ['sustained', 'active']:
            recommendations.append("✅ Strong classification candidate")
            recommendations.append("💰 Good balance + active pattern")
            recommendations.append("🎯 Safe for high-confidence classification")
        
        # Score-based
        if combined_score < 40:
            recommendations.append(f"⚠️ Low combined score ({combined_score:.1f}/100)")
            recommendations.append("🔽 Use minimum confidence threshold")
        elif combined_score >= 70:
            recommendations.append(f"✅ High combined score ({combined_score:.1f}/100)")
            recommendations.append("🔼 Safe for high-confidence classification")
        
        return recommendations


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_balance_scorer(
    min_active_balance_usd: float = 10_000
) -> BalanceScorer:
    """
    Create and initialize a BalanceScorer.
    
    Args:
        min_active_balance_usd: Minimum balance for "active" status
        
    Returns:
        Initialized BalanceScorer
    """
    return BalanceScorer(min_active_balance_usd=min_active_balance_usd)


def quick_balance_check(
    current_balance_usd: float,
    historical_volume_usd: float
) -> str:
    """
    Quick balance status check.
    
    Returns status string: depleted | minimal | active | growing | accumulating
    """
    scorer = BalanceScorer()
    status = scorer.classify_balance_status(
        current_balance_usd,
        historical_volume_usd
    )
    return status['status']


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    'BalanceScorer',
    'create_balance_scorer',
    'quick_balance_check'
]
