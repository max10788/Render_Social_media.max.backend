"""
Activity Analyzer - Temporal Pattern Analysis
==============================================

Analyzes wallet activity patterns over time to detect:
- Active vs. Dormant wallets
- Activity duration (burst vs. sustained)
- Temporal anomalies
- Lifecycle stages

Features:
- Activity window calculation
- Dormancy detection
- Pattern classification (burst, sustained, sporadic)
- Lifecycle analysis (early, active, declining, dormant)

Version: 1.0
Date: 2025-01-15
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)


class ActivityAnalyzer:
    """
    Analyzes temporal activity patterns of wallets.
    
    Key Metrics:
    - first_seen: First transaction timestamp
    - last_seen: Last transaction timestamp
    - activity_window: Duration between first and last activity
    - dormant_days: Days since last activity
    - is_dormant: Boolean flag (> 90 days inactive)
    - activity_pattern: burst | sustained | sporadic | dormant
    """
    
    def __init__(
        self,
        dormancy_threshold_days: int = 90,
        burst_window_days: int = 30,
        sustained_window_days: int = 180
    ):
        """
        Initialize Activity Analyzer.
        
        Args:
            dormancy_threshold_days: Days of inactivity to mark as dormant (default: 90)
            burst_window_days: Max days for "burst" classification (default: 30)
            sustained_window_days: Min days for "sustained" classification (default: 180)
        """
        self.dormancy_threshold = dormancy_threshold_days
        self.burst_window = burst_window_days
        self.sustained_window = sustained_window_days
        
        logger.info(
            f"✅ ActivityAnalyzer initialized "
            f"(dormancy: {dormancy_threshold_days}d, "
            f"burst: {burst_window_days}d, "
            f"sustained: {sustained_window_days}d)"
        )
    
    
    def analyze_activity_window(
        self,
        transactions: List[Dict],
        current_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Analyze activity window from transaction history.
        
        Args:
            transactions: List of transactions with 'timestamp' field
            current_time: Reference time (default: now)
            
        Returns:
            {
                'first_seen': datetime,
                'last_seen': datetime,
                'activity_window_days': int,
                'dormant_days': int,
                'is_dormant': bool,
                'total_transactions': int,
                'transactions_per_day': float,
                'activity_density': float  # transactions per day of activity
            }
        """
        if not transactions:
            logger.warning("⚠️ No transactions provided for activity analysis")
            return self._empty_activity_metrics()
        
        current_time = current_time or datetime.now()
        
        # Extract timestamps
        timestamps = []
        for tx in transactions:
            ts = tx.get('timestamp')
            
            # Parse timestamp if string
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except:
                    continue
            elif isinstance(ts, int):
                ts = datetime.fromtimestamp(ts)
            
            if isinstance(ts, datetime):
                timestamps.append(ts)
        
        if not timestamps:
            logger.warning("⚠️ No valid timestamps found in transactions")
            return self._empty_activity_metrics()
        
        # Sort timestamps
        timestamps.sort()
        
        first_seen = timestamps[0]
        last_seen = timestamps[-1]
        
        # Calculate activity window
        activity_window = (last_seen - first_seen).days
        
        # Calculate dormancy
        dormant_days = (current_time - last_seen).days
        is_dormant = dormant_days >= self.dormancy_threshold
        
        # Calculate activity density
        total_txs = len(transactions)
        
        if activity_window > 0:
            txs_per_day = total_txs / activity_window
            activity_density = total_txs / activity_window
        else:
            # All transactions on same day
            txs_per_day = total_txs
            activity_density = total_txs
        
        metrics = {
            'first_seen': first_seen,
            'last_seen': last_seen,
            'activity_window_days': activity_window,
            'dormant_days': dormant_days,
            'is_dormant': is_dormant,
            'total_transactions': total_txs,
            'transactions_per_day': round(txs_per_day, 2),
            'activity_density': round(activity_density, 2)
        }
        
        logger.debug(
            f"📊 Activity: {activity_window}d window, "
            f"{dormant_days}d dormant, "
            f"{txs_per_day:.1f} tx/day"
        )
        
        return metrics
    
    
    def calculate_activity_score(
        self,
        metrics: Dict[str, Any]
    ) -> float:
        """
        Calculate activity score (0-100) based on metrics.
        
        Higher score = More recent and sustained activity
        
        Scoring factors:
        - Recent activity (50 points): More recent = higher score
        - Activity duration (30 points): Longer sustained = higher score
        - Transaction frequency (20 points): More frequent = higher score
        
        Args:
            metrics: Activity metrics from analyze_activity_window()
            
        Returns:
            Score between 0-100
        """
        if not metrics or metrics.get('total_transactions', 0) == 0:
            return 0.0
        
        score = 0.0
        
        # ====================================================================
        # FACTOR 1: Recency Score (50 points)
        # ====================================================================
        dormant_days = metrics.get('dormant_days', 999)
        
        if dormant_days <= 7:
            recency_score = 50.0  # Active within last week
        elif dormant_days <= 30:
            recency_score = 40.0  # Active within last month
        elif dormant_days <= 90:
            recency_score = 25.0  # Active within 3 months
        elif dormant_days <= 180:
            recency_score = 10.0  # Active within 6 months
        else:
            recency_score = 0.0   # Dormant > 6 months
        
        score += recency_score
        
        # ====================================================================
        # FACTOR 2: Duration Score (30 points)
        # ====================================================================
        activity_window = metrics.get('activity_window_days', 0)
        
        if activity_window >= 365:
            duration_score = 30.0  # Active > 1 year
        elif activity_window >= 180:
            duration_score = 25.0  # Active 6-12 months
        elif activity_window >= 90:
            duration_score = 20.0  # Active 3-6 months
        elif activity_window >= 30:
            duration_score = 15.0  # Active 1-3 months
        elif activity_window > 0:
            duration_score = 10.0  # Active < 1 month
        else:
            duration_score = 5.0   # All TXs on same day
        
        score += duration_score
        
        # ====================================================================
        # FACTOR 3: Frequency Score (20 points)
        # ====================================================================
        txs_per_day = metrics.get('transactions_per_day', 0)
        
        if txs_per_day >= 5:
            frequency_score = 20.0  # Very high frequency
        elif txs_per_day >= 2:
            frequency_score = 16.0  # High frequency
        elif txs_per_day >= 1:
            frequency_score = 12.0  # Daily activity
        elif txs_per_day >= 0.5:
            frequency_score = 8.0   # Every 2 days
        elif txs_per_day >= 0.1:
            frequency_score = 4.0   # Weekly activity
        else:
            frequency_score = 1.0   # Sporadic
        
        score += frequency_score
        
        # Ensure score is within bounds
        score = max(0.0, min(100.0, score))
        
        logger.debug(
            f"📊 Activity Score: {score:.1f}/100 "
            f"(recency: {recency_score}, duration: {duration_score}, freq: {frequency_score})"
        )
        
        return score
    
    
    def classify_activity_pattern(
        self,
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Classify wallet activity pattern.
        
        Patterns:
        - burst: Short, intense activity period (< 30 days)
        - sustained: Long-term consistent activity (> 180 days)
        - sporadic: Irregular activity with gaps
        - declining: Was active, now slowing down
        - dormant: No activity for > 90 days
        
        Args:
            metrics: Activity metrics from analyze_activity_window()
            
        Returns:
            {
                'pattern': str,
                'confidence': float,
                'description': str,
                'risk_level': str  # low | medium | high
            }
        """
        if not metrics or metrics.get('total_transactions', 0) == 0:
            return {
                'pattern': 'unknown',
                'confidence': 0.0,
                'description': 'No transaction data',
                'risk_level': 'high'
            }
        
        is_dormant = metrics.get('is_dormant', False)
        dormant_days = metrics.get('dormant_days', 0)
        activity_window = metrics.get('activity_window_days', 0)
        txs_per_day = metrics.get('transactions_per_day', 0)
        total_txs = metrics.get('total_transactions', 0)
        
        # ====================================================================
        # PATTERN 1: Dormant
        # ====================================================================
        if is_dormant:
            return {
                'pattern': 'dormant',
                'confidence': 1.0,
                'description': f'No activity for {dormant_days} days',
                'risk_level': 'high',
                'tags': ['inactive', 'stale']
            }
        
        # ====================================================================
        # PATTERN 2: Burst
        # ====================================================================
        if activity_window <= self.burst_window and total_txs >= 10:
            # Check if truly burst (high frequency in short time)
            if txs_per_day >= 2:
                return {
                    'pattern': 'burst',
                    'confidence': 0.9,
                    'description': f'Intense activity over {activity_window} days',
                    'risk_level': 'medium',
                    'tags': ['short_term', 'high_frequency']
                }
        
        # ====================================================================
        # PATTERN 3: Sustained
        # ====================================================================
        if activity_window >= self.sustained_window:
            # Check consistency
            if txs_per_day >= 0.5:  # At least every 2 days
                return {
                    'pattern': 'sustained',
                    'confidence': 0.95,
                    'description': f'Consistent activity over {activity_window} days',
                    'risk_level': 'low',
                    'tags': ['long_term', 'consistent', 'reliable']
                }
            else:
                return {
                    'pattern': 'sustained_sporadic',
                    'confidence': 0.8,
                    'description': f'Long-term but irregular activity over {activity_window} days',
                    'risk_level': 'low',
                    'tags': ['long_term', 'irregular']
                }
        
        # ====================================================================
        # PATTERN 4: Declining
        # ====================================================================
        if activity_window >= 60 and dormant_days >= 30:
            return {
                'pattern': 'declining',
                'confidence': 0.85,
                'description': f'Slowing down ({dormant_days}d since last activity)',
                'risk_level': 'medium',
                'tags': ['waning', 'losing_momentum']
            }
        
        # ====================================================================
        # PATTERN 5: Sporadic
        # ====================================================================
        if txs_per_day < 0.5 and activity_window > self.burst_window:
            return {
                'pattern': 'sporadic',
                'confidence': 0.75,
                'description': f'Irregular activity over {activity_window} days',
                'risk_level': 'medium',
                'tags': ['irregular', 'inconsistent']
            }
        
        # ====================================================================
        # PATTERN 6: Early Stage
        # ====================================================================
        if activity_window <= 30 and dormant_days <= 7:
            return {
                'pattern': 'early_stage',
                'confidence': 0.7,
                'description': f'New wallet with {total_txs} transactions',
                'risk_level': 'medium',
                'tags': ['new', 'establishing']
            }
        
        # ====================================================================
        # DEFAULT: Active
        # ====================================================================
        return {
            'pattern': 'active',
            'confidence': 0.6,
            'description': f'Active wallet ({dormant_days}d since last TX)',
            'risk_level': 'low',
            'tags': ['active']
        }
    
    
    def analyze_lifecycle_stage(
        self,
        metrics: Dict[str, Any],
        pattern: Dict[str, Any]
    ) -> str:
        """
        Determine wallet lifecycle stage.
        
        Stages:
        - genesis: < 7 days old, < 10 transactions
        - growth: 7-90 days, increasing activity
        - mature: 90+ days, stable activity
        - declining: Decreasing activity trend
        - dormant: > 90 days inactive
        
        Args:
            metrics: Activity metrics
            pattern: Pattern classification
            
        Returns:
            Lifecycle stage string
        """
        if not metrics:
            return 'unknown'
        
        total_age_days = metrics.get('activity_window_days', 0) + metrics.get('dormant_days', 0)
        is_dormant = metrics.get('is_dormant', False)
        pattern_type = pattern.get('pattern', 'unknown')
        total_txs = metrics.get('total_transactions', 0)
        
        # Dormant stage
        if is_dormant:
            return 'dormant'
        
        # Genesis stage (very new)
        if total_age_days <= 7 and total_txs < 10:
            return 'genesis'
        
        # Growth stage (building up)
        if total_age_days <= 90 and pattern_type in ['burst', 'early_stage', 'active']:
            return 'growth'
        
        # Declining stage
        if pattern_type in ['declining', 'sporadic'] and total_age_days > 90:
            return 'declining'
        
        # Mature stage (established)
        if total_age_days > 90 and pattern_type in ['sustained', 'sustained_sporadic', 'active']:
            return 'mature'
        
        return 'active'
    
    
    def get_temporal_analysis(
        self,
        transactions: List[Dict],
        current_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Complete temporal analysis of wallet activity.
        
        Combines all analysis methods into comprehensive report.
        
        Args:
            transactions: List of transactions
            current_time: Reference time (default: now)
            
        Returns:
            {
                'metrics': {...},
                'activity_score': float,
                'pattern': {...},
                'lifecycle_stage': str,
                'recommendations': List[str]
            }
        """
        logger.info(f"📅 Performing temporal analysis on {len(transactions)} transactions...")
        
        # Get activity metrics
        metrics = self.analyze_activity_window(transactions, current_time)
        
        # Calculate activity score
        activity_score = self.calculate_activity_score(metrics)
        
        # Classify pattern
        pattern = self.classify_activity_pattern(metrics)
        
        # Determine lifecycle stage
        lifecycle_stage = self.analyze_lifecycle_stage(metrics, pattern)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(metrics, pattern, activity_score)
        
        result = {
            'metrics': metrics,
            'activity_score': activity_score,
            'pattern': pattern,
            'lifecycle_stage': lifecycle_stage,
            'recommendations': recommendations
        }
        
        logger.info(
            f"✅ Analysis: {pattern['pattern']} pattern, "
            f"{lifecycle_stage} stage, "
            f"score: {activity_score:.1f}/100"
        )
        
        return result
    
    
    def _generate_recommendations(
        self,
        metrics: Dict[str, Any],
        pattern: Dict[str, Any],
        activity_score: float
    ) -> List[str]:
        """Generate actionable recommendations based on analysis."""
        recommendations = []
        
        pattern_type = pattern.get('pattern', 'unknown')
        risk_level = pattern.get('risk_level', 'unknown')
        
        # Risk-based recommendations
        if risk_level == 'high':
            recommendations.append("⚠️ High risk: Wallet is dormant or inactive")
            recommendations.append("Consider checking current balance before classification")
        
        # Pattern-specific recommendations
        if pattern_type == 'dormant':
            recommendations.append("🔍 Verify wallet is not just inactive but actually abandoned")
            recommendations.append("Check if funds have been withdrawn")
        
        elif pattern_type == 'burst':
            recommendations.append("⚡ Short-term trader - may be transient activity")
            recommendations.append("Monitor for sustained engagement before high classification")
        
        elif pattern_type == 'sustained':
            recommendations.append("✅ Reliable long-term activity pattern")
            recommendations.append("Good candidate for high-confidence classification")
        
        elif pattern_type == 'declining':
            recommendations.append("📉 Activity declining - wallet may become dormant soon")
            recommendations.append("Consider lower confidence score")
        
        # Score-based recommendations
        if activity_score < 30:
            recommendations.append("❌ Low activity score - use caution in classification")
        elif activity_score >= 70:
            recommendations.append("✅ High activity score - reliable for classification")
        
        return recommendations
    
    
    def _empty_activity_metrics(self) -> Dict[str, Any]:
        """Return empty metrics structure."""
        return {
            'first_seen': None,
            'last_seen': None,
            'activity_window_days': 0,
            'dormant_days': 999,
            'is_dormant': True,
            'total_transactions': 0,
            'transactions_per_day': 0.0,
            'activity_density': 0.0
        }


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def create_activity_analyzer(
    dormancy_threshold_days: int = 90
) -> ActivityAnalyzer:
    """
    Create and initialize an ActivityAnalyzer.
    
    Args:
        dormancy_threshold_days: Days threshold for dormancy (default: 90)
        
    Returns:
        Initialized ActivityAnalyzer
    """
    return ActivityAnalyzer(
        dormancy_threshold_days=dormancy_threshold_days
    )


def quick_activity_check(
    transactions: List[Dict],
    dormancy_threshold: int = 90
) -> Dict[str, Any]:
    """
    Quick activity check without full analysis.
    
    Returns basic metrics: is_dormant, dormant_days, activity_window
    """
    analyzer = ActivityAnalyzer(dormancy_threshold_days=dormancy_threshold)
    metrics = analyzer.analyze_activity_window(transactions)
    
    return {
        'is_dormant': metrics.get('is_dormant', True),
        'dormant_days': metrics.get('dormant_days', 999),
        'activity_window_days': metrics.get('activity_window_days', 0),
        'last_active': metrics.get('last_seen')
    }


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    'ActivityAnalyzer',
    'create_activity_analyzer',
    'quick_activity_check'
]
