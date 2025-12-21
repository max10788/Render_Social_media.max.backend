from typing import Dict, Optional, List
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
import logging

from app.core.otc_analysis.models.wallet import Wallet
from app.core.otc_analysis.models.transaction import Transaction  # Annahme: Transaction Model existiert
from app.core.otc_analysis.utils.cache import CacheManager

logger = logging.getLogger(__name__)

class StatisticsService:
    """
    Service for calculating OTC statistics.
    
    Used by GET /api/otc/statistics endpoint.
    """
    
    def __init__(self, cache_manager: Optional[CacheManager] = None):
        self.cache = cache_manager
    
    def get_statistics(
        self,
        db: Session,
        from_date: datetime,
        to_date: datetime,
        entity_type: Optional[str] = None
    ) -> Dict:
        """
        Calculate statistics for OTC activity.
        
        Args:
            db: Database session
            from_date: Start date
            to_date: End date
            entity_type: Optional filter ('otc_desk', 'institutional', etc.)
        
        Returns:
            {
                'total_volume_usd': float,
                'active_wallets': int,
                'avg_transfer_size': float,
                'avg_confidence_score': float,
                'volume_change_24h': float,
                'wallets_change_24h': float,
                'avg_size_change_24h': float,
                'confidence_change_24h': float,
                'last_updated': str
            }
        """
        # Check cache first
        cache_key = f"stats:{from_date.date()}:{to_date.date()}:{entity_type or 'all'}"
        if self.cache:
            cached = self.cache.get(cache_key, prefix='statistics')
            if cached:
                logger.info("📊 Statistics loaded from cache")
                return cached
        
        # Base query
        query = db.query(Wallet).filter(
            and_(
                Wallet.last_seen >= from_date,
                Wallet.last_seen <= to_date,
                Wallet.otc_probability > 0.5  # Only suspected OTC wallets
            )
        )
        
        # Filter by entity type if provided
        if entity_type:
            query = query.filter(Wallet.entity_type == entity_type)
        
        wallets = query.all()
        
        if not wallets:
            return self._empty_stats()
        
        # Calculate current period stats
        total_volume = sum(w.total_volume_usd for w in wallets)
        active_wallets = len(wallets)
        avg_transfer = sum(w.avg_transaction_usd for w in wallets) / active_wallets if active_wallets > 0 else 0
        avg_confidence = sum(w.confidence_score for w in wallets) / active_wallets if active_wallets > 0 else 0
        
        # Calculate 24h comparison
        yesterday = datetime.utcnow() - timedelta(days=1)
        yesterday_stats = self._get_comparison_stats(
            db,
            yesterday - timedelta(days=1),
            yesterday,
            entity_type
        )
        
        # Calculate changes
        volume_change = self._calculate_change(total_volume, yesterday_stats['total_volume'])
        wallets_change = self._calculate_change(active_wallets, yesterday_stats['active_wallets'])
        avg_size_change = self._calculate_change(avg_transfer, yesterday_stats['avg_transfer'])
        confidence_change = self._calculate_change(avg_confidence, yesterday_stats['avg_confidence'])
        
        result = {
            'total_volume_usd': total_volume,
            'active_wallets': active_wallets,
            'avg_transfer_size': avg_transfer,
            'avg_confidence_score': avg_confidence,
            'volume_change_24h': volume_change,
            'wallets_change_24h': wallets_change,
            'avg_size_change_24h': avg_size_change,
            'confidence_change_24h': confidence_change,
            'last_updated': datetime.utcnow().isoformat()
        }
        
        # Cache for 5 minutes
        if self.cache:
            self.cache.set(cache_key, result, ttl=300, prefix='statistics')
        
        logger.info(f"📊 Statistics calculated: {active_wallets} wallets, ${total_volume:,.0f} volume")
        
        return result
    
    def _get_comparison_stats(
        self,
        db: Session,
        from_date: datetime,
        to_date: datetime,
        entity_type: Optional[str] = None
    ) -> Dict:
        """Get stats for comparison period."""
        query = db.query(Wallet).filter(
            and_(
                Wallet.last_seen >= from_date,
                Wallet.last_seen <= to_date,
                Wallet.otc_probability > 0.5
            )
        )
        
        if entity_type:
            query = query.filter(Wallet.entity_type == entity_type)
        
        wallets = query.all()
        
        if not wallets:
            return {
                'total_volume': 0,
                'active_wallets': 0,
                'avg_transfer': 0,
                'avg_confidence': 0
            }
        
        active_count = len(wallets)
        
        return {
            'total_volume': sum(w.total_volume_usd for w in wallets),
            'active_wallets': active_count,
            'avg_transfer': sum(w.avg_transaction_usd for w in wallets) / active_count if active_count > 0 else 0,
            'avg_confidence': sum(w.confidence_score for w in wallets) / active_count if active_count > 0 else 0
        }
    
    def _calculate_change(self, current: float, previous: float) -> float:
        """Calculate percentage change."""
        if previous == 0:
            return 0 if current == 0 else 100
        return ((current - previous) / previous) * 100
    
    def _empty_stats(self) -> Dict:
        """Return empty statistics."""
        return {
            'total_volume_usd': 0,
            'active_wallets': 0,
            'avg_transfer_size': 0,
            'avg_confidence_score': 0,
            'volume_change_24h': 0,
            'wallets_change_24h': 0,
            'avg_size_change_24h': 0,
            'confidence_change_24h': 0,
            'last_updated': datetime.utcnow().isoformat()
        }
