"""
Chart Data Generators für Wallet Details
==========================================

Generiert Chart-Daten für Frontend-Visualisierungen.

Features:
- Activity Charts (7/30 Tage)
- Transfer Size Charts
- Volume Trend Charts
- Temporal Pattern Analysis

Version: 1.0
Date: 2025-01-06
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import statistics

logger = logging.getLogger(__name__)


class ChartDataGenerator:
    """
    Generiert Chart-Daten aus Transaktionslisten.
    """
    
    @staticmethod
    def generate_activity_chart(
        transactions: List[Dict],
        days: int = 7
    ) -> List[Dict]:
        """
        Generiert Activity Chart für letzte N Tage.
        
        Args:
            transactions: Liste von Transaktionen
            days: Anzahl Tage (default: 7)
            
        Returns:
            [
                {"date": "2024-01-10", "volume": 150000},
                {"date": "2024-01-11", "volume": 200000},
                ...
            ]
        """
        logger.info(f"📊 Generating {days}-day activity chart from {len(transactions)} transactions...")
        
        # ✅ FIX: Use timezone-aware datetime
        from datetime import timezone as tz
        now = datetime.now(tz.utc)
        cutoff = now - timedelta(days=days)
        
        # Filter letzte N Tage with timezone handling
        recent_txs = []
        for tx in transactions:
            if not tx.get('timestamp'):
                continue
                
            tx_timestamp = tx['timestamp']
            
            # Make naive timestamps aware (assume UTC)
            if tx_timestamp.tzinfo is None:
                tx_timestamp = tx_timestamp.replace(tzinfo=tz.utc)
            
            if tx_timestamp >= cutoff:
                recent_txs.append(tx)
        
        logger.info(f"   • Found {len(recent_txs)} transactions in last {days} days")
        
        # Group by date
        daily_volume = defaultdict(float)
        
        for tx in recent_txs:
            try:
                date_str = tx['timestamp'].strftime('%Y-%m-%d')
                usd_value = tx.get('usd_value', 0) or 0
                daily_volume[date_str] += usd_value
            except Exception as e:
                logger.debug(f"   ⚠️ Error processing TX: {e}")
                continue
        
        # Generate complete date range
        result = []
        for i in range(days):
            date = now - timedelta(days=days-i-1)
            date_str = date.strftime('%Y-%m-%d')
            
            result.append({
                'date': date_str,
                'volume': round(daily_volume.get(date_str, 0), 2)
            })
        
        # Log summary
        total_volume = sum(item['volume'] for item in result)
        non_zero_days = sum(1 for item in result if item['volume'] > 0)
        
        logger.info(f"   ✅ Chart generated: {non_zero_days}/{days} days with activity")
        logger.info(f"   💰 Total volume: ${total_volume:,.2f}")
        
        return result
    
    @staticmethod
    def generate_transfer_size_chart(
        transactions: List[Dict],
        days: int = 7
    ) -> List[Dict]:
        """
        Generiert Transfer Size Chart für letzte N Tage.
        
        Zeigt durchschnittliche Transaktionsgröße pro Tag.
        
        Args:
            transactions: Liste von Transaktionen
            days: Anzahl Tage (default: 7)
            
        Returns:
            [
                {"date": "2024-01-10", "size": 50000},
                {"date": "2024-01-11", "size": 75000},
                ...
            ]
        """
        logger.info(f"📊 Generating {days}-day transfer size chart...")
        
        # ✅ FIX: Use timezone-aware datetime
        from datetime import timezone as tz
        now = datetime.now(tz.utc)
        cutoff = now - timedelta(days=days)
        
        # Filter letzte N Tage with timezone handling
        recent_txs = []
        for tx in transactions:
            if not tx.get('timestamp'):
                continue
                
            tx_timestamp = tx['timestamp']
            
            # Make naive timestamps aware (assume UTC)
            if tx_timestamp.tzinfo is None:
                tx_timestamp = tx_timestamp.replace(tzinfo=tz.utc)
            
            if tx_timestamp >= cutoff:
                recent_txs.append(tx)
        
        # Group by date, collect all sizes
        daily_sizes = defaultdict(list)
        
        for tx in recent_txs:
            try:
                date_str = tx['timestamp'].strftime('%Y-%m-%d')
                usd_value = tx.get('usd_value', 0) or 0
                
                if usd_value > 0:
                    daily_sizes[date_str].append(usd_value)
            except Exception as e:
                logger.debug(f"   ⚠️ Error processing TX: {e}")
                continue
        
        # Generate complete date range with averages
        result = []
        for i in range(days):
            date = now - timedelta(days=days-i-1)
            date_str = date.strftime('%Y-%m-%d')
            
            sizes = daily_sizes.get(date_str, [])
            avg_size = statistics.mean(sizes) if sizes else 0
            
            result.append({
                'date': date_str,
                'size': round(avg_size, 2)
            })
        
        # Log summary
        avg_overall = statistics.mean([item['size'] for item in result if item['size'] > 0]) if any(item['size'] > 0 for item in result) else 0
        logger.info(f"   ✅ Chart generated, avg transfer size: ${avg_overall:,.2f}")
        
        return result
    
    @staticmethod
    def generate_volume_trend_chart(
        transactions: List[Dict],
        days: int = 30,
        aggregation: str = 'daily'
    ) -> List[Dict]:
        """
        Generiert Volume Trend Chart mit verschiedenen Aggregationen.
        
        Args:
            transactions: Liste von Transaktionen
            days: Anzahl Tage
            aggregation: 'daily', 'weekly', 'monthly'
            
        Returns:
            Chart-Daten mit aggregiertem Volume
        """
        logger.info(f"📊 Generating {days}-day volume trend ({aggregation} aggregation)...")
        
        cutoff = datetime.now() - timedelta(days=days)
        recent_txs = [
            tx for tx in transactions
            if tx.get('timestamp') and tx['timestamp'] >= cutoff
        ]
        
        if aggregation == 'daily':
            return ChartDataGenerator.generate_activity_chart(transactions, days)
        
        elif aggregation == 'weekly':
            # Group by week
            weekly_volume = defaultdict(float)
            
            for tx in recent_txs:
                try:
                    # Get ISO week
                    week_start = tx['timestamp'] - timedelta(days=tx['timestamp'].weekday())
                    week_str = week_start.strftime('%Y-%m-%d')
                    
                    usd_value = tx.get('usd_value', 0) or 0
                    weekly_volume[week_str] += usd_value
                except:
                    continue
            
            # Sort and return
            result = [
                {'date': week, 'volume': round(vol, 2)}
                for week, vol in sorted(weekly_volume.items())
            ]
            
            logger.info(f"   ✅ {len(result)} weeks of data")
            return result
        
        else:
            logger.warning(f"   ⚠️ Unknown aggregation: {aggregation}")
            return []
    
    @staticmethod
    def calculate_period_volumes(
        transactions: List[Dict]
    ) -> Dict[str, float]:
        """
        Berechnet Volume für verschiedene Zeitperioden.
        
        Returns:
            {
                'volume_24h': float,
                'volume_7d': float,
                'volume_30d': float,
                'volume_90d': float,
                'volume_1y': float
            }
        """
        logger.info("📊 Calculating period volumes...")
        
        # ✅ FIX: Use timezone-aware datetime
        from datetime import timezone as tz
        now = datetime.now(tz.utc)
        
        periods = {
            'volume_24h': timedelta(hours=24),
            'volume_7d': timedelta(days=7),
            'volume_30d': timedelta(days=30),
            'volume_90d': timedelta(days=90),
            'volume_1y': timedelta(days=365)
        }
        
        result = {}
        
        for period_name, delta in periods.items():
            cutoff = now - delta
            
            # ✅ FIX: Handle both naive and aware datetimes
            period_txs = []
            for tx in transactions:
                if not tx.get('timestamp'):
                    continue
                    
                tx_timestamp = tx['timestamp']
                
                # Make naive timestamps aware (assume UTC)
                if tx_timestamp.tzinfo is None:
                    tx_timestamp = tx_timestamp.replace(tzinfo=tz.utc)
                
                if tx_timestamp >= cutoff:
                    period_txs.append(tx)
            
            volume = sum(tx.get('usd_value', 0) or 0 for tx in period_txs)
            result[period_name] = round(volume, 2)
            
            logger.info(f"   • {period_name}: ${volume:,.2f}")
        
        return result
    
    @staticmethod
    def format_last_activity(last_seen: Optional[datetime]) -> str:
        """
        Formatiert letzten Aktivitäts-Timestamp für UI.
        
        Args:
            last_seen: Letzter Aktivitäts-Timestamp
            
        Returns:
            Formatierter String: "2 hours ago", "Yesterday", "2024-01-15"
        """
        if not last_seen:
            return "Unknown"
        
        # ✅ FIX: Use timezone-aware datetime
        from datetime import timezone as tz
        now = datetime.now(tz.utc)
        
        # Make both datetimes timezone-aware for comparison
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=tz.utc)
        
        if now.tzinfo is None:
            now = now.replace(tzinfo=tz.utc)
        
        delta = now - last_seen
        
        # Less than 1 hour
        if delta.total_seconds() < 3600:
            minutes = int(delta.total_seconds() / 60)
            if minutes < 1:
                return "Just now"
            return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
        
        # Less than 24 hours
        elif delta.days == 0:
            hours = int(delta.total_seconds() / 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        
        # Yesterday
        elif delta.days == 1:
            return "Yesterday"
        
        # Less than 7 days
        elif delta.days < 7:
            return f"{delta.days} days ago"
        
        # Less than 30 days
        elif delta.days < 30:
            weeks = delta.days // 7
            return f"{weeks} week{'s' if weeks != 1 else ''} ago"
        
        # Older - show date
        else:
            return last_seen.strftime('%Y-%m-%d')


class NetworkMetricsNormalizer:
    """
    Normalisiert Network Metrics auf 0-100 Skala für UI.
    """
    
    @staticmethod
    def normalize(metrics: Optional[Dict]) -> Optional[Dict]:
        """
        Normalisiert network metrics auf 0-100 Skala.
        
        Args:
            metrics: Raw network metrics from NetworkAnalysisService
                {
                    'betweenness': 0.00234,
                    'degree': 156,
                    'closeness': 0.456,
                    'eigenvector': 0.0123
                }
        
        Returns:
            Normalisierte metrics (0-100)
                {
                    'betweenness': 23.4,
                    'degree': 78.0,
                    'closeness': 45.6,
                    'eigenvector': 61.5
                }
        """
        if not metrics:
            return None
        
        logger.info("📊 Normalizing network metrics...")
        
        # Normalization factors (adjust based on your data distribution)
        normalized = {
            'betweenness': min(100, metrics.get('betweenness', 0) * 10000),
            'degree': min(100, metrics.get('degree', 0) / 10),
            'closeness': min(100, metrics.get('closeness', 0) * 100),
            'eigenvector': min(100, metrics.get('eigenvector', 0) * 5000)
        }
        
        # Round to 1 decimal
        normalized = {k: round(v, 1) for k, v in normalized.items()}
        
        logger.info(f"   ✅ Normalized: {normalized}")
        
        return normalized


# Export
__all__ = [
    'ChartDataGenerator',
    'NetworkMetricsNormalizer'
]
