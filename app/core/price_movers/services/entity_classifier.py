"""
Entity Classifier - Klassifiziert Entity-Typen

NO ML - Rule-based Classification
"""

import logging
from typing import Optional


logger = logging.getLogger(__name__)


class EntityClassifier:
    """
    Klassifiziert Entities basierend auf Charakteristiken
    
    Types:
    - whale: Große Trades, hoher Impact
    - bot: Viele kleine Trades, regelmäßiges Timing
    - market_maker: Beide Seiten, mittlere Größe
    - unknown: Rest
    """
    
    def __init__(self):
        logger.debug("EntityClassifier initialisiert")
    
    def classify(
        self,
        avg_trade_size: float,
        trade_count: int,
        size_consistency: float,
        timing_pattern: str,
        buy_sell_ratio: float,
        impact_score: float
    ) -> str:
        """
        Klassifiziert Entity-Typ
        
        Returns: 'whale', 'bot', 'market_maker', oder 'unknown'
        """
        
        # 1. Bot Detection (höchste Priorität)
        if self._is_bot(
            trade_count=trade_count,
            timing_pattern=timing_pattern,
            size_consistency=size_consistency
        ):
            return 'bot'
        
        # 2. Whale Detection
        if self._is_whale(
            avg_trade_size=avg_trade_size,
            impact_score=impact_score
        ):
            return 'whale'
        
        # 3. Market Maker Detection
        if self._is_market_maker(
            buy_sell_ratio=buy_sell_ratio,
            avg_trade_size=avg_trade_size,
            trade_count=trade_count
        ):
            return 'market_maker'
        
        # 4. Default
        return 'unknown'
    
    def _is_bot(
        self,
        trade_count: int,
        timing_pattern: str,
        size_consistency: float
    ) -> bool:
        """
        Bot Kriterien:
        - >= 10 Trades
        - Regular Timing
        - Hohe Size Consistency
        """
        return (
            trade_count >= 10 and
            timing_pattern == 'regular' and
            size_consistency > 0.7
        )
    
    def _is_whale(
        self,
        avg_trade_size: float,
        impact_score: float
    ) -> bool:
        """
        Whale Kriterien:
        - Avg Trade Size > $100k ODER
        - Impact Score > 0.7
        """
        return (
            avg_trade_size > 100_000 or
            impact_score > 0.7
        )
    
    def _is_market_maker(
        self,
        buy_sell_ratio: float,
        avg_trade_size: float,
        trade_count: int
    ) -> bool:
        """
        Market Maker Kriterien:
        - Ausgeglichenes Buy/Sell Ratio (0.7-1.3)
        - Mittlere Trade-Größe ($10k+)
        - Mehrere Trades (5+)
        """
        return (
            0.7 <= buy_sell_ratio <= 1.3 and
            avg_trade_size > 10_000 and
            trade_count >= 5
        )
