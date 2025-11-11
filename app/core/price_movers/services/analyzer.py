"""
Price Mover Analyzer - UPDATED with Hybrid Support

Dieses File exportiert BEIDE Analyzer:
1. PriceMoverAnalyzer (Legacy) - Für Single-Exchange Analyse
2. HybridPriceMoverAnalyzer (Neu) - Für CEX+DEX Combined Analyse

WICHTIG: Dieses File ersetzt die alte analyzer.py!
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Union
from collections import defaultdict
from dataclasses import dataclass

from app.core.price_movers.services.impact_calculator import ImpactCalculator
from app.core.price_movers.services.lightweight_entity_identifier import (
    LightweightEntityIdentifier,
    TradingEntity
)
from app.core.price_movers.services.entity_classifier import EntityClassifier
from app.core.price_movers.utils.metrics import (
    detect_bot_pattern,
    detect_whale_pattern,
    detect_smart_money_pattern,
    validate_trade_data,
    validate_candle_data,
    measure_time,
)


logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Einzelner Trade"""
    timestamp: datetime
    trade_type: str  # 'buy' oder 'sell'
    amount: float
    price: float
    value_usd: float
    trade_count: int = 1
    wallet_address: Optional[str] = None  # 🆕 Für DEX
    source: str = "cex"  # 'cex' oder 'dex'


@dataclass
class Candle:
    """OHLCV Candle"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    @property
    def price_change_pct(self) -> float:
        if self.open == 0:
            return 0.0
        return ((self.close - self.open) / self.open) * 100
    
    @property
    def volatility(self) -> float:
        if self.low == 0:
            return 0.0
        return ((self.high - self.low) / self.low) * 100


@dataclass
class WalletActivity:
    """Wallet-Aktivitäten Container (LEGACY)"""
    wallet_id: str
    trades: List[Trade]
    
    @property
    def trade_count(self) -> int:
        return len(self.trades)
    
    @property
    def total_volume(self) -> float:
        return sum(t.amount for t in self.trades)
    
    @property
    def total_value_usd(self) -> float:
        return sum(t.value_usd for t in self.trades)
    
    @property
    def avg_trade_size(self) -> float:
        if not self.trades:
            return 0.0
        return self.total_volume / len(self.trades)
    
    @property
    def buy_trades(self) -> int:
        return sum(1 for t in self.trades if t.trade_type == 'buy')
    
    @property
    def sell_trades(self) -> int:
        return sum(1 for t in self.trades if t.trade_type == 'sell')
    
    @property
    def timestamps(self) -> List[datetime]:
        return [t.timestamp for t in self.trades]
    
    def to_dict_list(self) -> List[Dict]:
        """Konvertiert Trades zu Dictionary-Liste"""
        return [
            {
                "timestamp": t.timestamp,
                "trade_type": t.trade_type,
                "amount": t.amount,
                "price": t.price,
                "value_usd": t.value_usd
            }
            for t in self.trades
        ]


class PriceMoverAnalyzer:
    """
    🔵 LEGACY Single-Exchange Analyzer
    
    Für Backward-Compatibility mit bestehendem Code.
    Nutzt Lightweight Entity Identifier für Pattern-basierte Analyse.
    
    EMPFEHLUNG: Für neue Features nutze HybridPriceMoverAnalyzer!
    """
    
    def __init__(
        self,
        exchange_collector=None,
        impact_calculator: Optional[ImpactCalculator] = None,
        use_lightweight: bool = True
    ):
        """
        Args:
            exchange_collector: Exchange Collector (Single)
            impact_calculator: Impact Calculator
            use_lightweight: Use Lightweight Identifier
        """
        self.exchange_collector = exchange_collector
        self.use_lightweight = use_lightweight
        
        if use_lightweight:
            self.entity_identifier = LightweightEntityIdentifier(
                exchange_collector=exchange_collector
            )
            logger.info("✓ PriceMoverAnalyzer: Lightweight Mode")
        else:
            self.impact_calculator = impact_calculator or ImpactCalculator()
            logger.info("⚠️ PriceMoverAnalyzer: Legacy Mode")
        
        self.classifier = EntityClassifier()
        
        logger.info("PriceMoverAnalyzer initialized (Legacy)")
    
    @measure_time
    async def analyze_candle(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        min_impact_threshold: float = 0.05,
        top_n_wallets: int = 10,
        include_trades: bool = False,
        use_enhanced: bool = True
    ) -> Dict:
        """
        Haupt-Analyse-Methode (Legacy)
        
        Analysiert eine einzelne Exchange (CEX oder DEX).
        
        Args:
            exchange: Exchange Name
            symbol: Trading Pair
            timeframe: Candle Timeframe
            start_time: Start
            end_time: Ende
            min_impact_threshold: Min Impact Score
            top_n_wallets: Anzahl Top Wallets
            include_trades: Include Trade Details
            use_enhanced: Use Aggregated Trades (wenn verfügbar)
            
        Returns:
            {
                'candle': {...},
                'top_movers': [...],
                'analysis_metadata': {...}
            }
        """
        start = datetime.now()
        logger.info(
            f"🔵 Single-Exchange Analysis: {exchange} {symbol} {timeframe}"
        )
        
        try:
            # Phase 1: Data Collection
            candle, trades = await self._fetch_all_data(
                exchange, symbol, timeframe, start_time, end_time, use_enhanced
            )
            
            if not trades:
                logger.warning("No trades found")
                return self._empty_response(candle, exchange, symbol, timeframe)
            
            logger.info(f"✓ {len(trades)} trades fetched")
            
            # Phase 2: Entity Identification
            if self.use_lightweight:
                entities = await self.entity_identifier.identify_entities(
                    trades=self._trades_to_dict_list(trades),
                    candle_data={
                        'timestamp': candle.timestamp,
                        'open': candle.open,
                        'high': candle.high,
                        'low': candle.low,
                        'close': candle.close,
                        'volume': candle.volume,
                        'price_change_pct': candle.price_change_pct
                    },
                    symbol=symbol,
                    exchange=exchange
                )
                
                top_movers = self._format_entities_as_movers(
                    entities[:top_n_wallets], include_trades
                )
            else:
                # Legacy mode
                wallet_activities = self._aggregate_wallet_activities(trades, candle)
                scored_wallets = await self._calculate_all_impacts(
                    wallet_activities, candle, candle.volume
                )
                top_movers = self._rank_and_filter(
                    scored_wallets, min_impact_threshold, top_n_wallets, include_trades
                )
            
            # Build Response
            duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            
            response = {
                "candle": {
                    "timestamp": candle.timestamp,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume
                },
                "top_movers": top_movers,
                "analysis_metadata": {
                    "analysis_timestamp": datetime.now(),
                    "processing_duration_ms": duration_ms,
                    "total_trades_analyzed": len(trades),
                    "unique_wallets_found": len(top_movers),
                    "exchange": str(exchange),
                    "symbol": symbol,
                    "timeframe": str(timeframe)
                }
            }
            
            logger.info(
                f"✅ Analysis complete: {len(top_movers)} movers found"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"❌ Analysis error: {e}", exc_info=True)
            raise
    
    async def _fetch_all_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        use_enhanced: bool = False
    ) -> Tuple[Candle, List[Trade]]:
        """Fetcht Daten von Exchange"""
        if not self.exchange_collector:
            logger.warning("No collector, using mock data")
            return await self._fetch_mock_data(start_time, end_time)
        
        try:
            # Fetch Candle
            candle_data = await self.exchange_collector.fetch_candle_data(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=start_time
            )
            
            # Fetch Trades
            trades_data = await self.exchange_collector.fetch_trades(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time
            )
            
            # Parse
            candle = Candle(**candle_data)
            
            trades = []
            for t in trades_data:
                if not validate_trade_data(t):
                    continue
                
                trade = Trade(
                    timestamp=t['timestamp'],
                    trade_type=t['trade_type'],
                    amount=t['amount'],
                    price=t['price'],
                    value_usd=t.get('value_usd', t['amount'] * t['price']),
                    trade_count=t.get('trade_count', 1),
                    wallet_address=t.get('wallet_address'),  # May be None (CEX)
                    source='dex' if t.get('wallet_address') else 'cex'
                )
                trades.append(trade)
            
            return candle, trades
            
        except Exception as e:
            logger.error(f"Fetch error: {e}")
            return await self._fetch_mock_data(start_time, end_time)
    
    async def _fetch_mock_data(
        self, start_time: datetime, end_time: datetime
    ) -> Tuple[Candle, List[Trade]]:
        """Mock data for testing"""
        import random
        
        candle = Candle(
            timestamp=start_time,
            open=67500.0,
            high=67800.0,
            low=67450.0,
            close=67750.0,
            volume=1234.56
        )
        
        trades = []
        current_time = start_time
        
        for _ in range(30):
            current_time += timedelta(seconds=random.randint(5, 15))
            if current_time > end_time:
                break
            
            trade = Trade(
                timestamp=current_time,
                trade_type='buy' if random.random() > 0.5 else 'sell',
                amount=random.uniform(0.1, 5.0),
                price=67500.0 + random.uniform(-100, 100),
                value_usd=random.uniform(100, 50000),
                trade_count=1
            )
            trades.append(trade)
        
        return candle, trades
    
    def _trades_to_dict_list(self, trades: List[Trade]) -> List[Dict]:
        """Convert to dict list"""
        return [
            {
                'timestamp': t.timestamp,
                'trade_type': t.trade_type,
                'amount': t.amount,
                'price': t.price,
                'value_usd': t.value_usd,
                'trade_count': t.trade_count
            }
            for t in trades
        ]
    
    def _format_entities_as_movers(
        self, entities: List[TradingEntity], include_trades: bool
    ) -> List[Dict]:
        """Format entities"""
        movers = []
        for entity in entities:
            mover = {
                "wallet_id": entity.entity_id,
                "wallet_type": entity.entity_type,
                "impact_score": entity.impact_score,
                "impact_level": entity.impact_level,
                "total_volume": round(entity.total_volume, 4),
                "total_value_usd": round(entity.total_value_usd, 2),
                "trade_count": entity.trade_count,
                "avg_trade_size": round(entity.avg_trade_size, 4),
                "volume_ratio": round(entity.impact_components["volume_ratio"], 3),
                "confidence_score": entity.confidence_score,
                "timing_pattern": entity.timing_pattern,
                "components": entity.impact_components,
            }
            movers.append(mover)
        return movers
    
    def _aggregate_wallet_activities(
        self, trades: List[Trade], candle: Candle
    ) -> Dict[str, WalletActivity]:
        """Legacy: Pattern-based grouping"""
        wallet_map = defaultdict(list)
        
        for trade in trades:
            wallet_id = self._identify_wallet_pattern(trade, candle)
            wallet_map[wallet_id].append(trade)
        
        return {
            wallet_id: WalletActivity(wallet_id=wallet_id, trades=trade_list)
            for wallet_id, trade_list in wallet_map.items()
        }
    
    def _identify_wallet_pattern(self, trade: Trade, candle: Candle) -> str:
        """Legacy: Pattern-based ID"""
        if trade.value_usd > 100_000:
            return f"whale_{hash(trade.value_usd) % 10}"
        elif trade.value_usd > 50_000:
            return f"smart_money_{hash(trade.amount) % 10}"
        elif trade.amount < 1.0:
            return f"bot_{hash(round(trade.amount, 2)) % 20}"
        else:
            return f"retail_{hash(trade.value_usd) % 50}"
    
    async def _calculate_all_impacts(
        self, wallet_activities: Dict[str, WalletActivity],
        candle: Candle, total_volume: float
    ) -> List[Dict]:
        """Legacy: Calculate impacts"""
        scored_wallets = []
        
        candle_data = {
            "timestamp": candle.timestamp,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "price_change_pct": candle.price_change_pct
        }
        
        impact_calculator = ImpactCalculator()
        
        for wallet_id, activity in wallet_activities.items():
            impact_result = impact_calculator.calculate_impact_score(
                wallet_trades=activity.to_dict_list(),
                candle_data=candle_data,
                total_volume=total_volume
            )
            
            wallet_type = self._classify_wallet_type_legacy(activity, impact_result["impact_score"])
            
            scored_wallets.append({
                "wallet_id": wallet_id,
                "wallet_type": wallet_type,
                "impact_score": impact_result["impact_score"],
                "impact_level": impact_result["impact_level"],
                "components": impact_result["components"],
                "activity": activity
            })
        
        return scored_wallets
    
    def _classify_wallet_type_legacy(
        self, activity: WalletActivity, impact_score: float
    ) -> str:
        """Legacy: Classify wallet"""
        trades_dict = activity.to_dict_list()
        
        if detect_bot_pattern(trades_dict):
            return "bot"
        if detect_whale_pattern(trades_dict):
            return "whale"
        if detect_smart_money_pattern(trades_dict):
            return "market_maker"
        
        if activity.buy_trades > 0 and activity.sell_trades > 0:
            ratio = activity.buy_trades / activity.sell_trades
            if 0.7 <= ratio <= 1.3:
                return "market_maker"
        
        avg_value = activity.total_value_usd / activity.trade_count if activity.trade_count > 0 else 0
        
        if avg_value > 100_000:
            return "whale"
        elif avg_value > 50_000:
            return "market_maker"
        elif activity.trade_count > 10:
            return "bot"
        else:
            return "unknown"
    
    def _rank_and_filter(
        self, scored_wallets: List[Dict], min_threshold: float,
        top_n: int, include_trades: bool
    ) -> List[Dict]:
        """Legacy: Filter and rank"""
        filtered = [w for w in scored_wallets if w["impact_score"] >= min_threshold]
        filtered.sort(key=lambda x: x["impact_score"], reverse=True)
        
        result = []
        for wallet in filtered[:top_n]:
            activity = wallet["activity"]
            wallet_data = {
                "wallet_id": wallet["wallet_id"],
                "wallet_type": wallet["wallet_type"],
                "impact_score": wallet["impact_score"],
                "impact_level": wallet["impact_level"],
                "total_volume": round(activity.total_volume, 4),
                "total_value_usd": round(activity.total_value_usd, 2),
                "trade_count": activity.trade_count,
                "avg_trade_size": round(activity.avg_trade_size, 4),
                "volume_ratio": round(wallet["components"]["volume_ratio"], 3),
                "components": wallet["components"]
            }
            result.append(wallet_data)
        
        return result
    
    def _empty_response(
        self, candle: Candle, exchange: str, symbol: str, timeframe: str
    ) -> Dict:
        """Empty response"""
        return {
            "candle": {
                "timestamp": candle.timestamp,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume
            },
            "top_movers": [],
            "analysis_metadata": {
                "analysis_timestamp": datetime.now(),
                "processing_duration_ms": 0,
                "total_trades_analyzed": 0,
                "unique_wallets_found": 0,
                "exchange": str(exchange),
                "symbol": symbol,
                "timeframe": str(timeframe)
            }
        }


# ==================== EXPORT BOTH CLASSES ====================

# Legacy class (already defined above)
# PriceMoverAnalyzer

# New Hybrid class (import from hybrid file)
try:
    from app.core.price_movers.services.analyzer_hybrid import HybridPriceMoverAnalyzer
    logger.info("✓ HybridPriceMoverAnalyzer imported")
except ImportError as e:
    logger.warning(f"⚠️ HybridPriceMoverAnalyzer not available: {e}")
    # Fallback: Use PriceMoverAnalyzer
    HybridPriceMoverAnalyzer = PriceMoverAnalyzer


__all__ = [
    'PriceMoverAnalyzer',      # Legacy
    'HybridPriceMoverAnalyzer', # New
    'Trade',
    'Candle',
    'WalletActivity',
]
