"""
HYBRID Price Mover Analyzer - CEX + DEX Combined Analysis - FIXED VERSION

🔧 FIXES:
1. ✅ JSON Serialization: float('inf') → 999.0
2. ✅ Datetime: Added timezone awareness
3. ✅ NaN/Inf Validation before JSON response

🆕 NEUE FEATURES:
- ✅ CEX (Bitget/Binance/Kraken) + DEX (Jupiter/Raydium/Orca) PARALLEL
- ✅ Cross-Exchange Correlation (finde Bitget Whales auf Solana DEX!)
- ✅ Pattern-based (CEX) vs. Wallet-based (DEX) Analyse
- ✅ Unified Response mit beiden Datenquellen

ARCHITECTURE:
1. Unified Collector routet automatisch zu CEX oder DEX
2. CEX: Pattern-based Entity Identification (virtuelle Wallets)
3. DEX: Wallet-based Analysis (echte On-chain Adressen)
4. Correlation Engine: Matched CEX-Pattern mit DEX-Wallets
"""

import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Optional, Union
from collections import defaultdict
from dataclasses import dataclass

from app.core.price_movers.collectors.unified_collector import UnifiedCollector
from app.core.price_movers.services.impact_calculator import ImpactCalculator
from app.core.price_movers.services.lightweight_entity_identifier import (
    LightweightEntityIdentifier,
    TradingEntity
)
from app.core.price_movers.services.entity_classifier import EntityClassifier
from app.core.price_movers.utils.metrics import (
    validate_trade_data,
    validate_candle_data,
    measure_time,
)


logger = logging.getLogger(__name__)


def sanitize_float(value: float) -> float:
    """
    🔧 FIX: Sanitize float values for JSON compatibility
    
    Replaces:
    - NaN → 0.0
    - Infinity → 999.0
    - -Infinity → -999.0
    """
    if math.isnan(value):
        return 0.0
    if math.isinf(value):
        return 999.0 if value > 0 else -999.0
    return value


def sanitize_dict_floats(data: Dict) -> Dict:
    """
    🔧 FIX: Recursively sanitize all float values in a dictionary
    """
    sanitized = {}
    for key, value in data.items():
        if isinstance(value, float):
            sanitized[key] = sanitize_float(value)
        elif isinstance(value, dict):
            sanitized[key] = sanitize_dict_floats(value)
        elif isinstance(value, list):
            sanitized[key] = [
                sanitize_dict_floats(item) if isinstance(item, dict)
                else sanitize_float(item) if isinstance(item, float)
                else item
                for item in value
            ]
        else:
            sanitized[key] = value
    return sanitized


@dataclass
class Trade:
    """Einzelner Trade (CEX oder DEX)"""
    timestamp: datetime
    trade_type: str  # 'buy' oder 'sell'
    amount: float
    price: float
    value_usd: float
    trade_count: int = 1
    wallet_address: Optional[str] = None  # 🆕 Nur bei DEX!
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


class HybridPriceMoverAnalyzer:
    """
    🆕 HYBRID Analyzer - CEX + DEX Combined
    
    Orchestrates:
    - CEX Analysis (Pattern-based virtual entities)
    - DEX Analysis (Wallet-based real addresses)
    - Cross-Exchange Correlation
    - Unified Response
    
    FEATURES:
    - ✅ Parallel CEX + DEX Fetching
    - ✅ Lightweight Entity Identification
    - ✅ Cross-Platform Wallet Tracking
    - ✅ Correlation Score Calculation
    """
    
    def __init__(
        self,
        unified_collector: Optional[UnifiedCollector] = None,
        impact_calculator: Optional[ImpactCalculator] = None,
        use_lightweight: bool = True
    ):
        """
        Args:
            unified_collector: Unified CEX/DEX Collector
            impact_calculator: Impact Score Calculator
            use_lightweight: Use Lightweight Entity Identifier
        """
        self.unified_collector = unified_collector
        self.use_lightweight = use_lightweight
        
        if use_lightweight:
            self.entity_identifier = LightweightEntityIdentifier()
            logger.info("✓ Lightweight Entity Identification ENABLED")
        else:
            self.impact_calculator = impact_calculator or ImpactCalculator()
            logger.info("⚠️ Using legacy pattern-based clustering")
        
        self.classifier = EntityClassifier()
        
        logger.info("HybridPriceMoverAnalyzer initialized")
    
    @measure_time
    async def analyze_hybrid_candle(
        self,
        cex_exchange: str,
        dex_exchange: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        min_impact_threshold: float = 0.05,
        top_n_wallets: int = 10,
        include_trades: bool = False
    ) -> Dict:
        """
        🆕 HYBRID ANALYSIS - CEX + DEX Combined
        
        Analyzes BOTH CEX and DEX in the same timeframe and finds:
        1. Top movers on CEX (pattern-based)
        2. Top movers on DEX (wallet-based)
        3. Cross-exchange correlation
        4. Potential wash trading detection
        
        Args:
            cex_exchange: CEX name (bitget/binance/kraken)
            dex_exchange: DEX name (jupiter/raydium/orca)
            symbol: Trading pair (z.B. SOL/USDT)
            timeframe: Candle timeframe
            start_time: Analysis start
            end_time: Analysis end
            min_impact_threshold: Minimum impact score
            top_n_wallets: Number of top movers
            include_trades: Include individual trades
            
        Returns:
            {
                'candle': {...},
                'cex_analysis': {...},
                'dex_analysis': {...},
                'correlation': {...},
                'analysis_metadata': {...}
            }
        """
        start = datetime.now(timezone.utc)  # 🔧 FIX: Use timezone-aware datetime
        
        # 🔧 FIX: Ensure input datetimes are timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        logger.info(
            f"🔀 HYBRID Analysis: CEX={cex_exchange} vs DEX={dex_exchange} "
            f"{symbol} {timeframe} ({start_time} - {end_time})"
        )
        
        try:
            # Phase 1: Parallel Data Collection
            logger.debug("Phase 1: Parallel CEX + DEX data collection")
            
            cex_task = self._fetch_cex_data(
                cex_exchange, symbol, timeframe, start_time, end_time
            )
            dex_task = self._fetch_dex_data(
                dex_exchange, symbol, timeframe, start_time, end_time
            )
            
            # Wait for both
            results = await asyncio.gather(
                cex_task, dex_task, return_exceptions=True
            )
            
            # Unpack results safely
            cex_result = results[0]
            dex_result = results[1]
            
            # Check for errors
            if isinstance(cex_result, Exception):
                logger.error(f"CEX fetch error: {cex_result}")
                cex_candle, cex_trades = None, []
            else:
                cex_candle, cex_trades = cex_result
            
            if isinstance(dex_result, Exception):
                logger.error(f"DEX fetch error: {dex_result}")
                dex_candle, dex_trades = None, []
            else:
                dex_candle, dex_trades = dex_result
            
            logger.info(
                f"✓ Data fetched: CEX={len(cex_trades)} trades, "
                f"DEX={len(dex_trades)} trades"
            )
            
            # Use CEX candle as primary (usually more accurate)
            candle = cex_candle or dex_candle
            
            if not candle:
                logger.error("No candle data available from either source")
                return self._empty_hybrid_response(
                    cex_exchange, dex_exchange, symbol, timeframe
                )
            
            # Phase 2: Analyze CEX (Pattern-based)
            logger.debug("Phase 2: CEX Analysis (Pattern-based)")
            cex_movers = await self._analyze_cex_trades(
                cex_trades, candle, symbol, cex_exchange, top_n_wallets
            )
            
            # Phase 3: Analyze DEX (Wallet-based)
            logger.debug("Phase 3: DEX Analysis (Wallet-based)")
            dex_movers = await self._analyze_dex_trades(
                dex_trades, candle, symbol, dex_exchange, top_n_wallets
            )
            
            # Phase 4: Cross-Exchange Correlation
            logger.debug("Phase 4: Cross-Exchange Correlation")
            correlation = self._calculate_correlation(
                cex_movers, dex_movers, cex_trades, dex_trades
            )
            
            # Build Response
            duration_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            
            response = {
                "candle": {
                    "timestamp": candle.timestamp,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume
                },
                "cex_analysis": {
                    "exchange": cex_exchange,
                    "top_movers": cex_movers,
                    "has_wallet_ids": False,
                    "data_source": "pattern_based",
                    "trade_count": len(cex_trades)
                },
                "dex_analysis": {
                    "exchange": dex_exchange,
                    "top_movers": dex_movers,
                    "has_wallet_ids": True,
                    "data_source": "on_chain",
                    "trade_count": len(dex_trades)
                },
                "correlation": correlation,
                "analysis_metadata": {
                    "analysis_timestamp": datetime.now(timezone.utc),
                    "processing_duration_ms": duration_ms,
                    "total_trades_analyzed": len(cex_trades) + len(dex_trades),
                    "cex_entities_found": len(cex_movers),
                    "dex_wallets_found": len(dex_movers),
                    "exchanges": f"{cex_exchange}+{dex_exchange}",
                    "symbol": symbol,
                    "timeframe": timeframe
                }
            }
            
            # 🔧 FIX: Sanitize all float values before returning
            response = sanitize_dict_floats(response)
            
            logger.info(
                f"✅ HYBRID Analysis complete in {duration_ms}ms. "
                f"CEX: {len(cex_movers)} movers, DEX: {len(dex_movers)} movers, "
                f"Correlation: {correlation['score']:.2f}"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"❌ Hybrid analysis error: {e}", exc_info=True)
            raise
    
    async def _fetch_cex_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[Candle, List[Trade]]:
        """Fetch CEX data (pattern-based)"""
        if not self.unified_collector:
            logger.warning("No unified collector, using mock data")
            return await self._fetch_mock_data(start_time, end_time, "cex")
        
        try:
            result = await self.unified_collector.fetch_trades(
                exchange=exchange,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                limit=5000
            )
            
            # Also fetch candle
            candle_data = await self.unified_collector.fetch_candle_data(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=start_time
            )
            
            candle = Candle(**candle_data)
            
            # Parse trades
            trades = []
            for t in result['trades']:
                trade = Trade(
                    timestamp=t['timestamp'],
                    trade_type=t['trade_type'],
                    amount=t['amount'],
                    price=t['price'],
                    value_usd=t.get('value_usd', t['amount'] * t['price']),
                    trade_count=t.get('trade_count', 1),
                    wallet_address=None,  # CEX = no wallet IDs
                    source='cex'
                )
                trades.append(trade)
            
            logger.info(f"✓ CEX data fetched: {len(trades)} trades")
            return candle, trades
            
        except Exception as e:
            logger.error(f"CEX fetch error: {e}")
            raise  # Re-raise to be caught in analyze_hybrid_candle
    
    async def _fetch_dex_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[Candle, List[Trade]]:
        """Fetch DEX data (wallet-based)"""
        if not self.unified_collector:
            logger.warning("No unified collector, using mock data")
            return await self._fetch_mock_data(start_time, end_time, "dex")
        
        try:
            result = await self.unified_collector.fetch_trades(
                exchange=exchange,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                limit=5000
            )
            
            # Also fetch candle
            candle_data = await self.unified_collector.fetch_candle_data(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                timestamp=start_time
            )
            
            candle = Candle(**candle_data)
            
            # Parse trades WITH wallet addresses
            trades = []
            for t in result['trades']:
                trade = Trade(
                    timestamp=t['timestamp'],
                    trade_type=t['trade_type'],
                    amount=t['amount'],
                    price=t['price'],
                    value_usd=t.get('value_usd', t['amount'] * t['price']),
                    trade_count=t.get('trade_count', 1),
                    wallet_address=t.get('wallet_address'),  # 🎯 DEX = REAL Wallet!
                    source='dex'
                )
                trades.append(trade)
            
            logger.info(
                f"✓ DEX data fetched: {len(trades)} trades "
                f"({sum(1 for t in trades if t.wallet_address) if trades else 0} with wallet IDs)"
            )
            return candle, trades
            
        except Exception as e:
            logger.error(f"DEX fetch error: {e}")
            raise  # Re-raise to be caught in analyze_hybrid_candle
    
    async def _analyze_cex_trades(
        self,
        trades: List[Trade],
        candle: Candle,
        symbol: str,
        exchange: str,
        top_n: int
    ) -> List[Dict]:
        """Analyze CEX trades (pattern-based)"""
        if not trades:
            return []
        
        # Convert to dict format for entity identifier
        trades_dict = [
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
        
        candle_data = {
            'timestamp': candle.timestamp,
            'open': candle.open,
            'high': candle.high,
            'low': candle.low,
            'close': candle.close,
            'volume': candle.volume,
            'price_change_pct': candle.price_change_pct
        }
        
        # Use lightweight identifier
        entities = await self.entity_identifier.identify_entities(
            trades=trades_dict,
            candle_data=candle_data,
            symbol=symbol,
            exchange=exchange
        )
        
        # Format as movers
        return self._format_entities_as_movers(entities[:top_n], False)
    
    async def _analyze_dex_trades(
        self,
        trades: List[Trade],
        candle: Candle,
        symbol: str,
        exchange: str,
        top_n: int
    ) -> List[Dict]:
        """Analyze DEX trades (wallet-based)"""
        if not trades:
            return []
        
        # Group by wallet address
        wallet_groups = defaultdict(list)
        
        for trade in trades:
            if trade.wallet_address:
                wallet_groups[trade.wallet_address].append(trade)
        
        logger.info(f"✓ DEX: {len(wallet_groups)} unique wallets found")
        
        # Convert to entity format
        entities = []
        
        for wallet_addr, wallet_trades in wallet_groups.items():
            # Calculate stats
            total_volume = sum(t.amount for t in wallet_trades)
            total_value = sum(t.value_usd for t in wallet_trades)
            trade_count = len(wallet_trades)
            
            buy_volume = sum(t.amount for t in wallet_trades if t.trade_type == 'buy')
            sell_volume = sum(t.amount for t in wallet_trades if t.trade_type == 'sell')
            
            # 🔧 FIX: Use large number instead of inf for JSON compatibility
            buy_sell_ratio = buy_volume / sell_volume if sell_volume > 0 else 999.0
            
            # Calculate impact score
            volume_ratio = total_volume / candle.volume if candle.volume > 0 else 0
            impact_score = min(volume_ratio * 2.0, 1.0)  # Simplified
            
            # Classify wallet type
            wallet_type = self.classifier.classify(
                avg_trade_size=total_value / trade_count if trade_count > 0 else 0,
                trade_count=trade_count,
                size_consistency=0.7,  # Simplified
                timing_pattern='random',
                buy_sell_ratio=buy_sell_ratio,
                impact_score=impact_score
            )
            
            entity = {
                'wallet_id': wallet_addr,
                'wallet_address': wallet_addr,  # 🎯 REAL Address!
                'wallet_type': wallet_type,
                'impact_score': sanitize_float(impact_score),
                'total_volume': sanitize_float(total_volume),
                'total_value_usd': sanitize_float(total_value),
                'trade_count': trade_count,
                'avg_trade_size': sanitize_float(total_volume / trade_count if trade_count > 0 else 0),
                'volume_ratio': sanitize_float(volume_ratio),
                'buy_sell_ratio': sanitize_float(buy_sell_ratio),
                'blockchain': 'solana',  # or auto-detect
                'dex': exchange
            }
            
            entities.append(entity)
        
        # Sort by impact score
        entities.sort(key=lambda e: e['impact_score'], reverse=True)
        
        return entities[:top_n]
    
    def _calculate_correlation(
        self,
        cex_movers: List[Dict],
        dex_movers: List[Dict],
        cex_trades: List[Trade],
        dex_trades: List[Trade]
    ) -> Dict:
        """
        Calculate cross-exchange correlation
        
        Checks:
        1. Volume correlation (are volumes similar?)
        2. Timing correlation (who moved first?)
        3. Pattern matching (similar entity types?)
        """
        if not cex_movers or not dex_movers:
            return {
                'score': 0.0,
                'cex_led_by_seconds': 0,
                'volume_correlation': 0.0,
                'timing_score': 0.0,
                'pattern_matches': [],
                'conclusion': 'Insufficient data for correlation'
            }
        
        # 1. Volume Correlation
        cex_total_volume = sum(m['total_volume'] for m in cex_movers)
        dex_total_volume = sum(m['total_volume'] for m in dex_movers)
        
        max_volume = max(cex_total_volume, dex_total_volume)
        if max_volume > 0:
            volume_ratio = min(cex_total_volume, dex_total_volume) / max_volume
        else:
            volume_ratio = 0.0
        
        volume_correlation = sanitize_float(volume_ratio)
        
        # 2. Timing Correlation
        if cex_trades and dex_trades:
            cex_avg_time = sum((t.timestamp.timestamp() for t in cex_trades)) / len(cex_trades)
            dex_avg_time = sum((t.timestamp.timestamp() for t in dex_trades)) / len(dex_trades)
            
            time_diff = cex_avg_time - dex_avg_time  # Positive = CEX first
        else:
            time_diff = 0
        
        # 3. Pattern Matching
        pattern_matches = []
        
        for cex_mover in cex_movers[:5]:  # Top 5 CEX
            cex_type = cex_mover['wallet_type']
            cex_volume = cex_mover['total_volume']
            
            # Find similar DEX movers
            for dex_mover in dex_movers[:10]:  # Top 10 DEX
                dex_type = dex_mover['wallet_type']
                dex_volume = dex_mover['total_volume']
                
                # Check if similar
                type_match = cex_type == dex_type
                
                max_vol = max(cex_volume, dex_volume)
                if max_vol > 0:
                    volume_similar = abs(cex_volume - dex_volume) / max_vol < 0.3
                else:
                    volume_similar = False
                
                if type_match and volume_similar:
                    volume_diff = abs(cex_volume - dex_volume) / max_vol * 100 if max_vol > 0 else 0
                    pattern_matches.append({
                        'cex_entity': cex_mover['wallet_id'],
                        'dex_wallet': dex_mover['wallet_address'],
                        'type': cex_type,
                        'volume_diff_pct': sanitize_float(volume_diff),
                        'confidence': 0.7  # Simplified
                    })
        
        # 4. Overall Correlation Score
        timing_score = sanitize_float(max(0, 1.0 - abs(time_diff) / 300))  # 5 min = 0 score
        
        min_movers = min(len(cex_movers), len(dex_movers))
        pattern_score = len(pattern_matches) / min_movers if min_movers > 0 else 0.0
        pattern_score = sanitize_float(pattern_score)
        
        overall_score = sanitize_float(
            volume_correlation * 0.4 +
            timing_score * 0.3 +
            pattern_score * 0.3
        )
        
        # 5. Conclusion
        if overall_score > 0.7:
            conclusion = "Strong correlation - CEX and DEX activity aligned"
        elif overall_score > 0.4:
            conclusion = "Moderate correlation - some alignment detected"
        else:
            conclusion = "Weak correlation - independent activity"
        
        if time_diff > 60:
            conclusion += f" | CEX led by {int(time_diff)}s"
        elif time_diff < -60:
            conclusion += f" | DEX led by {int(abs(time_diff))}s"
        
        return {
            'score': overall_score,
            'cex_led_by_seconds': int(time_diff),
            'volume_correlation': volume_correlation,
            'timing_score': timing_score,
            'pattern_matches': pattern_matches,
            'conclusion': conclusion
        }
    
    def _format_entities_as_movers(
        self,
        entities: List[Union[TradingEntity, Dict]],
        include_trades: bool
    ) -> List[Dict]:
        """Format entities to mover format"""
        movers = []
        
        for entity in entities:
            if isinstance(entity, TradingEntity):
                mover = {
                    "wallet_id": entity.entity_id,
                    "wallet_type": entity.entity_type,
                    "impact_score": sanitize_float(entity.impact_score),
                    "impact_level": entity.impact_level,
                    "total_volume": sanitize_float(round(entity.total_volume, 4)),
                    "total_value_usd": sanitize_float(round(entity.total_value_usd, 2)),
                    "trade_count": entity.trade_count,
                    "avg_trade_size": sanitize_float(round(entity.avg_trade_size, 4)),
                    "volume_ratio": sanitize_float(round(entity.impact_components["volume_ratio"], 3)),
                    "confidence_score": sanitize_float(entity.confidence_score),
                }
            else:
                mover = entity
            
            movers.append(mover)
        
        return movers
    
    async def _fetch_mock_data(
        self,
        start_time: datetime,
        end_time: datetime,
        source: str
    ) -> Tuple[Candle, List[Trade]]:
        """Generate mock data for testing"""
        import random
        
        # 🔧 FIX: Ensure mock timestamps are timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
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
        base_price = 67500.0
        
        for i in range(30):
            current_time += timedelta(seconds=random.randint(5, 15))
            if current_time > end_time:
                break
            
            price = base_price + random.uniform(-100, 100)
            amount = random.uniform(0.1, 5.0)
            
            # Mock wallet for DEX
            wallet = None
            if source == 'dex':
                wallet = f"mock_wallet_{random.randint(1, 10)}"
            
            trade = Trade(
                timestamp=current_time,
                trade_type='buy' if random.random() > 0.5 else 'sell',
                amount=amount,
                price=price,
                value_usd=amount * price,
                trade_count=1,
                wallet_address=wallet,
                source=source
            )
            trades.append(trade)
        
        return candle, trades
    
    def _empty_hybrid_response(
        self,
        cex_exchange: str,
        dex_exchange: str,
        symbol: str,
        timeframe: str
    ) -> Dict:
        """Empty response when no data"""
        return {
            "candle": None,
            "cex_analysis": {
                "exchange": cex_exchange,
                "top_movers": [],
                "has_wallet_ids": False,
                "data_source": "pattern_based",
                "trade_count": 0
            },
            "dex_analysis": {
                "exchange": dex_exchange,
                "top_movers": [],
                "has_wallet_ids": True,
                "data_source": "on_chain",
                "trade_count": 0
            },
            "correlation": {
                'score': 0.0,
                'cex_led_by_seconds': 0,
                'volume_correlation': 0.0,
                'timing_score': 0.0,
                'pattern_matches': [],
                'conclusion': 'No data available'
            },
            "analysis_metadata": {
                "analysis_timestamp": datetime.now(timezone.utc),
                "processing_duration_ms": 0,
                "total_trades_analyzed": 0,
                "cex_entities_found": 0,
                "dex_wallets_found": 0,
                "exchanges": f"{cex_exchange}+{dex_exchange}",
                "symbol": symbol,
                "timeframe": timeframe
            }
        }
