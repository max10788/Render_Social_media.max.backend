"""
Price Mover Analyzer - Kern-Logik

Orchestriert die gesamte Analyse-Pipeline:
1. Data Collection
2. Wallet Aggregation  
3. Impact Calculation
4. Ranking & Filtering
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Einzelner Trade"""
    timestamp: datetime
    trade_type: str  # 'buy' oder 'sell'
    amount: float
    price: float
    value_usd: float


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
    """Wallet-Aktivitäten Container"""
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


class PriceMoverAnalyzer:
    """
    Hauptklasse für Price Mover Analyse
    
    Orchestriert:
    - Datensammlung von Exchanges
    - Wallet-Pattern Erkennung
    - Impact Score Berechnung
    - Ranking und Filterung
    """
    
    def __init__(
        self,
        exchange_collector=None,
        impact_calculator=None,
        wallet_classifier=None
    ):
        """
        Initialisiert den Analyzer
        
        Args:
            exchange_collector: Collector für Exchange-Daten
            impact_calculator: Calculator für Impact Scores
            wallet_classifier: Classifier für Wallet-Typen
        """
        self.exchange_collector = exchange_collector
        self.impact_calculator = impact_calculator
        self.wallet_classifier = wallet_classifier
        
        logger.info("PriceMoverAnalyzer initialisiert")
    
    async def analyze_candle(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        min_impact_threshold: float = 0.05,
        top_n_wallets: int = 10,
        include_trades: bool = False
    ) -> Dict:
        """
        Haupt-Analyse-Methode
        
        Args:
            exchange: Exchange Name (bitget/binance/kraken)
            symbol: Trading Pair (z.B. BTC/USDT)
            timeframe: Candle Timeframe (z.B. 5m)
            start_time: Start der Analyse
            end_time: Ende der Analyse
            min_impact_threshold: Minimaler Impact Score
            top_n_wallets: Anzahl Top Wallets
            include_trades: Trades in Response inkludieren
            
        Returns:
            Analysis Response Dictionary
        """
        start = datetime.now()
        logger.info(
            f"Starte Analyse: {exchange} {symbol} {timeframe} "
            f"({start_time} - {end_time})"
        )
        
        try:
            # Phase 1: Data Collection
            logger.debug("Phase 1: Datensammlung")
            candle, trades = await self._fetch_all_data(
                exchange, symbol, timeframe, start_time, end_time
            )
            
            if not trades:
                logger.warning("Keine Trades gefunden")
                return self._empty_response(candle)
            
            # Phase 2: Wallet Aggregation
            logger.debug(f"Phase 2: Aggregiere {len(trades)} Trades")
            wallet_activities = self._aggregate_wallet_activities(trades, candle)
            
            # Phase 3: Impact Calculation
            logger.debug(f"Phase 3: Berechne Impact für {len(wallet_activities)} Wallets")
            scored_wallets = await self._calculate_all_impacts(
                wallet_activities,
                candle,
                candle.volume
            )
            
            # Phase 4: Ranking & Filtering
            logger.debug("Phase 4: Ranking und Filterung")
            top_movers = self._rank_and_filter(
                scored_wallets,
                min_impact_threshold,
                top_n_wallets,
                include_trades
            )
            
            # Build Response
            duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            
            response = {
                "candle": {
                    "timestamp": candle.timestamp.isoformat(),
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume,
                    "price_change_pct": round(candle.price_change_pct, 2)
                },
                "top_movers": top_movers,
                "analysis_metadata": {
                    "total_unique_wallets": len(wallet_activities),
                    "total_volume": candle.volume,
                    "total_trades": len(trades),
                    "analysis_duration_ms": duration_ms,
                    "data_sources": [f"{exchange}_trades", f"{exchange}_candles"],
                    "timestamp": datetime.now().isoformat()
                }
            }
            
            logger.info(
                f"Analyse abgeschlossen in {duration_ms}ms. "
                f"Top Movers: {len(top_movers)}"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Fehler bei Analyse: {e}", exc_info=True)
            raise
    
    async def _fetch_all_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[Candle, List[Trade]]:
        """
        Sammelt alle benötigten Daten parallel
        
        Returns:
            Tuple of (Candle, List[Trade])
        """
        # TODO: Integration mit Exchange Collectors
        # Placeholder für jetzt
        
        # Simulierte Candle
        candle = Candle(
            timestamp=start_time,
            open=67500.0,
            high=67800.0,
            low=67450.0,
            close=67750.0,
            volume=1234.56
        )
        
        # Simulierte Trades
        trades = self._generate_mock_trades(start_time, end_time)
        
        return candle, trades
    
    def _generate_mock_trades(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[Trade]:
        """Generiert Mock-Trades für Testing"""
        import random
        from datetime import timedelta
        
        trades = []
        current_time = start_time
        base_price = 67500.0
        
        for i in range(50):
            current_time += timedelta(seconds=random.randint(5, 15))
            if current_time > end_time:
                break
            
            price = base_price + random.uniform(-100, 100)
            amount = random.uniform(0.1, 5.0)
            
            trade = Trade(
                timestamp=current_time,
                trade_type='buy' if random.random() > 0.5 else 'sell',
                amount=amount,
                price=price,
                value_usd=amount * price
            )
            trades.append(trade)
        
        return trades
    
    def _aggregate_wallet_activities(
        self,
        trades: List[Trade],
        candle: Candle
    ) -> Dict[str, WalletActivity]:
        """
        Gruppiert Trades nach Wallet-Pattern
        
        Bei CEX: Pattern-basierte "virtuelle Wallets"
        """
        wallet_map: Dict[str, List[Trade]] = defaultdict(list)
        
        for trade in trades:
            # Identifiziere Wallet-Pattern
            wallet_id = self._identify_wallet_pattern(trade, candle)
            wallet_map[wallet_id].append(trade)
        
        # Konvertiere zu WalletActivity Objekten
        return {
            wallet_id: WalletActivity(wallet_id=wallet_id, trades=trade_list)
            for wallet_id, trade_list in wallet_map.items()
        }
    
    def _identify_wallet_pattern(self, trade: Trade, candle: Candle) -> str:
        """
        Erstellt Pattern-basierte Wallet-ID
        
        Clustering basierend auf:
        - Trade Size
        - Timing
        - Price Level
        """
        # Whale: Große Trades (> $100k)
        if trade.value_usd > 100_000:
            size_bucket = int(trade.value_usd / 50_000)
            return f"whale_{size_bucket}"
        
        # Smart Money: Mittelgroße Trades (> $50k)
        elif trade.value_usd > 50_000:
            return f"smart_money_{hash(trade.amount) % 10}"
        
        # Bot: Regelmäßige kleine Trades
        elif trade.amount < 1.0:
            return f"bot_{hash(round(trade.amount, 2)) % 20}"
        
        # Retail: Rest
        else:
            return f"retail_{hash(trade.value_usd) % 50}"
    
    async def _calculate_all_impacts(
        self,
        wallet_activities: Dict[str, WalletActivity],
        candle: Candle,
        total_volume: float
    ) -> List[Dict]:
        """
        Berechnet Impact Scores für alle Wallets
        """
        scored_wallets = []
        
        for wallet_id, activity in wallet_activities.items():
            # Berechne Impact Score (vereinfacht)
            volume_ratio = activity.total_volume / total_volume if total_volume > 0 else 0
            timing_score = self._calculate_timing_score(activity, candle)
            
            impact_score = (volume_ratio * 0.6) + (timing_score * 0.4)
            
            # Klassifiziere Wallet-Typ
            wallet_type = self._classify_wallet_type(activity, impact_score)
            
            scored_wallets.append({
                "wallet_id": wallet_id,
                "wallet_type": wallet_type,
                "impact_score": round(impact_score, 3),
                "activity": activity
            })
        
        return scored_wallets
    
    def _calculate_timing_score(
        self,
        activity: WalletActivity,
        candle: Candle
    ) -> float:
        """Berechnet Timing Score"""
        if not activity.trades:
            return 0.0
        
        candle_duration = 300  # 5 Minuten
        early_trades = sum(
            1 for t in activity.trades
            if (t.timestamp - candle.timestamp).total_seconds() < candle_duration * 0.3
        )
        
        timing_score = early_trades / len(activity.trades)
        
        # Bonus für große Preisbewegungen
        if abs(candle.price_change_pct) > 0.5:
            timing_score *= 1.2
        
        return min(timing_score, 1.0)
    
    def _classify_wallet_type(
        self,
        activity: WalletActivity,
        impact_score: float
    ) -> str:
        """Klassifiziert Wallet-Typ"""
        avg_value = activity.total_value_usd / activity.trade_count if activity.trade_count > 0 else 0
        
        if avg_value > 100_000:
            return "whale"
        elif avg_value > 50_000:
            return "smart_money"
        elif activity.trade_count > 10 and abs(activity.buy_trades - activity.sell_trades) < 2:
            return "market_maker"
        elif activity.trade_count > 10:
            return "bot"
        else:
            return "retail"
    
    def _rank_and_filter(
        self,
        scored_wallets: List[Dict],
        min_threshold: float,
        top_n: int,
        include_trades: bool
    ) -> List[Dict]:
        """
        Filtert und ranked Wallets
        """
        # Filter nach Threshold
        filtered = [
            w for w in scored_wallets
            if w["impact_score"] >= min_threshold
        ]
        
        # Sortiere nach Impact Score
        filtered.sort(key=lambda x: x["impact_score"], reverse=True)
        
        # Top N
        top_wallets = filtered[:top_n]
        
        # Format Response
        result = []
        for wallet in top_wallets:
            activity = wallet["activity"]
            
            wallet_data = {
                "wallet_id": wallet["wallet_id"],
                "wallet_type": wallet["wallet_type"],
                "impact_score": wallet["impact_score"],
                "total_volume": round(activity.total_volume, 2),
                "total_value_usd": round(activity.total_value_usd, 2),
                "trade_count": activity.trade_count,
                "avg_trade_size": round(activity.avg_trade_size, 2),
                "timing_score": round(self._calculate_timing_score(activity, None), 2),
                "volume_ratio": 0.0  # TODO: Calculate
            }
            
            if include_trades:
                wallet_data["trades"] = [
                    {
                        "timestamp": t.timestamp.isoformat(),
                        "trade_type": t.trade_type,
                        "amount": round(t.amount, 2),
                        "price": round(t.price, 2),
                        "price_impact_est": 0.0,  # TODO: Calculate
                        "value_usd": round(t.value_usd, 2)
                    }
                    for t in activity.trades
                ]
            
            result.append(wallet_data)
        
        return result
    
    def _empty_response(self, candle: Candle) -> Dict:
        """Leere Response wenn keine Daten"""
        return {
            "candle": {
                "timestamp": candle.timestamp.isoformat(),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
                "price_change_pct": round(candle.price_change_pct, 2)
            },
            "top_movers": [],
            "analysis_metadata": {
                "total_unique_wallets": 0,
                "total_volume": candle.volume,
                "total_trades": 0,
                "analysis_duration_ms": 0,
                "data_sources": [],
                "timestamp": datetime.now().isoformat()
            }
        }
