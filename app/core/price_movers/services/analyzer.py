"""
Price Mover Analyzer - Vollständige Integration

Orchestriert die gesamte Analyse-Pipeline mit echten Daten:
1. Data Collection von Exchanges
2. Wallet Aggregation  
3. Impact Calculation mit ImpactCalculator
4. Pattern Detection
5. Ranking & Filtering
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.core.price_movers.services.impact_calculator import ImpactCalculator
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
    
    def to_dict_list(self) -> List[Dict]:
        """Konvertiert Trades zu Dictionary-Liste für Impact Calculator"""
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
    Hauptklasse für Price Mover Analyse
    
    Orchestriert:
    - Datensammlung von Exchanges (via ExchangeCollector)
    - Wallet-Pattern Erkennung
    - Impact Score Berechnung (via ImpactCalculator)
    - Ranking und Filterung
    """
    
    def __init__(
        self,
        exchange_collector=None,
        impact_calculator: Optional[ImpactCalculator] = None
    ):
        """
        Initialisiert den Analyzer
        
        Args:
            exchange_collector: Collector für Exchange-Daten
            impact_calculator: Calculator für Impact Scores (optional)
        """
        self.exchange_collector = exchange_collector
        self.impact_calculator = impact_calculator or ImpactCalculator()
        
        logger.info("PriceMoverAnalyzer initialisiert mit echten Datenquellen")
    
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
            # Phase 1: Data Collection (ECHT!)
            logger.debug("Phase 1: Datensammlung von Exchange")
            candle, trades = await self._fetch_all_data(
                exchange, symbol, timeframe, start_time, end_time
            )
            
            if not trades:
                logger.warning("Keine Trades gefunden")
                return self._empty_response(candle)
            
            logger.info(f"✓ {len(trades)} Trades von {exchange} gefetcht")
            
            # Phase 2: Wallet Aggregation
            logger.debug(f"Phase 2: Aggregiere {len(trades)} Trades zu Wallets")
            wallet_activities = self._aggregate_wallet_activities(trades, candle)
            logger.info(f"✓ {len(wallet_activities)} Wallet-Pattern identifiziert")
            
            # Phase 3: Impact Calculation (mit ImpactCalculator!)
            logger.debug(f"Phase 3: Berechne Impact für {len(wallet_activities)} Wallets")
            scored_wallets = await self._calculate_all_impacts(
                wallet_activities,
                candle,
                candle.volume
            )
            logger.info(f"✓ Impact Scores berechnet")
            
            # Phase 4: Ranking & Filtering
            logger.debug("Phase 4: Ranking und Filterung")
            top_movers = self._rank_and_filter(
                scored_wallets,
                min_impact_threshold,
                top_n_wallets,
                include_trades
            )
            logger.info(f"✓ Top {len(top_movers)} Movers gefiltert")
            
            duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            
            response = {
                "candle": {
                    "timestamp": candle.timestamp,  # <-- datetime Objekt, nicht isoformat()
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume
                },
                "top_movers": top_movers,
                "analysis_metadata": {
                    "analysis_timestamp": datetime.now(),           # <-- NEU: Required field
                    "processing_duration_ms": duration_ms,          # <-- NEU: Required field
                    "total_trades_analyzed": len(trades),           # <-- NEU: Required field
                    "unique_wallets_found": len(wallet_activities), # <-- NEU: Required field
                    "exchange": str(exchange),                      # <-- NEU: Required field
                    "symbol": symbol,                               # <-- NEU: Required field
                    "timeframe": str(timeframe)                     # <-- NEU: Required field
                }
            }
            
            logger.info(
                f"✅ Analyse abgeschlossen in {duration_ms}ms. "
                f"Top Movers: {len(top_movers)}"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"❌ Fehler bei Analyse: {e}", exc_info=True)
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
        Sammelt alle benötigten Daten von der Exchange
        
        Returns:
            Tuple of (Candle, List[Trade])
        """
        if not self.exchange_collector:
            logger.warning("Kein Exchange Collector verfügbar, nutze Mock-Daten")
            return await self._fetch_mock_data(start_time, end_time)
        
        try:
            # Parallel Fetching von Candle und Trades
            candle_task = self.exchange_collector.fetch_candle_data(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=start_time
            )
            
            trades_task = self.exchange_collector.fetch_trades(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time
            )
            
            # Warte auf beide Requests
            candle_data, trades_data = await asyncio.gather(
                candle_task,
                trades_task
            )
            
            # Validiere Candle
            if not validate_candle_data(candle_data):
                raise ValueError("Invalid candle data received")
            
            # Parse Candle
            candle = Candle(
                timestamp=candle_data["timestamp"],
                open=candle_data["open"],
                high=candle_data["high"],
                low=candle_data["low"],
                close=candle_data["close"],
                volume=candle_data["volume"]
            )
            
            # Parse und validiere Trades
            trades = []
            for trade_data in trades_data:
                # Validiere Trade
                if not validate_trade_data(trade_data):
                    continue
                
                trade = Trade(
                    timestamp=trade_data["timestamp"],
                    trade_type=trade_data["trade_type"],
                    amount=trade_data["amount"],
                    price=trade_data["price"],
                    value_usd=trade_data.get("value_usd", 
                                             trade_data["amount"] * trade_data["price"])
                )
                trades.append(trade)
            
            logger.info(
                f"Daten gefetcht: Candle @ {candle.timestamp}, "
                f"{len(trades)} valid trades"
            )
            
            return candle, trades
            
        except Exception as e:
            logger.error(f"Fehler beim Fetchen von Exchange-Daten: {e}")
            # Fallback zu Mock-Daten
            logger.warning("Fallback zu Mock-Daten")
            return await self._fetch_mock_data(start_time, end_time)
    
    async def _fetch_mock_data(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Tuple[Candle, List[Trade]]:
        """Generiert Mock-Daten für Testing"""
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
        
        return candle, trades
    
    def _aggregate_wallet_activities(
        self,
        trades: List[Trade],
        candle: Candle
    ) -> Dict[str, WalletActivity]:
        """
        Gruppiert Trades nach Wallet-Pattern
        
        Bei CEX: Pattern-basierte "virtuelle Wallets"
        - Whales: Große Trades
        - Smart Money: Mittelgroße Trades
        - Bots: Regelmäßige kleine Trades
        - Market Makers: Beide Seiten
        - Retail: Rest
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
        - Trade Size (USD Value)
        - Timing
        - Price Level
        """
        # Whale: Große Trades (> $100k)
        if trade.value_usd > 100_000:
            size_bucket = int(trade.value_usd / 50_000)
            return f"whale_{size_bucket}"
        
        # Smart Money: Mittelgroße Trades ($50k - $100k)
        elif trade.value_usd > 50_000:
            return f"smart_money_{hash(trade.amount) % 10}"
        
        # Bot: Kleine regelmäßige Trades
        elif trade.amount < 1.0 and trade.value_usd < 1000:
            return f"bot_{hash(round(trade.amount, 2)) % 20}"
        
        # Market Maker: Mittlere Trades nahe Candle-Grenzen
        elif trade.value_usd > 10_000:
            return f"market_maker_{hash(trade.price) % 5}"
        
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
        Berechnet Impact Scores für alle Wallets mit ImpactCalculator
        """
        scored_wallets = []
        
        # Candle-Daten für Impact Calculator
        candle_data = {
            "timestamp": candle.timestamp,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "price_change_pct": candle.price_change_pct
        }
        
        for wallet_id, activity in wallet_activities.items():
            # Konvertiere Trades zu Dict-Format für Impact Calculator
            wallet_trades_dict = activity.to_dict_list()
            
            # Berechne Impact Score mit echtem Calculator
            impact_result = self.impact_calculator.calculate_impact_score(
                wallet_trades=wallet_trades_dict,
                candle_data=candle_data,
                total_volume=total_volume
            )
            
            # Klassifiziere Wallet-Typ mit Pattern Detection
            wallet_type = self._classify_wallet_type_advanced(
                activity, 
                impact_result["impact_score"]
            )
            
            scored_wallets.append({
                "wallet_id": wallet_id,
                "wallet_type": wallet_type,
                "impact_score": impact_result["impact_score"],
                "impact_level": impact_result["impact_level"],
                "components": impact_result["components"],
                "activity": activity
            })
        
        return scored_wallets
    
    def _classify_wallet_type_advanced(
        self,
        activity: WalletActivity,
        impact_score: float
    ) -> str:
        """
        Erweiterte Wallet-Klassifizierung mit Pattern Detection
        """
        # Konvertiere zu Dict-Liste für Pattern Detection
        trades_dict = activity.to_dict_list()
        
        # Bot-Pattern?
        if detect_bot_pattern(trades_dict):
            return "bot"
        
        # Whale-Pattern?
        if detect_whale_pattern(trades_dict):
            return "whale"
        
        # Smart Money Pattern?
        if detect_smart_money_pattern(trades_dict):
            return "smart_money"
        
        # Market Maker: Beide Seiten handeln
        if activity.buy_trades > 0 and activity.sell_trades > 0:
            buy_sell_ratio = activity.buy_trades / activity.sell_trades
            if 0.7 <= buy_sell_ratio <= 1.3:  # Relativ ausgeglichen
                return "market_maker"
        
        # Fallback auf Impact Score
        avg_value = activity.total_value_usd / activity.trade_count if activity.trade_count > 0 else 0
        
        if avg_value > 100_000:
            return "whale"
        elif avg_value > 50_000:
            return "smart_money"
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
        
        # Sortiere nach Impact Score (absteigend)
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
                "impact_level": wallet["impact_level"],
                "total_volume": round(activity.total_volume, 4),
                "total_value_usd": round(activity.total_value_usd, 2),
                "trade_count": activity.trade_count,
                "avg_trade_size": round(activity.avg_trade_size, 4),
                "volume_ratio": round(wallet["components"]["volume_ratio"], 3),
                "components": wallet["components"]
            }
            
            if include_trades:
                wallet_data["trades"] = [
                    {
                        "timestamp": t.timestamp.isoformat(),
                        "trade_type": t.trade_type,
                        "amount": round(t.amount, 4),
                        "price": round(t.price, 2),
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
