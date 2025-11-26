"""
Impact Calculator - Berechnet Impact Scores für Wallets

Berücksichtigt:
- Volume Ratio (Anteil am Gesamt-Volume)
- Timing Score (Wann wurden Trades platziert)
- Size Impact (Größe relativ zum Orderbook)
- Price Correlation (Korrelation mit Preisbewegung)
- Slippage Caused (Verursachter Slippage)
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import statistics

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.core.price_movers.utils.constants import (
    IMPACT_SCORE_WEIGHTS,
    TIMING_WINDOW_BEFORE_MOVE,
    TIMING_WINDOW_AFTER_MOVE,
    SIGNIFICANT_PRICE_MOVE_PCT,
)


logger = logging.getLogger(__name__)


@dataclass
class ImpactComponents:
    """Container für Impact Score Komponenten"""
    volume_ratio: float
    timing_score: float
    size_impact: float
    price_correlation: float
    slippage_caused: float
    
    @property
    def total_score(self) -> float:
        """Berechnet gewichteten Gesamt-Score"""
        return (
            self.volume_ratio * IMPACT_SCORE_WEIGHTS["volume_ratio"] +
            self.timing_score * IMPACT_SCORE_WEIGHTS["timing_score"] +
            self.size_impact * IMPACT_SCORE_WEIGHTS["size_impact"] +
            self.price_correlation * IMPACT_SCORE_WEIGHTS["price_correlation"] +
            self.slippage_caused * IMPACT_SCORE_WEIGHTS["slippage_caused"]
        )


class ImpactCalculator:
    """
    Berechnet Impact Scores für Wallet-Aktivitäten
    
    Score Range: 0.0 - 1.0
    - 0.0 - 0.2: Geringer Impact
    - 0.2 - 0.5: Mittlerer Impact
    - 0.5 - 0.8: Hoher Impact
    - 0.8 - 1.0: Sehr hoher Impact
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialisiert Impact Calculator
        
        Args:
            config: Optionale Konfiguration
        """
        self.config = config or {}
        logger.info("ImpactCalculator initialisiert")
    
    def calculate_impact_score(
            self,
            wallet_trades: List[Dict[str, Any]],
            candle_data: Dict[str, Any],
            total_volume: float, # <-- Dies ist der entscheidende Parameter
            all_trades: Optional[List[Dict[str, Any]]] = None,
            apply_liquidity_multipliers: bool = False  # ✅ NEU
        ) -> Dict[str, Any]:
            """
            Berechnet vollständigen Impact Score für ein Wallet
            
            Args:
                wallet_trades: Trades des Wallets
                candle_data: Candle-Informationen
                total_volume: Gesamt-Volume der Candle
                all_trades: Alle Trades (für Kontext)
                apply_liquidity_multipliers: Ob Liquidity Event Multipliers angewendet werden
                
            Returns:
                Dictionary mit Impact Score und Komponenten
            """
            # --- NEUES DETAIL-LOGGING ---
            logger.debug(f"--- calculate_impact_score START ---")
            logger.debug(f"Wallet ID: {wallet_trades[0].get('wallet_address', 'unknown') if wallet_trades else 'no_trades'}")
            logger.debug(f"Anzahl Trades: {len(wallet_trades)}")
            logger.debug(f"Übergebenes total_volume: {total_volume}")
            logger.debug(f"apply_liquidity_multipliers: {apply_liquidity_multipliers}")
            # --- ENDE NEUES DETAIL-LOGGING ---

            if not wallet_trades:
                logger.debug("Keine Trades für Wallet vorhanden -> Zero Impact")
                result = self._zero_impact()
                logger.debug(f"--- calculate_impact_score END (Zero Impact) ---")
                return result
            
            # Berechne volume ratio
            volume_ratio = self._calculate_volume_ratio(wallet_trades, total_volume)
            
            # ✅ NEU: Apply Liquidity Multipliers wenn aktiviert
            if apply_liquidity_multipliers:
                weighted_volume_ratio = 0.0
                has_liquidity_events = False
                liquidity_event_count = 0
                
                for trade in wallet_trades:
                    trade_volume = trade.get("amount", 0.0)
                    trade_ratio = trade_volume / total_volume if total_volume > 0 else 0
                    
                    # Get multiplier
                    multiplier = self.calculate_liquidity_multiplier(
                        trade=trade,
                        candle_volume=total_volume
                    )
                    
                    # Track liquidity events
                    tx_type = trade.get('transaction_type', 'SWAP')
                    if tx_type in ['ADD_LIQUIDITY', 'REMOVE_LIQUIDITY']:
                        has_liquidity_events = True
                        liquidity_event_count += 1
                    
                    weighted_volume_ratio += trade_ratio * multiplier
                
                # Normalize und cap bei 1.0
                volume_ratio = min(weighted_volume_ratio, 1.0)
                
                logger.debug(
                    f"Liquidity-weighted volume ratio: {volume_ratio:.3f} "
                    f"(raw: {sum(t.get('amount', 0) for t in wallet_trades) / total_volume:.3f}) "
                    f"- {liquidity_event_count} liquidity events"
                )
            
            # Berechne alle anderen Komponenten (bleibt gleich)
            timing_score = self._calculate_timing_score(wallet_trades, candle_data)
            size_impact = self._calculate_size_impact(wallet_trades, candle_data)
            price_correlation = self._calculate_price_correlation(
                wallet_trades, candle_data
            )
            slippage_caused = self._calculate_slippage_score(
                wallet_trades, candle_data
            )
            
            # Erstelle Impact Components
            components = ImpactComponents(
                volume_ratio=volume_ratio,
                timing_score=timing_score,
                size_impact=size_impact,
                price_correlation=price_correlation,
                slippage_caused=slippage_caused
            )
            
            # Gesamt-Score
            total_score = components.total_score
            
            # ✅ NEU: Check for liquidity events
            has_liquidity_events = any(
                t.get('transaction_type') in ['ADD_LIQUIDITY', 'REMOVE_LIQUIDITY']
                for t in wallet_trades
            )
            
            result = {
                "impact_score": round(total_score, 3),
                "components": {
                    "volume_ratio": round(volume_ratio, 3),
                    "timing_score": round(timing_score, 3),
                    "size_impact": round(size_impact, 3),
                    "price_correlation": round(price_correlation, 3),
                    "slippage_caused": round(slippage_caused, 3)
                },
                "impact_level": self._get_impact_level(total_score),
                "has_liquidity_events": has_liquidity_events  # ✅ NEU
            }

            # --- NEUES DETAIL-LOGGING ---
            logger.debug(f"Ergebnis für Wallet: {result}")
            logger.debug(f"--- calculate_impact_score END ---")
            # --- ENDE NEUES DETAIL-LOGGING ---
            
            log_suffix = ""
            if apply_liquidity_multipliers and has_liquidity_events:
                log_suffix = " [LIQUIDITY-WEIGHTED]"
            
            logger.debug(
                f"Impact Score berechnet: {total_score:.3f} "
                f"(vol={volume_ratio:.2f}, timing={timing_score:.2f}, "
                f"size={size_impact:.2f}, corr={price_correlation:.2f}, "
                f"slip={slippage_caused:.2f}){log_suffix}"
            )
            
            return result
    
    def _calculate_volume_ratio(
        self, 
        wallet_trades: List[Dict], 
        total_volume: float
    ) -> float:
        """Berechne Volume Ratio - FIXED: USD/USD statt SOL/USD"""
        try:
            if not wallet_trades or total_volume <= 0:
                return 0.0
            
            # ✅ FIX: Berechne Wallet Volume IN USD (amount * price)
            wallet_volume_usd = sum(
                float(t.get('amount', 0)) * float(t.get('price', 0))
                for t in wallet_trades
            )
            
            # DEBUG
            logger.debug(
                f"💰 Volume: Wallet=${wallet_volume_usd:.2f}, "
                f"Total=${total_volume:.2f}, "
                f"Ratio={wallet_volume_usd/total_volume:.4f}"
            )
            
            volume_ratio = wallet_volume_usd / total_volume
            
            # Sanity check
            if volume_ratio > 1.0:
                logger.warning(f"⚠️ Volume ratio > 1.0, capping at 1.0")
                volume_ratio = 1.0
            
            return volume_ratio
            
        except Exception as e:
            logger.error(f"❌ Volume ratio error: {e}")
            return 0.0
    
    def _calculate_timing_score(
        self,
        wallet_trades: List[Dict[str, Any]],
        candle_data: Dict[str, Any]
    ) -> float:
        """
        Berechnet Timing Score
        
        Bewertet:
        - Trades VOR großen Preisbewegungen (höhere Score)
        - Frühe Trades in der Candle
        - Konzentration der Trades
        
        Args:
            wallet_trades: Trades des Wallets
            candle_data: Candle-Daten
            
        Returns:
            Timing Score (0-1)
        """
        if not wallet_trades:
            return 0.0
        
        candle_start = candle_data.get("timestamp")
        if isinstance(candle_start, str):
            candle_start = datetime.fromisoformat(candle_start.replace('Z', '+00:00'))
        
        price_change_pct = candle_data.get("price_change_pct", 0.0)
        
        # Berechne zeitliche Verteilung
        timestamps = []
        for trade in wallet_trades:
            ts = trade.get("timestamp")
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            timestamps.append(ts)
        
        if not timestamps:
            return 0.0
        
        # Score 1: Frühe Trades (erstes Drittel der Candle)
        candle_duration = 300  # 5 Minuten (Standard)
        early_trades = sum(
            1 for ts in timestamps
            if (ts - candle_start).total_seconds() < candle_duration * 0.33
        )
        early_ratio = early_trades / len(timestamps)
        
        # Score 2: Konzentration (weniger Streuung = höherer Score)
        if len(timestamps) > 1:
            time_diffs = [
                (timestamps[i+1] - timestamps[i]).total_seconds()
                for i in range(len(timestamps) - 1)
            ]
            avg_diff = statistics.mean(time_diffs)
            concentration = 1.0 / (1.0 + avg_diff / 60.0)  # Normalisiert auf Minuten
        else:
            concentration = 1.0
        
        # Score 3: Timing vor großer Bewegung
        movement_timing = 0.0
        if abs(price_change_pct) > SIGNIFICANT_PRICE_MOVE_PCT:
            # Bonus wenn Trades VOR der Bewegung waren
            avg_timestamp = sum(
                (ts - candle_start).total_seconds() for ts in timestamps
            ) / len(timestamps)
            
            # Höherer Score für frühere Trades bei großen Bewegungen
            movement_timing = max(0, 1.0 - (avg_timestamp / candle_duration))
        
        # Gewichtete Kombination
        timing_score = (
            early_ratio * 0.4 +
            concentration * 0.3 +
            movement_timing * 0.3
        )
        
        return min(timing_score, 1.0)
    
    def _calculate_size_impact(
        self,
        wallet_trades: List[Dict[str, Any]],
        candle_data: Dict[str, Any]
    ) -> float:
        """
        Berechnet Size Impact Score
        
        Große Trades haben mehr Impact
        
        Args:
            wallet_trades: Trades des Wallets
            candle_data: Candle-Daten
            
        Returns:
            Size Impact (0-1)
        """
        if not wallet_trades:
            return 0.0
        
        # Durchschnittliche Trade-Größe
        avg_trade_size = sum(
            trade.get("value_usd", 0.0) for trade in wallet_trades
        ) / len(wallet_trades)
        
        # Größte Trade
        max_trade_size = max(
            trade.get("value_usd", 0.0) for trade in wallet_trades
        )
        
        # Normalisiere auf bekannte Thresholds
        # $10k = 0.2, $50k = 0.5, $100k = 0.7, $500k+ = 1.0
        def normalize_size(size: float) -> float:
            if size < 10_000:
                return size / 50_000
            elif size < 50_000:
                return 0.2 + (size - 10_000) / 133_333
            elif size < 100_000:
                return 0.5 + (size - 50_000) / 250_000
            elif size < 500_000:
                return 0.7 + (size - 100_000) / 1_333_333
            else:
                return 1.0
        
        avg_score = normalize_size(avg_trade_size)
        max_score = normalize_size(max_trade_size)
        
        # Gewichtete Kombination
        size_impact = (avg_score * 0.6) + (max_score * 0.4)
        
        return min(size_impact, 1.0)
    
    def _calculate_price_correlation(
        self,
        wallet_trades: List[Dict[str, Any]],
        candle_data: Dict[str, Any]
    ) -> float:
        """
        Berechnet Korrelation zwischen Trades und Preisbewegung
        
        Args:
            wallet_trades: Trades des Wallets
            candle_data: Candle-Daten
            
        Returns:
            Price Correlation (0-1)
        """
        if not wallet_trades:
            return 0.0
        
        price_change_pct = candle_data.get("price_change_pct", 0.0)
        
        # Zähle Buy vs Sell Trades
        buy_volume = sum(
            trade.get("amount", 0.0)
            for trade in wallet_trades
            if trade.get("trade_type") == "buy"
        )
        sell_volume = sum(
            trade.get("amount", 0.0)
            for trade in wallet_trades
            if trade.get("trade_type") == "sell"
        )
        
        total_volume_from_trades = buy_volume + sell_volume
        if total_volume_from_trades == 0:
            return 0.0
        
        # Buy/Sell Ratio
        buy_ratio = buy_volume / total_volume_from_trades
        
        # Korrelation mit Preisbewegung
        if price_change_pct > 0:
            # Preis stieg: Mehr Buys = höhere Korrelation
            correlation = buy_ratio
        elif price_change_pct < 0:
            # Preis fiel: Mehr Sells = höhere Korrelation
            correlation = 1.0 - buy_ratio
        else:
            # Keine Bewegung
            correlation = 0.5
        
        # Skaliere mit Größe der Preisbewegung
        movement_scale = min(abs(price_change_pct) / 2.0, 1.0)
        scaled_correlation = correlation * movement_scale
        
        return min(scaled_correlation, 1.0)
    
    def _calculate_slippage_score(
        self,
        wallet_trades: List[Dict[str, Any]],
        candle_data: Dict[str, Any]
    ) -> float:
        """
        Schätzt verursachten Slippage
        
        Große Trades in kurzer Zeit verursachen mehr Slippage
        
        Args:
            wallet_trades: Trades des Wallets
            candle_data: Candle-Daten
            
        Returns:
            Slippage Score (0-1)
        """
        if not wallet_trades or len(wallet_trades) < 2:
            return 0.0
        
        # Volatilität der Candle
        high = candle_data.get("high", 0.0)
        low = candle_data.get("low", 0.0)
        avg_price = (high + low) / 2
        
        if avg_price == 0:
            return 0.0
        
        volatility_pct = ((high - low) / avg_price) * 100
        
        # Durchschnittliche Trade-Größe relativ zur Candle
        total_trade_volume = sum(
            trade.get("amount", 0.0) for trade in wallet_trades
        )
        candle_volume = candle_data.get("volume", 0.0)
        
        if candle_volume == 0:
            return 0.0
        
        volume_ratio = total_trade_volume / candle_volume
        
        # Zeitliche Konzentration
        timestamps = [
            trade.get("timestamp") for trade in wallet_trades
        ]
        if isinstance(timestamps[0], str):
            timestamps = [
                datetime.fromisoformat(ts.replace('Z', '+00:00'))
                for ts in timestamps
            ]
        
        time_span = (max(timestamps) - min(timestamps)).total_seconds()
        concentration = 1.0 / (1.0 + time_span / 60.0)  # Normalisiert auf Minuten
        
        # Slippage Score = Volumen * Konzentration * Volatilität
        slippage_score = min(volume_ratio * 2.0, 1.0) * concentration * min(volatility_pct / 2.0, 1.0)
        
        return min(slippage_score, 1.0)

    def calculate_liquidity_multiplier(
            self,
            trade: Dict[str, Any],
            candle_volume: float
        ) -> float:
            """
            Berechnet Liquidity Event Multiplier
            
            KRITISCH: Liquidity Events haben 2-5x höheren Impact!
            - REMOVE_LIQUIDITY: 2-5x (erhöht Slippage für alle anderen)
            - ADD_LIQUIDITY: 1.5-2.5x (reduziert Slippage)
            
            Args:
                trade: Trade dict mit transaction_type und liquidity_delta
                candle_volume: Candle volume für Kontext
                
            Returns:
                Impact multiplier (1.0-5.0)
            """
            tx_type = trade.get('transaction_type', 'SWAP')
            
            # Normale SWAPs = 1.0x
            if tx_type not in ['ADD_LIQUIDITY', 'REMOVE_LIQUIDITY']:
                return 1.0
            
            liquidity_delta = trade.get('liquidity_delta', 0)
            
            if liquidity_delta == 0 or candle_volume == 0:
                return 1.0
            
            # Liquidity als % vom Volume
            liquidity_ratio = liquidity_delta / candle_volume
            
            if tx_type == 'REMOVE_LIQUIDITY':
                # Liquidity Removal: 2-5x Impact (erhöht Slippage drastisch)
                if liquidity_ratio > 0.10:  # >10% of volume - KRITISCH!
                    logger.debug(
                        f"💧 CRITICAL REMOVE_LIQUIDITY: {liquidity_delta:.2f} "
                        f"({liquidity_ratio*100:.1f}% of volume) → 5.0x impact"
                    )
                    return 5.0
                elif liquidity_ratio > 0.05:  # >5%
                    return 4.0
                elif liquidity_ratio > 0.02:  # >2%
                    return 3.0
                else:
                    return 2.0  # Mindestens 2x
            
            elif tx_type == 'ADD_LIQUIDITY':
                # Liquidity Addition: 1.5-2.5x Impact (reduziert Slippage)
                if liquidity_ratio > 0.10:
                    logger.debug(
                        f"💧 LARGE ADD_LIQUIDITY: {liquidity_delta:.2f} "
                        f"({liquidity_ratio*100:.1f}% of volume) → 2.5x impact"
                    )
                    return 2.5
                elif liquidity_ratio > 0.05:
                    return 2.0
                else:
                    return 1.5
            
            return 1.0

    def _get_impact_level(self, score: float) -> str:
        """
        Konvertiert numerischen Score zu Level
        
        Args:
            score: Impact Score (0-1)
            
        Returns:
            Impact Level String
        """
        if score >= 0.8:
            return "very_high"
        elif score >= 0.5:
            return "high"
        elif score >= 0.2:
            return "medium"
        else:
            return "low"
    
    def _zero_impact(self) -> Dict[str, Any]:
        """Returns zero impact result"""
        return {
            "impact_score": 0.0,
            "components": {
                "volume_ratio": 0.0,
                "timing_score": 0.0,
                "size_impact": 0.0,
                "price_correlation": 0.0,
                "slippage_caused": 0.0
            },
            "impact_level": "none"
        }
    
    def calculate_batch_impact(
        self,
        wallet_activities: Dict[str, List[Dict[str, Any]]],
        candle_data: Dict[str, Any],
        total_volume: float # <-- Dies ist der entscheidende Parameter
    ) -> Dict[str, Dict[str, Any]]:
        """
        Berechnet Impact Scores für mehrere Wallets gleichzeitig
        
        Args:
            wallet_activities: Dictionary wallet_id -> trades
            candle_data: Candle-Daten
            total_volume: Gesamt-Volume
            
        Returns:
            Dictionary wallet_id -> impact_result
        """
        # --- NEUES DETAIL-LOGGING ---
        logger.info(f"--- calculate_batch_impact START ---")
        logger.info(f"Gesamtanzahl Wallets: {len(wallet_activities)}")
        logger.info(f"Übergebenes total_volume für Batch: {total_volume}")
        # --- ENDE NEUES DETAIL-LOGGING ---

        results = {}
        
        for wallet_id, trades in wallet_activities.items():
            # --- NEUES DETAIL-LOGGING ---
            logger.debug(f"Verarbeite Wallet: {wallet_id}")
            # --- ENDE NEUES DETAIL-LOGGING ---
            results[wallet_id] = self.calculate_impact_score(
                wallet_trades=trades,
                candle_data=candle_data,
                total_volume=total_volume
            )
        
        logger.info(f"Batch Impact berechnet für {len(results)} Wallets")
        
        # --- NEUES DETAIL-LOGGING ---
        logger.info(f"--- calculate_batch_impact END ---")
        # --- ENDE NEUES DETAIL-LOGGING ---
        
        return results
