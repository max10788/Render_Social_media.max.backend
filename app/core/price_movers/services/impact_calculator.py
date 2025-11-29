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
            total_volume: float,
            all_trades: Optional[List[Dict[str, Any]]] = None,
            apply_liquidity_multipliers: bool = False
        ) -> Dict[str, Any]:
            """Calculate impact score - FULL DEBUG VERSION"""
            
            logger.info(f"\n{'='*80}")
            logger.info(f"🎯 CALCULATE_IMPACT_SCORE START")
            logger.info(f"{'='*80}")
            
            wallet_id = wallet_trades[0].get('wallet_address', 'unknown')[:16] if wallet_trades else 'no_trades'
            logger.info(f"Wallet: {wallet_id}...")
            logger.info(f"Trades: {len(wallet_trades)}")
            logger.info(f"Total Volume: ${total_volume:.2f}")
            
            if not wallet_trades:
                logger.info("❌ No trades -> Zero Impact")
                return self._zero_impact()
            
            # ✅ DEBUG Input
            wallet_volume_usd = sum(
                float(t.get('amount', 0)) * float(t.get('price', 0)) 
                for t in wallet_trades
            )
            logger.info(f"Wallet Volume: ${wallet_volume_usd:.2f}")
            logger.info(f"Expected Ratio: {wallet_volume_usd/total_volume:.4f}")
            
            # ==================== COMPONENT 1: Volume Ratio ====================
            logger.info(f"\n{'─'*80}")
            logger.info(f"📊 COMPONENT 1: VOLUME RATIO")
            logger.info(f"{'─'*80}")
            
            volume_ratio = self._calculate_volume_ratio(wallet_trades, total_volume)
            
            logger.info(f"✅ Volume Ratio: {volume_ratio:.4f} ({volume_ratio*100:.1f}%)")
            
            # Apply liquidity multipliers if needed
            if apply_liquidity_multipliers:
                # ... [liquidity logic bleibt gleich] ...
                pass
            
            # ==================== COMPONENT 2: Timing Score ====================
            logger.info(f"\n{'─'*80}")
            logger.info(f"⏰ COMPONENT 2: TIMING SCORE")
            logger.info(f"{'─'*80}")
            
            timing_score = self._calculate_timing_score(wallet_trades, candle_data)
            
            logger.info(f"✅ Timing Score: {timing_score:.4f} ({timing_score*100:.1f}%)")
            
            # ==================== COMPONENT 3: Size Impact ====================
            logger.info(f"\n{'─'*80}")
            logger.info(f"📏 COMPONENT 3: SIZE IMPACT")
            logger.info(f"{'─'*80}")
            
            size_impact = self._calculate_size_impact(wallet_trades, candle_data)
            
            logger.info(f"✅ Size Impact: {size_impact:.4f} ({size_impact*100:.1f}%)")
            
            # ==================== COMPONENT 4: Price Correlation ====================
            logger.info(f"\n{'─'*80}")
            logger.info(f"📈 COMPONENT 4: PRICE CORRELATION")
            logger.info(f"{'─'*80}")
            
            price_correlation = self._calculate_price_correlation(wallet_trades, candle_data)
            
            logger.info(f"✅ Price Correlation: {price_correlation:.4f} ({price_correlation*100:.1f}%)")
            
            # ==================== COMPONENT 5: Slippage ====================
            logger.info(f"\n{'─'*80}")
            logger.info(f"💨 COMPONENT 5: SLIPPAGE")
            logger.info(f"{'─'*80}")
            
            slippage_caused = self._calculate_slippage_score(wallet_trades, candle_data)
            
            logger.info(f"✅ Slippage: {slippage_caused:.4f} ({slippage_caused*100:.1f}%)")
            
            # ==================== FINAL CALCULATION ====================
            logger.info(f"\n{'='*80}")
            logger.info(f"🎯 FINAL IMPACT CALCULATION")
            logger.info(f"{'='*80}")
            
            components = ImpactComponents(
                volume_ratio=volume_ratio,
                timing_score=timing_score,
                size_impact=size_impact,
                price_correlation=price_correlation,
                slippage_caused=slippage_caused
            )
            
            total_score = components.total_score
            
            logger.info(f"Weighted Components:")
            logger.info(f"  volume_ratio:      {volume_ratio:.4f} × 0.35 = {volume_ratio * 0.35:.4f}")
            logger.info(f"  timing_score:      {timing_score:.4f} × 0.25 = {timing_score * 0.25:.4f}")
            logger.info(f"  size_impact:       {size_impact:.4f} × 0.20 = {size_impact * 0.20:.4f}")
            logger.info(f"  price_correlation: {price_correlation:.4f} × 0.15 = {price_correlation * 0.15:.4f}")
            logger.info(f"  slippage_caused:   {slippage_caused:.4f} × 0.05 = {slippage_caused * 0.05:.4f}")
            logger.info(f"  {'─'*50}")
            logger.info(f"  TOTAL IMPACT:      {total_score:.4f} ({total_score*100:.1f}%)")
            logger.info(f"  Impact Level:      {self._get_impact_level(total_score).upper()}")
            logger.info(f"{'='*80}\n")
            
            # Check for liquidity events
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
                "has_liquidity_events": has_liquidity_events
            }
            
            logger.info(f"📤 Returning result: impact_score={result['impact_score']:.3f}")
            
            return result

    
    def _calculate_volume_ratio(
        self, 
        wallet_trades: List[Dict], 
        total_volume: float
    ) -> float:
        """
        Berechne Volume Ratio - ENHANCED DEBUG VERSION
        
        Returns:
            Normalized value (0.0 - 1.0)
        """
        try:
            logger.info(f"\n{'='*80}")
            logger.info(f"🔍 _calculate_volume_ratio START")
            logger.info(f"{'='*80}")
            
            # Check inputs
            logger.info(f"Input Check:")
            logger.info(f"  wallet_trades: {len(wallet_trades) if wallet_trades else 0} trades")
            logger.info(f"  total_volume: {total_volume}")
            
            if not wallet_trades:
                logger.warning(f"❌ No wallet trades -> return 0.0")
                return 0.0
                
            if total_volume <= 0:
                logger.warning(f"❌ Total volume <= 0 -> return 0.0")
                return 0.0
            
            # Calculate wallet volume USD
            logger.info(f"\nCalculating Wallet Volume USD:")
            wallet_volume_usd = 0.0
            
            for i, trade in enumerate(wallet_trades):
                amount = float(trade.get('amount', 0))
                price = float(trade.get('price', 0))
                trade_value = amount * price
                
                logger.info(f"  Trade {i+1}: amount={amount:.4f}, price={price:.2f}, value=${trade_value:.2f}")
                
                wallet_volume_usd += trade_value
            
            logger.info(f"\n💰 RESULT:")
            logger.info(f"  Wallet Volume USD: ${wallet_volume_usd:.2f}")
            logger.info(f"  Total Volume: ${total_volume:.2f}")
            
            # Calculate ratio (as FACTOR, not percent!)
            if total_volume <= 0:
                logger.warning(f"❌ Total volume is 0, cannot divide -> return 0.0")
                return 0.0
            
            volume_factor = wallet_volume_usd / total_volume
            
            # ✅ NEW: Better logging with BOTH representations
            logger.info(
                f"\n📊 Volume Analysis:\n"
                f"   Wallet: ${wallet_volume_usd:.2f}\n"
                f"   Total:  ${total_volume:.2f}\n"
                f"   Factor: {volume_factor:.4f}x\n"
                f"   Percent: {volume_factor * 100:.2f}%"
            )
            
            # ✅ Detect anomalies
            if volume_factor > 1.0:
                logger.warning(
                    f"\n🚨 ANOMALY DETECTED!\n"
                    f"   → Wallet volume EXCEEDS candle volume!\n"
                    f"   → This indicates data quality issues!"
                )
                
                # Check if it's a liquidity event
                has_liquidity = any(
                    t.get('transaction_type') in ['ADD_LIQUIDITY', 'REMOVE_LIQUIDITY']
                    for t in wallet_trades
                )
                
                if has_liquidity:
                    logger.info(
                        f"   ℹ️ This is a LIQUIDITY EVENT\n"
                        f"   → Using special handling (capped at 1.0)"
                    )
                else:
                    logger.error(
                        f"   ❌ No liquidity event - LIKELY BAD DATA!\n"
                        f"   → Check price validation in Helius parser"
                    )
                    
                    # Show sample trades
                    logger.error(f"\n   Sample trades:")
                    for i, t in enumerate(wallet_trades[:3]):
                        logger.error(
                            f"      Trade {i+1}: "
                            f"amount={t.get('amount'):.4f}, "
                            f"price=${t.get('price'):.2f}, "
                            f"value=${t.get('amount', 0) * t.get('price', 0):.2f}"
                        )
                
                # Cap at 1.0
                volume_factor = 1.0
                logger.info(f"   → Capped at 1.0")
            
            logger.info(f"\n✅ Final Volume Factor: {volume_factor:.4f}")
            logger.info(f"{'='*80}\n")
            
            return volume_factor
            
        except Exception as e:
            logger.error(f"\n{'='*80}")
            logger.error(f"❌ EXCEPTION in _calculate_volume_ratio!")
            logger.error(f"{'='*80}")
            logger.error(f"Error: {e}", exc_info=True)
            logger.error(f"Wallet trades: {wallet_trades}")
            logger.error(f"Total volume: {total_volume}")
            logger.error(f"{'='*80}\n")
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
        Berechnet Size Impact Score - FIXED
        
        Große Trades haben mehr Impact
        """
        if not wallet_trades:
            return 0.0
        
        # ✅ FIX: Berechne value_usd = amount * price (Feld existiert nicht im Trade!)
        trade_values = []
        for trade in wallet_trades:
            amount = float(trade.get("amount", 0))
            price = float(trade.get("price", 0))
            value_usd = amount * price
            trade_values.append(value_usd)
        
        if not trade_values:
            logger.debug(f"No valid trade values")
            return 0.0
        
        # Durchschnittliche Trade-Größe
        avg_trade_size = sum(trade_values) / len(trade_values)
        
        # Größte Trade
        max_trade_size = max(trade_values)
        
        logger.debug(f"💰 Size Impact: avg=${avg_trade_size:.2f}, max=${max_trade_size:.2f}")
        
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
        
        logger.debug(f"📊 Scores: avg_score={avg_score:.3f}, max_score={max_score:.3f}")
        
        # Gewichtete Kombination
        size_impact = (avg_score * 0.6) + (max_score * 0.4)
        
        return min(size_impact, 1.0)
    
    def _calculate_price_correlation(
        self,
        wallet_trades: List[Dict[str, Any]],
        candle_data: Dict[str, Any]
    ) -> float:
        """
        Berechnet Korrelation zwischen Trades und Preisbewegung - FINAL FIX
        
        Fixes:
        1. Handles 'swap' type trades properly
        2. Robust None/0 handling for price_change_pct
        3. Uses threshold instead of exact == 0 comparison
        """
        if not wallet_trades:
            return 0.0
        
        # ✅ FIX 1: Robust extraction with None handling
        price_change_pct = candle_data.get("price_change_pct")
        
        # Handle None explicitly
        if price_change_pct is None:
            logger.warning(f"⚠️ price_change_pct is None in candle_data!")
            price_change_pct = 0.0
        
        # Convert to float
        try:
            price_change_pct = float(price_change_pct)
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ Cannot convert price_change_pct to float: {e}")
            price_change_pct = 0.0
        
        logger.debug(f"📈 Price Correlation: price_change={price_change_pct:.4f}%")
        
        # ✅ FIX 2: Enhanced trade type detection
        buy_volume = 0.0
        sell_volume = 0.0
        swap_volume = 0.0
        
        for trade in wallet_trades:
            amount = float(trade.get("amount", 0))
            
            # Check multiple fields for trade type
            trade_type = (
                trade.get("trade_type", "").lower() or 
                trade.get("side", "").lower() or
                "unknown"
            )
            
            logger.debug(f"  Trade: type={trade_type}, amount={amount:.2f}")
            
            if trade_type == "buy":
                buy_volume += amount
            elif trade_type == "sell":
                sell_volume += amount
            elif trade_type == "swap":
                swap_volume += amount
            else:
                logger.debug(f"  Unknown trade type: {trade_type}, counting as swap")
                swap_volume += amount
        
        logger.debug(
            f"  Volumes: buy={buy_volume:.2f}, sell={sell_volume:.2f}, "
            f"swap={swap_volume:.2f}"
        )
        
        directional_volume = buy_volume + sell_volume
        total_volume = buy_volume + sell_volume + swap_volume
        
        if directional_volume == 0:
            # ✅ FIX 3: Use threshold instead of exact == 0
            has_price_movement = abs(price_change_pct) > 0.001  # 0.001% threshold
            has_volume = total_volume > 0
            
            logger.debug(
                f"  All swaps detected. "
                f"price_movement={has_price_movement} ({price_change_pct:.4f}%), "
                f"volume={has_volume} ({total_volume:.2f})"
            )
            
            if has_price_movement and has_volume:
                # Base correlation for being active during price movement
                correlation = 0.3
                logger.debug(f"  → Base correlation: 0.3")
            else:
                correlation = 0.0
                if not has_price_movement:
                    logger.debug(f"  → Zero correlation: no significant price movement")
                if not has_volume:
                    logger.debug(f"  → Zero correlation: no volume")
        else:
            # Normal directional calculation
            buy_ratio = buy_volume / directional_volume
            
            logger.debug(f"  Directional trades: buy_ratio={buy_ratio:.3f}")
            
            # ✅ FIX 4: Use threshold for price direction too
            if price_change_pct > 0.001:
                # Price up: more buys = higher correlation
                correlation = buy_ratio
                logger.debug(f"  Price UP → correlation = buy_ratio = {correlation:.3f}")
            elif price_change_pct < -0.001:
                # Price down: more sells = higher correlation
                correlation = 1.0 - buy_ratio
                logger.debug(f"  Price DOWN → correlation = (1 - buy_ratio) = {correlation:.3f}")
            else:
                # No significant movement
                correlation = 0.5
                logger.debug(f"  Price FLAT → correlation = 0.5")
            
            # Scale with size of price movement
            movement_scale = min(abs(price_change_pct) / 2.0, 1.0)
            correlation = correlation * movement_scale
            
            logger.debug(
                f"  Scaled: {correlation / movement_scale if movement_scale > 0 else 0:.3f} * {movement_scale:.3f} = {correlation:.3f}"
            )
        
        return min(correlation, 1.0)
    
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
        total_volume: float
    ) -> Dict[str, Dict[str, Any]]:
        """
        Berechnet Impact Scores für mehrere Wallets gleichzeitig
        
        Args:
            wallet_activities: Dictionary wallet_id -> trades
            candle_data: Candle-Daten
            total_volume: Gesamt-Volume IN USD
            
        Returns:
            Dictionary wallet_id -> impact_result
        """
        logger.info(f"--- calculate_batch_impact START ---")
        logger.info(f"Gesamtanzahl Wallets: {len(wallet_activities)}")
        logger.info(f"Übergebenes total_volume für Batch: {total_volume}")

        results = {}
        
        for wallet_id, trades in wallet_activities.items():
            logger.debug(f"Verarbeite Wallet: {wallet_id}")
            results[wallet_id] = self.calculate_impact_score(
                wallet_trades=trades,
                candle_data=candle_data,
                total_volume=total_volume
            )
        
        logger.info(f"Batch Impact berechnet für {len(results)} Wallets")
        logger.info(f"--- calculate_batch_impact END ---")
        
        return results
