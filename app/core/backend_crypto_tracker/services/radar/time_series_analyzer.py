import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class TimeSeriesPoint:
    timestamp: datetime
    transaction_count: int
    unique_wallets: List[str]
    volume_transferred: float
    gas_used: float
    method_calls: Dict[str, int]

class TimeSeriesAnalyzer:
    """Analysiert Zeitreihen-Daten für die Radar-Visualisierung"""
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 300  # 5 Minuten Cache
    
    async def analyze_contract_activity(self, 
                                   contract_address: str, 
                                   chain: str, 
                                   time_period_hours: int,
                                   interval_minutes: int = 60) -> Dict[str, Any]:
        """Analysiert Contract-Aktivität über einen Zeitraum"""
        try:
            # Cache-Key generieren
            cache_key = f"{contract_address}:{chain}:{time_period_hours}:{interval_minutes}"
            
            # Prüfen ob Daten im Cache
            if self._is_cache_valid(cache_key):
                return self.cache[cache_key]
            
            # Zeitreihen-Daten abrufen
            time_series = await self._fetch_time_series_data(
                contract_address, chain, time_period_hours, interval_minutes
            )
            
            # Aktivitätsdichte berechnen
            activity_density = self._calculate_activity_density(time_series)
            
            # Perioden-basierte Filterung
            filtered_data = self._filter_by_activity_periods(time_series)
            
            # Peak-Aktivitätszeiten identifizieren
            peak_times = self._identify_peak_activity_times(time_series)
            
            # Trend-Analyse
            trend_analysis = self._analyze_trends(time_series)
            
            # Ergebnis zusammenstellen
            result = {
                "contract_address": contract_address,
                "chain": chain,
                "time_period_hours": time_period_hours,
                "interval_minutes": interval_minutes,
                "time_series": time_series,
                "activity_density": activity_density,
                "filtered_data": filtered_data,
                "peak_activity_times": peak_times,
                "trend_analysis": trend_analysis,
                "generated_at": datetime.utcnow().isoformat()
            }
            
            # Im Cache speichern
            self._cache_result(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing contract activity: {e}")
            raise
    
    async def _fetch_time_series_data(self, 
                                      contract_address: str, 
                                      chain: str,
                                      time_period_hours: int,
                                      interval_minutes: int) -> List[TimeSeriesPoint]:
        """Ruft Zeitreihen-Daten von der Blockchain ab"""
        # In einer echten Implementierung würden hier Blockchain-APIs abgefragt
        # Für jetzt generieren wir Beispieldaten
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=time_period_hours)
        
        time_series = []
        current_time = start_time
        
        while current_time < end_time:
            # Zufällige Daten für Demo-Zwecke
            transaction_count = int(50 + 200 * (0.5 + 0.5 * (current_time.hour % 24) / 24))
            unique_wallets = [f"wallet_{i}" for i in range(5, min(25, transaction_count // 10))]
            volume_transferred = transaction_count * 0.1 * (0.5 + (current_time.minute / 60))
            gas_used = transaction_count * 21000
            
            method_calls = {
                "transfer": int(transaction_count * 0.6),
                "approve": int(transaction_count * 0.2),
                "swap": int(transaction_count * 0.15),
                "addLiquidity": int(transaction_count * 0.05)
            }
            
            time_series.append(TimeSeriesPoint(
                timestamp=current_time,
                transaction_count=transaction_count,
                unique_wallets=unique_wallets,
                volume_transferred=volume_transferred,
                gas_used=gas_used,
                method_calls=method_calls
            ))
            
            current_time += timedelta(minutes=interval_minutes)
        
        return time_series
    
    def _calculate_activity_density(self, time_series: List[TimeSeriesPoint]) -> Dict[str, float]:
        """Berechnet Aktivitätsdichte für verschiedene Zeitabschnitte"""
        if not time_series:
            return {}
        
        # Gesamtaktivität berechnen
        total_transactions = sum(point.transaction_count for point in time_series)
        total_volume = sum(point.volume_transferred for point in time_series)
        
        # Aktivitätsdichte pro Intervall
        interval_density = total_transactions / len(time_series)
        
        # Stündliche Aktivitätsverteilung
        hourly_density = {}
        for point in time_series:
            hour = point.timestamp.hour
            if hour not in hourly_density:
                hourly_density[hour] = 0
            hourly_density[hour] += point.transaction_count
        
        # Normalisieren
        max_hourly = max(hourly_density.values()) if hourly_density else 1
        normalized_hourly = {hour: count / max_hourly for hour, count in hourly_density.items()}
        
        return {
            "overall_density": interval_density,
            "total_volume": total_volume,
            "hourly_distribution": normalized_hourly,
            "peak_hour": max(hourly_density.items(), key=lambda x: x[1])[0] if hourly_density else None
        }
    
    def _filter_by_activity_periods(self, time_series: List[TimeSeriesPoint]) -> Dict[str, Any]:
        """Filtert Daten nach Aktivitätsperioden"""
        if not time_series:
            return {}
        
        # Schwellenwerte für Aktivitätsklassifizierung
        avg_transactions = sum(point.transaction_count for point in time_series) / len(time_series)
        high_activity_threshold = avg_transactions * 1.5
        low_activity_threshold = avg_transactions * 0.5
        
        high_activity_periods = []
        low_activity_periods = []
        
        for point in time_series:
            if point.transaction_count >= high_activity_threshold:
                high_activity_periods.append({
                    "timestamp": point.timestamp.isoformat(),
                    "transaction_count": point.transaction_count,
                    "volume": point.volume_transferred
                })
            elif point.transaction_count <= low_activity_threshold:
                low_activity_periods.append({
                    "timestamp": point.timestamp.isoformat(),
                    "transaction_count": point.transaction_count,
                    "volume": point.volume_transferred
                })
        
        return {
            "high_activity_periods": high_activity_periods,
            "low_activity_periods": low_activity_periods,
            "activity_thresholds": {
                "high": high_activity_threshold,
                "low": low_activity_threshold,
                "average": avg_transactions
            }
        }
    
    def _identify_peak_activity_times(self, time_series: List[TimeSeriesPoint]) -> List[str]:
        """Identifiziert Zeiten mit höchster Aktivität"""
        if not time_series:
            return []
        
        # Nach Transaktionsanzahl sortieren
        sorted_points = sorted(time_series, key=lambda x: x.transaction_count, reverse=True)
        
        # Top 5 Peak-Zeiten zurückgeben
        return [point.timestamp.isoformat() for point in sorted_points[:5]]
    
    def _analyze_trends(self, time_series: List[TimeSeriesPoint]) -> Dict[str, Any]:
        """Analysiert Trends in den Zeitreihen-Daten"""
        if len(time_series) < 2:
            return {"trend": "insufficient_data"}
        
        # Einfache Trend-Analyse
        transactions = [point.transaction_count for point in time_series]
        volumes = [point.volume_transferred for point in time_series]
        
        # Lineare Regression für Transaktions-Trend
        n = len(transactions)
        x = list(range(n))
        
        sum_x = sum(x)
        sum_y = sum(transactions)
        sum_xy = sum(xi * yi for xi, yi in zip(x, transactions))
        sum_x2 = sum(xi ** 2 for xi in x)
        
        transaction_slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2) if n * sum_x2 - sum_x ** 2 != 0 else 0
        
        # Trend-Richtung bestimmen
        if transaction_slope > 1:
            transaction_trend = "strongly_increasing"
        elif transaction_slope > 0.1:
            transaction_trend = "moderately_increasing"
        elif transaction_slope < -1:
            transaction_trend = "strongly_decreasing"
        elif transaction_slope < -0.1:
            transaction_trend = "moderately_decreasing"
        else:
            transaction_trend = "stable"
        
        # Volatilität berechnen
        if len(transactions) > 1:
            mean_transactions = sum(transactions) / n
            variance = sum((t - mean_transactions) ** 2 for t in transactions) / n
            volatility = variance ** 0.5
        else:
            volatility = 0
        
        return {
            "transaction_trend": transaction_trend,
            "transaction_slope": transaction_slope,
            "volatility": volatility,
            "data_points": n,
            "analysis_period": {
                "start": time_series[0].timestamp.isoformat(),
                "end": time_series[-1].timestamp.isoformat()
            }
        }
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Prüft ob Cache-Eintrag noch gültig ist"""
        if cache_key not in self.cache:
            return False
        
        cache_time = self.cache[cache_key].get("generated_at")
        if not cache_time:
            return False
        
        cache_datetime = datetime.fromisoformat(cache_time)
        return (datetime.utcnow() - cache_datetime).total_seconds() < self.cache_ttl
    
    def _cache_result(self, cache_key: str, result: Dict[str, Any]):
        """Speichert Ergebnis im Cache"""
        self.cache[cache_key] = result
        
        # Alte Cache-Einträge aufräumen
        if len(self.cache) > 100:
            oldest_key = min(self.cache.keys(), 
                           key=lambda k: datetime.fromisoformat(self.cache[k]["generated_at"]))
            del self.cache[oldest_key]
