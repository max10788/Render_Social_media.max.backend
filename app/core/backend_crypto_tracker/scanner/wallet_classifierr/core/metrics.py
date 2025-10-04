# ============================================================================
# wallet_classifier/core/metrics.py
# ============================================================================
"""Metric calculation and result management"""

from typing import List, Dict

class MetricResult:
    """Speichert das Ergebnis einer Metrik-Berechnung"""
    
    def __init__(self, name: str, value: float, weight: float = 1.0):
        self.name = name
        self.value = value
        self.weight = weight
    
    def __repr__(self):
        return f"MetricResult(name='{self.name}', value={self.value:.4f}, weight={self.weight})"


class MetricCalculator:
    """Basis-Klasse für Metrik-Berechnungen"""
    
    def __init__(self):
        self.results: List[MetricResult] = []
    
    def add_result(self, name: str, value: float, weight: float = 1.0):
        """Fügt ein Metrik-Ergebnis hinzu"""
        self.results.append(MetricResult(name, value, weight))
    
    def get_weighted_score(self) -> float:
        """Berechnet gewichteten Score aller Metriken"""
        if not self.results:
            return 0.0
        total_weight = sum(r.weight for r in self.results)
        if total_weight == 0:
            return 0.0
        return sum(r.value * r.weight for r in self.results) / total_weight
    
    def get_results_dict(self) -> Dict[str, float]:
        """Gibt alle Ergebnisse als Dictionary zurück"""
        return {r.name: r.value for r in self.results}
    
    def clear(self):
        """Löscht alle Ergebnisse"""
        self.results = []
