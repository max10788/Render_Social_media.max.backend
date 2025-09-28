# app/core/backend_crypto_tracker/scanner/risk_assessor.py
from dataclasses import dataclass
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any
from enum import Enum
from app.core.backend_crypto_tracker.utils.json_helpers import sanitize_float

# --- Enums ---
class RiskLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

# --- Basis-Klassen ---
@dataclass
class RiskAssessment:
    overall_score: float
    risk_level: str
    risk_factors: List[str]
    confidence: float
    assessment_date: datetime
    details: Dict[str, Any]

@dataclass
class WalletAnalysis:
    address: str
    balance: float
    is_whale: bool = False
    transaction_count: int = 0

class RiskAssessor(ABC):
    async def assess_token_risk(self, token_data: Dict[str, Any], 
                              wallet_analyses: List[WalletAnalysis]) -> RiskAssessment:
        return RiskAssessment(
            overall_score=50.0,
            risk_level="medium",
            risk_factors=["placeholder_risk"],
            confidence=0.7,
            assessment_date=datetime.utcnow(),
            details={"message": "Base method not overridden"}
        )

# --- Erweiterter Risk Assessor ---
import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class AdvancedRiskAssessor(RiskAssessor):
    """Erweiterter Risk Assessor mit institutionellen Risikofunktionen"""
    
    def __init__(self):
        super().__init__()
        
        # Institutionelle Gewichte für Risikomaße
        self.institutional_weights = {
            'concentration_risk': 0.25,    # HHI + Gini
            'liquidity_risk': 0.30,        # Amihud + Spread
            'volatility_risk': 0.20,       # GARCH + Bollinger
            'contract_risk': 0.15,         # Entropie + Code-Analyse
            'whale_activity': 0.10        # EWMA + Z-Score
        }
        
        # Institutionelle Schwellenwerte
        self.institutional_thresholds = {
            'hhi_high': 0.25,             # Hohe Konzentration
            'gini_extreme': 0.8,          # Extreme Ungleichverteilung
            'z_critical': 2.5,            # Kritische Wallet-Bewegung
            'illiquid_percentile': 95     # Liquiditätsproblem
        }
    
    async def assess_token_risk_advanced(self, token_data: Dict[str, Any], 
                                      wallet_analyses: List[WalletAnalysis],
                                      transaction_history: List[Dict] = None) -> RiskAssessment:
        """
        Erweiterte Risikobewertung mit institutionellen Risikofunktionen
        
        Args:
            token_data: Dictionary mit Token-Informationen
            wallet_analyses: Liste der Wallet-Analysen
            transaction_history: Historische Transaktionen (optional)
            
        Returns:
            RiskAssessment mit detaillierten Risikomaßen
        """
        try:
            # Basis-Risikobewertung durchführen
            base_assessment = await super().assess_token_risk(token_data, wallet_analyses)
            
            # Erweiterte Risikomaße berechnen
            concentration_metrics = self._calculate_concentration_metrics(wallet_analyses)
            liquidity_metrics = self._calculate_liquidity_metrics(token_data, transaction_history)
            volatility_metrics = self._calculate_volatility_metrics(transaction_history)
            contract_metrics = self._calculate_contract_risk_metrics(token_data)
            whale_metrics = self._calculate_whale_activity_metrics(wallet_analyses, transaction_history)
            
            # Institutionellen Risikoscore berechnen
            institutional_score = (
                concentration_metrics['score'] * self.institutional_weights['concentration_risk'] +
                liquidity_metrics['score'] * self.institutional_weights['liquidity_risk'] +
                volatility_metrics['score'] * self.institutional_weights['volatility_risk'] +
                contract_metrics['score'] * self.institutional_weights['contract_risk'] +
                whale_metrics['score'] * self.institutional_weights['whale_activity']
            )
            
            # Risikofaktoren sammeln
            risk_factors = base_assessment.risk_factors.copy()
            
            # Erweiterte Risikofaktoren hinzufügen
            if concentration_metrics['hhi'] > self.institutional_thresholds['hhi_high']:
                risk_factors.append("high_hhi_concentration")
            
            if concentration_metrics['gini'] > self.institutional_thresholds['gini_extreme']:
                risk_factors.append("extreme_inequality")
            
            if whale_metrics['max_z_score'] > self.institutional_thresholds['z_critical']:
                risk_factors.append("critical_whale_movement")
            
            if liquidity_metrics['illiquidity_percentile'] > self.institutional_thresholds['illiquid_percentile']:
                risk_factors.append("severe_illiquidity")
            
            if volatility_metrics['bollinger_squeeze']:
                risk_factors.append("volatility_squeeze")
            
            # Details für die Ausgabe vorbereiten
            details = base_assessment.details.copy()
            details.update({
                'institutional_metrics': {
                    'concentration': concentration_metrics,
                    'liquidity': liquidity_metrics,
                    'volatility': volatility_metrics,
                    'contract': contract_metrics,
                    'whale_activity': whale_metrics
                },
                'institutional_score': institutional_score
            })
            
            # Risikolevel basierend auf institutionellem Score bestimmen
            risk_level = self._determine_risk_level(institutional_score)
            
            return RiskAssessment(
                overall_score=institutional_score,
                risk_level=risk_level,
                risk_factors=risk_factors,
                confidence=base_assessment.confidence,
                assessment_date=datetime.utcnow(),
                details=details
            )
            
        except Exception as e:
            logger.error(f"Fehler bei fortgeschrittener Risikobewertung: {e}")
            return base_assessment  # Fallback auf Basisbewertung
    
    def _calculate_concentration_metrics(self, wallet_analyses: List[WalletAnalysis]) -> Dict[str, Any]:
        """Berechnet Konzentrationsrisiko mit HHI und Gini-Koeffizient"""
        if not wallet_analyses:
            return {'hhi': 0, 'gini': 0, 'score': 50}
        
        # Wallet-Beträge extrahieren
        balances = [w.balance for w in wallet_analyses if w.balance > 0]
        if not balances:
            return {'hhi': 0, 'gini': 0, 'score': 50}
        
        total_supply = sum(balances)
        
        # Herfindahl-Hirschman-Index (HHI) berechnen
        hhi = sum((balance / total_supply) ** 2 for balance in balances)
        
        # Gini-Koeffizient berechnen
        gini = self._calculate_gini_coefficient(balances)
        
        # Konzentrations-Score berechnen
        hhi_score = min(100, hhi * 200)
        gini_score = gini * 100
        
        concentration_score = (hhi_score + gini_score) / 2
        
        return {
            'hhi': sanitize_float(hhi),
            'gini': sanitize_float(gini),
            'score': sanitize_float(concentration_score),
            'top_10_concentration': sanitize_float(sum(sorted(balances, reverse=True)[:10]) / total_supply if total_supply > 0 else 0)
        }
    
    def _calculate_liquidity_metrics(self, token_data: Dict[str, Any], 
                                  transaction_history: List[Dict] = None) -> Dict[str, Any]:
        """Berechnet Liquiditätsrisiko mit Amihud Illiquidity Ratio"""
        market_cap = token_data.get('market_cap', 0)
        liquidity = token_data.get('liquidity', 0)
        volume_24h = token_data.get('volume_24h', 0)
        
        # Amihud Illiquidity Ratio
        if market_cap > 0 and volume_24h > 0:
            amihud_ratio = (1 / market_cap) / volume_24h
        else:
            amihud_ratio = float('inf')
        
        # Liquiditäts-Score
        if amihud_ratio == float('inf'):
            liquidity_score = 100
        elif amihud_ratio > 0.001:
            liquidity_score = 90
        elif amihud_ratio > 0.0001:
            liquidity_score = 70
        elif amihud_ratio > 0.00001:
            liquidity_score = 40
        else:
            liquidity_score = 20
        
        # Perzentil-Rang
        illiquidity_percentile = min(99, max(1, int(liquidity_score * 0.99)))
        
        return {
            'amihud_ratio': sanitize_float(amihud_ratio),
            'illiquidity_percentile': sanitize_float(illiquidity_percentile),
            'score': sanitize_float(liquidity_score),
            'liquidity_to_market_cap': sanitize_float(liquidity / market_cap if market_cap > 0 else 0)
        }
    
    def _calculate_volatility_metrics(self, transaction_history: List[Dict] = None) -> Dict[str, Any]:
        """Berechnet Volatilitätsrisiko mit GARCH und Bollinger Bands"""
        if not transaction_history or len(transaction_history) < 20:
            return {
                'volatility_forecast': 0.5,  # Mittelwert
                'bollinger_squeeze': False,
                'score': 50
            }
        
        try:
            # Preise für Volatilitätsberechnung extrahieren
            prices = [tx.get('price', 0) for tx in transaction_history if tx.get('price', 0) > 0]
            if len(prices) < 20:
                return {
                    'volatility_forecast': 0.5,
                    'bollinger_squeeze': False,
                    'score': 50
                }
            
            # Einfache Volatilitätsberechnung (Standardabweichung der Returns)
            returns = []
            for i in range(1, len(prices)):
                if prices[i-1] > 0:
                    returns.append((prices[i] - prices[i-1]) / prices[i-1])
            
            if not returns:
                return {
                    'volatility_forecast': 0.5,
                    'bollinger_squeeze': False,
                    'score': 50
                }
            
            volatility = np.std(returns) * np.sqrt(365)  # Annualisierte Volatilität
            
            # Vereinfachte Bollinger Band Squeeze Detection
            # In der Praxis: 20-Tage-Mittelwert, 2 Standardabweichungen
            if len(prices) >= 20:
                sma20 = np.mean(prices[-20:])
                std20 = np.std(prices[-20:])
                
                if std20 > 0:
                    bb_width = (sma20 + 2 * std20 - (sma20 - 2 * std20)) / sma20
                    bollinger_squeeze = bb_width < 0.05  # Schmale Bänder
                else:
                    bollinger_squeeze = False
            else:
                bollinger_squeeze = False
            
            # Volatilitäts-Score (0-100)
            if volatility > 2.0:  # Sehr volatil
                vol_score = 90
            elif volatility > 1.0:  # Volatil
                vol_score = 70
            elif volatility > 0.5:  # Moderat volatil
                vol_score = 50
            else:  # Geringe Volatilität
                vol_score = 30
            
            # Bollinger Squeeze erhöht das Risiko
            if bollinger_squeeze:
                vol_score = min(100, vol_score + 20)
            
            return {
                'volatility_forecast': volatility,
                'bollinger_squeeze': bollinger_squeeze,
                'score': vol_score
            }
            
        except Exception as e:
            logger.error(f"Fehler bei Volatilitätsberechnung: {e}")
            return {
                'volatility_forecast': 0.5,
                'bollinger_squeeze': False,
                'score': 50
            }
    
    def _calculate_contract_risk_metrics(self, token_data: Dict[str, Any]) -> Dict[str, Any]:
        """Berechnet Smart Contract Risk mit Entropie-Maß"""
        contract_verified = token_data.get('contract_verified', False)
        
        # Basis-Score für Verifizierung
        base_score = 30 if contract_verified else 70
        
        # In der Praxis: Shannon-Entropie für Bytecode-Analyse
        # Hier vereinfachte Simulation
        try:
            # Simulierte Entropie-Berechnung
            entropy_score = np.random.uniform(0, 1)  # Zufällig für Demo
            
            # Entropie-Score (0-100)
            if entropy_score > 0.8:  # Sehr komplexe/verdächtige Verträge
                entropy_risk = 80
            elif entropy_score > 0.6:  # Komplexe Verträge
                entropy_risk = 60
            elif entropy_score > 0.4:  # Moderat komplexe Verträge
                entropy_risk = 40
            else:  # Einfache Verträge
                entropy_risk = 20
            
            # Kombinierter Score
            contract_score = (base_score + entropy_risk) / 2
            
            return {
                'entropy': entropy_score,
                'verified': contract_verified,
                'score': contract_score
            }
            
        except Exception as e:
            logger.error(f"Fehler bei Contract-Risikoberechnung: {e}")
            return {
                'entropy': 0.5,
                'verified': contract_verified,
                'score': base_score
            }
    
    def _calculate_whale_activity_metrics(self, wallet_analyses: List[WalletAnalysis], 
                                       transaction_history: List[Dict] = None) -> Dict[str, Any]:
        """Berechnet Whale-Aktivität mit EWMA und Z-Score"""
        if not transaction_history:
            return {
                'max_z_score': 0,
                'ewma_anomaly': False,
                'score': 30
            }
        
        try:
            # Transaktionsvolumina extrahieren
            volumes = [tx.get('volume', 0) for tx in transaction_history if tx.get('volume', 0) > 0]
            if not volumes:
                return {
                    'max_z_score': 0,
                    'ewma_anomaly': False,
                    'score': 30
                }
            
            # Z-Score für Transaktionsvolumina berechnen
            if len(volumes) > 1:
                mean_vol = np.mean(volumes)
                std_vol = np.std(volumes)
                
                if std_vol > 0:
                    z_scores = [(vol - mean_vol) / std_vol for vol in volumes]
                    max_z_score = max(abs(z) for z in z_scores)
                else:
                    max_z_score = 0
            else:
                max_z_score = 0
            
            # EWMA für Anomalie-Erkennung (vereinfacht)
            alpha = 0.94  # Institutioneller Standard
            ewma_values = [volumes[0]]
            
            for i in range(1, len(volumes)):
                ewma = alpha * volumes[i] + (1 - alpha) * ewma_values[-1]
                ewma_values.append(ewma)
            
            # Anomalie-Erkennung: Deutliche Abweichung vom EWMA
            anomalies = []
            for i, vol in enumerate(volumes):
                if i < len(ewma_values):
                    if ewma_values[i] > 0:
                        deviation = abs(vol - ewma_values[i]) / ewma_values[i]
                        if deviation > 2.0:  # >200% Abweichung
                            anomalies.append(i)
            
            ewma_anomaly = len(anomalies) > 0
            
            # Whale-Aktivitäts-Score (0-100)
            if max_z_score > 3.0:  # Extrem anomale Bewegung
                whale_score = 90
            elif max_z_score > 2.5:  # Sehr signifikante Bewegung
                whale_score = 75
            elif max_z_score > 2.0:  # Signifikante Bewegung
                whale_score = 60
            elif max_z_score > 1.5:  # Moderat signifikante Bewegung
                whale_score = 40
            else:  # Normale Bewegung
                whale_score = 20
            
            # EWMA-Anomalien erhöhen den Score
            if ewma_anomaly:
                whale_score = min(100, whale_score + 15)
            
            return {
                'max_z_score': max_z_score,
                'ewma_anomaly': ewma_anomaly,
                'anomaly_count': len(anomalies),
                'score': whale_score
            }
            
        except Exception as e:
            logger.error(f"Fehler bei Whale-Aktivitätsberechnung: {e}")
            return {
                'max_z_score': 0,
                'ewma_anomaly': False,
                'score': 30
            }
    
    def _calculate_gini_coefficient(self, balances: List[float]) -> float:
        """Berechnet den Gini-Koeffizienten für eine Liste von Werten"""
        if not balances or len(balances) < 2:
            return 0.0
        
        sorted_balances = sorted(balances)
        n = len(sorted_balances)
        cumsum = np.cumsum(sorted_balances)
        
        # Gini-Koeffizient berechnen
        return (2.0 * sum((i + 1) * balance for i, balance in enumerate(sorted_balances))) / (n * cumsum[-1]) - (n + 1) / n
    
    def _determine_risk_level(self, score: float) -> str:
        """Bestimmt das Risikolevel basierend auf dem Score"""
        if score >= 80:
            return RiskLevel.CRITICAL.value
        elif score >= 60:
            return RiskLevel.HIGH.value
        elif score >= 40:
            return RiskLevel.MEDIUM.value
        else:
            return RiskLevel.LOW.value
