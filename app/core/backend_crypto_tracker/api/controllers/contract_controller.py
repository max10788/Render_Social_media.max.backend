from fastapi import APIRouter, Depends, HTTPException, Query, Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.backend_crypto_tracker.config.database import get_db
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, NotFoundException
from app.core.backend_crypto_tracker.services.contract.contract_analyzer import ContractAnalyzer
from app.core.backend_crypto_tracker.services.contract.contract_metadata import ContractMetadataService
from app.core.backend_crypto_tracker.services.contract.security_scanner import SecurityScanner
from pydantic import BaseModel, Field
import asyncio

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1/contracts", tags=["contracts"])

# Pydantic Modelle
class ContractInfoResponse(BaseModel):
    address: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    chain: str
    deployment_date: Optional[datetime] = None
    creator_address: Optional[str] = None
    verification_status: bool
    contract_type: Optional[str] = None
    abi_hash: Optional[str] = None
    bytecode_hash: Optional[str] = None
    total_transactions: int = 0
    unique_users: int = 0
    last_activity: Optional[datetime] = None

class ContractInteractionResponse(BaseModel):
    method_name: str
    call_count: int
    unique_callers: int
    average_gas_used: float
    total_value_transferred: float
    first_call: Optional[datetime] = None
    last_call: Optional[datetime] = None
    popularity_score: float

class ContractSecurityResponse(BaseModel):
    overall_score: float
    security_level: str
    vulnerabilities: List[Dict[str, Any]]
    code_quality_metrics: Dict[str, Any]
    access_control_issues: List[str]
    economic_risks: List[str]
    verification_confidence: float

class ContractTimeSeriesResponse(BaseModel):
    time_period: str
    interval_data: List[Dict[str, Any]]
    total_transactions: int
    unique_wallets: int
    volume_transferred: float
    activity_density: float
    peak_activity_times: List[str]
    trend_direction: str

class ContractController:
    def __init__(self):
        self.contract_analyzer = ContractAnalyzer()
        self.metadata_service = ContractMetadataService()
        self.security_scanner = SecurityScanner()
    
    async def get_contract_info(self, address: str, chain: str, db: AsyncSession) -> Dict[str, Any]:
        """Holt umfassende Informationen über einen Smart Contract"""
        try:
            # Metadaten abrufen
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Basis-Informationen aus der Datenbank abrufen
            contract_data = await self._get_contract_from_db(address, chain, db)
            
            # Kombinierte Antwort erstellen
            response = {
                "address": address,
                "chain": chain,
                "name": metadata.get("name"),
                "symbol": metadata.get("symbol"),
                "deployment_date": metadata.get("deployment_date"),
                "creator_address": metadata.get("creator_address"),
                "verification_status": metadata.get("verification_status", False),
                "contract_type": metadata.get("contract_type"),
                "abi_hash": metadata.get("abi_hash"),
                "bytecode_hash": metadata.get("bytecode_hash"),
                "total_transactions": contract_data.get("total_transactions", 0),
                "unique_users": contract_data.get("unique_users", 0),
                "last_activity": contract_data.get("last_activity")
            }
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting contract info for {address} on {chain}: {e}")
            raise APIException(f"Failed to get contract info: {str(e)}")
    
    async def get_contract_interactions(self, address: str, chain: str, 
                                      time_period: str = "24h",
                                      db: AsyncSession = None) -> List[Dict[str, Any]]:
        """Analysiert Contract-Interaktionen und Methodenaufrufe"""
        try:
            # Zeitperiode in Stunden umrechnen
            period_hours = self._parse_time_period(time_period)
            
            # Interaktionsdaten abrufen
            interactions = await self.contract_analyzer.analyze_contract_interactions(
                address, chain, period_hours
            )
            
            # Für jede Methode detaillierte Statistiken berechnen
            method_stats = []
            for method_name, data in interactions.items():
                popularity_score = self._calculate_popularity_score(data)
                
                method_stats.append({
                    "method_name": method_name,
                    "call_count": data.get("call_count", 0),
                    "unique_callers": data.get("unique_callers", 0),
                    "average_gas_used": data.get("average_gas_used", 0),
                    "total_value_transferred": data.get("total_value_transferred", 0),
                    "first_call": data.get("first_call"),
                    "last_call": data.get("last_call"),
                    "popularity_score": popularity_score
                })
            
            # Nach Popularität sortieren
            method_stats.sort(key=lambda x: x["popularity_score"], reverse=True)
            
            return method_stats
            
        except Exception as e:
            logger.error(f"Error getting contract interactions for {address} on {chain}: {e}")
            raise APIException(f"Failed to get contract interactions: {str(e)}")
    
    async def get_contract_security_assessment(self, address: str, chain: str) -> Dict[str, Any]:
        """Führt Sicherheitsbewertung des Smart Contracts durch"""
        try:
            # Sicherheits-Scan durchführen
            security_data = await self.security_scanner.scan_contract_security(address, chain)
            
            # Gesamtscore berechnen
            overall_score = self._calculate_security_score(security_data)
            
            # Sicherheitslevel bestimmen
            security_level = self._determine_security_level(overall_score)
            
            return {
                "overall_score": overall_score,
                "security_level": security_level,
                "vulnerabilities": security_data.get("vulnerabilities", []),
                "code_quality_metrics": security_data.get("code_quality_metrics", {}),
                "access_control_issues": security_data.get("access_control_issues", []),
                "economic_risks": security_data.get("economic_risks", []),
                "verification_confidence": security_data.get("verification_confidence", 0.5)
            }
            
        except Exception as e:
            logger.error(f"Error getting contract security assessment for {address} on {chain}: {e}")
            raise APIException(f"Failed to get contract security assessment: {str(e)}")
    
    async def get_contract_time_series(self, address: str, chain: str, 
                                       time_period: str = "24h",
                                       interval: str = "1h") -> Dict[str, Any]:
        """Holt Zeitreihen-Daten für Contract-Aktivität"""
        try:
            period_hours = self._parse_time_period(time_period)
            interval_minutes = self._parse_interval(interval)
            
            # Zeitreihen-Daten abrufen
            time_series = await self.contract_analyzer.get_time_series_data(
                address, chain, period_hours, interval_minutes
            )
            
            # Aktivitätsdichte berechnen
            activity_density = self._calculate_activity_density(time_series)
            
            # Trend-Richtung bestimmen
            trend_direction = self._determine_trend_direction(time_series)
            
            # Peak-Aktivitätszeiten finden
            peak_times = self._find_peak_activity_times(time_series)
            
            return {
                "time_period": time_period,
                "interval": interval,
                "interval_data": time_series,
                "total_transactions": sum(item.get("transaction_count", 0) for item in time_series),
                "unique_wallets": len(set(
                    wallet for item in time_series 
                    for wallet in item.get("unique_wallets", [])
                )),
                "volume_transferred": sum(item.get("volume_transferred", 0) for item in time_series),
                "activity_density": activity_density,
                "peak_activity_times": peak_times,
                "trend_direction": trend_direction
            }
            
        except Exception as e:
            logger.error(f"Error getting contract time series for {address} on {chain}: {e}")
            raise APIException(f"Failed to get contract time series: {str(e)}")
    
    def _parse_time_period(self, time_period: str) -> int:
        """Konvertiert Zeitperiode in Stunden"""
        period_map = {
            "1h": 1,
            "24h": 24,
            "7d": 168,
            "30d": 720
        }
        return period_map.get(time_period, 24)
    
    def _parse_interval(self, interval: str) -> int:
        """Konvertiert Intervall in Minuten"""
        interval_map = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "1h": 60,
            "4h": 240
        }
        return interval_map.get(interval, 60)
    
    async def _get_contract_from_db(self, address: str, chain: str, db: AsyncSession) -> Dict[str, Any]:
        """Holt Contract-Daten aus der Datenbank"""
        # Implementierung für Datenbankabfrage
        return {
            "total_transactions": 0,
            "unique_users": 0,
            "last_activity": None
        }
    
    def _calculate_popularity_score(self, interaction_data: Dict[str, Any]) -> float:
        """Berechnet Popularitäts-Score basierend auf Interaktionsdaten"""
        call_count = interaction_data.get("call_count", 0)
        unique_callers = interaction_data.get("unique_callers", 0)
        total_value = interaction_data.get("total_value_transferred", 0)
        
        # Gewichtete Berechnung
        score = (call_count * 0.4) + (unique_callers * 0.3) + (total_value * 0.3)
        return min(100, score)
    
    def _calculate_security_score(self, security_data: Dict[str, Any]) -> float:
        """Berechnet Gesamt-Sicherheits-Score"""
        vulnerabilities = security_data.get("vulnerabilities", [])
        code_quality = security_data.get("code_quality_metrics", {})
        access_issues = security_data.get("access_control_issues", [])
        
        # Basis-Score
        base_score = 100
        
        # Abzüge für Schwachstellen
        severity_weights = {"critical": 30, "high": 20, "medium": 10, "low": 5}
        for vuln in vulnerabilities:
            severity = vuln.get("severity", "low")
            base_score -= severity_weights.get(severity, 5)
        
        # Abzüge für Code-Qualität
        code_score = code_quality.get("complexity_score", 0)
        if code_score > 70:
            base_score -= 15
        
        # Abzüge für Access-Control Issues
        base_score -= len(access_issues) * 5
        
        return max(0, min(100, base_score))
    
    def _determine_security_level(self, score: float) -> str:
        """Bestimmt Sicherheitslevel basierend auf Score"""
        if score >= 80:
            return "secure"
        elif score >= 60:
            return "moderate"
        elif score >= 40:
            return "risky"
        else:
            return "dangerous"
    
    def _calculate_activity_density(self, time_series: List[Dict[str, Any]]) -> float:
        """Berechnet Aktivitätsdichte für Zeitreihen"""
        if not time_series:
            return 0.0
        
        total_transactions = sum(item.get("transaction_count", 0) for item in time_series)
        period_length = len(time_series)
        
        return total_transactions / period_length if period_length > 0 else 0.0
    
    def _determine_trend_direction(self, time_series: List[Dict[str, Any]]) -> str:
        """Bestimmt Trend-Richtung aus Zeitreihen"""
        if len(time_series) < 2:
            return "stable"
        
        # Einfache lineare Regression für Trend
        x = list(range(len(time_series)))
        y = [item.get("transaction_count", 0) for item in time_series]
        
        n = len(x)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_x2 = sum(xi ** 2 for xi in x)
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2) if n * sum_x2 - sum_x ** 2 != 0 else 0
        
        if slope > 0.1:
            return "increasing"
        elif slope < -0.1:
            return "decreasing"
        else:
            return "stable"
    
    def _find_peak_activity_times(self, time_series: List[Dict[str, Any]]) -> List[str]:
        """Findet Zeiten mit höchster Aktivität"""
        if not time_series:
            return []
        
        # Nach Transaktionsanzahl sortieren
        sorted_times = sorted(time_series, key=lambda x: x.get("transaction_count", 0), reverse=True)
        
        # Top 3 Peak-Zeiten zurückgeben
        return [item.get("timestamp") for item in sorted_times[:3]]

# Controller-Instanz für die Routes
contract_controller = ContractController()
