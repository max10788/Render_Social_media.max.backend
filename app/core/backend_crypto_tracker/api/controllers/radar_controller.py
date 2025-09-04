import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from fastapi import HTTPException
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, NotFoundException
from app.core.backend_crypto_tracker.services.radar.position_calculator import PositionCalculator
from app.core.backend_crypto_tracker.services.radar.time_series_analyzer import TimeSeriesAnalyzer
from app.core.backend_crypto_tracker.services.radar.risk_classifier import RiskClassifier

logger = get_logger(__name__)

class RadarController:
    """Controller für Radar-spezifische Daten und Analysen"""
    
    def __init__(self):
        self.position_calculator = PositionCalculator()
        self.time_series_analyzer = TimeSeriesAnalyzer()
        self.risk_classifier = RiskClassifier()
    
    async def get_contract_radar_data(self, 
                                     contract_address: str, 
                                     chain: str, 
                                     time_period: str = "24h") -> Dict[str, Any]:
        """Holt alle für den Radar benötigten Daten für einen Contract"""
        try:
            # Zeitreihen-Analyse durchführen
            time_period_hours = self._parse_time_period(time_period)
            time_series_data = await self.time_series_analyzer.analyze_contract_activity(
                contract_address, chain, time_period_hours
            )
            
            # Wallet-Daten extrahieren
            wallet_data = self._extract_wallet_data(time_series_data)
            
            # Transaktionsdaten für Verbindungen
            transaction_data = self._extract_transaction_data(time_series_data)
            
            # Positionen berechnen
            wallet_positions = self.position_calculator.calculate_wallet_positions(
                wallet_data, time_series_data
            )
            
            # Verbindungen berechnen
            wallet_connections = self.position_calculator.calculate_wallet_connections(
                wallet_data, transaction_data
            )
            
            # Risikoklassifizierung
            risk_classification = await self.risk_classifier.classify_wallets(wallet_data)
            
            # Radar-spezifische Metriken berechnen
            radar_metrics = self._calculate_radar_metrics(
                wallet_positions, wallet_connections, time_series_data
            )
            
            # Ergebnis zusammenstellen
            result = {
                "contract_address": contract_address,
                "chain": chain,
                "time_period": time_period,
                "generated_at": datetime.utcnow().isoformat(),
                "wallet_positions": [
                    {
                        "address": pos.wallet_address,
                        "x": pos.x,
                        "y": pos.y,
                        "distance_from_center": pos.distance_from_center,
                        "angle": pos.angle,
                        "activity_score": pos.activity_score,
                        "risk_score": pos.risk_score,
                        "connection_count": pos.connection_count
                    }
                    for pos in wallet_positions
                ],
                "wallet_connections": [
                    {
                        "from_wallet": conn.from_wallet,
                        "to_wallet": conn.to_wallet,
                        "strength": conn.strength,
                        "interaction_count": conn.interaction_count,
                        "total_value": conn.total_value,
                        "last_interaction": conn.last_interaction.isoformat()
                    }
                    for conn in wallet_connections
                ],
                "risk_classification": risk_classification,
                "radar_metrics": radar_metrics,
                "time_series_summary": {
                    "total_transactions": time_series_data.get("total_transactions", 0),
                    "unique_wallets": time_series_data.get("unique_wallets", 0),
                    "activity_density": time_series_data.get("activity_density", {}),
                    "peak_activity_times": time_series_data.get("peak_activity_times", []),
                    "trend_direction": time_series_data.get("trend_analysis", {}).get("transaction_trend", "stable")
                }
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting contract radar data: {e}")
            raise APIException(f"Failed to get radar data: {str(e)}")
    
    async def get_wallet_radar_details(self, 
                                      wallet_address: str, 
                                      chain: str, 
                                      contract_address: str,
                                      time_period: str = "24h") -> Dict[str, Any]:
        """Holt detaillierte Radar-Informationen für eine spezifische Wallet"""
        try:
            # Zuerst allgemeine Radar-Daten für den Contract holen
            contract_radar_data = await self.get_contract_radar_data(
                contract_address, chain, time_period
            )
            
            # Spezifische Wallet-Details finden
            wallet_position = None
            wallet_connections = []
            
            for pos in contract_radar_data["wallet_positions"]:
                if pos["address"] == wallet_address:
                    wallet_position = pos
                    break
            
            for conn in contract_radar_data["wallet_connections"]:
                if conn["from_wallet"] == wallet_address or conn["to_wallet"] == wallet_address:
                    wallet_connections.append(conn)
            
            if not wallet_position:
                raise NotFoundException(f"Wallet {wallet_address} not found in radar data")
            
            # Risikoklassifizierung für diese Wallet
            wallet_risk = None
            for wallet_risk_data in contract_radar_data["risk_classification"]["wallets"]:
                if wallet_risk_data["address"] == wallet_address:
                    wallet_risk = wallet_risk_data
                    break
            
            # Detaillierte Analyse
            detailed_analysis = self._analyze_wallet_in_detail(
                wallet_position, wallet_connections, wallet_risk
            )
            
            return {
                "wallet_address": wallet_address,
                "contract_address": contract_address,
                "chain": chain,
                "position": wallet_position,
                "connections": wallet_connections,
                "risk_assessment": wallet_risk,
                "detailed_analysis": detailed_analysis,
                "generated_at": datetime.utcnow().isoformat()
            }
            
        except NotFoundException:
            raise
        except Exception as e:
            logger.error(f"Error getting wallet radar details: {e}")
            raise APIException(f"Failed to get wallet radar details: {str(e)}")
    
    def _parse_time_period(self, time_period: str) -> int:
        """Konvertiert Zeitperiode in Stunden"""
        period_map = {
            "1h": 1,
            "24h": 24,
            "7d": 168,
            "30d": 720
        }
        return period_map.get(time_period, 24)
    
    def _extract_wallet_data(self, time_series_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrahiert Wallet-Daten aus Zeitreihen-Analyse"""
        # In einer echten Implementierung würden hier die Wallets aus den Zeitreihen extrahiert
        # Für jetzt generieren wir Beispieldaten basierend auf der Aktivität
        
        unique_wallets = set()
        for point in time_series_data.get("time_series", []):
            unique_wallets.update(point.unique_wallets)
        
        wallet_data = []
        for i, wallet_address in enumerate(unique_wallets):
            # Zufällige Daten für Demo-Zwecke
            wallet_data.append({
                "address": wallet_address,
                "transaction_count": 50 + i * 25,
                "unique_interactions": 5 + i,
                "total_value": 10000 + i * 5000,
                "last_activity": datetime.utcnow().isoformat(),
                "risk_score": 0.3 + (i * 0.1),
                "connections": [f"wallet_{j}" for j in range(1, min(4, i + 1))]
            })
        
        return wallet_data
    
    def _extract_transaction_data(self, time_series_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extrahiert Transaktionsdaten aus Zeitreihen"""
        # Ähnlich wie _extract_wallet_data, aber für Transaktionen
        return []
    
    def _calculate_radar_metrics(self, 
                                 positions: List, 
                                 connections: List, 
                                 time_series_data: Dict[str, Any]) -> Dict[str, Any]:
        """Berechnet Radar-spezifische Metriken"""
        try:
            # Durchschnittliche Entfernung vom Zentrum
            avg_distance = sum(pos.distance_from_center for pos in positions) / len(positions) if positions else 0
            
            # Verbindungsstärke-Verteilung
            connection_strengths = [conn.strength for conn in connections]
            avg_connection_strength = sum(connection_strengths) / len(connection_strengths) if connection_strengths else 0
            max_connection_strength = max(connection_strengths) if connection_strengths else 0
            
            # Aktivitätsverteilung
            activity_scores = [pos.activity_score for pos in positions]
            avg_activity = sum(activity_scores) / len(activity_scores) if activity_scores else 0
            activity_variance = sum((score - avg_activity) ** 2 for score in activity_scores) / len(activity_scores) if activity_scores else 0
            
            # Risikoverteilung
            risk_scores = [pos.risk_score for pos in positions]
            avg_risk = sum(risk_scores) / len(risk_scores) if risk_scores else 0
            
            # Netzwerk-Dichte
            network_density = len(connections) / (len(positions) * (len(positions) - 1)) if len(positions) > 1 else 0
            
            return {
                "average_distance_from_center": avg_distance,
                "average_connection_strength": avg_connection_strength,
                "max_connection_strength": max_connection_strength,
                "activity_distribution": {
                    "average": avg_activity,
                    "variance": activity_variance
                },
                "risk_distribution": {
                    "average": avg_risk,
                    "high_risk_wallets": len([r for r in risk_scores if r >= 0.7]),
                    "low_risk_wallets": len([r for r in risk_scores if r <= 0.3])
                },
                "network_density": network_density,
                "total_wallets": len(positions),
                "total_connections": len(connections)
            }
            
        except Exception as e:
            logger.error(f"Error calculating radar metrics: {e}")
            return {}
    
    def _analyze_wallet_in_detail(self, 
                                  position, 
                                  connections, 
                                  risk_assessment) -> Dict[str, Any]:
        """Führt detaillierte Analyse einer Wallet durch"""
        try:
            # Positionsanalyse
            position_analysis = {
                "distance_category": self._categorize_distance(position.distance_from_center),
                "quadrant": self._determine_quadrant(position.x, position.y),
                "activity_rank": self._rank_activity(position.activity_score),
                "risk_rank": self._rank_risk(position.risk_score)
            }
            
            # Verbindungsanalyse
            connection_analysis = {
                "total_connections": len(connections),
                "strong_connections": len([c for c in connections if c.strength >= 0.7]),
                "weak_connections": len([c for c in connections if c.strength < 0.3]),
                "most_connected_to": max(
                    [(c.to_wallet, c.strength) for c in connections if c.from_wallet == position.wallet_address],
                    key=lambda x: x[1],
                    default=(None, 0)
                )[0],
                "most_connected_from": max(
                    [(c.from_wallet, c.strength) for c in connections if c.to_wallet == position.wallet_address],
                    key=lambda x: x[1],
                    default=(None, 0)
                )[0]
            }
            
            # Verhaltensmuster
            behavior_patterns = {
                "activity_pattern": self._analyze_activity_pattern(position, connections),
                "interaction_frequency": self._analyze_interaction_frequency(connections),
                "value_flow_pattern": self._analyze_value_flow(connections)
            }
            
            return {
                "position_analysis": position_analysis,
                "connection_analysis": connection_analysis,
                "behavior_patterns": behavior_patterns
            }
            
        except Exception as e:
            logger.error(f"Error analyzing wallet in detail: {e}")
            return {}
    
    def _categorize_distance(self, distance: float) -> str:
        """Kategorisiert die Entfernung vom Zentrum"""
        if distance < 0.3:
            return "core"
        elif distance < 0.6:
            return "inner_ring"
        else:
            return "outer_ring"
    
    def _determine_quadrant(self, x: float, y: float) -> str:
        """Bestimmt den Quadranten der Position"""
        if x >= 0.5 and y < 0.5:
            return "Q1"  # Oben rechts
        elif x < 0.5 and y < 0.5:
            return "Q2"  # Oben links
        elif x < 0.5 and y >= 0.5:
            return "Q3"  # Unten links
        else:
            return "Q4"  # Unten rechts
    
    def _rank_activity(self, activity_score: float) -> str:
        """Bewertet die Aktivität einer Wallet"""
        if activity_score >= 0.8:
            return "very_high"
        elif activity_score >= 0.6:
            return "high"
        elif activity_score >= 0.4:
            return "medium"
        else:
            return "low"
    
    def _rank_risk(self, risk_score: float) -> str:
        """Bewertet das Risiko einer Wallet"""
        if risk_score >= 0.7:
            return "high"
        elif risk_score >= 0.4:
            return "medium"
        else:
            return "low"
    
    def _analyze_activity_pattern(self, position, connections) -> str:
        """Analysiert das Aktivitätsmuster"""
        connection_count = len(connections)
        activity_score = position.activity_score
        
        if connection_count > 5 and activity_score > 0.7:
            return "hub"
        elif connection_count <= 2 and activity_score < 0.4:
            return "peripheral"
        else:
            return "regular"
    
    def _analyze_interaction_frequency(self, connections) -> str:
        """Analysiert die Interaktionsfrequenz"""
        if not connections:
            return "isolated"
        
        # Zeitliche Verteilung der Interaktionen analysieren
        now = datetime.utcnow()
        recent_interactions = sum(1 for conn in connections 
                                 if (now - conn.last_interaction).days < 7)
        
        total_interactions = len(connections)
        
        if recent_interactions / total_interactions > 0.7:
            return "very_active"
        elif recent_interactions / total_interactions > 0.3:
            return "active"
        else:
            return "dormant"
    
    def _analyze_value_flow(self, connections) -> str:
        """Analysiert den Wertfluss"""
        if not connections:
            return "no_flow"
        
        total_value = sum(conn.total_value for conn in connections)
        
        if total_value > 1000000:
            return "high_value"
        elif total_value > 100000:
            return "medium_value"
        else:
            return "low_value"

# Controller-Instanz
radar_controller = RadarController()
