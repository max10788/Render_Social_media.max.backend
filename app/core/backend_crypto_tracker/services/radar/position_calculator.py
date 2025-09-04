import math
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class WalletPosition:
    wallet_address: str
    x: float
    y: float
    distance_from_center: float
    angle: float
    activity_score: float
    risk_score: float
    connection_count: int

@dataclass
class WalletConnection:
    from_wallet: str
    to_wallet: str
    strength: float
    interaction_count: int
    total_value: float
    last_interaction: datetime

class PositionCalculator:
    """Berechnet Positionen und Verbindungen für die Radar-Visualisierung"""
    
    def __init__(self):
        self.max_radius = 0.9  # Maximaler Radius des Radars (0-1)
        self.center_x = 0.5
        self.center_y = 0.5
    
    def calculate_wallet_positions(self, 
                                 wallet_data: List[Dict[str, Any]], 
                                 time_series_data: Dict[str, Any]) -> List[WalletPosition]:
        """Berechnet Positionen von Wallets im Radar-Koordinatensystem"""
        try:
            positions = []
            
            for wallet in wallet_data:
                # Aktivitäts-Score berechnen
                activity_score = self._calculate_activity_score(wallet, time_series_data)
                
                # Risiko-Score (bereits in den Daten vorhanden)
                risk_score = wallet.get("risk_score", 0.5)
                
                # Verbindungszahl
                connection_count = len(wallet.get("connections", []))
                
                # Distanz vom Zentrum basierend auf Aktivität
                distance_from_center = self._calculate_distance_from_center(activity_score)
                
                # Winkel basierend auf diversen Faktoren
                angle = self._calculate_wallet_angle(wallet, activity_score, risk_score)
                
                # Kartesische Koordinaten berechnen
                x, y = self._polar_to_cartesian(distance_from_center, angle)
                
                positions.append(WalletPosition(
                    wallet_address=wallet["address"],
                    x=x,
                    y=y,
                    distance_from_center=distance_from_center,
                    angle=angle,
                    activity_score=activity_score,
                    risk_score=risk_score,
                    connection_count=connection_count
                ))
            
            return positions
            
        except Exception as e:
            logger.error(f"Error calculating wallet positions: {e}")
            return []
    
    def calculate_wallet_connections(self, 
                                    wallet_data: List[Dict[str, Any]], 
                                    transaction_data: List[Dict[str, Any]]) -> List[WalletConnection]:
        """Berechnet Verbindungsstärke zwischen Wallets"""
        try:
            connections = []
            
            # Transaktionsdaten für schnellen Zugriff indizieren
            tx_index = {}
            for tx in transaction_data:
                from_addr = tx.get("from_address")
                to_addr = tx.get("to_address")
                
                if from_addr and to_addr:
                    if from_addr not in tx_index:
                        tx_index[from_addr] = {}
                    if to_addr not in tx_index:
                        tx_index[to_addr] = {}
                    
                    if to_addr not in tx_index[from_addr]:
                        tx_index[from_addr][to_addr] = []
                    tx_index[from_addr][to_addr].append(tx)
            
            # Verbindungen zwischen Wallets analysieren
            wallet_addresses = [wallet["address"] for wallet in wallet_data]
            
            for i, wallet1 in enumerate(wallet_data):
                addr1 = wallet1["address"]
                
                for j, wallet2 in enumerate(wallet_data[i+1:], i+1):
                    addr2 = wallet2["address"]
                    
                    # Transaktionen zwischen den Wallets finden
                    transactions_1_to_2 = tx_index.get(addr1, {}).get(addr2, [])
                    transactions_2_to_1 = tx_index.get(addr2, {}).get(addr1, [])
                    
                    all_transactions = transactions_1_to_2 + transactions_2_to_1
                    
                    if all_transactions:
                        # Verbindungsstärke berechnen
                        strength = self._calculate_connection_strength(all_transactions)
                        
                        total_value = sum(tx.get("value", 0) for tx in all_transactions)
                        last_interaction = max(
                            (tx.get("timestamp") for tx in all_transactions),
                            default=datetime.min
                        )
                        
                        connections.append(WalletConnection(
                            from_wallet=addr1,
                            to_wallet=addr2,
                            strength=strength,
                            interaction_count=len(all_transactions),
                            total_value=total_value,
                            last_interaction=last_interaction
                        ))
            
            # Nach Stärke sortieren
            connections.sort(key=lambda x: x.strength, reverse=True)
            
            return connections
            
        except Exception as e:
            logger.error(f"Error calculating wallet connections: {e}")
            return []
    
    def _calculate_activity_score(self, wallet: Dict[str, Any], time_series_data: Dict[str, Any]) -> float:
        """Berechnet Aktivitäts-Score für eine Wallet"""
        try:
            # Transaktionsanzahl
            transaction_count = wallet.get("transaction_count", 0)
            
            # Einzigartige Interaktionen
            unique_interactions = wallet.get("unique_interactions", 0)
            
            # Gesamtwert
            total_value = wallet.get("total_value", 0)
            
            # Letzte Aktivität
            last_activity = wallet.get("last_activity")
            recency_score = 0
            if last_activity:
                last_activity_dt = datetime.fromisoformat(last_activity)
                days_since_activity = (datetime.utcnow() - last_activity_dt).days
                recency_score = max(0, 1 - days_since_activity / 30)  # Innerhalb 30 Tagen
            
            # Normalisierte Scores
            transaction_score = min(1.0, transaction_count / 1000)
            interaction_score = min(1.0, unique_interactions / 100)
            value_score = min(1.0, total_value / 1000000)
            
            # Gewichteter Gesamtscore
            activity_score = (
                transaction_score * 0.4 +
                interaction_score * 0.3 +
                value_score * 0.2 +
                recency_score * 0.1
            )
            
            return activity_score
            
        except Exception as e:
            logger.error(f"Error calculating activity score: {e}")
            return 0.5
    
    def _calculate_distance_from_center(self, activity_score: float) -> float:
        """Berechnet Distanz vom Zentrum basierend auf Aktivität"""
        # Höhere Aktivität = näher am Zentrum
        # Aktivitäts-Score von 0-1, Distanz von max_radius bis 0.1
        return self.max_radius - (activity_score * (self.max_radius - 0.1))
    
    def _calculate_wallet_angle(self, wallet: Dict[str, Any], activity_score: float, risk_score: float) -> float:
        """Berechnet Winkel für die Wallet-Position im Radar"""
        try:
            # Hash der Wallet-Adresse für konsistente Positionierung
            address_hash = hash(wallet["address"])
            
            # Basis-Winkel aus Hash ableiten
            base_angle = (address_hash % 360) * (math.pi / 180)
            
            # Risiko-basierte Anpassung
            risk_adjustment = (risk_score - 0.5) * 0.5  # -0.25 bis +0.25
            
            # Aktivitäts-basierte Anpassung
            activity_adjustment = (activity_score - 0.5) * 0.3
            
            # Zeitbasierte Anpassung (für dynamische Visualisierung)
            time_factor = (datetime.utcnow().timestamp() / 3600) % (2 * math.pi)
            
            final_angle = (base_angle + risk_adjustment + activity_adjustment + time_factor) % (2 * math.pi)
            
            return final_angle
            
        except Exception as e:
            logger.error(f"Error calculating wallet angle: {e}")
            return 0
    
    def _polar_to_cartesian(self, distance: float, angle: float) -> Tuple[float, float]:
        """Wandelt Polarkoordinaten in kartesische Koordinaten um"""
        x = self.center_x + distance * math.cos(angle)
        y = self.center_y + distance * math.sin(angle)
        return x, y
    
    def _calculate_connection_strength(self, transactions: List[Dict[str, Any]]) -> float:
        """Berechnet die Stärke einer Verbindung zwischen Wallets"""
        try:
            if not transactions:
                return 0.0
            
            # Anzahl der Transaktionen
            count_weight = min(1.0, len(transactions) / 50)
            
            # Gesamtwert
            total_value = sum(tx.get("value", 0) for tx in transactions)
            value_weight = min(1.0, total_value / 100000)
            
            # Reziprozität (beidseitige Transaktionen)
            reciprocity_factor = 1.0  # Würde in echter Implementierung berechnet
            
            # Zeitliche Nähe (jüngere Transaktionen = stärkere Verbindung)
            now = datetime.utcnow()
            recency_weight = 0
            for tx in transactions:
                tx_time = tx.get("timestamp")
                if tx_time:
                    tx_datetime = datetime.fromisoformat(tx_time) if isinstance(tx_time, str) else tx_time
                    days_ago = (now - tx_datetime).days
                    recency_weight += max(0, 1 - days_ago / 30)
            
            recency_weight = min(1.0, recency_weight / len(transactions))
            
            # Gewichtete Gesamtbewertung
            strength = (
                count_weight * 0.4 +
                value_weight * 0.3 +
                reciprocity_factor * 0.1 +
                recency_weight * 0.2
            )
            
            return strength
            
        except Exception as e:
            logger.error(f"Error calculating connection strength: {e}")
            return 0.0
