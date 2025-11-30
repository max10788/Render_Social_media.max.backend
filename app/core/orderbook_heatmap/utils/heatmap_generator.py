"""
Heatmap-Generierung und Visualisierungs-Utils
"""
from typing import List, Dict, Tuple, Optional
import numpy as np
from datetime import datetime

from ..models.heatmap import HeatmapSnapshot, HeatmapTimeSeries, HeatmapConfig


class HeatmapGenerator:
    """
    Utility-Klasse für Heatmap-Generierung und -Visualisierung
    """
    
    @staticmethod
    def normalize_liquidity(
        matrix: List[List[float]],
        method: str = "minmax"
    ) -> List[List[float]]:
        """
        Normalisiert Liquiditäts-Matrix
        
        Args:
            matrix: 2D Matrix (Börsen x Preise)
            method: Normalisierungs-Methode ("minmax", "zscore", "log")
            
        Returns:
            Normalisierte Matrix
        """
        np_matrix = np.array(matrix)
        
        if method == "minmax":
            min_val = np_matrix.min()
            max_val = np_matrix.max()
            
            if max_val == min_val:
                return [[0.0 for _ in row] for row in matrix]
            
            normalized = (np_matrix - min_val) / (max_val - min_val)
            
        elif method == "zscore":
            mean = np_matrix.mean()
            std = np_matrix.std()
            
            if std == 0:
                return [[0.0 for _ in row] for row in matrix]
            
            normalized = (np_matrix - mean) / std
            # Clip zu [0, 1]
            normalized = np.clip((normalized + 3) / 6, 0, 1)
            
        elif method == "log":
            # Logarithmische Skalierung
            normalized = np.log1p(np_matrix)
            min_val = normalized.min()
            max_val = normalized.max()
            
            if max_val == min_val:
                return [[0.0 for _ in row] for row in matrix]
            
            normalized = (normalized - min_val) / (max_val - min_val)
            
        else:
            normalized = np_matrix
        
        return normalized.tolist()
    
    @staticmethod
    def apply_colormap(
        value: float,
        colormap: str = "viridis"
    ) -> Tuple[int, int, int]:
        """
        Wendet Colormap auf Wert an
        
        Args:
            value: Normalisierter Wert (0-1)
            colormap: Colormap-Name
            
        Returns:
            RGB-Tuple
        """
        value = np.clip(value, 0, 1)
        
        if colormap == "viridis":
            # Viridis colormap (vereinfacht)
            r = int(255 * (0.267 + 0.329 * value))
            g = int(255 * (0.005 + 0.880 * value))
            b = int(255 * (0.329 + 0.525 * value))
            
        elif colormap == "plasma":
            # Plasma colormap (vereinfacht)
            r = int(255 * (0.050 + 0.900 * value))
            g = int(255 * (0.030 + 0.800 * value ** 0.5))
            b = int(255 * (0.530 + 0.400 * (1 - value)))
            
        elif colormap == "hot":
            # Hot colormap (rot-gelb-weiß)
            if value < 0.33:
                r = int(255 * (value / 0.33))
                g = 0
                b = 0
            elif value < 0.66:
                r = 255
                g = int(255 * ((value - 0.33) / 0.33))
                b = 0
            else:
                r = 255
                g = 255
                b = int(255 * ((value - 0.66) / 0.34))
            
        elif colormap == "cool":
            # Cool colormap (cyan-magenta)
            r = int(255 * value)
            g = int(255 * (1 - value))
            b = 255
            
        elif colormap == "greyscale":
            # Greyscale
            grey = int(255 * value)
            r = g = b = grey
            
        else:
            # Default: Viridis
            r = int(255 * (0.267 + 0.329 * value))
            g = int(255 * (0.005 + 0.880 * value))
            b = int(255 * (0.329 + 0.525 * value))
        
        return (r, g, b)
    
    @staticmethod
    def generate_heatmap_colors(
        matrix: List[List[float]],
        colormap: str = "viridis",
        normalize: bool = True
    ) -> List[List[Tuple[int, int, int]]]:
        """
        Generiert Farben für Heatmap
        
        Args:
            matrix: 2D Liquiditäts-Matrix
            colormap: Colormap-Name
            normalize: Ob Matrix normalisiert werden soll
            
        Returns:
            2D Matrix mit RGB-Tuples
        """
        if normalize:
            matrix = HeatmapGenerator.normalize_liquidity(matrix)
        
        colors = []
        for row in matrix:
            color_row = [
                HeatmapGenerator.apply_colormap(value, colormap)
                for value in row
            ]
            colors.append(color_row)
        
        return colors
    
    @staticmethod
    def calculate_liquidity_gradient(
        snapshot: HeatmapSnapshot,
        exchange: str
    ) -> List[float]:
        """
        Berechnet Liquiditäts-Gradienten (Änderungsrate)
        
        Args:
            snapshot: HeatmapSnapshot
            exchange: Börsen-Name
            
        Returns:
            Liste von Gradienten pro Preis-Level
        """
        if not snapshot.price_levels:
            return []
        
        gradients = []
        prev_liquidity = None
        
        for level in snapshot.price_levels:
            liquidity = level.liquidity_by_exchange.get(exchange, 0.0)
            
            if prev_liquidity is not None:
                gradient = liquidity - prev_liquidity
            else:
                gradient = 0.0
            
            gradients.append(gradient)
            prev_liquidity = liquidity
        
        return gradients
    
    @staticmethod
    def find_liquidity_walls(
        snapshot: HeatmapSnapshot,
        threshold_percentile: float = 90
    ) -> Dict[str, List[float]]:
        """
        Findet "Liquiditäts-Wände" (große Limit Orders)
        
        Args:
            snapshot: HeatmapSnapshot
            threshold_percentile: Percentil-Schwelle
            
        Returns:
            Dict mit Börse -> Liste von Wall-Preisen
        """
        if not snapshot.price_levels:
            return {}
        
        # Sammle Liquidität pro Börse
        liquidity_by_exchange: Dict[str, List[Tuple[float, float]]] = {}
        
        for level in snapshot.price_levels:
            for exchange, liquidity in level.liquidity_by_exchange.items():
                if exchange not in liquidity_by_exchange:
                    liquidity_by_exchange[exchange] = []
                liquidity_by_exchange[exchange].append((level.price, liquidity))
        
        # Finde Walls
        walls = {}
        for exchange, data in liquidity_by_exchange.items():
            liquidities = [liq for _, liq in data]
            threshold = np.percentile(liquidities, threshold_percentile)
            
            wall_prices = [
                price for price, liq in data
                if liq >= threshold
            ]
            
            walls[exchange] = wall_prices
        
        return walls
    
    @staticmethod
    def calculate_imbalance(
        snapshot: HeatmapSnapshot,
        mid_price: float
    ) -> Dict[str, float]:
        """
        Berechnet Bid/Ask Liquiditäts-Imbalance
        
        Args:
            snapshot: HeatmapSnapshot
            mid_price: Mid-Price
            
        Returns:
            Dict mit Börse -> Imbalance (-1 bis 1)
        """
        if not snapshot.price_levels:
            return {}
        
        imbalances = {}
        
        # Sammle Liquidität über/unter mid_price
        for level in snapshot.price_levels:
            for exchange, liquidity in level.liquidity_by_exchange.items():
                if exchange not in imbalances:
                    imbalances[exchange] = {"bid": 0.0, "ask": 0.0}
                
                if level.price < mid_price:
                    imbalances[exchange]["bid"] += liquidity
                elif level.price > mid_price:
                    imbalances[exchange]["ask"] += liquidity
        
        # Berechne Imbalance Ratio
        result = {}
        for exchange, data in imbalances.items():
            total = data["bid"] + data["ask"]
            if total > 0:
                # -1 = nur Bids, +1 = nur Asks, 0 = balanced
                result[exchange] = (data["ask"] - data["bid"]) / total
            else:
                result[exchange] = 0.0
        
        return result
    
    @staticmethod
    def get_dominant_exchange(snapshot: HeatmapSnapshot) -> Optional[str]:
        """
        Findet Börse mit höchster Liquidität
        
        Args:
            snapshot: HeatmapSnapshot
            
        Returns:
            Börsen-Name oder None
        """
        if not snapshot.price_levels:
            return None
        
        total_by_exchange: Dict[str, float] = {}
        
        for level in snapshot.price_levels:
            for exchange, liquidity in level.liquidity_by_exchange.items():
                total_by_exchange[exchange] = (
                    total_by_exchange.get(exchange, 0.0) + liquidity
                )
        
        if not total_by_exchange:
            return None
        
        return max(total_by_exchange.items(), key=lambda x: x[1])[0]
    
    @staticmethod
    def calculate_volatility_proxy(
        timeseries: HeatmapTimeSeries,
        exchange: str
    ) -> float:
        """
        Berechnet Volatilitäts-Proxy aus Liquiditäts-Änderungen
        
        Args:
            timeseries: HeatmapTimeSeries
            exchange: Börsen-Name
            
        Returns:
            Volatilitäts-Wert
        """
        if len(timeseries.snapshots) < 2:
            return 0.0
        
        changes = []
        
        for i in range(1, len(timeseries.snapshots)):
            prev_snapshot = timeseries.snapshots[i - 1]
            curr_snapshot = timeseries.snapshots[i]
            
            # Vergleiche Gesamtliquidität
            prev_total = sum(
                level.liquidity_by_exchange.get(exchange, 0.0)
                for level in prev_snapshot.price_levels
            )
            curr_total = sum(
                level.liquidity_by_exchange.get(exchange, 0.0)
                for level in curr_snapshot.price_levels
            )
            
            if prev_total > 0:
                change = abs(curr_total - prev_total) / prev_total
                changes.append(change)
        
        if not changes:
            return 0.0
        
        return float(np.mean(changes))
