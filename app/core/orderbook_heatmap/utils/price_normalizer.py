"""
Preis-Normalisierung und -Quantisierung Utils
"""
from typing import List, Tuple
import numpy as np
from decimal import Decimal


class PriceNormalizer:
    """
    Utility-Klasse für Preis-Normalisierung und -Quantisierung
    """
    
    @staticmethod
    def normalize_price_range(
        prices: List[float],
        target_min: float = 0.0,
        target_max: float = 1.0
    ) -> List[float]:
        """
        Normalisiert Preise auf einen Zielbereich
        
        Args:
            prices: Liste von Preisen
            target_min: Ziel-Minimum
            target_max: Ziel-Maximum
            
        Returns:
            Normalisierte Preise
        """
        if not prices:
            return []
        
        min_price = min(prices)
        max_price = max(prices)
        
        if min_price == max_price:
            return [target_min] * len(prices)
        
        normalized = [
            target_min + (target_max - target_min) * (p - min_price) / (max_price - min_price)
            for p in prices
        ]
        
        return normalized
    
    @staticmethod
    def quantize_price(price: float, bucket_size: float) -> float:
        """
        Quantisiert Preis auf Bucket-Größe
        
        Args:
            price: Original-Preis
            bucket_size: Bucket-Größe
            
        Returns:
            Quantisierter Preis
        """
        return round(price / bucket_size) * bucket_size
    
    @staticmethod
    def create_price_buckets(
        min_price: float,
        max_price: float,
        bucket_size: float
    ) -> List[float]:
        """
        Erstellt Preis-Buckets
        
        Args:
            min_price: Minimum-Preis
            max_price: Maximum-Preis
            bucket_size: Bucket-Größe
            
        Returns:
            Liste von Bucket-Preisen
        """
        buckets = []
        current = PriceNormalizer.quantize_price(min_price, bucket_size)
        
        while current <= max_price:
            buckets.append(current)
            current += bucket_size
        
        return buckets
    
    @staticmethod
    def get_price_bucket_index(price: float, buckets: List[float]) -> int:
        """
        Findet Index des nächsten Buckets
        
        Args:
            price: Preis
            buckets: Liste von Bucket-Preisen (sortiert)
            
        Returns:
            Index des nächsten Buckets
        """
        if not buckets:
            return 0
        
        # Binäre Suche
        left, right = 0, len(buckets) - 1
        
        while left <= right:
            mid = (left + right) // 2
            
            if buckets[mid] == price:
                return mid
            elif buckets[mid] < price:
                left = mid + 1
            else:
                right = mid - 1
        
        # Finde nächsten Bucket
        if left >= len(buckets):
            return len(buckets) - 1
        if right < 0:
            return 0
        
        # Wähle näheren Bucket
        if abs(buckets[left] - price) < abs(buckets[right] - price):
            return left
        return right
    
    @staticmethod
    def calculate_percentage_change(old_price: float, new_price: float) -> float:
        """
        Berechnet prozentuale Preisänderung
        
        Args:
            old_price: Alter Preis
            new_price: Neuer Preis
            
        Returns:
            Prozentuale Änderung
        """
        if old_price == 0:
            return 0.0
        
        return ((new_price - old_price) / old_price) * 100.0
    
    @staticmethod
    def get_significant_digits(price: float) -> int:
        """
        Ermittelt Anzahl signifikanter Stellen
        
        Args:
            price: Preis
            
        Returns:
            Anzahl signifikanter Stellen
        """
        if price == 0:
            return 0
        
        return -int(np.floor(np.log10(abs(price))))
    
    @staticmethod
    def round_to_significant(value: float, significant_digits: int) -> float:
        """
        Rundet auf signifikante Stellen
        
        Args:
            value: Wert
            significant_digits: Anzahl signifikanter Stellen
            
        Returns:
            Gerundeter Wert
        """
        if value == 0:
            return 0.0
        
        magnitude = 10 ** (significant_digits - int(np.floor(np.log10(abs(value)))) - 1)
        return round(value * magnitude) / magnitude
    
    @staticmethod
    def calculate_optimal_bucket_size(
        min_price: float,
        max_price: float,
        target_buckets: int = 100
    ) -> float:
        """
        Berechnet optimale Bucket-Größe
        
        Args:
            min_price: Minimum-Preis
            max_price: Maximum-Preis
            target_buckets: Ziel-Anzahl Buckets
            
        Returns:
            Optimale Bucket-Größe
        """
        if min_price == max_price:
            return 1.0
        
        price_range = max_price - min_price
        raw_bucket_size = price_range / target_buckets
        
        # Runde auf "schöne" Werte (1, 2, 5, 10, 20, 50, 100, ...)
        magnitude = 10 ** int(np.floor(np.log10(raw_bucket_size)))
        normalized = raw_bucket_size / magnitude
        
        if normalized <= 1:
            nice_size = 1
        elif normalized <= 2:
            nice_size = 2
        elif normalized <= 5:
            nice_size = 5
        else:
            nice_size = 10
        
        return nice_size * magnitude
