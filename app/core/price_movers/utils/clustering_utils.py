"""
Clustering Utilities - Helper-Funktionen

Pure Python/NumPy - NO sklearn!
"""

from datetime import datetime
from typing import List


def calculate_size_category(value_usd: float) -> int:
    """
    Kategorisiert Trade-Größe
    
    Returns:
        0: Micro (< $1k)
        1: Small ($1k-$10k)
        2: Medium ($10k-$50k)
        3: Large ($50k-$100k)
        4: Very Large ($100k-$500k)
        5: Whale (> $500k)
    """
    if value_usd < 1_000:
        return 0
    elif value_usd < 10_000:
        return 1
    elif value_usd < 50_000:
        return 2
    elif value_usd < 100_000:
        return 3
    elif value_usd < 500_000:
        return 4
    else:
        return 5


def calculate_price_level(price: float, candle_mid: float) -> int:
    """
    Berechnet relative Price Level
    
    Returns:
        1: Above midpoint
        -1: Below midpoint
    """
    return 1 if price >= candle_mid else -1


def calculate_time_bucket(
    timestamp: datetime,
    candle_start: datetime,
    bucket_seconds: int
) -> int:
    """
    Berechnet Time Bucket
    
    Args:
        timestamp: Trade Timestamp
        candle_start: Candle Start Time
        bucket_seconds: Bucket-Größe in Sekunden
        
    Returns:
        Bucket Index (z.B. 0-29 für 5min Candle mit 10s Buckets)
    """
    time_diff = (timestamp - candle_start).total_seconds()
    bucket = int(time_diff / bucket_seconds)
    return max(0, bucket)  # Negative vermeiden


def is_round_number(amount: float) -> bool:
    """
    Prüft ob Trade-Größe eine runde Zahl ist
    
    Runde Zahlen sind oft Indikator für:
    - Manuelle Trades
    - Bot Trades mit festen Größen
    """
    round_numbers = [
        0.1, 0.25, 0.5, 1.0, 2.0, 2.5, 5.0, 
        10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0
    ]
    
    for rn in round_numbers:
        # Exakte Übereinstimmung oder Vielfaches
        if abs(amount - rn) < 0.01:
            return True
        if amount > rn and abs(amount % rn) < 0.01:
            return True
    
    return False
