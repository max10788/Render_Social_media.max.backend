"""Utility functions for wallet analysis."""
from datetime import datetime, timedelta
from typing import List, Dict, Any


def convert_to_usd(value_btc: float, btc_price: float = 50000) -> float:
    """Convert BTC value to USD."""
    return value_btc * btc_price


def calculate_time_difference(timestamp1: int, timestamp2: int, unit: str = 'days') -> float:
    """Calculate time difference between two timestamps."""
    diff_seconds = abs(timestamp2 - timestamp1)
    
    if unit == 'days':
        return diff_seconds / 86400
    elif unit == 'hours':
        return diff_seconds / 3600
    elif unit == 'minutes':
        return diff_seconds / 60
    return diff_seconds


def normalize_score(value: float, min_val: float = 0, max_val: float = 1) -> float:
    """Normalize a value to [0, 1] range."""
    if max_val == min_val:
        return 0.5
    normalized = (value - min_val) / (max_val - min_val)
    return max(0, min(1, normalized))


def calculate_entropy(values: List[float]) -> float:
    """Calculate entropy of a value distribution."""
    if not values:
        return 0
    
    import math
    from collections import Counter
    
    # Discretize values into bins
    bins = 10
    min_val, max_val = min(values), max(values)
    if min_val == max_val:
        return 0
    
    bin_size = (max_val - min_val) / bins
    binned = [int((v - min_val) / bin_size) if v != max_val else bins - 1 for v in values]
    
    counts = Counter(binned)
    total = len(values)
    entropy = -sum((count / total) * math.log2(count / total) for count in counts.values())
    
    return entropy


def calculate_gini_coefficient(values: List[float]) -> float:
    """
    Calculate Gini coefficient for inequality measurement.
    
    Returns 0 for empty lists, single values, or zero sum.
    Returns value between 0 and 1 where 0 = perfect equality, 1 = perfect inequality.
    """
    if not values or len(values) < 2:
        return 0
    
    sorted_values = sorted(values)
    n = len(sorted_values)
    total_sum = sum(sorted_values)
    
    # ✅ FIX: Guard against division by zero
    if total_sum == 0:
        return 0
    
    cumsum = sum((i + 1) * val for i, val in enumerate(sorted_values))
    
    return (2 * cumsum) / (n * total_sum) - (n + 1) / n


def is_round_amount(value: float, tolerance: float = 0.01) -> bool:
    """Check if value is a round number (e.g., 0.1, 1.0, 10.0)."""
    powers = [0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000]
    return any(abs(value - p) / p < tolerance for p in powers if p > 0)
