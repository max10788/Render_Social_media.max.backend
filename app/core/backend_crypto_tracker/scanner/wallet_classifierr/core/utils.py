"""Utility functions for wallet analysis and classification metrics."""

import math
from datetime import datetime, timedelta
from typing import List, Dict, Any
from collections import Counter


def convert_to_usd(value_btc: float, btc_price: float = 50000.0) -> float:
    """
    Convert BTC value to USD.
    
    Handles both BTC and satoshi inputs:
    - If value < 1 and very small (e.g., < 1e-4), assumes satoshis and converts to BTC first.
    - Otherwise treats input as BTC.
    
    Args:
        value_btc: Amount in BTC (or satoshis if extremely small)
        btc_price: Current BTC price in USD
        
    Returns:
        USD value
    """
    # Heuristic: if value is tiny, likely in satoshis
    if value_btc < 1e-4:
        value_btc = value_btc / 100_000_000  # Convert satoshis to BTC

    return value_btc * btc_price


def calculate_time_difference(timestamp1: int, timestamp2: int, unit: str = 'days') -> float:
    """Calculate time difference between two Unix timestamps."""
    diff_seconds = abs(timestamp2 - timestamp1)
    
    if unit == 'days':
        return diff_seconds / 86400.0
    elif unit == 'hours':
        return diff_seconds / 3600.0
    elif unit == 'minutes':
        return diff_seconds / 60.0
    else:
        return float(diff_seconds)


def normalize_score(value: float, min_val: float = 0.0, max_val: float = 1.0) -> float:
    """Normalize a value to the [0, 1] range."""
    if max_val == min_val:
        return 0.5
    normalized = (value - min_val) / (max_val - min_val)
    return max(0.0, min(1.0, normalized))


def calculate_entropy(values: List[float]) -> float:
    """
    Calculate Shannon entropy of a list of numeric values.
    Higher entropy = more randomness/diversity (e.g., in transaction timing or amounts).
    
    For continuous data, bins values into 10 quantiles or hour-based bins (for timestamps).
    
    Args:
        values: List of numeric values (e.g., timestamps, amounts)
        
    Returns:
        Entropy value ≥ 0
    """
    if not values:
        return 0.0

    # Special handling for timestamps (likely in seconds): bin by hour
    if all(v > 1e9 for v in values):  # Unix timestamps are > 1e9
        bins = [int(v // 3600) for v in values]
    else:
        # General continuous values → 10 equal-width bins
        min_val, max_val = min(values), max(values)
        if min_val == max_val:
            return 0.0
        bin_width = (max_val - min_val) / 10
        bins = [min(9, int((v - min_val) / bin_width)) for v in values]

    counts = Counter(bins)
    total = sum(counts.values())
    entropy = 0.0
    for count in counts.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


def calculate_gini_coefficient(values: List[float]) -> float:
    """
    Calculate Gini coefficient for inequality measurement.
    0 = perfect equality, 1 = maximum inequality.
    
    Ignores zero or negative values.
    
    Args:
        values: List of numeric values (e.g., transaction amounts, balances)
        
    Returns:
        Gini coefficient in [0, 1]
    """
    if not values or len(values) < 2:
        return 0.0

    # Filter out non-positive values
    positive_values = [v for v in values if v > 0]
    if len(positive_values) < 2:
        return 0.0

    sorted_vals = sorted(positive_values)
    n = len(sorted_vals)
    total_sum = sum(sorted_vals)

    if total_sum == 0:
        return 0.0

    # Efficient Gini calculation: G = 1 - (2 * Σ(i * x_i)) / (n * Σx)
    cumsum = sum((i + 1) * val for i, val in enumerate(sorted_vals))
    gini = (2 * cumsum) / (n * total_sum) - (n + 1) / n
    return max(0.0, min(1.0, gini))


def is_round_amount(value: float, tolerance: float = 0.01) -> bool:
    """
    Check if a value is a "round" amount (common in mixers or user-friendly transfers).
    
    Checks against common round values and powers of 10.
    
    Args:
        value: The amount to check
        tolerance: Relative tolerance for matching (default: 1%)
        
    Returns:
        True if value is considered round
    """
    if value <= 0:
        return False

    # Common round BTC/ETH amounts
    round_values = [
        0.001, 0.005, 0.01, 0.05, 0.1, 0.5,
        1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0
    ]

    for rv in round_values:
        if rv > 0 and abs(value - rv) / max(value, rv) < tolerance:
            return True

    # Also check if it's approximately a power of 10
    if value >= 1:
        log_val = math.log10(value)
        if abs(log_val - round(log_val)) < tolerance:
            return True

    return False


def calculate_consolidation_ratio(inputs_per_tx: Dict[str, int], outputs_per_tx: Dict[str, int]) -> float:
    """
    Calculate the ratio of consolidation transactions.
    Consolidation = many inputs (≥5), few outputs (typically 1).
    
    Args:
        inputs_per_tx: Mapping tx_hash → number of inputs
        outputs_per_tx: Mapping tx_hash → number of outputs
        
    Returns:
        Consolidation ratio in [0, 1]
    """
    if not inputs_per_tx:
        return 0.0

    consolidation_count = 0
    for tx_hash, input_count in inputs_per_tx.items():
        output_count = outputs_per_tx.get(tx_hash, 0)
        if input_count >= 5 and output_count == 1:
            consolidation_count += 1

    return consolidation_count / len(inputs_per_tx)


def calculate_balance_retention(current_balance: float, total_received: float) -> float:
    """
    Calculate balance retention ratio (HODLing indicator).
    
    Args:
        current_balance: Current wallet balance
        total_received: Total amount ever received
        
    Returns:
        Retention ratio in [0, 1]
    """
    if total_received <= 0:
        return 0.0
    return min(1.0, current_balance / total_received)


def calculate_turnover_rate(total_sent: float, current_balance: float) -> float:
    """
    Calculate turnover rate (trading activity indicator).
    
    Args:
        total_sent: Total amount sent from wallet
        current_balance: Current wallet balance
        
    Returns:
        Turnover rate (can be >1 for very active wallets)
    """
    if current_balance <= 0:
        return 0.0
    return total_sent / current_balance


def detect_equal_outputs(output_values: List[float], tolerance: float = 0.001) -> float:
    """
    Detect proportion of equal-value outputs (common in mixers).
    
    Args:
        output_values: List of transaction output amounts
        tolerance: Absolute tolerance for equality (e.g., 0.001 BTC)
        
    Returns:
        Proportion of outputs that have duplicates [0, 1]
    """
    if not output_values or len(output_values) < 2:
        return 0.0

    # Round to mitigate floating-point errors
    rounded = [round(v, 8) for v in output_values]
    counts = Counter(rounded)
    duplicate_count = sum(count for count in counts.values() if count > 1)

    return duplicate_count / len(output_values)
