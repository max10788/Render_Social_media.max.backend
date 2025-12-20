import math
import numpy as np
from typing import List, Dict, Tuple, Optional
from datetime import datetime
from collections import Counter

def calculate_z_score(value: float, mean: float, std_dev: float) -> float:
    """
    Calculate Z-score for anomaly detection.
    Used for transfer size anomaly detection.
    """
    if std_dev == 0:
        return 0.0
    return (value - mean) / std_dev


def rolling_statistics(values: List[float], window_days: int = 30) -> Dict[str, float]:
    """
    Calculate rolling window statistics for a list of values.
    
    Returns:
        dict with 'mean', 'std', 'median', 'min', 'max'
    """
    if not values:
        return {'mean': 0, 'std': 0, 'median': 0, 'min': 0, 'max': 0}
    
    arr = np.array(values)
    return {
        'mean': float(np.mean(arr)),
        'std': float(np.std(arr)),
        'median': float(np.median(arr)),
        'min': float(np.min(arr)),
        'max': float(np.max(arr)),
        'percentile_90': float(np.percentile(arr, 90)),
        'percentile_95': float(np.percentile(arr, 95)),
        'percentile_99': float(np.percentile(arr, 99))
    }


def shannon_entropy(values: List[str]) -> float:
    """
    Calculate Shannon entropy for counterparty diversity.
    Higher entropy = more diverse counterparties.
    
    Used in wallet profiling to measure how distributed interactions are.
    """
    if not values:
        return 0.0
    
    counter = Counter(values)
    total = len(values)
    
    entropy = 0.0
    for count in counter.values():
        probability = count / total
        if probability > 0:
            entropy -= probability * math.log2(probability)
    
    return entropy


def calculate_similarity_score(
    wallet_a: Dict,
    wallet_b: Dict,
    weights: Optional[Dict[str, float]] = None
) -> float:
    """
    Calculate similarity score between two wallets for clustering.
    
    Similarity(A, B) = w1 * TransactionFrequency + 
                       w2 * TemporalProximity + 
                       w3 * AmountCorrelation +
                       w4 * SharedCounterparties
    
    Default weights from doc: w1=0.25, w2=0.3, w3=0.25, w4=0.2
    """
    if weights is None:
        weights = {
            'transaction_frequency': 0.25,
            'temporal_proximity': 0.30,
            'amount_correlation': 0.25,
            'shared_counterparties': 0.20
        }
    
    # Transaction frequency similarity (0-1)
    freq_a = wallet_a.get('transaction_frequency', 0)
    freq_b = wallet_b.get('transaction_frequency', 0)
    freq_similarity = 1 - min(abs(freq_a - freq_b) / max(freq_a, freq_b, 1), 1)
    
    # Temporal proximity (based on active hours overlap)
    hours_a = set(wallet_a.get('active_hours', []))
    hours_b = set(wallet_b.get('active_hours', []))
    temporal_similarity = len(hours_a & hours_b) / max(len(hours_a | hours_b), 1)
    
    # Amount correlation (based on median transaction values)
    median_a = wallet_a.get('median_transaction_usd', 0)
    median_b = wallet_b.get('median_transaction_usd', 0)
    amount_similarity = 1 - min(abs(median_a - median_b) / max(median_a, median_b, 1), 1)
    
    # Shared counterparties (Jaccard similarity)
    counterparties_a = set(wallet_a.get('counterparties', []))
    counterparties_b = set(wallet_b.get('counterparties', []))
    shared_similarity = len(counterparties_a & counterparties_b) / max(len(counterparties_a | counterparties_b), 1)
    
    # Weighted sum
    total_score = (
        weights['transaction_frequency'] * freq_similarity +
        weights['temporal_proximity'] * temporal_similarity +
        weights['amount_correlation'] * amount_similarity +
        weights['shared_counterparties'] * shared_similarity
    )
    
    return total_score


def sigmoid(x: float, midpoint: float = 0, steepness: float = 1) -> float:
    """
    Sigmoid function for score normalization.
    Used in transfer size scoring.
    
    Args:
        x: Input value
        midpoint: Value at which sigmoid returns 0.5
        steepness: How steep the curve is
    """
    return 1 / (1 + math.exp(-steepness * (x - midpoint)))


def calculate_transfer_size_score(usd_value: float) -> float:
    """
    Calculate transfer size score using sigmoid function.
    Scores based on USD value of transaction.
    
    Returns: Score 0-100
    """
    # Sigmoid with midpoint at $500K, returns 0-1
    normalized = sigmoid(usd_value, midpoint=500000, steepness=0.000002)
    return normalized * 100


def is_round_number(usd_value: float, tolerance: float = 0.01) -> Tuple[bool, Optional[str]]:
    """
    Detect if a USD value is a psychologically round number.
    OTC deals are often negotiated at round fiat values.
    
    Returns:
        (is_round, level) where level is 'million', 'five_million', 'ten_million', etc.
    """
    round_thresholds = [
        (10_000_000, 'ten_million'),
        (5_000_000, 'five_million'),
        (1_000_000, 'million'),
        (500_000, 'half_million'),
        (100_000, 'hundred_k'),
    ]
    
    for threshold, level in round_thresholds:
        # Check if value is within tolerance of threshold
        if abs(usd_value - threshold) / threshold <= tolerance:
            return True, level
        
        # Check if it's a multiple of threshold (e.g., 2M, 3M, etc.)
        if usd_value >= threshold:
            remainder = usd_value % threshold
            if remainder / threshold <= tolerance or (threshold - remainder) / threshold <= tolerance:
                return True, level
    
    return False, None


def calculate_transaction_velocity(
    timestamps: List[datetime],
    window_days: int = 7
) -> float:
    """
    Calculate transaction velocity (transactions per day).
    
    Args:
        timestamps: List of transaction timestamps
        window_days: Window to calculate over
    
    Returns:
        Average transactions per day
    """
    if not timestamps or len(timestamps) < 2:
        return 0.0
    
    sorted_times = sorted(timestamps)
    time_span = (sorted_times[-1] - sorted_times[0]).total_seconds() / 86400  # Convert to days
    
    if time_span == 0:
        return float(len(timestamps))
    
    return len(timestamps) / time_span


def percentile_rank(value: float, distribution: List[float]) -> float:
    """
    Calculate percentile rank of a value in a distribution.
    
    Returns: Percentile (0-100)
    """
    if not distribution:
        return 50.0
    
    sorted_dist = sorted(distribution)
    position = sum(1 for x in sorted_dist if x < value)
    
    return (position / len(sorted_dist)) * 100


def exponential_moving_average(values: List[float], alpha: float = 0.3) -> List[float]:
    """
    Calculate exponential moving average for time series smoothing.
    
    Args:
        values: Time series values
        alpha: Smoothing factor (0-1), higher = more weight to recent values
    """
    if not values:
        return []
    
    ema = [values[0]]
    for value in values[1:]:
        ema.append(alpha * value + (1 - alpha) * ema[-1])
    
    return ema


def calculate_correlation(values_a: List[float], values_b: List[float]) -> float:
    """
    Calculate Pearson correlation coefficient between two value series.
    
    Returns: Correlation coefficient (-1 to 1)
    """
    if len(values_a) != len(values_b) or len(values_a) < 2:
        return 0.0
    
    arr_a = np.array(values_a)
    arr_b = np.array(values_b)
    
    return float(np.corrcoef(arr_a, arr_b)[0, 1])


def normalize_score(score: float, min_val: float = 0, max_val: float = 100) -> float:
    """
    Normalize a score to 0-100 range.
    """
    return max(min_val, min(max_val, score))


def weighted_average(scores: Dict[str, float], weights: Dict[str, float]) -> float:
    """
    Calculate weighted average of multiple scores.
    
    Used in OTC scoring system.
    """
    total_weight = sum(weights.values())
    if total_weight == 0:
        return 0.0
    
    weighted_sum = sum(scores.get(key, 0) * weight for key, weight in weights.items())
    return weighted_sum / total_weight
