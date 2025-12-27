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

def calculate_wallet_metrics(
    balance_eth: float,
    transactions: List[Dict],
    eth_price: float = 2921.68
) -> Dict:
    """
    Calculate wallet metrics from balance and transactions.
    
    Args:
        balance_eth: Current ETH balance
        transactions: List of transaction dicts from Etherscan
        eth_price: Current ETH price in USD
    
    Returns:
        Dict with metrics: balance_usd, volumes, activity, etc.
    """
    try:
        balance_usd = balance_eth * eth_price
        
        # Initialize metrics
        now = datetime.now()
        thirty_days_ago = now - timedelta(days=30)
        seven_days_ago = now - timedelta(days=7)
        
        recent_30d = []
        recent_7d = []
        total_volume = 0
        tx_count = len(transactions)
        
        # Process transactions
        for tx in transactions:
            try:
                # Safe Wei-to-ETH conversion
                tx_value_wei = int(tx.get("value", 0))
                tx_value_eth = tx_value_wei / 1e18
                tx_value_usd = tx_value_eth * eth_price
                
                # Sanity check
                if tx_value_eth > 100_000:
                    continue
                
                tx_time = datetime.fromtimestamp(int(tx.get("timeStamp", 0)))
                
                total_volume += tx_value_usd
                
                if tx_time >= thirty_days_ago:
                    recent_30d.append(tx_value_usd)
                if tx_time >= seven_days_ago:
                    recent_7d.append(tx_value_usd)
                    
            except (ValueError, OverflowError):
                continue
        
        # Calculate averages
        avg_transfer_size = total_volume / tx_count if tx_count > 0 else 0
        
        # Final sanity check
        if total_volume > 1_000_000_000_000:
            total_volume = 0
            avg_transfer_size = 0
        
        # Calculate last activity
        last_activity_text = "Unknown"
        last_tx_timestamp = 0
        is_active = False
        
        if transactions:
            try:
                last_tx_timestamp = int(transactions[0].get("timeStamp", 0))
                time_diff = now.timestamp() - last_tx_timestamp
                
                if time_diff < 3600:
                    last_activity_text = f"{int(time_diff / 60)}m ago"
                    is_active = True
                elif time_diff < 86400:
                    last_activity_text = f"{int(time_diff / 3600)}h ago"
                    is_active = True
                elif time_diff < 604800:
                    last_activity_text = f"{int(time_diff / 86400)}d ago"
                    is_active = True
                else:
                    last_activity_text = f"{int(time_diff / 86400)}d ago"
                    is_active = False
            except (ValueError, OverflowError):
                pass
        
        return {
            "balance_eth": float(balance_eth),
            "balance_usd": float(balance_usd),
            "transaction_count": tx_count,
            "total_volume": float(total_volume),
            "recent_volume_30d": float(sum(recent_30d)),
            "recent_volume_7d": float(sum(recent_7d)),
            "avg_transfer_size": float(avg_transfer_size),
            "last_activity_text": last_activity_text,
            "last_tx_timestamp": last_tx_timestamp,
            "is_active": is_active
        }
        
    except Exception as e:
        return {
            "balance_eth": 0,
            "balance_usd": 0,
            "transaction_count": 0,
            "total_volume": 0,
            "recent_volume_30d": 0,
            "recent_volume_7d": 0,
            "avg_transfer_size": 0,
            "last_activity_text": "Unknown",
            "last_tx_timestamp": 0,
            "is_active": False
        }


def generate_activity_chart(
    total_volume: float,
    transaction_count: int,
    transactions: Optional[List[Dict]] = None,
    days: int = 7
) -> List[Dict]:
    """
    Generate activity chart data for last N days.
    
    Args:
        total_volume: Total transaction volume
        transaction_count: Total number of transactions
        transactions: Optional list of transactions for accurate data
        days: Number of days to generate
    
    Returns:
        List of {date, volume} dicts
    """
    from datetime import datetime, timedelta
    
    activity_data = []
    
    if transactions and len(transactions) > 0:
        # Use real transaction data
        daily_volumes = {}
        eth_price = 2921.68  # TODO: Use price oracle
        
        for tx in transactions:
            try:
                tx_date = datetime.fromtimestamp(int(tx["timeStamp"])).strftime('%m/%d')
                tx_value = int(tx.get("value", 0)) / 1e18 * eth_price
                
                if tx_value > 100_000:  # Skip suspicious
                    continue
                
                if tx_date not in daily_volumes:
                    daily_volumes[tx_date] = 0
                daily_volumes[tx_date] += tx_value
            except:
                continue
        
        # Create chart data
        for i in range(days):
            date = (datetime.now() - timedelta(days=days-1-i)).strftime('%m/%d')
            volume = daily_volumes.get(date, 0)
            activity_data.append({"date": date, "volume": round(volume, 2)})
    else:
        # Fallback: Estimate from total
        base_daily_volume = total_volume / 30 if total_volume else 0
        
        for i in range(days):
            date = (datetime.now() - timedelta(days=days-1-i)).strftime('%m/%d')
            variation = 0.7 + (i % 3) * 0.3
            volume = base_daily_volume * variation
            activity_data.append({"date": date, "volume": round(volume, 2)})
    
    return activity_data


def generate_transfer_size_chart(
    avg_transfer: float,
    transactions: Optional[List[Dict]] = None,
    days: int = 7
) -> List[Dict]:
    """
    Generate transfer size trend chart.
    
    Args:
        avg_transfer: Average transfer size
        transactions: Optional list of transactions
        days: Number of days
    
    Returns:
        List of {date, size} dicts
    """
    from datetime import datetime, timedelta
    
    transfer_size_data = []
    
    if transactions and len(transactions) > 0:
        # Use real data
        daily_sizes = {}
        daily_counts = {}
        eth_price = 2921.68
        
        for tx in transactions:
            try:
                tx_date = datetime.fromtimestamp(int(tx["timeStamp"])).strftime('%m/%d')
                tx_value = int(tx.get("value", 0)) / 1e18 * eth_price
                
                if tx_value > 100_000:
                    continue
                
                if tx_date not in daily_sizes:
                    daily_sizes[tx_date] = 0
                    daily_counts[tx_date] = 0
                
                daily_sizes[tx_date] += tx_value
                daily_counts[tx_date] += 1
            except:
                continue
        
        # Create chart data
        for i in range(days):
            date = (datetime.now() - timedelta(days=days-1-i)).strftime('%m/%d')
            count = daily_counts.get(date, 0)
            avg_size = daily_sizes.get(date, 0) / count if count > 0 else 0
            transfer_size_data.append({"date": date, "size": round(avg_size, 2)})
    else:
        # Fallback: Estimate
        for i in range(days):
            date = (datetime.now() - timedelta(days=days-1-i)).strftime('%m/%d')
            size_variation = 0.8 + (i % 4) * 0.2
            size = avg_transfer * size_variation
            transfer_size_data.append({"date": date, "size": round(size, 2)})
    
    return transfer_size_data
