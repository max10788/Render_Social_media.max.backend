# blockchain/utils/time_utils.py
from datetime import datetime, timedelta
from typing import Tuple, Optional, Union
import time


def to_timestamp(dt: datetime) -> int:
    """
    Convert datetime to unix timestamp (seconds).
    
    Args:
        dt: Datetime object to convert
        
    Returns:
        Unix timestamp in seconds
    """
    return int(dt.timestamp())


def from_timestamp(timestamp: Union[int, float]) -> datetime:
    """
    Convert unix timestamp to datetime.
    
    Args:
        timestamp: Unix timestamp in seconds
        
    Returns:
        Datetime object
    """
    return datetime.fromtimestamp(timestamp)


def timestamp_to_datetime(timestamp: Union[int, float]) -> datetime:
    """
    Convert unix timestamp to datetime.
    Alias for from_timestamp() for better readability in some contexts.
    
    Args:
        timestamp: Unix timestamp in seconds (or milliseconds, will be auto-detected)
        
    Returns:
        Datetime object
    """
    # Auto-detect if timestamp is in milliseconds (CoinGecko returns milliseconds)
    # Timestamps after year 2286 in seconds would be ~10 billion
    if timestamp > 10_000_000_000:
        timestamp = timestamp / 1000
    
    return datetime.fromtimestamp(timestamp)


def datetime_to_timestamp(dt: datetime) -> int:
    """
    Convert datetime to unix timestamp.
    Alias for to_timestamp() for better readability.
    
    Args:
        dt: Datetime object
        
    Returns:
        Unix timestamp in seconds
    """
    return int(dt.timestamp())


def get_time_range(
    period: str = "24h",
    end_time: Optional[datetime] = None
) -> Tuple[datetime, datetime]:
    """
    Get time range for given period.
    
    Args:
        period: Time period string (1h/24h/7d/30d/90d/1y)
        end_time: End time (defaults to now)
        
    Returns:
        Tuple of (start_time, end_time)
    """
    if end_time is None:
        end_time = datetime.now()
    
    periods = {
        "1h": timedelta(hours=1),
        "24h": timedelta(days=1),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
        "90d": timedelta(days=90),
        "1y": timedelta(days=365)
    }
    
    delta = periods.get(period, timedelta(days=1))
    start_time = end_time - delta
    
    return start_time, end_time


def format_datetime(dt: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format datetime as string.
    
    Args:
        dt: Datetime object
        format_str: Format string (default: YYYY-MM-DD HH:MM:SS)
        
    Returns:
        Formatted datetime string
    """
    return dt.strftime(format_str)


def parse_datetime(date_str: str, format_str: str = "%Y-%m-%d %H:%M:%S") -> datetime:
    """
    Parse datetime string.
    
    Args:
        date_str: Datetime string
        format_str: Format string (default: YYYY-MM-DD HH:MM:SS)
        
    Returns:
        Datetime object
    """
    return datetime.strptime(date_str, format_str)


def get_current_timestamp() -> int:
    """
    Get current unix timestamp.
    
    Returns:
        Current unix timestamp in seconds
    """
    return int(time.time())


def time_ago(dt: datetime) -> str:
    """
    Get human-readable time difference from now.
    
    Args:
        dt: Past datetime
        
    Returns:
        Human-readable string (e.g., "2 hours ago", "3 days ago")
    """
    now = datetime.now()
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return f"{int(seconds)} seconds ago"
    elif seconds < 3600:
        return f"{int(seconds / 60)} minutes ago"
    elif seconds < 86400:
        return f"{int(seconds / 3600)} hours ago"
    elif seconds < 604800:
        return f"{int(seconds / 86400)} days ago"
    elif seconds < 2592000:
        return f"{int(seconds / 604800)} weeks ago"
    elif seconds < 31536000:
        return f"{int(seconds / 2592000)} months ago"
    else:
        return f"{int(seconds / 31536000)} years ago"


def is_recent(dt: datetime, threshold_hours: int = 24) -> bool:
    """
    Check if datetime is recent (within threshold).
    
    Args:
        dt: Datetime to check
        threshold_hours: Hours threshold (default: 24)
        
    Returns:
        True if within threshold
    """
    now = datetime.now()
    diff = now - dt
    return diff.total_seconds() < (threshold_hours * 3600)


def round_to_hour(dt: datetime) -> datetime:
    """
    Round datetime to nearest hour.
    
    Args:
        dt: Datetime to round
        
    Returns:
        Rounded datetime
    """
    return dt.replace(minute=0, second=0, microsecond=0)


def round_to_day(dt: datetime) -> datetime:
    """
    Round datetime to start of day (midnight).
    
    Args:
        dt: Datetime to round
        
    Returns:
        Rounded datetime
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def get_day_boundaries(dt: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """
    Get start and end of day for given datetime.
    
    Args:
        dt: Datetime (defaults to today)
        
    Returns:
        Tuple of (start_of_day, end_of_day)
    """
    if dt is None:
        dt = datetime.now()
    
    start_of_day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    return start_of_day, end_of_day


def milliseconds_to_datetime(ms: int) -> datetime:
    """
    Convert milliseconds timestamp to datetime.
    Useful for APIs that return timestamps in milliseconds.
    
    Args:
        ms: Timestamp in milliseconds
        
    Returns:
        Datetime object
    """
    return datetime.fromtimestamp(ms / 1000)


def datetime_to_milliseconds(dt: datetime) -> int:
    """
    Convert datetime to milliseconds timestamp.
    
    Args:
        dt: Datetime object
        
    Returns:
        Timestamp in milliseconds
    """
    return int(dt.timestamp() * 1000)


def get_utc_now() -> datetime:
    """
    Get current UTC datetime.
    
    Returns:
        Current UTC datetime
    """
    return datetime.utcnow()


def seconds_to_human_readable(seconds: Union[int, float]) -> str:
    """
    Convert seconds to human-readable duration string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Human-readable string (e.g., "2h 30m", "5d 3h")
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s" if secs > 0 else f"{minutes}m"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m" if minutes > 0 else f"{hours}h"
    else:
        days = int(seconds / 86400)
        hours = int((seconds % 86400) / 3600)
        return f"{days}d {hours}h" if hours > 0 else f"{days}d"
