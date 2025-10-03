# blockchain/utils/time_utils.py
from datetime import datetime, timedelta
from typing import Tuple, Optional
import time

def to_timestamp(dt: datetime) -> int:
    """Convert datetime to unix timestamp"""
    return int(dt.timestamp())

def from_timestamp(timestamp: int) -> datetime:
    """Convert unix timestamp to datetime"""
    return datetime.fromtimestamp(timestamp)

def get_time_range(
    period: str = "24h",
    end_time: Optional[datetime] = None
) -> Tuple[datetime, datetime]:
    """Get time range for given period"""
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
