# blockchain/rate_limiters/rate_limiter.py
import time
from typing import Dict, Optional
from threading import Lock
from collections import deque

class RateLimiter:
    """Rate limiter for API calls"""
    
    def __init__(self, calls_per_second: float = 1.0, burst: int = 1):
        self.calls_per_second = calls_per_second
        self.burst = burst
        self.min_interval = 1.0 / calls_per_second
        self.call_times: deque = deque(maxlen=burst)
        self.lock = Lock()
    
    def wait_if_needed(self) -> None:
        """Wait if rate limit would be exceeded"""
        with self.lock:
            now = time.time()
            
            # Remove old calls outside burst window
            while self.call_times and now - self.call_times[0] > 1.0:
                self.call_times.popleft()
            
            # Check if we need to wait
            if len(self.call_times) >= self.burst:
                sleep_time = self.min_interval - (now - self.call_times[-1])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
            
            self.call_times.append(now)
    
    def __enter__(self):
        self.wait_if_needed()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
