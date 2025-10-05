# blockchain/rate_limiters/rate_limiter.py
import time
from typing import Deque
from threading import Lock
from collections import deque

class RateLimiter:
    """Rate limiter for API calls with a sliding window"""
    
    def __init__(self, max_calls: int = 1, time_window: float = 1.0):
        """
        Initialize the rate limiter.
        
        Args:
            max_calls: Maximum number of calls allowed in the time window.
            time_window: Time window in seconds.
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.call_times: Deque[float] = deque(maxlen=max_calls)
        self.lock = Lock()
    
    def wait_if_needed(self) -> None:
        """Wait if rate limit would be exceeded"""
        with self.lock:
            now = time.time()
            
            # Remove old calls outside the time window
            while self.call_times and now - self.call_times[0] > self.time_window:
                self.call_times.popleft()
            
            # Check if we need to wait
            if len(self.call_times) >= self.max_calls:
                # Calculate how long to wait until the oldest call is outside the window
                sleep_time = self.time_window - (now - self.call_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
            
            # Record the current call time
            self.call_times.append(now)
    
    def __enter__(self):
        self.wait_if_needed()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
