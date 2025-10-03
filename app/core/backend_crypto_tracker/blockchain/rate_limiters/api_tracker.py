# blockchain/rate_limiters/api_tracker.py
from typing import Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import json

class APITracker:
    """Track API usage across providers"""
    
    def __init__(self):
        self.usage: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.limits: Dict[str, Dict[str, int]] = {}
        self.reset_times: Dict[str, datetime] = {}
    
    def set_limit(self, provider: str, endpoint: str, limit: int, 
                  reset_hours: int = 24) -> None:
        """Set rate limit for provider endpoint"""
        key = f"{provider}:{endpoint}"
        self.limits[key] = limit
        self.reset_times[key] = datetime.now() + timedelta(hours=reset_hours)
    
    def track_call(self, provider: str, endpoint: str) -> None:
        """Track an API call"""
        key = f"{provider}:{endpoint}"
        
        # Check if we need to reset
        if key in self.reset_times and datetime.now() > self.reset_times[key]:
            self.usage[provider][endpoint] = 0
            if key in self.limits:
                self.reset_times[key] = datetime.now() + timedelta(hours=24)
        
        self.usage[provider][endpoint] += 1
    
    def can_call(self, provider: str, endpoint: str) -> bool:
        """Check if we can make another call"""
        key = f"{provider}:{endpoint}"
        if key not in self.limits:
            return True
        
        current_usage = self.usage[provider].get(endpoint, 0)
        return current_usage < self.limits[key]
    
    def get_usage_stats(self) -> Dict[str, Dict[str, int]]:
        """Get current usage statistics"""
        return dict(self.usage)
    
    def save_stats(self, filepath: str) -> None:
        """Save usage stats to file"""
        stats = {
            'usage': dict(self.usage),
            'reset_times': {k: v.isoformat() for k, v in self.reset_times.items()},
            'timestamp': datetime.now().isoformat()
        }
        with open(filepath, 'w') as f:
            json.dump(stats, f, indent=2)
