"""
Rate limiter implementation for controlling API request rates.
"""

import time
from typing import Dict, List


class RateLimiter:
    """Einfacher Rate-Limiter für API-Anfragen"""
    
    def __init__(self):
        self.request_timestamps: Dict[str, List[float]] = {}
        self.limits: Dict[str, Dict[str, int]] = {}
    
    async def acquire(self, service_name: str, max_requests: int, time_window: int) -> bool:
        """Prüft, ob eine Anfrage gemacht werden kann"""
        current_time = time.time()
        
        if service_name not in self.request_timestamps:
            self.request_timestamps[service_name] = []
        
        # Alte Zeitstempel entfernen
        window_start = current_time - time_window
        self.request_timestamps[service_name] = [
            ts for ts in self.request_timestamps[service_name] if ts > window_start
        ]
        
        # Prüfen, ob das Limit erreicht ist
        if len(self.request_timestamps[service_name]) >= max_requests:
            return False
        
        # Neue Anfrage hinzufügen
        self.request_timestamps[service_name].append(current_time)
        return True
