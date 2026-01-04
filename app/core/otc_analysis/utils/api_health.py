"""
API Health Monitor
==================

Monitors API health in real-time and auto-disables unhealthy APIs.
Implements circuit breaker pattern with cooldown periods.

Version: 1.0
Date: 2025-01-04
"""

from typing import Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class ApiHealthMonitor:
    """
    Circuit breaker for API calls.
    
    Features:
    - Tracks error rates per API
    - Auto-disables APIs with >50% failure rate
    - Re-enables after cooldown period (5 minutes)
    - Prevents cascading failures
    
    Usage:
        monitor = ApiHealthMonitor()
        
        if monitor.is_api_healthy("moralis"):
            result = call_moralis_api()
            if result:
                monitor.mark_success("moralis")
            else:
                monitor.mark_failure("moralis")
    """
    
    def __init__(self, cooldown_minutes: int = 5, error_threshold: float = 0.5):
        """
        Initialize health monitor.
        
        Args:
            cooldown_minutes: Minutes to wait before re-enabling failed API
            error_threshold: Error rate (0-1) that triggers circuit breaker
        """
        self.cooldown_minutes = cooldown_minutes
        self.error_threshold = error_threshold
        
        # API status tracking
        self.api_enabled: Dict[str, bool] = defaultdict(lambda: True)
        self.api_disabled_until: Dict[str, Optional[datetime]] = {}
        
        # Error tracking (sliding window)
        self.recent_calls: Dict[str, List[Dict]] = defaultdict(list)
        self.window_size = 20  # Track last 20 calls per API
        
        # Statistics
        self.total_success: Dict[str, int] = defaultdict(int)
        self.total_failures: Dict[str, int] = defaultdict(int)
        self.last_success: Dict[str, Optional[datetime]] = {}
    
    def is_api_healthy(self, api_name: str) -> bool:
        """
        Check if API is healthy and available.
        
        Args:
            api_name: Name of API to check
            
        Returns:
            True if API is healthy and should be used
        """
        # Check if currently disabled
        if not self.api_enabled[api_name]:
            # Check if cooldown period has passed
            if api_name in self.api_disabled_until:
                if datetime.now() >= self.api_disabled_until[api_name]:
                    # Re-enable API
                    logger.info(f"🔄 {api_name.capitalize()}: Cooldown expired - re-enabling")
                    self.api_enabled[api_name] = True
                    del self.api_disabled_until[api_name]
                    return True
                else:
                    # Still in cooldown
                    remaining = (self.api_disabled_until[api_name] - datetime.now()).total_seconds()
                    logger.debug(f"⏸️  {api_name.capitalize()}: In cooldown ({remaining:.0f}s remaining)")
                    return False
            return False
        
        # Check recent error rate
        error_rate = self._calculate_error_rate(api_name)
        
        if error_rate >= self.error_threshold:
            logger.warning(
                f"⚠️  {api_name.capitalize()}: High error rate ({error_rate:.1%}) - "
                f"disabling for {self.cooldown_minutes} minutes"
            )
            self._disable_api(api_name)
            return False
        
        return True
    
    def mark_success(self, api_name: str):
        """Mark a successful API call."""
        self._record_call(api_name, success=True)
        self.total_success[api_name] += 1
        self.last_success[api_name] = datetime.now()
    
    def mark_failure(self, api_name: str, error_type: Optional[str] = None):
        """Mark a failed API call."""
        self._record_call(api_name, success=False, error_type=error_type)
        self.total_failures[api_name] += 1
        
        # Check if should trigger circuit breaker
        error_rate = self._calculate_error_rate(api_name)
        if error_rate >= self.error_threshold:
            self._disable_api(api_name)
    
    def _record_call(self, api_name: str, success: bool, error_type: Optional[str] = None):
        """Record a call in the sliding window."""
        call_record = {
            'timestamp': datetime.now(),
            'success': success,
            'error_type': error_type
        }
        
        self.recent_calls[api_name].append(call_record)
        
        # Maintain window size
        if len(self.recent_calls[api_name]) > self.window_size:
            self.recent_calls[api_name].pop(0)
    
    def _calculate_error_rate(self, api_name: str) -> float:
        """Calculate error rate from recent calls."""
        if api_name not in self.recent_calls or not self.recent_calls[api_name]:
            return 0.0
        
        recent = self.recent_calls[api_name]
        failed = sum(1 for call in recent if not call['success'])
        total = len(recent)
        
        return failed / total if total > 0 else 0.0
    
    def _disable_api(self, api_name: str):
        """Disable an API with cooldown period."""
        self.api_enabled[api_name] = False
        self.api_disabled_until[api_name] = datetime.now() + timedelta(minutes=self.cooldown_minutes)
        
        logger.warning(
            f"🔴 {api_name.capitalize()}: Circuit breaker triggered - "
            f"disabled for {self.cooldown_minutes} minutes"
        )
    
    def disable_permanently(self, api_name: str):
        """Permanently disable an API (e.g., invalid key)."""
        self.api_enabled[api_name] = False
        # Set far future date
        self.api_disabled_until[api_name] = datetime.now() + timedelta(days=365)
        logger.error(f"🔴 {api_name.capitalize()}: Permanently disabled")
    
    def force_enable(self, api_name: str):
        """Manually re-enable an API."""
        self.api_enabled[api_name] = True
        if api_name in self.api_disabled_until:
            del self.api_disabled_until[api_name]
        logger.info(f"✅ {api_name.capitalize()}: Manually re-enabled")
    
    def get_status(self, api_name: str) -> Dict:
        """Get current status of an API."""
        error_rate = self._calculate_error_rate(api_name)
        
        status = {
            'enabled': self.api_enabled[api_name],
            'error_rate': error_rate,
            'total_success': self.total_success[api_name],
            'total_failures': self.total_failures[api_name],
            'last_success': self.last_success.get(api_name)
        }
        
        if api_name in self.api_disabled_until:
            status['disabled_until'] = self.api_disabled_until[api_name]
            status['cooldown_remaining'] = (
                self.api_disabled_until[api_name] - datetime.now()
            ).total_seconds()
        
        return status
    
    def get_all_statuses(self) -> Dict[str, Dict]:
        """Get status of all tracked APIs."""
        all_apis = set(
            list(self.api_enabled.keys()) +
            list(self.total_success.keys()) +
            list(self.total_failures.keys())
        )
        
        return {api: self.get_status(api) for api in all_apis}
    
    def reset(self):
        """Reset all health monitoring data."""
        self.api_enabled.clear()
        self.api_disabled_until.clear()
        self.recent_calls.clear()
        self.total_success.clear()
        self.total_failures.clear()
        self.last_success.clear()


# Export
__all__ = ['ApiHealthMonitor']
