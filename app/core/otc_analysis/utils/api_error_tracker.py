"""
API Error Tracker
=================

Tracks all API calls and their outcomes for monitoring and debugging.
Provides detailed error summaries at end of execution.

Version: 1.0
Date: 2025-01-04
"""

from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class ApiErrorTracker:
    """
    Singleton class to track API call success/failure across entire execution.
    
    Usage:
        from app.core.otc_analysis.utils.api_error_tracker import api_error_tracker
        
        # Track a call
        api_error_tracker.track_call("moralis", success=True)
        api_error_tracker.track_call("covalent", success=False, error="rate_limit")
        
        # Print summary at end
        api_error_tracker.print_summary()
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self.calls: Dict[str, Dict[str, int]] = defaultdict(lambda: {
            'success': 0,
            'failed': 0,
            'errors': defaultdict(int)
        })
        self.start_time = datetime.now()
    
    def track_call(
        self,
        api_name: str,
        success: bool,
        error: Optional[str] = None
    ):
        """
        Track a single API call.
        
        Args:
            api_name: Name of API (moralis, covalent, debank, etherscan)
            success: Whether call succeeded
            error: Error type if failed (rate_limit, timeout, invalid_key, etc.)
        """
        if success:
            self.calls[api_name]['success'] += 1
        else:
            self.calls[api_name]['failed'] += 1
            if error:
                self.calls[api_name]['errors'][error] += 1
    
    def get_stats(self, api_name: str) -> Dict:
        """Get statistics for a specific API."""
        if api_name not in self.calls:
            return {'success': 0, 'failed': 0, 'errors': {}}
        return dict(self.calls[api_name])
    
    def get_all_stats(self) -> Dict[str, Dict]:
        """Get statistics for all APIs."""
        return dict(self.calls)
    
    def get_total_calls(self) -> int:
        """Get total number of API calls made."""
        total = 0
        for api_data in self.calls.values():
            total += api_data['success'] + api_data['failed']
        return total
    
    def get_success_rate(self) -> float:
        """Get overall success rate across all APIs."""
        total_success = sum(data['success'] for data in self.calls.values())
        total_failed = sum(data['failed'] for data in self.calls.values())
        total = total_success + total_failed
        
        if total == 0:
            return 0.0
        return total_success / total
    
    def print_summary(self):
        """
        Print formatted summary of all API calls.
        
        Example output:
        ═══════════════════════════════════════════════════
        📊 API Call Summary
        ═══════════════════════════════════════════════════
        Moralis:    45 ✅  |  5 ❌  (rate_limit: 3, timeout: 2)
        Covalent:   38 ✅  |  0 ❌
        DeBank:      7 ✅  |  0 ❌
        Etherscan:   0 ✅  |  0 ❌  (not used)
        ───────────────────────────────────────────────────
        Total:      90 ✅  |  5 ❌  (Success Rate: 94.7%)
        Duration:   45.2 seconds
        ═══════════════════════════════════════════════════
        """
        if not self.calls:
            logger.info("📊 No API calls tracked")
            return
        
        duration = (datetime.now() - self.start_time).total_seconds()
        
        print("\n" + "═" * 55)
        print("📊 API Call Summary")
        print("═" * 55)
        
        total_success = 0
        total_failed = 0
        
        # Sort APIs by name for consistent output
        for api_name in sorted(self.calls.keys()):
            data = self.calls[api_name]
            success = data['success']
            failed = data['failed']
            
            total_success += success
            total_failed += failed
            
            # Format error details
            error_str = ""
            if data['errors']:
                error_parts = [f"{err}: {count}" for err, count in data['errors'].items()]
                error_str = f"  ({', '.join(error_parts)})"
            elif failed > 0:
                error_str = "  (various errors)"
            elif success == 0:
                error_str = "  (not used)"
            
            # Pad API name for alignment
            api_display = f"{api_name.capitalize()}:"
            print(f"{api_display:<12} {success:>3} ✅  |  {failed:>3} ❌{error_str}")
        
        print("─" * 55)
        
        # Total row
        total = total_success + total_failed
        success_rate = (total_success / total * 100) if total > 0 else 0
        print(f"{'Total:':<12} {total_success:>3} ✅  |  {total_failed:>3} ❌  (Success Rate: {success_rate:.1f}%)")
        print(f"Duration:    {duration:.1f} seconds")
        
        print("═" * 55 + "\n")
        
        # Log to logger as well
        logger.info(f"📊 API Summary: {total_success}/{total} successful ({success_rate:.1f}%)")
    
    def reset(self):
        """Reset all tracking data."""
        self.calls.clear()
        self.start_time = datetime.now()


# Singleton instance
api_error_tracker = ApiErrorTracker()


# Export
__all__ = ['api_error_tracker', 'ApiErrorTracker']
