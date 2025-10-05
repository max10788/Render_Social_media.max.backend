import sys
import os

# Füge Projektverzeichnis zum Python-Pfad hinzu
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import requests
from app.core.backend_crypto_tracker.blockchain.rate_limiters.rate_limiter import RateLimiter
from ..utils.error_handling import retry_on_failure

@dataclass
class ExchangeConfig:
    """Exchange configuration"""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    base_url: str = ""
    rate_limit: float = 10.0  # calls per second
    timeout: int = 30

class BaseProvider(ABC):
    """Base class for exchange data providers"""
    
    def __init__(self, config: ExchangeConfig):
        self.config = config
        self.rate_limiter = RateLimiter(calls_per_second=config.rate_limit)
        self.session = requests.Session()
        if config.api_key:
            self.session.headers['X-API-KEY'] = config.api_key
    
    @retry_on_failure(max_retries=3)
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make API request with rate limiting"""
        with self.rate_limiter:
            url = f"{self.config.base_url}{endpoint}"
            response = self.session.get(url, params=params, timeout=self.config.timeout)
            response.raise_for_status()
            return response.json()
    
    @abstractmethod
    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get ticker data for symbol"""
        pass
    
    @abstractmethod
    def get_orderbook(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """Get order book data"""
        pass
