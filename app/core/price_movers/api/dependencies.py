"""
FastAPI Dependencies

Dependency Injection für:
- Exchange Collectors
- Analyzer
- Authentication (future)
- Rate Limiting (future)
"""

import logging
from typing import Dict, Optional
from fastapi import HTTPException, Header, Depends
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.core.price_movers.collectors import ExchangeCollector, ExchangeCollectorFactory
from app.core.price_movers.services import PriceMoverAnalyzer
from app.core.price_movers.utils.constants import SUPPORTED_EXCHANGES
from app.core.price_movers.collectors.unified_collector import UnifiedCollector


logger = logging.getLogger(__name__)


# ==================== GLOBAL INSTANCES ====================

# Cache für Exchange Collectors (wiederverwendbar)
_exchange_collectors: Dict[str, ExchangeCollector] = {}

# Cache für Analyzer (wiederverwendbar)
_analyzer_instance: Optional[PriceMoverAnalyzer] = None


# ==================== COLLECTOR DEPENDENCIES ====================

async def get_exchange_collector(exchange: str) -> ExchangeCollector:
    """
    Dependency für Exchange Collector
    
    Erstellt oder gibt einen gecachten Exchange Collector zurück
    
    Args:
        exchange: Exchange Name (bitget/binance/kraken)
        
    Returns:
        ExchangeCollector Instance
        
    Raises:
        HTTPException: Wenn Exchange nicht unterstützt
    """
    # Validiere Exchange
    if exchange.lower() not in SUPPORTED_EXCHANGES:
        raise HTTPException(
            status_code=400,
            detail=f"Exchange '{exchange}' not supported. "
                   f"Supported: {', '.join(SUPPORTED_EXCHANGES)}"
        )
    
    # Verwende gecachten Collector wenn vorhanden
    exchange_lower = exchange.lower()
    if exchange_lower not in _exchange_collectors:
        try:
            logger.info(f"Creating new ExchangeCollector for {exchange_lower}")
            collector = ExchangeCollector(exchange_name=exchange_lower)
            _exchange_collectors[exchange_lower] = collector
        except Exception as e:
            logger.error(f"Failed to create ExchangeCollector: {e}")
            raise HTTPException(
                status_code=503,
                detail=f"Failed to connect to {exchange}: {str(e)}"
            )
    
    return _exchange_collectors[exchange_lower]


async def get_all_exchange_collectors() -> Dict[str, ExchangeCollector]:
    """
    Dependency für alle Exchange Collectors
    
    Returns:
        Dictionary: exchange_name -> ExchangeCollector
    """
    global _exchange_collectors
    
    # Erstelle alle Collectors wenn noch nicht vorhanden
    if not _exchange_collectors:
        try:
            logger.info("Creating all Exchange Collectors")
            _exchange_collectors = await ExchangeCollectorFactory.create_all()
        except Exception as e:
            logger.error(f"Failed to create Exchange Collectors: {e}")
            raise HTTPException(
                status_code=503,
                detail=f"Failed to initialize exchanges: {str(e)}"
            )
    
    return _exchange_collectors


# ==================== ANALYZER DEPENDENCY ====================

async def get_analyzer(
    exchange: str  # <-- Füge exchange Parameter hinzu
) -> PriceMoverAnalyzer:
    """
    Dependency für PriceMoverAnalyzer
    
    Args:
        exchange: Exchange name (wird automatisch aus Query/Body extrahiert)
    
    Returns:
        PriceMoverAnalyzer Instance
    """
    # Hole den passenden Collector
    collector = await get_exchange_collector(exchange)
    
    global _analyzer_instance
    
    # Erstelle oder verwende gecachte Analyzer-Instance
    if _analyzer_instance is None:
        logger.info("Creating new PriceMoverAnalyzer")
        _analyzer_instance = PriceMoverAnalyzer(
            exchange_collector=collector
        )
    else:
        # Update den Collector für den aktuellen Request
        _analyzer_instance.exchange_collector = collector
    
    return _analyzer_instance


# ==================== AUTHENTICATION (FUTURE) ====================

async def verify_api_key(
    x_api_key: Optional[str] = Header(None)
) -> Optional[str]:
    """
    Dependency für API Key Verification
    
    TODO: Implementiere echte Authentication
    
    Args:
        x_api_key: API Key aus Header
        
    Returns:
        API Key wenn valid, None wenn nicht required
        
    Raises:
        HTTPException: Wenn API Key invalid
    """
    # Placeholder - aktuell keine Authentication
    # In Production sollte hier ein echter Check sein
    
    # if x_api_key != "expected_key":
    #     raise HTTPException(
    #         status_code=401,
    #         detail="Invalid API Key"
    #     )
    
    return x_api_key


# ==================== RATE LIMITING (FUTURE) ====================

class RateLimiter:
    """
    Rate Limiter für API Endpoints
    
    TODO: Implementiere echtes Rate Limiting mit Redis
    """
    
    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.requests = {}
    
    async def check_rate_limit(
        self,
        client_id: str,
        x_forwarded_for: Optional[str] = Header(None)
    ) -> bool:
        """
        Prüft Rate Limit
        
        Args:
            client_id: Client Identifier (IP oder User ID)
            x_forwarded_for: Forwarded IP Header
            
        Returns:
            True wenn unter Limit
            
        Raises:
            HTTPException: Wenn Rate Limit überschritten
        """
        # Placeholder - aktuell kein echtes Rate Limiting
        # In Production mit Redis implementieren
        
        # ip = x_forwarded_for or client_id
        # if self._is_rate_limited(ip):
        #     raise HTTPException(
        #         status_code=429,
        #         detail="Rate limit exceeded. Try again later."
        #     )
        
        return True


# Rate Limiter Instance
rate_limiter = RateLimiter(requests_per_minute=60)


async def check_rate_limit(
    x_forwarded_for: Optional[str] = Header(None)
) -> bool:
    """
    Dependency für Rate Limiting
    
    Args:
        x_forwarded_for: Client IP
        
    Returns:
        True wenn OK
    """
    client_id = x_forwarded_for or "default"
    return await rate_limiter.check_rate_limit(client_id, x_forwarded_for)


# --- NEUER CODE START ---
async def get_unified_collector() -> UnifiedCollector:
    """
    Dependency für UnifiedCollector.
    
    Initialisiert und gibt eine Instanz des UnifiedCollectors zurück.
    Diese Instanz kann dann in Services wie dem HybridPriceMoverAnalyzer verwendet werden.
    """
    # Hier könntest du Konfigurationen oder API-Keys aus Umgebungsvariablen laden
    # Beispiel:
    # import os
    # cex_creds = {
    #     'binance': {'api_key': os.getenv('BINANCE_API_KEY'), 'api_secret': os.getenv('BINANCE_API_SECRET')},
    #     # ... andere CEXs
    # }
    # dex_keys = {
    #     'birdeye': os.getenv('BIRD_EYE_API_KEY'),
    #     # ... andere DEXs
    # }
    # collector = UnifiedCollector(cex_credentials=cex_creds, dex_api_keys=dex_keys)
    # 
    # Für den Moment erstellen wir ihn ohne spezielle Konfiguration,
    # was bedeutet, dass nur die Dinge verfügbar sind, die ohne API-Key funktionieren
    # (oder du setzt die Keys in den Umgebungsvariablen).
    collector = UnifiedCollector()
    return collector
# --- NEUER CODE ENDE ---


# ==================== CLEANUP ====================

async def cleanup_dependencies():
    """
    Cleanup Function für App Shutdown
    
    Schließt alle Exchange Connections
    """
    global _exchange_collectors, _analyzer_instance
    
    logger.info("Cleaning up dependencies...")
    
    # Schließe alle Exchange Collectors
    for exchange_name, collector in _exchange_collectors.items():
        try:
            await collector.close()
            logger.info(f"Closed {exchange_name} collector")
        except Exception as e:
            logger.error(f"Error closing {exchange_name} collector: {e}")
    
    _exchange_collectors.clear()
    _analyzer_instance = None
    
    logger.info("Cleanup complete")


# ==================== REQUEST LOGGING ====================

async def log_request(
    request_id: Optional[str] = Header(None, alias="X-Request-ID")
) -> str:
    """
    Dependency für Request Logging
    
    Args:
        request_id: Request ID aus Header
        
    Returns:
        Request ID (generiert wenn nicht vorhanden)
    """
    import uuid
    
    if not request_id:
        request_id = str(uuid.uuid4())
    
    logger.info(f"Request ID: {request_id}")
    
    return request_id
