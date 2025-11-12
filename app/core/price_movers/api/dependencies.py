"""
FastAPI Dependencies - FIXED VERSION

🔧 FIXES:
- ✅ BIRDEYE_API_KEY wird aus ENV geladen
- ✅ UnifiedCollector erhält DEX API Keys
- ✅ CEX API Keys optional unterstützt

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

# Cache für UnifiedCollector (wiederverwendbar)
_unified_collector_instance: Optional[UnifiedCollector] = None


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


# ==================== UNIFIED COLLECTOR DEPENDENCY (FIXED) ====================

async def get_unified_collector() -> UnifiedCollector:
    """
    🔧 FIXED: Dependency für UnifiedCollector mit API Keys aus ENV
    
    Initialisiert und gibt eine Instanz des UnifiedCollectors zurück mit:
    - CEX Credentials (optional, funktioniert auch ohne)
    - DEX API Keys (BIRDEYE_API_KEY aus ENV) ← FIX!
    
    Returns:
        UnifiedCollector Instance mit konfigurierten API Keys
    """
    global _unified_collector_instance
    
    # Verwende gecachte Instance wenn vorhanden
    if _unified_collector_instance is not None:
        return _unified_collector_instance
    
    logger.info("🔧 Creating UnifiedCollector with API Keys from ENV")
    
    # 🔧 FIX: Lade API Keys aus Environment
    birdeye_key = os.getenv('BIRDEYE_API_KEY')
    
    # CEX Credentials (optional - funktionieren auch ohne)
    cex_creds = {}
    
    # Binance
    binance_key = os.getenv('BINANCE_API_KEY')
    binance_secret = os.getenv('BINANCE_API_SECRET')
    if binance_key and binance_secret:
        cex_creds['binance'] = {
            'api_key': binance_key,
            'api_secret': binance_secret
        }
        logger.info("✅ Binance API Keys geladen")
    
    # Bitget
    bitget_key = os.getenv('BITGET_API_KEY')
    bitget_secret = os.getenv('BITGET_API_SECRET')
    if bitget_key and bitget_secret:
        cex_creds['bitget'] = {
            'api_key': bitget_key,
            'api_secret': bitget_secret
        }
        logger.info("✅ Bitget API Keys geladen")
    
    # Kraken
    kraken_key = os.getenv('KRAKEN_API_KEY')
    kraken_secret = os.getenv('KRAKEN_API_SECRET')
    if kraken_key and kraken_secret:
        cex_creds['kraken'] = {
            'api_key': kraken_key,
            'api_secret': kraken_secret
        }
        logger.info("✅ Kraken API Keys geladen")
    
    # DEX API Keys
    dex_keys = {}
    if birdeye_key:
        dex_keys['birdeye'] = birdeye_key
        logger.info(f"✅ Birdeye API Key geladen: {birdeye_key[:8]}...")
    else:
        logger.warning("⚠️ BIRDEYE_API_KEY nicht in ENV gefunden! DEX wird nicht verfügbar sein.")
    
    # Helius (optional, für alternative Solana DEX Daten)
    helius_key = os.getenv('HELIUS_API_KEY')
    if helius_key:
        dex_keys['helius'] = helius_key
        logger.info(f"✅ Helius API Key geladen: {helius_key[:8]}...")
    
    # Erstelle UnifiedCollector mit API Keys
    try:
        collector = UnifiedCollector(
            cex_credentials=cex_creds if cex_creds else None,
            dex_api_keys=dex_keys if dex_keys else None
        )
        
        # Cache die Instance
        _unified_collector_instance = collector
        
        # Log verfügbare Exchanges
        available = collector.list_available_exchanges()
        logger.info(
            f"✅ UnifiedCollector initialisiert: "
            f"CEX={available['cex']}, DEX={available['dex']}"
        )
        
        return collector
        
    except Exception as e:
        logger.error(f"❌ Failed to create UnifiedCollector: {e}", exc_info=True)
        # Erstelle fallback ohne API Keys
        collector = UnifiedCollector()
        _unified_collector_instance = collector
        return collector


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


# ==================== CLEANUP ====================

async def cleanup_dependencies():
    """
    Cleanup Function für App Shutdown
    
    Schließt alle Exchange Connections
    """
    global _exchange_collectors, _analyzer_instance, _unified_collector_instance
    
    logger.info("Cleaning up dependencies...")
    
    # Schließe alle Exchange Collectors
    for exchange_name, collector in _exchange_collectors.items():
        try:
            await collector.close()
            logger.info(f"Closed {exchange_name} collector")
        except Exception as e:
            logger.error(f"Error closing {exchange_name} collector: {e}")
    
    # Schließe UnifiedCollector
    if _unified_collector_instance:
        try:
            await _unified_collector_instance.close()
            logger.info("Closed UnifiedCollector")
        except Exception as e:
            logger.error(f"Error closing UnifiedCollector: {e}")
    
    _exchange_collectors.clear()
    _analyzer_instance = None
    _unified_collector_instance = None
    
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
