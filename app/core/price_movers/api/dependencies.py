"""
FastAPI Dependencies - COMPLETE FIXED VERSION

✅ Properly passes CEX credentials to UnifiedCollector
✅ Uses Binance public API as fallback
✅ Loads all API Keys from ENV
✅ All original dependencies included
"""

import logging
from typing import Dict, Optional
from fastapi import HTTPException, Header, Depends
from datetime import datetime
import uuid
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.core.price_movers.collectors import ExchangeCollector, ExchangeCollectorFactory
from app.core.price_movers.services import PriceMoverAnalyzer
from app.core.price_movers.utils.constants import SUPPORTED_EXCHANGES
from app.core.price_movers.collectors.unified_collector import UnifiedCollector

logger = logging.getLogger(__name__)


# ==================== GLOBAL INSTANCES ====================

# Cache für Exchange Collectors
_exchange_collectors: Dict[str, ExchangeCollector] = {}

# Cache für Analyzer
_analyzer_instance: Optional[PriceMoverAnalyzer] = None

# Cache für UnifiedCollector
_unified_collector_instance: Optional[UnifiedCollector] = None


# ==================== UNIFIED COLLECTOR DEPENDENCY (FIXED) ====================

async def get_unified_collector() -> UnifiedCollector:
    """
    🔧 FIXED: Dependency für UnifiedCollector with proper CEX initialization
    
    PRIORITY für DEX OHLCV:
    1. DEXSCREENER (current price, free)
    2. MORALIS (Solana + Ethereum historical, limited free tier)
    3. BIRDEYE (Solana only, if not suspended)
    4. HELIUS (Solana fallback, free tier)
    
    CEX Priority:
    1. BINANCE (public API, no keys needed)
    2. BITGET (if credentials available)
    3. KRAKEN (if credentials available)
    
    Returns:
        UnifiedCollector Instance mit konfigurierten API Keys
    """
    global _unified_collector_instance
    
    # Verwende gecachte Instance wenn vorhanden
    if _unified_collector_instance is not None:
        return _unified_collector_instance
    
    logger.info("🔧 Creating UnifiedCollector with Multi-Chain Support")
    
    # ==================== Load API Keys ====================
    
    # Moralis API Keys (Primary + 2 Fallbacks)
    moralis_key = os.getenv('MORALIS_API_KEY')
    moralis_fallback = os.getenv('MORALIS_API_KEY_FALLBACK')
    moralis_fallback2 = os.getenv('MORALIS_API_KEY_FALLBACK2')
    
    # DEX API Keys
    birdeye_key = os.getenv('BIRDEYE_API_KEY')
    helius_key = os.getenv('HELIUS_API_KEY')
    bitquery_key = os.getenv('BITQUERY_API_KEY')
    
    # CEX Credentials
    binance_key = os.getenv('BINANCE_API_KEY', '')  # Empty string for public API
    binance_secret = os.getenv('BINANCE_API_SECRET', '')
    bitget_key = os.getenv('BITGET_API_KEY')
    bitget_secret = os.getenv('BITGET_API_SECRET')
    bitget_passphrase = os.getenv('BITGET_PASSPHRASE')
    kraken_key = os.getenv('KRAKEN_API_KEY')
    kraken_secret = os.getenv('KRAKEN_API_SECRET')
    
    # ==================== Log Loaded Keys (Masked) ====================
    
    # Moralis Keys (most important!)
    moralis_count = sum([bool(k) for k in [moralis_key, moralis_fallback, moralis_fallback2]])
    if moralis_count > 0:
        logger.info(f"✅ Moralis API Keys loaded: {moralis_count} keys (Solana + Ethereum)")
        if moralis_key:
            logger.info(f"   Primary: {moralis_key[:20]}...")
    else:
        logger.warning("⚠️ MORALIS_API_KEY not found - Limited historical data!")
    
    # Other DEX Keys
    if birdeye_key:
        logger.info(f"✅ Birdeye API Key: {birdeye_key[:8]}... (Solana OHLCV)")
    else:
        logger.info("ℹ️ BIRDEYE_API_KEY not found")
    
    if helius_key:
        logger.info(f"✅ Helius API Key: {helius_key[:8]}... (Solana Trades)")
    else:
        logger.info("ℹ️ HELIUS_API_KEY not found")
    
    if bitquery_key:
        logger.info(f"✅ Bitquery API Key: {bitquery_key[:8]}...")
    
    # CEX Keys
    logger.info("✅ Binance: Using public API (no credentials required)")
    
    if bitget_key and bitget_secret:
        logger.info("✅ Bitget API Keys loaded")
    else:
        logger.info("ℹ️ Bitget credentials not found")
    
    if kraken_key and kraken_secret:
        logger.info("✅ Kraken API Keys loaded")
    else:
        logger.info("ℹ️ Kraken credentials not found")
    
    # ==================== Build Credentials ====================
    
    # CEX Credentials - CRITICAL FIX: Always initialize dict
    cex_creds = {}
    
    # Binance: ALWAYS ADD (public API works without keys)
    cex_creds['binance'] = {
        'api_key': binance_key,      # Can be empty
        'api_secret': binance_secret  # Can be empty
    }
    
    # Bitget: Only if credentials exist
    if bitget_key and bitget_secret:
        cex_creds['bitget'] = {
            'api_key': bitget_key,
            'api_secret': bitget_secret
        }
        if bitget_passphrase:
            cex_creds['bitget']['passphrase'] = bitget_passphrase
    
    # Kraken: Only if credentials exist
    if kraken_key and kraken_secret:
        cex_creds['kraken'] = {
            'api_key': kraken_key,
            'api_secret': kraken_secret
        }
    
    # DEX API Keys (including Moralis!)
    dex_keys = {}
    
    if moralis_key:
        dex_keys['moralis'] = moralis_key
    if moralis_fallback:
        dex_keys['moralis_fallback'] = moralis_fallback
    if moralis_fallback2:
        dex_keys['moralis_fallback2'] = moralis_fallback2
    
    if birdeye_key:
        dex_keys['birdeye'] = birdeye_key
    
    if helius_key:
        dex_keys['helius'] = helius_key
    
    if bitquery_key:
        dex_keys['bitquery'] = bitquery_key
    
    # Warnung wenn KEINE DEX Keys
    if not dex_keys:
        logger.warning("⚠️ KEINE DEX API Keys gefunden! DEX wird nicht verfügbar sein.")
        logger.info("💡 Tipp: Setze MORALIS_API_KEY, BIRDEYE_API_KEY oder HELIUS_API_KEY")
    
    # ==================== Create UnifiedCollector ====================
    
    try:
        # CRITICAL FIX: Always pass cex_credentials, even if minimal
        collector = UnifiedCollector(
            cex_credentials=cex_creds,  # ✅ Always pass, never None
            dex_api_keys=dex_keys if dex_keys else None
        )
        
        # Cache die Instance
        _unified_collector_instance = collector
        
        # Log verfügbare Exchanges
        available = collector.list_available_exchanges()
        logger.info(
            f"✅ UnifiedCollector initialized: "
            f"CEX={available['cex']} ({len(available['cex'])} exchanges), "
            f"DEX={available['dex']} ({len(available['dex'])} exchanges)"
        )
        
        # Log supported chains
        if hasattr(collector, 'moralis_collector') and collector.moralis_collector:
            chains = collector.moralis_collector.get_supported_chains()
            logger.info(f"🌐 Supported Blockchains: {', '.join(chains)}")
        
        return collector
        
    except Exception as e:
        logger.error(f"❌ Failed to create UnifiedCollector: {e}", exc_info=True)
        # Erstelle fallback mit minimal config
        collector = UnifiedCollector(
            cex_credentials={'binance': {'api_key': '', 'api_secret': ''}}
        )
        _unified_collector_instance = collector
        return collector


# ==================== COLLECTOR DEPENDENCIES ====================

async def get_exchange_collector(exchange: str) -> ExchangeCollector:
    """
    Dependency für Exchange Collector
    
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
    exchange: str
) -> PriceMoverAnalyzer:
    """
    Dependency für PriceMoverAnalyzer
    
    Args:
        exchange: Exchange name
    
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
    if not request_id:
        request_id = str(uuid.uuid4())
    
    logger.info(f"Request ID: {request_id}")
    
    return request_id


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
    """
    return x_api_key


# ==================== RATE LIMITING (FUTURE) ====================

class RateLimiter:
    """Rate Limiter für API Endpoints"""
    
    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.requests = {}
    
    async def check_rate_limit(
        self,
        client_id: str,
        x_forwarded_for: Optional[str] = Header(None)
    ) -> bool:
        """Prüft Rate Limit"""
        return True


rate_limiter = RateLimiter(requests_per_minute=60)


async def check_rate_limit(
    x_forwarded_for: Optional[str] = Header(None)
) -> bool:
    """Dependency für Rate Limiting"""
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


# ==================== STARTUP/SHUTDOWN EVENTS ====================

async def startup_event():
    """Application startup handler"""
    logger.info("🚀 Starting up application...")
    
    try:
        # Initialize UnifiedCollector
        collector = await get_unified_collector()
        
        # Run health check
        health = await collector.health_check()
        logger.info(f"📊 Health Check Results: {health}")
        
        if not any(h for h in health.values()):
            logger.warning("⚠️ All collectors unhealthy!")
        else:
            healthy_count = sum(1 for h in health.values() if h)
            logger.info(f"✅ {healthy_count}/{len(health)} collectors healthy")
        
        logger.info("✅ Application startup complete")
        
    except Exception as e:
        logger.error(f"❌ Startup error: {e}", exc_info=True)
        raise


async def shutdown_event():
    """Application shutdown handler"""
    logger.info("🛑 Shutting down application...")
    
    try:
        await cleanup_dependencies()
        logger.info("✅ Application shutdown complete")
    except Exception as e:
        logger.error(f"❌ Shutdown error: {e}", exc_info=True)
