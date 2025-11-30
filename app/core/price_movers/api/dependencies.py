"""
FastAPI Dependencies - COMPLETE WITH DEX SUPPORT - FIXED HELIUS INIT

✅ CEX: Binance US (no geo-restrictions)
✅ DEX: Dexscreener (free) + Helius (Solana) + Moralis (multi-chain)
✅ Proper initialization with error handling
🔧 FIXED: Helius Collector initialization
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

_exchange_collectors: Dict[str, ExchangeCollector] = {}
_analyzer_instance: Optional[PriceMoverAnalyzer] = None
_unified_collector_instance: Optional[UnifiedCollector] = None


# ==================== UNIFIED COLLECTOR DEPENDENCY ====================

async def get_unified_collector() -> UnifiedCollector:
    """Dependency for UnifiedCollector with CEX + DEX support"""
    global _unified_collector_instance
    
    if _unified_collector_instance is not None:
        return _unified_collector_instance
    
    logger.info("🔧 Creating UnifiedCollector with Multi-Chain Support")
    
    # Load API Keys
    moralis_key = os.getenv('MORALIS_API_KEY')
    moralis_fallback = os.getenv('MORALIS_API_KEY_FALLBACK')
    moralis_fallback2 = os.getenv('MORALIS_API_KEY_FALLBACK2')
    birdeye_key = os.getenv('BIRDEYE_API_KEY')
    helius_key = os.getenv('HELIUS_API_KEY')
    bitquery_key = os.getenv('BITQUERY_API_KEY')
    binance_key = os.getenv('BINANCE_API_KEY', '')
    binance_secret = os.getenv('BINANCE_API_SECRET', '')
    bitget_key = os.getenv('BITGET_API_KEY')
    bitget_secret = os.getenv('BITGET_API_SECRET')
    bitget_passphrase = os.getenv('BITGET_PASSPHRASE')
    kraken_key = os.getenv('KRAKEN_API_KEY')
    kraken_secret = os.getenv('KRAKEN_API_SECRET')
    
    # Log keys
    moralis_count = sum([bool(k) for k in [moralis_key, moralis_fallback, moralis_fallback2]])
    if moralis_count > 0:
        logger.info(f"✅ Moralis API Keys: {moralis_count} keys (Solana + Ethereum)")
    if birdeye_key:
        logger.info("✅ Birdeye API Key loaded")
    if helius_key:
        logger.info(f"✅ Helius API Key: {helius_key[:8]}... (length: {len(helius_key)})")
    else:
        logger.error("❌ HELIUS_API_KEY NOT FOUND!")
    if bitquery_key:
        logger.info("✅ Bitquery API Key loaded")
    logger.info("✅ Binance US: Using public API (no geo-restrictions)")
    
    # Build CEX credentials
    cex_creds = {
        'binance': {'api_key': binance_key, 'api_secret': binance_secret}
    }
    if bitget_key and bitget_secret:
        cex_creds['bitget'] = {
            'api_key': bitget_key,
            'api_secret': bitget_secret,
            'passphrase': bitget_passphrase or ''
        }
    if kraken_key and kraken_secret:
        cex_creds['kraken'] = {'api_key': kraken_key, 'api_secret': kraken_secret}
    
    # Initialize DEX Collectors
    try:
        dex_collectors_dict = {}
        
        # Dexscreener
        try:
            from app.core.price_movers.collectors.dexscreener_collector import DexscreenerCollector
            dex_collectors_dict['dexscreener'] = DexscreenerCollector()
            logger.info("✅ Dexscreener initialized (free, no API key)")
        except Exception as e:
            logger.warning(f"⚠️ Dexscreener init failed: {e}")
        
        # 🔧 HELIUS - CRITICAL FIX!
        if helius_key:
            try:
                logger.info(f"🔧 Initializing Helius with API key: {helius_key[:8]}...")
                
                # Import MUST work
                from app.core.price_movers.collectors.helius_collector import HeliusCollector
                logger.info("✅ HeliusCollector class imported successfully")
                
                # Create instance
                helius_instance = HeliusCollector(api_key=helius_key)
                logger.info("✅ HeliusCollector instance created")
                
                # Test health check
                is_healthy = await helius_instance.health_check()
                logger.info(f"✅ Helius health check: {is_healthy}")
                
                # Add to dict
                dex_collectors_dict['helius'] = helius_instance
                logger.info("✅ Helius added to collectors dict")
                
                # 🔍 VERIFY IT'S THERE
                logger.info(f"🔍 Helius in dict? {'helius' in dex_collectors_dict}")
                logger.info(f"🔍 Helius instance type: {type(dex_collectors_dict.get('helius'))}")
                
            except ImportError as e:
                logger.error(f"❌ CRITICAL: Cannot import HeliusCollector: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"❌ CRITICAL: Helius init failed: {e}", exc_info=True)
        else:
            logger.error("❌ HELIUS_API_KEY is None or empty!")
        
        # Moralis
        if moralis_key:
            try:
                from app.core.price_movers.collectors.moralis_collector import MoralisCollector
                dex_collectors_dict['moralis'] = MoralisCollector(
                    api_key=moralis_key,
                    fallback_keys=[moralis_fallback, moralis_fallback2]
                )
                logger.info("✅ Moralis initialized (multi-chain)")
            except Exception as e:
                logger.warning(f"⚠️ Moralis init failed: {e}")
        
        # Birdeye
        if birdeye_key:
            try:
                from app.core.price_movers.collectors.birdeye_collector import BirdeyeCollector
                dex_collectors_dict['birdeye'] = BirdeyeCollector(api_key=birdeye_key)
                logger.info("✅ Birdeye initialized (Solana)")
            except Exception as e:
                logger.warning(f"⚠️ Birdeye init failed: {e}")
        
        logger.info(f"📊 DEX Collectors initialized: {list(dex_collectors_dict.keys())}")
        
        # 🔍 DEBUG: Verify all collectors
        for name, collector in dex_collectors_dict.items():
            logger.info(f"   - {name}: {type(collector).__name__}")
        
        # Create UnifiedCollector
        logger.info("🔧 Creating UnifiedCollector with collectors...")
        collector = UnifiedCollector(
            helius_collector=dex_collectors_dict.get('helius'),
            dexscreener_collector=dex_collectors_dict.get('dexscreener'),
            moralis_collector=dex_collectors_dict.get('moralis'),
            birdeye_collector=dex_collectors_dict.get('birdeye'),
            cex_credentials=cex_creds
        )
        
        _unified_collector_instance = collector
        
        available = collector.list_available_exchanges()
        logger.info(
            f"✅ UnifiedCollector initialized: "
            f"CEX={available['cex']} ({len(available['cex'])} exchanges), "
            f"DEX={available['dex']} ({len(available['dex'])} exchanges)"
        )
        
        # 🔍 FINAL VERIFICATION
        logger.info(f"🔍 FINAL CHECK: helius_collector in UnifiedCollector? {collector.helius_collector is not None}")
        logger.info(f"🔍 FINAL CHECK: helius_collector type: {type(collector.helius_collector)}")
        
        return collector
        
    except Exception as e:
        logger.error(f"❌ Failed to create UnifiedCollector: {e}", exc_info=True)
        
        # Fallback: Create minimal UnifiedCollector
        logger.warning("⚠️ Creating FALLBACK UnifiedCollector (CEX only)")
        collector = UnifiedCollector(cex_credentials={'binance': {'api_key': '', 'api_secret': ''}})
        _unified_collector_instance = collector
        return collector


# ==================== OTHER DEPENDENCIES ====================

async def get_exchange_collector(exchange: str) -> ExchangeCollector:
    """Dependency for single Exchange Collector"""
    if exchange.lower() not in SUPPORTED_EXCHANGES:
        raise HTTPException(
            status_code=400,
            detail=f"Exchange '{exchange}' not supported. "
                   f"Supported: {', '.join(SUPPORTED_EXCHANGES)}"
        )
    
    exchange_lower = exchange.lower()
    if exchange_lower not in _exchange_collectors:
        try:
            logger.info(f"Creating ExchangeCollector for {exchange_lower}")
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
    """Dependency for all Exchange Collectors"""
    global _exchange_collectors
    
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


async def get_analyzer(exchange: str) -> PriceMoverAnalyzer:
    """Dependency for PriceMoverAnalyzer"""
    collector = await get_exchange_collector(exchange)
    
    global _analyzer_instance
    
    if _analyzer_instance is None:
        logger.info("Creating new PriceMoverAnalyzer")
        _analyzer_instance = PriceMoverAnalyzer(exchange_collector=collector)
    else:
        _analyzer_instance.exchange_collector = collector
    
    return _analyzer_instance


async def log_request(
    request_id: Optional[str] = Header(None, alias="X-Request-ID")
) -> str:
    """Dependency for Request Logging"""
    if not request_id:
        request_id = str(uuid.uuid4())
    logger.info(f"Request ID: {request_id}")
    return request_id


async def verify_api_key(
    x_api_key: Optional[str] = Header(None)
) -> Optional[str]:
    """Dependency for API Key Verification"""
    return x_api_key


# ==================== RATE LIMITING ====================

class RateLimiter:
    """Rate Limiter for API Endpoints"""
    def __init__(self, requests_per_minute: int = 60):
        self.requests_per_minute = requests_per_minute
        self.requests = {}
    
    async def check_rate_limit(
        self, client_id: str, x_forwarded_for: Optional[str] = Header(None)
    ) -> bool:
        return True


rate_limiter = RateLimiter(requests_per_minute=60)


async def check_rate_limit(
    x_forwarded_for: Optional[str] = Header(None)
) -> bool:
    """Dependency for Rate Limiting"""
    client_id = x_forwarded_for or "default"
    return await rate_limiter.check_rate_limit(client_id, x_forwarded_for)


# ==================== CLEANUP ====================

async def cleanup_dependencies():
    """Cleanup on app shutdown"""
    global _exchange_collectors, _analyzer_instance, _unified_collector_instance
    
    logger.info("Cleaning up dependencies...")
    
    for exchange_name, collector in _exchange_collectors.items():
        try:
            await collector.close()
            logger.info(f"Closed {exchange_name} collector")
        except Exception as e:
            logger.error(f"Error closing {exchange_name}: {e}")
    
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


# ==================== STARTUP/SHUTDOWN ====================

async def startup_event():
    """Application startup handler"""
    logger.info("🚀 Starting up application...")
    
    try:
        collector = await get_unified_collector()
        
        # 🔍 VERIFY HELIUS
        logger.info(f"🔍 STARTUP CHECK: Helius available? {collector.helius_collector is not None}")
        
        health = await collector.health_check()
        logger.info(f"📊 Health Check: {health}")
        
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
