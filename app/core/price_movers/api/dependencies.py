"""
FastAPI Dependencies - FIXED CEX INITIALIZATION

✅ Properly passes CEX credentials to UnifiedCollector
✅ Uses Binance public API as fallback
✅ Loads all API Keys from ENV
"""

import logging
import os
from typing import Dict, Optional
from fastapi import HTTPException, Header
import uuid

from app.core.price_movers.collectors.unified_collector import UnifiedCollector

logger = logging.getLogger(__name__)

# Global cache
_unified_collector_instance: Optional[UnifiedCollector] = None


async def get_unified_collector() -> UnifiedCollector:
    """
    Dependency for UnifiedCollector with proper CEX initialization
    
    Returns:
        UnifiedCollector with CEX + DEX support
    """
    global _unified_collector_instance
    
    if _unified_collector_instance is not None:
        return _unified_collector_instance
    
    logger.info("🔧 Creating UnifiedCollector with CEX + DEX Support")
    
    # ==================== Load API Keys ====================
    
    # Moralis (DEX Multi-Chain)
    moralis_key = os.getenv('MORALIS_API_KEY')
    moralis_fallback = os.getenv('MORALIS_API_KEY_FALLBACK')
    moralis_fallback2 = os.getenv('MORALIS_API_KEY_FALLBACK2')
    
    # DEX APIs
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
    
    # ==================== Build CEX Credentials ====================
    
    cex_creds = {}
    
    # Binance: Always add (public API works without keys)
    cex_creds['binance'] = {
        'api_key': binance_key,
        'api_secret': binance_secret
    }
    logger.info("✅ Binance added (public API)")
    
    # Bitget: Only if credentials exist
    if bitget_key and bitget_secret:
        cex_creds['bitget'] = {
            'api_key': bitget_key,
            'api_secret': bitget_secret,
            'passphrase': bitget_passphrase or ''
        }
        logger.info("✅ Bitget API Keys loaded")
    
    # Kraken: Only if credentials exist
    if kraken_key and kraken_secret:
        cex_creds['kraken'] = {
            'api_key': kraken_key,
            'api_secret': kraken_secret
        }
        logger.info("✅ Kraken API Keys loaded")
    
    # ==================== Build DEX API Keys ====================
    
    dex_keys = {}
    
    if moralis_key:
        dex_keys['moralis'] = moralis_key
        dex_keys['moralis_fallback'] = moralis_fallback
        dex_keys['moralis_fallback2'] = moralis_fallback2
        logger.info(f"✅ Moralis: {sum([bool(k) for k in [moralis_key, moralis_fallback, moralis_fallback2]])} keys")
    
    if birdeye_key:
        dex_keys['birdeye'] = birdeye_key
        logger.info("✅ Birdeye API Key loaded")
    
    if helius_key:
        dex_keys['helius'] = helius_key
        logger.info("✅ Helius API Key loaded")
    
    if bitquery_key:
        dex_keys['bitquery'] = bitquery_key
        logger.info("✅ Bitquery API Key loaded")
    
    # ==================== Create UnifiedCollector ====================
    
    try:
        # CRITICAL FIX: Pass cex_credentials properly
        collector = UnifiedCollector(
            cex_credentials=cex_creds,  # ✅ Always pass, even if empty
            dex_api_keys=dex_keys if dex_keys else None
        )
        
        _unified_collector_instance = collector
        
        available = collector.list_available_exchanges()
        logger.info(
            f"✅ UnifiedCollector: CEX={available['cex']} ({len(available['cex'])} exchanges), "
            f"DEX={available['dex']} ({len(available['dex'])} exchanges)"
        )
        
        return collector
        
    except Exception as e:
        logger.error(f"❌ Failed to create UnifiedCollector: {e}", exc_info=True)
        # Fallback: Create with empty config
        collector = UnifiedCollector()
        _unified_collector_instance = collector
        return collector


async def log_request(
    request_id: Optional[str] = Header(None, alias="X-Request-ID")
) -> str:
    """Request logging dependency"""
    if not request_id:
        request_id = str(uuid.uuid4())
    logger.info(f"Request ID: {request_id}")
    return request_id


async def cleanup_dependencies():
    """Cleanup on shutdown"""
    global _unified_collector_instance
    
    logger.info("Cleaning up dependencies...")
    
    if _unified_collector_instance:
        try:
            await _unified_collector_instance.close()
            logger.info("Closed UnifiedCollector")
        except Exception as e:
            logger.error(f"Error closing UnifiedCollector: {e}")
    
    _unified_collector_instance = None
    logger.info("Cleanup complete")


async def startup_event():
    """Application startup"""
    logger.info("🚀 Starting application...")
    
    try:
        collector = await get_unified_collector()
        health = await collector.health_check()
        logger.info(f"📊 Health: {health}")
        
        if not any(h for h in health.values()):
            logger.warning("⚠️ All collectors unhealthy!")
        
        logger.info("✅ Startup complete")
        
    except Exception as e:
        logger.error(f"❌ Startup error: {e}", exc_info=True)
        raise


async def shutdown_event():
    """Application shutdown"""
    logger.info("🛑 Shutting down...")
    try:
        await cleanup_dependencies()
        logger.info("✅ Shutdown complete")
    except Exception as e:
        logger.error(f"❌ Shutdown error: {e}", exc_info=True)
