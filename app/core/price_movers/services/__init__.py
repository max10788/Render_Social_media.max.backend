
"""
Services Package - Price Movers

Exports:
- PriceMoverAnalyzer (Legacy Single-Exchange)
- HybridPriceMoverAnalyzer (New CEX+DEX Combined)
- ImpactCalculator
- LightweightEntityIdentifier
- EntityClassifier
"""

import logging

logger = logging.getLogger(__name__)

# ==================== CORE SERVICES ====================

# Impact Calculator (always available)
try:
    from .impact_calculator import ImpactCalculator
    logger.debug("✓ ImpactCalculator imported")
except ImportError as e:
    logger.error(f"❌ Failed to import ImpactCalculator: {e}")
    ImpactCalculator = None

# Lightweight Entity Identifier (always available)
try:
    from .lightweight_entity_identifier import (
        LightweightEntityIdentifier,
        TradingEntity,
        EnrichedTrade
    )
    logger.debug("✓ LightweightEntityIdentifier imported")
except ImportError as e:
    logger.error(f"❌ Failed to import LightweightEntityIdentifier: {e}")
    LightweightEntityIdentifier = None
    TradingEntity = None
    EnrichedTrade = None

# Entity Classifier (always available)
try:
    from .entity_classifier import EntityClassifier
    logger.debug("✓ EntityClassifier imported")
except ImportError as e:
    logger.error(f"❌ Failed to import EntityClassifier: {e}")
    EntityClassifier = None


# ==================== ANALYZERS ====================

# Legacy Analyzer (PriceMoverAnalyzer)
PriceMoverAnalyzer = None

try:
    from .analyzer import PriceMoverAnalyzer
    logger.info("✓ PriceMoverAnalyzer imported (from analyzer.py)")
except ImportError as e:
    logger.warning(f"⚠️ Failed to import PriceMoverAnalyzer from analyzer.py: {e}")
    
    # Fallback: Try to create a minimal PriceMoverAnalyzer
    try:
        logger.info("Creating fallback PriceMoverAnalyzer...")
        
        class PriceMoverAnalyzer:
            """Fallback PriceMoverAnalyzer for backward compatibility"""
            def __init__(self, exchange_collector=None, **kwargs):
                self.exchange_collector = exchange_collector
                logger.warning("Using fallback PriceMoverAnalyzer (limited functionality)")
            
            async def analyze_candle(self, *args, **kwargs):
                raise NotImplementedError(
                    "PriceMoverAnalyzer not properly installed. "
                    "Please check analyzer.py"
                )
        
        logger.info("✓ Fallback PriceMoverAnalyzer created")
    except Exception as fallback_error:
        logger.error(f"❌ Failed to create fallback: {fallback_error}")
        PriceMoverAnalyzer = None


# Hybrid Analyzer (HybridPriceMoverAnalyzer)
HybridPriceMoverAnalyzer = None

try:
    from .analyzer_hybrid import HybridPriceMoverAnalyzer
    logger.info("✓ HybridPriceMoverAnalyzer imported (from analyzer_hybrid.py)")
except ImportError as e:
    logger.warning(f"⚠️ HybridPriceMoverAnalyzer not available: {e}")
    
    # Fallback: Use PriceMoverAnalyzer if available
    if PriceMoverAnalyzer:
        logger.info("Using PriceMoverAnalyzer as fallback for HybridPriceMoverAnalyzer")
        HybridPriceMoverAnalyzer = PriceMoverAnalyzer
    else:
        logger.error("❌ No analyzer available!")
        HybridPriceMoverAnalyzer = None


# ==================== EXPORTS ====================

__all__ = [
    # Analyzers
    'PriceMoverAnalyzer',
    'HybridPriceMoverAnalyzer',
    
    # Core Services
    'ImpactCalculator',
    'LightweightEntityIdentifier',
    'EntityClassifier',
    
    # Data Classes
    'TradingEntity',
    'EnrichedTrade',
]


# ==================== VALIDATION ====================

def validate_imports():
    """Validates that critical imports are available"""
    missing = []
    
    if PriceMoverAnalyzer is None:
        missing.append('PriceMoverAnalyzer')
    
    if ImpactCalculator is None:
        missing.append('ImpactCalculator')
    
    if LightweightEntityIdentifier is None:
        missing.append('LightweightEntityIdentifier')
    
    if EntityClassifier is None:
        missing.append('EntityClassifier')
    
    if missing:
        logger.warning(f"⚠️ Missing imports: {', '.join(missing)}")
        return False
    
    logger.info("✅ All critical imports available")
    return True


# Run validation on import
try:
    validate_imports()
except Exception as e:
    logger.error(f"Import validation error: {e}")


# ==================== VERSION INFO ====================

__version__ = "2.0.0"
__author__ = "Price Movers Team"
__description__ = "Hybrid CEX/DEX Price Mover Analysis"

logger.info(f"Price Movers Services v{__version__} initialized")
