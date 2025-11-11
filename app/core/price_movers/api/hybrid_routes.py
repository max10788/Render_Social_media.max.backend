"""
Hybrid Analysis Routes - CEX + DEX Combined Analysis

Neue Endpoints:
- POST /api/v1/hybrid/analyze - Parallel CEX + DEX Analyse
- POST /api/v1/hybrid/track-wallet - Tracke CEX Pattern auf DEX
- GET /api/v1/hybrid/correlation - Correlation History
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, status, Body
from pydantic import BaseModel, Field

from app.core.price_movers.api.test_schemas import (
    ExchangeEnum,
    TimeframeEnum,
    CandleData,
    WalletMover,
    ErrorResponse,
)
from app.core.price_movers.api.dependencies import (
    get_analyzer,
    log_request,
)
from app.core.price_movers.collectors.unified_collector import UnifiedCollector


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/hybrid",
    tags=["hybrid-analysis"],
    responses={
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


# ==================== SCHEMAS ====================

class HybridAnalysisRequest(BaseModel):
    """Request für Hybrid CEX/DEX Analyse"""
    cex_exchange: ExchangeEnum = Field(..., description="CEX Exchange (bitget/binance/kraken)")
    dex_exchange: str = Field(..., description="DEX Exchange (jupiter/raydium/orca)")
    symbol: str = Field(..., description="Trading pair (e.g., SOL/USDT)")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    start_time: datetime = Field(..., description="Start time")
    end_time: datetime = Field(..., description="End time")
    min_impact_threshold: float = Field(default=0.05, ge=0.0, le=1.0)
    top_n_wallets: int = Field(default=10, ge=1, le=100)
    
    class Config:
        json_schema_extra = {
            "example": {
                "cex_exchange": "bitget",
                "dex_exchange": "jupiter",
                "symbol": "SOL/USDT",
                "timeframe": "5m",
                "start_time": "2025-11-11T10:00:00Z",
                "end_time": "2025-11-11T10:05:00Z",
                "min_impact_threshold": 0.05,
                "top_n_wallets": 10
            }
        }


class CEXAnalysis(BaseModel):
    """CEX Analysis Result"""
    exchange: str
    top_movers: List[WalletMover]
    has_wallet_ids: bool = False
    data_source: str = "pattern_based"
    trade_count: int


class DEXAnalysis(BaseModel):
    """DEX Analysis Result"""
    exchange: str
    top_movers: List[Dict]  # With wallet_address field
    has_wallet_ids: bool = True
    data_source: str = "on_chain"
    trade_count: int


class PatternMatch(BaseModel):
    """Matched pattern between CEX and DEX"""
    cex_entity: str = Field(..., description="CEX Entity ID")
    dex_wallet: str = Field(..., description="DEX Wallet Address")
    type: str = Field(..., description="Entity type (whale/bot/market_maker)")
    volume_diff_pct: float = Field(..., description="Volume difference %")
    confidence: float = Field(..., description="Match confidence (0-1)")


class CorrelationResult(BaseModel):
    """Cross-Exchange Correlation"""
    score: float = Field(..., description="Overall correlation score (0-1)")
    cex_led_by_seconds: int = Field(..., description="Time difference (positive = CEX first)")
    volume_correlation: float = Field(..., description="Volume similarity (0-1)")
    timing_score: float = Field(..., description="Timing alignment (0-1)")
    pattern_matches: List[PatternMatch] = Field(..., description="Matched patterns")
    conclusion: str = Field(..., description="Human-readable conclusion")


class HybridAnalysisMetadata(BaseModel):
    """Metadata for hybrid analysis"""
    analysis_timestamp: datetime
    processing_duration_ms: int
    total_trades_analyzed: int
    cex_entities_found: int
    dex_wallets_found: int
    exchanges: str
    symbol: str
    timeframe: str


class HybridAnalysisResponse(BaseModel):
    """Response für Hybrid Analyse"""
    success: bool = True
    candle: CandleData
    cex_analysis: CEXAnalysis
    dex_analysis: DEXAnalysis
    correlation: CorrelationResult
    analysis_metadata: HybridAnalysisMetadata
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "candle": {
                    "timestamp": "2025-11-11T10:00:00Z",
                    "open": 150.50,
                    "high": 151.25,
                    "low": 150.10,
                    "close": 151.00,
                    "volume": 12500.5
                },
                "cex_analysis": {
                    "exchange": "bitget",
                    "top_movers": [],
                    "has_wallet_ids": False,
                    "data_source": "pattern_based",
                    "trade_count": 1250
                },
                "dex_analysis": {
                    "exchange": "jupiter",
                    "top_movers": [],
                    "has_wallet_ids": True,
                    "data_source": "on_chain",
                    "trade_count": 450
                },
                "correlation": {
                    "score": 0.75,
                    "cex_led_by_seconds": -120,
                    "volume_correlation": 0.82,
                    "timing_score": 0.65,
                    "pattern_matches": [],
                    "conclusion": "Moderate correlation - CEX led by 2 minutes"
                },
                "analysis_metadata": {
                    "analysis_timestamp": "2025-11-11T10:06:00Z",
                    "processing_duration_ms": 2500,
                    "total_trades_analyzed": 1700,
                    "cex_entities_found": 10,
                    "dex_wallets_found": 8,
                    "exchanges": "bitget+jupiter",
                    "symbol": "SOL/USDT",
                    "timeframe": "5m"
                }
            }
        }


class TrackWalletRequest(BaseModel):
    """Request to track CEX pattern on DEX"""
    cex_exchange: ExchangeEnum
    cex_entity_pattern: str = Field(..., description="CEX entity pattern (e.g., whale_5)")
    dex_exchange: str
    symbol: str
    time_range_hours: int = Field(default=24, ge=1, le=168)


class WalletTrackingResponse(BaseModel):
    """Response for wallet tracking"""
    success: bool = True
    cex_entity: str
    potential_dex_wallets: List[Dict]
    match_confidence: List[float]
    conclusion: str


# ==================== ENDPOINTS ====================

@router.post(
    "/analyze",
    response_model=HybridAnalysisResponse,
    status_code=status.HTTP_200_OK,
    summary="Hybrid CEX + DEX Analysis",
    description="Analysiert CEX und DEX parallel und findet Korrelationen"
)
async def analyze_hybrid(
    request: HybridAnalysisRequest = Body(...),
    request_id: str = Depends(log_request)
) -> HybridAnalysisResponse:
    """
    ## 🔀 Hybrid CEX + DEX Analyse
    
    Analysiert CEX und DEX im selben Zeitraum und findet:
    
    ### CEX Analysis (Pattern-based):
    - Identifiziert virtuelle Entities (Whales, Bots, Market Makers)
    - Basiert auf Trading-Pattern
    - KEINE echten Wallet-IDs
    
    ### DEX Analysis (Wallet-based):
    - Identifiziert echte On-Chain Wallets
    - Echte Wallet-Adressen
    - Blockchain-Explorer Links
    
    ### Cross-Exchange Correlation:
    - Volume Correlation
    - Timing Analysis (wer bewegte sich zuerst?)
    - Pattern Matching (CEX Whale = DEX Whale?)
    
    ### Use Case:
    Finde heraus, ob große CEX-Trader auch auf DEX aktiv sind!
    
    ### Beispiel Request:
    ```json
    {
        "cex_exchange": "bitget",
        "dex_exchange": "jupiter",
        "symbol": "SOL/USDT",
        "timeframe": "5m",
        "start_time": "2025-11-11T10:00:00Z",
        "end_time": "2025-11-11T10:05:00Z"
    }
    ```
    """
    try:
        logger.info(
            f"[{request_id}] Hybrid analysis: "
            f"CEX={request.cex_exchange} vs DEX={request.dex_exchange} "
            f"{request.symbol} {request.timeframe}"
        )
        
        # Get Unified Collector via dependency
        from app.core.price_movers.api.dependencies import get_unified_collector
        unified_collector = await get_unified_collector() # Holt den Collector aus der Dependency

        # Initialize Hybrid Analyzer with the real collector
        from app.core.price_movers.services.analyzer_hybrid import HybridPriceMoverAnalyzer

        analyzer = HybridPriceMoverAnalyzer(
            unified_collector=unified_collector, # <-- Jetzt wird der echte Collector übergeben
            use_lightweight=True
        )
        
        # Perform hybrid analysis
        result = await analyzer.analyze_hybrid_candle(
            cex_exchange=request.cex_exchange,
            dex_exchange=request.dex_exchange,
            symbol=request.symbol,
            timeframe=request.timeframe,
            start_time=request.start_time,
            end_time=request.end_time,
            min_impact_threshold=request.min_impact_threshold,
            top_n_wallets=request.top_n_wallets,
            include_trades=False
        )
        
        # Convert to response format
        response = HybridAnalysisResponse(
            candle=CandleData(**result['candle']),
            cex_analysis=CEXAnalysis(**result['cex_analysis']),
            dex_analysis=DEXAnalysis(**result['dex_analysis']),
            correlation=CorrelationResult(**result['correlation']),
            analysis_metadata=HybridAnalysisMetadata(**result['analysis_metadata'])
        )
        
        logger.info(
            f"[{request_id}] Hybrid analysis complete: "
            f"Correlation={result['correlation']['score']:.2f}, "
            f"CEX={len(result['cex_analysis']['top_movers'])} movers, "
            f"DEX={len(result['dex_analysis']['top_movers'])} wallets"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Hybrid analysis error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Hybrid analysis failed: {str(e)}"
        )


@router.post(
    "/track-wallet",
    response_model=WalletTrackingResponse,
    status_code=status.HTTP_200_OK,
    summary="Track CEX Pattern on DEX",
    description="Versucht, ein CEX Trading-Pattern auf DEX zu identifizieren"
)
async def track_wallet_across_exchanges(
    request: TrackWalletRequest = Body(...),
    request_id: str = Depends(log_request)
) -> WalletTrackingResponse:
    """
    ## 🔍 Cross-Exchange Wallet Tracking
    
    Identifiziert potenzielle DEX-Wallets, die einem CEX-Entity-Pattern entsprechen.
    
    ### Wie es funktioniert:
    1. Analysiert das CEX-Trading-Pattern (z.B. "whale_5")
    2. Sucht auf DEX nach Wallets mit ähnlichen Charakteristiken:
       - Ähnliche Trade-Größen
       - Ähnliches Timing
       - Ähnliche Buy/Sell Ratios
    3. Gibt Liste potentieller Matches mit Confidence-Score
    
    ### Use Case:
    "Dieser Whale auf Bitget... ist das derselbe auf Jupiter?"
    
    ### Beispiel Request:
    ```json
    {
        "cex_exchange": "bitget",
        "cex_entity_pattern": "whale_5",
        "dex_exchange": "jupiter",
        "symbol": "SOL/USDT",
        "time_range_hours": 24
    }
    ```
    
    ### Beispiel Response:
    ```json
    {
        "success": true,
        "cex_entity": "whale_5",
        "potential_dex_wallets": [
            {
                "wallet_address": "7xKXtg2CW87d97TXJSDpb...",
                "confidence": 0.85,
                "reason": "Similar volume pattern, timing aligned",
                "explorer_url": "https://solscan.io/account/7xKXt..."
            }
        ],
        "conclusion": "Found 1 high-confidence match on Jupiter"
    }
    ```
    """
    try:
        logger.info(
            f"[{request_id}] Wallet tracking: "
            f"{request.cex_entity_pattern} on {request.cex_exchange} → "
            f"{request.dex_exchange}"
        )
        
        # TODO: Implement actual tracking logic
        # This would:
        # 1. Analyze CEX pattern characteristics
        # 2. Fetch DEX trades in timeframe
        # 3. Find similar patterns
        # 4. Return matches with confidence scores
        
        # Placeholder response
        response = WalletTrackingResponse(
            cex_entity=request.cex_entity_pattern,
            potential_dex_wallets=[],
            match_confidence=[],
            conclusion="Wallet tracking not fully implemented yet"
        )
        
        logger.warning(f"[{request_id}] Wallet tracking not implemented")
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Wallet tracking error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Wallet tracking failed: {str(e)}"
        )


@router.get(
    "/correlation/history",
    summary="Get Correlation History",
    description="Historische Korrelation zwischen CEX und DEX"
)
async def get_correlation_history(
    cex_exchange: ExchangeEnum,
    dex_exchange: str,
    symbol: str,
    hours_back: int = 24,
    request_id: str = Depends(log_request)
):
    """
    ## 📊 Correlation History
    
    Zeigt historische Korrelation zwischen CEX und DEX über Zeit.
    
    ### Query Parameters:
    - **cex_exchange**: CEX (bitget/binance/kraken)
    - **dex_exchange**: DEX (jupiter/raydium/orca)
    - **symbol**: Trading Pair
    - **hours_back**: Zeitraum in Stunden
    
    ### Returns:
    - Zeitreihe der Korrelations-Scores
    - Durchschnittliche Korrelation
    - Leader-Board (wer bewegte sich zuerst?)
    """
    try:
        logger.info(
            f"[{request_id}] Correlation history: "
            f"{cex_exchange} vs {dex_exchange} {symbol}"
        )
        
        # TODO: Implement correlation history
        
        return {
            "success": True,
            "cex_exchange": cex_exchange,
            "dex_exchange": dex_exchange,
            "symbol": symbol,
            "hours_back": hours_back,
            "correlation_history": [],
            "avg_correlation": 0.0,
            "leader_board": {
                "cex_led_count": 0,
                "dex_led_count": 0,
                "simultaneous_count": 0
            },
            "message": "Correlation history not implemented yet"
        }
        
    except Exception as e:
        logger.error(f"[{request_id}] Correlation history error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Correlation history failed: {str(e)}"
        )


@router.get(
    "/supported-dexs",
    summary="Get Supported DEX Exchanges"
)
async def get_supported_dexs():
    """
    ## 📋 Unterstützte DEX Exchanges
    
    Liste aller unterstützten DEX-Exchanges für Hybrid-Analyse.
    """
    from app.core.price_movers.utils.constants import SUPPORTED_DEXS, DEX_CONFIGS
    
    dex_list = []
    
    for dex in SUPPORTED_DEXS:
        config = DEX_CONFIGS.get(dex, {})
        dex_list.append({
            "id": dex,
            "name": config.get("name", dex),
            "blockchain": config.get("blockchain", {}).value if hasattr(config.get("blockchain"), 'value') else str(config.get("blockchain", "unknown")),
            "has_wallet_ids": True,
            "api_provider": config.get("api_provider", "unknown")
        })
    
    return {
        "success": True,
        "total_dexs": len(dex_list),
        "dexs": dex_list
    }


# Export Router
__all__ = ['router']
