"""
Hybrid Analysis Routes - BACKWARDS COMPATIBLE VERSION

✅ Funktioniert mit altem Analyzer (ohne separate timeranges)
⚠️ Temporäre Lösung bis Analyzer updated ist
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException, Depends, status, Body
from pydantic import BaseModel, Field, validator

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
    """Request für Hybrid CEX/DEX Analyse - BACKWARDS COMPATIBLE"""
    cex_exchange: ExchangeEnum = Field(..., description="CEX Exchange (bitget/binance/kraken)")
    dex_exchange: str = Field(..., description="DEX Exchange (jupiter/raydium/orca)")
    symbol: str = Field(..., description="Trading pair (e.g., SOL/USDT)")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    
    # Standard Zeitraum (beide nutzen denselben)
    start_time: Optional[datetime] = Field(None, description="Start time (for both CEX and DEX)")
    end_time: Optional[datetime] = Field(None, description="End time (for both CEX and DEX)")
    
    # Settings
    min_impact_threshold: float = Field(default=0.05, ge=0.0, le=1.0)
    top_n_wallets: int = Field(default=10, ge=1, le=100)
    
    def get_timerange(self) -> tuple[datetime, datetime]:
        """Get effective time range with smart defaults"""
        if not self.start_time or not self.end_time:
            # Default: Last 5 minutes (fresh data for trade analysis)
            now = datetime.now(timezone.utc)
            end = now
            start = now - timedelta(minutes=5)
        else:
            start = self.start_time
            end = self.end_time
        
        # Ensure timezone-aware
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        
        return start, end
    
    def is_historical(self) -> bool:
        """Check if timerange is historical (>10 minutes old)"""
        _, end_time = self.get_timerange()
        now = datetime.now(timezone.utc)
        age = (now - end_time).total_seconds()
        return age > 600  # 10 minutes
    
    class Config:
        json_schema_extra = {
            "example": {
                "cex_exchange": "bitget",
                "dex_exchange": "jupiter",
                "symbol": "SOL/USDT",
                "timeframe": "5m",
                "start_time": "2025-11-13T10:00:00Z",
                "end_time": "2025-11-13T10:05:00Z",
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
    is_historical: bool = False
    warning: Optional[str] = None


class DEXAnalysis(BaseModel):
    """DEX Analysis Result"""
    exchange: str
    top_movers: List[Dict]
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
    warnings: List[str] = []


class HybridAnalysisResponse(BaseModel):
    """Response für Hybrid Analyse"""
    success: bool = True
    candle: CandleData
    cex_analysis: CEXAnalysis
    dex_analysis: DEXAnalysis
    correlation: CorrelationResult
    analysis_metadata: HybridAnalysisMetadata


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
    description="Analysiert CEX und DEX parallel (Backwards Compatible)"
)
async def analyze_hybrid(
    request: HybridAnalysisRequest = Body(...),
    request_id: str = Depends(log_request)
) -> HybridAnalysisResponse:
    """
    ## 🔀 Hybrid CEX + DEX Analyse
    
    **BACKWARDS COMPATIBLE VERSION**
    - Nutzt denselben Zeitraum für CEX und DEX
    - Funktioniert mit altem Analyzer
    
    ### Use Cases:
    
    **Live Price Mover Analysis** (letzte 5 Min):
    ```json
    {
        "cex_exchange": "bitget",
        "dex_exchange": "jupiter",
        "symbol": "SOL/USDT",
        "timeframe": "5m"
    }
    ```
    
    **Historical Analysis**:
    ```json
    {
        "cex_exchange": "bitget",
        "dex_exchange": "jupiter",
        "symbol": "SOL/USDT",
        "timeframe": "5m",
        "start_time": "2025-11-12T10:00:00Z",
        "end_time": "2025-11-12T10:05:00Z"
    }
    ```
    """
    try:
        # Get timerange
        start_time, end_time = request.get_timerange()
        
        # Warnings
        warnings = []
        if request.is_historical():
            warnings.append(
                f"⚠️ Timerange is historical (>10 min old). "
                f"CEX trade-level entity analysis may be limited (OHLCV fallback). "
                f"DEX wallet data should be available."
            )
        
        logger.info(
            f"[{request_id}] Hybrid analysis: "
            f"CEX={request.cex_exchange} vs DEX={request.dex_exchange} "
            f"{request.symbol} {request.timeframe} "
            f"[{start_time.strftime('%H:%M:%S')}-{end_time.strftime('%H:%M:%S')}]"
        )
        
        # Get Unified Collector
        from app.core.price_movers.api.dependencies import get_unified_collector
        unified_collector = await get_unified_collector()

        # Initialize Analyzer
        from app.core.price_movers.services.analyzer_hybrid import HybridPriceMoverAnalyzer
        analyzer = HybridPriceMoverAnalyzer(
            unified_collector=unified_collector,
            use_lightweight=True
        )
        
        # ✅ Perform analysis with OLD signature (backwards compatible)
        result = await analyzer.analyze_hybrid_candle(
            cex_exchange=request.cex_exchange,
            dex_exchange=request.dex_exchange,
            symbol=request.symbol,
            timeframe=request.timeframe,
            start_time=start_time,  # ✅ OLD parameter
            end_time=end_time,      # ✅ OLD parameter
            min_impact_threshold=request.min_impact_threshold,
            top_n_wallets=request.top_n_wallets,
            include_trades=False
        )
        
        # Add warnings to result
        if 'analysis_metadata' not in result:
            result['analysis_metadata'] = {}
        
        result['analysis_metadata']['warnings'] = warnings
        
        # Add historical flag to CEX analysis
        if 'cex_analysis' not in result:
            result['cex_analysis'] = {}
        
        result['cex_analysis']['is_historical'] = request.is_historical()
        if warnings:
            result['cex_analysis']['warning'] = warnings[0]
        
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
    """Cross-Exchange Wallet Tracking"""
    try:
        logger.info(
            f"[{request_id}] Wallet tracking: "
            f"{request.cex_entity_pattern} on {request.cex_exchange} → "
            f"{request.dex_exchange}"
        )
        
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
    """Correlation History"""
    try:
        logger.info(
            f"[{request_id}] Correlation history: "
            f"{cex_exchange} vs {dex_exchange} {symbol}"
        )
        
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
    """Liste aller unterstützten DEX-Exchanges"""
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
