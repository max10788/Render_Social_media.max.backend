"""
Hybrid Analysis Routes - CEX + DEX Combined Analysis - ENHANCED VERSION

🆕 NEUE FEATURES:
1. ✅ Separate Zeiträume für CEX und DEX
2. ✅ Analysis Modes: trades / candles / auto
3. ✅ Validation für historische CEX-Daten
4. ✅ Smart Defaults und Warnings

Neue Endpoints:
- POST /api/v1/hybrid/analyze - Parallel CEX + DEX Analyse (ENHANCED)
- POST /api/v1/hybrid/track-wallet - Tracke CEX Pattern auf DEX
- GET /api/v1/hybrid/correlation - Correlation History
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

class AnalysisMode(str):
    """Analysis Mode Enum"""
    TRADES = "trades"  # Trade-level analysis (nur für frische Daten)
    CANDLES = "candles"  # Nur OHLCV-Vergleich
    AUTO = "auto"  # Automatische Auswahl


class HybridAnalysisRequest(BaseModel):
    """
    Request für Hybrid CEX/DEX Analyse - ENHANCED VERSION
    
    🆕 Neue Features:
    - Separate Zeiträume für CEX und DEX (optional)
    - Analysis Mode (trades/candles/auto)
    - Auto-Validation
    """
    # Exchanges
    cex_exchange: ExchangeEnum = Field(..., description="CEX Exchange (bitget/binance/kraken)")
    dex_exchange: str = Field(..., description="DEX Exchange (jupiter/raydium/orca)")
    symbol: str = Field(..., description="Trading pair (e.g., SOL/USDT)")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    
    # Zeiträume - Default (für beide gleich)
    start_time: Optional[datetime] = Field(None, description="Default start time (for both CEX and DEX)")
    end_time: Optional[datetime] = Field(None, description="Default end time (for both CEX and DEX)")
    
    # 🆕 Separate Zeiträume (optional)
    cex_start_time: Optional[datetime] = Field(None, description="CEX-specific start time (overrides start_time)")
    cex_end_time: Optional[datetime] = Field(None, description="CEX-specific end time (overrides end_time)")
    dex_start_time: Optional[datetime] = Field(None, description="DEX-specific start time (overrides start_time)")
    dex_end_time: Optional[datetime] = Field(None, description="DEX-specific end time (overrides end_time)")
    
    # 🆕 Analysis Mode
    analysis_mode: str = Field(
        default="auto",
        description="Analysis mode: 'trades' (trade-level), 'candles' (OHLCV only), 'auto' (automatic)"
    )
    
    # Settings
    min_impact_threshold: float = Field(default=0.05, ge=0.0, le=1.0)
    top_n_wallets: int = Field(default=10, ge=1, le=100)
    
    @validator('analysis_mode')
    def validate_analysis_mode(cls, v):
        """Validate analysis mode"""
        valid_modes = ['trades', 'candles', 'auto']
        if v not in valid_modes:
            raise ValueError(f"analysis_mode must be one of {valid_modes}")
        return v
    
    def get_cex_timerange(self) -> tuple[datetime, datetime]:
        """Get effective CEX time range"""
        start = self.cex_start_time or self.start_time
        end = self.cex_end_time or self.end_time
        
        if not start or not end:
            # Default: Last 5 minutes (fresh CEX data)
            now = datetime.now(timezone.utc)
            end = now
            start = now - timedelta(minutes=5)
        
        return start, end
    
    def get_dex_timerange(self) -> tuple[datetime, datetime]:
        """Get effective DEX time range"""
        start = self.dex_start_time or self.start_time
        end = self.dex_end_time or self.end_time
        
        if not start or not end:
            # Default: Same as CEX
            return self.get_cex_timerange()
        
        return start, end
    
    def is_cex_historical(self) -> bool:
        """Check if CEX timerange is historical (>10 minutes old)"""
        _, cex_end = self.get_cex_timerange()
        now = datetime.now(timezone.utc)
        age = (now - cex_end).total_seconds()
        return age > 600  # 10 minutes
    
    def get_effective_mode(self) -> str:
        """Get effective analysis mode based on data freshness"""
        if self.analysis_mode == "trades":
            return "trades"
        elif self.analysis_mode == "candles":
            return "candles"
        else:  # auto
            # Auto: Use trades if CEX is fresh, otherwise candles
            if self.is_cex_historical():
                return "candles"
            else:
                return "trades"
    
    class Config:
        json_schema_extra = {
            "example": {
                "cex_exchange": "bitget",
                "dex_exchange": "jupiter",
                "symbol": "SOL/USDT",
                "timeframe": "5m",
                "start_time": "2025-11-11T10:00:00Z",
                "end_time": "2025-11-11T10:05:00Z",
                "analysis_mode": "auto",
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
    analysis_mode: str  # 🆕
    cex_timerange: str  # 🆕
    dex_timerange: str  # 🆕
    warnings: List[str] = []  # 🆕


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
                    "trade_count": 1250,
                    "is_historical": False
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
                    "timeframe": "5m",
                    "analysis_mode": "trades",
                    "cex_timerange": "10:00:00-10:05:00",
                    "dex_timerange": "10:00:00-10:05:00"
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
    summary="Hybrid CEX + DEX Analysis (Enhanced)",
    description="Analysiert CEX und DEX mit flexiblen Zeiträumen und Modes"
)
async def analyze_hybrid(
    request: HybridAnalysisRequest = Body(...),
    request_id: str = Depends(log_request)
) -> HybridAnalysisResponse:
    """
    ## 🔀 Hybrid CEX + DEX Analyse - ENHANCED VERSION
    
    ### 🆕 Neue Features:
    
    #### 1. Separate Zeiträume
    Du kannst nun unterschiedliche Zeiträume für CEX und DEX angeben:
    ```json
    {
        "cex_start_time": "2025-11-11T10:00:00Z",  // Letzte 5 Min (frisch!)
        "cex_end_time": "2025-11-11T10:05:00Z",
        "dex_start_time": "2025-11-10T10:00:00Z",  // Gestern (historisch)
        "dex_end_time": "2025-11-10T10:05:00Z"
    }
    ```
    
    #### 2. Analysis Modes
    - **"trades"**: Trade-level Analyse (nur bei frischen CEX-Daten <10 Min)
    - **"candles"**: Nur OHLCV-Vergleich (immer möglich, auch historisch)
    - **"auto"**: Automatische Auswahl (empfohlen)
    
    #### 3. Auto-Validation
    - Warnung wenn CEX-Zeitraum historisch ist (>10 Min)
    - Auto-Fallback zu "candles" Mode
    
    ### Use Cases:
    
    **Live Price Mover Analysis** (letzte 5-10 Min):
    ```json
    {
        "cex_exchange": "bitget",
        "dex_exchange": "jupiter",
        "symbol": "SOL/USDT",
        "timeframe": "5m",
        "analysis_mode": "trades"  // Trade-level mit Entities
    }
    ```
    
    **Historical Candle Comparison**:
    ```json
    {
        "cex_exchange": "bitget",
        "dex_exchange": "jupiter",
        "symbol": "SOL/USDT",
        "timeframe": "5m",
        "start_time": "2025-11-10T10:00:00Z",  // Gestern
        "end_time": "2025-11-10T10:05:00Z",
        "analysis_mode": "candles"  // Nur OHLCV
    }
    ```
    
    **Mixed Timeframes** (frischer CEX, historischer DEX):
    ```json
    {
        "cex_start_time": "2025-11-11T10:00:00Z",  // Jetzt
        "cex_end_time": "2025-11-11T10:05:00Z",
        "dex_start_time": "2025-11-10T10:00:00Z",  // Gestern
        "dex_end_time": "2025-11-10T10:05:00Z",
        "analysis_mode": "auto"
    }
    ```
    """
    try:
        # Get effective timeranges
        cex_start, cex_end = request.get_cex_timerange()
        dex_start, dex_end = request.get_dex_timerange()
        effective_mode = request.get_effective_mode()
        
        # Warnings
        warnings = []
        if request.is_cex_historical():
            warnings.append(
                f"⚠️ CEX timerange is historical (>10 min old). "
                f"Trade-level entity analysis may be limited. "
                f"Consider using analysis_mode='candles' for historical data."
            )
        
        if effective_mode != request.analysis_mode:
            warnings.append(
                f"ℹ️ Analysis mode auto-adjusted from '{request.analysis_mode}' "
                f"to '{effective_mode}' based on data freshness."
            )
        
        logger.info(
            f"[{request_id}] Hybrid analysis: "
            f"CEX={request.cex_exchange} [{cex_start.strftime('%H:%M:%S')}-{cex_end.strftime('%H:%M:%S')}] vs "
            f"DEX={request.dex_exchange} [{dex_start.strftime('%H:%M:%S')}-{dex_end.strftime('%H:%M:%S')}] "
            f"{request.symbol} {request.timeframe} (mode={effective_mode})"
        )
        
        # Get Unified Collector via dependency
        from app.core.price_movers.api.dependencies import get_unified_collector
        unified_collector = await get_unified_collector()

        # Initialize Hybrid Analyzer with the real collector
        from app.core.price_movers.services.analyzer_hybrid import HybridPriceMoverAnalyzer

        analyzer = HybridPriceMoverAnalyzer(
            unified_collector=unified_collector,
            use_lightweight=True
        )
        
        # Perform hybrid analysis with separate timeranges
        result = await analyzer.analyze_hybrid_candle(
            cex_exchange=request.cex_exchange,
            dex_exchange=request.dex_exchange,
            symbol=request.symbol,
            timeframe=request.timeframe,
            cex_start_time=cex_start,  # 🆕 Separate
            cex_end_time=cex_end,      # 🆕 Separate
            dex_start_time=dex_start,  # 🆕 Separate
            dex_end_time=dex_end,      # 🆕 Separate
            analysis_mode=effective_mode,  # 🆕 Mode
            min_impact_threshold=request.min_impact_threshold,
            top_n_wallets=request.top_n_wallets,
            include_trades=False
        )
        
        # Add warnings to result
        result['analysis_metadata']['warnings'] = warnings
        result['analysis_metadata']['analysis_mode'] = effective_mode
        result['analysis_metadata']['cex_timerange'] = f"{cex_start.strftime('%H:%M:%S')}-{cex_end.strftime('%H:%M:%S')}"
        result['analysis_metadata']['dex_timerange'] = f"{dex_start.strftime('%H:%M:%S')}-{dex_end.strftime('%H:%M:%S')}"
        
        # Add historical flag to CEX analysis
        result['cex_analysis']['is_historical'] = request.is_cex_historical()
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
            f"DEX={len(result['dex_analysis']['top_movers'])} wallets, "
            f"Mode={effective_mode}"
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
    """Cross-Exchange Wallet Tracking - siehe Original-Doku"""
    try:
        logger.info(
            f"[{request_id}] Wallet tracking: "
            f"{request.cex_entity_pattern} on {request.cex_exchange} → "
            f"{request.dex_exchange}"
        )
        
        # TODO: Implement actual tracking logic
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
    """Correlation History - siehe Original-Doku"""
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
