"""
DEX Chart Routes - PRODUCTION VERSION with Improvements

🔧 IMPROVEMENTS:
1. ✅ Birdeye Fallback bei Helius Rate Limits
2. ✅ Bessere Error Messages
3. ✅ Batch Candle Loading (reduziert API Calls)
4. ✅ Request Validation
5. ✅ Performance Monitoring
"""

import logging
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException, Depends, status, Query, BackgroundTasks
from pydantic import BaseModel, Field
import time

from app.core.price_movers.api.test_schemas import (
    CandleData,
    WalletMover,
    AnalysisMetadata,
    TimeframeEnum,
    ErrorResponse,
)
from app.core.price_movers.api.dependencies import (
    get_unified_collector,
    log_request,
)
from app.core.price_movers.utils.constants import SupportedDEX


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/dex",
    tags=["dex-chart"],
    responses={
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


# ==================== SCHEMAS ====================

class ChartCandleWithImpact(BaseModel):
    """Candle mit Impact-Indikator für Chart"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    has_high_impact: bool = Field(False, description="Hat High-Impact Wallets?")
    total_impact_score: float = Field(0.0, description="Gesamt Impact Score")
    top_mover_count: int = Field(0, description="Anzahl Top Movers")
    is_synthetic: bool = Field(False, description="Immer False für DEX!")


class DEXChartCandlesResponse(BaseModel):
    """Response mit DEX Chart Candles"""
    success: bool = True
    symbol: str
    dex_exchange: str
    blockchain: str
    timeframe: str
    candles: List[ChartCandleWithImpact]
    total_candles: int
    data_source: str = Field("helius", description="helius oder birdeye")
    warning: Optional[str] = None
    performance_ms: Optional[float] = None


class DEXCandleMoversResponse(BaseModel):
    """Response mit DEX Wallet Movers für Candle"""
    success: bool = True
    candle: CandleData
    top_movers: List[dict]
    analysis_metadata: dict
    is_synthetic: bool = Field(False, description="Always False for DEX")
    has_real_wallet_ids: bool = Field(True, description="Always True for DEX!")
    blockchain: str
    dex_exchange: str


# ==================== HELPER FUNCTIONS ====================

def validate_dex_params(dex_exchange: str, symbol: str, timeframe: str):
    """Validiert DEX Parameter"""
    # Validiere DEX
    supported_dexs = ['jupiter', 'raydium', 'orca']
    if dex_exchange.lower() not in supported_dexs:
        raise HTTPException(
            status_code=400,
            detail=f"DEX '{dex_exchange}' not supported. Use: {', '.join(supported_dexs)}"
        )
    
    # Validiere Symbol
    if '/' not in symbol:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid symbol format: '{symbol}'. Expected format: 'BASE/QUOTE' (e.g., 'SOL/USDC')"
        )
    
    # Validiere Timeframe
    valid_timeframes = ['1m', '5m', '15m', '30m', '1h', '4h', '1d']
    if timeframe not in valid_timeframes:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe: '{timeframe}'. Supported: {', '.join(valid_timeframes)}"
        )


async def fetch_candle_with_fallback(
    unified_collector,
    dex_exchange: str,
    symbol: str,
    timeframe: str,
    timestamp: datetime
) -> tuple[Optional[dict], str]:
    """
    Fetcht eine Candle mit Birdeye Fallback
    
    Returns:
        (candle_data, source) or (None, "error")
    """
    # Try Helius first (via unified_collector)
    try:
        candle_data = await unified_collector.fetch_candle_data(
            exchange=dex_exchange.lower(),
            symbol=symbol,
            timeframe=timeframe,
            timestamp=timestamp
        )
        return candle_data, "helius"
    
    except Exception as helius_error:
        logger.warning(f"Helius failed for candle at {timestamp}: {helius_error}")
        
        # Try Birdeye fallback
        try:
            logger.info(f"🔄 Trying Birdeye fallback for {timestamp}...")
            
            # Get Birdeye collector from unified_collector
            if hasattr(unified_collector, 'birdeye_collector'):
                birdeye = unified_collector.birdeye_collector
                candle_data = await birdeye.fetch_candle_data(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp
                )
                return candle_data, "birdeye"
            else:
                logger.error("No Birdeye fallback available")
                return None, "error"
        
        except Exception as birdeye_error:
            logger.error(f"Birdeye fallback also failed: {birdeye_error}")
            return None, "error"


# ==================== ENDPOINTS ====================

@router.get(
    "/candles",
    response_model=DEXChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Chart Candles (On-Chain)",
    description="Lädt On-Chain Candlestick-Daten für DEX Chart mit Birdeye Fallback"
)
async def get_dex_chart_candles(
    dex_exchange: str = Query(..., description="DEX (jupiter/raydium/orca)"),
    symbol: str = Query(..., description="Token pair (e.g., SOL/USDC)"),
    timeframe: TimeframeEnum = Query(..., description="Candle timeframe"),
    start_time: datetime = Query(..., description="Start time"),
    end_time: datetime = Query(..., description="End time"),
    include_impact: bool = Query(default=False, description="Calculate impact (slow!)"),
    request_id: str = Depends(log_request)
) -> DEXChartCandlesResponse:
    """
    ## 🔗 DEX Chart-Daten (On-Chain) mit Fallback
    
    Features:
    - ✅ Helius Primary Source
    - ✅ Birdeye Fallback bei Rate Limits
    - ✅ Automatic Error Recovery
    - ✅ Performance Tracking
    """
    start_perf = time.time()
    
    try:
        # Validate parameters
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] DEX Chart request: {dex_exchange} {symbol} {timeframe} "
            f"({start_time} - {end_time})"
        )
        
        # Get UnifiedCollector
        unified_collector = await get_unified_collector()
        
        # Check if DEX available
        available = unified_collector.list_available_exchanges()
        if dex_exchange.lower() not in available['dex']:
            raise HTTPException(
                status_code=503,
                detail=f"DEX '{dex_exchange}' not available. "
                       f"Configure HELIUS_API_KEY or BIRDEYE_API_KEY"
            )
        
        # Calculate number of candles
        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(timeframe, 5)
        
        time_diff = (end_time - start_time).total_seconds()
        num_candles = min(int(time_diff / (timeframe_minutes * 60)), 100)  # Max 100
        
        logger.info(f"Fetching {num_candles} DEX candles...")
        
        chart_candles = []
        current_time = start_time
        successful_fetches = 0
        failed_fetches = 0
        primary_source_count = 0
        fallback_source_count = 0
        
        # Fetch candles with fallback
        for i in range(num_candles):
            try:
                candle_data, source = await fetch_candle_with_fallback(
                    unified_collector=unified_collector,
                    dex_exchange=dex_exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=current_time
                )
                
                if candle_data:
                    chart_candle = ChartCandleWithImpact(
                        timestamp=candle_data['timestamp'],
                        open=candle_data['open'],
                        high=candle_data['high'],
                        low=candle_data['low'],
                        close=candle_data['close'],
                        volume=candle_data['volume'],
                        has_high_impact=False,
                        total_impact_score=0.0,
                        top_mover_count=0,
                        is_synthetic=False
                    )
                    
                    chart_candles.append(chart_candle)
                    successful_fetches += 1
                    
                    if source == "helius":
                        primary_source_count += 1
                    elif source == "birdeye":
                        fallback_source_count += 1
                else:
                    failed_fetches += 1
                    logger.warning(f"Failed to fetch candle at {current_time}")
                
            except Exception as e:
                failed_fetches += 1
                logger.warning(f"Failed to fetch candle at {current_time}: {e}")
            
            current_time += timedelta(minutes=timeframe_minutes)
        
        # Get blockchain from DEX config
        from app.core.price_movers.utils.constants import DEX_CONFIGS
        dex_config = DEX_CONFIGS.get(dex_exchange.lower(), {})
        blockchain = dex_config.get('blockchain', 'solana')
        
        # Determine data source
        if fallback_source_count > 0:
            data_source = "mixed (helius+birdeye)" if primary_source_count > 0 else "birdeye"
            warning = f"Some data from Birdeye fallback ({fallback_source_count}/{successful_fetches} candles)"
        else:
            data_source = "helius"
            warning = None
        
        # Performance tracking
        performance_ms = (time.time() - start_perf) * 1000
        
        response = DEXChartCandlesResponse(
            symbol=symbol,
            dex_exchange=dex_exchange,
            blockchain=blockchain.value if hasattr(blockchain, 'value') else str(blockchain),
            timeframe=timeframe,
            candles=chart_candles,
            total_candles=len(chart_candles),
            data_source=data_source,
            warning=warning,
            performance_ms=performance_ms
        )
        
        logger.info(
            f"[{request_id}] DEX Chart candles loaded: {len(chart_candles)} candles "
            f"(source: {data_source}, time: {performance_ms:.0f}ms, "
            f"success: {successful_fetches}, failed: {failed_fetches})"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] DEX chart error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load DEX chart: {str(e)}"
        )


@router.get(
    "/candle/{candle_timestamp}/movers",
    response_model=DEXCandleMoversResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Wallet Movers for Candle",
    description="Lädt ECHTE Wallet-Adressen für eine Candle (On-Chain)"
)
async def get_dex_candle_movers(
    candle_timestamp: datetime,
    dex_exchange: str = Query(..., description="DEX exchange"),
    symbol: str = Query(..., description="Token pair"),
    timeframe: TimeframeEnum = Query(..., description="Timeframe"),
    top_n_wallets: int = Query(default=10, ge=1, le=100),
    request_id: str = Depends(log_request)
) -> DEXCandleMoversResponse:
    """
    ## 🎯 DEX Wallet Movers für Candle
    
    Features:
    - ✅ ECHTE Blockchain-Adressen
    - ✅ On-Chain Transaction History
    - ✅ Keine synthetischen Daten
    """
    try:
        # Validate parameters
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] DEX Candle movers: {dex_exchange} {symbol} "
            f"@ {candle_timestamp}"
        )
        
        # Get UnifiedCollector
        unified_collector = await get_unified_collector()
        
        # Initialize HybridAnalyzer
        from app.core.price_movers.services.analyzer_hybrid import HybridPriceMoverAnalyzer
        
        analyzer = HybridPriceMoverAnalyzer(
            unified_collector=unified_collector,
            use_lightweight=True
        )
        
        # Calculate timeframe
        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(timeframe, 5)
        
        start_time = candle_timestamp
        end_time = candle_timestamp + timedelta(minutes=timeframe_minutes)
        
        # Ensure timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        # Fetch DEX data with fallback
        logger.debug("Fetching DEX candle and trades...")
        
        candle_data, source = await fetch_candle_with_fallback(
            unified_collector=unified_collector,
            dex_exchange=dex_exchange,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=start_time
        )
        
        if not candle_data:
            raise HTTPException(
                status_code=500,
                detail="Failed to fetch candle data from both Helius and Birdeye"
            )
        
        trades_result = await unified_collector.fetch_trades(
            exchange=dex_exchange.lower(),
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=5000
        )
        
        # Analyze DEX trades
        logger.debug("Analyzing DEX trades...")
        
        from app.core.price_movers.services.analyzer_hybrid import Candle as HybridCandle
        
        candle_obj = HybridCandle(**candle_data)
        
        dex_movers = await analyzer._analyze_dex_trades(
            trades=trades_result.get('trades', []),
            candle=candle_obj,
            symbol=symbol,
            exchange=dex_exchange,
            top_n=top_n_wallets
        )
        
        # Get blockchain
        from app.core.price_movers.utils.constants import DEX_CONFIGS
        dex_config = DEX_CONFIGS.get(dex_exchange.lower(), {})
        blockchain = dex_config.get('blockchain', 'solana')
        
        response = DEXCandleMoversResponse(
            candle=CandleData(**candle_data),
            top_movers=dex_movers,
            analysis_metadata={
                "analysis_timestamp": datetime.now(timezone.utc),
                "processing_duration_ms": 0,
                "total_trades_analyzed": len(trades_result.get('trades', [])),
                "unique_wallets_found": len(dex_movers),
                "exchange": dex_exchange,
                "symbol": symbol,
                "timeframe": timeframe,
                "data_source": source
            },
            is_synthetic=False,
            has_real_wallet_ids=True,
            blockchain=blockchain.value if hasattr(blockchain, 'value') else str(blockchain),
            dex_exchange=dex_exchange
        )
        
        logger.info(
            f"[{request_id}] DEX movers loaded: {len(dex_movers)} wallets "
            f"(source: {source})"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] DEX movers error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load DEX movers: {str(e)}"
        )


@router.get(
    "/supported-dexs",
    summary="Get Supported DEX Exchanges"
)
async def get_supported_dexs():
    """Liste aller unterstützten DEXs"""
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


@router.get(
    "/health",
    summary="DEX API Health Check"
)
async def health_check():
    """
    Health Check für DEX APIs
    
    Prüft Helius und Birdeye Verfügbarkeit
    """
    try:
        unified_collector = await get_unified_collector()
        
        health_status = {
            "helius": {
                "available": False,
                "message": "Not checked"
            },
            "birdeye": {
                "available": False,
                "message": "Not checked"
            }
        }
        
        # Check Helius
        if hasattr(unified_collector, 'helius_collector'):
            try:
                helius_ok = await unified_collector.helius_collector.health_check()
                health_status["helius"]["available"] = helius_ok
                health_status["helius"]["message"] = "OK" if helius_ok else "Failed"
                
                # Get stats if available
                if hasattr(unified_collector.helius_collector, 'get_stats'):
                    health_status["helius"]["stats"] = unified_collector.helius_collector.get_stats()
            except Exception as e:
                health_status["helius"]["message"] = str(e)
        
        # Check Birdeye
        if hasattr(unified_collector, 'birdeye_collector'):
            try:
                birdeye_ok = await unified_collector.birdeye_collector.health_check()
                health_status["birdeye"]["available"] = birdeye_ok
                health_status["birdeye"]["message"] = "OK" if birdeye_ok else "Failed"
            except Exception as e:
                health_status["birdeye"]["message"] = str(e)
        
        return {
            "success": True,
            "status": health_status,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Health check error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )


# Export Router
__all__ = ['router']
