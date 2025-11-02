"""
Enhanced FastAPI Routes für Price Movers API mit Chart-Support (IMPROVED)

WICHTIGE ÄNDERUNG:
- Warnung hinzugefügt für historische Daten
- OHLCV-basierte Analyse als Fallback
- Trades sind nur ~5-10 Minuten verfügbar bei CEX!

Neue Endpoints für interaktiven Candlestick Chart:
- GET /api/v1/chart/candles - Candlestick-Daten für Chart
- GET /api/v1/chart/candle/{timestamp}/movers - Price Movers für spezifische Candle
- POST /api/v1/chart/batch-analyze - Batch-Analyse für mehrere Candles
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException, Depends, status, Query

from app.core.price_movers.api.test_schemas import (
    CandleData,
    WalletMover,
    AnalysisMetadata,
    ExchangeEnum,
    TimeframeEnum,
    ErrorResponse,
)
from app.core.price_movers.api.dependencies import (
    get_exchange_collector,
    get_analyzer,
    log_request,
)
from app.core.price_movers.collectors import ExchangeCollector
from app.core.price_movers.services import PriceMoverAnalyzer
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)

# Router erstellen
router = APIRouter(
    prefix="/api/v1/chart",
    tags=["chart"],
    responses={
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


# ==================== NEW SCHEMAS ====================

class ChartCandlesRequest(BaseModel):
    """Request für Chart Candle-Daten"""
    exchange: ExchangeEnum = Field(..., description="Exchange")
    symbol: str = Field(..., description="Trading pair (e.g., BTC/USDT)")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    start_time: datetime = Field(..., description="Start of time range")
    end_time: datetime = Field(..., description="End of time range")
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "start_time": "2025-10-30T10:00:00Z",
                "end_time": "2025-10-30T12:00:00Z"
            }
        }


class ChartCandleWithImpact(BaseModel):
    """Candle mit Impact-Indikator für Chart"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    has_high_impact: bool = Field(False, description="Hat diese Candle High-Impact Movers?")
    total_impact_score: float = Field(0.0, description="Gesamter Impact Score aller Movers")
    top_mover_count: int = Field(0, description="Anzahl signifikanter Movers")
    is_synthetic: bool = Field(False, description="Basiert auf OHLCV-Daten (historisch)?")


class ChartCandlesResponse(BaseModel):
    """Response mit Chart Candle-Daten"""
    success: bool = True
    symbol: str
    exchange: str
    timeframe: str
    candles: List[ChartCandleWithImpact]
    total_candles: int
    warning: Optional[str] = Field(None, description="Warnung bei historischen Daten")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "symbol": "BTC/USDT",
                "exchange": "binance",
                "timeframe": "5m",
                "candles": [
                    {
                        "timestamp": "2025-10-30T10:00:00Z",
                        "open": 68500.50,
                        "high": 68750.25,
                        "low": 68400.00,
                        "close": 68650.75,
                        "volume": 1250.5,
                        "has_high_impact": True,
                        "total_impact_score": 2.45,
                        "top_mover_count": 8,
                        "is_synthetic": False
                    }
                ],
                "total_candles": 24,
                "warning": None
            }
        }


class CandleMoversResponse(BaseModel):
    """Response mit Price Movers für eine spezifische Candle"""
    success: bool = True
    candle: CandleData
    top_movers: List[WalletMover]
    analysis_metadata: AnalysisMetadata
    is_synthetic: bool = Field(False, description="Basiert auf OHLCV-Daten?")
    warning: Optional[str] = Field(None, description="Warnung bei synthetischen Daten")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "candle": {
                    "timestamp": "2025-10-30T12:00:00Z",
                    "open": 68500.50,
                    "high": 68750.25,
                    "low": 68400.00,
                    "close": 68650.75,
                    "volume": 1250.5
                },
                "top_movers": [],
                "analysis_metadata": {
                    "analysis_timestamp": "2025-10-30T12:05:30Z",
                    "processing_duration_ms": 1250,
                    "total_trades_analyzed": 5420,
                    "unique_wallets_found": 342,
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "5m"
                },
                "is_synthetic": False,
                "warning": None
            }
        }


class BatchAnalyzeRequest(BaseModel):
    """Request für Batch-Analyse mehrerer Candles"""
    exchange: ExchangeEnum
    symbol: str
    timeframe: TimeframeEnum
    candle_timestamps: List[datetime] = Field(..., max_items=50, description="Liste von Candle-Timestamps (max 50)")
    top_n_wallets: int = Field(default=10, ge=1, le=100)


class BatchCandleResult(BaseModel):
    """Ergebnis für eine einzelne Candle im Batch"""
    timestamp: datetime
    candle: Optional[CandleData]
    top_movers: List[WalletMover]
    error: Optional[str] = None
    is_synthetic: bool = Field(False, description="Basiert auf OHLCV?")


class BatchAnalyzeResponse(BaseModel):
    """Response für Batch-Analyse"""
    success: bool = True
    symbol: str
    exchange: str
    timeframe: str
    results: List[BatchCandleResult]
    successful_analyses: int
    failed_analyses: int
    warning: Optional[str] = None


# ==================== HELPER FUNCTIONS ====================

def check_if_historical(start_time: datetime) -> Tuple[bool, Optional[str]]:
    """
    Prüft, ob der Zeitbereich historisch ist (> 10 Minuten)
    
    WICHTIG: Handled timezone-aware und timezone-naive datetimes korrekt!
    
    Args:
        start_time: Zu prüfender Zeitpunkt
        
    Returns:
        (is_historical, warning_message)
    """
    # Hole aktuelle Zeit - matche timezone von start_time
    if start_time.tzinfo is not None:
        # start_time ist timezone-aware → verwende UTC
        now = datetime.now(timezone.utc)
    else:
        # start_time ist timezone-naive → verwende local time
        now = datetime.now()
    
    time_diff = now - start_time
    
    if time_diff > timedelta(minutes=10):
        warning = (
            f"⚠️ Historische Daten angefordert ({time_diff.total_seconds() / 60:.1f} min alt). "
            f"Exchanges speichern Trades nur ~5-10 Minuten. "
            f"Analyse basiert auf OHLCV-Daten (synthetische Trades)."
        )
        return True, warning
    
    return False, None


# ==================== CHART ENDPOINTS ====================

@router.get(
    "/candles",
    response_model=ChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get Chart Candle Data",
    description="Lädt Candlestick-Daten für Chart mit Impact-Indikatoren"
)
async def get_chart_candles(
    exchange: ExchangeEnum = Query(..., description="Exchange"),
    symbol: str = Query(..., description="Trading pair (e.g., BTC/USDT)"),
    timeframe: TimeframeEnum = Query(..., description="Candle timeframe"),
    start_time: datetime = Query(..., description="Start of time range"),
    end_time: datetime = Query(..., description="End of time range"),
    include_impact: bool = Query(default=True, description="Include impact indicators"),
    request_id: str = Depends(log_request)
) -> ChartCandlesResponse:
    """
    ## Chart Candle-Daten
    
    Lädt OHLCV-Daten für einen Zeitraum mit optionalen Impact-Indikatoren.
    
    ⚠️ WICHTIG: Für historische Daten (> 10 Minuten) werden synthetische Trades
    aus OHLCV-Daten verwendet, da Exchanges echte Trades nur kurz speichern!
    
    ### Query Parameter:
    - **exchange**: Exchange (binance, bitget, kraken)
    - **symbol**: Trading Pair (z.B. BTC/USDT)
    - **timeframe**: Candle Timeframe (1m, 5m, 15m, etc.)
    - **start_time**: Start-Zeitpunkt (ISO 8601)
    - **end_time**: End-Zeitpunkt (ISO 8601)
    - **include_impact**: Impact-Indikatoren berechnen (default: true)
    
    ### Returns:
    - Liste von Candles mit OHLCV-Daten
    - Impact-Indikatoren (wenn aktiviert)
    - Warnung bei historischen Daten
    
    ### Verwendung:
    Dieser Endpoint wird vom Chart verwendet, um die initialen Candlestick-Daten zu laden.
    """
    try:
        # Prüfe, ob historisch
        is_historical, warning = check_if_historical(start_time)
        
        if is_historical:
            logger.warning(
                f"[{request_id}] Historical data requested: {start_time} - {end_time}"
            )
        
        logger.info(
            f"[{request_id}] Chart candles request: {exchange} {symbol} {timeframe} "
            f"({start_time} - {end_time})"
        )
        
        # Hole Collector
        collector = await get_exchange_collector(exchange)
        
        # Fetch OHLCV-Daten
        candles_raw = await collector.fetch_ohlcv_range(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time
        )
        
        # Konvertiere zu Chart-Format
        chart_candles = []
        
        for candle_raw in candles_raw:
            chart_candle = ChartCandleWithImpact(
                timestamp=candle_raw['timestamp'],
                open=candle_raw['open'],
                high=candle_raw['high'],
                low=candle_raw['low'],
                close=candle_raw['close'],
                volume=candle_raw['volume'],
                has_high_impact=False,
                total_impact_score=0.0,
                top_mover_count=0,
                is_synthetic=is_historical
            )
            
            # Optional: Berechne Impact-Indikatoren
            if include_impact:
                try:
                    # Quick-Analyse für diese Candle
                    analyzer = PriceMoverAnalyzer(exchange_collector=collector)
                    
                    # Berechne Zeitfenster für diese Candle
                    timeframe_minutes = {
                        "1m": 1, "5m": 5, "15m": 15, "30m": 30,
                        "1h": 60, "4h": 240, "1d": 1440
                    }.get(timeframe, 5)
                    
                    candle_start = candle_raw['timestamp']
                    candle_end = candle_start + timedelta(minutes=timeframe_minutes)
                    
                    result = await analyzer.analyze_candle(
                        exchange=exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        start_time=candle_start,
                        end_time=candle_end,
                        top_n_wallets=5,  # Nur Top 5 für Performance
                        include_trades=False
                    )
                    
                    # Berechne Impact-Score
                    top_movers = result.get('top_movers', [])
                    if top_movers:
                        total_impact = sum(m['impact_score'] for m in top_movers)
                        high_impact_count = sum(1 for m in top_movers if m['impact_score'] > 0.5)
                        
                        chart_candle.total_impact_score = total_impact
                        chart_candle.top_mover_count = len(top_movers)
                        chart_candle.has_high_impact = high_impact_count > 0
                        
                except Exception as e:
                    logger.warning(
                        f"[{request_id}] Failed to calculate impact for candle "
                        f"{candle_raw['timestamp']}: {e}"
                    )
                    # Continue ohne Impact-Daten
            
            chart_candles.append(chart_candle)
        
        response = ChartCandlesResponse(
            symbol=symbol,
            exchange=exchange,
            timeframe=timeframe,
            candles=chart_candles,
            total_candles=len(chart_candles),
            warning=warning
        )
        
        logger.info(
            f"[{request_id}] Chart candles loaded: {len(chart_candles)} candles "
            f"(synthetic: {is_historical})"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Chart candles error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load chart candles: {str(e)}"
        )


@router.get(
    "/candle/{candle_timestamp}/movers",
    response_model=CandleMoversResponse,
    status_code=status.HTTP_200_OK,
    summary="Get Price Movers for Specific Candle",
    description="Lädt Price Movers für eine angeklickte Candle"
)
async def get_candle_movers(
    candle_timestamp: datetime,
    exchange: ExchangeEnum = Query(..., description="Exchange"),
    symbol: str = Query(..., description="Trading pair"),
    timeframe: TimeframeEnum = Query(..., description="Candle timeframe"),
    top_n_wallets: int = Query(default=10, ge=1, le=100, description="Number of top wallets"),
    request_id: str = Depends(log_request)
) -> CandleMoversResponse:
    """
    ## Price Movers für spezifische Candle
    
    Wird aufgerufen, wenn User auf eine Candle im Chart klickt.
    
    ⚠️ WICHTIG: Für historische Candles (> 10 Minuten) werden synthetische Trades
    aus OHLCV-Daten verwendet!
    
    ### Path Parameter:
    - **candle_timestamp**: Timestamp der Candle (ISO 8601)
    
    ### Query Parameter:
    - **exchange**: Exchange
    - **symbol**: Trading Pair
    - **timeframe**: Candle Timeframe
    - **top_n_wallets**: Anzahl Top Wallets (default: 10)
    
    ### Returns:
    - Candle-Daten (OHLCV)
    - Top Movers mit Impact Scores
    - Analyse-Metadaten
    - Warnung bei synthetischen Daten
    
    ### Verwendung:
    Dieser Endpoint wird aufgerufen, wenn der User im Chart auf eine Candle klickt,
    um die einflussreichsten Wallets für diese spezifische Candle zu sehen.
    """
    try:
        # Prüfe, ob historisch
        is_historical, warning = check_if_historical(candle_timestamp)
        
        logger.info(
            f"[{request_id}] Candle movers request: {exchange} {symbol} "
            f"@ {candle_timestamp} (synthetic: {is_historical})"
        )
        
        # Hole Analyzer
        collector = await get_exchange_collector(exchange)
        analyzer = PriceMoverAnalyzer(exchange_collector=collector)
        
        # Berechne Zeitfenster für diese Candle
        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(timeframe, 5)
        
        start_time = candle_timestamp
        end_time = candle_timestamp + timedelta(minutes=timeframe_minutes)
        
        # Führe Analyse aus
        result = await analyzer.analyze_candle(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            top_n_wallets=top_n_wallets,
            include_trades=False
        )
        
        # Konvertiere zu Response
        response = CandleMoversResponse(
            candle=CandleData(**result['candle']),
            top_movers=[WalletMover(**m) for m in result['top_movers']],
            analysis_metadata=AnalysisMetadata(**result['analysis_metadata']),
            is_synthetic=is_historical,
            warning=warning
        )
        
        logger.info(
            f"[{request_id}] Candle movers loaded: "
            f"{len(result['top_movers'])} movers found"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Candle movers error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load candle movers: {str(e)}"
        )


@router.post(
    "/batch-analyze",
    response_model=BatchAnalyzeResponse,
    status_code=status.HTTP_200_OK,
    summary="Batch Analyze Multiple Candles",
    description="Analysiert mehrere Candles in einem Request (für Performance)"
)
async def batch_analyze_candles(
    request: BatchAnalyzeRequest,
    request_id: str = Depends(log_request)
) -> BatchAnalyzeResponse:
    """
    ## Batch-Analyse mehrerer Candles
    
    Analysiert bis zu 50 Candles in einem Request für bessere Performance.
    
    ⚠️ WICHTIG: Für historische Candles werden synthetische Trades verwendet!
    
    ### Request Body:
    - **exchange**: Exchange
    - **symbol**: Trading Pair
    - **timeframe**: Candle Timeframe
    - **candle_timestamps**: Liste von Candle-Timestamps (max 50)
    - **top_n_wallets**: Anzahl Top Wallets pro Candle
    
    ### Returns:
    - Liste von Analyse-Ergebnissen pro Candle
    - Erfolgs-/Fehler-Statistiken
    - Warnung bei historischen Daten
    
    ### Verwendung:
    Kann verwendet werden, um Impact-Daten für alle sichtbaren Candles
    im Chart auf einmal zu laden (z.B. beim Zoom).
    """
    try:
        # Prüfe, ob historisch (nimm frühesten Timestamp)
        earliest_time = min(request.candle_timestamps)
        is_historical, warning = check_if_historical(earliest_time)
        
        logger.info(
            f"[{request_id}] Batch analyze request: {request.exchange} "
            f"{request.symbol} - {len(request.candle_timestamps)} candles "
            f"(synthetic: {is_historical})"
        )
        
        # Validierung
        if len(request.candle_timestamps) > 50:
            raise HTTPException(
                status_code=400,
                detail="Maximum 50 candles per batch request"
            )
        
        # Hole Analyzer
        collector = await get_exchange_collector(request.exchange)
        analyzer = PriceMoverAnalyzer(exchange_collector=collector)
        
        # Berechne Timeframe in Minuten
        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(request.timeframe, 5)
        
        results = []
        successful = 0
        failed = 0
        
        # Analysiere jede Candle
        for timestamp in request.candle_timestamps:
            try:
                start_time = timestamp
                end_time = timestamp + timedelta(minutes=timeframe_minutes)
                
                # Prüfe, ob diese spezifische Candle historisch ist
                candle_is_historical, _ = check_if_historical(timestamp)
                
                result = await analyzer.analyze_candle(
                    exchange=request.exchange,
                    symbol=request.symbol,
                    timeframe=request.timeframe,
                    start_time=start_time,
                    end_time=end_time,
                    top_n_wallets=request.top_n_wallets,
                    include_trades=False
                )
                
                batch_result = BatchCandleResult(
                    timestamp=timestamp,
                    candle=CandleData(**result['candle']),
                    top_movers=[WalletMover(**m) for m in result['top_movers']],
                    is_synthetic=candle_is_historical
                )
                
                results.append(batch_result)
                successful += 1
                
            except Exception as e:
                logger.warning(
                    f"[{request_id}] Failed to analyze candle {timestamp}: {e}"
                )
                
                batch_result = BatchCandleResult(
                    timestamp=timestamp,
                    candle=None,
                    top_movers=[],
                    error=str(e),
                    is_synthetic=False
                )
                
                results.append(batch_result)
                failed += 1
        
        response = BatchAnalyzeResponse(
            symbol=request.symbol,
            exchange=request.exchange,
            timeframe=request.timeframe,
            results=results,
            successful_analyses=successful,
            failed_analyses=failed,
            warning=warning
        )
        
        logger.info(
            f"[{request_id}] Batch analyze complete: "
            f"{successful} successful, {failed} failed"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Batch analyze error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Batch analysis failed: {str(e)}"
        )


# ==================== UTILITY ENDPOINTS ====================

@router.get(
    "/timeframes",
    summary="Get Available Timeframes",
    description="Liste aller verfügbaren Timeframes"
)
async def get_available_timeframes():
    """
    ## Verfügbare Timeframes
    
    Gibt eine Liste aller unterstützten Timeframes zurück.
    """
    return {
        "success": True,
        "timeframes": [
            {"value": "1m", "label": "1 Minute", "seconds": 60},
            {"value": "5m", "label": "5 Minutes", "seconds": 300},
            {"value": "15m", "label": "15 Minutes", "seconds": 900},
            {"value": "30m", "label": "30 Minutes", "seconds": 1800},
            {"value": "1h", "label": "1 Hour", "seconds": 3600},
            {"value": "4h", "label": "4 Hours", "seconds": 14400},
            {"value": "1d", "label": "1 Day", "seconds": 86400},
        ]
    }


@router.get(
    "/symbols",
    summary="Get Available Symbols",
    description="Liste verfügbarer Trading Pairs"
)
async def get_available_symbols(
    exchange: ExchangeEnum = Query(..., description="Exchange")
):
    """
    ## Verfügbare Trading Pairs
    
    Gibt eine Liste aller verfügbaren Trading Pairs für eine Exchange zurück.
    """
    try:
        collector = await get_exchange_collector(exchange)
        
        # Load markets
        if hasattr(collector.exchange, 'load_markets'):
            await collector.exchange.load_markets()
            markets = collector.exchange.markets
            
            symbols = [
                {
                    "symbol": symbol,
                    "base": market['base'],
                    "quote": market['quote']
                }
                for symbol, market in markets.items()
                if market.get('active', True) and market.get('spot', True)
            ]
            
            return {
                "success": True,
                "exchange": exchange,
                "total_symbols": len(symbols),
                "symbols": symbols[:100]  # Limit für Performance
            }
        else:
            return {
                "success": False,
                "error": "Exchange not initialized properly"
            }
            
    except Exception as e:
        logger.error(f"Failed to fetch symbols: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch symbols: {str(e)}"
        )


@router.get(
    "/data-availability",
    summary="Check Data Availability",
    description="Prüft, ob echte Trade-Daten für einen Zeitraum verfügbar sind"
)
async def check_data_availability(
    exchange: ExchangeEnum = Query(..., description="Exchange"),
    symbol: str = Query(..., description="Trading pair"),
    start_time: datetime = Query(..., description="Start time to check")
):
    """
    ## Daten-Verfügbarkeit prüfen
    
    Prüft, ob echte Trade-Daten für den angefragten Zeitraum verfügbar sind,
    oder ob OHLCV-Fallback verwendet werden muss.
    
    ### Returns:
    - `has_real_trades`: Sind echte Trades verfügbar?
    - `will_use_synthetic`: Wird OHLCV-Fallback verwendet?
    - `time_since_request`: Wie alt ist der angefragte Zeitraum?
    - `warning`: Warnung bei synthetischen Daten
    """
    is_historical, warning = check_if_historical(start_time)
    
    # Berechne time_diff timezone-safe
    if start_time.tzinfo is not None:
        now = datetime.now(timezone.utc)
    else:
        now = datetime.now()
    
    time_diff = now - start_time
    
    return {
        "success": True,
        "exchange": exchange,
        "symbol": symbol,
        "start_time": start_time,
        "has_real_trades": not is_historical,
        "will_use_synthetic": is_historical,
        "time_since_request_minutes": time_diff.total_seconds() / 60,
        "warning": warning
    }


# Export Router
__all__ = ['router']
