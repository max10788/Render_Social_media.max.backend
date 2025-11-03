"""
Quick Analysis Routes für Price Movers API

Neue Endpoints:
- POST /api/v1/analyze/quick - Schnelle Analyse der aktuellen Candle
"""

import logging
from typing import Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, status

from app.core.price_movers.api.test_schemas import (
    QuickAnalysisRequest,
    AnalysisResponse,
    CandleData,
    WalletMover,
    AnalysisMetadata,
    ErrorResponse,
)
from app.core.price_movers.api.dependencies import (
    get_exchange_collector,
    get_analyzer,
    log_request,
)
from app.core.price_movers.services import PriceMoverAnalyzer


logger = logging.getLogger(__name__)

# Router erstellen mit korrektem Prefix
router = APIRouter(
    prefix="/api/v1/analyze",
    tags=["analyze"],
    responses={
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


@router.post(
    "/quick",
    response_model=AnalysisResponse,
    status_code=status.HTTP_200_OK,
    summary="Quick Analysis of Latest Candle",
    description="Führt eine schnelle Analyse der aktuellsten Candle durch und identifiziert Top Price Movers"
)
async def quick_analyze(
    request: QuickAnalysisRequest,
    request_id: str = Depends(log_request)
) -> AnalysisResponse:
    """
    ## Quick Analyse
    
    Analysiert die aktuellste Candle eines Trading Pairs und identifiziert
    die einflussreichsten Wallets (Price Movers).
    
    ### Request Body:
    - **exchange**: Exchange (binance, bitget, kraken)
    - **symbol**: Trading Pair (z.B. BTC/USDT)
    - **timeframe**: Candle Timeframe (1m, 5m, 15m, etc.)
    - **top_n_wallets**: Anzahl der Top Wallets (default: 10)
    
    ### Returns:
    - Candle-Daten (OHLCV)
    - Top Price Movers mit Impact Scores
    - Analyse-Metadaten
    
    ### Verwendung:
    Dieser Endpoint wird verwendet, um eine schnelle Echtzeit-Analyse
    der aktuellen Marktsituation zu erhalten.
    
    ### Beispiel Request:
    ```json
    {
        "exchange": "bitget",
        "symbol": "BTC/USDT",
        "timeframe": "5m",
        "top_n_wallets": 10
    }
    ```
    """
    try:
        logger.info(
            f"[{request_id}] Quick analysis request: {request.exchange} "
            f"{request.symbol} {request.timeframe}"
        )
        
        # Hole Exchange Collector
        collector = await get_exchange_collector(request.exchange)
        
        # Initialisiere Analyzer
        analyzer = PriceMoverAnalyzer(exchange_collector=collector)
        
        # Berechne Timeframe in Minuten
        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(request.timeframe, 5)
        
        # Berechne Zeitfenster für die aktuelle Candle
        # Runde auf die letzte vollständige Candle
        now = datetime.utcnow()
        minutes_since_midnight = now.hour * 60 + now.minute
        candles_since_midnight = minutes_since_midnight // timeframe_minutes
        
        end_time = now.replace(
            minute=(candles_since_midnight * timeframe_minutes) % 60,
            second=0,
            microsecond=0
        )
        
        # Adjustiere Stunden wenn nötig
        if candles_since_midnight * timeframe_minutes >= 60:
            end_time = end_time.replace(
                hour=candles_since_midnight * timeframe_minutes // 60
            )
        
        start_time = end_time - timedelta(minutes=timeframe_minutes)
        
        logger.info(
            f"[{request_id}] Analyzing candle: {start_time} - {end_time}"
        )
        
        # Führe Analyse durch
        result = await analyzer.analyze_candle(
            exchange=request.exchange,
            symbol=request.symbol,
            timeframe=request.timeframe,
            start_time=start_time,
            end_time=end_time,
            top_n_wallets=request.top_n_wallets,
            include_trades=False
        )
        
        # Konvertiere zu Response-Format
        response = AnalysisResponse(
            candle=CandleData(**result['candle']),
            top_movers=[WalletMover(**m) for m in result['top_movers']],
            analysis_metadata=AnalysisMetadata(**result['analysis_metadata'])
        )
        
        logger.info(
            f"[{request_id}] Quick analysis completed: "
            f"{len(result['top_movers'])} top movers found"
        )
        
        return response
        
    except Exception as e:
        logger.error(
            f"[{request_id}] Quick analysis error: {e}",
            exc_info=True
        )
        raise HTTPException(
            status_code=500,
            detail=f"Quick analysis failed: {str(e)}"
        )


@router.get(
    "/status",
    summary="Get Analysis Service Status",
    description="Prüft den Status des Analyse-Services"
)
async def get_analysis_status():
    """
    ## Service Status
    
    Gibt den aktuellen Status des Analyse-Services zurück.
    
    ### Returns:
    - Service Status
    - Verfügbare Exchanges
    - System-Informationen
    """
    try:
        from app.core.price_movers.utils.constants import SUPPORTED_EXCHANGES
        
        return {
            "success": True,
            "status": "operational",
            "service": "Quick Analysis",
            "version": "1.0.0",
            "supported_exchanges": list(SUPPORTED_EXCHANGES),
            "supported_timeframes": ["1m", "5m", "15m", "30m", "1h", "4h", "1d"],
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Status check error: {e}")
        return {
            "success": False,
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


# Export Router
__all__ = ['router']
