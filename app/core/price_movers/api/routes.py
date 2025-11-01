"""
FastAPI Routes für Price Movers API

Endpoints:
- POST /api/v1/analyze/price-movers
- POST /api/v1/analyze/quick
- POST /api/v1/analyze/historical
- GET  /api/v1/wallet/{wallet_id}
- POST /api/v1/compare-exchanges
- GET  /api/v1/health
"""

import logging
from typing import Dict, List
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, status

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.core.price_movers.api.test_schemas import (
    AnalysisRequest,
    AnalysisResponse,
    QuickAnalysisRequest,
    HistoricalAnalysisRequest,
    HistoricalAnalysisResponse,
    WalletLookupRequest,
    WalletDetailResponse,
    CompareExchangesRequest,
    ExchangeComparison,
    HealthCheckResponse,
    ErrorResponse,
    SuccessResponse,
)
from app.core.price_movers.api.dependencies import (
    get_exchange_collector,
    get_all_exchange_collectors,
    get_analyzer,
    verify_api_key,
    check_rate_limit,
    log_request,
)
from app.core.price_movers.collectors import ExchangeCollector
from app.core.price_movers.services import PriceMoverAnalyzer


logger = logging.getLogger(__name__)

# Router erstellen
router = APIRouter(
    prefix="/api/v1",
    tags=["price-movers"],
    responses={
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
        503: {"model": ErrorResponse, "description": "Service Unavailable"},
    }
)


# ==================== MAIN ANALYSIS ENDPOINT ====================

@router.post(
    "/analyze/price-movers",
    response_model=AnalysisResponse,
    status_code=status.HTTP_200_OK,
    summary="Analyze Price Movers",
    description="Identifiziert Wallets mit dem größten Einfluss auf Preisbewegungen",
    responses={
        400: {"model": ErrorResponse, "description": "Invalid Request"},
        503: {"model": ErrorResponse, "description": "Exchange Unavailable"},
    }
)
async def analyze_price_movers(
    request: AnalysisRequest,
    analyzer: PriceMoverAnalyzer = Depends(get_analyzer),
    request_id: str = Depends(log_request),
    rate_limit_ok: bool = Depends(check_rate_limit),
    api_key: str = Depends(verify_api_key)
) -> AnalysisResponse:
    """
    ## Hauptanalyse für Price Movers
    
    Analysiert eine Candle und identifiziert die Top Wallets/Pattern mit dem
    größten Einfluss auf Preisbewegungen.
    
    ### Parameter:
    - **exchange**: Exchange (bitget, binance, kraken)
    - **symbol**: Trading Pair (z.B. BTC/USDT)
    - **timeframe**: Candle Timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d)
    - **start_time**: Start-Zeitpunkt (ISO 8601)
    - **end_time**: End-Zeitpunkt (ISO 8601)
    - **min_impact_threshold**: Minimaler Impact Score (0-1)
    - **top_n_wallets**: Anzahl Top Wallets (1-100)
    - **include_trades**: Einzelne Trades inkludieren
    
    ### Returns:
    - Candle-Daten (OHLCV)
    - Top Movers mit Impact Scores
    - Analyse-Metadaten
    
    ### Beispiel:
    ```json
    {
      "exchange": "binance",
      "symbol": "BTC/USDT",
      "timeframe": "5m",
      "start_time": "2024-10-27T10:00:00Z",
      "end_time": "2024-10-27T10:05:00Z",
      "min_impact_threshold": 0.1,
      "top_n_wallets": 10
    }
    ```
    """
    try:
        logger.info(
            f"[{request_id}] Analysis request: {request.exchange} "
            f"{request.symbol} {request.timeframe}"
        )
        
        # Führe Analyse aus
        result = await analyzer.analyze_candle(
            exchange=request.exchange,
            symbol=request.symbol,
            timeframe=request.timeframe,
            start_time=request.start_time,
            end_time=request.end_time,
            min_impact_threshold=request.min_impact_threshold,
            top_n_wallets=request.top_n_wallets,
            include_trades=request.include_trades
        )
        
        logger.info(
            f"[{request_id}] Analysis complete: "
            f"{len(result.get('top_movers', []))} movers found"
        )
        
        # Konvertiere zu Response Model
        return AnalysisResponse(**result)
        
    except ValueError as e:
        logger.error(f"[{request_id}] Validation error: {e}")
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"[{request_id}] Analysis error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )


# ==================== QUICK ANALYSIS ====================

@router.post(
    "/analyze/quick",
    response_model=AnalysisResponse,
    status_code=status.HTTP_200_OK,
    summary="Quick Analysis",
    description="Schnellanalyse der aktuellen/letzten Candle"
)
async def quick_analysis(
    request: QuickAnalysisRequest,
    request_id: str = Depends(log_request)
) -> AnalysisResponse:
    """
    ## Schnellanalyse
    
    Analysiert die aktuelle oder letzte abgeschlossene Candle.
    """
    try:
        logger.info(
            f"[{request_id}] Quick analysis: {request.exchange} "
            f"{request.symbol} {request.timeframe}"
        )
        
        # Hole Analyzer OHNE get_analyzer zu callen, erstelle direkt
        from app.core.price_movers.api.dependencies import get_exchange_collector
        
        collector = await get_exchange_collector(request.exchange)
        analyzer = PriceMoverAnalyzer(exchange_collector=collector)
        
        # Berechne Zeitfenster für letzte Candle
        now = datetime.now()
        
        # Timeframe zu Minuten
        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(request.timeframe, 5)
        
        end_time = now
        start_time = now - timedelta(minutes=timeframe_minutes)
        
        # Führe Analyse aus
        result = await analyzer.analyze_candle(
            exchange=request.exchange,
            symbol=request.symbol,
            timeframe=request.timeframe,
            start_time=start_time,
            end_time=end_time,
            top_n_wallets=request.top_n_wallets,
            include_trades=False
        )
        
        return AnalysisResponse(**result)
        
    except Exception as e:
        logger.error(f"[{request_id}] Quick analysis error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Quick analysis failed: {str(e)}"
        )


# ==================== HISTORICAL ANALYSIS ====================

@router.post(
    "/analyze/historical",
    response_model=HistoricalAnalysisResponse,
    status_code=status.HTTP_200_OK,
    summary="Historical Analysis",
    description="Analyse über mehrere Candles hinweg"
)
async def historical_analysis(
    request: HistoricalAnalysisRequest,
    analyzer: PriceMoverAnalyzer = Depends(get_analyzer),
    request_id: str = Depends(log_request)
) -> HistoricalAnalysisResponse:
    """
    ## Historische Analyse
    
    Analysiert mehrere Candles über einen Zeitraum und aggregiert die Top Movers.
    
    ### Parameter:
    - **exchange**: Exchange
    - **symbol**: Trading Pair
    - **timeframe**: Candle Timeframe
    - **start_time**: Start-Zeitpunkt
    - **end_time**: End-Zeitpunkt
    - **min_impact_threshold**: Minimaler Impact Score
    
    ### Returns:
    - Aggregierte Top Movers über den Zeitraum
    - Summary mit Statistiken
    """
    try:
        logger.info(
            f"[{request_id}] Historical analysis: {request.exchange} "
            f"{request.symbol} ({request.start_time} - {request.end_time})"
        )
        
        # TODO: Implementiere echte historische Analyse
        # Placeholder Response
        
        response = HistoricalAnalysisResponse(
            symbol=request.symbol,
            exchange=request.exchange,
            timeframe=request.timeframe,
            start_time=request.start_time,
            end_time=request.end_time,
            candles_analyzed=0,
            top_movers=[],
            summary={
                "total_volume": 0.0,
                "total_trades": 0,
                "unique_wallets": 0,
                "avg_impact_score": 0.0
            }
        )
        
        logger.warning(
            f"[{request_id}] Historical analysis not fully implemented yet"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Historical analysis error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Historical analysis failed: {str(e)}"
        )


# ==================== WALLET LOOKUP ====================

@router.get(
    "/wallet/{wallet_id}",
    response_model=WalletDetailResponse,
    status_code=status.HTTP_200_OK,
    summary="Wallet Lookup",
    description="Detaillierte Informationen zu einem Wallet"
)
async def wallet_lookup(
    wallet_id: str,
    exchange: str,
    symbol: str = None,
    time_range_hours: int = 24,
    request_id: str = Depends(log_request)
) -> WalletDetailResponse:
    """
    ## Wallet-Detail Lookup
    
    Gibt detaillierte Informationen zu einem spezifischen Wallet.
    
    ### Path Parameter:
    - **wallet_id**: Wallet Identifier (z.B. whale_0x742d35)
    
    ### Query Parameter:
    - **exchange**: Exchange (required)
    - **symbol**: Trading Pair (optional)
    - **time_range_hours**: Zeitraum in Stunden (default: 24)
    
    ### Returns:
    - Wallet-Details
    - Trading-Historie
    - Statistiken
    """
    try:
        logger.info(
            f"[{request_id}] Wallet lookup: {wallet_id} on {exchange}"
        )
        
        # TODO: Implementiere echtes Wallet Lookup
        # Placeholder Response
        
        response = WalletDetailResponse(
            wallet_id=wallet_id,
            wallet_type="unknown",
            first_seen=datetime.now() - timedelta(days=7),
            last_seen=datetime.now(),
            total_trades=0,
            total_volume=0.0,
            total_value_usd=0.0,
            avg_impact_score=0.0,
            recent_trades=[],
            statistics={}
        )
        
        logger.warning(
            f"[{request_id}] Wallet lookup not fully implemented yet"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Wallet lookup error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Wallet lookup failed: {str(e)}"
        )


# ==================== EXCHANGE COMPARISON ====================

@router.post(
    "/compare-exchanges",
    response_model=ExchangeComparison,
    status_code=status.HTTP_200_OK,
    summary="Compare Exchanges",
    description="Vergleicht Preise und Volume über mehrere Exchanges"
)
async def compare_exchanges(
    request: CompareExchangesRequest,
    collectors: Dict[str, ExchangeCollector] = Depends(get_all_exchange_collectors),
    request_id: str = Depends(log_request)
) -> ExchangeComparison:
    """
    ## Exchange-Vergleich
    
    Vergleicht aktuelle Preise und Volumes über mehrere Exchanges.
    
    ### Parameter:
    - **exchanges**: Liste von Exchanges (min. 1)
    - **symbol**: Trading Pair
    - **timeframe**: Candle Timeframe (default: 5m)
    
    ### Returns:
    - Exchange-spezifische Daten (Preis, Volume, Spread)
    - Best Price
    - Highest Volume
    """
    try:
        logger.info(
            f"[{request_id}] Compare exchanges: "
            f"{', '.join(request.exchanges)} for {request.symbol}"
        )
        
        exchange_data = {}
        
        # Fetch Daten von allen Exchanges
        for exchange_name in request.exchanges:
            try:
                collector = collectors.get(exchange_name)
                if not collector:
                    continue
                
                # Fetch Ticker
                ticker = await collector.fetch_ticker(request.symbol)
                
                exchange_data[exchange_name] = {
                    "price": ticker['last'],
                    "volume": ticker['volume'] or 0.0,
                    "bid": ticker['bid'],
                    "ask": ticker['ask'],
                    "spread": (ticker['ask'] - ticker['bid']) if ticker['ask'] and ticker['bid'] else 0.0
                }
                
            except Exception as e:
                logger.error(
                    f"[{request_id}] Failed to fetch from {exchange_name}: {e}"
                )
                exchange_data[exchange_name] = {
                    "error": str(e)
                }
        
        # Finde Best Price und Highest Volume
        valid_exchanges = {
            k: v for k, v in exchange_data.items() 
            if "error" not in v
        }
        
        best_price = None
        highest_volume = None
        
        if valid_exchanges:
            best_price_exchange = min(
                valid_exchanges.items(),
                key=lambda x: x[1]['price']
            )
            best_price = {
                "exchange": best_price_exchange[0],
                "price": best_price_exchange[1]['price']
            }
            
            highest_volume_exchange = max(
                valid_exchanges.items(),
                key=lambda x: x[1]['volume']
            )
            highest_volume = {
                "exchange": highest_volume_exchange[0],
                "volume": highest_volume_exchange[1]['volume']
            }
        
        response = ExchangeComparison(
            symbol=request.symbol,
            timeframe=request.timeframe,
            timestamp=datetime.now(),
            exchanges=exchange_data,
            best_price=best_price or {},
            highest_volume=highest_volume or {}
        )
        
        logger.info(
            f"[{request_id}] Exchange comparison complete: "
            f"{len(valid_exchanges)}/{len(request.exchanges)} successful"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Exchange comparison error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Exchange comparison failed: {str(e)}"
        )


# ==================== HEALTH CHECK ====================

@router.get(
    "/health",
    response_model=HealthCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Health Check",
    description="Prüft den Status aller Exchanges und des Systems"
)
async def health_check(
    collectors: Dict[str, ExchangeCollector] = Depends(get_all_exchange_collectors)
) -> HealthCheckResponse:
    """
    ## Health Check
    
    Prüft die Erreichbarkeit aller Exchanges und gibt System-Status zurück.
    
    ### Returns:
    - Gesamt-Status (healthy/unhealthy)
    - Exchange-Status (pro Exchange)
    - API Version
    """
    logger.info("Health check requested")
    
    exchange_status = {}
    
    # Prüfe alle Exchanges
    for exchange_name, collector in collectors.items():
        try:
            is_healthy = await collector.health_check()
            exchange_status[exchange_name] = is_healthy
        except Exception as e:
            logger.error(f"Health check failed for {exchange_name}: {e}")
            exchange_status[exchange_name] = False
    
    # Gesamt-Status
    all_healthy = all(exchange_status.values())
    overall_status = "healthy" if all_healthy else "degraded"
    
    response = HealthCheckResponse(
        status=overall_status,
        timestamp=datetime.now(),
        exchanges=exchange_status,
        version="0.1.0"
    )
    
    logger.info(
        f"Health check complete: {overall_status} "
        f"({sum(exchange_status.values())}/{len(exchange_status)} exchanges healthy)"
    )
    
    return response


# ==================== ROOT ENDPOINT ====================

@router.get(
    "/",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="API Root",
    description="API Information"
)
async def root() -> SuccessResponse:
    """
    ## API Root
    
    Gibt Informationen über die API zurück.
    """
    return SuccessResponse(
        message="Price Movers API",
        data={
            "version": "0.1.0",
            "status": "operational",
            "documentation": "/docs",
            "supported_exchanges": ["bitget", "binance", "kraken"]
        }
    )
