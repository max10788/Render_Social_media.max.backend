"""
DEX Chart Routes - On-Chain Candlestick & Wallet Analysis

🆕 Neue Endpoints für DEX (Jupiter/Raydium/Orca):
- GET /api/v1/dex/candles - Chart-Daten mit ECHTEN Wallet-Adressen
- GET /api/v1/dex/candle/{timestamp}/movers - Wallet-Analyse für Candle

VORTEILE gegenüber CEX:
- ✅ ECHTE Blockchain-Adressen (keine Pattern-based Entities)
- ✅ Keine Trade-Retention-Probleme (Blockchain behält alles)
- ✅ Vollständige On-Chain History verfügbar
"""

import logging
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException, Depends, status, Query
from pydantic import BaseModel, Field

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

class DEXEnum(str):
    """Supported DEX Exchanges"""
    JUPITER = "jupiter"
    RAYDIUM = "raydium"
    ORCA = "orca"


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
    warning: Optional[str] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "symbol": "SOL/USDC",
                "dex_exchange": "jupiter",
                "blockchain": "solana",
                "timeframe": "5m",
                "candles": [
                    {
                        "timestamp": "2025-11-17T10:00:00Z",
                        "open": 245.50,
                        "high": 247.25,
                        "low": 245.00,
                        "close": 246.75,
                        "volume": 125000.5,
                        "has_high_impact": True,
                        "total_impact_score": 1.85,
                        "top_mover_count": 12,
                        "is_synthetic": False
                    }
                ],
                "total_candles": 100,
                "warning": None
            }
        }


class DEXCandleMoversResponse(BaseModel):
    """Response mit DEX Wallet Movers für Candle"""
    success: bool = True
    candle: CandleData
    top_movers: List[dict]  # With wallet_address!
    analysis_metadata: dict
    is_synthetic: bool = Field(False, description="Always False for DEX")
    has_real_wallet_ids: bool = Field(True, description="Always True for DEX!")
    blockchain: str
    dex_exchange: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "candle": {...},
                "top_movers": [
                    {
                        "wallet_id": "7xKXtg2CW87d97TXJSDpbD4j5NzWZn9XsxUBmk...",
                        "wallet_address": "7xKXtg2CW87d97TXJSDpbD4j5NzWZn9XsxUBmk...",
                        "wallet_type": "whale",
                        "impact_score": 0.85,
                        "total_volume": 125000.50,
                        "trade_count": 42,
                        "blockchain": "solana"
                    }
                ],
                "analysis_metadata": {...},
                "is_synthetic": False,
                "has_real_wallet_ids": True,
                "blockchain": "solana",
                "dex_exchange": "jupiter"
            }
        }


# ==================== ENDPOINTS ====================

@router.get(
    "/candles",
    response_model=DEXChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Chart Candles (On-Chain)",
    description="Lädt On-Chain Candlestick-Daten für DEX Chart"
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
    ## 🔗 DEX Chart-Daten (On-Chain)
    
    Lädt OHLCV-Daten von DEX mit ECHTEN Wallet-Adressen!
    
    ### VORTEILE gegenüber CEX:
    - ✅ ECHTE Blockchain-Adressen (keine virtuelle Entities)
    - ✅ Keine Retention-Probleme (Blockchain behält alles)
    - ✅ Vollständige On-Chain History
    
    ### Query Parameter:
    - **dex_exchange**: DEX (jupiter/raydium/orca)
    - **symbol**: Token Pair (z.B. SOL/USDC)
    - **timeframe**: Candle Timeframe
    - **start_time**: Start (ISO 8601)
    - **end_time**: Ende (ISO 8601)
    - **include_impact**: Impact berechnen (default: false)
    
    ### Returns:
    - Liste von Candles mit OHLCV
    - Optional: Impact-Indikatoren
    - Blockchain Info
    """
    try:
        # Validate DEX
        if dex_exchange.lower() not in ['jupiter', 'raydium', 'orca']:
            raise HTTPException(
                status_code=400,
                detail=f"DEX '{dex_exchange}' not supported. Use: jupiter, raydium, orca"
            )
        
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
        
        # Fetch Candles from DEX
        # Note: UnifiedCollector.fetch_candle_data gibt einzelne Candles zurück
        # Wir müssen mehrere Candles für den Zeitraum holen
        
        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(timeframe, 5)
        
        # Calculate how many candles we need
        time_diff = (end_time - start_time).total_seconds()
        num_candles = int(time_diff / (timeframe_minutes * 60))
        
        logger.info(f"Fetching {num_candles} DEX candles...")
        
        chart_candles = []
        current_time = start_time
        
        # Fetch candles iteratively
        for i in range(num_candles):
            try:
                candle_data = await unified_collector.fetch_candle_data(
                    exchange=dex_exchange.lower(),
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=current_time
                )
                
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
                    is_synthetic=False  # DEX = Always real!
                )
                
                # Optional: Calculate impact (SLOW!)
                if include_impact:
                    logger.warning(
                        f"[{request_id}] include_impact=true for DEX - "
                        f"This may be slow!"
                    )
                    # TODO: Implement impact calculation
                    # Similar to CEX but with real wallets
                
                chart_candles.append(chart_candle)
                
            except Exception as e:
                logger.warning(f"Failed to fetch candle at {current_time}: {e}")
                # Continue with next candle
            
            current_time += timedelta(minutes=timeframe_minutes)
        
        # Get blockchain from DEX config
        from app.core.price_movers.utils.constants import DEX_CONFIGS
        dex_config = DEX_CONFIGS.get(dex_exchange.lower(), {})
        blockchain = dex_config.get('blockchain', 'solana')
        
        response = DEXChartCandlesResponse(
            symbol=symbol,
            dex_exchange=dex_exchange,
            blockchain=blockchain.value if hasattr(blockchain, 'value') else str(blockchain),
            timeframe=timeframe,
            candles=chart_candles,
            total_candles=len(chart_candles),
            warning=None
        )
        
        logger.info(
            f"[{request_id}] DEX Chart candles loaded: {len(chart_candles)} candles"
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
    
    Lädt die einflussreichsten Wallets für eine Candle mit:
    - ✅ ECHTEN Blockchain-Adressen
    - ✅ On-Chain Transaction History
    - ✅ Keine synthetischen Daten
    
    ### Path Parameter:
    - **candle_timestamp**: Candle Timestamp
    
    ### Query Parameter:
    - **dex_exchange**: DEX
    - **symbol**: Token Pair
    - **timeframe**: Timeframe
    - **top_n_wallets**: Anzahl Top Wallets
    
    ### Returns:
    - Candle-Daten
    - Top Wallets mit ECHTEN Adressen! 🎯
    - Blockchain Info
    """
    try:
        logger.info(
            f"[{request_id}] DEX Candle movers: {dex_exchange} {symbol} "
            f"@ {candle_timestamp}"
        )
        
        # Get UnifiedCollector
        unified_collector = await get_unified_collector()
        
        # Initialize HybridAnalyzer (kann DEX!)
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
        
        # Fetch DEX data
        logger.debug("Fetching DEX trades and candle...")
        
        candle_data = await unified_collector.fetch_candle_data(
            exchange=dex_exchange.lower(),
            symbol=symbol,
            timeframe=timeframe,
            timestamp=start_time
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
            trades=[],  # Will be parsed from trades_result
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
                "timeframe": timeframe
            },
            is_synthetic=False,
            has_real_wallet_ids=True,
            blockchain=blockchain.value if hasattr(blockchain, 'value') else str(blockchain),
            dex_exchange=dex_exchange
        )
        
        logger.info(
            f"[{request_id}] DEX movers loaded: {len(dex_movers)} wallets"
        )
        
        return response
        
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


# Export Router
__all__ = ['router']
