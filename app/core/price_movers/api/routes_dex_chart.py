"""
DEX Chart Routes - Fixed Import Version

✅ All imports corrected - no models.chart dependency
"""

import os
import time
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Query, Depends, HTTPException, status
from pydantic import BaseModel, Field

# ✅ CORRECTED: Import from existing schemas
from app.core.price_movers.api.test_schemas import (
    TimeframeEnum,
    CandleData,
)
from app.core.price_movers.api.dependencies import (
    get_unified_collector,
    log_request,
)

# ✅ ADDED: Import validator if it exists
try:
    from app.core.price_movers.utils.validators import validate_dex_params
except ImportError:
    # Fallback validator if module doesn't exist
    def validate_dex_params(dex_exchange: str, symbol: str, timeframe) -> None:
        """Simple validation fallback"""
        if not dex_exchange or not symbol:
            raise ValueError("DEX exchange and symbol are required")


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dex", tags=["DEX Charts"])


# ==================== Additional Models for DEX ====================

class ChartCandleWithImpact(CandleData):
    """Extended candle with DEX-specific impact data"""
    has_high_impact: bool = Field(default=False, description="High impact trades detected")
    total_impact_score: float = Field(default=0.0, description="Total impact score")
    top_mover_count: int = Field(default=0, description="Number of top movers")
    is_synthetic: bool = Field(default=False, description="Is synthetic/mock data")


class DEXChartCandlesResponse(BaseModel):
    """DEX Chart response"""
    symbol: str = Field(..., description="Trading pair")
    dex_exchange: str = Field(..., description="DEX exchange")
    blockchain: str = Field(..., description="Blockchain network")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    candles: List[ChartCandleWithImpact] = Field(..., description="Candle data")
    total_candles: int = Field(..., description="Number of candles")
    data_source: str = Field(..., description="Data source (birdeye/helius/mock)")
    warning: Optional[str] = Field(None, description="Warning message if any")
    performance_ms: float = Field(..., description="Performance in milliseconds")


# ==================== Routes ====================

@router.get(
    "/candles",
    response_model=DEXChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Chart Candles (with Fallback)",
    description="Tries Birdeye OHLCV first, falls back to Helius aggregation"
)
async def get_dex_chart_candles(
    dex_exchange: str = Query(..., description="DEX (jupiter/raydium/orca)"),
    symbol: str = Query(..., description="Token pair (e.g., SOL/USDC)"),
    timeframe: TimeframeEnum = Query(..., description="Candle timeframe"),
    start_time: datetime = Query(..., description="Start time"),
    end_time: datetime = Query(..., description="End time"),
    include_impact: bool = Query(default=False, description="Calculate impact"),
    request_id: str = Depends(log_request)
) -> DEXChartCandlesResponse:
    """
    ## 🚀 DEX Chart with Intelligent Fallback
    
    **Strategy:**
    1. Try Birdeye OHLCV (fastest - if Starter plan available)
    2. Fall back to Helius (slower but works with free tier)
    3. Mock data as last resort
    """
    start_perf = time.time()
    
    try:
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] 🚀 DEX Chart: {dex_exchange} {symbol} {timeframe.value} "
            f"({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})"
        )
        
        unified_collector = await get_unified_collector()
        
        # Parse symbol
        base_token, quote_token = symbol.split('/')
        
        # For SOL pairs, use quote token
        if base_token.upper() == 'SOL':
            token_for_chart = quote_token.upper()
            logger.info(f"📊 Using {token_for_chart} for chart (not SOL)")
        else:
            token_for_chart = base_token.upper()
        
        candles_data = []
        data_source = "unknown"
        warning = None
        
        # ==================== STRATEGY 1: Try Dexscreener (FREE!) ====================
        
        if unified_collector.dexscreener_collector:
            try:
                logger.info("🎯 Trying Dexscreener OHLCV (FREE!)...")
                
                # Dexscreener doesn't have batch endpoint, so we need to iterate
                timeframe_seconds = {
                    '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                    '1h': 3600, '4h': 14400, '1d': 86400,
                }.get(str(timeframe.value), 300)
                
                current_time = start_time
                dexscreener_candles = []
                
                while current_time < end_time and len(dexscreener_candles) < 100:
                    try:
                        candle = await unified_collector.dexscreener_collector.fetch_candle_data(
                            symbol=symbol,
                            timeframe=str(timeframe.value),
                            timestamp=current_time
                        )
                        
                        # Check if valid data
                        if candle and candle.get('open', 0) > 0:
                            dexscreener_candles.append(candle)
                        
                        current_time = current_time + timedelta(seconds=timeframe_seconds)
                        
                        # Rate limit: small delay
                        if len(dexscreener_candles) % 5 == 0:
                            await asyncio.sleep(0.1)
                            
                    except Exception as e:
                        logger.debug(f"Dexscreener single candle failed: {e}")
                        current_time = current_time + timedelta(seconds=timeframe_seconds)
                        continue
                
                if dexscreener_candles:
                    candles_data = dexscreener_candles
                    data_source = "dexscreener"
                    logger.info(f"✅ Dexscreener: {len(candles_data)} candles (FREE!)")
                    
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"⚠️ Dexscreener failed: {e}")
        
        # ==================== STRATEGY 2: Try Birdeye ====================
        
        if unified_collector.birdeye_collector:
            try:
                logger.info("🎯 Trying Birdeye OHLCV...")
                
                # Resolve token
                token_address = await unified_collector.birdeye_collector._resolve_symbol_to_address(
                    f"{token_for_chart}/USDC"
                )
                
                if token_address:
                    logger.info(f"🔍 Token: {token_address[:8]}...")
                    
                    # Fetch OHLCV
                    candles_data = await unified_collector.birdeye_collector.fetch_ohlcv_batch(
                        token_address=token_address,
                        timeframe=str(timeframe.value),
                        start_time=start_time,
                        end_time=end_time,
                        limit=100
                    )
                    
                    if candles_data:
                        data_source = "birdeye"
                        logger.info(f"✅ Birdeye: {len(candles_data)} candles")
                        
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"⚠️ Birdeye failed: {e}")
                
                if any(x in error_str for x in ["401", "403", "suspended", "permission"]):
                    warning = (
                        "⚠️ Birdeye OHLCV requires Starter plan ($99/mo). "
                        "Using Helius fallback. "
                        "Upgrade at https://bds.birdeye.so/pricing"
                    )
                    logger.info("💡 Birdeye needs paid plan - using Helius")
        
        # ==================== STRATEGY 2: Helius Fallback ====================
        
        if not candles_data and unified_collector.helius_collector:
            try:
                logger.info("🔄 Helius fallback (free tier compatible)...")
                
                timeframe_seconds = {
                    '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                    '1h': 3600, '4h': 14400, '1d': 86400,
                }.get(str(timeframe.value), 300)
                
                total_seconds = (end_time - start_time).total_seconds()
                num_candles = min(int(total_seconds / timeframe_seconds), 100)
                
                logger.info(f"📊 Aggregating {num_candles} candles...")
                
                candles_data = []
                current_time = start_time
                failed_candles = 0
                
                for i in range(num_candles):
                    candle_end = current_time + timedelta(seconds=timeframe_seconds)
                    
                    try:
                        trades_result = await unified_collector.helius_collector.fetch_dex_trades(
                            symbol=symbol,
                            start_time=current_time,
                            end_time=candle_end,
                            limit=100
                        )
                        
                        trades = trades_result if isinstance(trades_result, list) else []
                        
                        if trades:
                            prices = [t.get('price', 0) for t in trades if t.get('price')]
                            volumes = [t.get('value_usd', 0) for t in trades if t.get('value_usd')]
                            
                            if prices:
                                candle = {
                                    'timestamp': current_time,
                                    'open': prices[0],
                                    'high': max(prices),
                                    'low': min(prices),
                                    'close': prices[-1],
                                    'volume': sum(volumes) if volumes else 0,
                                }
                                candles_data.append(candle)
                            else:
                                failed_candles += 1
                        else:
                            failed_candles += 1
                            
                    except Exception:
                        failed_candles += 1
                    
                    current_time = candle_end
                    
                    if i % 5 == 0 and i > 0:
                        await asyncio.sleep(0.1)
                
                if candles_data:
                    data_source = "helius"
                    logger.info(f"✅ Helius: {len(candles_data)}/{num_candles} candles")
                    
                    if not warning:
                        warning = f"Using Helius data ({len(candles_data)} candles)"
                        
            except Exception as e:
                logger.error(f"❌ Helius failed: {e}")
        
        # ==================== STRATEGY 3: Mock Data ====================
        
        if not candles_data:
            logger.warning("⚠️ Using mock data")
            
            timeframe_seconds = {
                '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                '1h': 3600, '4h': 14400, '1d': 86400,
            }.get(str(timeframe.value), 300)
            
            base_price = 100.0
            current_time = start_time
            num_candles = min(100, int((end_time - start_time).total_seconds() / timeframe_seconds))
            
            candles_data = []
            for i in range(num_candles):
                variation = (i % 10 - 5) * 0.01
                
                candle = {
                    'timestamp': current_time,
                    'open': base_price + variation,
                    'high': base_price + variation + 0.5,
                    'low': base_price + variation - 0.5,
                    'close': base_price + variation + 0.2,
                    'volume': 1000.0 + (i * 10),
                }
                candles_data.append(candle)
                current_time = current_time + timedelta(seconds=timeframe_seconds)
            
            data_source = "mock"
            warning = "⚠️ MOCK DATA: Check API keys"
        
        # ==================== Build Response ====================
        
        chart_candles = []
        for candle in candles_data:
            # Calculate price_change_pct
            price_change_pct = 0.0
            if candle.get('open') and candle['open'] > 0:
                price_change_pct = ((candle.get('close', 0) - candle['open']) / candle['open']) * 100
            
            chart_candle = ChartCandleWithImpact(
                timestamp=candle['timestamp'],
                open=float(candle['open']),
                high=float(candle['high']),
                low=float(candle['low']),
                close=float(candle['close']),
                volume=float(candle.get('volume', 0)),
                price_change_pct=price_change_pct,
                has_high_impact=False,
                total_impact_score=0.0,
                top_mover_count=0,
                is_synthetic=(data_source == "mock")
            )
            chart_candles.append(chart_candle)
        
        # Get blockchain
        from app.core.price_movers.utils.constants import DEX_CONFIGS
        dex_config = DEX_CONFIGS.get(dex_exchange.lower(), {})
        blockchain = dex_config.get('blockchain', 'solana')
        
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
            f"[{request_id}] ✅ {len(chart_candles)} candles from {data_source} "
            f"in {performance_ms:.0f}ms"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] ❌ Error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load DEX chart: {str(e)}"
        )


@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="DEX Health Check"
)
async def dex_health_check():
    """Check DEX data sources"""
    try:
        unified_collector = await get_unified_collector()
        
        health = {
            "birdeye": {
                "available": unified_collector.birdeye_collector is not None,
                "note": "OHLCV requires Starter plan ($99/mo)"
            },
            "helius": {
                "available": unified_collector.helius_collector is not None,
                "note": "Free tier compatible"
            },
            "recommendation": None
        }
        
        # Test Birdeye
        if unified_collector.birdeye_collector:
            try:
                is_healthy = await unified_collector.birdeye_collector.health_check()
                health["birdeye"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["birdeye"]["status"] = f"error: {str(e)[:50]}"
        
        # Test Helius
        if unified_collector.helius_collector:
            try:
                is_healthy = await unified_collector.helius_collector.health_check()
                health["helius"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["helius"]["status"] = f"error: {str(e)[:50]}"
        
        # Recommendation
        if health.get("birdeye", {}).get("status") == "healthy":
            health["recommendation"] = "Using Birdeye (fastest)"
        elif health.get("helius", {}).get("status") == "healthy":
            health["recommendation"] = "Using Helius fallback"
        else:
            health["recommendation"] = "⚠️ No healthy sources"
        
        return health
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )


# ==================== Helper Functions ====================

async def fetch_candle_with_fallback(
    unified_collector,
    dex_exchange: str,
    symbol: str,
    timeframe: TimeframeEnum,
    timestamp: datetime
) -> tuple[Optional[Dict], str]:
    """
    Fetch candle data with Dexscreener -> Birdeye -> Helius fallback
    
    Returns:
        (candle_data, source) - candle dict and source name
    """
    # Parse symbol
    base_token, quote_token = symbol.split('/')
    if base_token.upper() == 'SOL':
        token_for_chart = quote_token.upper()
    else:
        token_for_chart = base_token.upper()
    
    # Try Dexscreener first (FREE!)
    if unified_collector.dexscreener_collector:
        try:
            logger.debug("🎯 Trying Dexscreener for candle (FREE!)...")
            candle = await unified_collector.dexscreener_collector.fetch_candle_data(
                symbol=symbol,
                timeframe=str(timeframe.value),
                timestamp=timestamp
            )
            
            # Check if we got valid data (not empty candle)
            if candle and candle.get('open', 0) > 0:
                open_price = float(candle.get('open', 0))
                close_price = float(candle.get('close', 0))
                price_change_pct = ((close_price - open_price) / open_price * 100) if open_price > 0 else 0.0
                
                return {
                    'timestamp': candle.get('timestamp', timestamp),
                    'open': open_price,
                    'high': float(candle.get('high', 0)),
                    'low': float(candle.get('low', 0)),
                    'close': close_price,
                    'volume': float(candle.get('volume', 0)),
                    'price_change_pct': price_change_pct
                }, "dexscreener"
            else:
                logger.debug("Dexscreener returned empty candle, trying next source...")
        except Exception as e:
            logger.warning(f"Dexscreener candle fetch failed: {e}")
    
    # Try Birdeye second
    if unified_collector.birdeye_collector:
        try:
            logger.debug("Trying Birdeye for candle...")
            token_address = await unified_collector.birdeye_collector._resolve_symbol_to_address(
                f"{token_for_chart}/USDC"
            )
            
            if token_address:
                candles = await unified_collector.birdeye_collector.fetch_ohlcv_batch(
                    token_address=token_address,
                    timeframe=str(timeframe.value),
                    start_time=timestamp,
                    end_time=timestamp + timedelta(minutes=5),
                    limit=1
                )
                
                if candles and len(candles) > 0:
                    candle = candles[0]
                    return {
                        'timestamp': candle.get('timestamp', timestamp),
                        'open': float(candle.get('open', 0)),
                        'high': float(candle.get('high', 0)),
                        'low': float(candle.get('low', 0)),
                        'close': float(candle.get('close', 0)),
                        'volume': float(candle.get('volume', 0)),
                        'price_change_pct': 0.0
                    }, "birdeye"
        except Exception as e:
            logger.warning(f"Birdeye candle fetch failed: {e}")
    
    # Fallback to Helius
    if unified_collector.helius_collector:
        try:
            logger.debug("Falling back to Helius for candle...")
            
            timeframe_seconds = {
                '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                '1h': 3600, '4h': 14400, '1d': 86400,
            }.get(str(timeframe.value), 300)
            
            end_time = timestamp + timedelta(seconds=timeframe_seconds)
            
            trades_result = await unified_collector.helius_collector.fetch_dex_trades(
                symbol=symbol,
                start_time=timestamp,
                end_time=end_time,
                limit=100
            )
            
            trades = trades_result if isinstance(trades_result, list) else []
            
            if trades:
                prices = [t.get('price', 0) for t in trades if t.get('price')]
                volumes = [t.get('value_usd', 0) for t in trades if t.get('value_usd')]
                
                if prices:
                    open_price = prices[0]
                    close_price = prices[-1]
                    price_change_pct = ((close_price - open_price) / open_price * 100) if open_price > 0 else 0.0
                    
                    return {
                        'timestamp': timestamp,
                        'open': open_price,
                        'high': max(prices),
                        'low': min(prices),
                        'close': close_price,
                        'volume': sum(volumes) if volumes else 0,
                        'price_change_pct': price_change_pct
                    }, "helius"
        except Exception as e:
            logger.error(f"Helius candle fetch failed: {e}")
    
    return None, "none"


# ==================== Additional Models for Wallet Movers ====================

class DEXCandleMoversResponse(BaseModel):
    """Response for DEX candle wallet movers"""
    candle: CandleData
    top_movers: List[Dict] = Field(..., description="Top wallet movers with real addresses")
    analysis_metadata: Dict[str, Any]
    is_synthetic: bool = False
    has_real_wallet_ids: bool = True
    blockchain: str
    dex_exchange: str


# ==================== Wallet Movers Endpoint ====================

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
    
    ### Path Parameters:
    - **candle_timestamp**: Candle timestamp (ISO 8601)
    
    ### Query Parameters:
    - **dex_exchange**: DEX name (jupiter/raydium/orca)
    - **symbol**: Trading pair (e.g., SOL/USDC)
    - **timeframe**: Candle timeframe
    - **top_n_wallets**: Number of top wallets to return
    
    ### Example:
    ```
    GET /dex/candle/2025-11-18T10:00:00Z/movers
        ?dex_exchange=jupiter
        &symbol=SOL/USDC
        &timeframe=5m
        &top_n_wallets=10
    ```
    """
    start_perf = time.time()
    
    try:
        # Validate parameters
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] DEX Candle movers: {dex_exchange} {symbol} "
            f"@ {candle_timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
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
        }.get(str(timeframe.value), 5)

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

        # Import Candle class from analyzer_hybrid
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

        performance_ms = (time.time() - start_perf) * 1000

        response = DEXCandleMoversResponse(
            candle=CandleData(**candle_data),
            top_movers=dex_movers,
            analysis_metadata={
                "analysis_timestamp": datetime.now(timezone.utc),
                "processing_duration_ms": int(performance_ms),
                "total_trades_analyzed": len(trades_result.get('trades', [])),
                "unique_wallets_found": len(dex_movers),
                "exchange": dex_exchange,
                "symbol": symbol,
                "timeframe": str(timeframe.value),
                "data_source": source
            },
            is_synthetic=False,
            has_real_wallet_ids=True,
            blockchain=blockchain.value if hasattr(blockchain, 'value') else str(blockchain),
            dex_exchange=dex_exchange
        )
        
        logger.info(
            f"[{request_id}] ✅ DEX movers loaded: {len(dex_movers)} wallets "
            f"from {len(trades_result.get('trades', []))} trades "
            f"(source: {source}, {performance_ms:.0f}ms)"
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] ❌ DEX movers error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load DEX movers: {str(e)}"
        )
