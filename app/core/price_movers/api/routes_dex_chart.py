"""
DEX Chart Routes - COMPLETE VERSION with Helius Fallback

Birdeye OHLCV (requires Starter plan $99/month) → Falls back to Helius if unavailable
"""

import os
import time
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Query, Depends, HTTPException, status

from app.core.price_movers.models.chart import (
    ChartCandleWithImpact,
    DEXChartCandlesResponse,
)
from app.core.price_movers.models.enums import TimeframeEnum
from app.core.price_movers.api.dependencies import (
    get_unified_collector,
    log_request,
)
from app.core.price_movers.utils.validators import validate_dex_params


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dex", tags=["DEX Charts"])


@router.get(
    "/candles",
    response_model=DEXChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Chart Candles (with Fallback)",
    description="Tries Birdeye OHLCV first, falls back to Helius aggregation if unavailable"
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
    1. **Try Birdeye OHLCV** (fastest - if Starter plan available)
    2. **Fall back to Helius** (slower but works with free tier)
    3. **Mock data** as last resort
    
    **Birdeye Requirements:**
    - Requires Starter plan ($99/month) or higher
    - 401/403 error → Falls back to Helius automatically
    
    **Performance:**
    - Birdeye: ~500ms for 100 candles
    - Helius: ~3-5s for 100 candles
    - Mock: Instant (but not real data)
    """
    start_perf = time.time()
    
    try:
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] 🚀 OPTIMIZED DEX Chart: {dex_exchange} {symbol} {timeframe} "
            f"({start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%H:%M')})"
        )
        
        unified_collector = await get_unified_collector()
        
        # Parse symbol
        base_token, quote_token = symbol.split('/')
        
        # For SOL pairs, use quote token (USDC/USDT has trades, SOL doesn't)
        if base_token.upper() == 'SOL':
            token_for_chart = quote_token.upper()
            logger.info(f"📊 Using {token_for_chart} for chart (not SOL)")
        else:
            token_for_chart = base_token.upper()
        
        candles_data = []
        data_source = "unknown"
        warning = None
        
        # ==================== STRATEGY 1: Try Birdeye OHLCV ====================
        
        if unified_collector.birdeye_collector:
            try:
                logger.info("🎯 Attempting Birdeye OHLCV (requires Starter plan)...")
                
                # Resolve token address
                token_address = await unified_collector.birdeye_collector._resolve_symbol_to_address(
                    f"{token_for_chart}/USDC"
                )
                
                if not token_address:
                    raise ValueError(f"Could not resolve token address for {token_for_chart}")
                
                logger.info(f"🔍 Token address: {token_address[:8]}... ({token_for_chart})")
                
                # Fetch OHLCV from Birdeye
                candles_data = await unified_collector.birdeye_collector.fetch_ohlcv_batch(
                    token_address=token_address,
                    timeframe=str(timeframe.value),
                    start_time=start_time,
                    end_time=end_time,
                    limit=100
                )
                
                if candles_data:
                    data_source = "birdeye"
                    logger.info(f"✅ Birdeye: Got {len(candles_data)} candles")
                else:
                    logger.warning("⚠️ Birdeye returned empty data")
                    
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"⚠️ Birdeye failed: {e}")
                
                # Check if it's a permission/plan error
                if any(x in error_str for x in ["401", "403", "suspended", "permission", "plan", "upgrade"]):
                    warning = (
                        "⚠️ Birdeye OHLCV requires Starter plan ($99/mo). "
                        "Using slower Helius fallback. "
                        "Upgrade at https://bds.birdeye.so/pricing for faster charts."
                    )
                    logger.info("💡 Birdeye requires paid plan - falling back to Helius")
                else:
                    warning = f"Birdeye temporarily unavailable ({str(e)[:50]})"
        else:
            logger.info("ℹ️ Birdeye not configured - using Helius")
        
        # ==================== STRATEGY 2: Helius Fallback ====================
        
        if not candles_data and unified_collector.helius_collector:
            try:
                logger.info("🔄 Using Helius fallback (free tier compatible)...")
                
                # Calculate timeframe in seconds
                timeframe_seconds = {
                    '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                    '1h': 3600, '4h': 14400, '1d': 86400,
                }.get(str(timeframe.value), 300)
                
                # Calculate number of candles
                total_seconds = (end_time - start_time).total_seconds()
                num_candles = min(int(total_seconds / timeframe_seconds), 100)
                
                logger.info(f"📊 Aggregating {num_candles} candles from Helius trades...")
                
                # Aggregate candles from Helius
                candles_data = []
                current_time = start_time
                failed_candles = 0
                
                for i in range(num_candles):
                    candle_end = current_time + timedelta(seconds=timeframe_seconds)
                    
                    try:
                        # Fetch trades for this candle
                        trades_result = await unified_collector.helius_collector.fetch_dex_trades(
                            symbol=symbol,
                            start_time=current_time,
                            end_time=candle_end,
                            limit=100
                        )
                        
                        trades = trades_result if isinstance(trades_result, list) else []
                        
                        if trades:
                            # Aggregate into candle
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
                            
                    except Exception as candle_error:
                        logger.debug(f"Failed candle {i}: {candle_error}")
                        failed_candles += 1
                    
                    current_time = candle_end
                    
                    # Rate limit protection
                    if i % 5 == 0 and i > 0:
                        await asyncio.sleep(0.1)
                
                if candles_data:
                    data_source = "helius"
                    logger.info(
                        f"✅ Helius: Aggregated {len(candles_data)}/{num_candles} candles "
                        f"({failed_candles} gaps)"
                    )
                    
                    if not warning:
                        warning = (
                            f"Using Helius data ({len(candles_data)} candles, {failed_candles} gaps). "
                            "For gapless data, upgrade to Birdeye Starter plan."
                        )
                else:
                    logger.warning("⚠️ Helius returned no candles")
                    
            except Exception as e:
                logger.error(f"❌ Helius fallback failed: {e}", exc_info=True)
                if not warning:
                    warning = f"Both Birdeye and Helius failed: {str(e)[:100]}"
        
        # ==================== STRATEGY 3: Mock Data (Last Resort) ====================
        
        if not candles_data:
            logger.warning("⚠️ All strategies failed - using mock data")
            
            timeframe_seconds = {
                '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                '1h': 3600, '4h': 14400, '1d': 86400,
            }.get(str(timeframe.value), 300)
            
            # Generate realistic-looking mock data
            base_price = 100.0
            current_time = start_time
            num_candles = min(100, int((end_time - start_time).total_seconds() / timeframe_seconds))
            
            candles_data = []
            for i in range(num_candles):
                # Add some variation
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
            warning = (
                "⚠️ MOCK DATA: Real data unavailable. "
                "Birdeye requires Starter plan ($99/mo), Helius API may be down. "
                "Check API keys and try again."
            )
        
        # ==================== Convert to Response Format ====================
        
        chart_candles = []
        for candle in candles_data:
            chart_candle = ChartCandleWithImpact(
                timestamp=candle['timestamp'],
                open=float(candle['open']),
                high=float(candle['high']),
                low=float(candle['low']),
                close=float(candle['close']),
                volume=float(candle.get('volume', 0)),
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
        
        # Performance
        performance_ms = (time.time() - start_perf) * 1000
        
        # Build response
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
            f"[{request_id}] ✅ DEX Chart: {len(chart_candles)} candles from {data_source} "
            f"in {performance_ms:.0f}ms"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] ❌ DEX chart error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load DEX chart: {str(e)}"
        )


@router.get(
    "/candle/{candle_timestamp}/movers",
    status_code=status.HTTP_200_OK,
    summary="Get Wallet Analysis for Candle",
    description="Analyze top wallet movers for a specific candle (Helius only)"
)
async def get_dex_candle_movers(
    candle_timestamp: datetime,
    dex_exchange: str = Query(..., description="DEX exchange"),
    symbol: str = Query(..., description="Trading pair"),
    timeframe: TimeframeEnum = Query(..., description="Timeframe"),
    min_volume: float = Query(default=1000.0, description="Min volume USD"),
    limit: int = Query(default=20, description="Max wallets to return"),
    request_id: str = Depends(log_request)
):
    """
    Get wallet activity analysis for a specific candle
    
    This endpoint uses Helius for real wallet addresses and trade data.
    """
    try:
        logger.info(
            f"[{request_id}] 🔍 DEX Candle Movers: {dex_exchange} {symbol} @ "
            f"{candle_timestamp.strftime('%Y-%m-%d %H:%M')}"
        )
        
        unified_collector = await get_unified_collector()
        
        if not unified_collector.helius_collector:
            raise HTTPException(
                status_code=503,
                detail="Helius not configured. Set HELIUS_API_KEY for wallet analysis."
            )
        
        # Calculate candle window
        timeframe_seconds = {
            '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '4h': 14400, '1d': 86400,
        }.get(str(timeframe.value), 300)
        
        start_time = candle_timestamp
        end_time = candle_timestamp + timedelta(seconds=timeframe_seconds)
        
        logger.info(f"📊 Fetching trades for window: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
        
        # Fetch trades
        trades_result = await unified_collector.helius_collector.fetch_dex_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=1000
        )
        
        trades = trades_result if isinstance(trades_result, list) else []
        
        if not trades:
            return {
                "candle_timestamp": candle_timestamp,
                "symbol": symbol,
                "dex_exchange": dex_exchange,
                "total_trades": 0,
                "top_movers": [],
                "warning": "No trades found in this candle window"
            }
        
        # Filter by volume
        high_volume_trades = [
            t for t in trades
            if t.get('value_usd', 0) >= min_volume
        ]
        
        logger.info(
            f"📈 Found {len(trades)} total trades, "
            f"{len(high_volume_trades)} above ${min_volume}"
        )
        
        # Aggregate by wallet
        wallet_stats = {}
        
        for trade in high_volume_trades:
            wallet = trade.get('wallet_address')
            if not wallet:
                continue
            
            if wallet not in wallet_stats:
                wallet_stats[wallet] = {
                    'wallet_address': wallet,
                    'total_volume_usd': 0,
                    'buy_volume_usd': 0,
                    'sell_volume_usd': 0,
                    'trade_count': 0,
                    'net_position': 0,
                }
            
            value = trade.get('value_usd', 0)
            trade_type = trade.get('trade_type', 'unknown')
            
            wallet_stats[wallet]['total_volume_usd'] += value
            wallet_stats[wallet]['trade_count'] += 1
            
            if trade_type == 'buy':
                wallet_stats[wallet]['buy_volume_usd'] += value
                wallet_stats[wallet]['net_position'] += trade.get('amount', 0)
            elif trade_type == 'sell':
                wallet_stats[wallet]['sell_volume_usd'] += value
                wallet_stats[wallet]['net_position'] -= trade.get('amount', 0)
        
        # Sort by total volume
        top_movers = sorted(
            wallet_stats.values(),
            key=lambda x: x['total_volume_usd'],
            reverse=True
        )[:limit]
        
        logger.info(f"✅ Top {len(top_movers)} wallet movers identified")
        
        return {
            "candle_timestamp": candle_timestamp,
            "symbol": symbol,
            "dex_exchange": dex_exchange,
            "timeframe": timeframe,
            "total_trades": len(trades),
            "high_volume_trades": len(high_volume_trades),
            "unique_wallets": len(wallet_stats),
            "top_movers": top_movers,
            "min_volume_filter": min_volume,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] ❌ DEX candle movers error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch candle movers: {str(e)}"
        )


@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="DEX Data Sources Health Check"
)
async def dex_health_check():
    """
    Check health of DEX data sources
    """
    try:
        unified_collector = await get_unified_collector()
        
        health = {
            "birdeye": {
                "available": unified_collector.birdeye_collector is not None,
                "status": "unknown",
                "note": "OHLCV requires Starter plan ($99/mo)"
            },
            "helius": {
                "available": unified_collector.helius_collector is not None,
                "status": "unknown",
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
        if health["birdeye"]["status"] == "healthy":
            health["recommendation"] = "Using Birdeye OHLCV (fastest)"
        elif health["helius"]["status"] == "healthy":
            health["recommendation"] = "Using Helius fallback (slower but works)"
        else:
            health["recommendation"] = "⚠️ No healthy data sources - will use mock data"
        
        return health
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )
