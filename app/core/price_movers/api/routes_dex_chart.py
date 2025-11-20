"""
DEX Chart Routes - COMPLETE FIXED VERSION
With intelligent Dexscreener usage and proper fallback strategy
"""

import os
import time
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Query, Depends, HTTPException, status
from pydantic import BaseModel, Field

# Import from existing schemas
from app.core.price_movers.api.test_schemas import (
    TimeframeEnum,
    CandleData,
)
from app.core.price_movers.api.dependencies import (
    get_unified_collector,
    log_request,
)
from app.core.price_movers.utils.constants import DEX_CONFIGS

# Import validator if it exists
try:
    from app.core.price_movers.utils.validators import validate_dex_params
except ImportError:
    def validate_dex_params(dex_exchange: str, symbol: str, timeframe) -> None:
        """Simple validation fallback"""
        if not dex_exchange or not symbol:
            raise ValueError("DEX exchange and symbol are required")


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dex", tags=["DEX Charts"])


# ==================== Models ====================

class ChartCandleWithImpact(CandleData):
    """Extended candle with DEX-specific impact data"""
    has_high_impact: bool = Field(default=False, description="High impact trades detected")
    total_impact_score: float = Field(default=0.0, description="Total impact score")
    top_mover_count: int = Field(default=0, description="Number of top movers")
    is_synthetic: bool = Field(default=False, description="Is synthetic/mock data")
    is_estimated: bool = Field(default=False, description="Is estimated from limited data")


class DEXChartCandlesResponse(BaseModel):
    """DEX Chart response"""
    symbol: str = Field(..., description="Trading pair")
    dex_exchange: str = Field(..., description="DEX exchange")
    blockchain: str = Field(..., description="Blockchain network")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    candles: List[ChartCandleWithImpact] = Field(..., description="Candle data")
    total_candles: int = Field(..., description="Number of candles")
    data_source: str = Field(..., description="Data source")
    data_quality: str = Field(..., description="Data quality indicator")
    warning: Optional[str] = Field(None, description="Warning message if any")
    performance_ms: float = Field(..., description="Performance in milliseconds")


class DEXCandleMoversResponse(BaseModel):
    """Response for DEX candle wallet movers"""
    candle: CandleData
    top_movers: List[Dict] = Field(..., description="Top wallet movers with real addresses")
    analysis_metadata: Dict[str, Any]
    is_synthetic: bool = False
    has_real_wallet_ids: bool = True
    blockchain: str
    dex_exchange: str


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
                    'price_change_pct': price_change_pct,
                    'source': 'dexscreener'
                }, "dexscreener"
        except Exception as e:
            logger.debug(f"Dexscreener candle fetch failed: {e}")
    
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


# ==================== Main Routes ====================

@router.get(
    "/candles",
    response_model=DEXChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Chart Candles (Fixed & Optimized)",
    description="Smart fallback: Dexscreener (current only) → Birdeye (historical) → Helius (aggregation) → Mock"
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
    ## 🚀 Fixed DEX Chart with Smart Fallback
    
    **Strategy:**
    1. **Dexscreener** - Current price only (FREE, <1h range)
    2. **Birdeye** - Full historical OHLCV (if available & not suspended)
    3. **Helius** - Trade aggregation for historical data
    4. **Mock** - Last resort
    
    **Fixed Issues:**
    - ✅ Detects when historical data is needed (>1h)
    - ✅ Skips Dexscreener for multi-candle requests
    - ✅ Falls back to Helius for historical aggregation
    - ✅ Parallel processing for faster results
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
        
        # Determine token for chart
        if base_token.upper() == 'SOL':
            token_for_chart = quote_token.upper()
            logger.info(f"📊 Using {token_for_chart} for chart (not SOL)")
        else:
            token_for_chart = base_token.upper()
        
        candles_data = []
        data_source = "unknown"
        data_quality = "unknown"
        warning = None
        
        # Calculate time range details
        time_range_hours = (end_time - start_time).total_seconds() / 3600
        is_recent_data = (datetime.now(timezone.utc) - end_time).total_seconds() < 3600  # Within last hour
        
        logger.info(f"📅 Time range: {time_range_hours:.1f}h, Recent: {is_recent_data}")
        
        # ==================== SMART SOURCE SELECTION ====================
        
        # For CURRENT data ONLY (single candle, <1h range): Try Dexscreener
        if is_recent_data and time_range_hours <= 1 and unified_collector.dexscreener_collector:
            try:
                logger.info("🎯 Strategy: Dexscreener for current price (FREE, single candle)")
                
                current_candle = await unified_collector.dexscreener_collector.fetch_candle_data(
                    symbol=symbol,
                    timeframe=str(timeframe.value),
                    timestamp=end_time
                )
                
                # Check if we got valid data
                if current_candle and current_candle.get('open', 0) > 0:
                    candles_data = [current_candle]
                    data_source = "dexscreener"
                    data_quality = "current_only"
                    logger.info(f"✅ Dexscreener: Got current candle")
                
            except Exception as e:
                logger.debug(f"Dexscreener failed: {e}")
        
        elif time_range_hours > 1:
            logger.info(f"⏭️ Skipping Dexscreener (need {time_range_hours:.1f}h history, it only provides current)")
        
        # For HISTORICAL data (>1h range): Try Birdeye first, then Helius
        needs_historical = not candles_data or time_range_hours > 1
        
        if needs_historical:
            logger.info(f"📜 Need historical data ({time_range_hours:.1f}h)")
            
            # Try Birdeye for historical OHLCV
            if unified_collector.birdeye_collector:
                try:
                    logger.info("🎯 Trying Birdeye for historical OHLCV...")
                    
                    # Resolve token address
                    token_address = await unified_collector.birdeye_collector._resolve_symbol_to_address(
                        f"{token_for_chart}/USDC"
                    )
                    
                    if token_address:
                        logger.info(f"🔍 Token: {token_address[:8]}...")
                        
                        # Fetch OHLCV batch
                        birdeye_candles = await unified_collector.birdeye_collector.fetch_ohlcv_batch(
                            token_address=token_address,
                            timeframe=str(timeframe.value),
                            start_time=start_time,
                            end_time=end_time,
                            limit=100
                        )
                        
                        if birdeye_candles:
                            candles_data = birdeye_candles
                            data_source = "birdeye"
                            data_quality = "historical"
                            logger.info(f"✅ Birdeye: {len(candles_data)} historical candles")
                            
                except Exception as e:
                    error_str = str(e).lower()
                    logger.warning(f"⚠️ Birdeye failed: {e}")
                    
                    if any(x in error_str for x in ["401", "403", "suspended", "permission"]):
                        logger.info("🔴 Birdeye suspended/limited - falling back to Helius")
            
            # FALLBACK: Helius trade aggregation for historical data
            if not candles_data and unified_collector.helius_collector:
                try:
                    logger.info("🔄 Helius fallback - aggregating trades into candles...")
                    
                    # Verify Helius is properly configured for this DEX
                    dex_config = DEX_CONFIGS.get(dex_exchange.lower(), {})
                    blockchain = dex_config.get('blockchain', 'solana')
                    
                    if str(blockchain) != 'solana' and not hasattr(blockchain, 'value'):
                        logger.warning(f"Helius may not support blockchain: {blockchain}")
                    
                    timeframe_seconds = {
                        '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                        '1h': 3600, '4h': 14400, '1d': 86400,
                    }.get(str(timeframe.value), 300)
                    
                    total_seconds = (end_time - start_time).total_seconds()
                    num_candles = min(int(total_seconds / timeframe_seconds), 100)  # Limit to 100 for performance
                    
                    logger.info(f"📊 Aggregating {num_candles} candles from trades (symbol={symbol}, dex={dex_exchange})...")
                    
                    # Parallel processing for faster aggregation
                    async def fetch_single_candle(candle_start: datetime, candle_index: int) -> Optional[Dict]:
                        candle_end = candle_start + timedelta(seconds=timeframe_seconds)
                        try:
                            # Use fetch_trades instead of fetch_dex_trades
                            trades_result = await unified_collector.fetch_trades(
                                exchange=dex_exchange.lower(),
                                symbol=symbol,
                                start_time=candle_start,
                                end_time=candle_end,
                                limit=100
                            )
                            
                            # Extract trades from result
                            trades = trades_result.get('trades', []) if isinstance(trades_result, dict) else trades_result
                            
                            if trades:
                                prices = [t.get('price', 0) for t in trades if t.get('price')]
                                volumes = [t.get('value_usd', 0) for t in trades if t.get('value_usd')]
                                
                                if prices:
                                    logger.debug(f"Candle {candle_index}: {len(trades)} trades, price range {min(prices):.2f}-{max(prices):.2f}")
                                    return {
                                        'timestamp': candle_start,
                                        'open': prices[0],
                                        'high': max(prices),
                                        'low': min(prices),
                                        'close': prices[-1],
                                        'volume': sum(volumes) if volumes else 0,
                                    }
                                else:
                                    logger.debug(f"Candle {candle_index}: No valid prices in {len(trades)} trades")
                            else:
                                logger.debug(f"Candle {candle_index}: No trades found")
                        except Exception as e:
                            logger.warning(f"Candle {candle_index} fetch error @ {candle_start.strftime('%Y-%m-%d %H:%M')}: {e}")
                        return None
                    
                    # Fetch candles in parallel batches of 5
                    batch_size = 5
                    successful_candles = 0
                    failed_candles = 0
                    
                    for batch_start in range(0, num_candles, batch_size):
                        batch_end = min(batch_start + batch_size, num_candles)
                        tasks = []
                        
                        for i in range(batch_start, batch_end):
                            candle_time = start_time + timedelta(seconds=i * timeframe_seconds)
                            tasks.append(fetch_single_candle(candle_time, i + 1))
                        
                        logger.info(f"Fetching batch {batch_start//batch_size + 1}/{(num_candles-1)//batch_size + 1} ({batch_end - batch_start} candles)...")
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for result in results:
                            if isinstance(result, dict) and result:
                                candles_data.append(result)
                                successful_candles += 1
                            else:
                                failed_candles += 1
                        
                        logger.info(f"Batch result: {len([r for r in results if isinstance(r, dict)])} successful, {len([r for r in results if not isinstance(r, dict) or not r])} failed")
                        
                        # Rate limiting between batches
                        if batch_end < num_candles:
                            await asyncio.sleep(0.3)
                    
                    logger.info(f"Helius aggregation complete: {successful_candles} successful, {failed_candles} failed")
                    
                    if candles_data:
                        data_source = "helius"
                        data_quality = "aggregated"
                        logger.info(f"✅ Helius: {len(candles_data)} candles from trade aggregation")
                        warning = f"Data aggregated from {len(candles_data)} trade periods (Helius)"
                    else:
                        logger.warning(f"❌ Helius: No candles aggregated from {num_candles} attempts")
                        logger.info("Possible reasons: No trades in time range, API rate limits, or incorrect symbol format")
                        
                except Exception as e:
                    logger.error(f"❌ Helius aggregation failed: {e}")
        
        # LAST RESORT: Mock data
        if not candles_data:
            logger.warning("⚠️ All sources failed, generating mock data")
            
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
            data_quality = "synthetic"
            warning = "⚠️ MOCK DATA: All data sources failed. Check API keys and connectivity."
        
        # ==================== Build Response ====================
        
        chart_candles = []
        for candle in candles_data:
            # Calculate price_change_pct
            price_change_pct = 0.0
            if candle.get('open') and candle['open'] > 0:
                price_change_pct = ((candle.get('close', 0) - candle['open']) / candle['open']) * 100
            
            # Check if data is estimated
            is_estimated = candle.get('source') == 'dexscreener_estimated'
            
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
                is_synthetic=(data_source == "mock"),
                is_estimated=is_estimated
            )
            chart_candles.append(chart_candle)
        
        # Get blockchain info
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
            data_quality=data_quality,
            warning=warning,
            performance_ms=performance_ms
        )
        
        logger.info(
            f"[{request_id}] ✅ {len(chart_candles)} candles from {data_source} "
            f"(quality: {data_quality}) in {performance_ms:.0f}ms"
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
    "/candle/{candle_timestamp}/movers",
    response_model=DEXCandleMoversResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Wallet Movers for Candle",
    description="Loads REAL wallet addresses for a candle period (On-Chain)"
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
    ## 🎯 DEX Wallet Movers for Candle
    
    Features:
    - ✅ REAL Blockchain Addresses
    - ✅ On-Chain Transaction History
    - ✅ No synthetic data
    
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
                detail="Failed to fetch candle data from all sources"
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


@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="DEX Health Check"
)
async def dex_health_check():
    """Check DEX data sources with detailed status"""
    try:
        unified_collector = await get_unified_collector()
        
        health = {
            "sources": {
                "dexscreener": {
                    "available": unified_collector.dexscreener_collector is not None,
                    "type": "current_price",
                    "cost": "FREE",
                    "limitations": "Current price only, limited historical",
                    "status": "unknown"
                },
                "birdeye": {
                    "available": unified_collector.birdeye_collector is not None,
                    "type": "historical_ohlcv",
                    "cost": "$99/mo for OHLCV",
                    "limitations": "Requires paid plan for full features",
                    "status": "unknown"
                },
                "helius": {
                    "available": unified_collector.helius_collector is not None,
                    "type": "trade_aggregation",
                    "cost": "Free tier available",
                    "limitations": "Slower, requires aggregation",
                    "status": "unknown"
                }
            },
            "recommendation": None
        }
        
        # Test Dexscreener
        if unified_collector.dexscreener_collector:
            try:
                is_healthy = await unified_collector.dexscreener_collector.health_check()
                health["sources"]["dexscreener"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["sources"]["dexscreener"]["status"] = f"error: {str(e)[:50]}"
        
        # Test Birdeye
        if unified_collector.birdeye_collector:
            try:
                is_healthy = await unified_collector.birdeye_collector.health_check()
                health["sources"]["birdeye"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["sources"]["birdeye"]["status"] = f"error: {str(e)[:50]}"
        
        # Test Helius
        if unified_collector.helius_collector:
            try:
                is_healthy = await unified_collector.helius_collector.health_check()
                health["sources"]["helius"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["sources"]["helius"]["status"] = f"error: {str(e)[:50]}"
        
        # Generate recommendation based on health
        healthy_sources = [
            name for name, info in health["sources"].items() 
            if info.get("status") == "healthy"
        ]
        
        if "dexscreener" in healthy_sources and "birdeye" in healthy_sources:
            health["recommendation"] = "Optimal: Dexscreener for current + Birdeye for historical"
        elif "dexscreener" in healthy_sources:
            health["recommendation"] = "Using Dexscreener for current prices (limited historical)"
        elif "birdeye" in healthy_sources:
            health["recommendation"] = "Using Birdeye for full OHLCV data"
        elif "helius" in healthy_sources:
            health["recommendation"] = "Using Helius trade aggregation (slower but works)"
        else:
            health["recommendation"] = "⚠️ No healthy sources - will use mock data"
        
        # Add usage tips
        health["tips"] = {
            "current_price": "Use Dexscreener (free) for current/recent data",
            "historical_data": "Use Birdeye (paid) or Helius (free but slower) for historical",
            "performance": "Cache results when possible to reduce API calls"
        }
        
        return health
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )


@router.post(
    "/cache/clear",
    status_code=status.HTTP_200_OK,
    summary="Clear Dexscreener Cache"
)
async def clear_dexscreener_cache(
    request_id: str = Depends(log_request)
):
    """Clear the Dexscreener pool address cache"""
    try:
        unified_collector = await get_unified_collector()
        
        if unified_collector.dexscreener_collector:
            unified_collector.dexscreener_collector.clear_cache()
            return {"status": "success", "message": "Dexscreener cache cleared"}
        else:
            return {"status": "skipped", "message": "Dexscreener collector not available"}
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear cache: {str(e)}"
        )
