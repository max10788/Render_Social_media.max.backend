"""
DEX Chart Routes - HYBRID APPROACH
Historical from CEX + Current from DEX
"""

import os
import time
import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Query, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.core.price_movers.api.test_schemas import (
    TimeframeEnum,
    CandleData,
)
from app.core.price_movers.api.dependencies import (
    get_unified_collector,
    log_request,
)

try:
    from app.core.price_movers.utils.validators import validate_dex_params
except ImportError:
    def validate_dex_params(dex_exchange: str, symbol: str, timeframe) -> None:
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
    """DEX Chart response with Hybrid data"""
    symbol: str = Field(..., description="Trading pair")
    dex_exchange: str = Field(..., description="DEX exchange")
    blockchain: str = Field(..., description="Blockchain network")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    candles: List[ChartCandleWithImpact] = Field(..., description="Candle data")
    total_candles: int = Field(..., description="Number of candles")
    data_source: str = Field(..., description="Data source (hybrid_cex+dex, cex_binance, mock)")
    data_quality: str = Field(..., description="Data quality indicator")
    warning: Optional[str] = Field(None, description="Warning message if hybrid mode")
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

def detect_blockchain(dex_exchange: str) -> str:
    """Detect blockchain from DEX name"""
    dex_lower = dex_exchange.lower()
    
    if dex_lower in ['jupiter', 'raydium', 'orca']:
        return 'solana'
    elif dex_lower in ['uniswap', 'uniswapv2', 'uniswapv3', 'sushiswap']:
        return 'ethereum'
    elif dex_lower in ['pancakeswap', 'pancakeswapv2', 'pancakeswapv3']:
        return 'bsc'
    elif dex_lower in ['quickswap']:
        return 'polygon'
    elif dex_lower in ['traderjoe', 'pangolin']:
        return 'avalanche'
    elif dex_lower in ['camelot']:
        return 'arbitrum'
    elif dex_lower in ['velodrome']:
        return 'optimism'
    elif dex_lower in ['aerodrome', 'baseswap']:
        return 'base'
    elif dex_lower in ['spookyswap', 'spiritswap']:
        return 'fantom'
    
    logger.warning(f"Unknown DEX '{dex_exchange}', defaulting to solana")
    return 'solana'


def get_cex_symbol(base_token: str, quote_token: str, cex_exchange: str = 'binance') -> str:
    """Convert DEX symbol to CEX format"""
    # CEX format: SOLUSDT (no separator)
    return f"{base_token}{quote_token}".upper()


async def fetch_current_dex_candle(
    unified_collector,
    dex_exchange: str,
    symbol: str,
    timeframe: TimeframeEnum,
    timestamp: datetime
) -> tuple[Optional[Dict], str]:
    """
    Fetch ONLY current candle from DEX
    
    Returns:
        (candle_data, source)
    """
    blockchain = detect_blockchain(dex_exchange)
    
    # Only for Solana
    if blockchain != 'solana':
        return None, "none"
    
    # Try Dexscreener first (fastest)
    if unified_collector.dexscreener_collector:
        try:
            logger.debug("🎯 Trying Dexscreener for current candle...")
            candle = await unified_collector.dexscreener_collector.fetch_candle_data(
                symbol=symbol,
                timeframe=str(timeframe.value),
                timestamp=timestamp
            )
            
            if candle and candle.get('open', 0) > 0:
                return {
                    'timestamp': candle.get('timestamp', timestamp),
                    'open': float(candle.get('open', 0)),
                    'high': float(candle.get('high', 0)),
                    'low': float(candle.get('low', 0)),
                    'close': float(candle.get('close', 0)),
                    'volume': float(candle.get('volume', 0)),
                }, "dexscreener"
        except Exception as e:
            logger.debug(f"Dexscreener failed: {e}")
    
    # Try Helius as fallback
    if unified_collector.helius_collector:
        try:
            logger.debug("🎯 Trying Helius for current candle...")
            candle = await unified_collector.helius_collector.fetch_candle_data(
                symbol=symbol,
                timeframe=str(timeframe.value),
                timestamp=timestamp
            )
            
            if candle and candle.get('open', 0) > 0:
                return {
                    'timestamp': candle.get('timestamp', timestamp),
                    'open': float(candle.get('open', 0)),
                    'high': float(candle.get('high', 0)),
                    'low': float(candle.get('low', 0)),
                    'close': float(candle.get('close', 0)),
                    'volume': float(candle.get('volume', 0)),
                }, "helius"
        except Exception as e:
            logger.debug(f"Helius failed: {e}")
    
    return None, "none"


# ==================== Main Routes ====================

@router.get(
    "/candles",
    response_model=DEXChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Chart Candles (HYBRID: CEX Historical + DEX Current)",
    description="🚀 Fast & Reliable: 99 candles from CEX + 1 current candle from DEX with real wallets"
)
async def get_dex_chart_candles(
    dex_exchange: str = Query(..., description="DEX (jupiter/raydium/orca)"),
    symbol: str = Query(..., description="Token pair (e.g., SOL/USDT)"),
    timeframe: TimeframeEnum = Query(..., description="Candle timeframe"),
    start_time: datetime = Query(..., description="Start time"),
    end_time: datetime = Query(..., description="End time"),
    include_impact: bool = Query(default=False, description="Calculate impact (only for current candle)"),
    request_id: str = Depends(log_request)
) -> DEXChartCandlesResponse:
    """
    ## 🚀 Hybrid DEX Chart Strategy
    
    **Why Hybrid?**
    - Historical DEX data is unreliable (rate limits, filtering issues)
    - CEX has perfect historical data
    - Only CURRENT activity needs real wallet analysis
    
    **Strategy:**
    1. **Historical Candles (0-99)**: From CEX (Binance/Bitget)
       - Fast, reliable, no rate limits
       - Perfect OHLCV data
    
    2. **Current Candle (100)**: From DEX (Helius/Dexscreener)
       - Real wallet addresses
       - Live trading activity
       - High-impact analysis
    
    **Performance:**
    - Old: ~90s (100 DEX requests)
    - New: ~1s (99 CEX + 1 DEX)
    """
    start_perf = time.time()
    
    try:
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] 🚀 HYBRID Chart: {dex_exchange} {symbol} {timeframe.value} "
            f"({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})"
        )
        
        unified_collector = await get_unified_collector()
        blockchain = detect_blockchain(dex_exchange)
        
        # Parse symbol
        base_token, quote_token = symbol.split('/')
        
        candles_data = []
        data_source = "unknown"
        data_quality = "unknown"
        warning = None
        
        # Calculate time range
        time_range_hours = (end_time - start_time).total_seconds() / 3600
        is_recent = (datetime.now(timezone.utc) - end_time).total_seconds() < 3600
        
        timeframe_seconds = {
            '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '4h': 14400, '1d': 86400,
        }.get(str(timeframe.value), 300)
        
        num_candles = int((end_time - start_time).total_seconds() / timeframe_seconds)
        
        # ==================== STRATEGY: CEX for Historical ====================

        if time_range_hours > 1:
            logger.info(f"📊 Strategy: CEX Historical ({time_range_hours:.1f}h)")
            
            # ✅ PRIORITÄT: Bitget vor Binance
            cex_exchange = None
            ccxt_exchange = None
            
            # 1. Versuche zuerst Bitget
            if 'bitget' in unified_collector.cex_collectors:
                cex_exchange = 'bitget'
                ccxt_exchange = unified_collector.cex_collectors['bitget']
                logger.info("✅ Using Bitget for historical data")
            
            # 2. Fallback zu Binance nur wenn Bitget nicht verfügbar
            elif 'binance' in unified_collector.cex_collectors:
                cex_exchange = 'binance'
                ccxt_exchange = unified_collector.cex_collectors['binance']
                logger.info("⚠️ Using Binance (Bitget not available)")
            
            # 3. Versuche Kraken als letzten Fallback
            elif 'kraken' in unified_collector.cex_collectors:
                cex_exchange = 'kraken'
                ccxt_exchange = unified_collector.cex_collectors['kraken']
                logger.info("⚠️ Using Kraken (Bitget and Binance not available)")
            
            if ccxt_exchange:
                try:
                    cex_symbol = get_cex_symbol(base_token, quote_token, cex_exchange)
                    logger.info(f"Fetching {num_candles - 1} candles from {cex_exchange}...")
                    
                    # CCXT batch fetch
                    since = int(start_time.timestamp() * 1000)
                    limit = min(num_candles, 100)
                    
                    ohlcv_data = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: ccxt_exchange.fetch_ohlcv(
                            cex_symbol,
                            str(timeframe.value),
                            since,
                            limit
                        )
                    )
                    
                    # Convert to candle format (skip last candle)
                    for candle_data in ohlcv_data[:-1]:
                        candles_data.append({
                            'timestamp': datetime.fromtimestamp(candle_data[0] / 1000, tz=timezone.utc),
                            'open': float(candle_data[1]),
                            'high': float(candle_data[2]),
                            'low': float(candle_data[3]),
                            'close': float(candle_data[4]),
                            'volume': float(candle_data[5]),
                        })
                    
                    logger.info(f"✅ CEX Historical: {len(candles_data)} candles from {cex_exchange}")
                    data_source = f"cex_{cex_exchange}"
                    data_quality = "historical_reliable"
                    
                except Exception as e:
                    logger.error(f"CEX historical failed ({cex_exchange}): {e}", exc_info=True)
                    # Wenn der primäre CEX fehlschlägt, versuche den nächsten
                    if cex_exchange == 'bitget' and 'binance' in unified_collector.cex_collectors:
                        logger.info("🔄 Bitget failed, trying Binance as fallback...")
                        try:
                            ccxt_exchange = unified_collector.cex_collectors['binance']
                            cex_symbol = get_cex_symbol(base_token, quote_token, 'binance')
                            since = int(start_time.timestamp() * 1000)
                            limit = min(num_candles, 100)
                            
                            ohlcv_data = await asyncio.get_event_loop().run_in_executor(
                                None,
                                lambda: ccxt_exchange.fetch_ohlcv(cex_symbol, str(timeframe.value), since, limit)
                            )
                            
                            for candle_data in ohlcv_data[:-1]:
                                candles_data.append({
                                    'timestamp': datetime.fromtimestamp(candle_data[0] / 1000, tz=timezone.utc),
                                    'open': float(candle_data[1]),
                                    'high': float(candle_data[2]),
                                    'low': float(candle_data[3]),
                                    'close': float(candle_data[4]),
                                    'volume': float(candle_data[5]),
                                })
                            
                            logger.info(f"✅ CEX Historical from Binance fallback: {len(candles_data)} candles")
                            data_source = "cex_binance"
                            data_quality = "historical_reliable"
                        except Exception as fallback_error:
                            logger.error(f"Binance fallback also failed: {fallback_error}")
            else:
                logger.warning("⚠️ No CEX collectors available for historical data")
        
        # ==================== STRATEGY: DEX for Current Candle ====================
        
        if is_recent and blockchain == 'solana':
            logger.info("🎯 Strategy: DEX Current Candle (with real wallets)")
            
            try:
                # Calculate current candle time
                current_candle_time = end_time - timedelta(seconds=timeframe_seconds)
                
                current_candle, dex_source = await fetch_current_dex_candle(
                    unified_collector=unified_collector,
                    dex_exchange=dex_exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=current_candle_time
                )
                
                if current_candle:
                    candles_data.append(current_candle)
                    data_source = f"hybrid_{data_source.split('_')[1] if '_' in data_source else 'cex'}+{dex_source}"
                    data_quality = "current_with_wallets"
                    warning = f"Historical data from CEX, current activity from {dex_source.upper()} DEX"
                    logger.info(f"✅ DEX Current: From {dex_source}")
                else:
                    logger.warning("DEX current candle unavailable, using CEX only")
                    
            except Exception as e:
                logger.warning(f"DEX current failed: {e}")
        
        # ==================== Fallback: Mock Data ====================
        
        if not candles_data:
            logger.warning("⚠️ All sources failed, generating mock data")
            
            base_price = 100.0
            current_time = start_time
            
            for i in range(min(num_candles, 100)):
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
            warning = "⚠️ MOCK DATA: Enable CEX/DEX APIs for real data"
        
        # ==================== Build Response ====================
        
        chart_candles = []
        for idx, candle in enumerate(candles_data):
            price_change_pct = 0.0
            if candle.get('open') and candle['open'] > 0:
                price_change_pct = ((candle.get('close', 0) - candle['open']) / candle['open']) * 100
            
            # Only last candle can have wallet analysis
            is_last_candle = (idx == len(candles_data) - 1)
            
            chart_candle = ChartCandleWithImpact(
                timestamp=candle['timestamp'],
                open=float(candle['open']),
                high=float(candle['high']),
                low=float(candle['low']),
                close=float(candle['close']),
                volume=float(candle.get('volume', 0)),
                price_change_pct=price_change_pct,
                has_high_impact=(is_last_candle and 'dex' in data_source and include_impact),
                total_impact_score=0.0,
                top_mover_count=0,
                is_synthetic=(data_source == "mock"),
                is_estimated=False
            )
            chart_candles.append(chart_candle)
        
        performance_ms = (time.time() - start_perf) * 1000
        
        response = DEXChartCandlesResponse(
            symbol=symbol,
            dex_exchange=dex_exchange,
            blockchain=blockchain,
            timeframe=timeframe,
            candles=chart_candles,
            total_candles=len(chart_candles),
            data_source=data_source,
            data_quality=data_quality,
            warning=warning,
            performance_ms=performance_ms
        )
        
        logger.info(
            f"[{request_id}] ✅ {len(chart_candles)} candles "
            f"(source: {data_source}, quality: {data_quality}) in {performance_ms:.0f}ms"
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] ❌ Error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load chart: {str(e)}"
        )


@router.get(
    "/candle/{candle_timestamp}/movers",
    response_model=DEXCandleMoversResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Wallet Movers for Current Candle",
    description="Loads REAL wallet addresses for the CURRENT candle only"
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
    ## 🎯 Wallet Movers for CURRENT Candle
    
    **Important:** This endpoint is designed for the CURRENT candle only!
    Historical candles don't have wallet data (they come from CEX).
    
    Features:
    - ✅ REAL Blockchain Addresses
    - ✅ On-Chain Transaction History
    - ✅ Only works for current/recent activity
    """
    start_perf = time.time()
    
    try:
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        now = datetime.now(timezone.utc)
        if candle_timestamp.tzinfo is None:
            candle_timestamp = candle_timestamp.replace(tzinfo=timezone.utc)
        
        hours_ago = (now - candle_timestamp).total_seconds() / 3600
        
        if hours_ago > 2:
            raise HTTPException(
                status_code=400,
                detail=f"Wallet analysis only available for recent candles (<2h). "
                       f"This candle is {hours_ago:.1f}h old. Historical candles use CEX data."
            )
        
        logger.info(
            f"[{request_id}] Wallet movers: {dex_exchange} {symbol} "
            f"@ {candle_timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        unified_collector = await get_unified_collector()
        blockchain = detect_blockchain(dex_exchange)

        from app.core.price_movers.services.analyzer_hybrid import HybridPriceMoverAnalyzer
        
        analyzer = HybridPriceMoverAnalyzer(
            unified_collector=unified_collector,
            use_lightweight=True
        )

        timeframe_minutes = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "4h": 240, "1d": 1440
        }.get(str(timeframe.value), 5)

        start_time = candle_timestamp
        end_time = candle_timestamp + timedelta(minutes=timeframe_minutes)
        
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        logger.debug("Fetching DEX candle and trades...")

        current_candle, source = await fetch_current_dex_candle(
            unified_collector=unified_collector,
            dex_exchange=dex_exchange,
            symbol=symbol,
            timeframe=timeframe,
            timestamp=start_time
        )
        
        if not current_candle:
            raise HTTPException(
                status_code=500,
                detail="Failed to fetch current candle from DEX"
            )
        
        trades_result = await unified_collector.fetch_trades(
            exchange=dex_exchange.lower(),
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=5000
        )

        logger.debug("Analyzing DEX trades...")

        from app.core.price_movers.services.analyzer_hybrid import Candle as HybridCandle

        candle_obj = HybridCandle(**current_candle)

        dex_movers = await analyzer._analyze_dex_trades(
            trades=trades_result.get('trades', []),
            candle=candle_obj,
            symbol=symbol,
            exchange=dex_exchange,
            top_n=top_n_wallets
        )

        # --- Impact Score Calculation ---
        impact_calculator = ImpactCalculator()
        total_volume = current_candle.get('volume', 0.0)
        logger.debug(f"Total candle volume for impact calc: {total_volume}")

        # Group trades by wallet for the calculator
        wallet_activities = {}
        for trade in trades_result.get('trades', []):
            wallet_addr = trade.get('wallet_address')
            if wallet_addr:
                if wallet_addr not in wallet_activities:
                    wallet_activities[wallet_addr] = []
                wallet_activities[wallet_addr].append(trade)

        # Calculate impact scores
        impact_results = impact_calculator.calculate_batch_impact(
            wallet_activities=wallet_activities,
            candle_data=current_candle,
            total_volume=total_volume
        )

        # Update the top_movers list with impact scores
        for mover in dex_movers:
            wallet_id = mover.get('wallet_address') # or the key used in the response
            if wallet_id in impact_results:
                impact_data = impact_results[wallet_id]
                # Add impact score to the existing mover data
                mover['total_impact_score'] = impact_data.get('impact_score', 0.0)
                mover['impact_components'] = impact_data.get('components', {})
                mover['impact_level'] = impact_data.get('impact_level', 'none')

        # --- End Impact Score Calculation ---

        performance_ms = (time.time() - start_perf) * 1000

        response = DEXCandleMoversResponse(
            candle=CandleData(**current_candle),
            top_movers=dex_movers,
            analysis_metadata={
                "analysis_timestamp": datetime.now(timezone.utc),
                "processing_duration_ms": int(performance_ms),
                "total_trades_analyzed": len(trades_result.get('trades', [])),
                "unique_wallets_found": len(dex_movers),
                "exchange": dex_exchange,
                "symbol": symbol,
                "timeframe": str(timeframe.value),
                "data_source": source,
                "blockchain": blockchain
            },
            is_synthetic=False,
            has_real_wallet_ids=True,
            blockchain=blockchain,
            dex_exchange=dex_exchange
        )
        
        logger.info(
            f"[{request_id}] ✅ Wallet movers: {len(dex_movers)} wallets "
            f"from {len(trades_result.get('trades', []))} trades "
            f"({blockchain}, {performance_ms:.0f}ms)"
        )

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] ❌ Wallet movers error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load wallet movers: {str(e)}"
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
            "strategy": "hybrid_cex_dex",
            "sources": {
                "cex": {
                    "binance": {
                        "available": 'binance' in unified_collector.cex_collectors,
                        "purpose": "Historical candles (0-99)",
                        "status": "unknown"
                    },
                    "bitget": {
                        "available": 'bitget' in unified_collector.cex_collectors,
                        "purpose": "Fallback historical",
                        "status": "unknown"
                    }
                },
                "dex": {
                    "dexscreener": {
                        "available": unified_collector.dexscreener_collector is not None,
                        "purpose": "Current candle (fast)",
                        "status": "unknown"
                    },
                    "helius": {
                        "available": unified_collector.helius_collector is not None,
                        "purpose": "Current candle with wallets",
                        "status": "unknown"
                    }
                }
            },
            "recommendation": None,
            "performance": {
                "old_approach": "~90s (100 DEX requests)",
                "new_approach": "~1s (99 CEX + 1 DEX)"
            }
        }
        
        # Test CEX
        for cex_name in ['binance', 'bitget']:
            if cex_name in unified_collector.cex_collectors:
                try:
                    cex_collector = unified_collector.cex_collectors[cex_name]
                    is_healthy = await cex_collector.health_check()
                    health["sources"]["cex"][cex_name]["status"] = "healthy" if is_healthy else "unhealthy"
                except Exception as e:
                    health["sources"]["cex"][cex_name]["status"] = f"error: {str(e)[:50]}"
        
        # Test DEX
        if unified_collector.dexscreener_collector:
            try:
                is_healthy = await unified_collector.dexscreener_collector.health_check()
                health["sources"]["dex"]["dexscreener"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["sources"]["dex"]["dexscreener"]["status"] = f"error: {str(e)[:50]}"
        
        if unified_collector.helius_collector:
            try:
                is_healthy = await unified_collector.helius_collector.health_check()
                health["sources"]["dex"]["helius"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["sources"]["dex"]["helius"]["status"] = f"error: {str(e)[:50]}"
        
        # Generate recommendation
        cex_healthy = any(
            health["sources"]["cex"][cex].get("status") == "healthy"
            for cex in ["binance", "bitget"]
        )
        
        dex_healthy = any(
            health["sources"]["dex"][dex].get("status") == "healthy"
            for dex in ["dexscreener", "helius"]
        )
        
        if cex_healthy and dex_healthy:
            health["recommendation"] = "✅ Optimal: CEX for historical + DEX for current with wallets"
        elif cex_healthy:
            health["recommendation"] = "⚠️ CEX only: Historical data available, but no current wallet analysis"
        elif dex_healthy:
            health["recommendation"] = "⚠️ DEX only: Current data available, but slow for historical"
        else:
            health["recommendation"] = "❌ No healthy sources - will use mock data"
        
        return health
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )


@router.post(
    "/cache/clear",
    status_code=status.HTTP_200_OK,
    summary="Clear Cache"
)
async def clear_cache(
    request_id: str = Depends(log_request)
):
    """Clear collector caches"""
    try:
        unified_collector = await get_unified_collector()
        
        cleared = []
        
        if unified_collector.dexscreener_collector:
            unified_collector.dexscreener_collector.clear_cache()
            cleared.append("dexscreener")
        
        if unified_collector.helius_collector:
            unified_collector.helius_collector.cache.clear()
            cleared.append("helius")
        
        return {
            "status": "success",
            "cleared": cleared,
            "message": f"Cache cleared for: {', '.join(cleared)}"
        }
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to clear cache: {str(e)}"
        )
