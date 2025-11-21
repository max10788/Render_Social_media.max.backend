"""
DEX Chart Routes - COMPLETE VERSION WITH MORALIS
Multi-Chain Support: Solana + Ethereum
Smart Fallback: Dexscreener → Moralis → Birdeye → Helius → Mock
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

def detect_blockchain(dex_exchange: str) -> str:
    """Detect blockchain from DEX name"""
    dex_lower = dex_exchange.lower()
    
    # Solana DEXes
    if dex_lower in ['jupiter', 'raydium', 'orca']:
        return 'solana'
    
    # Ethereum DEXes
    if dex_lower in ['uniswap', 'uniswapv2', 'uniswapv3', 'sushiswap']:
        return 'ethereum'
    
    # BSC DEXes
    if dex_lower in ['pancakeswap', 'pancakeswapv2', 'pancakeswapv3']:
        return 'bsc'
    
    # Polygon DEXes
    if dex_lower in ['quickswap']:
        return 'polygon'
    
    # Avalanche DEXes
    if dex_lower in ['traderjoe', 'pangolin']:
        return 'avalanche'
    
    # Arbitrum DEXes
    if dex_lower in ['camelot']:
        return 'arbitrum'
    
    # Optimism DEXes
    if dex_lower in ['velodrome']:
        return 'optimism'
    
    # Base DEXes
    if dex_lower in ['aerodrome', 'baseswap']:
        return 'base'
    
    # Fantom DEXes
    if dex_lower in ['spookyswap', 'spiritswap']:
        return 'fantom'
    
    logger.warning(f"Unknown DEX '{dex_exchange}', defaulting to ethereum")
    return 'ethereum'


async def fetch_candle_with_fallback(
    unified_collector,
    dex_exchange: str,
    symbol: str,
    timeframe: TimeframeEnum,
    timestamp: datetime
) -> tuple[Optional[Dict], str]:
    """
    Fetch candle data with smart fallback chain
    
    Priority:
    1. Dexscreener (current only, FREE)
    2. Moralis (historical, Solana + Ethereum)
    3. Birdeye (Solana only, if not suspended)
    4. Helius (Solana fallback)
    
    Returns:
        (candle_data, source) - candle dict and source name
    """
    # Detect blockchain
    blockchain = detect_blockchain(dex_exchange)
    
    # Parse symbol
    base_token, quote_token = symbol.split('/')
    if base_token.upper() == 'SOL':
        token_for_chart = quote_token.upper()
    else:
        token_for_chart = base_token.upper()
    
    # 1. Try Dexscreener (FREE! Current data only)
    if unified_collector.dexscreener_collector and blockchain == 'solana':
        try:
            logger.debug("🎯 Trying Dexscreener for candle (FREE, current)...")
            candle = await unified_collector.dexscreener_collector.fetch_candle_data(
                symbol=symbol,
                timeframe=str(timeframe.value),
                timestamp=timestamp
            )
            
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
            logger.debug(f"Dexscreener failed: {e}")
    
    # 2. Try Moralis (Historical, Multi-Chain!)
    if unified_collector.moralis_collector:
        try:
            logger.debug(f"🎯 Trying Moralis for candle ({blockchain})...")
            candle = await unified_collector.moralis_collector.fetch_candle_data(
                symbol=symbol,
                timeframe=str(timeframe.value),
                timestamp=timestamp,
                blockchain=blockchain,
                dex_exchange=dex_exchange
            )
            
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
                    'source': f'moralis_{blockchain}'
                }, f"moralis_{blockchain}"
        except Exception as e:
            logger.warning(f"Moralis {blockchain} failed: {e}")
    
    # 3. Try Birdeye (Solana only)
    if blockchain == 'solana' and unified_collector.birdeye_collector:
        try:
            logger.debug("🎯 Trying Birdeye for candle (Solana)...")
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
                        'price_change_pct': price_change_pct,
                        'source': 'birdeye'
                    }, "birdeye"
        except Exception as e:
            logger.warning(f"Birdeye failed: {e}")
    
    # 4. Fallback to Helius (Solana only, trade aggregation)
    if blockchain == 'solana' and unified_collector.helius_collector:
        try:
            logger.debug("🎯 Trying Helius fallback (Solana)...")
            
            timeframe_seconds = {
                '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                '1h': 3600, '4h': 14400, '1d': 86400,
            }.get(str(timeframe.value), 300)
            
            end_time = timestamp + timedelta(seconds=timeframe_seconds)
            
            # Use fetch_trades from unified collector
            trades_result = await unified_collector.fetch_trades(
                exchange=dex_exchange.lower(),
                symbol=symbol,
                start_time=timestamp,
                end_time=end_time,
                limit=100
            )
            
            trades = trades_result.get('trades', []) if isinstance(trades_result, dict) else trades_result
            
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
                        'price_change_pct': price_change_pct,
                        'source': 'helius'
                    }, "helius"
        except Exception as e:
            logger.error(f"Helius failed: {e}")
    
    return None, "none"


# ==================== Main Routes ====================

@router.get(
    "/candles",
    response_model=DEXChartCandlesResponse,
    status_code=status.HTTP_200_OK,
    summary="Get DEX Chart Candles (Multi-Chain with Moralis)",
    description="Smart fallback: Dexscreener → Moralis (Solana+ETH) → Birdeye → Helius → Mock"
)
async def get_dex_chart_candles(
    dex_exchange: str = Query(..., description="DEX (jupiter/raydium/orca/uniswap/sushiswap)"),
    symbol: str = Query(..., description="Token pair (e.g., SOL/USDC, ETH/USDT)"),
    timeframe: TimeframeEnum = Query(..., description="Candle timeframe"),
    start_time: datetime = Query(..., description="Start time"),
    end_time: datetime = Query(..., description="End time"),
    include_impact: bool = Query(default=False, description="Calculate impact"),
    request_id: str = Depends(log_request)
) -> DEXChartCandlesResponse:
    """
    ## 🚀 Multi-Chain DEX Chart with Moralis Support
    
    **Supported Chains:**
    - Solana: Jupiter, Raydium, Orca
    - Ethereum: Uniswap, Sushiswap
    
    **Data Source Priority:**
    1. **Dexscreener** - Current price (FREE, Solana only)
    2. **Moralis** - Historical OHLCV (Solana + Ethereum, 3 API keys)
    3. **Birdeye** - Solana historical (if not suspended)
    4. **Helius** - Solana trade aggregation
    5. **Mock** - Last resort
    """
    start_perf = time.time()
    
    try:
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] 🚀 DEX Chart: {dex_exchange} {symbol} {timeframe.value} "
            f"({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})"
        )
        
        unified_collector = await get_unified_collector()
        
        # Detect blockchain
        blockchain = detect_blockchain(dex_exchange)
        logger.info(f"🌐 Detected blockchain: {blockchain}")
        
        # Parse symbol
        base_token, quote_token = symbol.split('/')
        
        # Determine token for chart
        if base_token.upper() == 'SOL':
            token_for_chart = quote_token.upper()
            logger.info(f"📊 Using {token_for_chart} for chart (not SOL)")
        elif base_token.upper() == 'ETH' or base_token.upper() == 'WETH':
            token_for_chart = quote_token.upper()
            logger.info(f"📊 Using {token_for_chart} for chart (not ETH)")
        else:
            token_for_chart = base_token.upper()
        
        candles_data = []
        data_source = "unknown"
        data_quality = "unknown"
        warning = None
        
        # Calculate time range details
        time_range_hours = (end_time - start_time).total_seconds() / 3600
        is_recent_data = (datetime.now(timezone.utc) - end_time).total_seconds() < 3600
        
        logger.info(f"📅 Time range: {time_range_hours:.1f}h, Recent: {is_recent_data}")
        
        # ==================== SMART SOURCE SELECTION ====================
        
        # Strategy 1: Try Dexscreener for current Solana data (<1h, recent)
        if (is_recent_data and time_range_hours <= 1 and 
            blockchain == 'solana' and unified_collector.dexscreener_collector):
            try:
                logger.info("🎯 Strategy: Dexscreener (Solana, current, FREE)")
                
                current_candle = await unified_collector.dexscreener_collector.fetch_candle_data(
                    symbol=symbol,
                    timeframe=str(timeframe.value),
                    timestamp=end_time
                )
                
                if current_candle and current_candle.get('open', 0) > 0:
                    candles_data = [current_candle]
                    data_source = "dexscreener"
                    data_quality = "current_only"
                    logger.info(f"✅ Dexscreener: Got current candle")
                
            except Exception as e:
                logger.debug(f"Dexscreener failed: {e}")
        
        elif time_range_hours > 1:
            logger.info(f"⏭️ Skipping Dexscreener (need {time_range_hours:.1f}h history)")
        
        # Strategy 2: Try Moralis for historical data (Multi-Chain!)
        needs_historical = not candles_data or time_range_hours > 1
        
        if needs_historical and unified_collector.moralis_collector:
            try:
                logger.info(f"🎯 Strategy: Moralis ({blockchain}, historical)")
                
                # Fetch OHLCV batch from Moralis
                moralis_candles = await unified_collector.moralis_collector.fetch_ohlcv_batch(
                    symbol=symbol,
                    timeframe=str(timeframe.value),
                    start_time=start_time,
                    end_time=end_time,
                    limit=100,
                    blockchain=blockchain,
                    dex_exchange=dex_exchange
                )
                
                if moralis_candles:
                    candles_data = moralis_candles
                    data_source = f"moralis_{blockchain}"
                    data_quality = "historical"
                    logger.info(f"✅ Moralis: {len(candles_data)} {blockchain} candles")
                    
            except Exception as e:
                logger.warning(f"⚠️ Moralis {blockchain} failed: {e}")
        
        # Strategy 3: Try Birdeye for Solana (if Moralis failed)
        if not candles_data and blockchain == 'solana' and unified_collector.birdeye_collector:
            try:
                logger.info("🎯 Strategy: Birdeye (Solana fallback)")
                
                token_address = await unified_collector.birdeye_collector._resolve_symbol_to_address(
                    f"{token_for_chart}/USDC"
                )
                
                if token_address:
                    logger.info(f"🔍 Token: {token_address[:8]}...")
                    
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
                        logger.info(f"✅ Birdeye: {len(candles_data)} candles")
                        
            except Exception as e:
                error_str = str(e).lower()
                logger.warning(f"⚠️ Birdeye failed: {e}")
                
                if any(x in error_str for x in ["401", "403", "suspended", "permission"]):
                    logger.info("🔴 Birdeye suspended/limited")
        
        # Strategy 4: Helius trade aggregation (Solana only)
        if not candles_data and blockchain == 'solana' and unified_collector.helius_collector:
            try:
                logger.info("🔄 Strategy: Helius trade aggregation (Solana)")
                
                timeframe_seconds = {
                    '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                    '1h': 3600, '4h': 14400, '1d': 86400,
                }.get(str(timeframe.value), 300)
                
                total_seconds = (end_time - start_time).total_seconds()
                num_candles = min(int(total_seconds / timeframe_seconds), 100)
                
                logger.info(f"📊 Aggregating {num_candles} candles from trades...")
                
                # Parallel processing
                async def fetch_single_candle(candle_start: datetime, candle_index: int) -> Optional[Dict]:
                    candle_end = candle_start + timedelta(seconds=timeframe_seconds)
                    try:
                        trades_result = await unified_collector.fetch_trades(
                            exchange=dex_exchange.lower(),
                            symbol=symbol,
                            start_time=candle_start,
                            end_time=candle_end,
                            limit=100
                        )
                        
                        trades = trades_result.get('trades', []) if isinstance(trades_result, dict) else trades_result
                        
                        if trades:
                            prices = [t.get('price', 0) for t in trades if t.get('price')]
                            volumes = [t.get('value_usd', 0) for t in trades if t.get('value_usd')]
                            
                            if prices:
                                return {
                                    'timestamp': candle_start,
                                    'open': prices[0],
                                    'high': max(prices),
                                    'low': min(prices),
                                    'close': prices[-1],
                                    'volume': sum(volumes) if volumes else 0,
                                }
                    except Exception as e:
                        logger.debug(f"Candle {candle_index} error: {e}")
                    return None
                
                # Fetch in batches
                batch_size = 5
                for batch_start in range(0, num_candles, batch_size):
                    batch_end = min(batch_start + batch_size, num_candles)
                    tasks = []
                    
                    for i in range(batch_start, batch_end):
                        candle_time = start_time + timedelta(seconds=i * timeframe_seconds)
                        tasks.append(fetch_single_candle(candle_time, i + 1))
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, dict) and result:
                            candles_data.append(result)
                    
                    if batch_end < num_candles:
                        await asyncio.sleep(0.3)
                
                if candles_data:
                    data_source = "helius"
                    data_quality = "aggregated"
                    logger.info(f"✅ Helius: {len(candles_data)} aggregated candles")
                    warning = f"Data aggregated from {len(candles_data)} trade periods"
                        
            except Exception as e:
                logger.error(f"❌ Helius aggregation failed: {e}")
        
        # Strategy 5: Mock data (last resort)
        if not candles_data:
            logger.warning("⚠️ All sources failed, generating mock data")
            
            timeframe_seconds = {
                '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
                '1h': 3600, '4h': 14400, '1d': 86400,
            }.get(str(timeframe.value), 300)
            
            base_price = 100.0
            current_time = start_time
            num_candles = min(100, int((end_time - start_time).total_seconds() / timeframe_seconds))
            
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
            warning = "⚠️ MOCK DATA: All data sources failed. Enable Moralis API keys for real data."
        
        # ==================== Build Response ====================
        
        chart_candles = []
        for candle in candles_data:
            price_change_pct = 0.0
            if candle.get('open') and candle['open'] > 0:
                price_change_pct = ((candle.get('close', 0) - candle['open']) / candle['open']) * 100
            
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
            f"[{request_id}] ✅ {len(chart_candles)} candles from {data_source} "
            f"({blockchain}) in {performance_ms:.0f}ms"
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
    - ✅ Multi-Chain Support (Solana + Ethereum)
    """
    start_perf = time.time()
    
    try:
        validate_dex_params(dex_exchange, symbol, timeframe)
        
        logger.info(
            f"[{request_id}] DEX Candle movers: {dex_exchange} {symbol} "
            f"@ {candle_timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        unified_collector = await get_unified_collector()
        
        # Detect blockchain
        blockchain = detect_blockchain(dex_exchange)

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

        # Fetch candle and trades
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

        # Analyze trades
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
                "data_source": source,
                "blockchain": blockchain
            },
            is_synthetic=False,
            has_real_wallet_ids=True,
            blockchain=blockchain,
            dex_exchange=dex_exchange
        )
        
        logger.info(
            f"[{request_id}] ✅ DEX movers: {len(dex_movers)} wallets "
            f"from {len(trades_result.get('trades', []))} trades "
            f"({blockchain}, {performance_ms:.0f}ms)"
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
                    "chains": ["solana"],
                    "cost": "FREE",
                    "status": "unknown"
                },
                "moralis": {
                    "available": unified_collector.moralis_collector is not None,
                    "type": "historical_ohlcv",
                    "chains": ["solana", "ethereum"],
                    "cost": "Free tier + 2 fallbacks",
                    "status": "unknown"
                },
                "birdeye": {
                    "available": unified_collector.birdeye_collector is not None,
                    "type": "historical_ohlcv",
                    "chains": ["solana"],
                    "cost": "$99/mo for OHLCV",
                    "status": "unknown"
                },
                "helius": {
                    "available": unified_collector.helius_collector is not None,
                    "type": "trade_aggregation",
                    "chains": ["solana"],
                    "cost": "Free tier",
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
        
        # Test Moralis
        if unified_collector.moralis_collector:
            try:
                is_healthy = await unified_collector.moralis_collector.health_check()
                health["sources"]["moralis"]["status"] = "healthy" if is_healthy else "unhealthy"
            except Exception as e:
                health["sources"]["moralis"]["status"] = f"error: {str(e)[:50]}"
        
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
        
        # Generate recommendation
        healthy_sources = [
            name for name, info in health["sources"].items() 
            if info.get("status") == "healthy"
        ]
        
        if "moralis" in healthy_sources:
            health["recommendation"] = "✅ Optimal: Moralis available for Solana + Ethereum historical data"
        elif "dexscreener" in healthy_sources and "birdeye" in healthy_sources:
            health["recommendation"] = "Good: Dexscreener + Birdeye for Solana"
        elif "helius" in healthy_sources:
            health["recommendation"] = "Limited: Using Helius trade aggregation (slower)"
        else:
            health["recommendation"] = "⚠️ No healthy sources - using mock data"
        
        health["tips"] = {
            "best_setup": "Moralis (multi-chain) + Dexscreener (current prices)",
            "ethereum": "Moralis is required for Ethereum DEX data",
            "solana": "Multiple fallbacks available: Dexscreener → Moralis → Birdeye → Helius"
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
