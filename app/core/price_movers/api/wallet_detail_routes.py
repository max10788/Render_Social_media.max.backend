"""
Enhanced Wallet Detail Routes - DEX Wallet Support + DEBUG LOGGING

Neue Features:
- ✅ Echte DEX Wallet-Adressen mit Explorer-URLs
- ✅ CEX Pattern-Details
- ✅ Cross-Exchange Wallet Lookup
- ✅ On-Chain Verification Links
- ✅ ECHTE TRADE-DATEN (keine Mocks mehr!)
- 🔍 ENHANCED DEBUG LOGGING for troubleshooting
"""

import logging
from typing import Optional, Dict, List
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException, Depends, status, Query
from pydantic import BaseModel, Field
import json

from app.core.price_movers.api.test_schemas import (
    ExchangeEnum,
    ErrorResponse,
)
from app.core.price_movers.api.dependencies import (
    get_exchange_collector,
    get_unified_collector,
    log_request,
)
from app.core.price_movers.utils.constants import (
    BLOCKCHAIN_EXPLORERS,
    BlockchainNetwork
)


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/wallet",
    tags=["wallet-details"],
    responses={
        404: {"model": ErrorResponse, "description": "Wallet Not Found"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


# ==================== SCHEMAS ====================

class BlockchainExplorerInfo(BaseModel):
    """Blockchain Explorer Information"""
    blockchain: str = Field(..., description="Blockchain name")
    explorer_name: str = Field(..., description="Explorer name (Solscan, Etherscan, etc.)")
    wallet_url: Optional[str] = Field(None, description="Direct URL to wallet")
    transaction_base_url: Optional[str] = Field(None, description="Base URL for transactions")


class WalletStatistics(BaseModel):
    """Wallet Trading Statistics"""
    total_trades: int
    buy_trades: int
    sell_trades: int
    total_volume: float
    total_value_usd: float
    avg_trade_size: float
    avg_trade_value_usd: float
    buy_sell_ratio: float
    largest_trade_usd: float
    smallest_trade_usd: float
    first_seen: datetime
    last_seen: datetime
    active_hours: float


class TradeDetail(BaseModel):
    """Individual Trade Detail"""
    timestamp: datetime
    trade_type: str
    amount: float
    price: float
    value_usd: float
    signature: Optional[str] = Field(None, description="Transaction hash/signature (DEX only)")
    explorer_url: Optional[str] = Field(None, description="Explorer URL for this transaction")


class EnhancedWalletDetailResponse(BaseModel):
    """Enhanced Wallet Details with DEX Support"""
    success: bool = True
    
    # Basic Info
    wallet_id: str = Field(..., description="Wallet ID or Address")
    wallet_address: Optional[str] = Field(None, description="Real blockchain address (DEX only)")
    wallet_type: str = Field(..., description="Wallet type (whale/bot/market_maker/etc.)")
    
    # Source Info
    data_source: str = Field(..., description="'cex' or 'dex'")
    exchange: str = Field(..., description="Exchange name")
    has_real_address: bool = Field(..., description="Has real blockchain address?")
    
    # Blockchain Info (DEX only)
    blockchain: Optional[str] = Field(None, description="Blockchain (Solana/Ethereum/etc.)")
    explorer_info: Optional[BlockchainExplorerInfo] = None
    
    # Statistics
    statistics: WalletStatistics
    
    # Recent Activity
    recent_trades: List[TradeDetail] = Field(..., description="Recent trades")
    
    # Analysis Context
    symbol: str
    time_range_hours: int


class CrossExchangeLookupRequest(BaseModel):
    """Request to lookup wallet across multiple exchanges"""
    wallet_identifier: str = Field(..., description="Wallet address or entity pattern")
    exchanges: List[str] = Field(..., description="List of exchanges to search")
    symbol: str
    time_range_hours: int = Field(default=24, ge=1, le=168)


# ==================== HELPER FUNCTIONS ====================

def get_blockchain_from_address(address: str) -> Optional[str]:
    """Detect blockchain from address format"""
    address_lower = address.lower()
    
    # Solana (Base58, typically 32-44 chars, no 0x prefix)
    if len(address) >= 32 and len(address) <= 44 and not address_lower.startswith('0x'):
        return "solana"
    
    # Ethereum/EVM (0x prefix, 42 chars)
    elif address_lower.startswith('0x') and len(address) == 42:
        return "ethereum"
    
    # Bitcoin (bc1 or 1 or 3 prefix)
    elif address_lower.startswith(('bc1', '1', '3')):
        return "bitcoin"
    
    return None


def get_explorer_info(blockchain: str) -> Optional[BlockchainExplorerInfo]:
    """Get explorer information for blockchain"""
    try:
        # Convert string to enum if needed
        if isinstance(blockchain, str):
            blockchain_enum = BlockchainNetwork(blockchain)
        else:
            blockchain_enum = blockchain
        
        explorer_config = BLOCKCHAIN_EXPLORERS.get(blockchain_enum)
        
        if not explorer_config:
            return None
        
        return BlockchainExplorerInfo(
            blockchain=blockchain_enum.value,
            explorer_name=explorer_config['name'],
            wallet_url=explorer_config['wallet_url'],
            transaction_base_url=explorer_config.get('tx_url', '').rsplit('/', 1)[0] + '/'
        )
    except Exception as e:
        logger.warning(f"Failed to get explorer info for {blockchain}: {e}")
        return None


def format_wallet_url(address: str, blockchain: str) -> Optional[str]:
    """Format wallet explorer URL"""
    try:
        blockchain_enum = BlockchainNetwork(blockchain)
        explorer_config = BLOCKCHAIN_EXPLORERS.get(blockchain_enum)
        
        if not explorer_config:
            return None
        
        return explorer_config['wallet_url'].format(address=address)
    except Exception as e:
        logger.warning(f"Failed to format wallet URL: {e}")
        return None


def format_transaction_url(signature: str, blockchain: str) -> Optional[str]:
    """Format transaction explorer URL"""
    try:
        blockchain_enum = BlockchainNetwork(blockchain)
        explorer_config = BLOCKCHAIN_EXPLORERS.get(blockchain_enum)
        
        if not explorer_config:
            return None
        
        # Use appropriate field based on blockchain
        url_template = explorer_config.get('tx_url') or explorer_config.get('signature_url')
        
        if not url_template:
            return None
        
        return url_template.format(signature=signature, hash=signature)
    except Exception as e:
        logger.warning(f"Failed to format transaction URL: {e}")
        return None


def classify_wallet_type(statistics: WalletStatistics) -> str:
    """
    Klassifiziert Wallet basierend auf Trading-Verhalten
    
    Returns: 'whale', 'market_maker', 'bot', 'retail', 'unknown'
    """
    try:
        # Whale: Hohe Volumina
        if statistics.total_value_usd > 500000:
            return "whale"
        
        # Market Maker: Hohes Trade-Volume, ausgewogene Buy/Sell Ratio
        if (statistics.total_trades > 100 and 
            0.9 <= statistics.buy_sell_ratio <= 1.1):
            return "market_maker"
        
        # Bot: Sehr viele Trades, konstante Größen
        if (statistics.total_trades > 50 and 
            statistics.avg_trade_size > 0 and
            abs(statistics.avg_trade_value_usd - statistics.largest_trade_usd) < statistics.avg_trade_value_usd * 0.5):
            return "bot"
        
        # Retail: Normale Trading-Aktivität
        if statistics.total_trades < 50:
            return "retail"
        
        return "unknown"
    except:
        return "unknown"


# ==================== ENDPOINTS ====================

@router.get(
    "/{wallet_identifier}/details",
    response_model=EnhancedWalletDetailResponse,
    status_code=status.HTTP_200_OK,
    summary="Get Enhanced Wallet Details",
    description="Vollständige Wallet-Details mit DEX-Support und Explorer-Links + DEBUG LOGGING"
)
async def get_enhanced_wallet_details(
    wallet_identifier: str,
    exchange: str = Query(..., description="Exchange (CEX or DEX)"),
    symbol: str = Query(..., description="Trading pair"),
    time_range_hours: int = Query(default=2, ge=1, le=720),  # ✅ Default 2h
    candle_timestamp: Optional[str] = Query(None, description="Candle timestamp for context"),
    timeframe_minutes: Optional[int] = Query(None, description="Candle timeframe in minutes"),
    request_id: str = Depends(log_request)
) -> EnhancedWalletDetailResponse:
    """
    Enhanced Wallet Details mit WALLET-SPEZIFISCHEM Lookup + DEBUG LOGGING
    
    Neu: Holt ALLE Transaktionen der Wallet, nicht nur Zeit-basiert!
    """
    try:
        logger.info(
            f"[{request_id}] Enhanced wallet details: "
            f"{wallet_identifier[:16]}... on {exchange} {symbol}"
        )
        
        # Detect if DEX or CEX
        is_dex = exchange.lower() in ['jupiter', 'raydium', 'orca', 'uniswap', 'pancakeswap']
        has_real_address = is_dex and not wallet_identifier.startswith(('whale_', 'bot_', 'market_maker_'))
        
        # Get unified collector
        unified_collector = await get_unified_collector()
        
        # Detect blockchain if DEX
        blockchain = None
        explorer_info = None
        
        if has_real_address:
            blockchain = get_blockchain_from_address(wallet_identifier)
            if blockchain:
                explorer_info_obj = get_explorer_info(blockchain)
                
                if explorer_info_obj:
                    wallet_url = format_wallet_url(wallet_identifier, blockchain)
                    explorer_info = BlockchainExplorerInfo(
                        blockchain=explorer_info_obj.blockchain,
                        explorer_name=explorer_info_obj.explorer_name,
                        wallet_url=wallet_url,
                        transaction_base_url=explorer_info_obj.transaction_base_url
                    )
        
        # ==================== FETCH WALLET-SPECIFIC TRADES ====================
        
        logger.info(f"🎯 NEW APPROACH: Fetching wallet-specific trades for {wallet_identifier[:16]}...")
        
        wallet_trades = []
        all_trades = []
        
        try:
            # ✅ Strategy 1: Wallet-specific lookup (if available)
            if is_dex and hasattr(unified_collector, 'helius_collector'):
                helius = unified_collector.helius_collector
                
                # Resolve token address from symbol
                token_address = None
                if '/' in symbol:
                    quote_token = symbol.split('/', 1)[1]
                    token_mapping = {
                        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
                        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                    }
                    token_address = token_mapping.get(quote_token.upper())
                
                logger.info(f"🔍 Using wallet-specific lookup (target token: {token_address[:16] if token_address else 'All'}...)")
                
                # 🔍 DEBUG: Log token resolution
                logger.info(
                    f"🔍 TOKEN RESOLUTION DEBUG:\n"
                    f"   Symbol: {symbol}\n"
                    f"   Quote Token: {quote_token if '/' in symbol else 'N/A'}\n"
                    f"   Resolved Token Address: {token_address if token_address else 'NOT FOUND'}\n"
                    f"   Expected USDT: Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
                )
                
                # Call new wallet-specific method
                wallet_result = await helius.fetch_wallet_trades(
                    wallet_address=wallet_identifier,
                    target_token_address=token_address,
                    limit=1000
                )
                
                wallet_trades = wallet_result.get('pair_trades', [])
                other_activities = wallet_result.get('other_activities', [])
                token_summary = wallet_result.get('token_summary', {})
                
                logger.info(
                    f"✅ Wallet-specific lookup results:\n"
                    f"   Total Transactions: {wallet_result.get('total_transactions', 0)}\n"
                    f"   {symbol} Trades: {len(wallet_trades)}\n"
                    f"   Other Activities: {len(other_activities)}\n"
                    f"   Unique Tokens: {len(token_summary)}"
                )
                
                # 🔍 DEBUG: Show token summary
                if token_summary:
                    logger.info(f"🔍 Token Summary (top 10):")
                    sorted_tokens = sorted(token_summary.items(), key=lambda x: x[1], reverse=True)[:10]
                    for token, count in sorted_tokens:
                        logger.info(f"   - {token}... : {count} transfers")
                
                # 🔍 DEBUG: Sample trades found
                if wallet_trades:
                    logger.info(f"🔍 SAMPLE TRADES FOUND (first 3):")
                    for i, trade in enumerate(wallet_trades[:3]):
                        logger.info(
                            f"   Trade #{i+1}:\n"
                            f"      Type: {trade.get('trade_type')}\n"
                            f"      Amount: {trade.get('amount')}\n"
                            f"      Price: {trade.get('price')}\n"
                            f"      Wallet: {trade.get('wallet_address', '')[:16]}...\n"
                            f"      Timestamp: {trade.get('timestamp')}"
                        )
                else:
                    logger.warning(
                        f"⚠️ NO TRADES FOUND!\n"
                        f"   Wallet: {wallet_identifier[:16]}...\n"
                        f"   Target Token: {token_address[:16] if token_address else 'None'}...\n"
                        f"   Other Activities: {len(other_activities)}"
                    )
                
            else:
                # ✅ Strategy 2: Fallback to time-based (optimized)
                logger.info(f"🔄 Fallback: Using time-based lookup with optimized time ranges")
                
                time_ranges = [
                    ('1h', timedelta(hours=1)),
                    ('2h', timedelta(hours=2)),
                    ('6h', timedelta(hours=6)),
                ]
                
                for range_label, time_delta in time_ranges:
                    end_time = datetime.now(timezone.utc)
                    start_time = end_time - time_delta
                    
                    # Use candle timestamp if provided
                    if candle_timestamp and timeframe_minutes:
                        try:
                            candle_time = datetime.fromisoformat(candle_timestamp.replace('Z', '+00:00'))
                            start_time = candle_time - timedelta(minutes=timeframe_minutes * 2)
                            end_time = candle_time + timedelta(minutes=timeframe_minutes * 2)
                            logger.info(f"🎯 Using candle-based time: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
                        except:
                            pass
                    
                    logger.info(f"🔍 Trying {range_label}: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
                    
                    trades_result = await unified_collector.fetch_trades(
                        exchange=exchange.lower(),
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        limit=10000
                    )
                    
                    all_trades = trades_result.get('trades', [])
                    logger.info(f"📊 Fetched {len(all_trades)} total trades in {range_label}")
                    
                    # 🔍 DEBUG: Sample wallets in all_trades
                    if all_trades:
                        sample_wallets = set()
                        for trade in all_trades[:20]:
                            w = (
                                trade.get('wallet_address') or 
                                trade.get('wallet_id') or
                                trade.get('fromUserAccount') or
                                trade.get('toUserAccount') or
                                'unknown'
                            )
                            if w != 'unknown':
                                sample_wallets.add(w[:16] + '...')
                        
                        logger.info(f"🔍 Sample wallets in {len(all_trades)} trades (first 10):")
                        for sw in list(sample_wallets)[:10]:
                            logger.info(f"   - {sw}")
                        
                        logger.info(f"🔍 Looking for: {wallet_identifier[:16]}...")
                    
                    # Filter for wallet
                    wallet_trades = []
                    for trade in all_trades:
                        trade_wallet = (
                            trade.get('wallet_address') or 
                            trade.get('wallet_id') or
                            trade.get('fromUserAccount') or
                            trade.get('toUserAccount')
                        )
                        
                        # 🔍 DEBUG: Log matching attempts
                        if trade_wallet:
                            logger.debug(f"Comparing: {trade_wallet[:16]}... == {wallet_identifier[:16]}...")
                        
                        if trade_wallet == wallet_identifier:
                            wallet_trades.append(trade)
                            logger.info(f"✅ MATCH FOUND! Trade wallet: {trade_wallet[:16]}...")
                    
                    logger.info(f"✅ Found {len(wallet_trades)} trades for wallet in {range_label}")
                    
                    if len(wallet_trades) >= 5:
                        logger.info(f"✅ Sufficient trades in {range_label}, stopping search")
                        break
            
            # ✅ DEBUG: Why no trades?
            if len(wallet_trades) == 0 and len(all_trades) > 0:
                logger.warning(
                    f"\n{'='*80}\n"
                    f"⚠️ WALLET TRADE FILTERING FAILED\n"
                    f"   Wallet: {wallet_identifier[:20]}...\n"
                    f"   Total trades available: {len(all_trades)}\n"
                    f"   Trades for this wallet: 0\n"
                    f"{'='*80}"
                )
                
                # Sample wallets
                sample_wallets = set()
                for trade in all_trades[:10]:
                    w = (
                        trade.get('wallet_address') or 
                        trade.get('wallet_id') or 
                        'unknown'
                    )
                    if w != 'unknown':
                        sample_wallets.add(w[:16] + '...')
                
                logger.warning(f"📋 Sample wallets in {len(all_trades)} trades:")
                for sw in list(sample_wallets)[:5]:
                    logger.warning(f"   - {sw}")
                
                logger.warning(f"🔍 Looking for: {wallet_identifier[:16]}...")
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch trades: {e}", exc_info=True)
            wallet_trades = []
        
        # ==================== CALCULATE STATISTICS ====================
        
        if wallet_trades:
            # Detect trade_type field (can be 'side' or 'trade_type')
            buy_trades = []
            sell_trades = []
            
            for t in wallet_trades:
                tt = (t.get('trade_type') or t.get('side', '')).lower()
                if tt == 'buy':
                    buy_trades.append(t)
                elif tt == 'sell':
                    sell_trades.append(t)
            
            total_volume = sum(t.get('amount', 0) for t in wallet_trades)
            total_value_usd = sum(t.get('amount', 0) * t.get('price', 0) for t in wallet_trades)
            
            trade_values = [t.get('amount', 0) * t.get('price', 0) for t in wallet_trades if t.get('amount') and t.get('price')]
            largest_trade = max(trade_values) if trade_values else 0.0
            smallest_trade = min(trade_values) if trade_values else 0.0
            
            # Time range
            timestamps = [t.get('timestamp') for t in wallet_trades if t.get('timestamp')]
            if timestamps:
                parsed_timestamps = []
                for ts in timestamps:
                    if isinstance(ts, str):
                        try:
                            parsed_timestamps.append(datetime.fromisoformat(ts.replace('Z', '+00:00')))
                        except:
                            pass
                    elif isinstance(ts, datetime):
                        parsed_timestamps.append(ts)
                
                if parsed_timestamps:
                    first_seen = min(parsed_timestamps)
                    last_seen = max(parsed_timestamps)
                    active_hours = (last_seen - first_seen).total_seconds() / 3600
                else:
                    first_seen = datetime.now(timezone.utc) - timedelta(hours=time_range_hours)
                    last_seen = datetime.now(timezone.utc)
                    active_hours = time_range_hours
            else:
                first_seen = datetime.now(timezone.utc) - timedelta(hours=time_range_hours)
                last_seen = datetime.now(timezone.utc)
                active_hours = time_range_hours
            
            buy_sell_ratio = len(buy_trades) / max(len(sell_trades), 1)
            
            statistics = WalletStatistics(
                total_trades=len(wallet_trades),
                buy_trades=len(buy_trades),
                sell_trades=len(sell_trades),
                total_volume=total_volume,
                total_value_usd=total_value_usd,
                avg_trade_size=total_volume / max(len(wallet_trades), 1),
                avg_trade_value_usd=total_value_usd / max(len(wallet_trades), 1),
                buy_sell_ratio=buy_sell_ratio,
                largest_trade_usd=largest_trade,
                smallest_trade_usd=smallest_trade,
                first_seen=first_seen,
                last_seen=last_seen,
                active_hours=active_hours
            )
            
            # Recent trades
            recent_wallet_trades = sorted(
                wallet_trades,
                key=lambda t: t.get('timestamp', datetime.min),
                reverse=True
            )[:10]
            
            recent_trades = []
            for trade in recent_wallet_trades:
                ts = trade.get('timestamp')
                if isinstance(ts, str):
                    try:
                        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except:
                        ts = datetime.now(timezone.utc)
                elif not isinstance(ts, datetime):
                    ts = datetime.now(timezone.utc)
                
                signature = trade.get('signature') or trade.get('transaction_hash') or trade.get('id')
                
                explorer_url = None
                if has_real_address and blockchain and signature:
                    explorer_url = format_transaction_url(signature, blockchain)
                
                trade_type = (trade.get('trade_type') or trade.get('side', 'unknown')).lower()
                
                recent_trades.append(TradeDetail(
                    timestamp=ts,
                    trade_type=trade_type,
                    amount=float(trade.get('amount', 0)),
                    price=float(trade.get('price', 0)),
                    value_usd=float(trade.get('amount', 0)) * float(trade.get('price', 0)),
                    signature=signature if has_real_address else None,
                    explorer_url=explorer_url
                ))
            
            wallet_type = classify_wallet_type(statistics)
            
        else:
            logger.warning(f"No trades found for wallet {wallet_identifier}")
            
            statistics = WalletStatistics(
                total_trades=0,
                buy_trades=0,
                sell_trades=0,
                total_volume=0.0,
                total_value_usd=0.0,
                avg_trade_size=0.0,
                avg_trade_value_usd=0.0,
                buy_sell_ratio=0.0,
                largest_trade_usd=0.0,
                smallest_trade_usd=0.0,
                first_seen=datetime.now(timezone.utc) - timedelta(hours=time_range_hours),
                last_seen=datetime.now(timezone.utc),
                active_hours=0.0
            )
            
            recent_trades = []
            wallet_type = "unknown"
        
        # Build response
        response = EnhancedWalletDetailResponse(
            wallet_id=wallet_identifier,
            wallet_address=wallet_identifier if has_real_address else None,
            wallet_type=wallet_type,
            data_source="dex" if is_dex else "cex",
            exchange=exchange,
            has_real_address=has_real_address,
            blockchain=blockchain,
            explorer_info=explorer_info,
            statistics=statistics,
            recent_trades=recent_trades,
            symbol=symbol,
            time_range_hours=time_range_hours
        )
        
        logger.info(
            f"[{request_id}] ✅ Wallet details loaded: "
            f"{'DEX' if is_dex else 'CEX'} wallet, "
            f"{statistics.total_trades} trades, "
            f"${statistics.total_value_usd:,.2f} volume"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] ❌ Wallet details error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load wallet details: {str(e)}"
        )


@router.post(
    "/lookup/cross-exchange",
    summary="Cross-Exchange Wallet Lookup",
    description="Sucht Wallet über mehrere Exchanges hinweg"
)
async def cross_exchange_wallet_lookup(
    request: CrossExchangeLookupRequest,
    request_id: str = Depends(log_request)
):
    """
    ## 🔄 Cross-Exchange Wallet Lookup
    
    Sucht nach einem Wallet über mehrere Exchanges hinweg.
    """
    try:
        logger.info(
            f"[{request_id}] Cross-exchange lookup: "
            f"{request.wallet_identifier} across {request.exchanges}"
        )
        
        # TODO: Implement cross-exchange lookup
        
        return {
            "success": True,
            "wallet_identifier": request.wallet_identifier,
            "exchanges_checked": request.exchanges,
            "active_on": [],
            "statistics_by_exchange": {},
            "conclusion": "Cross-exchange lookup not implemented yet"
        }
        
    except Exception as e:
        logger.error(f"[{request_id}] Cross-exchange lookup error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Cross-exchange lookup failed: {str(e)}"
        )


@router.get(
    "/verify/{wallet_address}",
    summary="Verify Blockchain Address",
    description="Verifiziert eine Blockchain-Adresse und gibt Explorer-Links"
)
async def verify_blockchain_address(
    wallet_address: str,
    request_id: str = Depends(log_request)
):
    """
    ## ✅ Verify Blockchain Address
    
    Verifiziert eine Blockchain-Adresse und gibt:
    - Blockchain-Typ (Solana/Ethereum/etc.)
    - Explorer-URLs
    - Adress-Format-Validierung
    """
    try:
        logger.info(f"[{request_id}] Verify address: {wallet_address}")
        
        # Detect blockchain
        blockchain = get_blockchain_from_address(wallet_address)
        
        if not blockchain:
            return {
                "success": False,
                "is_valid": False,
                "error": "Unknown blockchain format",
                "wallet_address": wallet_address
            }
        
        # Get explorer info
        explorer_info = get_explorer_info(blockchain)
        
        # Format URLs
        wallet_url = format_wallet_url(wallet_address, blockchain) if explorer_info else None
        
        return {
            "success": True,
            "is_valid": True,
            "wallet_address": wallet_address,
            "blockchain": blockchain,
            "address_format": "base58" if blockchain == "solana" else "hex",
            "explorer_info": {
                "name": explorer_info.explorer_name if explorer_info else None,
                "wallet_url": wallet_url,
                "transaction_base_url": explorer_info.transaction_base_url if explorer_info else None
            } if explorer_info else None
        }
        
    except Exception as e:
        logger.error(f"[{request_id}] Address verification error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Address verification failed: {str(e)}"
        )


# Export Router
__all__ = ['router']
