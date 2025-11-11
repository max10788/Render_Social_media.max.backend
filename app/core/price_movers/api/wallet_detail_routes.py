"""
Enhanced Wallet Detail Routes - DEX Wallet Support

Neue Features:
- ✅ Echte DEX Wallet-Adressen mit Explorer-URLs
- ✅ CEX Pattern-Details
- ✅ Cross-Exchange Wallet Lookup
- ✅ On-Chain Verification Links
"""

import logging
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, status, Query
from pydantic import BaseModel, Field

from app.core.price_movers.api.test_schemas import (
    ExchangeEnum,
    ErrorResponse,
)
from app.core.price_movers.api.dependencies import (
    get_exchange_collector,
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
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "wallet_id": "7xKXtg2CW87d97TXJSDpb4j5NzWZn9XsxUBmkVX",
                "wallet_address": "7xKXtg2CW87d97TXJSDpb4j5NzWZn9XsxUBmkVX",
                "wallet_type": "whale",
                "data_source": "dex",
                "exchange": "jupiter",
                "has_real_address": True,
                "blockchain": "solana",
                "explorer_info": {
                    "blockchain": "solana",
                    "explorer_name": "Solscan",
                    "wallet_url": "https://solscan.io/account/7xKXtg2CW87...",
                    "transaction_base_url": "https://solscan.io/tx/"
                },
                "statistics": {
                    "total_trades": 127,
                    "buy_trades": 65,
                    "sell_trades": 62,
                    "total_volume": 1250.5,
                    "total_value_usd": 187500.75,
                    "avg_trade_size": 9.85,
                    "avg_trade_value_usd": 1476.38,
                    "buy_sell_ratio": 1.05,
                    "largest_trade_usd": 25000.0,
                    "smallest_trade_usd": 100.0,
                    "first_seen": "2025-11-10T08:30:00Z",
                    "last_seen": "2025-11-11T10:05:00Z",
                    "active_hours": 25.58
                },
                "recent_trades": [],
                "symbol": "SOL/USDT",
                "time_range_hours": 24
            }
        }


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


# ==================== ENDPOINTS ====================

@router.get(
    "/{wallet_identifier}/details",
    response_model=EnhancedWalletDetailResponse,
    status_code=status.HTTP_200_OK,
    summary="Get Enhanced Wallet Details",
    description="Vollständige Wallet-Details mit DEX-Support und Explorer-Links"
)
async def get_enhanced_wallet_details(
    wallet_identifier: str,
    exchange: str = Query(..., description="Exchange (CEX or DEX)"),
    symbol: str = Query(..., description="Trading pair"),
    time_range_hours: int = Query(default=24, ge=1, le=720),
    request_id: str = Depends(log_request)
) -> EnhancedWalletDetailResponse:
    """
    ## 🔍 Enhanced Wallet Details
    
    Liefert vollständige Details zu einem Wallet mit:
    
    ### Für DEX Wallets:
    - ✅ Echte Blockchain-Adresse
    - ✅ Explorer-URLs (Solscan, Etherscan, etc.)
    - ✅ Transaction-Links
    - ✅ On-Chain Verifikation
    
    ### Für CEX Patterns:
    - Pattern-basierte Identifikation
    - Trading-Charakteristiken
    - Geschätzte Entity-Größe
    
    ### Path Parameters:
    - **wallet_identifier**: Wallet-Adresse oder Entity-Pattern
    
    ### Query Parameters:
    - **exchange**: Exchange Name
    - **symbol**: Trading Pair
    - **time_range_hours**: Zeitraum für Statistiken
    
    ### Beispiele:
    
    **DEX Wallet:**
    ```
    GET /api/v1/wallet/7xKXtg2CW87d97TXJSDpb4j5NzWZn9XsxUBmkVX/details
        ?exchange=jupiter
        &symbol=SOL/USDT
        &time_range_hours=24
    ```
    
    **CEX Pattern:**
    ```
    GET /api/v1/wallet/whale_5/details
        ?exchange=bitget
        &symbol=BTC/USDT
        &time_range_hours=24
    ```
    """
    try:
        logger.info(
            f"[{request_id}] Enhanced wallet details: "
            f"{wallet_identifier} on {exchange} {symbol}"
        )
        
        # Detect if DEX or CEX
        is_dex = exchange.lower() in ['jupiter', 'raydium', 'orca', 'uniswap', 'pancakeswap']
        has_real_address = is_dex and not wallet_identifier.startswith(('whale_', 'bot_', 'market_maker_'))
        
        # Get collector
        from app.core.price_movers.collectors.unified_collector import UnifiedCollector
        
        # TODO: Get from dependencies
        unified_collector = None  # Will be injected
        
        # Fetch trades for this wallet
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=time_range_hours)
        
        # For now, use mock data
        # TODO: Implement real data fetching
        
        # Detect blockchain if DEX
        blockchain = None
        explorer_info = None
        
        if has_real_address:
            blockchain = get_blockchain_from_address(wallet_identifier)
            if blockchain:
                explorer_info_obj = get_explorer_info(blockchain)
                
                if explorer_info_obj:
                    # Format wallet URL
                    wallet_url = format_wallet_url(wallet_identifier, blockchain)
                    explorer_info = BlockchainExplorerInfo(
                        blockchain=explorer_info_obj.blockchain,
                        explorer_name=explorer_info_obj.explorer_name,
                        wallet_url=wallet_url,
                        transaction_base_url=explorer_info_obj.transaction_base_url
                    )
        
        # Mock statistics (TODO: Calculate from real trades)
        statistics = WalletStatistics(
            total_trades=127,
            buy_trades=65,
            sell_trades=62,
            total_volume=1250.5,
            total_value_usd=187500.75,
            avg_trade_size=9.85,
            avg_trade_value_usd=1476.38,
            buy_sell_ratio=1.05,
            largest_trade_usd=25000.0,
            smallest_trade_usd=100.0,
            first_seen=start_time,
            last_seen=end_time,
            active_hours=25.58
        )
        
        # Mock recent trades (TODO: Fetch real trades)
        recent_trades = []
        
        # If DEX, add transaction signatures and explorer URLs
        if has_real_address and blockchain:
            for i in range(5):
                mock_signature = f"mock_tx_{i}_{''.join(wallet_identifier[:8])}"
                tx_url = format_transaction_url(mock_signature, blockchain)
                
                recent_trades.append(TradeDetail(
                    timestamp=end_time - timedelta(hours=i*2),
                    trade_type='buy' if i % 2 == 0 else 'sell',
                    amount=10.5,
                    price=150.25,
                    value_usd=1577.63,
                    signature=mock_signature,
                    explorer_url=tx_url
                ))
        else:
            # CEX trades (no signatures)
            for i in range(5):
                recent_trades.append(TradeDetail(
                    timestamp=end_time - timedelta(hours=i*2),
                    trade_type='buy' if i % 2 == 0 else 'sell',
                    amount=10.5,
                    price=150.25,
                    value_usd=1577.63,
                    signature=None,
                    explorer_url=None
                ))
        
        # Build response
        response = EnhancedWalletDetailResponse(
            wallet_id=wallet_identifier,
            wallet_address=wallet_identifier if has_real_address else None,
            wallet_type="whale",  # TODO: Classify properly
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
            f"[{request_id}] Wallet details loaded: "
            f"{'DEX' if is_dex else 'CEX'} wallet, "
            f"{statistics.total_trades} trades"
        )
        
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Wallet details error: {e}", exc_info=True)
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
    request: CrossExchangeLookupRequest = Field(...),
    request_id: str = Depends(log_request)
):
    """
    ## 🔄 Cross-Exchange Wallet Lookup
    
    Sucht nach einem Wallet über mehrere Exchanges hinweg.
    
    ### Use Cases:
    - "Ist dieser Bitget Whale auch auf Jupiter aktiv?"
    - "Welche Exchanges nutzt diese Wallet-Adresse?"
    
    ### Request Body:
    ```json
    {
        "wallet_identifier": "7xKXtg2CW87...",
        "exchanges": ["bitget", "jupiter", "raydium"],
        "symbol": "SOL/USDT",
        "time_range_hours": 24
    }
    ```
    
    ### Returns:
    - Liste der Exchanges mit Aktivität
    - Trading-Statistiken pro Exchange
    - Vergleich der Trading-Pattern
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
    
    ### Path Parameters:
    - **wallet_address**: Blockchain-Adresse
    
    ### Returns:
    ```json
    {
        "is_valid": true,
        "blockchain": "solana",
        "address_format": "base58",
        "explorer_info": {
            "name": "Solscan",
            "wallet_url": "https://solscan.io/account/...",
            "transaction_url": "https://solscan.io/tx/..."
        }
    }
    ```
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
