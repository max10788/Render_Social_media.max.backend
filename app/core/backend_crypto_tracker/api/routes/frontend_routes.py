from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime
import logging

# Import existing controllers and models
from app.core.backend_crypto_tracker.api.controllers.transaction_controller import transaction_controller
from app.core.backend_crypto_tracker.api.controllers.scanner_controller import ScannerController
from app.core.backend_crypto_tracker.processor.database.models.manager import DatabaseManager
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException

logger = get_logger(__name__)

router = APIRouter(prefix="/api", tags=["frontend-compatibility"])

# Pydantic models for frontend compatibility
class Transaction(BaseModel):
    hash: str
    amount: float
    fee: float
    to_address: str
    from_address: str
    timestamp: int
    currency: str
    direction: str
    block_number: Optional[int] = None
    gas_price: Optional[float] = None

class TrackingResult(BaseModel):
    transactions: List[Transaction]
    source_currency: str
    target_currency: str
    start_transaction: str
    transactions_count: int
    tracking_timestamp: int
    exchange_rate: Optional[float] = None

class TokenData(BaseModel):
    address: str
    name: str
    symbol: str
    chain: str
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    liquidity: Optional[float] = None
    holders_count: Optional[int] = None
    contract_verified: Optional[bool] = None
    creation_date: Optional[str] = None
    last_analyzed: Optional[str] = None
    token_score: Optional[float] = None

class WalletAnalysis(BaseModel):
    address: str
    risk_score: float
    entity_type: str
    labels: List[str]
    confidence: float
    transaction_count: int
    total_value: float
    first_activity: Optional[str] = None
    last_activity: Optional[str] = None
    associated_entities: List[str]
    compliance_flags: List[str]

class DiscoveryParams(BaseModel):
    chain: str
    maxMarketCap: Optional[float] = None
    minVolume: Optional[float] = None
    hoursAgo: Optional[int] = None
    limit: Optional[int] = None

# Database dependency
async def get_db_manager():
    db_manager = DatabaseManager()
    await db_manager.initialize()
    try:
        yield db_manager
    finally:
        await db_manager.close()

@router.post("/track-transaction-chain", response_model=TrackingResult)
async def track_transaction_chain(
    start_tx_hash: str,
    target_currency: str,
    num_transactions: int = 10
):
    """Track a transaction chain - frontend compatibility endpoint"""
    try:
        # Import here to avoid circular imports
        from app.core.backend_crypto_tracker.config.database import get_db
        from sqlalchemy.orm import Session
        
        # Get database session
        db = next(get_db())
        
        # Call the existing transaction controller
        result = await transaction_controller.get_transaction(start_tx_hash, target_currency, db)
        
        # Transform to expected format
        transactions = []
        if result:
            transactions = [Transaction(
                hash=result.get("hash", ""),
                amount=result.get("value", 0),
                fee=result.get("fee", 0),
                to_address=result.get("to_address", ""),
                from_address=result.get("from_address", ""),
                timestamp=int(result.get("timestamp", datetime.now().timestamp())),
                currency=target_currency,
                direction="out"
            )]
        
        return TrackingResult(
            transactions=transactions,
            source_currency=target_currency,
            target_currency=target_currency,
            start_transaction=start_tx_hash,
            transactions_count=len(transactions),
            tracking_timestamp=int(datetime.now().timestamp())
        )
    except Exception as e:
        logger.error(f"Error tracking transaction chain: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/discover-tokens", response_model=List[TokenData])
async def discover_tokens(params: DiscoveryParams):
    """Discover tokens - frontend compatibility endpoint"""
    try:
        # Get database manager
        db_manager = DatabaseManager()
        await db_manager.initialize()
        
        # Call existing token discovery functionality
        tokens = await db_manager.get_tokens(
            limit=params.limit or 100,
            chain=params.chain,
            max_market_cap=params.maxMarketCap
        )
        
        # Transform to expected format
        result = []
        for token in tokens:
            result.append(TokenData(
                address=token.get("address", ""),
                name=token.get("name", ""),
                symbol=token.get("symbol", ""),
                chain=token.get("chain", params.chain),
                market_cap=token.get("market_cap"),
                volume_24h=token.get("volume_24h"),
                liquidity=token.get("liquidity"),
                holders_count=token.get("holders_count"),
                contract_verified=token.get("contract_verified", False),
                creation_date=token.get("creation_date"),
                last_analyzed=token.get("last_analyzed"),
                token_score=token.get("token_score")
            ))
        
        await db_manager.close()
        return result
    except Exception as e:
        logger.error(f"Error discovering tokens: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/discover-trending-tokens", response_model=List[TokenData])
async def discover_trending_tokens(params: DiscoveryParams):
    """Discover trending tokens - frontend compatibility endpoint"""
    try:
        # Similar to discover_tokens but with trending logic
        # For now, just call the same function with different parameters
        return await discover_tokens(params)
    except Exception as e:
        logger.error(f"Error discovering trending tokens: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analyze-wallet/{address}", response_model=WalletAnalysis)
async def analyze_wallet(address: str):
    """Analyze wallet - frontend compatibility endpoint"""
    try:
        # Import here to avoid circular imports
        from app.core.backend_crypto_tracker.config.database import get_db
        from sqlalchemy.orm import Session
        
        # Get database session
        db = next(get_db())
        
        # Call existing wallet analysis functionality
        # For now, create a placeholder response
        return WalletAnalysis(
            address=address,
            risk_score=0.5,
            entity_type="unknown",
            labels=[],
            confidence=0.5,
            transaction_count=0,
            total_value=0.0,
            associated_entities=[],
            compliance_flags=[]
        )
    except Exception as e:
        logger.error(f"Error analyzing wallet: {e}")
        raise HTTPException(status_code=500, detail=str(e))
