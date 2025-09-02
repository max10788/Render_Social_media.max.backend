# app/core/backend_crypto_tracker/api/routes/token_routes.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from app.core.backend_crypto_tracker.processor.database.models.manager import DatabaseManager
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import DatabaseException

logger = get_logger(__name__)
router = APIRouter(prefix="/tokens", tags=["tokens"])

# Pydantic Models für API-Antworten
class TokenResponse(BaseModel):
    id: int
    address: str
    name: str
    symbol: str
    chain: str
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    liquidity: Optional[float] = None
    holders_count: Optional[int] = None
    contract_verified: bool
    creation_date: Optional[datetime] = None
    last_analyzed: Optional[datetime] = None
    token_score: Optional[float] = None
    
    class Config:
        orm_mode = True

class WalletAnalysisResponse(BaseModel):
    wallet_address: str
    wallet_type: str
    balance: float
    percentage_of_supply: float
    transaction_count: int
    risk_score: float
    
    class Config:
        orm_mode = True

class TokenDetailResponse(TokenResponse):
    wallet_analyses: List[WalletAnalysisResponse] = []
    
class TokenAnalysisRequest(BaseModel):
    token_address: str = Field(..., description="Token contract address")
    chain: str = Field(..., description="Blockchain (ethereum, bsc, solana, sui)")

class TokenAnalysisResponse(BaseModel):
    token_info: Dict[str, Any]
    score: float
    metrics: Dict[str, Any]
    risk_flags: List[str]
    wallet_analysis: Dict[str, Any]

# Dependency für Datenbank-Manager
async def get_db_manager():
    db_manager = DatabaseManager()
    await db_manager.initialize()
    try:
        yield db_manager
    finally:
        await db_manager.close()

@router.get("/", response_model=List[TokenResponse])
async def get_tokens(
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of tokens to return"),
    min_score: float = Query(0, ge=0, le=100, description="Minimum token score"),
    chain: Optional[str] = Query(None, description="Filter by blockchain"),
    search: Optional[str] = Query(None, description="Search in token name or symbol"),
    max_market_cap: Optional[float] = Query(None, description="Maximum market cap"),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get a list of analyzed tokens"""
    try:
        tokens = await db_manager.get_tokens(
            limit=limit,
            min_score=min_score,
            chain=chain,
            search=search,
            max_market_cap=max_market_cap
        )
        return tokens
    except DatabaseException as e:
        logger.error(f"Database error in get_tokens: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        logger.error(f"Unexpected error in get_tokens: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{token_id}", response_model=TokenResponse)
async def get_token_by_id(
    token_id: int = Path(..., description="Token ID"),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get a token by ID"""
    try:
        # Hier müsste eine Methode im DatabaseManager implementiert werden
        # Für jetzt ein Platzhalter
        raise HTTPException(status_code=501, detail="Not implemented yet")
    except DatabaseException as e:
        logger.error(f"Database error in get_token_by_id: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        logger.error(f"Unexpected error in get_token_by_id: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/address/{address}", response_model=TokenResponse)
async def get_token_by_address(
    address: str = Path(..., description="Token address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get a token by address and chain"""
    try:
        token = await db_manager.get_token_by_address(address, chain)
        if not token:
            raise HTTPException(status_code=404, detail="Token not found")
        return token
    except DatabaseException as e:
        logger.error(f"Database error in get_token_by_address: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_token_by_address: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{address}/wallets", response_model=List[WalletAnalysisResponse])
async def get_token_wallets(
    address: str = Path(..., description="Token address"),
    chain: str = Query(..., description="Blockchain (ethereum, bsc, solana, sui)"),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get wallet analysis for a token"""
    try:
        # Hier müsste eine Methode im DatabaseManager implementiert werden
        # Für jetzt ein Platzhalter
        raise HTTPException(status_code=501, detail="Not implemented yet")
    except DatabaseException as e:
        logger.error(f"Database error in get_token_wallets: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_token_wallets: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/analyze", response_model=TokenAnalysisResponse)
async def analyze_token(
    request: TokenAnalysisRequest,
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Analyze a custom token"""
    try:
        from scanner.token_analyzer import TokenAnalyzer
        
        async with TokenAnalyzer() as analyzer:
            analysis_result = await analyzer.analyze_custom_token(
                request.token_address, 
                request.chain
            )
            
            # Speichere die Analyse in der Datenbank
            await db_manager.save_custom_analysis({
                'token_address': request.token_address,
                'chain': request.chain,
                'token_name': analysis_result['token_info']['name'],
                'token_symbol': analysis_result['token_info']['symbol'],
                'market_cap': analysis_result['token_info']['market_cap'],
                'volume_24h': analysis_result['token_info']['volume_24h'],
                'liquidity': analysis_result['token_info']['liquidity'],
                'holders_count': analysis_result['token_info']['holders_count'],
                'total_score': analysis_result['score'],
                'metrics': analysis_result['metrics'],
                'risk_flags': analysis_result['risk_flags']
            })
            
            return analysis_result
    except Exception as e:
        logger.error(f"Error analyzing token {request.token_address}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to analyze token: {str(e)}")

@router.get("/analysis/history", response_model=List[Dict[str, Any]])
async def get_analysis_history(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    session_id: Optional[str] = Query(None, description="Filter by session ID"),
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of analyses to return"),
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get history of custom token analyses"""
    try:
        history = await db_manager.get_custom_analysis_history(
            user_id=user_id,
            session_id=session_id,
            limit=limit
        )
        return history
    except DatabaseException as e:
        logger.error(f"Database error in get_analysis_history: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        logger.error(f"Unexpected error in get_analysis_history: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/statistics/chains", response_model=Dict[str, Dict[str, Any]])
async def get_chain_statistics(
    db_manager: DatabaseManager = Depends(get_db_manager)
):
    """Get statistics for different blockchains"""
    try:
        stats = await db_manager.get_chain_statistics()
        return stats
    except DatabaseException as e:
        logger.error(f"Database error in get_chain_statistics: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        logger.error(f"Unexpected error in get_chain_statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
