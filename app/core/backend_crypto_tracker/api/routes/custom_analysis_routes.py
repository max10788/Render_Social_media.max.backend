# app/core/backend_crypto_tracker/api/routes/custom_analysis_routes.py
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, validator
from typing import Optional
from datetime import datetime
import re

router = APIRouter(prefix="/api/analyze", tags=["custom-analysis"])

class CustomAnalysisRequest(BaseModel):
    token_address: str
    chain: str
    
    @validator('chain')
    def validate_chain(cls, v):
        allowed_chains = ['ethereum', 'bsc', 'solana', 'sui']
        if v.lower() not in allowed_chains:
            raise ValueError(f'Chain must be one of: {allowed_chains}')
        return v.lower()
    
    @validator('token_address')
    def validate_address(cls, v, values):
        chain = values.get('chain', '').lower()
        
        if chain in ['ethereum', 'bsc']:
            # Ethereum/BSC: 0x + 40 hex characters
            if not re.match(r'^0x[a-fA-F0-9]{40}$', v):
                raise ValueError('Invalid Ethereum/BSC address format')
        elif chain == 'solana':
            # Solana: Base58 string, 32-44 characters
            if not re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', v):
                raise ValueError('Invalid Solana address format')
        elif chain == 'sui':
            # Sui: 0x + 64 hex characters
            if not re.match(r'^0x[a-fA-F0-9]{64}$', v):
                raise ValueError('Invalid Sui address format')
        
        return v

class CustomAnalysisResponse(BaseModel):
    success: bool
    token_address: str
    chain: str
    analysis_result: Optional[dict] = None
    error_message: Optional[str] = None
    analyzed_at: datetime

@router.post("/custom", response_model=CustomAnalysisResponse)
async def analyze_custom_token(request: CustomAnalysisRequest):
    """
    Analysiert einen benutzerdefinierten Token basierend auf Adresse und Chain
    """
    try:
        # Import des Analyzers (sollte als Dependency injiziert werden)
        from app.core.backend_crypto_tracker.scanner.low_cap_analyzer import LowCapAnalyzer
        from app.core.backend_crypto_tracker.processor.database.models.manager import DatabaseManager
        
        # Initialisierung (in echtem Setup über Dependency Injection)
        LowCapAnalyzer = TokenAnalyzer
        
        # Analyse durchführen
        result = await analyzer.analyze_custom_token(
            token_address=request.token_address,
            chain=request.chain
        )
        
        return CustomAnalysisResponse(
            success=True,
            token_address=request.token_address,
            chain=request.chain,
            analysis_result=result,
            analyzed_at=datetime.utcnow()
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return CustomAnalysisResponse(
            success=False,
            token_address=request.token_address,
            chain=request.chain,
            error_message=str(e),
            analyzed_at=datetime.utcnow()
        )
