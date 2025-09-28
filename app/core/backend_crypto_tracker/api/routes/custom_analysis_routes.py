from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator
from typing import Optional
from datetime import datetime
import re
import logging

# Importieren Sie die Hilfsfunktionen für JSON-Bereinigung
from app.core.backend_crypto_tracker.utils.json_helpers import sanitize_value, SafeJSONEncoder

router = APIRouter(prefix="/api/analyze", tags=["custom-analysis"])
logger = logging.getLogger(__name__)

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

@router.post("/custom")
async def analyze_custom_token(request: CustomAnalysisRequest):
    """Analysiert einen benutzerdefinierten Token basierend auf Adresse und Chain"""
    try:
        # Import des Analyzers
        from app.core.backend_crypto_tracker.scanner.low_cap_analyzer import LowCapAnalyzer
        
        # Initialisierung mit async with context manager
        async with LowCapAnalyzer() as analyzer:
            # Analyse durchführen
            result = await analyzer.analyze_custom_token(
                token_address=request.token_address,
                chain=request.chain
            )
        
        # Bereinige das Ergebnis von ungültigen Float-Werten
        sanitized_result = sanitize_value(result)
        
        # Erstelle die Antwort
        response_data = CustomAnalysisResponse(
            success=True,
            token_address=request.token_address,
            chain=request.chain,
            analysis_result=sanitized_result,
            analyzed_at=datetime.utcnow()
        )
        
        # Konvertiere zu Dictionary und bereinige erneut (für Sicherheit)
        response_dict = response_data.dict()
        sanitized_response_dict = sanitize_value(response_dict)
        
        # Verwende JSONResponse mit SafeJSONEncoder
        return JSONResponse(
            content=sanitized_response_dict,
            media_type="application/json",
            json_encoder=SafeJSONEncoder
        )
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        # Bereinige die Fehlermeldung
        sanitized_error = sanitize_value({"error": str(e)})
        return JSONResponse(
            content=sanitized_error,
            status_code=400,
            json_encoder=SafeJSONEncoder
        )
    except Exception as e:
        # Detaillierte Fehlermeldung für Debugging
        error_msg = str(e)
        logger.error(f"Fehler in analyze_custom_token: {error_msg}", exc_info=True)
        
        # Benutzerfreundliche Fehlermeldung
        user_msg = "Ein unerwarteter Fehler ist aufgetreten. Bitte versuchen Sie es später erneut."
        
        if "Tokendaten konnten nicht abgerufen werden" in error_msg:
            user_msg = error_msg
        elif "Analyse fehlgeschlagen" in error_msg:
            user_msg = error_msg
        elif "Rate limit exceeded" in error_msg:
            user_msg = "Zu viele Anfragen. Bitte warten Sie einige Minuten und versuchen Sie es erneut."
        
        # Bereinige die Fehlerantwort
        error_response = CustomAnalysisResponse(
            success=False,
            token_address=request.token_address,
            chain=request.chain,
            error_message=user_msg,
            analyzed_at=datetime.utcnow()
        )
        
        error_dict = error_response.dict()
        sanitized_error_dict = sanitize_value(error_dict)
        
        return JSONResponse(
            content=sanitized_error_dict,
            status_code=500,
            json_encoder=SafeJSONEncoder
        )
