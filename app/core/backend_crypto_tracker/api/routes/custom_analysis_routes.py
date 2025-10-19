"""
Custom Analysis Routes - API Endpoint für Token-Analyse
UNVERÄNDERT - Funktioniert mit dem neuen System out-of-the-box
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator
from typing import Optional
from datetime import datetime
import re
import logging
import json

# Importieren Sie die Hilfsfunktionen für JSON-Bereinigung
from app.core.backend_crypto_tracker.utils.json_helpers import sanitize_value, SafeJSONEncoder

router = APIRouter(prefix="/api/analyze", tags=["custom-analysis"])
logger = logging.getLogger(__name__)


class CustomAnalysisRequest(BaseModel):
    """Request Model für Custom Token Analysis"""
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
    """Response Model für Custom Token Analysis"""
    success: bool
    token_address: str
    chain: str
    analysis_result: Optional[dict] = None
    error_message: Optional[str] = None
    analyzed_at: datetime


@router.post("/custom", response_model=CustomAnalysisResponse)
async def analyze_custom_token(request: CustomAnalysisRequest):
    """
    Analysiert einen benutzerdefinierten Token basierend auf Adresse und Chain.
    
    **Supported Chains:**
    - ethereum
    - bsc
    - solana
    - sui
    
    **Returns:**
    - Token Info (name, symbol, market_cap, volume_24h, etc.)
    - Token Score (0-100)
    - Risk Flags
    - Wallet Analysis mit 5 Klassifizierungstypen:
      - Dust Sweeper
      - Hodler
      - Mixer
      - Trader
      - Whale
    
    **Example Request:**
    ```json
    {
        "token_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
        "chain": "ethereum"
    }
    ```
    """
    try:
        logger.info(f"Starting analysis for {request.token_address} on {request.chain}")
        
        # Import des Analyzers
        from app.core.backend_crypto_tracker.scanner.low_cap_analyzer import LowCapAnalyzer
        
        # Initialisierung mit async with context manager
        async with LowCapAnalyzer() as analyzer:
            # Analyse durchführen
            result = await analyzer.analyze_custom_token(
                token_address=request.token_address,
                chain=request.chain
            )
            
            # Prüfe, ob der Token gefunden wurde
            if result.get('token_info', {}).get('name') == 'Unknown':
                logger.warning(f"Token not found: {request.token_address} on {request.chain}")
                
                # Token nicht gefunden - gib eine detaillierte Fehlermeldung zurück
                error_details = {
                    "error": "TOKEN_NOT_FOUND",
                    "message": f"Token mit der Adresse {request.token_address} konnte nicht in den Datenbanken gefunden werden.",
                    "details": {
                        "token_address": request.token_address,
                        "chain": request.chain,
                        "possible_reasons": [
                            "Die Token-Adresse ist falsch",
                            "Der Token existiert nicht mehr",
                            "Der Token ist noch nicht in den Datenbanken gelistet",
                            "Der Token ist auf einer anderen Blockchain"
                        ],
                        "suggestions": [
                            "Überprüfen Sie die Token-Adresse auf Tippfehler",
                            "Stellen Sie sicher, dass die richtige Blockchain ausgewählt ist",
                            "Versuchen Sie es mit einer bekannten Token-Adresse zur Überprüfung"
                        ]
                    }
                }
                
                response = CustomAnalysisResponse(
                    success=False,
                    token_address=request.token_address,
                    chain=request.chain,
                    analysis_result=error_details,
                    error_message=f"Token nicht gefunden: {request.token_address}",
                    analyzed_at=datetime.utcnow()
                )
                
                # JSON Response mit Sanitizing
                response_dict = response.dict()
                sanitized_response = sanitize_value(response_dict)
                json_content = json.dumps(sanitized_response, cls=SafeJSONEncoder)
                
                return JSONResponse(
                    content=json.loads(json_content),
                    status_code=404,
                    media_type="application/json"
                )
        
        logger.info(f"Analysis completed successfully for {request.token_address}")
        
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
        
        # Verwende json.dumps mit SafeJSONEncoder
        json_content = json.dumps(sanitized_response_dict, cls=SafeJSONEncoder)
        
        return JSONResponse(
            content=json.loads(json_content),
            media_type="application/json"
        )
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        
        # Bereinige die Fehlermeldung
        error_response = CustomAnalysisResponse(
            success=False,
            token_address=request.token_address,
            chain=request.chain,
            error_message=str(e),
            analyzed_at=datetime.utcnow()
        )
        
        error_dict = error_response.dict()
        sanitized_error = sanitize_value(error_dict)
        json_content = json.dumps(sanitized_error, cls=SafeJSONEncoder)
        
        return JSONResponse(
            content=json.loads(json_content),
            status_code=400,
            media_type="application/json"
        )
        
    except Exception as e:
        # Detaillierte Fehlermeldung für Debugging
        error_msg = str(e)
        logger.error(f"Fehler in analyze_custom_token: {error_msg}", exc_info=True)
        
        # Benutzerfreundliche Fehlermeldung
        user_msg = "Ein unerwarteter Fehler ist aufgetreten. Bitte versuchen Sie es später erneut."
        
        if "Token data could not be retrieved" in error_msg:
            user_msg = f"Token mit der Adresse {request.token_address} konnte nicht gefunden werden. Bitte überprüfen Sie die Adresse oder versuchen Sie es mit einem anderen Token."
        elif "Analyse fehlgeschlagen" in error_msg:
            user_msg = error_msg
        elif "Rate limit exceeded" in error_msg:
            user_msg = "Zu viele Anfragen. Bitte warten Sie einige Minuten und versuchen Sie es erneut."
        elif "Initialisierung fehlgeschlagen" in error_msg:
            user_msg = "Service-Initialisierung fehlgeschlagen. Bitte versuchen Sie es später erneut."
        
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
        json_content = json.dumps(sanitized_error_dict, cls=SafeJSONEncoder)
        
        return JSONResponse(
            content=json.loads(json_content),
            status_code=500,
            media_type="application/json"
        )


@router.get("/health")
async def health_check():
    """
    Health Check Endpoint für den Analysis Service
    """
    return {
        "status": "healthy",
        "service": "custom-analysis",
        "timestamp": datetime.utcnow().isoformat(),
        "supported_chains": ["ethereum", "bsc", "solana", "sui"],
        "wallet_classification_types": [
            "DUST_SWEEPER",
            "HODLER", 
            "MIXER",
            "TRADER",
            "WHALE"
        ]
    }


@router.get("/supported-chains")
async def get_supported_chains():
    """
    Gibt eine Liste der unterstützten Blockchains zurück
    """
    return {
        "chains": [
            {
                "id": "ethereum",
                "name": "Ethereum",
                "address_format": "0x + 40 hex characters",
                "example": "0x6b175474e89094c44da98b954eedeac495271d0f"
            },
            {
                "id": "bsc",
                "name": "Binance Smart Chain",
                "address_format": "0x + 40 hex characters",
                "example": "0xe9e7cea3dedca5984780bafc599bd69add087d56"
            },
            {
                "id": "solana",
                "name": "Solana",
                "address_format": "Base58 string (32-44 characters)",
                "example": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            },
            {
                "id": "sui",
                "name": "Sui",
                "address_format": "0x + 64 hex characters",
                "example": "0x2::sui::SUI"
            }
        ]
    }


@router.get("/wallet-types")
async def get_wallet_types():
    """
    Gibt Informationen über die Wallet-Klassifizierungstypen zurück
    """
    from app.core.backend_crypto_tracker.processor.database.models.wallet import (
        get_wallet_type_description,
        get_wallet_type_risk_level,
        WalletTypeEnum
    )
    
    wallet_types = []
    
    # Nur die 5 Haupttypen aus dem neuen System
    main_types = [
        WalletTypeEnum.DUST_SWEEPER,
        WalletTypeEnum.HODLER,
        WalletTypeEnum.MIXER,
        WalletTypeEnum.TRADER,
        WalletTypeEnum.WHALE
    ]
    
    for wallet_type in main_types:
        wallet_types.append({
            "type": wallet_type.value,
            "description": get_wallet_type_description(wallet_type),
            "risk_level": get_wallet_type_risk_level(wallet_type),
            "threshold": {
                "DUST_SWEEPER": 0.65,
                "HODLER": 0.70,
                "MIXER": 0.60,
                "TRADER": 0.60,
                "WHALE": 0.70
            }.get(wallet_type.value, 0.50)
        })
    
    return {
        "wallet_types": wallet_types,
        "classification_method": "3-Stage Pipeline",
        "stages": [
            "Stage 1: Raw Metrics (Transaction data)",
            "Stage 2: Derived Metrics (Calculated indicators)",
            "Stage 3: Context Analysis (Network & external data)"
        ]
    }
