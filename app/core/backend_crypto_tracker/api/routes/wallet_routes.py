# ============================================================================
# api/routes/wallet_routes.py
# ============================================================================
"""FastAPI Routes für Wallet-Analyse-API mit ausführlichem Logging"""

import logging
import time
import uuid
import json
import re
from typing import Dict, Any
from fastapi import APIRouter, Request, HTTPException, status
from datetime import datetime
from api.controllers.wallet_controller import WalletController

# Logger konfigurieren
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Console Handler für Entwicklung
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Hilfsfunktionen für Logging
def mask_wallet_address(address: str) -> str:
    """Maskiert Wallet-Adressen für das Logging"""
    if not address:
        return "N/A"
    return f"{address[:6]}...{address[-4:]}"

def log_request_data(request_id: str, data: Dict[str, Any]) -> None:
    """Protokolliert Request-Data sicher"""
    masked_data = {
        'wallet_address': mask_wallet_address(data.get('wallet_address')),
        'blockchain': data.get('blockchain', 'N/A'),
        'transactions_count': len(data.get('transactions', [])),
        'stage': data.get('stage', 1),
        'top_n': data.get('top_n', 3),
        'fetch_limit': data.get('fetch_limit', 100)
    }
    logger.info(f"[{request_id}] Request-Daten: {masked_data}")

def log_response_data(request_id: str, response: Dict[str, Any]) -> None:
    """Protokolliert Response-Data sicher"""
    if response.get('success'):
        data = response.get('data', {})
        logger.info(f"[{request_id}] Erfolgreiche Antwort: "
                   f"Wallet={mask_wallet_address(data.get('wallet_address'))}, "
                   f"Blockchain={data.get('blockchain', 'N/A')}, "
                   f"Transaktionen={data.get('analysis', {}).get('transaction_count', 0)}")
    else:
        logger.error(f"[{request_id}] Fehlerhafte Antwort: {response.get('error')}")

def sanitize_json_string(json_str: str) -> str:
    """
    Bereinigt JSON-Strings mit unmaskierten Anführungszeichen
    
    Args:
        json_str: Roher JSON-String
        
    Returns:
        Bereinigter JSON-String
    """
    try:
        # Versuche zuerst, normal zu parsen
        json.loads(json_str)
        return json_str
    except json.JSONDecodeError:
        # Ersetze unmaskierte Anführungszeichen innerhalb von Werten
        string_pattern = re.compile(r'"(?:[^"\\]|\\.)*"')
        
        def replace_quotes_in_string(match):
            string = match.group(0)
            if len(string) > 2:
                inner = string[1:-1]
                inner = inner.replace('\\"', '__TEMP_QUOTE__')
                inner = inner.replace('"', '\\"')
                inner = inner.replace('__TEMP_QUOTE__', '\\"')
                return '"' + inner + '"'
            return string
        
        sanitized = string_pattern.sub(replace_quotes_in_string, json_str)
        
        try:
            json.loads(sanitized)
            return sanitized
        except json.JSONDecodeError as e:
            logger.warning(f"Konnte JSON nicht bereinigen: {str(e)}")
            return json_str

# Erstelle Router
router = APIRouter(prefix="/api/v1/wallet", tags=["wallet"])


@router.post("/analyze")
async def analyze_wallet(request: Request):
    """
    POST /api/v1/wallet/analyze
    
    Analysiert eine einzelne Wallet
    
    Request Body:
    {
        "wallet_address": "0x123...",  // Optional
        "transactions": [...],          // Required
        "stage": 1                      // Optional, default: 1
    }
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    logger.info(f"[{request_id}] Neue Wallet-Analyse-Anfrage gestartet")
    
    try:
        # JSON-Body mit Fehlerbehandlung parsen
        try:
            raw_body = await request.body()
            body_str = raw_body.decode('utf-8', errors='replace')
            
            try:
                data = json.loads(body_str)
            except json.JSONDecodeError:
                logger.warning(f"[{request_id}] Ungültiges JSON erkannt, versuche Bereinigung...")
                sanitized_body = sanitize_json_string(body_str)
                data = json.loads(sanitized_body)
                logger.info(f"[{request_id}] JSON erfolgreich bereinigt")
                
        except json.JSONDecodeError as e:
            logger.error(f"[{request_id}] JSONDecodeError: {str(e)}")
            logger.error(f"[{request_id}] Request-Body: {body_str[:500]}...")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    'success': False,
                    'error': 'Ungültiges JSON-Format',
                    'error_code': 'INVALID_JSON',
                    'details': str(e),
                    'hint': 'Stellen Sie sicher, dass alle Anführungszeichen in Strings korrekt maskiert sind (\\")'
                }
            )
        
        log_request_data(request_id, data)
        
        if not data:
            logger.warning(f"[{request_id}] Leerer Request-Body")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    'success': False,
                    'error': 'Kein JSON-Body bereitgestellt',
                    'error_code': 'INVALID_REQUEST'
                }
            )
        
        transactions = data.get('transactions', [])
        stage = data.get('stage', 1)
        wallet_address = data.get('wallet_address')
        
        logger.info(f"[{request_id}] Starte Wallet-Analyse mit Stage {stage}")
        result = WalletController.analyze_wallet(
            transactions=transactions,
            stage=stage,
            wallet_address=wallet_address
        )
        
        log_response_data(request_id, result)
        
        if not result.get('success'):
            logger.error(f"[{request_id}] Analyse fehlgeschlagen: {result.get('error')}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result
            )
        
        duration = time.time() - start_time
        logger.info(f"[{request_id}] Analyse erfolgreich abgeschlossen in {duration:.2f}s")
        return result
        
    except HTTPException as he:
        duration = time.time() - start_time
        logger.error(f"[{request_id}] HTTPException nach {duration:.2f}s: {he.detail}")
        raise
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{request_id}] Unerwarteter Fehler nach {duration:.2f}s: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                'success': False,
                'error': str(e),
                'error_code': 'SERVER_ERROR'
            }
        )


@router.post("/analyze/top-matches")
async def get_top_matches(request: Request):
    """
    POST /api/v1/wallet/analyze/top-matches
    
    Gibt die Top-N wahrscheinlichsten Wallet-Typen zurück
    
    Request Body:
    {
        "transactions": [...],      // Optional
        "wallet_address": "0x123...", // Optional
        "blockchain": "ethereum",    // Optional
        "stage": 1,                 // Optional, default: 1
        "top_n": 3,                 // Optional, default: 3
        "fetch_limit": 100          // Optional, default: 100
    }
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    logger.info(f"[{request_id}] Neue Top-Matches-Anfrage gestartet")
    
    try:
        # JSON-Body mit Fehlerbehandlung parsen
        try:
            raw_body = await request.body()
            body_str = raw_body.decode('utf-8', errors='replace')
            
            try:
                data = json.loads(body_str)
            except json.JSONDecodeError:
                logger.warning(f"[{request_id}] Ungültiges JSON erkannt, versuche Bereinigung...")
                sanitized_body = sanitize_json_string(body_str)
                data = json.loads(sanitized_body)
                logger.info(f"[{request_id}] JSON erfolgreich bereinigt")
                
        except json.JSONDecodeError as e:
            logger.error(f"[{request_id}] JSONDecodeError: {str(e)}")
            logger.error(f"[{request_id}] Request-Body: {body_str[:500]}...")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    'success': False,
                    'error': 'Ungültiges JSON-Format',
                    'error_code': 'INVALID_JSON',
                    'details': str(e),
                    'hint': 'Stellen Sie sicher, dass alle Anführungszeichen in Strings korrekt maskiert sind (\\")'
                }
            )
        
        log_request_data(request_id, data)
        
        if not data:
            logger.warning(f"[{request_id}] Leerer Request-Body")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    'success': False,
                    'error': 'Kein JSON-Body bereitgestellt',
                    'error_code': 'INVALID_REQUEST'
                }
            )
        
        transactions = data.get('transactions')
        wallet_address = data.get('wallet_address')
        blockchain = data.get('blockchain')
        stage = data.get('stage', 1)
        top_n = data.get('top_n', 3)
        fetch_limit = data.get('fetch_limit', 100)
        
        logger.info(f"[{request_id}] Starte Top-Matches-Analyse mit Stage {stage}, Top-N={top_n}")
        
        # Provider-Imports wurden entfernt - sie werden jetzt im Controller verwaltet
        # Der Controller wird die Provider bei Bedarf selbst instanziieren
        
        # Wallet-Analyse durchführen
        result = WalletController.get_top_matches(
            transactions=transactions,
            wallet_address=wallet_address,
            blockchain=blockchain,
            stage=stage,
            top_n=top_n,
            fetch_limit=fetch_limit
        )
        
        log_response_data(request_id, result)
        
        if not result.get('success'):
            logger.error(f"[{request_id}] Top-Matches-Analyse fehlgeschlagen: {result.get('error')}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result
            )
        
        duration = time.time() - start_time
        logger.info(f"[{request_id}] Top-Matches-Analyse erfolgreich in {duration:.2f}s")
        return result
        
    except HTTPException as he:
        duration = time.time() - start_time
        logger.error(f"[{request_id}] HTTPException nach {duration:.2f}s: {he.detail}")
        raise
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{request_id}] Unerwarteter Fehler nach {duration:.2f}s: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                'success': False,
                'error': str(e),
                'error_code': 'SERVER_ERROR'
            }
        )


@router.post("/analyze/batch")
async def batch_analyze(request: Request):
    """
    POST /api/v1/wallet/analyze/batch
    
    Analysiert mehrere Wallets gleichzeitig
    
    Request Body:
    {
        "wallets": [
            {
                "address": "0x123...",
                "transactions": [...]
            },
            {
                "address": "0x456...",
                "transactions": [...]
            }
        ],
        "stage": 1  // Optional, default: 1
    }
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    logger.info(f"[{request_id}] Neue Batch-Analyse-Anfrage gestartet")
    
    try:
        # JSON-Body mit Fehlerbehandlung parsen
        try:
            raw_body = await request.body()
            body_str = raw_body.decode('utf-8', errors='replace')
            
            try:
                data = json.loads(body_str)
            except json.JSONDecodeError:
                logger.warning(f"[{request_id}] Ungültiges JSON erkannt, versuche Bereinigung...")
                sanitized_body = sanitize_json_string(body_str)
                data = json.loads(sanitized_body)
                logger.info(f"[{request_id}] JSON erfolgreich bereinigt")
                
        except json.JSONDecodeError as e:
            logger.error(f"[{request_id}] JSONDecodeError: {str(e)}")
            logger.error(f"[{request_id}] Request-Body: {body_str[:500]}...")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    'success': False,
                    'error': 'Ungültiges JSON-Format',
                    'error_code': 'INVALID_JSON',
                    'details': str(e),
                    'hint': 'Stellen Sie sicher, dass alle Anführungszeichen in Strings korrekt maskiert sind (\\")'
                }
            )
        
        if not data:
            logger.warning(f"[{request_id}] Leerer Request-Body")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    'success': False,
                    'error': 'Kein JSON-Body bereitgestellt',
                    'error_code': 'INVALID_REQUEST'
                }
            )
        
        wallets = data.get('wallets', [])
        stage = data.get('stage', 1)
        
        logger.info(f"[{request_id}] Batch-Analyse mit {len(wallets)} Wallets, Stage {stage}")
        
        # Maskierte Wallet-Adressen für Logging
        masked_wallets = [mask_wallet_address(w.get('address')) for w in wallets]
        logger.info(f"[{request_id}] Wallet-Adressen: {masked_wallets}")
        
        if not wallets:
            logger.warning(f"[{request_id}] Keine Wallets in Request")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    'success': False,
                    'error': 'Keine Wallets bereitgestellt',
                    'error_code': 'NO_WALLETS'
                }
            )
        
        result = WalletController.batch_analyze(
            wallets=wallets,
            stage=stage
        )
        
        if not result.get('success'):
            logger.error(f"[{request_id}] Batch-Analyse fehlgeschlagen: {result.get('error')}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result
            )
        
        duration = time.time() - start_time
        logger.info(f"[{request_id}] Batch-Analyse erfolgreich in {duration:.2f}s")
        return result
        
    except HTTPException as he:
        duration = time.time() - start_time
        logger.error(f"[{request_id}] HTTPException nach {duration:.2f}s: {he.detail}")
        raise
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"[{request_id}] Unerwarteter Fehler nach {duration:.2f}s: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                'success': False,
                'error': str(e),
                'error_code': 'SERVER_ERROR'
            }
        )


@router.get("/health")
async def health_check():
    """
    GET /api/v1/wallet/health
    
    Health-Check für den Wallet-Analyse-Service
    
    Response:
    {
        "status": "healthy",
        "service": "wallet-analyzer",
        "version": "1.0.0",
        "timestamp": "2025-01-15T10:30:00"
    }
    """
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Health-Check angefordert")
    
    try:
        response = {
            'status': 'healthy',
            'service': 'wallet-analyzer',
            'version': '1.0.0',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info(f"[{request_id}] Health-Check erfolgreich: {response}")
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Health-Check fehlgeschlagen: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                'success': False,
                'error': str(e),
                'error_code': 'HEALTH_CHECK_FAILED'
            }
        )
