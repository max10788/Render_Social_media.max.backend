# ============================================================================
# api/routes/wallet_routes.py
# ============================================================================
"""FastAPI Routes für Wallet-Analyse-API"""

from fastapi import APIRouter, Request, HTTPException, status
from datetime import datetime
from api.controllers.wallet_controller import WalletController

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
    
    Response:
    {
        "success": true,
        "data": {
            "wallet_address": "0x123...",
            "analysis": {
                "dominant_type": "trader",
                "confidence": 0.8523,
                "stage": 1,
                "transaction_count": 150
            },
            "classifications": [...]
        },
        "timestamp": "2025-01-15T10:30:00"
    }
    """
    try:
        data = await request.json()
        
        if not data:
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
        
        result = WalletController.analyze_wallet(
            transactions=transactions,
            stage=stage,
            wallet_address=wallet_address
        )
        
        if not result.get('success'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
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
        "transactions": [...],  // Required
        "stage": 1,            // Optional, default: 1
        "top_n": 3             // Optional, default: 3
    }
    
    Response:
    {
        "success": true,
        "data": {
            "top_matches": [
                {"rank": 1, "type": "trader", "score": 0.85, "is_match": true},
                {"rank": 2, "type": "whale", "score": 0.72, "is_match": true},
                {"rank": 3, "type": "mixer", "score": 0.45, "is_match": false}
            ],
            "stage": 1,
            "transaction_count": 150
        },
        "timestamp": "2025-01-15T10:30:00"
    }
    """
    try:
        data = await request.json()
        
        if not data:
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
        top_n = data.get('top_n', 3)
        
        result = WalletController.get_top_matches(
            transactions=transactions,
            stage=stage,
            top_n=top_n
        )
        
        if not result.get('success'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
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
    
    Response:
    {
        "success": true,
        "data": {
            "analyzed_wallets": 2,
            "stage": 1,
            "results": [
                {
                    "address": "0x123...",
                    "success": true,
                    "dominant_type": "trader",
                    "confidence": 0.85,
                    "transaction_count": 150
                },
                ...
            ]
        },
        "timestamp": "2025-01-15T10:30:00"
    }
    """
    try:
        data = await request.json()
        
        if not data:
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
        
        if not wallets:
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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result
            )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
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
    return {
        'status': 'healthy',
        'service': 'wallet-analyzer',
        'version': '1.0.0',
        'timestamp': datetime.utcnow().isoformat()
    }
