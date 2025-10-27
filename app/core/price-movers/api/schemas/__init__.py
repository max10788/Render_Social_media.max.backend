"""
API Package

API Layer mit Schemas und Endpoints
"""

from .schemas import (
    # Request Models
    AnalysisRequest,
    QuickAnalysisRequest,
    HistoricalAnalysisRequest,
    WalletLookupRequest,
    CompareExchangesRequest,
    
    # Response Models
    AnalysisResponse,
    HistoricalAnalysisResponse,
    WalletDetailResponse,
    ExchangeComparison,
    HealthCheckResponse,
    ErrorResponse,
    SuccessResponse,
)

__all__ = [
    # Request Models
    "AnalysisRequest",
    "QuickAnalysisRequest",
    "HistoricalAnalysisRequest",
    "WalletLookupRequest",
    "CompareExchangesRequest",
    
    # Response Models
    "AnalysisResponse",
    "HistoricalAnalysisResponse",
    "WalletDetailResponse",
    "ExchangeComparison",
    "HealthCheckResponse",
    "ErrorResponse",
    "SuccessResponse",
]
