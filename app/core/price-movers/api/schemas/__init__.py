"""
API Schemas Package

Pydantic Models für Request/Response Validation
"""

from .request import (
    AnalysisRequest,
    QuickAnalysisRequest,
    HistoricalAnalysisRequest,
    WalletLookupRequest,
    CompareExchangesRequest,
)

from .response import (
    AnalysisResponse,
    HistoricalAnalysisResponse,
    WalletDetailResponse,
    ExchangeComparison,
    HealthCheckResponse,
    CandleData,
    WalletMover,
    TradeData,
    AnalysisMetadata,
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
    "CandleData",
    "WalletMover",
    "TradeData",
    "AnalysisMetadata",
    "ErrorResponse",
    "SuccessResponse",
]
