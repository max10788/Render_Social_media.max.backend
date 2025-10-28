"""
API Schemas Package

Exportiert alle Request und Response Schemas
"""

from .request import (
    AnalysisRequest,
    QuickAnalysisRequest,
    HistoricalAnalysisRequest,
    WalletLookupRequest,
    CompareExchangesRequest,
)

from .response import (
    CandleData,
    TradeData,
    ImpactComponents,
    WalletMover,
    AnalysisMetadata,
    AnalysisResponse,
    HistoricalAnalysisResponse,
    WalletDetailResponse,
    ExchangeComparison,
    HealthCheckResponse,
    ErrorResponse,
    SuccessResponse,
)


__all__ = [
    # Request Schemas
    "AnalysisRequest",
    "QuickAnalysisRequest",
    "HistoricalAnalysisRequest",
    "WalletLookupRequest",
    "CompareExchangesRequest",
    
    # Response Schemas
    "CandleData",
    "TradeData",
    "ImpactComponents",
    "WalletMover",
    "AnalysisMetadata",
    "AnalysisResponse",
    "HistoricalAnalysisResponse",
    "WalletDetailResponse",
    "ExchangeComparison",
    "HealthCheckResponse",
    "ErrorResponse",
    "SuccessResponse",
]
