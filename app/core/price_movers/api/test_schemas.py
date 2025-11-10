"""
Price Movers API Schemas
========================

Pydantic models for request/response validation and documentation.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, validator


# ============================================
# Enums
# ============================================

class ExchangeEnum(str, Enum):
    """Supported exchanges"""
    BINANCE = "binance"
    BITGET = "bitget"
    KRAKEN = "kraken"


class TimeframeEnum(str, Enum):
    """Supported timeframes"""
    ONE_MIN = "1m"
    FIVE_MIN = "5m"
    FIFTEEN_MIN = "15m"
    THIRTY_MIN = "30m"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"
    ONE_DAY = "1d"


class WalletTypeEnum(str, Enum):
    """Wallet classification types"""
    WHALE = "whale"
    MARKET_MAKER = "market_maker"
    BOT = "bot"
    UNKNOWN = "unknown"


# ============================================
# Base Models
# ============================================

class CandleData(BaseModel):
    """OHLCV Candle data"""
    timestamp: datetime
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    close: float = Field(..., description="Closing price")
    volume: float = Field(..., description="Trading volume")
    
    class Config:
        json_schema_extra = {
            "example": {
                "timestamp": "2025-10-30T12:00:00Z",
                "open": 68500.50,
                "high": 68750.25,
                "low": 68400.00,
                "close": 68650.75,
                "volume": 1250.5
            }
        }


class WalletMover(BaseModel):
    """Wallet with impact on price movement"""
    wallet_id: str = Field(..., description="Unique wallet identifier")
    wallet_type: WalletTypeEnum = Field(..., description="Classification of wallet")
    impact_score: float = Field(..., ge=0, le=1, description="Impact score (0-1)")
    total_volume: float = Field(..., description="Total trading volume")
    trade_count: int = Field(..., description="Number of trades")
    avg_trade_size: float = Field(..., description="Average trade size")
    
    # 🆕 NEU: Lightweight-spezifische Felder
    confidence_score: Optional[float] = Field(
        None, 
        ge=0, 
        le=1, 
        description="Confidence that this is a real entity (0-1)"
    )
    timing_pattern: Optional[str] = Field(
        None,
        description="Timing pattern: 'regular', 'burst', 'random'"
    )
    
    first_trade: Optional[datetime] = Field(None, description="First trade timestamp")
    last_trade: Optional[datetime] = Field(None, description="Last trade timestamp")


class AnalysisMetadata(BaseModel):
    """Metadata about the analysis process"""
    analysis_timestamp: datetime = Field(..., description="When analysis was performed")
    processing_duration_ms: int = Field(..., description="Processing time in milliseconds")
    total_trades_analyzed: int = Field(..., description="Total number of trades analyzed")
    unique_wallets_found: int = Field(..., description="Number of unique wallets identified")
    exchange: str = Field(..., description="Exchange used")
    symbol: str = Field(..., description="Trading pair")
    timeframe: str = Field(..., description="Candle timeframe")
    
    class Config:
        json_schema_extra = {
            "example": {
                "analysis_timestamp": "2025-10-30T12:05:30Z",
                "processing_duration_ms": 1250,
                "total_trades_analyzed": 5420,
                "unique_wallets_found": 342,
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m"
            }
        }


# ============================================
# Request Models
# ============================================

class QuickAnalysisRequest(BaseModel):
    """Request for quick analysis of latest candle"""
    exchange: ExchangeEnum = Field(..., description="Exchange to analyze")
    symbol: str = Field(..., description="Trading pair (e.g., BTC/USDT)")
    timeframe: TimeframeEnum = Field(default=TimeframeEnum.FIVE_MIN, description="Candle timeframe")
    top_n_wallets: int = Field(default=10, ge=1, le=100, description="Number of top wallets to return")
    
    @validator('symbol')
    def validate_symbol(cls, v):
        """Ensure symbol is in correct format"""
        if '/' not in v:
            raise ValueError("Symbol must be in format 'BASE/QUOTE' (e.g., BTC/USDT)")
        return v.upper()
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "top_n_wallets": 10
            }
        }


class AnalysisRequest(BaseModel):
    """Request for detailed analysis with custom parameters"""
    exchange: ExchangeEnum = Field(..., description="Exchange to analyze")
    symbol: str = Field(..., description="Trading pair (e.g., BTC/USDT)")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    start_time: datetime = Field(..., description="Start of analysis period")
    end_time: datetime = Field(..., description="End of analysis period")
    min_impact_threshold: float = Field(default=0.1, ge=0, le=1, description="Minimum impact score to include")
    top_n_wallets: int = Field(default=10, ge=1, le=100, description="Number of top wallets to return")
    include_trades: bool = Field(default=False, description="Include individual trade data")
    
    @validator('symbol')
    def validate_symbol(cls, v):
        """Ensure symbol is in correct format"""
        if '/' not in v:
            raise ValueError("Symbol must be in format 'BASE/QUOTE' (e.g., BTC/USDT)")
        return v.upper()
    
    @validator('end_time')
    def validate_time_range(cls, v, values):
        """Ensure end_time is after start_time"""
        if 'start_time' in values and v <= values['start_time']:
            raise ValueError("end_time must be after start_time")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "start_time": "2025-10-30T12:00:00Z",
                "end_time": "2025-10-30T12:05:00Z",
                "min_impact_threshold": 0.1,
                "top_n_wallets": 10,
                "include_trades": False
            }
        }


class HistoricalAnalysisRequest(BaseModel):
    """Request for historical analysis across multiple candles"""
    exchange: ExchangeEnum = Field(..., description="Exchange to analyze")
    symbol: str = Field(..., description="Trading pair (e.g., BTC/USDT)")
    timeframe: TimeframeEnum = Field(..., description="Candle timeframe")
    start_time: datetime = Field(..., description="Start of analysis period")
    end_time: datetime = Field(..., description="End of analysis period")
    min_impact_threshold: float = Field(default=0.1, ge=0, le=1, description="Minimum impact score to include")
    
    @validator('symbol')
    def validate_symbol(cls, v):
        """Ensure symbol is in correct format"""
        if '/' not in v:
            raise ValueError("Symbol must be in format 'BASE/QUOTE' (e.g., BTC/USDT)")
        return v.upper()
    
    @validator('end_time')
    def validate_time_range(cls, v, values):
        """Ensure end_time is after start_time"""
        if 'start_time' in values and v <= values['start_time']:
            raise ValueError("end_time must be after start_time")
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "start_time": "2025-10-30T10:00:00Z",
                "end_time": "2025-10-30T12:00:00Z",
                "min_impact_threshold": 0.1
            }
        }


class WalletLookupRequest(BaseModel):
    """Request for detailed wallet information"""
    wallet_id: str = Field(..., description="Wallet address to lookup")
    exchange: ExchangeEnum = Field(..., description="Exchange to query")
    symbol: str = Field(..., description="Trading pair (e.g., BTC/USDT)")
    lookback_hours: int = Field(default=24, ge=1, le=720, description="Hours of history to include")
    
    @validator('symbol')
    def validate_symbol(cls, v):
        """Ensure symbol is in correct format"""
        if '/' not in v:
            raise ValueError("Symbol must be in format 'BASE/QUOTE' (e.g., BTC/USDT)")
        return v.upper()
    
    class Config:
        json_schema_extra = {
            "example": {
                "wallet_id": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "lookback_hours": 24
            }
        }


class CompareExchangesRequest(BaseModel):
    """Request to compare data across multiple exchanges"""
    exchanges: List[ExchangeEnum] = Field(..., min_items=2, max_items=5, description="Exchanges to compare")
    symbol: str = Field(..., description="Trading pair (e.g., BTC/USDT)")
    timeframe: TimeframeEnum = Field(default=TimeframeEnum.FIVE_MIN, description="Candle timeframe")
    
    @validator('symbol')
    def validate_symbol(cls, v):
        """Ensure symbol is in correct format"""
        if '/' not in v:
            raise ValueError("Symbol must be in format 'BASE/QUOTE' (e.g., BTC/USDT)")
        return v.upper()
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchanges": ["binance", "bitget", "kraken"],
                "symbol": "BTC/USDT",
                "timeframe": "5m"
            }
        }


# ============================================
# Response Models
# ============================================

class AnalysisResponse(BaseModel):
    """Response with analysis results"""
    success: bool = Field(default=True, description="Whether analysis succeeded")
    candle: CandleData = Field(..., description="Candle data that was analyzed")
    top_movers: List[WalletMover] = Field(..., description="Top wallets by impact")
    analysis_metadata: AnalysisMetadata = Field(..., description="Analysis metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "candle": {
                    "timestamp": "2025-10-30T12:00:00Z",
                    "open": 68500.50,
                    "high": 68750.25,
                    "low": 68400.00,
                    "close": 68650.75,
                    "volume": 1250.5
                },
                "top_movers": [
                    {
                        "wallet_id": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
                        "wallet_type": "whale",
                        "impact_score": 0.85,
                        "total_volume": 2400000.50,
                        "trade_count": 127,
                        "avg_trade_size": 18897.64,
                        "first_trade": "2025-10-30T12:00:00Z",
                        "last_trade": "2025-10-30T12:05:00Z"
                    }
                ],
                "analysis_metadata": {
                    "analysis_timestamp": "2025-10-30T12:05:30Z",
                    "processing_duration_ms": 1250,
                    "total_trades_analyzed": 5420,
                    "unique_wallets_found": 342,
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "5m"
                }
            }
        }


class HistoricalCandle(BaseModel):
    """Historical candle with top movers"""
    candle: CandleData = Field(..., description="Candle data")
    top_movers: List[WalletMover] = Field(..., description="Top wallets for this candle")


class HistoricalAnalysisResponse(BaseModel):
    """Response with historical analysis results"""
    success: bool = Field(default=True, description="Whether analysis succeeded")
    candles: List[HistoricalCandle] = Field(..., description="Historical candles with movers")
    total_candles: int = Field(..., description="Total number of candles analyzed")
    unique_wallets: int = Field(..., description="Total unique wallets found")
    analysis_metadata: AnalysisMetadata = Field(..., description="Analysis metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "candles": [
                    {
                        "candle": {
                            "timestamp": "2025-10-30T12:00:00Z",
                            "open": 68500.50,
                            "high": 68750.25,
                            "low": 68400.00,
                            "close": 68650.75,
                            "volume": 1250.5
                        },
                        "top_movers": []
                    }
                ],
                "total_candles": 24,
                "unique_wallets": 850,
                "analysis_metadata": {
                    "analysis_timestamp": "2025-10-30T12:05:30Z",
                    "processing_duration_ms": 5250,
                    "total_trades_analyzed": 45420,
                    "unique_wallets_found": 850,
                    "exchange": "binance",
                    "symbol": "BTC/USDT",
                    "timeframe": "5m"
                }
            }
        }


class WalletDetailResponse(BaseModel):
    """Detailed information about a specific wallet"""
    success: bool = Field(default=True, description="Whether lookup succeeded")
    wallet_id: str = Field(..., description="Wallet address")
    wallet_type: WalletTypeEnum = Field(..., description="Classification of wallet")
    first_seen: datetime = Field(..., description="First trade timestamp")
    last_seen: datetime = Field(..., description="Last trade timestamp")
    total_trades: int = Field(..., description="Total number of trades")
    total_volume: float = Field(..., description="Total trading volume")
    total_value_usd: float = Field(..., description="Total value in USD")
    avg_impact_score: float = Field(..., description="Average impact score")
    exchange: str = Field(..., description="Exchange")
    symbol: str = Field(..., description="Trading pair")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "wallet_id": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
                "wallet_type": "whale",
                "first_seen": "2025-10-29T08:30:00Z",
                "last_seen": "2025-10-30T12:05:00Z",
                "total_trades": 342,
                "total_volume": 12500000.75,
                "total_value_usd": 12500000.75,
                "avg_impact_score": 0.72,
                "exchange": "binance",
                "symbol": "BTC/USDT"
            }
        }


class ExchangeData(BaseModel):
    """Data for a single exchange"""
    price: Optional[float] = Field(None, description="Current price")
    volume: Optional[float] = Field(None, description="24h volume")
    spread: Optional[float] = Field(None, description="Bid-ask spread")
    error: Optional[str] = Field(None, description="Error message if failed")
    
    class Config:
        json_schema_extra = {
            "example": {
                "price": 68650.75,
                "volume": 125000.50,
                "spread": 0.25,
                "error": None
            }
        }


class ExchangeComparison(BaseModel):
    """Comparison data across multiple exchanges"""
    success: bool = Field(default=True, description="Whether comparison succeeded")
    symbol: str = Field(..., description="Trading pair")
    timeframe: str = Field(..., description="Timeframe")
    timestamp: datetime = Field(..., description="Comparison timestamp")
    exchanges: Dict[str, ExchangeData] = Field(..., description="Data per exchange")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "timestamp": "2025-10-30T12:05:30Z",
                "exchanges": {
                    "binance": {
                        "price": 68650.75,
                        "volume": 125000.50,
                        "spread": 0.25,
                        "error": None
                    },
                    "bitget": {
                        "price": 68655.25,
                        "volume": 95000.30,
                        "spread": 0.35,
                        "error": None
                    }
                }
            }
        }


class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str = Field(default="healthy", description="Service status")
    timestamp: datetime = Field(..., description="Check timestamp")
    version: str = Field(..., description="API version")
    uptime_seconds: int = Field(..., description="Service uptime in seconds")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2025-10-30T12:05:30Z",
                "version": "1.0.0",
                "uptime_seconds": 86400
            }
        }


class ErrorResponse(BaseModel):
    """Error response"""
    success: bool = Field(default=False, description="Always false for errors")
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": False,
                "error": "Invalid symbol format",
                "error_code": "VALIDATION_ERROR",
                "details": {"field": "symbol", "expected": "BASE/QUOTE"},
                "timestamp": "2025-10-30T12:05:30Z"
            }
        }


class SuccessResponse(BaseModel):
    """Generic success response"""
    success: bool = Field(default=True, description="Operation success")
    message: str = Field(..., description="Success message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Operation completed successfully",
                "timestamp": "2025-10-30T12:05:30Z"
            }
        }


# ============================================
# Utility Functions
# ============================================

def validate_trading_pair(symbol: str) -> bool:
    """
    Validate trading pair format
    
    Args:
        symbol: Trading pair string (e.g., "BTC/USDT")
        
    Returns:
        True if valid, False otherwise
    """
    if not symbol or '/' not in symbol:
        return False
    
    parts = symbol.split('/')
    if len(parts) != 2:
        return False
    
    base, quote = parts
    if not base or not quote:
        return False
    
    return True


def parse_timeframe_to_seconds(timeframe: str) -> int:
    """
    Convert timeframe string to seconds
    
    Args:
        timeframe: Timeframe string (e.g., "5m", "1h", "1d")
        
    Returns:
        Number of seconds
        
    Raises:
        ValueError: If timeframe format is invalid
    """
    timeframe_map = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }
    
    if timeframe not in timeframe_map:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    return timeframe_map[timeframe]


# ============================================
# Export All
# ============================================

__all__ = [
    # Enums
    "ExchangeEnum",
    "TimeframeEnum",
    "WalletTypeEnum",
    
    # Base Models
    "CandleData",
    "WalletMover",
    "AnalysisMetadata",
    
    # Requests
    "QuickAnalysisRequest",
    "AnalysisRequest",
    "HistoricalAnalysisRequest",
    "WalletLookupRequest",
    "CompareExchangesRequest",
    
    # Responses
    "AnalysisResponse",
    "HistoricalAnalysisResponse",
    "HistoricalCandle",
    "WalletDetailResponse",
    "ExchangeData",
    "ExchangeComparison",
    "HealthCheckResponse",
    "ErrorResponse",
    "SuccessResponse",
    
    # Utilities
    "validate_trading_pair",
    "parse_timeframe_to_seconds",
]
