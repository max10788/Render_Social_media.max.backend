"""
Request Schemas für Price Movers API

Pydantic Models für Request Validation
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, validator, root_validator

from ...utils.constants import (
    SUPPORTED_EXCHANGES,
    SUPPORTED_TIMEFRAMES,
    MIN_PRICE_MOVEMENT_PCT,
    DEFAULT_TOP_N_WALLETS,
    MAX_TOP_N_WALLETS,
    MIN_IMPACT_SCORE,
    MAX_ANALYSIS_TIMESPAN_HOURS,
    ERROR_MESSAGES,
)


class AnalysisRequest(BaseModel):
    """
    Request Model für Price Movers Analyse
    
    Attributes:
        exchange: Exchange für Analyse (bitget, binance, kraken)
        symbol: Trading Pair (z.B. BTC/USDT)
        timeframe: Candle Timeframe (z.B. 5m, 1h)
        start_time: Start-Zeitpunkt der Analyse
        end_time: End-Zeitpunkt der Analyse
        min_impact_threshold: Minimaler Impact Score (0-1)
        top_n_wallets: Anzahl Top Wallets im Response
        include_trades: Trades in Response inkludieren
    """
    
    exchange: str = Field(
        ...,
        description="Exchange für Analyse",
        example="binance"
    )
    
    symbol: str = Field(
        ...,
        description="Trading Pair (Format: BASE/QUOTE)",
        example="BTC/USDT"
    )
    
    timeframe: str = Field(
        ...,
        description="Candle Timeframe",
        example="5m"
    )
    
    start_time: datetime = Field(
        ...,
        description="Start-Zeitpunkt der Analyse (ISO 8601)",
        example="2024-10-27T10:00:00Z"
    )
    
    end_time: datetime = Field(
        ...,
        description="End-Zeitpunkt der Analyse (ISO 8601)",
        example="2024-10-27T10:05:00Z"
    )
    
    min_impact_threshold: float = Field(
        default=MIN_IMPACT_SCORE,
        ge=0.0,
        le=1.0,
        description="Minimaler Impact Score für Inclusion (0-1)",
        example=0.1
    )
    
    top_n_wallets: int = Field(
        default=DEFAULT_TOP_N_WALLETS,
        ge=1,
        le=MAX_TOP_N_WALLETS,
        description="Anzahl Top Wallets im Response",
        example=10
    )
    
    include_trades: bool = Field(
        default=False,
        description="Einzelne Trades in Response inkludieren"
    )
    
    @validator("exchange")
    def validate_exchange(cls, v):
        """Validiere Exchange"""
        if v.lower() not in SUPPORTED_EXCHANGES:
            raise ValueError(
                ERROR_MESSAGES["unsupported_exchange"].format(
                    exchange=v,
                    exchanges=", ".join(SUPPORTED_EXCHANGES)
                )
            )
        return v.lower()
    
    @validator("timeframe")
    def validate_timeframe(cls, v):
        """Validiere Timeframe"""
        if v.lower() not in SUPPORTED_TIMEFRAMES:
            raise ValueError(
                ERROR_MESSAGES["unsupported_timeframe"].format(
                    timeframe=v,
                    timeframes=", ".join(SUPPORTED_TIMEFRAMES)
                )
            )
        return v.lower()
    
    @validator("symbol")
    def validate_symbol(cls, v):
        """Validiere Trading Pair Format"""
        if "/" not in v:
            raise ValueError(
                ERROR_MESSAGES["invalid_symbol"].format(symbol=v)
            )
        
        parts = v.split("/")
        if len(parts) != 2 or not all(parts):
            raise ValueError(
                ERROR_MESSAGES["invalid_symbol"].format(symbol=v)
            )
        
        return v.upper()
    
    @root_validator
    def validate_time_range(cls, values):
        """Validiere Zeitspanne"""
        start_time = values.get("start_time")
        end_time = values.get("end_time")
        
        if start_time and end_time:
            # Prüfe ob end_time nach start_time liegt
            if end_time <= start_time:
                raise ValueError(ERROR_MESSAGES["invalid_time_range"])
            
            # Prüfe maximale Zeitspanne
            time_diff = end_time - start_time
            hours_diff = time_diff.total_seconds() / 3600
            
            if hours_diff > MAX_ANALYSIS_TIMESPAN_HOURS:
                raise ValueError(
                    ERROR_MESSAGES["time_range_too_large"].format(
                        max_hours=MAX_ANALYSIS_TIMESPAN_HOURS
                    )
                )
        
        return values
    
    class Config:
        schema_extra = {
            "example": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "start_time": "2024-10-27T10:00:00Z",
                "end_time": "2024-10-27T10:05:00Z",
                "min_impact_threshold": 0.1,
                "top_n_wallets": 10,
                "include_trades": False
            }
        }


class QuickAnalysisRequest(BaseModel):
    """
    Vereinfachter Request für Quick Analysis
    
    Analysiert die letzte Candle für ein Symbol
    """
    
    exchange: str = Field(
        ...,
        description="Exchange für Analyse",
        example="binance"
    )
    
    symbol: str = Field(
        ...,
        description="Trading Pair (Format: BASE/QUOTE)",
        example="BTC/USDT"
    )
    
    timeframe: str = Field(
        default="5m",
        description="Candle Timeframe",
        example="5m"
    )
    
    top_n_wallets: int = Field(
        default=DEFAULT_TOP_N_WALLETS,
        ge=1,
        le=MAX_TOP_N_WALLETS,
        description="Anzahl Top Wallets im Response"
    )
    
    @validator("exchange")
    def validate_exchange(cls, v):
        """Validiere Exchange"""
        if v.lower() not in SUPPORTED_EXCHANGES:
            raise ValueError(
                ERROR_MESSAGES["unsupported_exchange"].format(
                    exchange=v,
                    exchanges=", ".join(SUPPORTED_EXCHANGES)
                )
            )
        return v.lower()
    
    @validator("timeframe")
    def validate_timeframe(cls, v):
        """Validiere Timeframe"""
        if v.lower() not in SUPPORTED_TIMEFRAMES:
            raise ValueError(
                ERROR_MESSAGES["unsupported_timeframe"].format(
                    timeframe=v,
                    timeframes=", ".join(SUPPORTED_TIMEFRAMES)
                )
            )
        return v.lower()
    
    @validator("symbol")
    def validate_symbol(cls, v):
        """Validiere Trading Pair Format"""
        if "/" not in v:
            raise ValueError(
                ERROR_MESSAGES["invalid_symbol"].format(symbol=v)
            )
        return v.upper()
    
    class Config:
        schema_extra = {
            "example": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "top_n_wallets": 10
            }
        }
