"""
Pydantic Request Schemas für Price-Movers API
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from ..utils.constants import (
    SupportedExchange,
    Timeframe,
    DEFAULT_TOP_N_WALLETS,
    DEFAULT_MIN_IMPACT_THRESHOLD,
    MAX_TOP_N_WALLETS,
    SUPPORTED_EXCHANGES,
    SUPPORTED_TIMEFRAMES,
    MAX_ANALYSIS_TIMESPAN_HOURS
)


class AnalysisRequest(BaseModel):
    """
    Request Schema für Price-Mover Analyse
    
    Example:
        {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "timeframe": "5m",
            "start_time": "2024-10-27T10:00:00Z",
            "end_time": "2024-10-27T10:05:00Z",
            "min_impact_threshold": 0.1,
            "top_n_wallets": 10
        }
    """
    
    exchange: str = Field(
        ...,
        description="Exchange Name (bitget, binance, kraken)",
        examples=["binance"]
    )
    
    symbol: str = Field(
        ...,
        description="Trading Pair Symbol (z.B. BTC/USDT, ETH/USDT)",
        examples=["BTC/USDT", "ETH/USDT"],
        min_length=3,
        max_length=20
    )
    
    timeframe: str = Field(
        ...,
        description="Candle Timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d)",
        examples=["5m"]
    )
    
    start_time: datetime = Field(
        ...,
        description="Start-Zeitpunkt der Analyse (ISO 8601 Format)"
    )
    
    end_time: datetime = Field(
        ...,
        description="End-Zeitpunkt der Analyse (ISO 8601 Format)"
    )
    
    min_impact_threshold: float = Field(
        default=DEFAULT_MIN_IMPACT_THRESHOLD,
        description="Minimaler Impact Score (0.0 - 1.0) für Wallet-Filterung",
        ge=0.0,
        le=1.0
    )
    
    top_n_wallets: int = Field(
        default=DEFAULT_TOP_N_WALLETS,
        description="Anzahl der Top Wallets im Result",
        ge=1,
        le=MAX_TOP_N_WALLETS
    )
    
    # Optional: Erweiterte Filter
    include_bot_wallets: bool = Field(
        default=True,
        description="Bot-Wallets in Ergebnissen einbeziehen"
    )
    
    include_small_traders: bool = Field(
        default=True,
        description="Kleine Trader (<2 BTC Volumen) einbeziehen"
    )
    
    @field_validator("exchange")
    @classmethod
    def validate_exchange(cls, v: str) -> str:
        """Validiere dass Exchange unterstützt wird"""
        v_lower = v.lower()
        if v_lower not in SUPPORTED_EXCHANGES:
            raise ValueError(
                f"Exchange '{v}' wird nicht unterstützt. "
                f"Verfügbare: {', '.join(SUPPORTED_EXCHANGES)}"
            )
        return v_lower
    
    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, v: str) -> str:
        """Validiere dass Timeframe unterstützt wird"""
        v_lower = v.lower()
        if v_lower not in SUPPORTED_TIMEFRAMES:
            raise ValueError(
                f"Timeframe '{v}' wird nicht unterstützt. "
                f"Verfügbare: {', '.join(SUPPORTED_TIMEFRAMES)}"
            )
        return v_lower
    
    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validiere Symbol Format"""
        v_upper = v.upper()
        
        # Prüfe ob Symbol ein "/" enthält
        if "/" not in v_upper:
            raise ValueError(
                f"Symbol '{v}' muss Format 'BASE/QUOTE' haben (z.B. BTC/USDT)"
            )
        
        # Prüfe ob beide Teile existieren
        parts = v_upper.split("/")
        if len(parts) != 2 or not all(parts):
            raise ValueError(
                f"Ungültiges Symbol Format: '{v}'. Erwartet: 'BASE/QUOTE'"
            )
        
        return v_upper
    
    @model_validator(mode='after')
    def validate_time_range(self):
        """Validiere dass start_time vor end_time liegt und Zeitspanne sinnvoll ist"""
        if self.start_time >= self.end_time:
            raise ValueError(
                "start_time muss vor end_time liegen"
            )
        
        # Prüfe maximale Zeitspanne
        time_diff = self.end_time - self.start_time
        max_hours = MAX_ANALYSIS_TIMESPAN_HOURS
        
        if time_diff.total_seconds() > max_hours * 3600:
            raise ValueError(
                f"Zeitspanne zu groß. Maximum: {max_hours} Stunden"
            )
        
        # Prüfe Minimum Zeitspanne (z.B. mindestens 1 Minute)
        if time_diff.total_seconds() < 60:
            raise ValueError(
                "Zeitspanne muss mindestens 1 Minute betragen"
            )
        
        return self
    
    class Config:
        json_schema_extra = {
            "example": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "start_time": "2024-10-27T10:00:00Z",
                "end_time": "2024-10-27T10:05:00Z",
                "min_impact_threshold": 0.1,
                "top_n_wallets": 10,
                "include_bot_wallets": True,
                "include_small_traders": True
            }
        }


class WalletDetailRequest(BaseModel):
    """
    Request für Detail-Infos zu einem spezifischen Wallet
    (Für späteren /wallets/{address} Endpoint)
    """
    
    wallet_address: str = Field(
        ...,
        description="Wallet Adresse oder Virtual Wallet ID",
        min_length=8,
        max_length=100
    )
    
    exchange: Optional[str] = Field(
        None,
        description="Optional: Exchange für CEX-spezifische Daten"
    )
    
    include_history: bool = Field(
        default=False,
        description="Historische Aktivitäten einbeziehen"
    )
    
    history_days: int = Field(
        default=7,
        description="Anzahl Tage für historische Daten",
        ge=1,
        le=90
    )


class BatchAnalysisRequest(BaseModel):
    """
    Batch-Request für mehrere Analysen gleichzeitig
    (Für zukünftige Erweiterung)
    """
    
    requests: list[AnalysisRequest] = Field(
        ...,
        description="Liste von Analyse-Requests",
        min_length=1,
        max_length=10
    )
    
    parallel_execution: bool = Field(
        default=True,
        description="Requests parallel ausführen"
    )
