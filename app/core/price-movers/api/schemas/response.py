"""
Response Schemas für Price Movers API

Pydantic Models für API Responses
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

from ...utils.constants import WalletType


class CandleData(BaseModel):
    """
    Candle/OHLCV Daten
    
    Attributes:
        timestamp: Zeitpunkt der Candle
        open: Eröffnungskurs
        high: Höchstkurs
        low: Tiefstkurs
        close: Schlusskurs
        volume: Handelsvolumen
        price_change_pct: Prozentuale Preisänderung
    """
    
    timestamp: datetime = Field(
        ...,
        description="Zeitpunkt der Candle"
    )
    
    open: float = Field(
        ...,
        description="Eröffnungskurs"
    )
    
    high: float = Field(
        ...,
        description="Höchstkurs"
    )
    
    low: float = Field(
        ...,
        description="Tiefstkurs"
    )
    
    close: float = Field(
        ...,
        description="Schlusskurs"
    )
    
    volume: float = Field(
        ...,
        description="Handelsvolumen"
    )
    
    price_change_pct: float = Field(
        ...,
        description="Prozentuale Preisänderung (%)"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "timestamp": "2024-10-27T10:00:00Z",
                "open": 67500.00,
                "high": 67800.00,
                "low": 67450.00,
                "close": 67750.00,
                "volume": 1234.56,
                "price_change_pct": 0.37
            }
        }


class TradeData(BaseModel):
    """
    Einzelner Trade
    
    Attributes:
        timestamp: Zeitpunkt des Trades
        trade_type: Typ (buy/sell)
        amount: Handelsvolumen
        price: Preis
        price_impact_est: Geschätzter Price Impact (%)
        value_usd: Wert in USD
    """
    
    timestamp: datetime = Field(
        ...,
        description="Zeitpunkt des Trades"
    )
    
    trade_type: str = Field(
        ...,
        description="Trade Typ (buy/sell)"
    )
    
    amount: float = Field(
        ...,
        description="Handelsvolumen"
    )
    
    price: float = Field(
        ...,
        description="Ausführungspreis"
    )
    
    price_impact_est: float = Field(
        ...,
        description="Geschätzter Price Impact (%)"
    )
    
    value_usd: float = Field(
        ...,
        description="Wert in USD"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "timestamp": "2024-10-27T10:01:23Z",
                "trade_type": "buy",
                "amount": 5.5,
                "price": 67520.00,
                "price_impact_est": 0.15,
                "value_usd": 371360.00
            }
        }


class WalletMover(BaseModel):
    """
    Wallet mit Impact auf Preisbewegung
    
    Attributes:
        wallet_id: Wallet Identifier (virtuell bei CEX)
        wallet_type: Klassifizierung des Wallets
        impact_score: Impact Score (0-1)
        total_volume: Gesamtvolumen
        total_value_usd: Gesamtwert in USD
        trade_count: Anzahl Trades
        avg_trade_size: Durchschnittliche Trade-Größe
        timing_score: Timing Score (0-1)
        volume_ratio: Anteil am Gesamtvolumen
        trades: Liste einzelner Trades (optional)
    """
    
    wallet_id: str = Field(
        ...,
        description="Wallet Identifier (Pattern-basiert bei CEX)"
    )
    
    wallet_type: WalletType = Field(
        ...,
        description="Wallet-Typ Klassifizierung"
    )
    
    impact_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Impact Score (0-1)"
    )
    
    total_volume: float = Field(
        ...,
        description="Gesamtvolumen"
    )
    
    total_value_usd: float = Field(
        ...,
        description="Gesamtwert in USD"
    )
    
    trade_count: int = Field(
        ...,
        description="Anzahl Trades"
    )
    
    avg_trade_size: float = Field(
        ...,
        description="Durchschnittliche Trade-Größe"
    )
    
    timing_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Timing Score - Trades vor Preisbewegungen (0-1)"
    )
    
    volume_ratio: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Anteil am Gesamtvolumen (0-1)"
    )
    
    trades: Optional[List[TradeData]] = Field(
        default=None,
        description="Liste einzelner Trades (wenn requested)"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "wallet_id": "whale_0x742d35",
                "wallet_type": "whale",
                "impact_score": 0.85,
                "total_volume": 50.5,
                "total_value_usd": 3408750.00,
                "trade_count": 12,
                "avg_trade_size": 4.21,
                "timing_score": 0.92,
                "volume_ratio": 0.041,
                "trades": None
            }
        }


class AnalysisMetadata(BaseModel):
    """
    Metadaten zur Analyse
    
    Attributes:
        total_unique_wallets: Anzahl unique Wallets
        total_volume: Gesamtvolumen in der Candle
        total_trades: Gesamtanzahl Trades
        analysis_duration_ms: Analyse-Dauer in Millisekunden
        data_sources: Verwendete Datenquellen
        timestamp: Zeitpunkt der Analyse
    """
    
    total_unique_wallets: int = Field(
        ...,
        description="Anzahl unique Wallets/Pattern"
    )
    
    total_volume: float = Field(
        ...,
        description="Gesamtvolumen in der Candle"
    )
    
    total_trades: int = Field(
        ...,
        description="Gesamtanzahl Trades"
    )
    
    analysis_duration_ms: int = Field(
        ...,
        description="Analyse-Dauer in Millisekunden"
    )
    
    data_sources: List[str] = Field(
        ...,
        description="Verwendete Datenquellen"
    )
    
    timestamp: datetime = Field(
        ...,
        description="Zeitpunkt der Analyse"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "total_unique_wallets": 1523,
                "total_volume": 1234.56,
                "total_trades": 8432,
                "analysis_duration_ms": 450,
                "data_sources": ["binance_trades", "binance_candles"],
                "timestamp": "2024-10-27T10:06:00Z"
            }
        }


class AnalysisResponse(BaseModel):
    """
    Haupt-Response für Price Movers Analyse
    
    Attributes:
        candle: Candle-Daten
        top_movers: Top Wallets mit Impact
        analysis_metadata: Metadaten zur Analyse
    """
    
    candle: CandleData = Field(
        ...,
        description="Analysierte Candle-Daten"
    )
    
    top_movers: List[WalletMover] = Field(
        ...,
        description="Top Wallets mit größtem Impact"
    )
    
    analysis_metadata: AnalysisMetadata = Field(
        ...,
        description="Metadaten zur Analyse"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "candle": {
                    "timestamp": "2024-10-27T10:00:00Z",
                    "open": 67500.00,
                    "high": 67800.00,
                    "low": 67450.00,
                    "close": 67750.00,
                    "volume": 1234.56,
                    "price_change_pct": 0.37
                },
                "top_movers": [
                    {
                        "wallet_id": "whale_0x742d35",
                        "wallet_type": "whale",
                        "impact_score": 0.85,
                        "total_volume": 50.5,
                        "total_value_usd": 3408750.00,
                        "trade_count": 12,
                        "avg_trade_size": 4.21,
                        "timing_score": 0.92,
                        "volume_ratio": 0.041
                    }
                ],
                "analysis_metadata": {
                    "total_unique_wallets": 1523,
                    "total_volume": 1234.56,
                    "total_trades": 8432,
                    "analysis_duration_ms": 450,
                    "data_sources": ["binance_trades", "binance_candles"],
                    "timestamp": "2024-10-27T10:06:00Z"
                }
            }
        }


class ErrorResponse(BaseModel):
    """
    Error Response
    
    Attributes:
        error: Error Typ
        message: Error Message
        details: Zusätzliche Details (optional)
    """
    
    error: str = Field(
        ...,
        description="Error Typ"
    )
    
    message: str = Field(
        ...,
        description="Error Message"
    )
    
    details: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Zusätzliche Error Details"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "error": "ValidationError",
                "message": "Exchange 'invalid' wird nicht unterstützt",
                "details": {
                    "field": "exchange",
                    "supported_exchanges": ["bitget", "binance", "kraken"]
                }
            }
        }
