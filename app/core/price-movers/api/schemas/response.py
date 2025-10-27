"""
Response Schemas für Price Movers API

Pydantic Models für API Responses
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from app.core.price_movers.utils.constants import WalletType


class CandleData(BaseModel):
    """
    Candle/OHLCV Daten
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
    """
    
    wallet_id: str = Field(
        ...,
        description="Wallet Identifier (Pattern-basiert bei CEX)"
    )
    
    wallet_type: str = Field(
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


class HistoricalAnalysisResponse(BaseModel):
    """
    Response für historische Analyse über mehrere Candles
    """
    
    symbol: str = Field(
        ...,
        description="Trading Pair"
    )
    
    exchange: str = Field(
        ...,
        description="Exchange Name"
    )
    
    timeframe: str = Field(
        ...,
        description="Timeframe"
    )
    
    start_time: datetime = Field(
        ...,
        description="Start-Zeitpunkt"
    )
    
    end_time: datetime = Field(
        ...,
        description="End-Zeitpunkt"
    )
    
    candles_analyzed: int = Field(
        ...,
        description="Anzahl analysierter Candles"
    )
    
    top_movers: List[WalletMover] = Field(
        ...,
        description="Top Wallets über gesamten Zeitraum"
    )
    
    summary: Dict[str, Any] = Field(
        ...,
        description="Zusammenfassung"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "symbol": "BTC/USDT",
                "exchange": "binance",
                "timeframe": "5m",
                "start_time": "2024-10-27T10:00:00Z",
                "end_time": "2024-10-27T12:00:00Z",
                "candles_analyzed": 24,
                "top_movers": [],
                "summary": {
                    "total_volume": 29630.45,
                    "total_trades": 201924,
                    "unique_wallets": 36542,
                    "avg_impact_score": 0.15
                }
            }
        }


class WalletDetailResponse(BaseModel):
    """
    Response für Wallet-Detail Lookup
    """
    
    wallet_id: str = Field(
        ...,
        description="Wallet ID"
    )
    
    wallet_type: str = Field(
        ...,
        description="Wallet-Typ"
    )
    
    first_seen: datetime = Field(
        ...,
        description="Erstmalig gesehen"
    )
    
    last_seen: datetime = Field(
        ...,
        description="Zuletzt gesehen"
    )
    
    total_trades: int = Field(
        ...,
        description="Gesamtanzahl Trades"
    )
    
    total_volume: float = Field(
        ...,
        description="Gesamtvolumen"
    )
    
    total_value_usd: float = Field(
        ...,
        description="Gesamtwert in USD"
    )
    
    avg_impact_score: float = Field(
        ...,
        description="Durchschnittlicher Impact Score"
    )
    
    recent_trades: List[TradeData] = Field(
        ...,
        description="Neueste Trades"
    )
    
    statistics: Dict[str, Any] = Field(
        ...,
        description="Zusätzliche Statistiken"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "wallet_id": "whale_0x742d35",
                "wallet_type": "whale",
                "first_seen": "2024-10-20T08:00:00Z",
                "last_seen": "2024-10-27T10:05:00Z",
                "total_trades": 156,
                "total_volume": 782.5,
                "total_value_usd": 52843750.00,
                "avg_impact_score": 0.78,
                "recent_trades": [],
                "statistics": {
                    "win_rate": 0.65,
                    "avg_trade_size": 5.02,
                    "buy_sell_ratio": 1.15
                }
            }
        }


class ExchangeComparison(BaseModel):
    """
    Response für Exchange-Vergleich
    """
    
    symbol: str = Field(
        ...,
        description="Trading Pair"
    )
    
    timeframe: str = Field(
        ...,
        description="Timeframe"
    )
    
    timestamp: datetime = Field(
        ...,
        description="Zeitpunkt der Analyse"
    )
    
    exchanges: Dict[str, Dict[str, Any]] = Field(
        ...,
        description="Exchange-spezifische Daten"
    )
    
    best_price: Dict[str, Any] = Field(
        ...,
        description="Bester Preis"
    )
    
    highest_volume: Dict[str, Any] = Field(
        ...,
        description="Höchstes Volume"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "timestamp": "2024-10-27T10:05:00Z",
                "exchanges": {
                    "binance": {
                        "price": 67750.00,
                        "volume": 1234.56,
                        "bid": 67749.50,
                        "ask": 67750.50,
                        "spread": 1.00
                    },
                    "bitget": {
                        "price": 67745.00,
                        "volume": 987.32,
                        "bid": 67744.50,
                        "ask": 67745.50,
                        "spread": 1.00
                    },
                    "kraken": {
                        "price": 67755.00,
                        "volume": 654.21,
                        "bid": 67754.00,
                        "ask": 67756.00,
                        "spread": 2.00
                    }
                },
                "best_price": {
                    "exchange": "bitget",
                    "price": 67745.00
                },
                "highest_volume": {
                    "exchange": "binance",
                    "volume": 1234.56
                }
            }
        }


class HealthCheckResponse(BaseModel):
    """
    Response für Health Check
    """
    
    status: str = Field(
        ...,
        description="Status (healthy/unhealthy)"
    )
    
    timestamp: datetime = Field(
        ...,
        description="Zeitpunkt des Checks"
    )
    
    exchanges: Dict[str, bool] = Field(
        ...,
        description="Exchange Status"
    )
    
    version: str = Field(
        ...,
        description="API Version"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2024-10-27T10:05:00Z",
                "exchanges": {
                    "binance": True,
                    "bitget": True,
                    "kraken": True
                },
                "version": "0.1.0"
            }
        }


class ErrorResponse(BaseModel):
    """
    Error Response
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
    
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="Zeitpunkt des Fehlers"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "error": "ValidationError",
                "message": "Exchange 'invalid' wird nicht unterstützt",
                "details": {
                    "field": "exchange",
                    "supported_exchanges": ["bitget", "binance", "kraken"]
                },
                "timestamp": "2024-10-27T10:05:00Z"
            }
        }


class SuccessResponse(BaseModel):
    """
    Generische Success Response
    """
    
    success: bool = Field(
        default=True,
        description="Operation erfolgreich"
    )
    
    message: str = Field(
        ...,
        description="Success Message"
    )
    
    data: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Zusätzliche Daten"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "success": True,
                "message": "Analyse erfolgreich gestartet",
                "data": {
                    "job_id": "abc123",
                    "estimated_duration_seconds": 5
                }
            }
        }
