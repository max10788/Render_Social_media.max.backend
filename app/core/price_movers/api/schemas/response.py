"""
Response Schemas für Price Movers API

Pydantic Models für API Responses
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))


class CandleData(BaseModel):
    """OHLCV Candle Daten"""
    timestamp: datetime = Field(..., description="Candle Zeitstempel")
    open: float = Field(..., description="Open Preis")
    high: float = Field(..., description="High Preis")
    low: float = Field(..., description="Low Preis")
    close: float = Field(..., description="Close Preis")
    volume: float = Field(..., description="Volume")
    price_change_pct: float = Field(..., description="Preisänderung in %")


class TradeData(BaseModel):
    """Einzelner Trade"""
    timestamp: datetime = Field(..., description="Trade Zeitstempel")
    trade_type: str = Field(..., description="Trade Typ (buy/sell)")
    amount: float = Field(..., description="Trade Amount")
    price: float = Field(..., description="Trade Preis")
    value_usd: float = Field(..., description="Trade Wert in USD")
    price_impact_est: float = Field(default=0.0, description="Geschätzter Price Impact")


class ImpactComponents(BaseModel):
    """Impact Score Komponenten"""
    volume_ratio: float = Field(..., description="Volumen-Anteil")
    timing_score: float = Field(..., description="Timing Score")
    size_impact: float = Field(..., description="Size Impact")
    price_correlation: float = Field(..., description="Preis-Korrelation")
    slippage_caused: float = Field(..., description="Verursachter Slippage")


class WalletMover(BaseModel):
    """Top Wallet mit Impact-Daten"""
    wallet_id: str = Field(..., description="Wallet Identifier")
    wallet_type: str = Field(..., description="Wallet Typ (whale/smart_money/bot/etc.)")
    impact_score: float = Field(..., description="Gesamt Impact Score (0-1)")
    impact_level: Optional[str] = Field(None, description="Impact Level (low/medium/high/very_high)")
    total_volume: float = Field(..., description="Gesamt-Volume des Wallets")
    total_value_usd: float = Field(..., description="Gesamt-Wert in USD")
    trade_count: int = Field(..., description="Anzahl Trades")
    avg_trade_size: float = Field(..., description="Durchschnittliche Trade-Größe")
    timing_score: float = Field(..., description="Timing Score")
    volume_ratio: float = Field(..., description="Anteil am Gesamt-Volume")
    components: Optional[ImpactComponents] = Field(None, description="Detaillierte Impact-Komponenten")
    trades: Optional[List[TradeData]] = Field(None, description="Einzelne Trades (optional)")
    
    class Config:
        schema_extra = {
            "example": {
                "wallet_id": "whale_0x742d35",
                "wallet_type": "whale",
                "impact_score": 0.857,
                "impact_level": "very_high",
                "total_volume": 125.45,
                "total_value_usd": 8_478_750.0,
                "trade_count": 5,
                "avg_trade_size": 25.09,
                "timing_score": 0.92,
                "volume_ratio": 0.156
            }
        }


class AnalysisMetadata(BaseModel):
    """Metadaten der Analyse"""
    total_unique_wallets: int = Field(..., description="Anzahl eindeutiger Wallets")
    total_volume: float = Field(..., description="Gesamt-Volume")
    total_trades: int = Field(..., description="Gesamt-Anzahl Trades")
    analysis_duration_ms: int = Field(..., description="Analyse-Dauer in Millisekunden")
    data_sources: List[str] = Field(..., description="Verwendete Datenquellen")
    timestamp: datetime = Field(..., description="Zeitstempel der Analyse")
    
    class Config:
        schema_extra = {
            "example": {
                "total_unique_wallets": 47,
                "total_volume": 805.32,
                "total_trades": 156,
                "analysis_duration_ms": 234,
                "data_sources": ["binance_trades", "binance_candles"],
                "timestamp": "2024-10-27T10:05:32Z"
            }
        }


class AnalysisResponse(BaseModel):
    """Haupt-Response für Analyse"""
    candle: CandleData = Field(..., description="Candle-Daten")
    top_movers: List[WalletMover] = Field(..., description="Top Wallets nach Impact")
    analysis_metadata: AnalysisMetadata = Field(..., description="Analyse-Metadaten")
    
    class Config:
        schema_extra = {
            "example": {
                "candle": {
                    "timestamp": "2024-10-27T10:00:00Z",
                    "open": 67500.0,
                    "high": 67800.0,
                    "low": 67450.0,
                    "close": 67750.0,
                    "volume": 1234.56,
                    "price_change_pct": 0.37
                },
                "top_movers": [
                    {
                        "wallet_id": "whale_0x742d35",
                        "wallet_type": "whale",
                        "impact_score": 0.857,
                        "total_volume": 125.45,
                        "total_value_usd": 8_478_750.0,
                        "trade_count": 5,
                        "avg_trade_size": 25.09,
                        "timing_score": 0.92,
                        "volume_ratio": 0.156
                    }
                ],
                "analysis_metadata": {
                    "total_unique_wallets": 47,
                    "total_volume": 805.32,
                    "total_trades": 156,
                    "analysis_duration_ms": 234,
                    "data_sources": ["binance_trades", "binance_candles"],
                    "timestamp": "2024-10-27T10:05:32Z"
                }
            }
        }


class HistoricalAnalysisResponse(BaseModel):
    """Response für historische Analyse über mehrere Candles"""
    period_start: datetime = Field(..., description="Start-Zeitpunkt")
    period_end: datetime = Field(..., description="End-Zeitpunkt")
    total_candles: int = Field(..., description="Anzahl analysierter Candles")
    aggregated_movers: List[WalletMover] = Field(..., description="Aggregierte Top Movers")
    summary: Dict[str, Any] = Field(..., description="Zusammenfassung")
    
    class Config:
        schema_extra = {
            "example": {
                "period_start": "2024-10-27T10:00:00Z",
                "period_end": "2024-10-27T12:00:00Z",
                "total_candles": 24,
                "aggregated_movers": [],
                "summary": {
                    "total_volume": 29_456.78,
                    "total_trades": 3_542,
                    "unique_wallets": 234,
                    "avg_price_change_pct": 0.45
                }
            }
        }


class WalletDetailResponse(BaseModel):
    """Detaillierte Wallet-Informationen"""
    wallet_id: str = Field(..., description="Wallet ID")
    wallet_type: str = Field(..., description="Wallet Typ")
    first_seen: datetime = Field(..., description="Erste Aktivität")
    last_seen: datetime = Field(..., description="Letzte Aktivität")
    total_trades: int = Field(..., description="Gesamt-Anzahl Trades")
    total_volume: float = Field(..., description="Gesamt-Volume")
    total_value_usd: float = Field(..., description="Gesamt-Wert in USD")
    avg_impact_score: float = Field(..., description="Durchschnittlicher Impact Score")
    recent_trades: List[TradeData] = Field(..., description="Letzte Trades")
    statistics: Dict[str, Any] = Field(..., description="Statistiken")
    
    class Config:
        schema_extra = {
            "example": {
                "wallet_id": "whale_0x742d35",
                "wallet_type": "whale",
                "first_seen": "2024-10-20T14:23:00Z",
                "last_seen": "2024-10-27T10:05:00Z",
                "total_trades": 127,
                "total_volume": 3_456.78,
                "total_value_usd": 233_456_789.0,
                "avg_impact_score": 0.673,
                "recent_trades": [],
                "statistics": {
                    "avg_trade_size": 27.2,
                    "buy_sell_ratio": 1.34,
                    "active_days": 7
                }
            }
        }


class ExchangeComparison(BaseModel):
    """Vergleich mehrerer Exchanges"""
    symbol: str = Field(..., description="Trading Pair")
    timeframe: str = Field(..., description="Timeframe")
    timestamp: datetime = Field(..., description="Zeitstempel")
    exchanges: Dict[str, Any] = Field(..., description="Exchange-Daten")
    best_price: Dict[str, Any] = Field(..., description="Bester Preis")
    highest_volume: Dict[str, Any] = Field(..., description="Höchstes Volume")
    
    class Config:
        schema_extra = {
            "example": {
                "symbol": "BTC/USDT",
                "timeframe": "5m",
                "timestamp": "2024-10-27T10:05:00Z",
                "exchanges": {
                    "binance": {
                        "price": 67750.0,
                        "volume": 1234.56,
                        "bid": 67749.0,
                        "ask": 67751.0,
                        "spread": 2.0
                    }
                },
                "best_price": {
                    "exchange": "binance",
                    "price": 67750.0
                },
                "highest_volume": {
                    "exchange": "binance",
                    "volume": 1234.56
                }
            }
        }


class HealthCheckResponse(BaseModel):
    """Health Check Response"""
    status: str = Field(..., description="Gesamt-Status (healthy/degraded/unhealthy)")
    timestamp: datetime = Field(..., description="Zeitstempel")
    exchanges: Dict[str, bool] = Field(..., description="Exchange-Status")
    version: str = Field(..., description="API Version")
    
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
    """Error Response"""
    error: str = Field(..., description="Error Message")
    detail: Optional[str] = Field(None, description="Detaillierte Beschreibung")
    timestamp: datetime = Field(default_factory=datetime.now, description="Zeitstempel")
    
    class Config:
        schema_extra = {
            "example": {
                "error": "Exchange unavailable",
                "detail": "Failed to connect to Binance API",
                "timestamp": "2024-10-27T10:05:00Z"
            }
        }


class SuccessResponse(BaseModel):
    """Generic Success Response"""
    message: str = Field(..., description="Success Message")
    data: Optional[Dict[str, Any]] = Field(None, description="Optional Data")
    timestamp: datetime = Field(default_factory=datetime.now, description="Zeitstempel")
    
    class Config:
        schema_extra = {
            "example": {
                "message": "Operation successful",
                "data": {"key": "value"},
                "timestamp": "2024-10-27T10:05:00Z"
            }
        }
