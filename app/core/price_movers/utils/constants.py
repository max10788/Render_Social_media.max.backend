"""
Konstanten und Konfiguration für Price Movers Analyse

Unterstützte Exchanges: Bitget, Binance, Kraken
"""

from enum import Enum
from typing import Dict, List


# ==================== SUPPORTED EXCHANGES ====================

class SupportedExchange(str, Enum):
    """Unterstützte Exchanges für Analyse"""
    BITGET = "bitget"
    BINANCE = "binance"
    KRAKEN = "kraken"


SUPPORTED_EXCHANGES: List[str] = [
    SupportedExchange.BITGET,
    SupportedExchange.BINANCE,
    SupportedExchange.KRAKEN,
]


# ==================== TIMEFRAMES ====================

class Timeframe(str, Enum):
    """Unterstützte Candle-Timeframes"""
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1d"


SUPPORTED_TIMEFRAMES: List[str] = [
    Timeframe.ONE_MINUTE,
    Timeframe.FIVE_MINUTES,
    Timeframe.FIFTEEN_MINUTES,
    Timeframe.THIRTY_MINUTES,
    Timeframe.ONE_HOUR,
    Timeframe.FOUR_HOURS,
    Timeframe.ONE_DAY,
]

# Timeframe zu Millisekunden Mapping
TIMEFRAME_TO_MS: Dict[str, int] = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}


# ==================== WALLET CLASSIFICATION ====================

class WalletType(str, Enum):
    """Wallet-Typen für Klassifizierung"""
    WHALE = "whale"
    SMART_MONEY = "smart_money"
    BOT = "bot"
    MARKET_MAKER = "market_maker"
    RETAIL = "retail"
    UNKNOWN = "unknown"


# Thresholds für Wallet-Klassifizierung (in USD)
WHALE_THRESHOLD_USD: float = 100_000.0
SMART_MONEY_THRESHOLD_USD: float = 50_000.0
BOT_MIN_TRADES: int = 10


# ==================== IMPACT SCORE WEIGHTS ====================

IMPACT_SCORE_WEIGHTS: Dict[str, float] = {
    "volume_ratio": 0.30,
    "timing_score": 0.25,
    "size_impact": 0.20,
    "price_correlation": 0.15,
    "slippage_caused": 0.10,
}


# ==================== ANALYSIS THRESHOLDS ====================

MIN_PRICE_MOVEMENT_PCT: float = 0.1
MIN_IMPACT_SCORE: float = 0.05
DEFAULT_TOP_N_WALLETS: int = 10
MAX_TOP_N_WALLETS: int = 100
MIN_TRADE_VOLUME_USD: float = 1000.0


# ==================== TIMING CORRELATION ====================

TIMING_WINDOW_BEFORE_MOVE: int = 60
TIMING_WINDOW_AFTER_MOVE: int = 30
SIGNIFICANT_PRICE_MOVE_PCT: float = 0.5


# ==================== PATTERN DETECTION ====================

BOT_DETECTION: Dict[str, any] = {
    "min_trades": 10,
    "max_time_variance_seconds": 5,
    "min_size_consistency": 0.85,
    "round_number_threshold": 0.70,
}

MARKET_MAKER_DETECTION: Dict[str, any] = {
    "min_bid_ask_trades": 5,
    "max_spread_pct": 0.1,
    "min_volume_both_sides": 10_000,
}


# ==================== CACHE SETTINGS ====================

CACHE_TTL: Dict[str, int] = {
    "candle_data": 300,
    "trade_data": 180,
    "analysis_result": 600,
    "known_wallets": 3600,
}


# ==================== API RATE LIMITS ====================

EXCHANGE_RATE_LIMITS: Dict[str, int] = {
    SupportedExchange.BITGET: 20,
    SupportedExchange.BINANCE: 20,
    SupportedExchange.KRAKEN: 15,
}


# ==================== DATA VALIDATION ====================

MAX_ANALYSIS_TIMESPAN_HOURS: int = 24
MAX_TRADES_PER_REQUEST: int = 100_000


# ==================== EXCHANGE SPECIFIC CONFIGS ====================

EXCHANGE_CONFIGS: Dict[str, Dict[str, any]] = {
    SupportedExchange.BITGET: {
        "name": "Bitget",
        "has_trade_history": True,
        "has_orderbook": True,
        "min_trade_amount": 10.0,
        "rate_limit": 20,
    },
    SupportedExchange.BINANCE: {
        "name": "Binance",
        "has_trade_history": True,
        "has_orderbook": True,
        "min_trade_amount": 10.0,
        "rate_limit": 20,
    },
    SupportedExchange.KRAKEN: {
        "name": "Kraken",
        "has_trade_history": True,
        "has_orderbook": True,
        "min_trade_amount": 10.0,
        "rate_limit": 15,
    },
}


# ==================== ERROR MESSAGES ====================

ERROR_MESSAGES: Dict[str, str] = {
    "unsupported_exchange": "Exchange '{exchange}' wird nicht unterstützt. Verfügbar: {exchanges}",
    "unsupported_timeframe": "Timeframe '{timeframe}' wird nicht unterstützt. Verfügbar: {timeframes}",
    "invalid_time_range": "Ungültige Zeitspanne: end_time muss nach start_time liegen",
    "time_range_too_large": "Zeitspanne zu groß. Maximum: {max_hours} Stunden",
    "invalid_symbol": "Ungültiges Trading-Pair: {symbol}",
    "no_data_available": "Keine Daten verfügbar für den angegebenen Zeitraum",
    "rate_limit_exceeded": "Rate Limit für {exchange} überschritten. Bitte warten Sie {wait_seconds}s",
}

LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL: str = "INFO"
