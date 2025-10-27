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
WHALE_THRESHOLD_USD: float = 100_000.0  # 100k USD per Trade
SMART_MONEY_THRESHOLD_USD: float = 50_000.0  # 50k USD per Trade
BOT_MIN_TRADES: int = 10  # Mindestanzahl Trades für Bot-Erkennung


# ==================== IMPACT SCORE WEIGHTS ====================

# Gewichtung der verschiedenen Faktoren für Impact Score (Summe = 1.0)
IMPACT_SCORE_WEIGHTS: Dict[str, float] = {
    "volume_ratio": 0.30,        # Anteil am Gesamt-Volume
    "timing_score": 0.25,        # Timing vor Preisbewegungen
    "size_impact": 0.20,         # Order-Größe vs Orderbook
    "price_correlation": 0.15,   # Korrelation mit Preisbewegung
    "slippage_caused": 0.10,     # Verursachter Slippage
}


# ==================== ANALYSIS THRESHOLDS ====================

# Minimale Preisbewegung für Analyse (in %)
MIN_PRICE_MOVEMENT_PCT: float = 0.1  # 0.1% Minimum

# Minimaler Impact Score für Inclusion (0-1)
MIN_IMPACT_SCORE: float = 0.05  # 5% Impact

# Standard Anzahl Top Wallets im Response
DEFAULT_TOP_N_WALLETS: int = 10
MAX_TOP_N_WALLETS: int = 100

# Minimales Trade Volume für Berücksichtigung (USD)
MIN_TRADE_VOLUME_USD: float = 1000.0  # 1k USD


# ==================== TIMING CORRELATION ====================

# Zeitfenster für Timing-Analyse (in Sekunden)
TIMING_WINDOW_BEFORE_MOVE: int = 60  # 60 Sekunden vor Bewegung
TIMING_WINDOW_AFTER_MOVE: int = 30   # 30 Sekunden nach Bewegung

# Threshold für signifikante Preisbewegung (in %)
SIGNIFICANT_PRICE_MOVE_PCT: float = 0.5  # 0.5%


# ==================== PATTERN DETECTION ====================

# Bot-Erkennung Parameter
BOT_DETECTION: Dict[str, any] = {
    "min_trades": 10,                    # Min. Trades für Bot-Pattern
    "max_time_variance_seconds": 5,      # Max. Zeitabweichung zwischen Trades
    "min_size_consistency": 0.85,        # Min. Konsistenz der Trade-Größen (85%)
    "round_number_threshold": 0.70,      # % Trades mit runden Zahlen
}

# Market Maker Erkennung
MARKET_MAKER_DETECTION: Dict[str, any] = {
    "min_bid_ask_trades": 5,             # Min. Trades auf beiden Seiten
    "max_spread_pct": 0.1,               # Max. Spread zwischen Bid/Ask (0.1%)
    "min_volume_both_sides": 10_000,     # Min. Volume auf beiden Seiten (USD)
}


# ==================== CACHE SETTINGS ====================

# Cache TTL in Sekunden
CACHE_TTL: Dict[str, int] = {
    "candle_data": 300,          # 5 Minuten
    "trade_data": 180,           # 3 Minuten
    "analysis_result": 600,      # 10 Minuten
    "known_wallets": 3600,       # 1 Stunde
}


# ==================== API RATE LIMITS ====================

# Rate Limits pro Exchange (Requests pro Minute)
EXCHANGE_RATE_LIMITS: Dict[str, int] = {
    SupportedExchange.BITGET: 20,
    SupportedExchange.BINANCE: 20,
    SupportedExchange.KRAKEN: 15,
}


# ==================== DATA VALIDATION ====================

# Maximale Zeitspanne für Analyse (in Stunden)
MAX_ANALYSIS_TIMESPAN_HOURS: int = 24

# Maximale Anzahl Trades für Verarbeitung
MAX_TRADES_PER_REQUEST: int = 100_000


# ==================== EXCHANGE SPECIFIC CONFIGS ====================

EXCHANGE_CONFIGS: Dict[str, Dict[str, any]] = {
    SupportedExchange.BITGET: {
        "name": "Bitget",
        "has_trade_history": True,
        "has_orderbook": True,
        "min_trade_amount": 10.0,  # USD
        "rate_limit": 20,
    },
    SupportedExchange.BINANCE: {
        "name": "Binance",
        "has_trade_history": True,
        "has_orderbook": True,
        "min_trade_amount": 10.0,  # USD
        "rate_limit": 20,
    },
    SupportedExchange.KRAKEN: {
        "name": "Kraken",
        "has_trade_history": True,
        "has_orderbook": True,
        "min_trade_amount": 10.0,  # USD
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


# ==================== LOGGING ====================

LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL: str = "INFO"
