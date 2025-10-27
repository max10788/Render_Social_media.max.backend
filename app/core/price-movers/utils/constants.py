"""
Konstanten und Konfigurationen für das Price-Movers Analyse Tool
"""
from enum import Enum
from typing import Dict, List


# =============================================================================
# SUPPORTED EXCHANGES
# =============================================================================

class SupportedExchange(str, Enum):
    """Unterstützte Börsen für Analyse"""
    BITGET = "bitget"
    BINANCE = "binance"
    KRAKEN = "kraken"


SUPPORTED_EXCHANGES: List[str] = [
    SupportedExchange.BITGET,
    SupportedExchange.BINANCE,
    SupportedExchange.KRAKEN
]


# =============================================================================
# TIMEFRAMES
# =============================================================================

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
    Timeframe.ONE_DAY
]

# Timeframe zu Sekunden Mapping
TIMEFRAME_TO_SECONDS: Dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400
}


# =============================================================================
# WALLET CLASSIFICATION THRESHOLDS
# =============================================================================

class WalletType(str, Enum):
    """Wallet-Typen Klassifizierung"""
    WHALE = "whale"
    SMART_MONEY = "smart_money"
    BOT = "bot"
    MARKET_MAKER = "market_maker"
    RETAIL = "retail"
    UNKNOWN = "unknown"


# Volume-basierte Thresholds (in BTC-Äquivalent)
WHALE_VOLUME_THRESHOLD_BTC = 10.0  # > 10 BTC = Whale
LARGE_TRADER_THRESHOLD_BTC = 2.0   # > 2 BTC = Large Trader

# Trade-Frequenz Thresholds für Bot-Erkennung
BOT_TRADE_FREQUENCY_MIN = 10  # Min. 10 Trades im Zeitfenster
BOT_AVG_TIME_BETWEEN_TRADES_SEC = 5  # < 5 Sekunden zwischen Trades

# Smart Money Indikatoren
SMART_MONEY_TIMING_THRESHOLD = 0.75  # Timing Score > 0.75
SMART_MONEY_WIN_RATE_THRESHOLD = 0.60  # Win Rate > 60%


# =============================================================================
# IMPACT SCORE WEIGHTS
# =============================================================================

class ImpactScoreWeights:
    """
    Gewichtung der verschiedenen Faktoren für Impact Score Berechnung
    Summe sollte 1.0 ergeben
    """
    VOLUME_RATIO = 0.30        # 30% - Anteil am Gesamtvolumen
    TIMING_CORRELATION = 0.25  # 25% - Zeitliche Korrelation mit Preisbewegung
    SIZE_IMPACT = 0.20         # 20% - Order Size vs. Orderbook Depth
    PRICE_CORRELATION = 0.15   # 15% - Korrelation Trade -> Price Move
    SLIPPAGE_CAUSED = 0.10     # 10% - Geschätzter Slippage


# =============================================================================
# ANALYSIS PARAMETERS
# =============================================================================

# Minimum Impact Thresholds
MIN_IMPACT_SCORE = 0.01  # Minimum 1% Impact Score
DEFAULT_MIN_IMPACT_THRESHOLD = 0.1  # Default: 10% Impact

# Top N Wallets
DEFAULT_TOP_N_WALLETS = 10
MAX_TOP_N_WALLETS = 100

# Minimum Preisbewegung für Analyse (in %)
MIN_PRICE_MOVEMENT_PERCENT = 0.1  # 0.1% Bewegung


# =============================================================================
# DATA COLLECTION PARAMETERS
# =============================================================================

# API Rate Limits (Requests pro Minute)
RATE_LIMITS: Dict[str, int] = {
    SupportedExchange.BINANCE: 1200,  # 1200 req/min
    SupportedExchange.BITGET: 600,    # 600 req/min
    SupportedExchange.KRAKEN: 180     # 180 req/min (15 req/sec * 12)
}

# Maximum Trades pro Request
MAX_TRADES_PER_REQUEST = 1000

# Timeout für API Calls (Sekunden)
API_TIMEOUT_SECONDS = 30

# Retry-Konfiguration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1


# =============================================================================
# CACHING CONFIGURATION
# =============================================================================

# Cache TTL (Time To Live) in Sekunden
CACHE_TTL_CANDLE_DATA = 3600      # 1 Stunde
CACHE_TTL_TRADE_DATA = 300        # 5 Minuten
CACHE_TTL_ANALYSIS_RESULT = 600   # 10 Minuten
CACHE_TTL_WALLET_INFO = 86400     # 24 Stunden

# Cache Key Präfixe
CACHE_PREFIX_CANDLE = "pricemovers:candle"
CACHE_PREFIX_TRADES = "pricemovers:trades"
CACHE_PREFIX_ANALYSIS = "pricemovers:analysis"
CACHE_PREFIX_WALLET = "pricemovers:wallet"


# =============================================================================
# CLUSTERING PARAMETERS (für CEX Virtual Wallets)
# =============================================================================

# Size-basiertes Clustering
SIZE_CLUSTER_TOLERANCE = 0.15  # ±15% für gleiche Cluster
TIME_CLUSTER_WINDOW_SECONDS = 60  # 1 Minute Zeitfenster

# Pattern Recognition
MIN_PATTERN_TRADES = 3  # Minimum 3 Trades für Pattern
PATTERN_SIMILARITY_THRESHOLD = 0.80  # 80% Ähnlichkeit


# =============================================================================
# TRADE CLASSIFICATION
# =============================================================================

class TradeType(str, Enum):
    """Trade-Typen"""
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class TradeSide(str, Enum):
    """Trade Sides (für CCXT Kompatibilität)"""
    BUY = "buy"
    SELL = "sell"


# =============================================================================
# ERROR MESSAGES
# =============================================================================

ERROR_MESSAGES = {
    "unsupported_exchange": "Exchange '{exchange}' wird nicht unterstützt. Verfügbare: {supported}",
    "unsupported_timeframe": "Timeframe '{timeframe}' wird nicht unterstützt. Verfügbare: {supported}",
    "invalid_time_range": "Ungültiger Zeitbereich: start_time muss vor end_time liegen",
    "no_data_found": "Keine Daten für den angegebenen Zeitraum gefunden",
    "api_error": "Fehler beim Abrufen von Daten von {exchange}: {error}",
    "insufficient_data": "Nicht genügend Daten für eine valide Analyse",
    "rate_limit_exceeded": "Rate Limit für {exchange} überschritten. Bitte warten Sie {wait_time}s"
}


# =============================================================================
# VALIDATION RULES
# =============================================================================

# Maximum Zeitspanne für Analyse (in Stunden)
MAX_ANALYSIS_TIMESPAN_HOURS = 24

# Minimum Required Trades für Analyse
MIN_REQUIRED_TRADES = 10

# Minimum Candle Volume (in Quote Currency)
MIN_CANDLE_VOLUME = 1000  # $1000 minimum volume


# =============================================================================
# EXCHANGE-SPECIFIC CONFIGURATIONS
# =============================================================================

EXCHANGE_CONFIGS = {
    SupportedExchange.BINANCE: {
        "base_url": "https://api.binance.com",
        "has_fetch_trades": True,
        "has_fetch_ohlcv": True,
        "trade_pagination": True,
        "max_candles_per_request": 1000,
    },
    SupportedExchange.BITGET: {
        "base_url": "https://api.bitget.com",
        "has_fetch_trades": True,
        "has_fetch_ohlcv": True,
        "trade_pagination": True,
        "max_candles_per_request": 1000,
    },
    SupportedExchange.KRAKEN: {
        "base_url": "https://api.kraken.com",
        "has_fetch_trades": True,
        "has_fetch_ohlcv": True,
        "trade_pagination": True,
        "max_candles_per_request": 720,
    }
}


# =============================================================================
# METRICS & SCORING
# =============================================================================

# Normalisierungs-Parameter
SCORE_NORMALIZATION_METHOD = "minmax"  # oder "zscore"
OUTLIER_REMOVAL_THRESHOLD = 3.0  # Z-Score Threshold


# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# =============================================================================
# FEATURE FLAGS
# =============================================================================

class FeatureFlags:
    """Feature Flags für verschiedene Funktionalitäten"""
    ENABLE_ONCHAIN_DATA = False  # Onchain-Daten erst in Phase 2
    ENABLE_ML_CLASSIFICATION = False  # ML Pattern Recognition erst in Phase 3
    ENABLE_REALTIME_STREAMING = False  # Real-time erst in Phase 3
    ENABLE_ADVANCED_CACHING = True
    ENABLE_RATE_LIMITING = True
