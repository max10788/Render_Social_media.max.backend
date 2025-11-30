from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AnyUrl, field_validator
from typing import Optional, Dict, Any
from functools import lru_cache
import os
from dotenv import load_dotenv

load_dotenv()


class MetricsConfig:
    """Metrics configuration."""
    METRICS_WINDOW_SIZE = 3600  # 1 hour
    ALERT_THRESHOLD = 0.1  # 10%


class SolanaConfig:
    def __init__(self):
        self.primary_rpc_url = os.getenv("SOLANA_RPC_URL")
        self.fallback_rpc_urls = self._get_fallback_rpc_urls()
        self.rate_limit_rate = int(os.getenv("SOLANA_RATE_LIMIT_RATE", "50"))
        self.rate_limit_capacity = os.getenv("SOLANA_RATE_LIMIT_CAPACITY", "100")
        self.health_check_interval = int(os.getenv("SOLANA_HEALTH_CHECK_INTERVAL", "60"))

    def _get_fallback_rpc_urls(self) -> list[str]:
        fallback_urls_str = os.getenv("SOLANA_FALLBACK_RPC_URLS", "")
        return [url.strip() for url in fallback_urls_str.split(",")] if fallback_urls_str else []


class Settings(BaseSettings):
    # Existing Settings
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./crypto_tracker.db")
    REDIS_URL: Optional[str] = os.getenv("REDIS_URL")
    
    # Twitter API Settings
    TWITTER_BEARER_TOKEN: Optional[str] = os.getenv("TWITTER_BEARER_TOKEN")
    TWITTER_API_KEY: Optional[str] = os.getenv("TWITTER_API_KEY")
    TWITTER_API_SECRET: Optional[str] = os.getenv("TWITTER_API_SECRET")
    TWITTER_ACCESS_TOKEN: Optional[str] = os.getenv("TWITTER_ACCESS_TOKEN")
    TWITTER_ACCESS_SECRET: Optional[str] = os.getenv("TWITTER_ACCESS_SECRET")
    
    # Blockchain RPC URLs
    SOLANA_RPC_URL: str = os.getenv("SOLANA_RPC_URL", "https://api.devnet.solana.com")
    ETHEREUM_RPC_URL: Optional[str] = os.getenv("ETHEREUM_RPC_URL")
    BINANCE_RPC_URL: Optional[str] = os.getenv("BINANCE_RPC_URL")
    POLYGON_RPC_URL: Optional[str] = os.getenv("POLYGON_RPC_URL")
    
    # API Keys
    MORALIS_API_KEY: Optional[str] = os.getenv("MORALIS_API_KEY")
    ETHERSCAN_API_KEY: Optional[str] = os.getenv("ETHERSCAN_API_KEY")
    COINGECKO_API_KEY: Optional[str] = os.getenv("COINGECKO_API_KEY")
    
    # Application Settings
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Social Media & Blockchain Analysis"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # Security Settings
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-here")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Cache Settings
    CACHE_TTL: int = 3600
    
    # New Settings for Solana Tracking
    MAX_CHAIN_DEPTH: int = 20
    TRANSACTION_BATCH_SIZE: int = 100
    MIN_AMOUNT_SOL: float = 0.000001
    ENABLE_CACHING: bool = True
    ENABLE_RATE_LIMITING: bool = True

    @field_validator("SOLANA_RPC_URL")
    @classmethod
    def validate_rpc_url(cls, v: str) -> str:
        if not v:
            raise ValueError("SOLANA_RPC_URL must be set")
        return v

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()


def validate_settings() -> None:
    """Validate all required settings."""
    required = {
        "DATABASE_URL": settings.DATABASE_URL,
        "SOLANA_RPC_URL": settings.SOLANA_RPC_URL,
        "TWITTER_BEARER_TOKEN": settings.TWITTER_BEARER_TOKEN,
        "SECRET_KEY": settings.SECRET_KEY
    }
    
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise ValueError(f"Missing required settings: {', '.join(missing)}")


try:
    validate_settings()
except ValueError as e:
    import logging
    logging.warning(f"Settings validation warning: {str(e)}")
