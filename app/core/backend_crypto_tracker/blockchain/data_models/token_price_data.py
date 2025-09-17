"""
Data model for token price information.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional


@dataclass
class TokenPriceData:
    price: float
    market_cap: float
    volume_24h: float
    price_change_percentage_24h: Optional[float] = None
    source: str = ""  # Welche API hat die Daten geliefert
    # Erweiterte Felder für bessere Nutzung der APIs
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    last_updated: Optional[datetime] = None
    # Historische Daten
    historical_prices: Optional[Dict[str, float]] = None  # Zeitraum -> Preis
    # Token-Metadaten
    token_name: Optional[str] = None
    token_symbol: Optional[str] = None
    description: Optional[str] = None
    website: Optional[str] = None
    social_links: Optional[Dict[str, str]] = None
    # On-chain Daten
    liquidity: Optional[float] = None
    unique_traders_24h: Optional[int] = None
    # Orderbuch-Daten
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    bid_volume: Optional[float] = None
    ask_volume: Optional[float] = None
