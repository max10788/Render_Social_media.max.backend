# app/core/price_movers/collectors/dexscreener_collector.py

import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import os
from .base import BaseCollector

logger = logging.getLogger(__name__)

class DexscreenerCollector(BaseCollector):
    """
    DEX Data Collector mit Dexscreener API (kostenlos).
    Lädt OHLCV-Candles.
    Für detaillierte Trade-Analyse (z.B. Price Movers) werden andere Collector genutzt.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.base_url = "https://api.dexscreener.com/latest/dex"
        self.blockchain = "solana" # Dexscreener hat auch andere
        logger.info("✅ Dexscreener Collector initialized (OHLCV only)")
        self._is_initialized = True

    async def fetch_candle_data(
        self,
        symbol: str,  # z.B. 'SOL/USDC' -> Dexscreener braucht Pool-ID oder Token-Paar
        timeframe: str, # z.B. '5m', '1h'
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Holt eine einzelne Candle von Dexscreener.
        Dies ist eine Vereinfachung. Dexscreener liefert Candles als Array.
        Du musst also den Zeitraum abfragen und die passende Candle finden.
        """
        logger.debug(f"DexscreenerCollector.fetch_candle_data: {symbol} {timeframe} @ {timestamp}")
        # Parsen des Timeframes
        interval_minutes = int(timeframe[:-1]) if timeframe.endswith(('m', 'h', 'd')) else 5
        if timeframe.endswith('h'):
            interval_minutes *= 60
        elif timeframe.endswith('d'):
            interval_minutes *= 24 * 60

        # Bestimme Token (Vereinfachung: suche nach bekanntem Pair)
        # Dexscreener braucht entweder die Pool-ID oder das Token-Paar im Format token0/token1
        # z.B. token0=So11111111111111111111111111111111111111112&token1=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
        # Oder Pool-ID direkt, falls bekannt.
        # Lass uns annehmen, symbol ist Token-Pair wie 'SOL/USDC' und mappe es.
        token_map = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        }
        base_token, quote_token = symbol.split('/')
        token0 = token_map.get(base_token.upper(), base_token)
        token1 = token_map.get(quote_token.upper(), quote_token)

        # Versuche, den Pool zu finden
        pair_id = f"{token0}-{token1}"
        url = f"{self.base_url}/pair/{pair_id}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        logger.warning(f"Dexscreener pair lookup failed: {response.status}")
                        return self._empty_candle(timestamp)

                    data = await response.json()
                    pair_info = data.get('pair')
                    if not pair_info:
                        logger.warning(f"Dexscreener: No pair found for {pair_id}")
                        return self._empty_candle(timestamp)

            # Jetzt versuche, Candles abzurufen
            # Dexscreener hat ein separates Endpoint für Candles
            # https://api.dexscreener.com/latest/dex/candles?pairAddress=<PAIR_ADDRESS>&timeSpan=<TIME_SPAN>
            # TIME_SPAN: 1h, 4h, 1d, 1w, 1M
            # Dies ist begrenzt. Für 5m oder 1m müssen wir ggf. andere Quellen oder Aggregation nutzen.
            # Für 1h, 4h, 1d ist es gut geeignet.
            # Lass uns annehmen, timeframe ist 1h, 4h, 1d.
            # Achtung: Die verfügbaren timeSpans sind beschränkt.
            # 5m, 15m, 30m sind nicht direkt verfügbar.
            # Für diese müsste man entweder mehrere Anfragen stellen oder auf andere Quellen wie Bitquery setzen.
            # Für den Moment: Nur unterstützte Timeframes.
            supported_timeframes = {
                '1h': '1h',
                '4h': '4h',
                '1d': '1d',
                '1w': '1w',
                '1m': '1M' # Annahme: '1m' bedeutet 1 Monat
            }
            time_span = supported_timeframes.get(timeframe)
            if not time_span:
                logger.warning(f"Dexscreener: Timeframe {timeframe} not directly supported, using 1h as fallback.")
                time_span = '1h'

            pair_address = pair_info.get('pairAddress')
            if not pair_address:
                 logger.warning(f"Dexscreener: No pairAddress found for {pair_id}")
                 return self._empty_candle(timestamp)

            candles_url = f"{self.base_url}/candles?pairAddress={pair_address}&timeSpan={time_span}"

            async with aiohttp.ClientSession() as session:
                async with session.get(candles_url, timeout=aiohttp.ClientTimeout(total=10)) as candles_response:
                    if candles_response.status != 200:
                        logger.warning(f"Dexscreener candles fetch failed: {candles_response.status}")
                        return self._empty_candle(timestamp)

                    candles_data = await candles_response.json()
                    candles_list = candles_data.get('candles', [])
                    if not candles_list:
                        logger.warning(f"Dexscreener: No candles returned for {pair_id} {time_span}")
                        return self._empty_candle(timestamp)

            # Finde die Candle, die dem Timestamp am nächsten kommt
            target_timestamp_ms = int(timestamp.timestamp() * 1000)
            interval_ms = interval_minutes * 60 * 1000
            start_range = target_timestamp_ms - (interval_ms / 2)
            end_range = target_timestamp_ms + (interval_ms / 2)

            for candle in candles_list:
                candle_time_ms = candle.get('timestamp')
                if start_range <= candle_time_ms <= end_range:
                    # Konvertiere das Format von Dexscreener zu unserem Standard
                    # Dexscreener: { "timestamp": 1689811200000, "open": "0.123", "high": "0.125", "low": "0.121", "close": "0.124", "volume": "12345.67" }
                    # Unser Ziel: { 'timestamp': datetime, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float, ... }
                    candle_dt = datetime.fromtimestamp(candle_time_ms / 1000.0)
                    formatted_candle = {
                        'timestamp': candle_dt,
                        'open': float(candle.get('open', 0)),
                        'high': float(candle.get('high', 0)),
                        'low': float(candle.get('low', 0)),
                        'close': float(candle.get('close', 0)),
                        'volume': float(candle.get('volume', 0)),
                        'volume_usd': 0.0, # Dexscreener liefert hier kein USD-Volumen direkt
                        'trade_count': 0 # Nicht verfügbar
                    }
                    logger.debug(f"Found candle from Dexscreener for {timestamp}: {formatted_candle}")
                    return formatted_candle

            logger.warning(f"Dexscreener: No candle found for exact timestamp {timestamp} in range {start_range} - {end_range}")
            return self._empty_candle(timestamp)

        except Exception as e:
            logger.error(f"❌ Dexscreener error: {e}")
            return self._empty_candle(timestamp)

    def _empty_candle(self, timestamp: datetime) -> Dict[str, Any]:
        """Hilfsfunktion für leere Candle."""
        return {
            'timestamp': timestamp,
            'open': 0.0,
            'high': 0.0,
            'low': 0.0,
            'close': 0.0,
            'volume': 0.0,
            'volume_usd': 0.0,
            'trade_count': 0
        }

    async def fetch_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None
    ) -> list:
        """
        Dexscreener liefert keine Rohtrades über die öffentliche API.
        Diese Methode ist nicht implementiert und wirft einen Fehler.
        """
        logger.warning("DexscreenerCollector: fetch_trades not supported. Use Helius or Bitquery for trade details.")
        # Gibt leere Liste zurück, da keine Trade-Daten verfügbar sind
        return []

    async def health_check(self) -> bool:
        """
        Einfacher Health Check: Versuche, die Basis-URL zu erreichen.
        """
        try:
            async with aiohttp.ClientSession() as session:
                # Versuche, die aktuelle Pair-Liste zu holen (oder einfach die Basis-URL)
                async with session.get(f"{self.base_url}/pairs", timeout=aiohttp.ClientTimeout(total=5)) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"❌ Dexscreener health check failed: {e}")
            return False

    async def close(self):
        """
        Schließt interne Ressourcen.
        """
        pass # Keine persistenten Ressourcen
