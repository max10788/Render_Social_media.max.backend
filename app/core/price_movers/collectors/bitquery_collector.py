# app/core/price_movers/collectors/bitquery_collector.py

import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import os
from .base import BaseCollector

logger = logging.getLogger(__name__)


class BitqueryCollector:
    """
    Kostenloser DEX Data Collector mit Bitquery GraphQL API
    für Solana OHLCV-Daten.
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialisiere Bitquery Collector
        Args:
            api_key: Bitquery API Key (optional, free tier verfügbar)
        """
        self.api_key = api_key or os.getenv("BITQUERY_API_KEY")
        self.base_url = "https://graphql.bitquery.io" # Standard-GraphQL-Endpunkt

        # DEX Program Addresses für Solana
        self.dex_programs = {
            'raydium': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            'raydium_v4': 'ECvrnhtqTNJoFUW4HUfF4CDRrVRhxoVvdD7b83pY5EcQ',
            'raydium_cpmm': 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
            'jupiter': 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
            'orca': 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
        }
        logger.info("✅ Bitquery Collector initialized")

    async def fetch_ohlcv_data(
        self,
        token_mint: str,
        quote_mint: str = "So11111111111111111111111111111111111111112",  # SOL
        start_time: datetime = None,
        end_time: datetime = None,
        interval_minutes: int = 5,
        dex: str = 'raydium'
    ) -> List[Dict]:
        """
        Hole OHLCV-Daten von Bitquery (kann kostenlos mit Free Tier genutzt werden).
        Nutzt Rohtrades und aggregiert sie intern.
        """
        try:
            if not start_time:
                start_time = datetime.utcnow() - timedelta(hours=24)
            if not end_time:
                end_time = datetime.utcnow()

            # GraphQL Query für Rohtrades
            query = """
            query GetRawTrades($tokenMint: String!, $quoteMint: String!, $startTime: DateTime!, $endTime: DateTime!, $dexProgram: String!) {
              solana: solana {
                dexTrades: dexTrades(
                  where: {
                    currency: { mintAddress: { is: $tokenMint } }
                    side: { currency: { mintAddress: { is: $quoteMint } } }
                    dex: { programAddress: { is: $dexProgram } }
                    block: { time: { since: $startTime, till: $endTime } }
                  }
                  orderBy: { field: "block_time", direction: "ASC" }
                ) {
                  trade {
                    price
                    amount
                    amountInUsd: side_amountInUsd
                    block {
                      time
                    }
                  }
                }
              }
            }
            """

            variables = {
                "tokenMint": token_mint,
                "quoteMint": quote_mint,
                "startTime": start_time.isoformat(),
                "endTime": end_time.isoformat(),
                "dexProgram": self.dex_programs.get(dex.lower(), self.dex_programs['raydium']),
            }

            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["X-API-KEY"] = self.api_key

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.base_url,
                    json={"query": query, "variables": variables},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"Bitquery error: {response.status} - {error_text}")

                    data = await response.json()
                    if "errors" in data:
                        raise Exception(f"GraphQL errors: {data['errors']}")

            # Rohtrades verarbeiten und zu Candles aggregieren
            raw_trades = data.get("data", {}).get("solana", {}).get("dexTrades", [])
            logger.debug(f"Raw trades fetched for OHLCV: {len(raw_trades)}")

            interval_timedelta = timedelta(minutes=interval_minutes)
            candle_map = {}

            for trade_entry in raw_trades:
                trade = trade_entry.get("trade", {})
                price = trade.get("price")
                amount = trade.get("amount", 0)
                amount_usd = trade.get("amountInUsd", 0)
                block_time_str = trade.get("block", {}).get("time")
                if not price or not block_time_str:
                    continue
                block_time = datetime.fromisoformat(block_time_str.replace('Z', '+00:00'))

                # Bestimme das Intervall
                interval_start = block_time - timedelta(seconds=block_time.second, microseconds=block_time.microsecond) + timedelta(minutes=(block_time.minute // interval_minutes) * interval_minutes)

                if interval_start not in candle_map:
                    candle_map[interval_start] = {
                        'open': price,
                        'high': price,
                        'low': price,
                        'close': price,
                        'volume': 0,
                        'volume_usd': 0,
                        'trade_count': 0,
                        'prices': [price]
                    }
                else:
                    current_candle = candle_map[interval_start]
                    current_candle['high'] = max(current_candle['high'], price)
                    current_candle['low'] = min(current_candle['low'], price)
                    current_candle['close'] = price # Letzter Preis
                    current_candle['prices'].append(price)
                candle_map[interval_start]['volume'] += amount
                candle_map[interval_start]['volume_usd'] += amount_usd
                candle_map[interval_start]['trade_count'] += 1

            # Erstelle finale Candles
            candles = []
            for interval_start, values in sorted(candle_map.items()):
                 open_price = values['prices'][0]
                 close_price = values['prices'][-1]
                 final_candle = {
                     'timestamp': interval_start,
                     'open': open_price,
                     'high': values['high'],
                     'low': values['low'],
                     'close': close_price,
                     'volume': values['volume'],
                     'volume_usd': values['volume_usd'],
                     'trade_count': values['trade_count']
                 }
                 candles.append(final_candle)

            logger.info(f"✅ Bitquery: {len(candles)} candles aggregated from raw trades")
            return candles

        except Exception as e:
            logger.error(f"❌ Bitquery OHLCV error: {e}")
            return []

    async def health_check(self) -> bool:
        """Prüft die Verbindung zur Bitquery API."""
        try:
            query = "{ __typename }"
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["X-API-KEY"] = self.api_key

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.base_url,
                    json={"query": query},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"❌ Bitquery health check failed: {e}")
            return False


class RaydiumNativeCollector:
    """
    Kostenloser Raydium Native API Collector für aktuelle Preise.
    """
    def __init__(self):
        self.base_url = "https://api.raydium.io/v2"
        logger.info("✅ Raydium Native API initialized")

    async def fetch_token_price(self, token_address: str) -> Optional[float]:
        """Holt den aktuellen Token-Preis von Raydium."""
        try:
            url = f"{self.base_url}/main/price"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        return None
                    data = await response.json()
                    price = data.get(token_address)
                    if price:
                        logger.debug(f"Raydium price for {token_address[:8]}: ${price}")
                        return float(price)
                    return None
        except Exception as e:
            logger.error(f"❌ Raydium API error: {e}")
            return None


class SolanaDexCollector(BaseCollector):
    """
    DEX Collector für Solana, nutzt Bitquery als primäre Quelle für OHLCV
    und Raydium Native als sekundäre Quelle für Preise.
    Implementiert die BaseCollector-Schnittstelle.
    Kann optional Helius für Trades mit Wallets nutzen.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.blockchain = "solana"
        self.dex_programs = {
            'raydium': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            'jupiter': 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
            'orca': 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
        }

        # Initialisiere interne Collector
        self.bitquery_collector = BitqueryCollector(api_key=config.get('bitquery_api_key'))
        self.raydium_collector = RaydiumNativeCollector()
        self.helius_collector = config.get('helius_collector_instance') # Wird vom UnifiedCollector gesetzt

        logger.info(f"✅ SolanaDexCollector initialized. Sources: Bitquery={self.bitquery_collector is not None}, Raydium={self.raydium_collector is not None}, HeliusFallback={self.helius_collector is not None}")
        self._is_initialized = True

    def provides_wallet_ids(self) -> bool:
        """
        Gibt an, ob dieser Collector Trades mit Wallet-Adressen liefern kann.
        Ja, wenn Helius als Fallback verfügbar ist, Nein, wenn nur Bitquery genutzt wird.
        """
        return self.helius_collector is not None

    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Fetcht eine einzelne Candle für Solana DEX. Nutzt Bitquery."""
        logger.debug(f"SolanaDexCollector.fetch_candle_data: {symbol} {timeframe} @ {timestamp}")
        interval_minutes = int(timeframe[:-1]) if timeframe.endswith(('m', 'h', 'd')) else 5
        if timeframe.endswith('h'):
            interval_minutes *= 60
        elif timeframe.endswith('d'):
            interval_minutes *= 24 * 60

        # Symbol-Parsing (Vereinfacht)
        token_map = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        }
        base_token, quote_token = symbol.split('/')
        base_mint = token_map.get(base_token.upper(), base_token)
        quote_mint = token_map.get(quote_token.upper(), quote_token)

        start_time = timestamp
        end_time = timestamp + timedelta(minutes=interval_minutes)

        if self.bitquery_collector:
            candles = await self.bitquery_collector.fetch_ohlcv_data(
                token_mint=base_mint,
                quote_mint=quote_mint,
                start_time=start_time,
                end_time=end_time,
                interval_minutes=interval_minutes,
                dex='raydium' # Standard
            )
            if candles:
                 candle = candles[0]
                 if candle['timestamp'] <= timestamp < (candle['timestamp'] + timedelta(minutes=interval_minutes)):
                     logger.debug(f"Found candle from Bitquery for {timestamp}")
                     return candle

        logger.warning(f"No candle found for {symbol} at {timestamp} via Bitquery.")
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
        """Fetcht Trade History. Nutzt Helius als Fallback für Wallet-IDs, sonst Bitquery."""
        logger.debug(f"SolanaDexCollector.fetch_trades: {symbol} {start_time} - {end_time}, limit={limit}")

        # Versuche Helius als Fallback, wenn verfügbar
        if self.helius_collector:
            logger.debug("Using Helius fallback for trade data (likely with wallet IDs)...")
            try:
                trades = await self.helius_collector.fetch_trades(symbol, start_time, end_time, limit)
                logger.info(f"✅ Retrieved {len(trades)} trades from Helius fallback.")
                return trades
            except Exception as e:
                logger.error(f"Helius fallback failed: {e}")

        # Fallback: Rohtrades über Bitquery (ohne Wallet-IDs)
        logger.debug("Using Bitquery for raw trade data (no wallet IDs)...")
        token_map = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        }
        base_token, quote_token = symbol.split('/')
        base_mint = token_map.get(base_token.upper(), base_token)
        quote_mint = token_map.get(quote_token.upper(), quote_token)

        if not self.bitquery_collector:
            logger.warning("No Bitquery collector available for trades.")
            return []

        api_key = self.bitquery_collector.api_key
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-API-KEY"] = api_key

        query = """
        query GetRawTrades($tokenMint: String!, $quoteMint: String!, $startTime: DateTime!, $endTime: DateTime!, $dexProgram: String!, $limit: Int) {
          solana: solana {
            dexTrades: dexTrades(
              where: {
                currency: { mintAddress: { is: $tokenMint } }
                side: { currency: { mintAddress: { is: $quoteMint } } }
                dex: { programAddress: { is: $dexProgram } }
                block: { time: { since: $startTime, till: $endTime } }
              }
              orderBy: { field: "block_time", direction: "ASC" }
              limit: { count: $limit }
            ) {
              trade {
                price
                amount
                amountInUsd: side_amountInUsd
                block {
                  time
                }
              }
            }
          }
        }
        """

        variables = {
            "tokenMint": base_mint,
            "quoteMint": quote_mint,
            "startTime": start_time.isoformat(),
            "endTime": end_time.isoformat(),
            "dexProgram": self.dex_programs.get('raydium', self.dex_programs['raydium']),
            "limit": limit or 1000
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.bitquery_collector.base_url,
                    json={"query": query, "variables": variables},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Bitquery raw trades error: {response.status} - {error_text}")
                        return []

                    data = await response.json()
                    if "errors" in data:
                        logger.error(f"GraphQL raw trades errors: {data['errors']}")
                        return []

            raw_trades = data.get("data", {}).get("solana", {}).get("dexTrades", [])
            logger.debug(f"Fetched {len(raw_trades)} raw trades from Bitquery.")

            # Konvertiere Rohtrades
            formatted_trades = []
            for trade_entry in raw_trades:
                trade_detail = trade_entry.get("trade", {})
                price = trade_detail.get("price")
                amount = trade_detail.get("amount", 0)
                amount_usd = trade_detail.get("amountInUsd", 0)
                block_time_str = trade_detail.get("block", {}).get("time")
                if not price or not block_time_str:
                    continue
                block_time = datetime.fromisoformat(block_time_str.replace('Z', '+00:00'))

                formatted_trade = {
                    'timestamp': block_time,
                    'price': float(price),
                    'amount': float(amount),
                    'value_usd': float(amount_usd),
                    'side': 'buy' if amount_usd > 0 else 'sell' # Einfache Annahme
                    # Kein 'taker' oder 'maker' Feld -> keine echten Wallet-IDs
                }
                formatted_trades.append(formatted_trade)

            logger.info(f"✅ Formatted {len(formatted_trades)} trades from Bitquery raw data.")
            return formatted_trades

        except Exception as e:
            logger.error(f"❌ Error fetching raw trades from Bitquery: {e}")
            return []

    async def health_check(self) -> bool:
        """Prüft den Status der integrierten Collector."""
        bitquery_ok = True
        if self.bitquery_collector:
            bitquery_ok = await self.bitquery_collector.health_check()
            logger.debug(f"Bitquery health: {bitquery_ok}")

        raydium_ok = True # Kein echter Health Check für Raydium Native

        helius_ok = True
        if self.helius_collector:
             try:
                 helius_ok = await self.helius_collector.health_check()
                 logger.debug(f"Helius (fallback) health: {helius_ok}")
             except:
                 helius_ok = False

        overall = bitquery_ok and raydium_ok and helius_ok
        logger.info(f"SolanaDexCollector health: Bitquery={bitquery_ok}, Raydium={raydium_ok}, HeliusFallback={helius_ok} -> Overall={overall}")
        return overall

    async def close(self):
        """Schließt interne Ressourcen."""
        logger.debug("Closing SolanaDexCollector resources...")
        # Keine persistenten Ressourcen zu schließen
        pass
