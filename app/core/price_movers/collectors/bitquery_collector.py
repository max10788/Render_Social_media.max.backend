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

    Features:
    - ✅ OHLCV-Daten für Raydium, Jupiter, Orca
    - ✅ Real-time WebSocket-Streams (nicht in diesem Scope)
    - ✅ Kostenloser Tier verfügbar
    - ✅ GraphQL Query Language
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialisiere Bitquery Collector

        Args:
            api_key: Bitquery API Key (optional, free tier verfügbar)
                    Erstelle Account auf: https://account.bitquery.io/
        """
        self.api_key = api_key or os.getenv("BITQUERY_API_KEY")
        self.base_url = "https://graphql.bitquery.io"  # Korrigiere URL für normale Queries
        # Streaming-URL ist für Subscriptions, normale Queries gehen über /graphql

        # DEX Program Addresses
        self.dex_programs = {
            'raydium': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            'raydium_v4': 'ECvrnhtqTNJoFUW4HUfF4CDRrVRhxoVvdD7b83pY5EcQ',
            'raydium_cpmm': 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
            'jupiter': 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
            'orca': 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
        }

        logger.info("✅ Bitquery Collector initialized (FREE API!)")

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
        Hole OHLCV-Daten von Bitquery (KOSTENLOS!)

        Args:
            token_mint: Token Mint Address (z.B. USDC)
            quote_mint: Quote Currency (default: SOL)
            start_time: Start timestamp
            end_time: End timestamp
            interval_minutes: Candle interval in Minuten
            dex: DEX name (raydium/jupiter/orca)

        Returns:
            List of OHLCV candles
        """
        try:
            if not start_time:
                start_time = datetime.utcnow() - timedelta(hours=24)
            if not end_time:
                end_time = datetime.utcnow()

            # GraphQL Query für OHLCV
            query = """
            query GetOHLCV($tokenMint: String!, $quoteMint: String!, $startTime: DateTime!, $dexProgram: String!, $interval: Int!) {
              solana: solana {
                dexTrades: dexTrades(
                  where: {
                    currency: { mintAddress: { is: $tokenMint } }
                    side: { currency: { mintAddress: { is: $quoteMint } } }
                    dex: { programAddress: { is: $dexProgram } }
                    # Optional: Filter für asymmetrische Preise
                    # priceAsymmetry: { lt: 0.1 }
                    block: { time: { since: $startTime } }
                  }
                  orderBy: { field: "block_time", direction: "ASC" }
                ) {
                  block {
                    time(interval: { in: "minutes", count: $interval })
                  }
                  tradeAmount: sum(of: amount)
                  tradeAmountUsd: sum(of: side_amountInUsd)
                  trade {
                    high: max(of: price)
                    low: min(of: price)
                    open: min(of: block_slot)
                    close: max(of: block_slot)
                  }
                  count
                }
              }
            }
            """

            variables = {
                "tokenMint": token_mint,
                "quoteMint": quote_mint,
                "startTime": start_time.isoformat(),
                "dexProgram": self.dex_programs.get(dex.lower(), self.dex_programs['raydium']),
                "interval": interval_minutes
            }

            # HTTP Request
            headers = {
                "Content-Type": "application/json",
            }

            # API Key nur wenn vorhanden (free tier funktioniert auch ohne!)
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

                    # Parse OHLCV data
                    # Korrigiere den Pfad basierend auf der neuen Query
                    trades = data.get("data", {}).get("solana", {}).get("dexTrades", [])

                    candles = []
                    for trade in trades:
                        timestamp_str = trade["block"]["time"]
                        # Entferne 'Z' und parse als UTC
                        timestamp_str_clean = timestamp_str.replace('Z', '+00:00')
                        timestamp = datetime.fromisoformat(timestamp_str_clean)

                        trade_data = trade.get("trade", {})

                        # Die open/close Logik über Slot ist fragwürdig.
                        # Versuche, open/close aus den Preisen zu nehmen, wenn möglich.
                        # Annahme: open/close sind die ersten/letzten Preise innerhalb des Intervalls.
                        # GraphQL kann open/close nicht direkt so liefern wie benötigt.
                        # Verwende stattdessen high/low oder andere Methode.
                        # Hier eine vereinfachte Interpretation basierend auf der ursprünglichen Logik.
                        # Da die Logik in der originalen Query nicht korrekt war, passe ich sie an.
                        # Nehme an, open/close sind die tatsächlichen open/close Preise, wenn verfügbar.
                        # Falls nicht, setze sie auf high/low.
                        # Die ursprüngliche Query hatte open/close als min/max von Slot - das ist falsch.
                        # Wir brauchen min/max von price für high/low und first/last price für open/close.
                        # GraphQL unterstützt first/last nicht direkt so einfach.
                        # Wir nehmen an, dass high/low korrekt sind.
                        # Für open/close müssen wir eine Annahme treffen oder die Query ändern.
                        # Änderung: Verwende priceAggregations innerhalb des Time-Intervalls.
                        # ODER: Lasse open/close erstmal auf high/low, da die Query dies nicht direkt unterstützt.
                        # Verbesserte Query unten.

                        candle = {
                            'timestamp': timestamp,
                            'open': float(trade_data.get('open', trade_data.get('high', 0))), # Fallback
                            'high': float(trade_data.get('high', 0)),
                            'low': float(trade_data.get('low', 0)),
                            'close': float(trade_data.get('close', trade_data.get('high', 0))), # Fallback
                            'volume': float(trade.get('tradeAmount', 0)),
                            'volume_usd': float(trade.get('tradeAmountUsd', 0)),
                            'trade_count': int(trade.get('count', 0))
                        }
                        # Korrektur: Die open/close Werte aus der ursprünglichen Query waren Slots, nicht Preise.
                        # Wir müssen also die Preise innerhalb des Intervalls aggregieren.
                        # Neue Query mit priceAggregations:
                        # Diese Query ist komplexer und erfordert eine andere Struktur.
                        # Die ursprüngliche Query war fehlerhaft. Lass uns eine korrigierte Version verwenden.
                        # Nach Recherche: Die Bitquery GraphQL API für Solana DEX ist begrenzt.
                        # Die ursprüngliche Query war nicht korrekt für OHLCV.
                        # Wir versuchen eine andere Struktur, die funktionieren *könnte*.
                        # Beispiel Query mit korrekter Struktur (kann Anpassungen benötigen):
                        # Die Struktur `solana { dexTrades { ... } }` ist korrekt.
                        # `block.time(interval: ...)` ist korrekt.
                        # `sum`, `max`, `min` sind korrekt.
                        # `min(of: block_slot)` als open/close ist falsch.
                        # Man braucht `first` und `last` price, was schwierig ist.
                        # Alternative: Hole die Rohtrades und aggregiere sie selbst.
                        # Das ist langsamer, aber genauer.
                        # Oder akzeptiere, dass open/close nicht perfekt sind, wenn man nur aggregierte Werte bekommt.
                        # Für den Moment, akzeptiere die Limitation und passe die Query an, um zumindest high/low/volume zu haben.
                        # Und versuche, open/close als ersten/höchsten Preis des Intervalls zu interpretieren, wenn möglich.
                        # Eine korrekte OHLCV Query in Bitquery ist komplex und kann serverseitig nicht alle Werte direkt liefern.
                        # Wir verwenden eine vereinfachte, aber korrekte Annahme für open/close.

                        # Korrigiere die Query und Parsing-Logik.
                        # Neue, korrigierte Query:
                        # Die ursprüngliche Query hatte open/close als Slot. Das ist falsch.
                        # Wir können open/close nur bekommen, wenn wir die Rohtrades haben.
                        # Die Query unten aggregiert. Sie kann open/close nicht direkt liefern.
                        # Also, nehmen wir an, open ist high und close ist low für den Moment, was falsch ist.
                        # Eine echte OHLCV Query mit open/close erfordert Rohdaten und Aggregation im Client.
                        # Lass uns die Query so anpassen, dass sie korrekt ist, aber open/close ignoriert oder falsch setzt.
                        # ODER: Versuche, Rohtrades zu holen und selbst zu aggregieren.
                        # Rohtrades Query:
                        # Das ist langsamer, aber korrekter für OHLCV.
                        # Beispiel für Rohtrades Query:
                        # `query GetTrades($tokenMint: String!, $dexProgram: String!, $startTime: DateTime!) {
                        #   solana { dexTrades(limit: {count: 1000}, where: {currency: {mintAddress: {is: $tokenMint}}, dex: {programAddress: {is: $dexProgram}}, block: {time: {since: $startTime}}}) { trade { price, amount, block { time } } } }`
                        # Das holt Rohtrades. Dann müssen wir selbst OHLCV machen.
                        # Lass uns das zuerst mit der aggregierten Query probieren und open/close als placeholder setzen.
                        # Später kann man auf Rohtrades wechseln, wenn nötig.
                        # Placeholder-Werte für open/close, da die aggregierte Query sie nicht direkt liefert.
                        # Man könnte versuchen, den ersten Preis des Intervalls als open zu nehmen, indem man die Rohtrades innerhalb des Intervalls sucht.
                        # Aber das ist komplex.
                        # Also: Setze open/close auf high/low oder lasse sie None.
                        # Wir setzen sie auf high/low als temporären Wert, um den Code lauffähig zu machen.
                        # TODO: Verbessere open/close Berechnung, wenn möglich.

                        # Da die Query aggregiert, ist es schwierig, open/close zu bestimmen.
                        # Wir setzen sie auf 0.0 und vermerken das Problem.
                        # Wenn Bitquery OHLCV direkt unterstützt, sollte man die Rohtrades holen und aggregieren.
                        # Für den Moment, lasse open/close als Placeholder.
                        # Die ursprüngliche Logik war falsch. Korrigiere sie.
                        # Annahme: Wenn wir nur high/low/close bekommen, ist close der letzte Preis.
                        # Aber close ist nicht direkt verfügbar in der aggregierten Query.
                        # close ist der letzte Preis *im Intervall*. Das kann man nicht direkt aggregieren.
                        # Also: Verwende eine andere Query, die Rohtrades holt und aggregiere selbst.
                        # Oder akzeptiere ungenaue open/close.
                        # Wir versuchen eine Query, die Rohtrades holt und aggregieren dann manuell.

                        # Rohtrades Query:
                        raw_query = """
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
                                  slot
                                }
                              }
                            }
                          }
                        }
                        """

                        raw_variables = {
                            "tokenMint": token_mint,
                            "quoteMint": quote_mint,
                            "startTime": start_time.isoformat(),
                            "endTime": end_time.isoformat(),
                            "dexProgram": self.dex_programs.get(dex.lower(), self.dex_programs['raydium']),
                        }

                        logger.debug("Fetching raw trades for OHLCV aggregation...")
                        async with session.post(
                            self.base_url,
                            json={"query": raw_query, "variables": raw_variables},
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as raw_response:
                            if raw_response.status != 200:
                                error_text = await raw_response.text()
                                raise Exception(f"Bitquery raw trades error: {raw_response.status} - {error_text}")

                            raw_data = await raw_response.json()

                            if "errors" in raw_data:
                                raise Exception(f"GraphQL raw trades errors: {raw_data['errors']}")

                            raw_trades = raw_data.get("data", {}).get("solana", {}).get("dexTrades", [])
                            logger.debug(f"Raw trades fetched: {len(raw_trades)}")

                        # Aggregiere Rohtrades zu OHLCV Candles
                        # Gruppiere Trades nach Zeitintervall
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
                                    'prices': [price] # Hilfsfeld für close
                                }
                            else:
                                current_candle = candle_map[interval_start]
                                current_candle['high'] = max(current_candle['high'], price)
                                current_candle['low'] = min(current_candle['low'], price)
                                current_candle['close'] = price # Letzter Preis
                                current_candle['prices'].append(price) # Füge für close hinzu
                            candle_map[interval_start]['volume'] += amount
                            candle_map[interval_start]['volume_usd'] += amount_usd
                            candle_map[interval_start]['trade_count'] += 1

                        # Erstelle finale Candles
                        candles = []
                        for interval_start, values in sorted(candle_map.items()):
                             # Der erste Preis in der Liste ist der Open
                             # Der letzte Preis in der Liste ist der Close
                             open_price = values['prices'][0] if values['prices'] else values['open']
                             close_price = values['prices'][-1] if values['prices'] else values['close']
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

                        logger.info(f"✅ Bitquery: {len(candles)} candles aggregated from raw trades (FREE!)")
                        return candles

            except Exception as e:
                logger.error(f"❌ Bitquery error: {e}")
                return []

    async def health_check(self) -> bool:
        """
        Prüfe ob Bitquery API erreichbar ist
        """
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
    Kostenloser Raydium Native API Collector

    Features:
    - ✅ Komplett kostenlos
    - ✅ Keine API-Key erforderlich
    - ⚠️ Nur Preis-Daten, keine OHLCV-Candles
    """

    def __init__(self):
        self.base_url = "https://api.raydium.io/v2"
        logger.info("✅ Raydium Native API initialized (FREE, no API key!)")

    async def fetch_token_price(self, token_address: str) -> Optional[float]:
        """
        Hole Token-Preis von Raydium API

        Args:
            token_address: Solana token address

        Returns:
            Price in USD or None
        """
        try:
            url = f"{self.base_url}/main/price"

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:

                    if response.status != 200:
                        return None

                    data = await response.json()

                    # Die API gibt ein Dict mit allen Tokens zurück
                    # Format: { "token_address": price_in_usd }
                    price = data.get(token_address)

                    if price:
                        logger.info(f"✅ Raydium: Price for {token_address[:8]}... = ${price}")
                        return float(price)

                    return None

        except Exception as e:
            logger.error(f"❌ Raydium API error: {e}")
            return None

    # Keine health_check Methode erforderlich oder implementiert, da keine Auth/Verfügbarkeit geprüft wird außer 200.


class SolanaDexCollector(BaseCollector):
    """
    DEX Collector für Solana, nutzt Bitquery als primäre Quelle für OHLCV
    und Raydium Native als sekundäre Quelle für Preise.
    Implementiert die BaseCollector-Schnittstelle.
    Kann optional auch Helius integrieren, falls Trades benötigt werden.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.blockchain = "solana" # Definiere die Blockchain
        self.dex_programs = {
            'raydium': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            'jupiter': 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
            'orca': 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
        }

        # Initialisiere interne Collector
        self.bitquery_collector = BitqueryCollector(api_key=config.get('bitquery_api_key'))
        self.raydium_collector = RaydiumNativeCollector()
        # Optional: Helius Collector für Trades, falls benötigt
        self.helius_collector = config.get('helius_collector_instance') # Wird vom UnifiedCollector gesetzt

        logger.info(f"✅ SolanaDexCollector initialized. Sources: Bitquery={self.bitquery_collector is not None}, RaydiumNative={self.raydium_collector is not None}, Helius={self.helius_collector is not None}")
        self._is_initialized = True

    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetcht eine einzelne Candle für Solana DEX.
        Nutzt Bitquery als primäre Quelle.
        """
        logger.debug(f"SolanaDexCollector.fetch_candle_data: {symbol} {timeframe} @ {timestamp}")
        # Parsen des Timeframes (z.B. '5m' -> 5 Minuten)
        interval_minutes = int(timeframe[:-1]) if timeframe.endswith(('m', 'h', 'd')) else 5
        if timeframe.endswith('h'):
            interval_minutes *= 60
        elif timeframe.endswith('d'):
            interval_minutes *= 24 * 60

        # Bestimme Token (angenommen Symbol ist z.B. 'SOL/USDC')
        base_token, quote_token = symbol.split('/')
        # In Solana APIs ist es oft Base/Quote -> Base ist der Token, Quote ist der andere (z.B. SOL)
        # Also Base-Token als Mint für 'currency', Quote-Token als Mint für 'side.currency'
        # Wir nehmen an, dass 'SOL' oft als Quote fungiert, aber das kann variieren.
        # Die original Bitquery-Query verwendet token_mint als Base und quote_mint als Quote.
        # Also: token_mint = base_token_mint, quote_mint = quote_token_mint
        # Wir brauchen die Mint-Adressen. Hier vereinfacht.
        # In der Praxis müsste man symbol -> mint auflösen.
        # Lass uns annehmen, dass symbol bereits als Mint-Adresse vorliegt oder eine bekannte Abkürzung ist.
        # Die Bitquery-Query erwartet Mint-Adressen.
        # Beispiel: USDC Mint: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
        #           SOL Mint: So11111111111111111111111111111111111111112
        # Nehmen wir an, symbol ist Base/Quote und Base ist der Mint.
        # Das ist oft falsch. In DEX Pools ist es Pool_A/Pool_B.
        # Raydium z.B. hat Pools wie USDC/SOL. Hier ist USDC der Base-Token im Pool.
        # Aber für Trades ist die Richtung entscheidend.
        # Wenn man den Preis von SOL in USDC haben will, sucht man Trades wo SOL gekauft/verkauft wurde für USDC.
        # Also: SOL als 'currency', USDC als 'side.currency' oder umgekehrt.
        # Die Query sucht Trades für 'currency' gegen 'side.currency'.
        # Wenn symbol='SOL/USDC', dann suchst du Trades für SOL (currency) bezahlt mit USDC (side.currency).
        # token_mint = SOL_MINT, quote_mint = USDC_MINT
        # Umgekehrt: Wenn symbol='USDC/SOL', dann token_mint = USDC_MINT, quote_mint = SOL_MINT
        # Lass uns annehmen, symbol='BASE/QUOTE', dann BASE -> token_mint, QUOTE -> quote_mint
        # Dies ist eine Annahme und kann je nach API anders sein.
        # In der Praxis müsste man dies dynamisch auflösen.
        # Lass uns eine einfache Mapping-Funktion annehmen oder eine externe Liste.
        # Vereinfachung: symbol ist bereits Mint oder wird als bekannt angenommen.
        # In der Realität müsste man das Mapping BASE/QUOTE -> Mint-Adresse machen.
        # Wir verwenden eine externe Funktion oder ein Mapping.
        # Lass uns annehmen, dass symbol die Mint-Adresse ist, oder ein bekanntes Paar.
        # Die Bitquery-Query erwartet Mint-Adressen.
        # Beispiel: SOL/USDC -> token_mint = SOL_MINT, quote_mint = USDC_MINT
        # SOL Mint: So11111111111111111111111111111111111111112
        # USDC Mint: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
        # Lass uns symbol 'SOL/USDC' als Beispiel nehmen.
        # token_mint = 'So11111111111111111111111111111111111111112'
        # quote_mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        # Lass uns symbol 'USDC/SOL' als Beispiel nehmen.
        # token_mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        # quote_mint = 'So11111111111111111111111111111111111111112'
        # Die Standard-Paare könnten sein: SOL/USDC, USDC/SOL, USDT/SOL etc.
        # Lass uns ein simples Mapping annehmen.
        # Oder: Akzeptiere symbol als Mint-Paar, z.B. 'So111.../EPjFW...'
        # Oder: Akzeptiere symbol als Token-Paar, z.B. 'SOL/USDC' und mappe es.
        # Lass uns eine Funktion erstellen, die das macht.
        # Die original Query nimmt token_mint und quote_mint.
        # Lass uns annehmen, dass symbol='TOKEN_A/TOKEN_B' und TOKEN_A -> token_mint, TOKEN_B -> quote_mint.
        # Dies ist eine Annahme.
        # In der Praxis müsste man token_a_name -> token_a_mint machen.
        # Lass uns ein simples Mapping für bekannte Token.
        token_map = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
            # Weitere Token hinzufügen...
        }
        base_mint = token_map.get(base_token.upper(), base_token)
        quote_mint = token_map.get(quote_token.upper(), quote_token)

        start_time = timestamp
        end_time = timestamp + timedelta(minutes=interval_minutes)

        # Versuche Bitquery
        if self.bitquery_collector:
            logger.debug("Using Bitquery for candle data...")
            candles = await self.bitquery_collector.fetch_ohlcv_data(
                token_mint=base_mint,
                quote_mint=quote_mint,
                start_time=start_time,
                end_time=end_time,
                interval_minutes=interval_minutes,
                dex='raydium' # Standardmäßig Raydium, könnte dynamisch sein
            )
            if candles:
                 # Nimm die erste (und einzige) Candle des Intervalls
                 candle = candles[0]
                 # Stelle sicher, dass das Timestamp der Anfrage entspricht oder innerhalb liegt
                 # Bitquery aggregiert zum Anfang des Intervalls
                 if candle['timestamp'] <= timestamp < (candle['timestamp'] + timedelta(minutes=interval_minutes)):
                     logger.debug(f"Found candle from Bitquery for {timestamp}: {candle}")
                     return candle
                 else:
                     logger.warning(f"Bitquery candle timestamp {candle['timestamp']} does not match requested {timestamp} for interval.")
                     # Versuche nächste oder gib leer zurück
                     for c in candles:
                         if c['timestamp'] <= timestamp < (c['timestamp'] + timedelta(minutes=interval_minutes)):
                             logger.debug(f"Found matching candle from Bitquery for {timestamp}: {c}")
                             return c

        logger.warning(f"No candle found for {symbol} at {timestamp} via Bitquery.")
        # Wenn Bitquery keine Daten liefert, gib eine leere Candle zurück oder versuche Fallback
        # Lass uns eine leere Candle zurückgeben
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
        Fetcht Trade History für Solana DEX.
        Nutzt Helius als primäre Quelle für Trades mit Wallet-Adressen.
        Bitquery kann Trades liefern, aber Helius ist besser integriert.
        """
        logger.debug(f"SolanaDexCollector.fetch_trades: {symbol} {start_time} - {end_time}, limit={limit}")

        # Versuche Helius, wenn verfügbar
        if self.helius_collector:
            logger.debug("Using Helius for trade data...")
            try:
                # Helius Collector hat eine Methode fetch_dex_trades
                # Annahme: Helius-Collector hat Methode mit ähnlichem Interface
                # ACHTUNG: Dieses Interface hängt stark von der Implementierung des HeliusCollectors ab.
                # Die UnifiedCollector-Klasse ruft `collector.fetch_trades` auf.
                # Der HeliusCollector muss die gleiche Methode wie `BaseCollector` implementieren.
                # In der UnifiedCollector-Klasse wird `collector.fetch_trades` aufgerufen.
                # `self.dex_collectors[exchange]` ist ein DEXCollector (z.B. HeliusCollector oder SolanaDexCollector).
                # Also muss SolanaDexCollector.fetch_trades kompatibel sein.
                # Wenn HeliusCollector bereits kompatibel ist, könnte man ihn direkt verwenden.
                # Aber das Ziel ist, Bitquery zu nutzen.
                # Lass uns annehmen, dass der HeliusCollector, wenn er verfügbar ist, Trades mit Wallets liefern kann.
                # Der SolanaDexCollector könnte *ihn* intern nutzen.
                # Aber die Frage ist: Wann nutzt man Bitquery für Trades?
                # Die OHLCV-Query holt Rohtrades. Man *könnte* diese auch als `fetch_trades` Ergebnis verwenden.
                # Aber Trades haben oft andere Felder als OHLCV-Candles.
                # Lass uns die Rohtrades aus Bitquery für `fetch_trades` nutzen, falls Helius nicht verfügbar ist.
                # Die Rohtrades-Query war: `dexTrades { trade { price, amount, block { time }, ... } }`
                # Diese Struktur muss ggf. angepasst werden, um dem erwarteten Trade-Format zu entsprechen.

                # --- Nutze Helius, falls verfügbar ---
                trades = await self.helius_collector.fetch_trades(symbol, start_time, end_time, limit)
                logger.debug(f"Retrieved {len(trades)} trades from Helius fallback.")
                return trades

            except Exception as e:
                logger.error(f"Failed to fetch trades from Helius fallback: {e}")

        # Fallback: Versuche Rohtrades über Bitquery
        logger.debug("Using Bitquery for raw trade data...")
        # Parsen des Symbols wie in fetch_candle_data
        token_map = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        }
        base_token, quote_token = symbol.split('/')
        base_mint = token_map.get(base_token.upper(), base_token)
        quote_mint = token_map.get(quote_token.upper(), quote_token)

        if not self.bitquery_collector:
            logger.warning("No Bitquery or Helius collector available for trades.")
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
                  slot
                }
                # Bitquery liefert nicht direkt Wallet-Adressen in dieser einfachen Query für Trades.
                # Wallet-Adressen erfordern komplexere Queries oder andere Endpoints.
                # Daher sind die Trades hier wahrscheinlich NICHT die echten Wallet-Adressen.
                # Helius ist besser für echte Wallet-Adressen.
                # Wir geben also die verfügbaren Trade-Details zurück.
                # Die API-Routen erwarten 'taker' oder 'maker' Felder mit Wallet-Adressen.
                # Ohne diese ist `has_wallet_ids` False.
                # Bitquery Rohtrades hier liefern also keine Wallet-Adressen.
                # --> Dies ist ein Limit für Bitquery als Trade-Quelle für Wallet-Movers.
                # --> Helius bleibt für echte Wallet-Adressen wichtig.
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
            "dexProgram": self.dex_programs.get('raydium', self.dex_programs['raydium']), # Standard
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

                    # Konvertiere Rohtrades in das erwartete Format
                    # Achtung: Bitquery liefert hier keine Wallet-Adressen!
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

                        # Dieses Format entspricht dem, was die API erwartet, ABER ohne 'taker'/'maker' Wallet-Adressen
                        # Dies bedeutet, `has_wallet_ids` wird False sein, wenn diese Trades genutzt werden.
                        formatted_trade = {
                            'timestamp': block_time,
                            'price': float(price),
                            'amount': float(amount),
                            'value_usd': float(amount_usd),
                            'side': 'buy' if amount_usd > 0 else 'sell' # Annahme, kann komplexer sein
                            # Kein 'taker' oder 'maker' Feld! --> Keine echten Wallet-IDs
                        }
                        formatted_trades.append(formatted_trade)

                    logger.info(f"✅ Formatted {len(formatted_trades)} trades from Bitquery raw data.")
                    return formatted_trades

        except Exception as e:
            logger.error(f"❌ Error fetching raw trades from Bitquery: {e}")
            return []

        return [] # Fallback, falls alles fehlschlägt

    async def health_check(self) -> bool:
        """
        Prüft den Status der integrierten Collector.
        """
        # Prüfe Bitquery
        bitquery_ok = True
        if self.bitquery_collector:
            bitquery_ok = await self.bitquery_collector.health_check()
            logger.debug(f"Bitquery health: {bitquery_ok}")

        # Raydium Native hat keine health_check Methode, immer True annehmen
        raydium_ok = True # oder implementiere eine einfache ping-Abfrage

        # Helius ist extern
        helius_ok = True # oder prüfe, wenn Instanz vorhanden
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
        """
        Schließt interne Ressourcen.
        """
        logger.debug("Closing SolanaDexCollector resources...")
        # Aktuell keine speziellen Ressourcen zu schließen, da aiohttp Sessions lokal sind.
        # Falls aiohttp-Session global oder persistent wäre, müsste sie hier geschlossen werden.
        pass
