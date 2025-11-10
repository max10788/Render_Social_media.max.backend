"""
Exchange Collector - CCXT Integration (IMPROVED VERSION)

HAUPTÄNDERUNG:
- OHLCV-Fallback für historische Trade-Daten
- Bitget/Binance speichern Trades nur ~5-10 Minuten
- Für historische Analyse: Nutze Candle-Daten statt Trades

Sammelt Daten von Centralized Exchanges:
- Bitget
- Binance  
- Kraken

Verwendet CCXT Library für einheitliches Interface
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import ccxt.async_support as ccxt

from .base import BaseCollector
from ..utils.constants import (
    SupportedExchange,
    EXCHANGE_CONFIGS,
    EXCHANGE_RATE_LIMITS,
    TIMEFRAME_TO_MS,
    ERROR_MESSAGES,
)


logger = logging.getLogger(__name__)


class ExchangeCollector(BaseCollector):
    """
    Collector für CEX-Daten via CCXT
    
    Unterstützt:
    - Bitget
    - Binance
    - Kraken
    
    WICHTIG: Trades sind nur ~5-10 Minuten verfügbar!
    Für historische Daten wird OHLCV-Fallback verwendet.
    """
    
    def __init__(
        self,
        exchange_name: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialisiert Exchange Collector
        
        Args:
            exchange_name: Name der Exchange (bitget/binance/kraken)
            api_key: API Key (optional, für private endpoints)
            api_secret: API Secret (optional)
            config: Zusätzliche Konfiguration
        """
        super().__init__(config)
        
        # Validiere Exchange
        if exchange_name.lower() not in [e.value for e in SupportedExchange]:
            raise ValueError(
                ERROR_MESSAGES["unsupported_exchange"].format(
                    exchange=exchange_name,
                    exchanges=", ".join([e.value for e in SupportedExchange])
                )
            )
        
        self.exchange_name = exchange_name.lower()
        self.exchange_config = EXCHANGE_CONFIGS.get(self.exchange_name, {})
        
        # Rate Limiting - SETZE DEFAULTS ZUERST!
        self.rate_limit = EXCHANGE_RATE_LIMITS.get(self.exchange_name, 20)  # Default: 20
        self._last_request_time = None
        
        # Exchange initialisieren (kann fehlschlagen)
        try:
            self.exchange = self._init_exchange(api_key, api_secret)
        except Exception as e:
            logger.error(f"Fehler beim Initialisieren von {exchange_name}: {e}")
            # Setze Dummy-Exchange für Fallback
            self.exchange = None
            raise
        
        logger.info(
            f"ExchangeCollector initialisiert für {self.exchange_name.upper()}"
        )
    
    def _init_exchange(
        self,
        api_key: Optional[str],
        api_secret: Optional[str]
    ) -> ccxt.Exchange:
        """
        Initialisiert CCXT Exchange Instance
        
        Returns:
            CCXT Exchange Object
        """
        try:
            exchange_class = getattr(ccxt, self.exchange_name)
        except AttributeError:
            raise ValueError(f"Exchange '{self.exchange_name}' not found in ccxt")
        
        config = {
            'enableRateLimit': True,
            'rateLimit': 1000 / self.rate_limit,
        }
        
        # API Credentials (optional)
        if api_key and api_secret:
            config['apiKey'] = api_key
            config['secret'] = api_secret
        
        # Exchange-spezifische Optionen
        if self.exchange_name == SupportedExchange.KRAKEN:
            config['options'] = {'adjustForTimeDifference': True}
        
        # Binance Geo-Blocking Fix - Vollständige URL-Konfiguration
        if self.exchange_name == 'binance':
            config['hostname'] = 'data-api.binance.vision'  # Haupt-Hostname
            config['urls'] = {
                'api': {
                    'public': 'https://data-api.binance.vision/api/v3',
                    'private': 'https://api.binance.com/api/v3',  # Falls du API Keys nutzt
                },
                'www': 'https://www.binance.com',
            }
            # Deaktiviere Futures/Margin APIs (die verursachen das fapi.binance.com Problem)
            config['options'] = {
                'defaultType': 'spot',  # Nur Spot Trading
                'fetchMarkets': ['spot'],  # Nur Spot Markets laden
            }
            logger.info("Binance mit Geo-Blocking Workaround konfiguriert (Spot-only)")
        
        # Bitget-spezifische Konfiguration
        if self.exchange_name == 'bitget':
            config['options'] = {
                'defaultType': 'spot',
            }
            logger.info("Bitget mit Spot-Konfiguration initialisiert")
        
        return exchange_class(config)
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetcht eine einzelne OHLCV Candle
        
        Args:
            symbol: Trading Pair (z.B. BTC/USDT)
            timeframe: Timeframe (z.B. 5m, 1h)
            timestamp: Zeitpunkt der Candle
            
        Returns:
            Dictionary mit Candle-Daten
        """
        if not self.exchange:
            raise RuntimeError(f"Exchange {self.exchange_name} not initialized")
        
        await self._rate_limit_wait()
        
        try:
            # Konvertiere Zeitpunkt zu Millisekunden
            since = int(timestamp.timestamp() * 1000)
            
            # Fetch OHLCV
            ohlcv = await self.exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=since,
                limit=1
            )
            
            if not ohlcv:
                raise ValueError(ERROR_MESSAGES["no_data_available"])
            
            # Parse OHLCV
            candle = ohlcv[0]
            
            result = {
                'timestamp': datetime.fromtimestamp(candle[0] / 1000),
                'open': float(candle[1]),
                'high': float(candle[2]),
                'low': float(candle[3]),
                'close': float(candle[4]),
                'volume': float(candle[5]),
            }
            
            logger.debug(
                f"Candle gefetcht: {symbol} {timeframe} @ {result['timestamp']}"
            )
            
            return result
            
        except Exception as e:
            logger.error(
                f"Fehler beim Fetchen von Candle-Daten: {e}",
                exc_info=True
            )
            raise
    
    async def fetch_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetcht Trade History im Zeitraum
        
        ⚠️ WICHTIG: Exchanges speichern Trades nur ~5-10 Minuten!
        Für historische Daten wird automatisch OHLCV-Fallback verwendet.
        
        Args:
            symbol: Trading Pair
            start_time: Start-Zeitpunkt
            end_time: End-Zeitpunkt
            limit: Max. Anzahl Trades pro Request
            
        Returns:
            Liste von Trades (oder synthetische Trades aus OHLCV)
        """
        if not self.exchange:
            raise RuntimeError(f"Exchange {self.exchange_name} not initialized")
        
        # Prüfe, ob Zeitbereich zu alt ist (> 10 Minuten)
        time_diff = datetime.now() - start_time
        is_historical = time_diff > timedelta(minutes=10)
        
        if is_historical:
            logger.warning(
                f"Angeforderter Zeitbereich ist historisch ({time_diff} alt). "
                f"Trades sind nur ~5-10 Minuten verfügbar. "
                f"Verwende OHLCV-Fallback für {symbol}."
            )
            return await self._fetch_trades_from_ohlcv_fallback(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time
            )
        
        await self._rate_limit_wait()
        
        try:
            logger.info(f"=== fetch_trades Debug Info ===")
            logger.info(f"Exchange: {self.exchange_name}")
            logger.info(f"Symbol: {symbol}")
            logger.info(f"Start time: {start_time}")
            logger.info(f"End time: {end_time}")
            
            # Konvertiere Zeitstempel
            since = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            # Prüfe, ob Symbol existiert
            if not hasattr(self.exchange, 'markets') or not self.exchange.markets:
                await self.exchange.load_markets()
            
            if symbol not in self.exchange.markets:
                logger.error(f"Symbol {symbol} not found in {self.exchange_name} markets")
                return []
            
            all_trades = []
            
            # Versuche Trades zu holen
            try:
                trades = await self.exchange.fetch_trades(
                    symbol=symbol,
                    since=since,
                    limit=limit
                )
                
                # Filtere Trades im Zeitfenster
                for trade in trades:
                    trade_time = trade['timestamp']
                    if since <= trade_time <= end_ms:
                        parsed_trade = self._parse_trade(trade)
                        all_trades.append(parsed_trade)
                
                logger.info(f"Trades erhalten: {len(all_trades)} im Zeitfenster")
                
            except Exception as e:
                logger.warning(f"fetch_trades fehlgeschlagen: {e}")
            
            # FALLBACK: Wenn keine Trades verfügbar, nutze OHLCV
            if len(all_trades) == 0:
                logger.warning(
                    f"Keine Trades verfügbar für {symbol} @ {start_time} - "
                    f"Verwende OHLCV-Fallback"
                )
                return await self._fetch_trades_from_ohlcv_fallback(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time
                )
            
            return all_trades
            
        except Exception as e:
            logger.error(
                f"Fehler beim Fetchen von Trades: {e}",
                exc_info=True
            )
            # Versuche OHLCV-Fallback als letztes Mittel
            try:
                return await self._fetch_trades_from_ohlcv_fallback(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time
                )
            except:
                raise  # Original-Fehler wenn auch Fallback fehlschlägt
    
    async def _fetch_trades_from_ohlcv_fallback(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fallback: Erstelle synthetische Trades aus OHLCV-Daten
        
        Verwendet 1-Minuten Candles um Trade-Aktivität zu approximieren.
        Nützlich für historische Daten, wo echte Trades nicht mehr verfügbar sind.
        
        Args:
            symbol: Trading Pair
            start_time: Start-Zeitpunkt
            end_time: End-Zeitpunkt
            
        Returns:
            Liste von synthetischen Trades
        """
        try:
            logger.info(f"OHLCV-Fallback aktiviert für {symbol}")
            
            # Hole 1-Minuten Candles für beste Granularität
            ohlcv_data = await self.fetch_ohlcv_range(
                symbol=symbol,
                timeframe='1m',
                start_time=start_time,
                end_time=end_time
            )
            
            if not ohlcv_data:
                logger.warning("Keine OHLCV-Daten verfügbar für Fallback")
                return []
            
            logger.info(
                f"OHLCV-Fallback: {len(ohlcv_data)} Candles gefunden - "
                f"Erstelle synthetische Trades"
            )
            
            synthetic_trades = []
            
            for candle in ohlcv_data:
                # Erstelle 4 synthetische Trades pro Candle (Open, High, Low, Close)
                # Dies approximiert die Trade-Aktivität basierend auf Preis-Bewegungen
                volume_per_trade = candle['volume'] / 4
                
                # Bestimme Trade-Richtung basierend auf Candle-Bewegung
                is_bullish = candle['close'] > candle['open']
                
                synthetic_trades.extend([
                    {
                        'id': f"ohlcv_{candle['timestamp'].isoformat()}_open",
                        'timestamp': candle['timestamp'],
                        'trade_type': 'buy' if is_bullish else 'sell',
                        'amount': volume_per_trade,
                        'price': candle['open'],
                        'value_usd': volume_per_trade * candle['open'],
                    },
                    {
                        'id': f"ohlcv_{candle['timestamp'].isoformat()}_high",
                        'timestamp': candle['timestamp'] + timedelta(seconds=15),
                        'trade_type': 'buy',  # High = Kaufdruck
                        'amount': volume_per_trade,
                        'price': candle['high'],
                        'value_usd': volume_per_trade * candle['high'],
                    },
                    {
                        'id': f"ohlcv_{candle['timestamp'].isoformat()}_low",
                        'timestamp': candle['timestamp'] + timedelta(seconds=30),
                        'trade_type': 'sell',  # Low = Verkaufsdruck
                        'amount': volume_per_trade,
                        'price': candle['low'],
                        'value_usd': volume_per_trade * candle['low'],
                    },
                    {
                        'id': f"ohlcv_{candle['timestamp'].isoformat()}_close",
                        'timestamp': candle['timestamp'] + timedelta(seconds=45),
                        'trade_type': 'buy' if is_bullish else 'sell',
                        'amount': volume_per_trade,
                        'price': candle['close'],
                        'value_usd': volume_per_trade * candle['close'],
                    },
                ])
            
            logger.info(
                f"✅ OHLCV-Fallback erfolgreich: {len(synthetic_trades)} "
                f"synthetische Trades erstellt aus {len(ohlcv_data)} Candles"
            )
            
            return synthetic_trades
            
        except Exception as e:
            logger.error(f"OHLCV-Fallback fehlgeschlagen: {e}", exc_info=True)
            return []
    
    def _parse_trade(self, raw_trade: Dict) -> Dict[str, Any]:
        """
        Parsed einen Trade aus CCXT Format
        
        Args:
            raw_trade: Raw Trade von CCXT
            
        Returns:
            Parsed Trade Dictionary
        """
        return {
            'id': raw_trade.get('id'),
            'timestamp': datetime.fromtimestamp(
                raw_trade['timestamp'] / 1000
            ) if raw_trade.get('timestamp') else datetime.now(),
            'trade_type': raw_trade.get('side', 'unknown'),  # 'buy' oder 'sell'
            'amount': float(raw_trade.get('amount', 0.0)),
            'price': float(raw_trade.get('price', 0.0)),
            'value_usd': float(raw_trade.get('cost', 0.0)),  # In Quote Currency
        }
    
    async def fetch_orderbook(
        self,
        symbol: str,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Fetcht Orderbook
        
        Args:
            symbol: Trading Pair
            limit: Anzahl Bids/Asks
            
        Returns:
            Dictionary mit Orderbook-Daten
        """
        if not self.exchange:
            raise RuntimeError(f"Exchange {self.exchange_name} not initialized")
        
        await self._rate_limit_wait()
        
        try:
            orderbook = await self.exchange.fetch_order_book(
                symbol=symbol,
                limit=limit
            )
            
            result = {
                'timestamp': datetime.fromtimestamp(
                    orderbook['timestamp'] / 1000
                ) if orderbook.get('timestamp') else datetime.now(),
                'bids': orderbook['bids'],  # [[price, amount], ...]
                'asks': orderbook['asks'],
                'bid': orderbook['bids'][0][0] if orderbook['bids'] else None,
                'ask': orderbook['asks'][0][0] if orderbook['asks'] else None,
                'spread': None,
            }
            
            # Berechne Spread
            if result['bid'] and result['ask']:
                result['spread'] = result['ask'] - result['bid']
                result['spread_pct'] = (result['spread'] / result['bid']) * 100
            
            logger.debug(f"Orderbook gefetcht: {symbol}")
            
            return result
            
        except Exception as e:
            logger.error(
                f"Fehler beim Fetchen von Orderbook: {e}",
                exc_info=True
            )
            raise
    
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Fetcht aktuellen Ticker (Preis, Volume, etc.)
        
        Args:
            symbol: Trading Pair
            
        Returns:
            Dictionary mit Ticker-Daten
        """
        if not self.exchange:
            raise RuntimeError(f"Exchange {self.exchange_name} not initialized")
        
        await self._rate_limit_wait()
        
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            
            result = {
                'symbol': ticker['symbol'],
                'timestamp': datetime.fromtimestamp(
                    ticker['timestamp'] / 1000
                ) if ticker.get('timestamp') else datetime.now(),
                'last': float(ticker['last']),
                'bid': float(ticker['bid']) if ticker.get('bid') else None,
                'ask': float(ticker['ask']) if ticker.get('ask') else None,
                'high': float(ticker['high']) if ticker.get('high') else None,
                'low': float(ticker['low']) if ticker.get('low') else None,
                'volume': float(ticker['baseVolume']) if ticker.get('baseVolume') else None,
                'quote_volume': float(ticker['quoteVolume']) if ticker.get('quoteVolume') else None,
                'change': float(ticker['change']) if ticker.get('change') else None,
                'percentage': float(ticker['percentage']) if ticker.get('percentage') else None,
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Fehler beim Fetchen von Ticker: {e}", exc_info=True)
            raise
    
    async def fetch_ohlcv_range(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetcht mehrere OHLCV Candles in einem Zeitraum
        
        Args:
            symbol: Trading Pair
            timeframe: Timeframe
            start_time: Start
            end_time: Ende
            
        Returns:
            Liste von Candles
        """
        if not self.exchange:
            raise RuntimeError(f"Exchange {self.exchange_name} not initialized")
        
        await self._rate_limit_wait()
        
        try:
            since = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            all_candles = []
            
            # Fetch in Batches
            while since < end_ms:
                ohlcv = await self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    since=since,
                    limit=500
                )
                
                if not ohlcv:
                    break
                
                for candle in ohlcv:
                    if candle[0] <= end_ms:
                        all_candles.append({
                            'timestamp': datetime.fromtimestamp(candle[0] / 1000),
                            'open': float(candle[1]),
                            'high': float(candle[2]),
                            'low': float(candle[3]),
                            'close': float(candle[4]),
                            'volume': float(candle[5]),
                        })
                
                # Update since
                since = ohlcv[-1][0] + TIMEFRAME_TO_MS.get(timeframe, 60000)
                
                if len(ohlcv) < 500:
                    break
                
                await asyncio.sleep(0.1)
            
            logger.info(
                f"{len(all_candles)} Candles gefetcht für {symbol} {timeframe}"
            )
            
            return all_candles
            
        except Exception as e:
            logger.error(
                f"Fehler beim Fetchen von OHLCV Range: {e}",
                exc_info=True
            )
            raise
    
    async def health_check(self) -> bool:
        """
        Prüft ob Exchange erreichbar ist
        
        Returns:
            True wenn erreichbar
        """
        if not self.exchange:
            logger.error(f"Exchange {self.exchange_name} not initialized")
            return False
        
        try:
            await self.exchange.load_markets()
            logger.info(f"Health Check OK: {self.exchange_name}")
            return True
        except Exception as e:
            logger.error(
                f"Health Check FAILED für {self.exchange_name}: {e}"
            )
            return False
    
    async def _rate_limit_wait(self):
        """Wartet falls Rate Limit erreicht"""
        if self._last_request_time:
            elapsed = (datetime.now() - self._last_request_time).total_seconds()
            min_interval = 60 / self.rate_limit  # Sekunden pro Request
            
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                await asyncio.sleep(wait_time)
        
        self._last_request_time = datetime.now()

    async def fetch_aggregate_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetcht Aggregated Trades (bessere Entity-Approximation)
        
        Binance: Native aggTrades API
        Bitget: Manuelle Aggregation
        Kraken: Normale Trades
        
        Aggregierte Trades gruppieren kleine Trades vom gleichen Maker
        → Bessere "Entity" Erkennung
        
        Args:
            symbol: Trading Pair
            start_time: Start-Zeitpunkt
            end_time: End-Zeitpunkt
            limit: Max. Anzahl Trades
            
        Returns:
            Liste von aggregierten Trades
        """
        if not self.exchange:
            raise RuntimeError(f"Exchange {self.exchange_name} not initialized")
        
        # Prüfe, ob historisch
        time_diff = datetime.now() - start_time
        is_historical = time_diff > timedelta(minutes=10)
        
        if is_historical:
            logger.warning(
                f"Aggregate trades nicht verfügbar für historische Daten. "
                f"Verwende OHLCV-Fallback."
            )
            return await self._fetch_trades_from_ohlcv_fallback(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time
            )
        
        # Exchange-spezifische Implementierung
        if self.exchange_name == 'binance':
            return await self._fetch_binance_agg_trades(symbol, start_time, end_time, limit)
        elif self.exchange_name == 'bitget':
            return await self._fetch_bitget_agg_trades(symbol, start_time, end_time, limit)
        elif self.exchange_name == 'kraken':
            # Kraken hat keine aggTrades → normale Trades + manuelle Aggregation
            trades = await self.fetch_trades(symbol, start_time, end_time, limit)
            return self._aggregate_trades_manually(trades)
        else:
            # Fallback: Normale Trades
            return await self.fetch_trades(symbol, start_time, end_time, limit)
    
    async def _fetch_binance_agg_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        Binance Native Aggregated Trades
        
        Verwendet /api/v3/aggTrades Endpoint
        Trades vom gleichen Maker sind bereits gruppiert
        """
        await self._rate_limit_wait()
        
        try:
            # Binance aggTrades nutzt CCXT nicht direkt
            # Wir müssen den Raw API Call machen
            import aiohttp
            
            url = "https://data-api.binance.vision/api/v3/aggTrades"
            
            params = {
                'symbol': symbol.replace('/', ''),  # BTCUSDT statt BTC/USDT
                'startTime': int(start_time.timestamp() * 1000),
                'endTime': int(end_time.timestamp() * 1000),
                'limit': min(limit, 1000)  # Max 1000 per request
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Binance aggTrades failed: {response.status}")
                        # Fallback zu normalen Trades
                        return await self.fetch_trades(symbol, start_time, end_time, limit)
                    
                    agg_trades_raw = await response.json()
            
            # Parse zu unserem Format
            agg_trades = []
            for agg in agg_trades_raw:
                agg_trades.append({
                    'id': f"agg_{agg['a']}",  # Aggregate trade ID
                    'timestamp': datetime.fromtimestamp(agg['T'] / 1000),
                    'trade_type': 'buy' if agg['m'] else 'sell',  # m = is buyer maker
                    'amount': float(agg['q']),  # Quantity
                    'price': float(agg['p']),   # Price
                    'value_usd': float(agg['q']) * float(agg['p']),
                    'first_trade_id': agg['f'],  # First trade ID in aggregate
                    'last_trade_id': agg['l'],   # Last trade ID in aggregate
                    'trade_count': agg['l'] - agg['f'] + 1,  # 🆕 Anzahl aggregierter Trades
                })
            
            logger.info(
                f"✅ Binance aggTrades: {len(agg_trades)} aggregated trades fetched "
                f"(avg {sum(t['trade_count'] for t in agg_trades) / len(agg_trades):.1f} trades per aggregate)"
            )
            
            return agg_trades
            
        except Exception as e:
            logger.error(f"Binance aggTrades error: {e}", exc_info=True)
            # Fallback zu normalen Trades
            return await self.fetch_trades(symbol, start_time, end_time, limit)
    
    async def _fetch_bitget_agg_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        Bitget: Hole normale Trades + manuelle Aggregation
        
        Bitget hat keinen nativen aggTrades Endpoint
        """
        # Hole normale Trades
        trades = await self.fetch_trades(symbol, start_time, end_time, limit)
        
        # Aggregiere manuell
        return self._aggregate_trades_manually(trades)
    
    def _aggregate_trades_manually(
        self,
        trades: List[Dict[str, Any]],
        time_threshold_seconds: float = 5.0,
        size_threshold_pct: float = 0.05
    ) -> List[Dict[str, Any]]:
        """
        Manuelle Trade-Aggregation
        
        Gruppiert Trades die wahrscheinlich vom gleichen Entity stammen:
        - Zeitlich nah (<5 Sekunden)
        - Ähnliche Größe (±5%)
        - Gleiche Richtung (buy/sell)
        - Ähnlicher Preis (±0.1%)
        
        Args:
            trades: Liste von Trades
            time_threshold_seconds: Max. Zeit zwischen Trades
            size_threshold_pct: Max. Größen-Unterschied (als %)
            
        Returns:
            Liste von aggregierten Trades
        """
        if not trades:
            return []
        
        # Sortiere nach Zeit
        sorted_trades = sorted(trades, key=lambda t: t['timestamp'])
        
        aggregated = []
        current_group = []
        
        for trade in sorted_trades:
            if not current_group:
                # Starte neue Gruppe
                current_group.append(trade)
                continue
            
            last_trade = current_group[-1]
            
            # Prüfe Ähnlichkeit
            time_diff = (trade['timestamp'] - last_trade['timestamp']).total_seconds()
            
            # Prüfe Size-Ähnlichkeit (nur wenn > 0)
            if last_trade['amount'] > 0:
                size_diff_pct = abs(trade['amount'] - last_trade['amount']) / last_trade['amount']
            else:
                size_diff_pct = 1.0  # Nicht ähnlich
            
            # Prüfe Preis-Ähnlichkeit
            if last_trade['price'] > 0:
                price_diff_pct = abs(trade['price'] - last_trade['price']) / last_trade['price']
            else:
                price_diff_pct = 1.0
            
            same_side = trade['trade_type'] == last_trade['trade_type']
            
            # Aggregations-Kriterien
            should_aggregate = (
                time_diff < time_threshold_seconds and
                size_diff_pct < size_threshold_pct and
                price_diff_pct < 0.001 and  # ±0.1% Preis
                same_side
            )
            
            if should_aggregate:
                # Füge zu aktueller Gruppe hinzu
                current_group.append(trade)
            else:
                # Finalize aktuelle Gruppe
                if len(current_group) >= 1:  # Auch einzelne Trades behalten
                    aggregated.append(self._merge_trade_group(current_group))
                
                # Starte neue Gruppe
                current_group = [trade]
        
        # Don't forget last group
        if current_group:
            aggregated.append(self._merge_trade_group(current_group))
        
        logger.info(
            f"✅ Manual aggregation: {len(trades)} trades → {len(aggregated)} aggregated "
            f"(compression: {len(trades) / len(aggregated):.1f}x)"
        )
        
        return aggregated
    
    def _merge_trade_group(self, group: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merged eine Gruppe von Trades zu einem Aggregat
        
        Args:
            group: Liste von Trades
            
        Returns:
            Aggregierter Trade
        """
        if len(group) == 1:
            # Einzelner Trade → füge nur trade_count hinzu
            trade = group[0].copy()
            trade['trade_count'] = 1
            return trade
        
        # Multiple Trades → merge
        total_amount = sum(t['amount'] for t in group)
        total_value = sum(t['value_usd'] for t in group)
        
        # Volume-weighted average price
        vwap = total_value / total_amount if total_amount > 0 else group[0]['price']
        
        return {
            'id': f"agg_{group[0]['id']}",
            'timestamp': group[0]['timestamp'],  # Nutze ersten Timestamp
            'trade_type': group[0]['trade_type'],
            'amount': total_amount,
            'price': vwap,
            'value_usd': total_value,
            'trade_count': len(group),  # 🆕 Wie viele Trades wurden gemerged
            'first_trade_id': group[0]['id'],
            'last_trade_id': group[-1]['id'],
            'time_span_seconds': (group[-1]['timestamp'] - group[0]['timestamp']).total_seconds()
        }

    async def close(self):
        """Schließt Exchange Connection"""
        if self.exchange:
            await self.exchange.close()
            logger.info(f"Exchange Connection geschlossen: {self.exchange_name}")
    
    def __str__(self) -> str:
        return f"ExchangeCollector({self.exchange_name.upper()})"
    
    def __repr__(self) -> str:
        return self.__str__()


class ExchangeCollectorFactory:
    """
    Factory für Exchange Collectors
    
    Erstellt Collectors für verschiedene Exchanges
    """
    
    @staticmethod
    def create(
        exchange_name: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> ExchangeCollector:
        """
        Erstellt einen Exchange Collector
        
        Args:
            exchange_name: bitget/binance/kraken
            api_key: API Key (optional)
            api_secret: API Secret (optional)
            config: Zusätzliche Config
            
        Returns:
            ExchangeCollector Instance
        """
        return ExchangeCollector(
            exchange_name=exchange_name,
            api_key=api_key,
            api_secret=api_secret,
            config=config
        )
    
    @staticmethod
    async def create_all(
        credentials: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, ExchangeCollector]:
        """
        Erstellt Collectors für alle unterstützten Exchanges
        
        Args:
            credentials: Dictionary mit API Credentials
                {
                    'binance': {'api_key': '...', 'api_secret': '...'},
                    'bitget': {...},
                    'kraken': {...}
                }
        
        Returns:
            Dictionary: exchange_name -> ExchangeCollector
        """
        credentials = credentials or {}
        collectors = {}
        
        for exchange in SupportedExchange:
            creds = credentials.get(exchange.value, {})
            
            try:
                collector = ExchangeCollectorFactory.create(
                    exchange_name=exchange.value,
                    api_key=creds.get('api_key'),
                    api_secret=creds.get('api_secret')
                )
                collectors[exchange.value] = collector
                logger.info(f"✓ {exchange.value} collector created")
            except Exception as e:
                logger.error(f"✗ Failed to create {exchange.value} collector: {e}")
                # Continue with other exchanges
                continue
        
        logger.info(
            f"Exchange Collectors erstellt: {', '.join(collectors.keys())}"
        )
        
        return collectors
