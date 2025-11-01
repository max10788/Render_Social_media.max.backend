"""
Exchange Collector - CCXT Integration (FIXED)

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
        
        Args:
            symbol: Trading Pair
            start_time: Start-Zeitpunkt
            end_time: End-Zeitpunkt
            limit: Max. Anzahl Trades pro Request
            
        Returns:
            Liste von Trades
        """
        if not self.exchange:
            raise RuntimeError(f"Exchange {self.exchange_name} not initialized")
        
        await self._rate_limit_wait()
        
        try:
            all_trades = []
            since = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)
            
            # Fetch Trades in Batches
            while since < end_ms:
                trades = await self.exchange.fetch_trades(
                    symbol=symbol,
                    since=since,
                    limit=limit
                )
                
                if not trades:
                    break
                
                # Filtere Trades im Zeitfenster
                for trade in trades:
                    trade_time = trade['timestamp']
                    
                    if trade_time >= since and trade_time <= end_ms:
                        parsed_trade = self._parse_trade(trade)
                        all_trades.append(parsed_trade)
                
                # Update since für nächsten Batch
                since = trades[-1]['timestamp'] + 1
                
                # Verhindere Endlosschleife
                if len(trades) < limit:
                    break
                
                # Rate Limiting
                await asyncio.sleep(0.1)
            
            logger.info(
                f"{len(all_trades)} Trades gefetcht für {symbol} "
                f"({start_time} - {end_time})"
            )
            
            return all_trades
            
        except Exception as e:
            logger.error(
                f"Fehler beim Fetchen von Trades: {e}",
                exc_info=True
            )
            raise
    
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
