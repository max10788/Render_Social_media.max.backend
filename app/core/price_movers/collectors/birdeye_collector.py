"""
Birdeye Collector - Solana DEX Data (EMPFOHLEN)

Birdeye API für Solana DEXs:
- Jupiter
- Raydium
- Orca
- Alle anderen Solana DEXs

API Docs: https://docs.birdeye.so

Rate Limits:
- Free: 100 requests/minute
- Paid: 300 requests/minute
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import aiohttp

from .dex_collector import DEXCollector
from ..utils.constants import (
    BlockchainNetwork,
    API_ENDPOINTS,
    DEX_API_RATE_LIMITS,
)


logger = logging.getLogger(__name__)


class BirdeyeCollector(DEXCollector):
    """
    Birdeye API Collector für Solana DEXs
    
    Features:
    - ✅ Bereits geparste Daten
    - ✅ Wallet-Adressen inkludiert
    - ✅ DEX-Zuordnung (Jupiter/Raydium/Orca)
    - ✅ USD-Werte berechnet
    - ✅ Multi-DEX Support
    
    API Key: Erforderlich (kostenlos auf birdeye.so)
    """
    
    BASE_URL = API_ENDPOINTS['birdeye']['base_url']
    
    def __init__(
        self,
        api_key: str,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialisiert Birdeye Collector
        
        Args:
            api_key: Birdeye API Key (https://birdeye.so)
            config: Zusätzliche Konfiguration
        """
        if not api_key:
            raise ValueError("Birdeye API Key erforderlich!")
        
        super().__init__(
            dex_name="birdeye",  # Multi-DEX
            blockchain=BlockchainNetwork.SOLANA,
            api_key=api_key,
            config=config
        )
        
        self.rate_limit = DEX_API_RATE_LIMITS.get('birdeye', 100)
        self._last_request_time = None
        
        logger.info("✓ Birdeye Collector initialisiert (Solana Multi-DEX)")
    
    async def fetch_dex_trades(
        self,
        token_address: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetcht DEX Trades von Birdeye API
        
        Endpoint: /defi/txs/token
        
        Args:
            token_address: Solana Token Mint Address
            start_time: Start-Zeitpunkt
            end_time: End-Zeitpunkt
            limit: Max. Anzahl Trades (max 100 per request)
            
        Returns:
            Liste von Trades mit ECHTEN Wallet-Adressen
        """
        await self._rate_limit_wait()
        
        try:
            # Birdeye nutzt Pagination (max 100 per request)
            all_trades = []
            offset = 0
            batch_size = min(limit, 100)
            
            while len(all_trades) < limit:
                logger.debug(
                    f"Fetching trades: offset={offset}, limit={batch_size}"
                )
                
                trades_batch = await self._fetch_trades_batch(
                    token_address=token_address,
                    offset=offset,
                    limit=batch_size
                )
                
                if not trades_batch:
                    logger.debug("Keine weiteren Trades verfügbar")
                    break
                
                # Filtere nach Zeitfenster
                for trade in trades_batch:
                    trade_time = trade['timestamp']
                    
                    if start_time <= trade_time <= end_time:
                        all_trades.append(trade)
                
                # Pagination
                offset += batch_size
                
                # Stoppe wenn weniger als batch_size zurückkamen
                if len(trades_batch) < batch_size:
                    break
                
                # Rate Limit Pause
                await asyncio.sleep(0.1)
            
            logger.info(
                f"✓ Birdeye: {len(all_trades)} Trades gefetcht "
                f"(Token: {token_address[:8]}...)"
            )
            
            return all_trades
            
        except Exception as e:
            logger.error(f"❌ Birdeye fetch_dex_trades error: {e}", exc_info=True)
            return []
    
    async def _fetch_trades_batch(
        self,
        token_address: str,
        offset: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetcht einen Batch von Trades
        
        Args:
            token_address: Token Mint Address
            offset: Pagination Offset
            limit: Batch Size (max 100)
            
        Returns:
            Liste von Trades
        """
        url = f"{self.BASE_URL}{API_ENDPOINTS['birdeye']['trades']}"
        
        headers = {
            "X-API-KEY": self.api_key,
            "Accept": "application/json"
        }
        
        params = {
            "address": token_address,
            "tx_type": "swap",  # Nur Swaps
            "offset": offset,
            "limit": min(limit, 100)  # Max 100 per request
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            f"Birdeye API error: {response.status} - {error_text}"
                        )
                        return []
                    
                    data = await response.json()
                    
                    if not data.get('success'):
                        logger.warning(f"Birdeye API unsuccessful: {data}")
                        return []
                    
                    items = data.get('data', {}).get('items', [])
                    
                    # Parse zu unserem Format
                    trades = []
                    for item in items:
                        trade = self._parse_birdeye_trade(item)
                        if trade:
                            trades.append(trade)
                    
                    return trades
                    
        except asyncio.TimeoutError:
            logger.error("Birdeye API Timeout")
            return []
        except Exception as e:
            logger.error(f"Birdeye API Request error: {e}", exc_info=True)
            return []
    
    def _parse_birdeye_trade(self, raw_trade: Dict) -> Optional[Dict[str, Any]]:
        """
        Parsed einen Birdeye Trade zu unserem Format
        
        Birdeye Format:
        {
            "txHash": "...",
            "blockUnixTime": 1699520450,
            "from": "7xKXtg2CW87d97TXJSDpb...",  # ← Wallet! 🎯
            "side": "buy" oder "sell",
            "amount": "1.5",
            "price": "67500.0",
            "volumeUSD": "101250.0",
            "source": "Jupiter"  # DEX
        }
        
        Args:
            raw_trade: Raw Birdeye Trade
            
        Returns:
            Parsed Trade Dictionary oder None
        """
        try:
            wallet_address = raw_trade.get('from')
            
            if not wallet_address:
                logger.warning("Trade ohne Wallet-Adresse")
                return None
            
            # Parse Timestamp
            block_time = raw_trade.get('blockUnixTime')
            timestamp = datetime.fromtimestamp(block_time) if block_time else datetime.now()
            
            # Parse Trade Type
            side = raw_trade.get('side', '').lower()
            trade_type = 'buy' if side == 'buy' else 'sell'
            
            # Parse Numeric Values
            amount = float(raw_trade.get('amount', 0))
            price = float(raw_trade.get('price', 0))
            value_usd = float(raw_trade.get('volumeUSD', amount * price))
            
            # DEX Source
            dex_source = raw_trade.get('source', 'unknown').lower()
            
            return {
                'id': raw_trade.get('txHash', f"birdeye_{block_time}"),
                'wallet_address': wallet_address,  # ← ECHTE Wallet! 🎯
                'timestamp': timestamp,
                'trade_type': trade_type,
                'amount': amount,
                'price': price,
                'value_usd': value_usd,
                'dex': dex_source,
                'signature': raw_trade.get('txHash'),
                'blockchain': 'solana',
                # Extra Birdeye Fields
                'maker': raw_trade.get('maker'),
                'slot': raw_trade.get('slot'),
            }
            
        except Exception as e:
            logger.warning(f"Failed to parse Birdeye trade: {e}")
            return None
    
    async def _resolve_symbol_to_address(self, symbol: str) -> Optional[str]:
        """
        Resolved Trading Pair Symbol zu Token Mint Address
        
        z.B. SOL/USDC → EPjFWdd5AufqSSqeM2qN1xzYbApSqN1MaPqQb (USDC)
        
        Args:
            symbol: Trading Pair (z.B. SOL/USDC)
            
        Returns:
            Token Mint Address
        """
        # Bekannte Token Mints (Solana)
        known_tokens = {
            'SOL': 'So11111111111111111111111111111111111111112',
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzYbApSqN1MaPqQb7MWstg',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
            'BTC': '9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E',  # Wrapped BTC
            'ETH': '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs',  # Wrapped ETH
        }
        
        # Parse Symbol (z.B. SOL/USDC → SOL)
        parts = symbol.split('/')
        
        if len(parts) != 2:
            logger.error(f"Ungültiges Symbol-Format: {symbol}")
            return None
        
        base_token = parts[0].upper()
        
        # Lookup bekannter Token
        token_address = known_tokens.get(base_token)
        
        if not token_address:
            logger.warning(
                f"Token '{base_token}' nicht in bekannten Tokens. "
                f"Nutze Birdeye Token Search..."
            )
            # TODO: Implementiere Token Search via Birdeye API
            return None
        
        return token_address
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetcht OHLCV Candle von Birdeye
        
        Birdeye hat nativen OHLCV Endpoint!
        
        Endpoint: /defi/ohlcv
        
        Args:
            symbol: Trading Pair
            timeframe: Timeframe (1m, 5m, 15m, 1h, 4h, 1d)
            timestamp: Candle-Zeitpunkt
            
        Returns:
            Candle Dictionary
        """
        await self._rate_limit_wait()
        
        try:
            token_address = await self._resolve_symbol_to_address(symbol)
            
            if not token_address:
                raise ValueError(f"Konnte Token-Adresse nicht auflösen: {symbol}")
            
            url = f"{self.BASE_URL}{API_ENDPOINTS['birdeye']['ohlcv']}"
            
            headers = {
                "X-API-KEY": self.api_key,
                "Accept": "application/json"
            }
            
            # Birdeye nutzt "type" statt "timeframe"
            # 1m, 5m, 15m, 1H, 4H, 1D
            timeframe_map = {
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '30m': '30m',
                '1h': '1H',
                '4h': '4H',
                '1d': '1D',
            }
            
            birdeye_timeframe = timeframe_map.get(timeframe, '5m')
            
            params = {
                "address": token_address,
                "type": birdeye_timeframe,
                "time_from": int(timestamp.timestamp()),
                "time_to": int((timestamp + timedelta(hours=1)).timestamp())
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Birdeye OHLCV error: {response.status} - {error_text}")
                        # Fallback: Aggregiere aus Trades
                        return await super().fetch_candle_data(symbol, timeframe, timestamp)
                    
                    data = await response.json()
                    
                    if not data.get('success'):
                        logger.warning("Birdeye OHLCV unsuccessful, using fallback")
                        return await super().fetch_candle_data(symbol, timeframe, timestamp)
                    
                    items = data.get('data', {}).get('items', [])
                    
                    if not items:
                        raise ValueError("Keine OHLCV Daten verfügbar")
                    
                    # Nutze ersten Candle
                    candle_raw = items[0]
                    
                    return {
                        'timestamp': datetime.fromtimestamp(candle_raw['unixTime']),
                        'open': float(candle_raw['o']),
                        'high': float(candle_raw['h']),
                        'low': float(candle_raw['l']),
                        'close': float(candle_raw['c']),
                        'volume': float(candle_raw['v']),
                    }
                    
        except Exception as e:
            logger.error(f"Birdeye OHLCV fetch error: {e}", exc_info=True)
            # Fallback: Aggregiere aus Trades
            logger.info("Using trade aggregation fallback for candle data")
            return await super().fetch_candle_data(symbol, timeframe, timestamp)
    
    async def health_check(self) -> bool:
        """
        Prüft ob Birdeye API erreichbar ist
        
        Returns:
            True wenn OK
        """
        try:
            url = f"{self.BASE_URL}/public/price?address=So11111111111111111111111111111111111111112"
            
            headers = {
                "X-API-KEY": self.api_key,
                "Accept": "application/json"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    
                    if response.status == 200:
                        logger.info("✓ Birdeye Health Check OK")
                        return True
                    else:
                        logger.error(f"✗ Birdeye Health Check FAILED: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"✗ Birdeye Health Check error: {e}")
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
        """Schließt Connections"""
        logger.info("Birdeye Collector closed")
