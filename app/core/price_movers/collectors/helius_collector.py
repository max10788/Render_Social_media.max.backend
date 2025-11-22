"""
Helius Collector - FIXED VERSION mit erweitertem Zeitfenster

🔧 FIX: Erweitert das Zeitfenster von 1 Minute auf 10 Minuten
Grund: Helius API hat keine Zeitfilter-Parameter und gibt nur die letzten X TXs zurück
"""

import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import time

from .dex_collector import DEXCollector
from ..utils.constants import BlockchainNetwork


logger = logging.getLogger(__name__)


class SimpleCache:
    """Simple in-memory cache with TTL"""
    def __init__(self, ttl_seconds: int = 60):
        self.cache = {}
        self.ttl_seconds = ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl_seconds:
                return value
            del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        self.cache[key] = (value, time.time())
    
    def clear(self):
        self.cache.clear()


class HeliusCollector(DEXCollector):
    """
    Helius Collector - FIXED VERSION
    
    🔧 FIX: Erweitert Zeitfenster für Trade-Abfragen
    - Vorher: Exakt 1 Minute (12:30:00 - 12:31:00)
    - Nachher: ±5 Minuten Puffer (12:25:00 - 12:35:00)
    
    Grund: Helius API gibt einfach die letzten X TXs zurück,
           ohne Zeitfilter. Daher müssen wir ein größeres Fenster nutzen.
    """
    
    API_BASE = "https://api-mainnet.helius-rpc.com"
    
    # 🔧 FIX: Konfigurierbarer Zeitpuffer
    TIME_BUFFER_MINUTES = 5  # ±5 Minuten = 10 Minuten total
    
    TOKEN_MINTS = {
        'SOL': 'So11111111111111111111111111111111111111112',
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    }
    
    def __init__(
        self, 
        api_key: str, 
        config: Optional[Dict[str, Any]] = None,
        birdeye_fallback: Optional['BirdeyeCollector'] = None
    ):
        super().__init__(
            dex_name="helius",
            blockchain=BlockchainNetwork.SOLANA,
            api_key=api_key,
            config=config or {}
        )
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache = SimpleCache(ttl_seconds=60)
        self.birdeye_fallback = birdeye_fallback
        
        # 🔧 Allow custom time buffer from config
        self.time_buffer_minutes = config.get('time_buffer_minutes', self.TIME_BUFFER_MINUTES)
        
        logger.info(f"✅ Helius Collector initialized (Time buffer: ±{self.time_buffer_minutes}m)")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def _resolve_symbol_to_address(self, symbol: str) -> Optional[str]:
        """Resolve symbol to token address"""
        try:
            parts = symbol.upper().split('/')
            if len(parts) != 2:
                return None
            
            base_token, quote_token = parts
            
            # For SOL pairs, query the OTHER token
            if base_token in ['SOL', 'WSOL']:
                return self.TOKEN_MINTS.get(quote_token)
            else:
                return self.TOKEN_MINTS.get(base_token)
            
        except Exception as e:
            logger.error(f"Symbol resolution error: {e}")
            return None
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch CURRENT candle with extended time window
        """
        logger.info(f"🔗 Helius: Fetching CURRENT candle for {symbol}")
        
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Check cache
        cache_key = f"current_{symbol}_{timeframe}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug("📦 Using cached current candle")
            return cached
        
        timeframe_seconds = {
            '1m': 60, '5m': 300, '15m': 900,
            '1h': 3600, '4h': 14400, '1d': 86400,
        }.get(timeframe, 300)
        
        # 🔧 FIX: Erweitere das Zeitfenster
        # Statt nur [timestamp, timestamp+timeframe]
        # Nutze [timestamp-buffer, timestamp+timeframe+buffer]
        buffer_seconds = self.time_buffer_minutes * 60
        
        start_time = timestamp - timedelta(seconds=buffer_seconds)
        end_time = timestamp + timedelta(seconds=timeframe_seconds + buffer_seconds)
        
        logger.info(
            f"🔍 Extended time window: {start_time} to {end_time} "
            f"(±{self.time_buffer_minutes}m buffer)"
        )
        
        trades = await self.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=200
        )
        
        if not trades:
            logger.warning(f"⚠️ No trades found for {symbol} in extended window")
            return {
                'timestamp': timestamp,
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': 0.0
            }
        
        # Filter trades to the ACTUAL candle window (for accurate OHLCV)
        candle_trades = [
            t for t in trades
            if timestamp <= t['timestamp'] < (timestamp + timedelta(seconds=timeframe_seconds))
        ]
        
        if not candle_trades:
            # Use all trades if none in exact window
            logger.warning(f"⚠️ No trades in exact candle window, using all {len(trades)} trades")
            candle_trades = trades
        
        # Aggregate to candle
        prices = [t['price'] for t in candle_trades if t.get('price', 0) > 0]
        volumes = [t['amount'] for t in candle_trades if t.get('amount', 0) > 0]
        
        if not prices:
            return {
                'timestamp': timestamp,
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': sum(volumes) if volumes else 0.0
            }
        
        candle = {
            'timestamp': timestamp,
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes) if volumes else 0.0
        }
        
        self.cache.set(cache_key, candle)
        
        logger.info(
            f"✅ Helius: Candle built from {len(candle_trades)} trades "
            f"({len(trades)} total in extended window)"
        )
        return candle
    
    async def fetch_dex_trades(
        self,
        token_address: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 200
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades with extended time window
        
        🔧 FIX: Das Zeitfenster wurde bereits von fetch_candle_data erweitert,
                sodass wir hier mehr Trades bekommen
        """
        logger.debug(
            f"🔍 Fetching trades: {token_address} "
            f"({start_time} - {end_time})"
        )
        
        session = await self._get_session()
        
        url = f"{self.API_BASE}/v0/addresses/{token_address}/transactions"
        
        params = {
            'api-key': self.api_key,
            'limit': min(limit, 100),
            'type': 'SWAP',
        }
        
        try:
            async with session.get(
                url, 
                params=params, 
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                
                if response.status != 200:
                    logger.error(f"Helius error: {response.status}")
                    return []
                
                data = await response.json()
                
                if not data:
                    return []
                
                # Parse trades
                trades = []
                for tx in data:
                    try:
                        trade = self._parse_transaction(tx)
                        
                        # 🔧 FIX: Mit erweitertem Zeitfenster sollten wir jetzt Trades finden
                        if trade and start_time <= trade['timestamp'] <= end_time:
                            trades.append(trade)
                            
                    except Exception:
                        continue
                
                logger.info(
                    f"✅ Helius: {len(trades)} trades found "
                    f"(from {len(data)} transactions)"
                )
                return trades
                
        except Exception as e:
            logger.error(f"Helius fetch error: {e}")
            return []
    
    def _parse_transaction(self, tx: Dict) -> Optional[Dict[str, Any]]:
        """Parse transaction to trade"""
        try:
            timestamp = datetime.fromtimestamp(tx.get('timestamp', 0), tz=timezone.utc)
            
            token_transfers = tx.get('tokenTransfers', [])
            if not token_transfers:
                return None
            
            transfer = token_transfers[0]
            
            # Get wallet
            wallet = transfer.get('fromUserAccount') or transfer.get('toUserAccount')
            if not wallet:
                return None
            
            # Get amount
            raw_amount = float(transfer.get('tokenAmount', 0))
            if raw_amount <= 0:
                return None
            
            mint = transfer.get('mint', '')
            decimals = 9 if 'So111' in mint else 6
            amount = raw_amount / (10 ** decimals)
            
            # Estimate price
            native_transfers = tx.get('nativeTransfers', [])
            price = 100.0  # Default
            
            if native_transfers:
                sol_amount = sum(float(nt.get('amount', 0)) for nt in native_transfers) / 1e9
                if sol_amount > 0 and amount > 0:
                    price = sol_amount / amount
            
            return {
                'id': tx.get('signature', ''),
                'timestamp': timestamp,
                'trade_type': 'buy',
                'amount': amount,
                'price': price,
                'value_usd': amount * price,
                'wallet_address': wallet,
                'dex': 'jupiter',
                'signature': tx.get('signature'),
                'blockchain': 'solana',
            }
            
        except Exception as e:
            logger.warning(f"Parse error: {e}")
            return None
    
    async def health_check(self) -> bool:
        """Quick health check"""
        try:
            session = await self._get_session()
            url = f"{self.API_BASE}/v0/addresses/So11111111111111111111111111111111111111112/transactions"
            
            params = {
                'api-key': self.api_key,
                'limit': 1
            }
            
            async with session.get(
                url, 
                params=params, 
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                return response.status == 200
                    
        except Exception:
            return False
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
        self.cache.clear()


def create_helius_collector(
    api_key: str, 
    birdeye_collector: Optional['BirdeyeCollector'] = None,
    config: Optional[Dict[str, Any]] = None
) -> HeliusCollector:
    """
    Create Helius Collector with fixed time window
    
    🔧 Config options:
        - time_buffer_minutes: ±X minutes buffer (default: 5)
    """
    return HeliusCollector(
        api_key=api_key,
        config=config or {},
        birdeye_fallback=birdeye_collector
    )
