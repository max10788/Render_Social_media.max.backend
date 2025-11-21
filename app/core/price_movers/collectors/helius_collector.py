"""
Helius Collector - FIXED VERSION with Better Parsing

🔧 CRITICAL FIXES:
1. ✅ Better Transaction Parsing (mehr Fallbacks)
2. ✅ Debug Logging um zu sehen was gefiltert wird
3. ✅ Bessere Symbol Resolution für DEX-Paare
4. ✅ Rate Limiting bleibt
"""

import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from functools import lru_cache
import time

from .dex_collector import DEXCollector
from ..utils.constants import BlockchainNetwork


logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple Rate Limiter with exponential backoff"""
    def __init__(self, max_requests_per_second: int = 5):
        self.max_requests_per_second = max_requests_per_second
        self.min_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0
        self.consecutive_429s = 0
        self.backoff_until = 0
    
    async def wait_if_needed(self):
        """Wait if we're too close to last request or in backoff"""
        now = time.time()
        if now < self.backoff_until:
            wait_time = self.backoff_until - now
            logger.warning(f"⏳ Rate limit backoff: waiting {wait_time:.1f}s")
            await asyncio.sleep(wait_time)
        
        elapsed = now - self.last_request_time
        if elapsed < self.min_interval:
            await asyncio.sleep(self.min_interval - elapsed)
        
        self.last_request_time = time.time()
    
    def record_429(self):
        """Record a 429 response and calculate backoff"""
        self.consecutive_429s += 1
        backoff_seconds = min(2 ** self.consecutive_429s, 300)
        self.backoff_until = time.time() + backoff_seconds
        logger.error(
            f"🚫 Rate limited! Backoff #{self.consecutive_429s}: "
            f"waiting {backoff_seconds}s until {datetime.fromtimestamp(self.backoff_until).strftime('%H:%M:%S')}"
        )
    
    def record_success(self):
        """Record a successful request"""
        self.consecutive_429s = 0


class SimpleCache:
    """Simple in-memory cache with TTL"""
    def __init__(self, ttl_seconds: int = 300):
        self.cache = {}
        self.ttl_seconds = ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired"""
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl_seconds:
                logger.debug(f"✅ Cache HIT: {key}")
                return value
            else:
                logger.debug(f"⌛ Cache EXPIRED: {key}")
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        """Cache a value with current timestamp"""
        self.cache[key] = (value, time.time())
        logger.debug(f"💾 Cache SET: {key}")
    
    def clear(self):
        """Clear all cache"""
        self.cache.clear()
        logger.info("🗑️ Cache cleared")


class HeliusCollector(DEXCollector):
    """
    Helius Collector für Solana DEX Daten - FIXED VERSION
    """
    
    API_BASE = "https://api-mainnet.helius-rpc.com"
    
    DEX_PROGRAMS = {
        'jupiter': 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
        'raydium': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
        'orca': 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
    }
    
    TOKEN_MINTS = {
        'SOL': 'So11111111111111111111111111111111111111112',
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        'RAY': '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',
        'SRM': 'SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt',
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
        
        max_rps = config.get('max_requests_per_second', 5) if config else 5
        self.rate_limiter = RateLimiter(max_requests_per_second=max_rps)
        
        cache_ttl = config.get('cache_ttl_seconds', 300) if config else 300
        self.cache = SimpleCache(ttl_seconds=cache_ttl)
        
        self.birdeye_fallback = birdeye_fallback
        
        self.stats = {
            'requests_made': 0,
            'rate_limits_hit': 0,
            'cache_hits': 0,
            'fallback_uses': 0,
            'errors': 0,
            'transactions_received': 0,
            'transactions_parsed': 0,
            'transactions_filtered': 0,
        }
        
        logger.info(
            f"✅ Helius Collector initialized "
            f"(Rate Limit: {max_rps} req/s, Cache TTL: {cache_ttl}s, "
            f"Fallback: {'Enabled' if birdeye_fallback else 'Disabled'})"
        )
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def _resolve_symbol_to_address(self, symbol: str) -> Optional[str]:
        """
        Resolve trading pair symbol to token mint address
        
        For DEX pairs like SOL/USDT:
        - We fetch BOTH token addresses
        - Primary token is the one we query for
        """
        try:
            parts = symbol.upper().split('/')
            
            if len(parts) != 2:
                logger.error(f"Invalid symbol format: {symbol}")
                return None
            
            base_token, quote_token = parts
            
            # Get both addresses
            base_address = self.TOKEN_MINTS.get(base_token)
            quote_address = self.TOKEN_MINTS.get(quote_token)
            
            if not base_address:
                logger.warning(f"Unknown base token: {base_token}")
                return None
            
            if not quote_address:
                logger.warning(f"Unknown quote token: {quote_token}")
                return None
            
            # For SOL pairs, query the OTHER token (not SOL)
            # because SOL is involved in ALL swaps
            if base_token in ['SOL', 'WSOL']:
                primary_address = quote_address
                logger.info(f"Resolved {symbol} -> {quote_token} ({primary_address[:8]}...)")
            else:
                primary_address = base_address
                logger.info(f"Resolved {symbol} -> {base_token} ({primary_address[:8]}...)")
            
            return primary_address
            
        except Exception as e:
            logger.error(f"Symbol resolution error: {e}", exc_info=True)
            return None
    
    async def fetch_dex_trades(
        self,
        token_address: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """Fetcht DEX Trades von Helius API mit Rate Limiting"""
        cache_key = f"trades_{token_address}_{start_time.isoformat()}_{end_time.isoformat()}_{limit}"
        
        cached = self.cache.get(cache_key)
        if cached is not None:
            self.stats['cache_hits'] += 1
            logger.info(f"📦 Cache hit: {len(cached)} trades")
            return cached
        
        logger.info(f"🔗 Helius: Fetching DEX trades for token {token_address[:8]}...")
        
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        try:
            trades = await self._fetch_enhanced_transactions(
                token_mint=token_address,
                start_time=start_time,
                end_time=end_time,
                limit=limit
            )
            
            if trades:
                self.cache.set(cache_key, trades)
            
            logger.info(f"✅ Helius: {len(trades)} DEX trades fetched")
            return trades
            
        except Exception as e:
            logger.error(f"❌ Helius fetch_dex_trades error: {e}")
            
            if self.birdeye_fallback:
                logger.warning("🔄 Trying Birdeye fallback...")
                try:
                    self.stats['fallback_uses'] += 1
                    trades = await self.birdeye_fallback.fetch_dex_trades(
                        token_address=token_address,
                        start_time=start_time,
                        end_time=end_time,
                        limit=limit
                    )
                    logger.info(f"✅ Birdeye fallback: {len(trades)} trades")
                    return trades
                except Exception as fallback_error:
                    logger.error(f"❌ Birdeye fallback failed: {fallback_error}")
            
            self.stats['errors'] += 1
            return []
    
    async def _fetch_enhanced_transactions(
        self,
        token_mint: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> List[Dict[str, Any]]:
        """Fetch transactions via Helius Enhanced Transactions API"""
        session = await self._get_session()
        
        url = f"{self.API_BASE}/v0/addresses/{token_mint}/transactions"
        
        params = {
            'api-key': self.api_key,
            'limit': min(limit, 100),
            'type': 'SWAP',
        }
        
        all_trades = []
        before_signature = None
        max_iterations = 10
        iterations_done = 0
        
        try:
            for iteration in range(max_iterations):
                iterations_done = iteration + 1
                
                await self.rate_limiter.wait_if_needed()
                
                current_params = params.copy()
                if before_signature:
                    current_params['before'] = before_signature
                
                try:
                    self.stats['requests_made'] += 1
                    
                    async with session.get(
                        url, 
                        params=current_params, 
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        
                        if response.status == 429:
                            self.stats['rate_limits_hit'] += 1
                            error_text = await response.text()
                            logger.error(f"Helius API error: 429 - {error_text}")
                            self.rate_limiter.record_429()
                            await self.rate_limiter.wait_if_needed()
                            continue
                        
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Helius API error: {response.status} - {error_text}")
                            break
                        
                        self.rate_limiter.record_success()
                        data = await response.json()
                
                except aiohttp.ClientError as e:
                    logger.error(f"Helius request error: {e}")
                    self.stats['errors'] += 1
                    break
                
                if not data or len(data) == 0:
                    logger.debug(f"Helius: No more transactions (iteration {iteration})")
                    break
                
                # 🆕 TRACK RECEIVED
                self.stats['transactions_received'] += len(data)
                logger.info(f"📦 Helius batch {iteration + 1}: Received {len(data)} transactions")
                
                # Parse transactions with debug
                batch_trades = []
                parse_failures = 0
                
                for tx in data:
                    try:
                        trade = self._parse_helius_transaction(tx)
                        if trade:
                            batch_trades.append(trade)
                            self.stats['transactions_parsed'] += 1
                        else:
                            parse_failures += 1
                    except Exception as e:
                        logger.warning(f"Failed to parse transaction: {e}")
                        parse_failures += 1
                        continue
                
                logger.info(
                    f"📊 Parsing result: {len(batch_trades)} trades parsed, "
                    f"{parse_failures} failed/filtered"
                )
                
                # Client-side time filtering
                before_filter = len(batch_trades)
                filtered_trades = [
                    t for t in batch_trades
                    if start_time <= t['timestamp'] <= end_time
                ]
                filtered_out = before_filter - len(filtered_trades)
                
                if filtered_out > 0:
                    logger.info(
                        f"⏰ Time filter: {filtered_out} trades outside range "
                        f"({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})"
                    )
                    self.stats['transactions_filtered'] += filtered_out
                
                all_trades.extend(filtered_trades)
                
                # Check if we have enough
                if len(all_trades) >= limit:
                    logger.info(f"Helius: Reached limit of {limit} trades")
                    all_trades = all_trades[:limit]
                    break
                
                # Check if all trades are too old
                if batch_trades and all(t['timestamp'] < start_time for t in batch_trades):
                    logger.info("Helius: All trades before start_time, stopping")
                    break
                
                # Update pagination
                if data:
                    before_signature = data[-1].get('signature')
                    if not before_signature:
                        logger.warning("No signature for pagination, stopping")
                        break
                
                if len(data) < current_params['limit']:
                    logger.info(
                        f"Helius: Received less than limit "
                        f"({len(data)} < {current_params['limit']}), probably no more data"
                    )
                    break
            
            logger.info(
                f"✅ Helius: {len(all_trades)} trades fetched in {iterations_done} iterations "
                f"(from {start_time.strftime('%H:%M:%S')} to {end_time.strftime('%H:%M:%S')})"
            )
            logger.info(
                f"📈 Stats: {self.stats['transactions_received']} received, "
                f"{self.stats['transactions_parsed']} parsed, "
                f"{self.stats['transactions_filtered']} filtered"
            )
            
            return all_trades
            
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            self.stats['errors'] += 1
            return []
    
    def _parse_helius_transaction(self, tx: Dict) -> Optional[Dict[str, Any]]:
        """
        Parse Helius transaction to trade format - IMPROVED VERSION
        """
        try:
            # Get timestamp
            timestamp_val = tx.get('timestamp', 0)
            if not timestamp_val:
                logger.debug("❌ No timestamp")
                return None
            
            timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc)
            
            # Get signature
            signature = tx.get('signature')
            if not signature:
                logger.debug("❌ No signature")
                return None
            
            # Get token transfers
            token_transfers = tx.get('tokenTransfers', [])
            if not token_transfers:
                logger.debug(f"❌ No token transfers in tx {signature[:8]}...")
                return None
            
            # Get native transfers (SOL)
            native_transfers = tx.get('nativeTransfers', [])
            
            # Get account keys
            accounts = tx.get('accountData', [])
            
            # Try to find wallet address
            wallet_address = None
            
            # Method 1: From token transfer
            transfer = token_transfers[0]
            from_user = transfer.get('fromUserAccount')
            to_user = transfer.get('toUserAccount')
            
            if from_user:
                wallet_address = from_user
                trade_type = 'sell'
            elif to_user:
                wallet_address = to_user
                trade_type = 'buy'
            
            # Method 2: From first account
            if not wallet_address and accounts:
                wallet_address = accounts[0].get('account')
                trade_type = 'buy'
            
            # Method 3: From native transfers
            if not wallet_address and native_transfers:
                wallet_address = native_transfers[0].get('fromUserAccount') or native_transfers[0].get('toUserAccount')
                trade_type = 'buy'
            
            if not wallet_address:
                logger.debug(f"❌ No wallet address found in tx {signature[:8]}...")
                return None
            
            # Get amount
            raw_amount = float(transfer.get('tokenAmount', 0))
            if raw_amount <= 0:
                logger.debug(f"❌ Invalid amount: {raw_amount}")
                return None
            
            mint = transfer.get('mint', '')
            decimals = self._get_token_decimals(mint)
            
            # Adjust for decimals
            if decimals > 0:
                amount = raw_amount / (10 ** decimals)
            else:
                amount = raw_amount / 1e9
            
            # Calculate price from SOL transfers
            price = 0.0
            value_usd = 0.0
            
            if native_transfers:
                sol_amount = 0.0
                for nt in native_transfers:
                    sol_val = float(nt.get('amount', 0))
                    if sol_val > 0:
                        sol_amount += sol_val
                
                sol_amount = sol_amount / 1e9  # SOL decimals
                
                if sol_amount > 0 and amount > 0:
                    price = sol_amount / amount
                    value_usd = sol_amount * 210  # Rough SOL price
            
            # If no price, estimate
            if price == 0:
                price = 100.0  # Fallback
                value_usd = amount * price
            
            # Identify DEX
            dex = self._identify_dex(tx)
            
            trade = {
                'id': signature,
                'timestamp': timestamp,
                'trade_type': trade_type,
                'amount': amount,
                'price': price,
                'value_usd': value_usd,
                'wallet_address': wallet_address,
                'dex': dex,
                'signature': signature,
                'blockchain': 'solana',
            }
            
            logger.debug(
                f"✅ Parsed: {trade_type} {amount:.2f} @ ${price:.2f} "
                f"(wallet: {wallet_address[:8]}...)"
            )
            
            return trade
            
        except Exception as e:
            logger.warning(f"Parse error: {e}", exc_info=True)
            return None
    
    def _identify_dex(self, tx: Dict) -> str:
        """Identify which DEX was used"""
        try:
            instructions = tx.get('instructions', [])
            
            for instruction in instructions:
                program_id = instruction.get('programId', '')
                
                for dex_name, dex_program_id in self.DEX_PROGRAMS.items():
                    if program_id == dex_program_id:
                        return dex_name
            
            return 'jupiter'
            
        except Exception:
            return 'unknown'
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Fetch candle data by aggregating trades"""
        logger.info(f"🔗 Helius: Aggregating candle for {symbol}")
        
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        timeframe_map = {
            '1m': 60, '5m': 300, '15m': 900,
            '1h': 3600, '4h': 14400, '1d': 86400,
        }
        
        duration = timeframe_map.get(timeframe, 300)
        start_time = timestamp
        end_time = timestamp + timedelta(seconds=duration)
        
        # Fetch trades
        trades = await self.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=1000
        )
        
        if not trades:
            logger.warning("No trades for candle aggregation")
            return {
                'timestamp': timestamp,
                'open': 0.0,
                'high': 0.0,
                'low': 0.0,
                'close': 0.0,
                'volume': 0.0
            }
        
        # Aggregate to OHLCV
        prices = [t['price'] for t in trades if t.get('price', 0) > 0]
        volumes = [t['amount'] for t in trades if t.get('amount', 0) > 0]
        
        if not prices:
            logger.warning("No valid prices")
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
        
        logger.info(f"✅ Helius Candle: {len(trades)} trades aggregated")
        return candle
    
    def _get_token_decimals(self, mint: str) -> int:
        """Get token decimals for mint address"""
        KNOWN_DECIMALS = {
            'So11111111111111111111111111111111111111112': 9,   # SOL
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 6,  # USDC
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 6,  # USDT
            '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R': 6,  # RAY
            'SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt': 6,  # SRM
        }
        
        return KNOWN_DECIMALS.get(mint, 9)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get collector statistics"""
        return {
            **self.stats,
            'cache_size': len(self.cache.cache),
            'current_backoff': max(0, self.backoff_until - time.time()) 
                if hasattr(self, 'backoff_until') else 0,
        }
    
    async def health_check(self) -> bool:
        """Check Helius API health"""
        try:
            await self.rate_limiter.wait_if_needed()
            
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
                if response.status == 200:
                    logger.info("✅ Helius Health Check: OK")
                    return True
                else:
                    logger.warning(f"⚠️ Helius Health Check: {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Helius Health Check failed: {e}")
            return False
    
    async def close(self):
        """Close session and clear cache"""
        if self.session and not self.session.closed:
            await self.session.close()
        self.cache.clear()
        logger.info(f"✅ Helius Collector closed. Stats: {self.get_stats()}")


def create_helius_collector(
    api_key: str, 
    birdeye_collector: Optional['BirdeyeCollector'] = None,
    config: Optional[Dict[str, Any]] = None
) -> HeliusCollector:
    """Create Helius Collector with optional Birdeye fallback"""
    return HeliusCollector(
        api_key=api_key,
        config=config,
        birdeye_fallback=birdeye_collector
    )
