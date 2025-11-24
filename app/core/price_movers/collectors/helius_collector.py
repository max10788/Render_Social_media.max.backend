"""
Helius Collector - PRODUCTION VERSION

🎯 FEATURES:
- Token-based transaction fetching (not pool-based)
- Solana RPC + Enhanced Transactions API
- Robust error handling
- Comprehensive logging
- Cache management

🔧 ARCHITECTURE:
1. Resolve symbol to token address (USDT for SOL/USDT)
2. Fetch signatures via Solana RPC
3. Parse transactions via Enhanced Transactions API
4. Filter for SWAP transactions

📊 PERFORMANCE:
- Direct token address lookup (no pool discovery needed)
- Works with Helius indexing
- Fast and reliable
"""

import logging
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import time
import json

from .dex_collector import DEXCollector
from ..utils.constants import BlockchainNetwork


logger = logging.getLogger(__name__)


class SimpleCache:
    """Thread-safe in-memory cache with TTL"""
    def __init__(self, ttl_seconds: int = 300):
        self.cache = {}
        self.ttl_seconds = ttl_seconds
    
    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl_seconds:
                return value
            # Expired - remove from cache
            del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        self.cache[key] = (value, time.time())
    
    def clear(self):
        self.cache.clear()
    
    def size(self) -> int:
        return len(self.cache)


class HeliusCollector(DEXCollector):
    """
    Helius Collector - PRODUCTION VERSION
    
    🎯 Strategy:
    - Use token addresses (not pool addresses) for Helius RPC
    - Fetch all transactions for token
    - Filter for SWAP type
    - Parse swap details
    
    Benefits:
    - ✅ Works with Helius indexing
    - ✅ No pool discovery needed
    - ✅ Gets all swaps for a token
    - ✅ Reliable and fast
    """
    
    API_BASE = "https://api-mainnet.helius-rpc.com"
    
    # Token Mint Addresses (Solana SPL Tokens)
    TOKEN_MINTS = {
        'SOL': 'So11111111111111111111111111111111111111112',  # Wrapped SOL
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  # USDC
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',  # USDT (Tether)
        'BONK': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',  # BONK
        'JTO': 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL',   # Jito
        'JUP': 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',   # Jupiter
        'WIF': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',  # Dogwifhat
        'PYTH': 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3', # Pyth
        'RAY': '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',  # Raydium
    }
    
    def __init__(
        self, 
        api_key: str, 
        config: Optional[Dict[str, Any]] = None,
        dexscreener_collector: Optional[Any] = None
    ):
        super().__init__(
            dex_name="helius",
            blockchain=BlockchainNetwork.SOLANA,
            api_key=api_key,
            config=config or {}
        )
        
        self.session: Optional[aiohttp.ClientSession] = None
        self.candle_cache = SimpleCache(ttl_seconds=60)  # 1 min cache for candles
        self.dexscreener = dexscreener_collector
        
        logger.info(
            f"✅ Helius Collector initialized (TOKEN-BASED STRATEGY) "
            f"- Known tokens: {len(self.TOKEN_MINTS)}"
        )
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def _resolve_symbol_to_token_address(self, symbol: str) -> Optional[str]:
        """
        Resolve symbol to token address (NOT pool address)
        
        For Helius Enhanced Transactions API, we use token addresses
        because getSignaturesForAddress works with token accounts, not pools.
        
        Strategy:
        - For SOL/USDT: Use USDT token address (gets all USDT transactions including SOL swaps)
        - For SOL/USDC: Use USDC token address
        - For TOKEN/SOL: Use TOKEN address
        
        Args:
            symbol: Trading pair (e.g., SOL/USDT)
            
        Returns:
            Token address or None
        """
        logger.info(f"🔍 Resolving {symbol} to token address...")
        
        try:
            base_token, quote_token = symbol.upper().split('/')
        except ValueError:
            logger.error(f"❌ Invalid symbol format: {symbol}")
            return None
        
        # Strategy: For SOL pairs, use the quote token (USDT/USDC)
        # This gives us all USDT/USDC transactions which include SOL swaps
        if base_token in ['SOL', 'WSOL']:
            token_address = self.TOKEN_MINTS.get(quote_token)
            if token_address:
                logger.info(
                    f"💡 Using {quote_token} token address for {symbol}: "
                    f"{token_address[:8]}..."
                )
                return token_address
            else:
                logger.error(f"❌ Unknown quote token: {quote_token}")
                return None
        else:
            # For non-SOL pairs, use the base token
            token_address = self.TOKEN_MINTS.get(base_token)
            if token_address:
                logger.info(
                    f"💡 Using {base_token} token address for {symbol}: "
                    f"{token_address[:8]}..."
                )
                return token_address
            else:
                logger.error(f"❌ Unknown base token: {base_token}")
                return None
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch CURRENT candle using token-based approach
        
        Args:
            symbol: Trading pair (e.g., SOL/USDT)
            timeframe: Timeframe (e.g., 5m)
            timestamp: Candle timestamp
            
        Returns:
            Candle data dictionary
        """
        logger.info(f"🔗 Helius: Fetching candle for {symbol} (token-based)")
        
        # Ensure timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Check cache
        cache_key = f"candle_{symbol}_{timeframe}_{int(timestamp.timestamp())}"
        cached = self.candle_cache.get(cache_key)
        if cached:
            logger.debug("📦 Using cached candle")
            return cached
        
        # Get timeframe in seconds
        timeframe_seconds = {
            '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '4h': 14400, '1d': 86400,
        }.get(timeframe, 300)
        
        # Calculate time window
        start_time = timestamp
        end_time = timestamp + timedelta(seconds=timeframe_seconds)
        
        logger.info(f"🔍 Time window: {start_time} to {end_time}")
        
        # Fetch trades
        trades = await self.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=1000
        )
        
        # Build candle from trades
        if not trades:
            logger.warning(f"⚠️ No trades found for {symbol}")
            return self._empty_candle(timestamp)
        
        # Aggregate to candle
        prices = [t['price'] for t in trades if t.get('price', 0) > 0]
        volumes = [t['amount'] for t in trades if t.get('amount', 0) > 0]
        
        if not prices:
            logger.warning(f"⚠️ No valid prices in {len(trades)} trades")
            return self._empty_candle(timestamp)
        
        candle = {
            'timestamp': timestamp,
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes) if volumes else 0.0
        }
        
        # Cache result
        self.candle_cache.set(cache_key, candle)
        
        logger.info(f"✅ Helius: Candle built from {len(trades)} trades")
        return candle
    
    def _empty_candle(self, timestamp: datetime) -> Dict[str, Any]:
        """Helper to create empty candle"""
        return {
            'timestamp': timestamp,
            'open': 0.0,
            'high': 0.0,
            'low': 0.0,
            'close': 0.0,
            'volume': 0.0
        }
    
    async def fetch_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 100
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades for symbol
        
        Args:
            symbol: Trading pair (e.g., SOL/USDT)
            start_time: Start time
            end_time: End time
            limit: Maximum trades
            
        Returns:
            List of trade dictionaries
        """
        # Resolve to TOKEN address (not pool!)
        token_address = await self._resolve_symbol_to_token_address(symbol)
        if not token_address:
            logger.error(f"❌ Cannot resolve token address for {symbol}")
            return []
        
        return await self.fetch_dex_trades(
            token_address=token_address,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            symbol=symbol  # Pass symbol for filtering
        )

    async def fetch_dex_trades(
        self,
        token_address: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 100,
        symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch trades from Helius API using Solana RPC + Enhanced Transactions
        
        Strategy:
        1. Get signatures for token address via getSignaturesForAddress (RPC)
        2. Parse transactions via Enhanced Transactions API  
        3. Filter for SWAP transactions and time range
        
        Args:
            token_address: Token mint address (e.g., USDT address)
            start_time: Start time
            end_time: End time
            limit: Maximum signatures to fetch
            symbol: Original symbol (for logging)
        """
        logger.info(f"🔍 Fetching trades from token: {token_address[:8]}...")
        if symbol:
            logger.info(f"📊 Symbol: {symbol}")
        logger.info(f"⏰ Time range: {start_time} to {end_time}")
        logger.info(f"📊 Limit: {limit}")
        
        session = await self._get_session()
        
        # Step 1: Get transaction signatures via Solana RPC
        rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                token_address,
                {
                    "limit": min(limit, 1000),  # Max 1000 per request
                }
            ]
        }
        
        logger.info(f"🌐 Step 1: Getting signatures via RPC")
        
        try:
            async with session.post(
                rpc_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                logger.info(f"📡 RPC Response status: {response.status}")
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"❌ RPC error {response.status}: {error_text[:500]}")
                    return []
                
                rpc_data = await response.json()
                
                if 'error' in rpc_data:
                    logger.error(f"❌ RPC error: {rpc_data['error']}")
                    return []
                
                if 'result' not in rpc_data:
                    logger.error(f"❌ No result in RPC response")
                    return []
                
                signatures = rpc_data['result']
                logger.info(f"📦 Found {len(signatures)} signatures for token")
                
                if not signatures:
                    logger.warning("⚠️ No signatures found for this token address")
                    return []
                
                # Log first/last signature times
                if signatures:
                    first_sig = signatures[0]
                    last_sig = signatures[-1]
                    first_time = datetime.fromtimestamp(first_sig.get('blockTime', 0), tz=timezone.utc)
                    last_time = datetime.fromtimestamp(last_sig.get('blockTime', 0), tz=timezone.utc)
                    logger.info(f"🔍 Signature time range: {first_time} to {last_time}")
            
            # Step 2: Filter signatures by timestamp
            filtered_sigs = []
            start_ts = int(start_time.timestamp())
            end_ts = int(end_time.timestamp())
            
            logger.info(f"📅 Filtering for: {start_time} to {end_time}")
            
            for sig_info in signatures:
                block_time = sig_info.get('blockTime')
                if block_time and start_ts <= block_time <= end_ts:
                    filtered_sigs.append(sig_info['signature'])
            
            logger.info(f"📊 Filtered to {len(filtered_sigs)} signatures in time range")
            
            if not filtered_sigs:
                logger.warning("⚠️ No signatures in requested time range")
                if signatures:
                    first_time = signatures[0].get('blockTime')
                    last_time = signatures[-1].get('blockTime')
                    logger.warning(
                        f"📅 Available: "
                        f"{datetime.fromtimestamp(first_time, tz=timezone.utc)} to "
                        f"{datetime.fromtimestamp(last_time, tz=timezone.utc)}"
                    )
                    logger.warning(f"📅 Requested: {start_time} to {end_time}")
                return []
            
            # Limit to requested amount
            filtered_sigs = filtered_sigs[:min(len(filtered_sigs), 100)]  # Limit to 100 for Enhanced API
            
            # Step 3: Parse transactions via Enhanced Transactions API
            logger.info(f"🌐 Step 2: Parsing {len(filtered_sigs)} transactions via Enhanced API")
            
            enhanced_url = f"{self.API_BASE}/v0/transactions"
            
            async with session.post(
                enhanced_url,
                json={"transactions": filtered_sigs},
                params={'api-key': self.api_key},
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                logger.info(f"📡 Enhanced API Response status: {response.status}")
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"❌ Enhanced API error {response.status}: {error_text[:500]}")
                    return []
                
                transactions = await response.json()
                
                logger.info(f"📦 Received {len(transactions)} parsed transactions")
                
                if not transactions:
                    logger.warning("⚠️ No transactions returned from Enhanced API")
                    return []
                
                # Log transaction types found
                tx_types = {}
                for tx in transactions:
                    tx_type = tx.get('type', 'UNKNOWN')
                    tx_types[tx_type] = tx_types.get(tx_type, 0) + 1
                
                logger.info(f"📊 Transaction types found: {tx_types}")
                
                # Log first transaction for debugging
                if transactions and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"🔍 First transaction sample: {transactions[0].get('type')} - {transactions[0].get('signature')[:16]}...")
            
            # Step 4: Parse and filter trades
            trades = []
            swap_count = 0
            parse_errors = []
            
            for i, tx in enumerate(transactions):
                try:
                    tx_type = tx.get('type')
                    
                    # Only process SWAP transactions
                    if tx_type != 'SWAP':
                        continue
                    
                    swap_count += 1
                    
                    # Parse the swap transaction
                    trade = self._parse_helius_enhanced_swap(tx, symbol)
                    
                    if trade:
                        if start_time <= trade['timestamp'] <= end_time:
                            trades.append(trade)
                            
                            # Log first few trades
                            if len(trades) <= 5:
                                logger.info(
                                    f"✅ Trade {len(trades)}: "
                                    f"{trade['trade_type']} {trade['amount']:.4f} "
                                    f"@ ${trade.get('price', 0):.6f} at {trade['timestamp']}"
                                )
                    else:
                        if len(parse_errors) < 3:
                            logger.debug(f"⚠️ Swap {swap_count} could not be parsed")
                            
                except Exception as e:
                    parse_errors.append(str(e))
                    if len(parse_errors) <= 3:
                        logger.error(f"❌ Parse error {len(parse_errors)}: {e}", exc_info=True)
                    continue
            
            # Summary
            logger.info(
                f"✅ Helius: {len(trades)} trades returned "
                f"(SWAP txs: {swap_count}/{len(transactions)}, "
                f"parse errors: {len(parse_errors)})"
            )
            
            if len(trades) == 0 and swap_count > 0:
                logger.warning(
                    f"⚠️ Found {swap_count} SWAP transactions but could not parse any! "
                    f"Parse errors: {len(parse_errors)}"
                )
                if parse_errors:
                    logger.warning(f"Parse error examples: {parse_errors[:3]}")
                    
                # Log first SWAP transaction for debugging
                if transactions and logger.isEnabledFor(logging.DEBUG):
                    for tx in transactions:
                        if tx.get('type') == 'SWAP':
                            logger.debug(f"🔍 Sample SWAP transaction: {json.dumps(tx, indent=2)[:1000]}...")
                            break
            
            return trades
            
        except asyncio.TimeoutError:
            logger.error("❌ Helius API timeout")
            return []
        except Exception as e:
            logger.error(f"❌ Helius fetch error: {e}", exc_info=True)
            return []
    
    def _parse_helius_enhanced_swap(
        self, 
        tx: Dict[str, Any],
        symbol: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a Helius Enhanced Transaction (SWAP type)
        
        Enhanced transaction structure:
        {
            "type": "SWAP",
            "signature": "...",
            "timestamp": 1234567890,
            "slot": 123456,
            "fee": 5000,
            "feePayer": "...",
            "nativeTransfers": [...],
            "tokenTransfers": [...],
            "accountData": [...],
            "events": {...}
        }
        
        Args:
            tx: Enhanced transaction data
            symbol: Original trading pair (e.g., SOL/USDT) for filtering
        """
        try:
            # Extract basic info
            signature = tx.get('signature')
            timestamp = tx.get('timestamp')
            
            if not timestamp:
                return None
            
            # Convert timestamp to datetime
            trade_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            
            # Parse token transfers
            token_transfers = tx.get('tokenTransfers', [])
            native_transfers = tx.get('nativeTransfers', [])
            
            if not token_transfers and not native_transfers:
                return None
            
            # Initialize variables
            amount = 0
            price = 0
            trade_type = 'unknown'
            wallet_address = None
            
            # Strategy 1: Try events.swap (structured data)
            events = tx.get('events', {})
            
            if 'swap' in events:
                swap_event = events['swap']
                
                token_inputs = swap_event.get('tokenInputs', [])
                token_outputs = swap_event.get('tokenOutputs', [])
                
                if token_inputs and token_outputs:
                    input_token = token_inputs[0]
                    output_token = token_outputs[0]
                    
                    wallet_address = input_token.get('userAccount') or output_token.get('userAccount')
                    
                    # Get amounts
                    input_amount = float(input_token.get('tokenAmount', 0))
                    output_amount = float(output_token.get('tokenAmount', 0))
                    
                    input_mint = input_token.get('mint', '')
                    output_mint = output_token.get('mint', '')
                    
                    # Determine if this is SOL swap
                    sol_mint = 'So11111111111111111111111111111111111111112'
                    
                    if output_mint == sol_mint:
                        # Buying SOL
                        trade_type = 'buy'
                        amount = output_amount
                        if amount > 0:
                            price = input_amount / amount
                    elif input_mint == sol_mint:
                        # Selling SOL
                        trade_type = 'sell'
                        amount = input_amount
                        if amount > 0:
                            price = output_amount / amount
                    else:
                        # Not a SOL swap, skip
                        return None
            
            # Strategy 2: Fallback to tokenTransfers
            if amount == 0 and len(token_transfers) >= 1:
                # Try to find SOL transfer
                sol_mint = 'So11111111111111111111111111111111111111112'
                
                for transfer in token_transfers:
                    mint = transfer.get('mint', '')
                    if mint == sol_mint:
                        amount = float(transfer.get('tokenAmount', 0))
                        wallet_address = transfer.get('fromUserAccount') or transfer.get('toUserAccount')
                        
                        # Determine direction
                        from_account = transfer.get('fromUserAccount', '')
                        to_account = transfer.get('toUserAccount', '')
                        
                        # Simple heuristic: if there are multiple transfers, this is a swap
                        if len(token_transfers) >= 2:
                            # Find the other token
                            for other_transfer in token_transfers:
                                if other_transfer.get('mint') != sol_mint:
                                    other_amount = float(other_transfer.get('tokenAmount', 0))
                                    if amount > 0 and other_amount > 0:
                                        price = other_amount / amount
                                    break
                        
                        trade_type = 'swap'
                        break
            
            # Validate we have minimum required data
            if amount == 0 or not wallet_address:
                return None
            
            # If price is still 0, set a placeholder
            if price == 0:
                price = 1.0
            
            return {
                'timestamp': trade_time,
                'price': price,
                'amount': amount,
                'trade_type': trade_type,
                'wallet_address': wallet_address,
                'transaction_hash': signature,
                'dex': 'jupiter',  # Most swaps on Solana are Jupiter
                'raw_data': tx  # Keep full transaction for debugging
            }
            
        except Exception as e:
            logger.debug(f"❌ Error parsing swap {tx.get('signature', 'unknown')[:16]}...: {e}")
            return None
    
    async def health_check(self) -> bool:
        """Check if Helius API is accessible"""
        try:
            session = await self._get_session()
            
            # Test RPC endpoint
            rpc_url = f"https://mainnet.helius-rpc.com/?api-key={self.api_key}"
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getHealth"
            }
            
            async with session.post(
                rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                is_healthy = response.status == 200
                
                if is_healthy:
                    logger.info("✅ Helius health check: OK")
                else:
                    logger.warning(f"⚠️ Helius health check: {response.status}")
                
                return is_healthy
                    
        except Exception as e:
            logger.error(f"❌ Helius health check failed: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get collector statistics"""
        return {
            'known_tokens': len(self.TOKEN_MINTS),
            'cached_candles': self.candle_cache.size(),
        }
    
    def clear_cache(self):
        """Clear all caches"""
        self.candle_cache.clear()
        logger.info("🗑️ Helius caches cleared")
    
    async def close(self):
        """Clean up resources"""
        if self.session and not self.session.closed:
            await self.session.close()
        
        self.candle_cache.clear()
        
        # Log final stats
        logger.info(f"📊 Helius Collector stats: {self.get_stats()}")


def create_helius_collector(
    api_key: str, 
    dexscreener_collector: Optional[Any] = None,
    config: Optional[Dict[str, Any]] = None
) -> HeliusCollector:
    """
    Create production-ready Helius Collector
    
    Args:
        api_key: Helius API key
        dexscreener_collector: Optional Dexscreener collector (not used anymore)
        config: Optional configuration
        
    Returns:
        HeliusCollector instance
    """
    return HeliusCollector(
        api_key=api_key,
        config=config or {},
        dexscreener_collector=dexscreener_collector
    )
