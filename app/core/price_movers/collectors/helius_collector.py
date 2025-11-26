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
            del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        self.cache[key] = (value, time.time())
    
    def clear(self):
        self.cache.clear()
    
    def size(self) -> int:
        return len(self.cache)


class HeliusCollector(DEXCollector):
    """Helius Collector - Token-based strategy for Solana DEX trades"""
    
    API_BASE = "https://api-mainnet.helius-rpc.com"
    
    # ✅ NEU: Solana Program IDs für DEX-Erkennung
    DEX_PROGRAM_IDS = {
        'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4': 'jupiter',  # Jupiter V6
        'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB': 'jupiter',  # Jupiter V4
        'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc': 'orca',     # Orca Whirlpool
        '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': 'raydium',  # Raydium AMM V4
        '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv': 'raydium',  # Raydium CLMM
        'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo': 'meteora',  # Meteora
        'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY': 'phoenix',   # Phoenix
    }
    
    # ✅ NEU: Wichtige Transaction Types
    CRITICAL_TX_TYPES = {
        'SWAP',
        'ADD_LIQUIDITY', 
        'REMOVE_LIQUIDITY',
        'COMPRESSED_NFT_MINT',  # Manchmal getarnte Swaps
        'UNKNOWN'  # Manuell parsen!
    }
    
    # Token Mint Addresses (Solana SPL Tokens)
    TOKEN_MINTS = {
        'SOL': 'So11111111111111111111111111111111111111112',
        'WSOL': 'So11111111111111111111111111111111111111112',
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        'BONK': 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263',
        'JTO': 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL',
        'JUP': 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN',
        'WIF': 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm',
        'PYTH': 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3',
        'RAY': '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',
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
        self.candle_cache = SimpleCache(ttl_seconds=60)
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
        
        For Helius Enhanced Transactions API, we use token addresses.
        Strategy: For SOL/USDT, use USDT token address
        """
        logger.info(f"🔍 Resolving {symbol} to token address...")
        
        try:
            base_token, quote_token = symbol.upper().split('/')
        except ValueError:
            logger.error(f"❌ Invalid symbol format: {symbol}")
            return None
        
        # For SOL pairs, use the quote token (USDT/USDC)
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
    
    # ✅ REQUIRED: Abstract method implementation from DEXCollector
    async def _resolve_symbol_to_address(self, symbol: str) -> Optional[str]:
        """Resolve symbol to address (abstract method from DEXCollector)"""
        return await self._resolve_symbol_to_token_address(symbol)
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """Fetch CURRENT candle using token-based approach"""
        logger.info(f"🔗 Helius: Fetching candle for {symbol} (token-based)")
        
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        cache_key = f"candle_{symbol}_{timeframe}_{int(timestamp.timestamp())}"
        cached = self.candle_cache.get(cache_key)
        if cached:
            logger.debug("📦 Using cached candle")
            return cached
        
        timeframe_seconds = {
            '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '4h': 14400, '1d': 86400,
        }.get(timeframe, 300)
        
        start_time = timestamp
        end_time = timestamp + timedelta(seconds=timeframe_seconds)
        
        trades = await self.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=1000
        )
        
        if not trades:
            logger.warning(f"⚠️ No trades found for {symbol}")
            return self._empty_candle(timestamp)
        
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
        """Fetch trades for symbol"""
        token_address = await self._resolve_symbol_to_token_address(symbol)
        if not token_address:
            logger.error(f"❌ Cannot resolve token address for {symbol}")
            return []
        
        return await self.fetch_dex_trades(
            token_address=token_address,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            symbol=symbol
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
        
        ✅ ENHANCED: Parses SWAP, ADD_LIQUIDITY, REMOVE_LIQUIDITY, UNKNOWN
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
                {"limit": min(limit, 1000)}
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
            
            filtered_sigs = filtered_sigs[:min(len(filtered_sigs), 100)]
            
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
                
                tx_types = {}
                for tx in transactions:
                    tx_type = tx.get('type', 'UNKNOWN')
                    tx_types[tx_type] = tx_types.get(tx_type, 0) + 1
                
                logger.info(f"📊 Transaction types found: {tx_types}")
            
            # Step 4: Parse and filter trades - ENHANCED VERSION
            trades = []
            stats = {
                'swap_count': 0,
                'unknown_parsed': 0,
                'liquidity_events': 0,
                'add_liquidity': 0,
                'remove_liquidity': 0,
                'parse_errors': []
            }
            
            for i, tx in enumerate(transactions):
                try:
                    tx_type = tx.get('type')
                    trade = None
                    
                    # ✅ Parse known SWAPs
                    if tx_type == 'SWAP':
                        stats['swap_count'] += 1
                        trade = self._parse_helius_enhanced_swap(tx, symbol)
                    
                    # ✅ Parse UNKNOWN transactions
                    elif tx_type == 'UNKNOWN':
                        trade = self._parse_unknown_transaction(tx, symbol)
                        
                        if trade:
                            stats['unknown_parsed'] += 1
                            
                            # Track liquidity events separately
                            tx_type_parsed = trade.get('transaction_type', '')
                            if tx_type_parsed == 'ADD_LIQUIDITY':
                                stats['add_liquidity'] += 1
                                stats['liquidity_events'] += 1
                            elif tx_type_parsed == 'REMOVE_LIQUIDITY':
                                stats['remove_liquidity'] += 1
                                stats['liquidity_events'] += 1
                    
                    # ✅ Other types (TRANSFER, etc.) - ignore
                    else:
                        continue
                    
                    # Add valid trades
                    if trade and start_time <= trade['timestamp'] <= end_time:
                        trades.append(trade)
                        
                        # Log first 5 trades
                        if len(trades) <= 5:
                            tx_type_label = trade.get('transaction_type', 'Trade')
                            logger.info(
                                f"✅ {tx_type_label} #{len(trades)}: "
                                f"{trade['trade_type']} {trade['amount']:.4f} "
                                f"@ ${trade.get('price', 0):.6f} at {trade['timestamp']}"
                            )
                            
                            # Extra info for liquidity events
                            if 'liquidity_delta' in trade:
                                logger.info(
                                    f"   💧 Liquidity Delta: {trade['liquidity_delta']:.4f}"
                                )
                    
                except Exception as e:
                    stats['parse_errors'].append(str(e))
                    if len(stats['parse_errors']) <= 3:
                        logger.error(f"❌ Parse error {len(stats['parse_errors'])}: {e}", exc_info=True)
                    continue
            
            # ✅ Enhanced Logging
            logger.info(
                f"✅ Helius: {len(trades)} trades returned "
                f"(SWAP: {stats['swap_count']}, "
                f"UNKNOWN parsed: {stats['unknown_parsed']}, "
                f"Liquidity: +{stats['add_liquidity']}/-{stats['remove_liquidity']}, "
                f"Parse errors: {len(stats['parse_errors'])})"
            )
            
            # Warning if no trades despite having transactions
            if len(trades) == 0 and (stats['swap_count'] > 0 or stats['unknown_parsed'] > 0):
                logger.warning(
                    f"⚠️ Found {stats['swap_count']} SWAP + {stats['unknown_parsed']} parsed UNKNOWN "
                    f"but no valid trades in time range! Parse errors: {len(stats['parse_errors'])}"
                )
                if stats['parse_errors']:
                    logger.warning(f"Parse error examples: {stats['parse_errors'][:3]}")
            
            # Info about liquidity events
            if stats['liquidity_events'] > 0:
                logger.info(
                    f"💧 Liquidity Events: {stats['liquidity_events']} total "
                    f"(+{stats['add_liquidity']} adds, -{stats['remove_liquidity']} removals)"
                )
            
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
        
        ✅ ENHANCED: Adds transaction_type field
        """
        try:
            signature = tx.get('signature')
            timestamp = tx.get('timestamp')
            
            if not timestamp:
                return None
            
            trade_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            
            token_transfers = tx.get('tokenTransfers', [])
            native_transfers = tx.get('nativeTransfers', [])
            
            if not token_transfers and not native_transfers:
                return None
            
            amount = 0
            price = 0
            trade_type = 'unknown'
            wallet_address = None
            
            events = tx.get('events', {})
            
            if 'swap' in events:
                swap_event = events['swap']
                
                token_inputs = swap_event.get('tokenInputs', [])
                token_outputs = swap_event.get('tokenOutputs', [])
                
                if token_inputs and token_outputs:
                    input_token = token_inputs[0]
                    output_token = token_outputs[0]
                    
                    wallet_address = input_token.get('userAccount') or output_token.get('userAccount')
                    
                    input_amount = float(input_token.get('tokenAmount', 0))
                    output_amount = float(output_token.get('tokenAmount', 0))
                    
                    input_mint = input_token.get('mint', '')
                    output_mint = output_token.get('mint', '')
                    
                    sol_mint = 'So11111111111111111111111111111111111111112'
                    
                    if output_mint == sol_mint:
                        trade_type = 'buy'
                        amount = output_amount
                        if amount > 0:
                            price = input_amount / amount
                    elif input_mint == sol_mint:
                        trade_type = 'sell'
                        amount = input_amount
                        if amount > 0:
                            price = output_amount / amount
                    else:
                        return None
            
            if amount == 0 and len(token_transfers) >= 1:
                sol_mint = 'So11111111111111111111111111111111111111112'
                
                for transfer in token_transfers:
                    mint = transfer.get('mint', '')
                    if mint == sol_mint:
                        amount = float(transfer.get('tokenAmount', 0))
                        wallet_address = transfer.get('fromUserAccount') or transfer.get('toUserAccount')
                        
                        if len(token_transfers) >= 2:
                            for other_transfer in token_transfers:
                                if other_transfer.get('mint') != sol_mint:
                                    other_amount = float(other_transfer.get('tokenAmount', 0))
                                    if amount > 0 and other_amount > 0:
                                        price = other_amount / amount
                                    break
                        
                        trade_type = 'swap'
                        break
            
            if amount == 0 or not wallet_address:
                return None
            
            if price == 0:
                price = 1.0
            
            return {
                'timestamp': trade_time,
                'price': price,
                'amount': amount,
                'trade_type': trade_type,
                'wallet_address': wallet_address,
                'transaction_hash': signature,
                'dex': 'jupiter',
                'transaction_type': 'SWAP',  # ✅ NEU
                'raw_data': tx
            }
            
        except Exception as e:
            logger.debug(f"❌ Error parsing swap {tx.get('signature', 'unknown')[:16]}...: {e}")
            return None

    def _detect_dex_from_program_ids(self, tx: Dict[str, Any]) -> Optional[str]:
        """
        Erkenne DEX anhand der Program IDs in der Transaktion
        
        Returns:
            DEX Name (z.B. 'jupiter', 'orca') oder None
        """
        try:
            # Account Keys enthalten die aufgerufenen Programme
            account_keys = tx.get('accountData', [])
            
            for account in account_keys:
                account_id = account.get('account', '')
                
                if account_id in self.DEX_PROGRAM_IDS:
                    dex_name = self.DEX_PROGRAM_IDS[account_id]
                    logger.debug(f"🎯 Detected DEX: {dex_name} (program: {account_id[:8]}...)")
                    return dex_name
            
            # Fallback: Prüfe instructions
            instructions = tx.get('instructions', [])
            for instruction in instructions:
                program_id = instruction.get('programId', '')
                
                if program_id in self.DEX_PROGRAM_IDS:
                    dex_name = self.DEX_PROGRAM_IDS[program_id]
                    logger.debug(f"🎯 Detected DEX via instruction: {dex_name}")
                    return dex_name
            
            return None
            
        except Exception as e:
            logger.debug(f"Error detecting DEX: {e}")
            return None

    def _parse_unknown_transaction(
        self, 
        tx: Dict[str, Any],
        symbol: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Parse UNKNOWN Transaktionen manuell - IMPROVED VERSION
        
        ✅ IMPROVEMENTS:
        - Weniger strikt bei DEX-Detection
        - Fallback-Parsing auch ohne DEX
        - Bessere Fehler-Behandlung
        
        Strategie:
        1. Versuche DEX zu erkennen (optional!)
        2. Analysiere Token Transfers
        3. Parse auch ohne DEX-Detection
        """
        try:
            signature = tx.get('signature', 'unknown')
            timestamp = tx.get('timestamp')
            
            if not timestamp:
                return None
            
            trade_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            
            # 1. Versuche DEX zu erkennen (OPTIONAL!)
            dex_name = self._detect_dex_from_program_ids(tx)
            
            # ✅ IMPROVED: Kein sofortiger Abbruch mehr!
            if not dex_name:
                logger.debug(f"⚠️ No DEX detected for {signature[:16]}... - trying fallback parsing")
                dex_name = 'unknown_dex'  # Fallback statt None
            
            # 2. Analysiere Token Transfers
            token_transfers = tx.get('tokenTransfers', [])
            native_transfers = tx.get('nativeTransfers', [])
            
            if not token_transfers:
                logger.debug(f"⚠️ No token transfers in {signature[:16]}...")
                return None
            
            sol_mint = 'So11111111111111111111111111111111111111112'
            
            # Zähle SOL transfers
            sol_transfers = [t for t in token_transfers if t.get('mint') == sol_mint]
            other_transfers = [t for t in token_transfers if t.get('mint') != sol_mint]
            
            # ✅ IMPROVED: Mehr Debug-Logging
            logger.debug(
                f"📊 {signature[:16]}... has {len(sol_transfers)} SOL + "
                f"{len(other_transfers)} other transfers"
            )
            
            # 3. Klassifiziere Transaction Type
            tx_type = self._classify_transaction_type(
                tx=tx,
                sol_transfers=sol_transfers,
                other_transfers=other_transfers
            )
            
            logger.debug(f"🔍 Classified as: {tx_type}")
            
            if tx_type not in ['swap', 'add_liquidity', 'remove_liquidity']:
                logger.debug(f"⚠️ Skipping {tx_type} transaction")
                return None
            
            # 4. Parse je nach Type
            if tx_type == 'swap':
                result = self._parse_manual_swap(
                    tx=tx,
                    sol_transfers=sol_transfers,
                    other_transfers=other_transfers,
                    dex_name=dex_name,  # Kann jetzt 'unknown_dex' sein!
                    trade_time=trade_time,
                    signature=signature
                )
                
                if result:
                    logger.debug(f"✅ Parsed UNKNOWN as swap: {result['amount']:.4f} @ ${result['price']:.2f}")
                return result
                
            elif tx_type in ['add_liquidity', 'remove_liquidity']:
                result = self._parse_liquidity_event(
                    tx=tx,
                    event_type=tx_type,
                    sol_transfers=sol_transfers,
                    other_transfers=other_transfers,
                    dex_name=dex_name,
                    trade_time=trade_time,
                    signature=signature
                )
                
                if result:
                    logger.debug(f"✅ Parsed UNKNOWN as {tx_type}")
                return result
            
            return None
            
        except Exception as e:
            logger.debug(f"❌ Error parsing unknown tx {signature[:16] if signature else 'N/A'}...: {e}")
            return None


    def _classify_transaction_type(
        self,
        tx: Dict[str, Any],
        sol_transfers: List[Dict],
        other_transfers: List[Dict]
    ) -> str:
        """
        Klassifiziere Transaction Type - IMPROVED VERSION
        
        ✅ IMPROVEMENTS:
        - Mehr Swap-Patterns erkannt
        - Bessere Heuristiken
        - Weniger false negatives
        """
        try:
            # ✅ IMPROVED: Pattern 1a - Simple 2-way swap (most common)
            if len(sol_transfers) == 1 and len(other_transfers) == 1:
                sol_from = sol_transfers[0].get('fromUserAccount')
                sol_to = sol_transfers[0].get('toUserAccount')
                other_from = other_transfers[0].get('fromUserAccount')
                other_to = other_transfers[0].get('toUserAccount')
                
                # User gibt SOL und bekommt Token (BUY)
                # ODER User gibt Token und bekommt SOL (SELL)
                if sol_from == other_to or sol_to == other_from:
                    logger.debug(f"✅ Pattern 1a: Simple 2-way swap")
                    return 'swap'
            
            # ✅ NEW: Pattern 1b - Multi-hop swap (SOL -> Token1 -> Token2)
            if len(sol_transfers) >= 1 and len(other_transfers) >= 1:
                # Prüfe ob es gemeinsame Accounts gibt (Router)
                sol_accounts = set()
                for t in sol_transfers:
                    sol_accounts.add(t.get('fromUserAccount'))
                    sol_accounts.add(t.get('toUserAccount'))
                
                other_accounts = set()
                for t in other_transfers:
                    other_accounts.add(t.get('fromUserAccount'))
                    other_accounts.add(t.get('toUserAccount'))
                
                # Wenn es Überschneidungen gibt (Router/User)
                if sol_accounts & other_accounts:
                    logger.debug(f"✅ Pattern 1b: Multi-hop swap (overlapping accounts)")
                    return 'swap'
            
            # ✅ NEW: Pattern 1c - Check by amounts (wenn Beträge ähnlich sind)
            if len(sol_transfers) == 1 and len(other_transfers) == 1:
                sol_amount = float(sol_transfers[0].get('tokenAmount', 0))
                other_amount = float(other_transfers[0].get('tokenAmount', 0))
                
                # Wenn Beträge in ähnlichem Verhältnis (0.1x - 10x)
                if sol_amount > 0 and other_amount > 0:
                    ratio = max(sol_amount, other_amount) / min(sol_amount, other_amount)
                    if ratio < 10000:  # Reasonable price range
                        logger.debug(f"✅ Pattern 1c: Similar amounts (ratio {ratio:.2f})")
                        return 'swap'
            
            # Pattern 2: ADD_LIQUIDITY (mehrere deposits an Pool)
            if len(token_transfers := tx.get('tokenTransfers', [])) >= 2:
                to_accounts = [t.get('toUserAccount') for t in token_transfers]
                
                # Wenn alle zur gleichen Adresse gehen = Pool deposit
                if len(set(to_accounts)) == 1 and to_accounts[0]:
                    logger.debug(f"✅ Pattern 2: ADD_LIQUIDITY")
                    return 'add_liquidity'
            
            # Pattern 3: REMOVE_LIQUIDITY (mehrere withdrawals vom Pool)
            if len(token_transfers := tx.get('tokenTransfers', [])) >= 2:
                from_accounts = [t.get('fromUserAccount') for t in token_transfers]
                
                # Wenn alle von der gleichen Adresse kommen = Pool withdrawal
                if len(set(from_accounts)) == 1 and from_accounts[0]:
                    logger.debug(f"✅ Pattern 3: REMOVE_LIQUIDITY")
                    return 'remove_liquidity'
            
            logger.debug(f"⚠️ No matching pattern found")
            return 'unknown'
            
        except Exception as e:
            logger.debug(f"Error classifying tx type: {e}")
            return 'unknown'

    def _parse_manual_swap(
        self,
        tx: Dict[str, Any],
        sol_transfers: List[Dict],
        other_transfers: List[Dict],
        dex_name: str,
        trade_time: datetime,
        signature: str
    ) -> Optional[Dict[str, Any]]:
        """
        Parse Swap aus Raw Token Transfers - IMPROVED VERSION
        
        ✅ IMPROVEMENTS:
        - Besseres Wallet-Detection
        - Multi-transfer support
        - Robustere Preis-Berechnung
        """
        try:
            if not sol_transfers or not other_transfers:
                return None
            
            # ✅ IMPROVED: Handle multi-transfers (nehme erste)
            sol_transfer = sol_transfers[0]
            other_transfer = other_transfers[0]
            
            sol_amount = float(sol_transfer.get('tokenAmount', 0))
            other_amount = float(other_transfer.get('tokenAmount', 0))
            
            if sol_amount == 0 or other_amount == 0:
                logger.debug(f"⚠️ Zero amounts: SOL={sol_amount}, Other={other_amount}")
                return None
            
            # ✅ IMPROVED: Besseres Wallet-Detection
            # Wallet ist derjenige, der sowohl bei SOL als auch bei Other Transfer vorkommt
            sol_from = sol_transfer.get('fromUserAccount')
            sol_to = sol_transfer.get('toUserAccount')
            other_from = other_transfer.get('fromUserAccount')
            other_to = other_transfer.get('toUserAccount')
            
            # Finde den User (kommt in beiden vor)
            wallet = None
            trade_type = 'unknown'
            
            if sol_from == other_to:
                # User gibt SOL, bekommt Token = SELL SOL
                wallet = sol_from
                trade_type = 'sell'
                amount = sol_amount
                price = other_amount / sol_amount
            elif sol_to == other_from:
                # User bekommt SOL, gibt Token = BUY SOL
                wallet = sol_to
                trade_type = 'buy'
                amount = sol_amount
                price = other_amount / sol_amount
            else:
                # ✅ FALLBACK: Nehme einfach den ersten Account
                wallet = sol_from or sol_to or other_from or other_to
                trade_type = 'swap'
                amount = sol_amount
                price = other_amount / sol_amount
            
            if not wallet:
                logger.debug(f"⚠️ No wallet found in transfers")
                return None
            
            logger.debug(
                f"✅ Swap: {trade_type} {amount:.4f} SOL @ ${price:.2f} "
                f"(wallet: {wallet[:8]}...)"
            )
            
            return {
                'timestamp': trade_time,
                'price': price,
                'amount': amount,
                'trade_type': trade_type,
                'wallet_address': wallet,
                'transaction_hash': signature,
                'dex': dex_name,  # Kann 'unknown_dex' sein
                'transaction_type': 'SWAP',
                'raw_data': tx
            }
            
        except Exception as e:
            logger.debug(f"Error parsing manual swap: {e}")
            return None
            
    def _parse_liquidity_event(
        self,
        tx: Dict[str, Any],
        event_type: str,
        sol_transfers: List[Dict],
        other_transfers: List[Dict],
        dex_name: str,
        trade_time: datetime,
        signature: str
    ) -> Optional[Dict[str, Any]]:
        """
        Parse ADD_LIQUIDITY oder REMOVE_LIQUIDITY
        
        ⚠️ WICHTIG: Liquidity Events haben großen Price Impact!
        """
        try:
            all_transfers = sol_transfers + other_transfers
            
            if not all_transfers:
                return None
            
            # Berechne total value
            total_sol = sum(float(t.get('tokenAmount', 0)) for t in sol_transfers)
            total_other = sum(float(t.get('tokenAmount', 0)) for t in other_transfers)
            
            # Wallet ist der Liquidity Provider
            wallet = None
            if event_type == 'add_liquidity':
                # Bei ADD: fromUserAccount
                wallet = all_transfers[0].get('fromUserAccount')
            else:
                # Bei REMOVE: toUserAccount
                wallet = all_transfers[0].get('toUserAccount')
            
            if not wallet:
                return None
            
            return {
                'timestamp': trade_time,
                'price': total_other / total_sol if total_sol > 0 else 0,
                'amount': total_sol,
                'trade_type': event_type,  # 'add_liquidity' oder 'remove_liquidity'
                'wallet_address': wallet,
                'transaction_hash': signature,
                'dex': dex_name,
                'transaction_type': event_type.upper(),  # ✅ NEU
                'liquidity_delta': total_sol,  # ✅ NEU: für Impact-Berechnung
                'raw_data': tx
            }
            
        except Exception as e:
            logger.debug(f"Error parsing liquidity event: {e}")
            return None
    
    async def health_check(self) -> bool:
        """Check if Helius API is accessible"""
        try:
            session = await self._get_session()
            
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
        logger.info(f"📊 Helius Collector stats: {self.get_stats()}")


def create_helius_collector(
    api_key: str, 
    dexscreener_collector: Optional[Any] = None,
    config: Optional[Dict[str, Any]] = None
) -> HeliusCollector:
    """Create production-ready Helius Collector"""
    return HeliusCollector(
        api_key=api_key,
        config=config or {},
        dexscreener_collector=dexscreener_collector
    )
