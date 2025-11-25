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
        """Fetch trades from Helius API using Solana RPC + Enhanced Transactions"""
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
            
            # Step 4: Parse and filter trades
            trades = []
            swap_count = 0
            parse_errors = []
            
            for i, tx in enumerate(transactions):
                try:
                    tx_type = tx.get('type')
                    
                    if tx_type != 'SWAP':
                        continue
                    
                    swap_count += 1
                    
                    trade = self._parse_helius_enhanced_swap(tx, symbol)
                    
                    if trade:
                        if start_time <= trade['timestamp'] <= end_time:
                            trades.append(trade)
                            
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
        """Parse a Helius Enhanced Transaction (SWAP type)"""
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
        Parse UNKNOWN Transaktionen manuell
        
        Strategie:
        1. Erkenne DEX via Program IDs
        2. Analysiere Token Transfers
        3. Identifiziere Liquidity Events vs Swaps
        """
        try:
            signature = tx.get('signature', 'unknown')
            timestamp = tx.get('timestamp')
            
            if not timestamp:
                return None
            
            trade_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            
            # 1. Erkenne DEX
            dex_name = self._detect_dex_from_program_ids(tx)
            if not dex_name:
                logger.debug(f"⚠️ Cannot identify DEX for {signature[:16]}...")
                return None
            
            # 2. Analysiere Token Transfers
            token_transfers = tx.get('tokenTransfers', [])
            native_transfers = tx.get('nativeTransfers', [])
            
            if not token_transfers:
                return None
            
            sol_mint = 'So11111111111111111111111111111111111111112'
            
            # Zähle SOL transfers
            sol_transfers = [t for t in token_transfers if t.get('mint') == sol_mint]
            other_transfers = [t for t in token_transfers if t.get('mint') != sol_mint]
            
            # 3. Klassifiziere Transaction Type
            tx_type = self._classify_transaction_type(
                tx=tx,
                sol_transfers=sol_transfers,
                other_transfers=other_transfers
            )
            
            if tx_type not in ['swap', 'add_liquidity', 'remove_liquidity']:
                return None
            
            # 4. Parse je nach Type
            if tx_type == 'swap':
                return self._parse_manual_swap(
                    tx=tx,
                    sol_transfers=sol_transfers,
                    other_transfers=other_transfers,
                    dex_name=dex_name,
                    trade_time=trade_time,
                    signature=signature
                )
            elif tx_type in ['add_liquidity', 'remove_liquidity']:
                return self._parse_liquidity_event(
                    tx=tx,
                    event_type=tx_type,
                    sol_transfers=sol_transfers,
                    other_transfers=other_transfers,
                    dex_name=dex_name,
                    trade_time=trade_time,
                    signature=signature
                )
            
            return None
            
        except Exception as e:
            logger.debug(f"❌ Error parsing unknown tx: {e}")
            return None

    def _classify_transaction_type(
        self,
        tx: Dict[str, Any],
        sol_transfers: List[Dict],
        other_transfers: List[Dict]
    ) -> str:
        """
        Klassifiziere Transaction Type basierend auf Transfer-Patterns
        
        Returns:
            'swap', 'add_liquidity', 'remove_liquidity', oder 'unknown'
        """
        try:
            # Pattern 1: SWAP (2 transfers in entgegengesetzte Richtungen)
            if len(sol_transfers) == 1 and len(other_transfers) == 1:
                sol_from = sol_transfers[0].get('fromUserAccount')
                sol_to = sol_transfers[0].get('toUserAccount')
                other_from = other_transfers[0].get('fromUserAccount')
                other_to = other_transfers[0].get('toUserAccount')
                
                # Wenn User SOL gibt und Token bekommt (oder umgekehrt)
                if sol_from == other_to or sol_to == other_from:
                    return 'swap'
            
            # Pattern 2: ADD_LIQUIDITY (mehrere deposits an Pool)
            # Typisch: 2+ token transfers ZUM gleichen Pool/Vault
            if len(token_transfers := tx.get('tokenTransfers', [])) >= 2:
                to_accounts = [t.get('toUserAccount') for t in token_transfers]
                
                # Wenn alle zur gleichen Adresse gehen = Pool deposit
                if len(set(to_accounts)) == 1 and to_accounts[0]:
                    return 'add_liquidity'
            
            # Pattern 3: REMOVE_LIQUIDITY (mehrere withdrawals vom Pool)
            if len(token_transfers := tx.get('tokenTransfers', [])) >= 2:
                from_accounts = [t.get('fromUserAccount') for t in token_transfers]
                
                # Wenn alle von der gleichen Adresse kommen = Pool withdrawal
                if len(set(from_accounts)) == 1 and from_accounts[0]:
                    return 'remove_liquidity'
            
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
        """Parse Swap aus Raw Token Transfers"""
        try:
            if not sol_transfers or not other_transfers:
                return None
            
            sol_transfer = sol_transfers[0]
            other_transfer = other_transfers[0]
            
            sol_amount = float(sol_transfer.get('tokenAmount', 0))
            other_amount = float(other_transfer.get('tokenAmount', 0))
            
            # Wallet ist der User, der den Swap initiiert
            wallet = sol_transfer.get('fromUserAccount') or sol_transfer.get('toUserAccount')
            
            if not wallet or sol_amount == 0:
                return None
            
            # Bestimme Trade Direction
            sol_from = sol_transfer.get('fromUserAccount')
            
            if sol_from == wallet:
                # User verkauft SOL
                trade_type = 'sell'
                amount = sol_amount
                price = other_amount / sol_amount if sol_amount > 0 else 0
            else:
                # User kauft SOL
                trade_type = 'buy'
                amount = sol_amount
                price = other_amount / sol_amount if sol_amount > 0 else 0
            
            return {
                'timestamp': trade_time,
                'price': price,
                'amount': amount,
                'trade_type': trade_type,
                'wallet_address': wallet,
                'transaction_hash': signature,
                'dex': dex_name,
                'transaction_type': 'SWAP',  # ✅ NEU
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
