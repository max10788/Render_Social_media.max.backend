"""
Helius Collector - Solana DEX Data via Helius Enhanced APIs

✅ VORTEILE gegenüber Birdeye:
- 100,000 requests/day FREE (Birdeye: paid only)
- Parse Transaction History API
- Enhanced Transaction API
- Webhook support
- Bessere Dokumentation

API Docs: https://docs.helius.dev/welcome/what-is-helius
"""

import logging
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from .dex_collector import DEXCollector
from ..utils.constants import BlockchainNetwork


logger = logging.getLogger(__name__)


class HeliusCollector(DEXCollector):
    """
    Helius Collector für Solana DEX Daten
    
    Nutzt:
    - Enhanced Transactions API
    - Parse Transaction History
    - Token Transactions API
    """
    
    # Helius API Endpoints
    API_BASE = "https://api-mainnet.helius-rpc.com"
    
    # Solana DEX Program IDs
    DEX_PROGRAMS = {
        'jupiter': 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
        'raydium': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
        'orca': 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
    }
    
    # Common Solana token addresses
    TOKEN_MINTS = {
        'SOL': 'So11111111111111111111111111111111111111112',  # Wrapped SOL
        'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        'RAY': '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R',
        'SRM': 'SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt',
    }
    
    def __init__(self, api_key: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Helius Collector
        
        Args:
            api_key: Helius API Key (free: 100k req/day)
            config: Optional configuration
        """
        super().__init__(
            dex_name="helius",
            blockchain=BlockchainNetwork.SOLANA,
            api_key=api_key,
            config=config or {}
        )
        
        self.session: Optional[aiohttp.ClientSession] = None
        
        logger.info("✅ Helius Collector initialisiert (Enhanced Solana APIs)")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def fetch_dex_trades(
        self,
        token_address: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetcht DEX Trades von Helius API
        
        Implementation der abstrakten Methode aus DEXCollector
        
        Args:
            token_address: Solana Token Mint Address
            start_time: Start-Zeitpunkt
            end_time: End-Zeitpunkt
            limit: Max. Anzahl Trades
            
        Returns:
            Liste von Trades mit ECHTEN Wallet-Adressen
        """
        logger.info(f"🔗 Helius: Fetching DEX trades for token {token_address[:8]}...")
        
        # Ensure timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        try:
            # Use Enhanced Transaction History API
            trades = await self._fetch_enhanced_transactions(
                token_mint=token_address,
                start_time=start_time,
                end_time=end_time,
                limit=limit
            )
            
            logger.info(f"✅ Helius: {len(trades)} DEX trades fetched")
            
            return trades
            
        except Exception as e:
            logger.error(f"❌ Helius fetch_dex_trades error: {e}", exc_info=True)
            return []
    
    async def _resolve_symbol_to_address(self, symbol: str) -> Optional[str]:
        """
        Resolved Trading Pair Symbol zu Token Mint Address
        
        Implementation der abstrakten Methode aus DEXCollector
        
        Args:
            symbol: Trading Pair (z.B. SOL/USDC)
            
        Returns:
            Token Mint Address
        """
        try:
            # Parse symbol (e.g., "SOL/USDC" -> "SOL")
            parts = symbol.split('/')
            
            if len(parts) != 2:
                logger.error(f"Ungültiges Symbol-Format: {symbol}")
                return None
            
            base_token = parts[0].upper()
            
            # Lookup in bekannten Tokens
            token_address = self.TOKEN_MINTS.get(base_token)
            
            if not token_address:
                logger.warning(
                    f"Token '{base_token}' nicht in bekannten Tokens. "
                    f"Nutze SOL als Fallback..."
                )
                token_address = self.TOKEN_MINTS['SOL']
            
            logger.debug(f"Resolved {symbol} -> {token_address}")
            
            return token_address
            
        except Exception as e:
            logger.error(f"Symbol resolution error: {e}", exc_info=True)
            return None
    
    async def _fetch_enhanced_transactions(
        self,
        token_mint: str,
        start_time: datetime,
        end_time: datetime,
        limit: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch transactions via Helius Enhanced Transactions API
        
        Uses: /v0/addresses/{address}/transactions
        """
        session = await self._get_session()
        
        url = f"{self.API_BASE}/v0/addresses/{token_mint}/transactions"
        
        params = {
            'api-key': self.api_key,
            'limit': min(limit, 100),  # Max 100 per request
            'before': int(end_time.timestamp()),
            'until': int(start_time.timestamp()),
            'type': 'SWAP',  # Filter for swaps only
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Helius API error: {response.status} - {error_text}")
                    return []
                
                data = await response.json()
            
            # Parse transactions
            trades = []
            
            for tx in data:
                try:
                    trade = self._parse_helius_transaction(tx)
                    if trade:
                        trades.append(trade)
                except Exception as e:
                    logger.warning(f"Failed to parse transaction: {e}")
                    continue
            
            return trades
            
        except aiohttp.ClientError as e:
            logger.error(f"Helius Enhanced Transactions error: {e}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Unexpected error in _fetch_enhanced_transactions: {e}", exc_info=True)
            return []
    
    def _parse_helius_transaction(self, tx: Dict) -> Optional[Dict[str, Any]]:
        """
        Parse Helius transaction to trade format
        
        Args:
            tx: Raw Helius transaction
            
        Returns:
            Parsed trade dict
        """
        try:
            # Extract key fields
            timestamp = datetime.fromtimestamp(
                tx.get('timestamp', 0),
                tz=timezone.utc
            )
            
            # Get account keys (wallet addresses)
            accounts = tx.get('accountData', [])
            wallet_address = accounts[0].get('account') if accounts else None
            
            # Get token transfers
            token_transfers = tx.get('tokenTransfers', [])
            
            if not token_transfers:
                return None
            
            # Parse first transfer (simplification)
            transfer = token_transfers[0]
            
            # Determine buy/sell
            from_user_account = transfer.get('fromUserAccount')
            to_user_account = transfer.get('toUserAccount')
            
            if from_user_account:
                trade_type = 'sell'
                wallet = from_user_account
            elif to_user_account:
                trade_type = 'buy'
                wallet = to_user_account
            else:
                wallet = wallet_address
                trade_type = 'buy'  # Default
            
            # Fallback if no wallet found
            if not wallet:
                logger.warning("Transaction ohne Wallet-Adresse gefunden")
                return None
            
            # Amount and price
            amount = float(transfer.get('tokenAmount', 0))
            
            # Get price from native transfers (SOL)
            native_transfers = tx.get('nativeTransfers', [])
            price = 0.0
            value_usd = 0.0
            
            if native_transfers:
                sol_amount = sum(float(t.get('amount', 0)) for t in native_transfers) / 1e9  # Lamports to SOL
                if amount > 0:
                    price = sol_amount / amount
                value_usd = sol_amount * 210  # Rough SOL price, TODO: get real price
            
            # Determine DEX from transaction
            dex = self._identify_dex(tx)
            
            trade = {
                'id': tx.get('signature', ''),
                'timestamp': timestamp,
                'trade_type': trade_type,
                'amount': amount,
                'price': price,
                'value_usd': value_usd,
                'wallet_address': wallet,
                'dex': dex,
                'signature': tx.get('signature'),
                'blockchain': 'solana',
            }
            
            return trade
            
        except Exception as e:
            logger.warning(f"Parse transaction error: {e}")
            return None
    
    def _identify_dex(self, tx: Dict) -> str:
        """
        Identify which DEX was used in transaction
        
        Args:
            tx: Transaction data
            
        Returns:
            DEX name (jupiter/raydium/orca/unknown)
        """
        try:
            # Check instructions for DEX program IDs
            instructions = tx.get('instructions', [])
            
            for instruction in instructions:
                program_id = instruction.get('programId', '')
                
                for dex_name, dex_program_id in self.DEX_PROGRAMS.items():
                    if program_id == dex_program_id:
                        return dex_name
            
            # Default to Jupiter (most common on Solana)
            return 'jupiter'
            
        except Exception:
            return 'unknown'
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Fetch candle data by aggregating trades
        
        Helius doesn't have direct OHLCV API, so we aggregate trades
        """
        logger.info(f"🔗 Helius: Aggregating candle for {symbol}")
        
        # Ensure timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        # Determine timeframe duration
        timeframe_map = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400,
        }
        
        duration = timeframe_map.get(timeframe, 300)  # Default 5m
        
        start_time = timestamp
        end_time = timestamp + timedelta(seconds=duration)
        
        # Fetch trades in timeframe
        trades = await self.fetch_trades(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=1000
        )
        
        if not trades:
            # Return mock candle if no trades
            logger.warning("Keine Trades für Candle-Aggregation verfügbar, nutze Mock-Daten")
            return {
                'timestamp': timestamp,
                'open': 210.0,
                'high': 211.0,
                'low': 209.0,
                'close': 210.5,
                'volume': 0.0
            }
        
        # Aggregate to OHLCV
        prices = [t['price'] for t in trades if t.get('price', 0) > 0]
        volumes = [t['amount'] for t in trades if t.get('amount', 0) > 0]
        
        if not prices:
            logger.warning("Keine gültigen Preise in Trades, nutze Mock-Daten")
            return {
                'timestamp': timestamp,
                'open': 210.0,
                'high': 211.0,
                'low': 209.0,
                'close': 210.5,
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
        
        logger.info(f"✅ Helius Candle aggregated: {len(trades)} trades")
        
        return candle
    
    async def health_check(self) -> bool:
        """Check Helius API health"""
        try:
            session = await self._get_session()
            
            # Simple RPC call to check connectivity
            url = f"{self.API_BASE}/v0/addresses/So11111111111111111111111111111111111111112/transactions"
            
            params = {
                'api-key': self.api_key,
                'limit': 1
            }
            
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
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
        """Close aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("✅ Helius Collector closed")


# Factory function
def create_helius_collector(api_key: str) -> HeliusCollector:
    """
    Create Helius Collector
    
    Args:
        api_key: Helius API Key
        
    Returns:
        HeliusCollector instance
    """
    return HeliusCollector(api_key=api_key)
