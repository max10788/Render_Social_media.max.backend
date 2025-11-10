"""
DEX Collector - Abstract Base Class für DEX Data Collection

Basis für alle DEX-spezifischen Collectors:
- Birdeye (Solana)
- Helius (Solana)
- The Graph (Ethereum/Uniswap)
- DexScreener (Multi-chain)
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional

from .base import BaseCollector
from ..utils.constants import BlockchainNetwork, DEX_CONFIGS
from ..utils.blockchain_utils import validate_wallet_address


logger = logging.getLogger(__name__)


class DEXCollector(BaseCollector):
    """
    Abstract Base Class für DEX Collectors
    
    Erweitert BaseCollector mit DEX-spezifischen Features:
    - ECHTE Wallet-Adressen ✅
    - On-chain Transaction Parsing
    - DEX-spezifische Metadaten
    """
    
    def __init__(
        self,
        dex_name: str,
        blockchain: BlockchainNetwork,
        api_key: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialisiert DEX Collector
        
        Args:
            dex_name: Name des DEX (jupiter/raydium/uniswap)
            blockchain: Blockchain Network (solana/ethereum/bsc)
            api_key: API Key für Service (falls benötigt)
            config: Zusätzliche Konfiguration
        """
        super().__init__(config)
        
        self.dex_name = dex_name.lower()
        self.blockchain = blockchain
        self.api_key = api_key
        
        # DEX Config laden
        self.dex_config = DEX_CONFIGS.get(self.dex_name, {})
        
        if not self.dex_config:
            logger.warning(f"Keine Config für DEX '{dex_name}' gefunden")
        
        logger.info(
            f"DEX Collector initialisiert: {dex_name.upper()} "
            f"(Blockchain: {blockchain.value})"
        )
    
    @abstractmethod
    async def fetch_dex_trades(
        self,
        token_address: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetcht DEX-Trades mit ECHTEN Wallet-Adressen
        
        WICHTIG: Im Gegensatz zu CEX liefert DEX:
        - wallet_address (ECHT! 🎯)
        - signature/tx_hash
        - dex (Jupiter/Raydium/etc.)
        
        Args:
            token_address: Token Contract Address / Mint
            start_time: Start-Zeitpunkt
            end_time: End-Zeitpunkt
            limit: Max. Anzahl Trades
            
        Returns:
            Liste von Trades mit Format:
            {
                'id': str,
                'wallet_address': str,  # ← ECHTE Wallet! 🎯
                'timestamp': datetime,
                'trade_type': 'buy' oder 'sell',
                'amount': float,
                'price': float,
                'value_usd': float,
                'dex': str,
                'signature': str,
                'blockchain': str
            }
        """
        pass
    
    def validate_trade(self, trade: Dict[str, Any]) -> bool:
        """
        Validiert einen Trade
        
        Args:
            trade: Trade Dictionary
            
        Returns:
            True wenn valide
        """
        required_fields = [
            'wallet_address',
            'timestamp',
            'trade_type',
            'amount',
            'price'
        ]
        
        # Prüfe Required Fields
        for field in required_fields:
            if field not in trade:
                logger.warning(f"Trade fehlt Field: {field}")
                return False
        
        # Validiere Wallet-Adresse
        wallet_address = trade.get('wallet_address')
        
        if not wallet_address:
            logger.warning("Trade hat keine Wallet-Adresse")
            return False
        
        try:
            is_valid = validate_wallet_address(wallet_address, self.blockchain)
            
            if not is_valid:
                logger.warning(
                    f"Ungültige Wallet-Adresse: {wallet_address} "
                    f"({self.blockchain})"
                )
                return False
        except Exception as e:
            logger.error(f"Wallet-Validierung fehlgeschlagen: {e}")
            return False
        
        # Validiere Numeric Fields
        try:
            assert float(trade['amount']) >= 0
            assert float(trade['price']) >= 0
        except (ValueError, AssertionError):
            logger.warning("Trade hat ungültige numerische Werte")
            return False
        
        return True
    
    def _add_blockchain_metadata(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fügt Blockchain-Metadaten zu Trade hinzu
        
        Args:
            trade: Trade Dictionary
            
        Returns:
            Trade mit Metadaten
        """
        from ..utils.blockchain_utils import (
            get_wallet_explorer_url,
            get_transaction_explorer_url,
            shorten_address
        )
        
        wallet_address = trade.get('wallet_address')
        signature = trade.get('signature')
        
        # Füge Explorer URLs hinzu
        trade['wallet_explorer_url'] = (
            get_wallet_explorer_url(wallet_address, self.blockchain)
            if wallet_address else None
        )
        
        trade['tx_explorer_url'] = (
            get_transaction_explorer_url(signature, self.blockchain)
            if signature else None
        )
        
        # Füge Shortened Address hinzu (für UI)
        trade['wallet_address_short'] = (
            shorten_address(wallet_address)
            if wallet_address else None
        )
        
        # Füge Blockchain hinzu
        trade['blockchain'] = self.blockchain.value
        trade['dex'] = self.dex_name
        
        return trade
    
    async def fetch_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """
        Implementation von BaseCollector.fetch_trades()
        
        Wrapper um fetch_dex_trades() mit Symbol → Token Address Mapping
        
        Args:
            symbol: Trading Pair (z.B. SOL/USDC)
            start_time: Start
            end_time: Ende
            limit: Max Trades
            
        Returns:
            Liste von Trades
        """
        # Parse Symbol zu Token Address
        # z.B. SOL/USDC → Token Mint Address
        token_address = await self._resolve_symbol_to_address(symbol)
        
        if not token_address:
            logger.error(f"Konnte Token-Adresse nicht auflösen für: {symbol}")
            return []
        
        # Fetch DEX Trades
        trades = await self.fetch_dex_trades(
            token_address=token_address,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        # Validiere und enriche Trades
        valid_trades = []
        
        for trade in trades:
            if self.validate_trade(trade):
                # Füge Metadata hinzu
                enriched_trade = self._add_blockchain_metadata(trade)
                valid_trades.append(enriched_trade)
        
        logger.info(
            f"✓ {len(valid_trades)}/{len(trades)} valide DEX Trades gefetcht "
            f"(DEX: {self.dex_name})"
        )
        
        return valid_trades
    
    @abstractmethod
    async def _resolve_symbol_to_address(self, symbol: str) -> Optional[str]:
        """
        Resolved Trading Pair Symbol zu Token Address
        
        z.B. SOL/USDC → EPjFWdd5AufqSSqeM2qN1xzYbApSqN1MaPqQb (USDC Mint)
        
        Args:
            symbol: Trading Pair (z.B. BTC/USDT)
            
        Returns:
            Token Address/Mint
        """
        pass
    
    async def fetch_candle_data(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Implementation von BaseCollector.fetch_candle_data()
        
        Für DEX: Entweder via API oder aus Trades aggregiert
        
        Args:
            symbol: Trading Pair
            timeframe: Timeframe
            timestamp: Zeitpunkt
            
        Returns:
            Candle Dictionary
        """
        # Standardmäßig: Aggregiere aus Trades
        # Kann in Sub-Klassen überschrieben werden wenn API verfügbar
        
        logger.warning(
            f"DEX Collector {self.dex_name} nutzt Trade-Aggregation für Candles"
        )
        
        # Fetch Trades im Timeframe
        from ..utils.constants import TIMEFRAME_TO_MS
        from datetime import timedelta
        
        timeframe_seconds = TIMEFRAME_TO_MS.get(timeframe, 60000) / 1000
        
        trades = await self.fetch_trades(
            symbol=symbol,
            start_time=timestamp,
            end_time=timestamp + timedelta(seconds=timeframe_seconds),
            limit=10000
        )
        
        if not trades:
            raise ValueError(f"Keine Trades verfügbar für Candle-Berechnung")
        
        # Aggregiere zu Candle
        return self._aggregate_trades_to_candle(trades, timestamp)
    
    def _aggregate_trades_to_candle(
        self,
        trades: List[Dict[str, Any]],
        timestamp: datetime
    ) -> Dict[str, Any]:
        """
        Aggregiert Trades zu einer OHLCV Candle
        
        Args:
            trades: Liste von Trades
            timestamp: Candle-Zeitpunkt
            
        Returns:
            Candle Dictionary
        """
        if not trades:
            raise ValueError("Keine Trades zum Aggregieren")
        
        prices = [t['price'] for t in trades]
        volumes = [t['amount'] for t in trades]
        
        return {
            'timestamp': timestamp,
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes),
        }
    
    def __str__(self) -> str:
        return f"DEXCollector({self.dex_name.upper()}/{self.blockchain.value})"
    
    def __repr__(self) -> str:
        return self.__str__()
