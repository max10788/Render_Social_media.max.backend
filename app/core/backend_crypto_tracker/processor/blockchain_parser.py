# processor/blockchain_parser.py
import logging
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, BlockchainException
from app.core.backend_crypto_tracker.blockchain import EthereumProvider, SolanaProvider, SuiProvider
from app.core.backend_crypto_tracker.processor.database.models.transaction import Transaction
from app.core.backend_crypto_tracker.processor.database.models.token import Token

logger = get_logger(__name__)

@dataclass
class ParsedTokenData:
    address: str
    name: str
    symbol: str
    chain: str
    decimals: int
    total_supply: Optional[float] = None
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    liquidity: Optional[float] = None
    holders_count: Optional[int] = None
    contract_verified: Optional[bool] = None
    creation_date: Optional[str] = None

@dataclass
class ParsedWalletData:
    address: str
    chain: str
    balance: float
    transactions_count: Optional[int] = None
    first_transaction: Optional[str] = None
    last_transaction: Optional[str] = None

class BlockchainParser:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Alte Initialisierung (ersetzen):
        # self.ethereum_api = None
        # self.bsc_api = None
        # self.solana_api = None
        # self.sui_api = None
        
        # Neue Initialisierung (hinzufügen):
        self.ethereum_provider = None
        self.bsc_provider = None
        self.solana_provider = None
        self.sui_provider = None
    
    # Alte Methode (ersetzen):
    # async def initialize(self):
    #     """Initialisiert die Blockchain-APIs"""
    #     try:
    #         self.ethereum_api = EtherscanAPI(self.config.get('etherscan_api_key'))
    #         self.bsc_api = BscScanAPI(self.config.get('bscscan_api_key'))
    #         self.solana_api = SolanaAPIService(
    #             self.config.get('solana_rpc'),
    #             self.config.get('helius_api_key')
    #         )
    #         self.sui_api = SuiAPIService(self.config.get('sui_rpc'))
            
    #         logger.info("Blockchain parser initialized successfully")
    #     except Exception as e:
    #         logger.error(f"Error initializing blockchain parser: {e}")
    #         raise

    # Neue Methode (hinzufügen):
    async def initialize(self):
        """Initialisiert die Blockchain-APIs"""
        try:
            self.ethereum_provider = EthereumProvider(self.config.get('etherscan_api_key'))
            self.bsc_provider = EthereumProvider(self.config.get('bscscan_api_key'))  # BSC verwendet auch EthereumProvider
            self.solana_provider = SolanaProvider(
                self.config.get('solana_rpc'),
                self.config.get('helius_api_key')
            )
            self.sui_provider = SuiProvider(self.config.get('sui_rpc'))
            
            logger.info("Blockchain parser initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing blockchain parser: {e}")
            raise
    
    async def parse_token(self, chain: str, address: str) -> Optional[ParsedTokenData]:
        """Parst Token-Daten für eine bestimmte Blockchain"""
        try:
            if chain.lower() == 'ethereum':
                return await self._parse_ethereum_token(address)
            elif chain.lower() == 'bsc':
                return await self._parse_bsc_token(address)
            elif chain.lower() == 'solana':
                return await self._parse_solana_token(address)
            elif chain.lower() == 'sui':
                return await self._parse_sui_token(address)
            else:
                raise BlockchainException(f"Unsupported chain: {chain}")
        except Exception as e:
            logger.error(f"Error parsing token for {chain}: {e}")
            raise
    
    async def _parse_ethereum_token(self, address: str) -> Optional[ParsedTokenData]:
        """Parst Ethereum-Token-Daten"""
        try:
            # Alte Methode: async with self.ethereum_api:
            # Neue Methode:
            async with self.ethereum_provider:
                # Token-Informationen abrufen
                # Alte Methode: token_info = await self.ethereum_api.get_token_info(address)
                # Neue Methode:
                token_info = await self.ethereum_provider.get_token_info(address)
                
                if not token_info:
                    return None
                
                # Token-Holder abrufen
                # Alte Methode: holders = await self.ethereum_api.get_token_holders(address, limit=10)
                # Neue Methode:
                holders = await self.ethereum_provider.get_token_holders(address, limit=10)
                
                return ParsedTokenData(
                    address=address,
                    name=token_info.get('tokenName', ''),
                    symbol=token_info.get('symbol', ''),
                    chain='ethereum',
                    decimals=int(token_info.get('divisor', '0')),
                    holders_count=len(holders),
                    contract_verified=token_info.get('verified_contract', False)
                )
        except Exception as e:
            logger.error(f"Error parsing Ethereum token: {e}")
            raise APIException(f"Error parsing Ethereum token: {str(e)}")
    
    async def _parse_bsc_token(self, address: str) -> Optional[ParsedTokenData]:
        """Parst BSC-Token-Daten"""
        try:
            # Alte Methode: async with self.bsc_api:
            # Neue Methode:
            async with self.bsc_provider:
                # Token-Informationen abrufen
                # Alte Methode: token_info = await self.bsc_api.get_token_info(address)
                # Neue Methode:
                token_info = await self.bsc_provider.get_token_info(address)
                
                if not token_info:
                    return None
                
                # Token-Holder abrufen
                # Alte Methode: holders = await self.bsc_api.get_token_holders(address, limit=10)
                # Neue Methode:
                holders = await self.bsc_provider.get_token_holders(address, limit=10)
                
                return ParsedTokenData(
                    address=address,
                    name=token_info.get('tokenName', ''),
                    symbol=token_info.get('symbol', ''),
                    chain='bsc',
                    decimals=int(token_info.get('divisor', '0')),
                    holders_count=len(holders),
                    contract_verified=token_info.get('verified_contract', False)
                )
        except Exception as e:
            logger.error(f"Error parsing BSC token: {e}")
            raise APIException(f"Error parsing BSC token: {str(e)}")
    
    async def _parse_solana_token(self, address: str) -> Optional[ParsedTokenData]:
        """Parst Solana-Token-Daten"""
        try:
            # Alte Methode: async with self.solana_api:
            # Neue Methode:
            async with self.solana_provider:
                # Token-Metadaten abrufen
                # Alte Methode: metadata = await self.solana_api.get_token_metadata(address)
                # Neue Methode:
                metadata = await self.solana_provider.get_token_metadata(address)
                
                if not metadata:
                    return None
                
                # Token-Supply abrufen
                # Alte Methode: supply = await self.solana_api.get_token_supply(address)
                # Neue Methode:
                supply = await self.solana_provider.get_token_supply(address)
                
                # Token-Holder abrufen
                # Alte Methode: holders = await self.solana_api.get_token_holders(address, limit=10)
                # Neue Methode:
                holders = await self.solana_provider.get_token_holders(address, limit=10)
                
                # Extrahiere Token-Informationen aus den Daten
                parsed_data = metadata.get('data', {}).get('parsed', {}).get('info', {})
                
                return ParsedTokenData(
                    address=address,
                    name=parsed_data.get('name', ''),
                    symbol=parsed_data.get('symbol', ''),
                    chain='solana',
                    decimals=int(parsed_data.get('decimals', 0)),
                    total_supply=float(supply.get('amount', 0)) / (10 ** int(parsed_data.get('decimals', 0))) if supply else None,
                    holders_count=len(holders)
                )
        except Exception as e:
            logger.error(f"Error parsing Solana token: {e}")
            raise APIException(f"Error parsing Solana token: {str(e)}")
    
    async def _parse_sui_token(self, address: str) -> Optional[ParsedTokenData]:
        """Parst Sui-Token-Daten"""
        try:
            # Alte Methode: async with self.sui_api:
            # Neue Methode:
            async with self.sui_provider:
                # Coin-Metadaten abrufen
                # Alte Methode: metadata = await self.sui_api.get_coin_metadata(address)
                # Neue Methode:
                metadata = await self.sui_provider.get_coin_metadata(address)
                
                if not metadata:
                    return None
                
                # Total Supply abrufen
                # Alte Methode: total_supply = await self.sui_api.get_total_supply(address)
                # Neue Methode:
                total_supply = await self.sui_provider.get_total_supply(address)
                
                # Token-Holder abrufen
                # Alte Methode: holders = await self.sui_api.get_token_holders(address, limit=10)
                # Neue Methode:
                holders = await self.sui_provider.get_token_holders(address, limit=10)
                
                return ParsedTokenData(
                    address=address,
                    name=metadata.get('name', ''),
                    symbol=metadata.get('symbol', ''),
                    chain='sui',
                    decimals=int(metadata.get('decimals', 0)),
                    total_supply=float(total_supply) if total_supply else None,
                    holders_count=len(holders)
                )
        except Exception as e:
            logger.error(f"Error parsing Sui token: {e}")
            raise APIException(f"Error parsing Sui token: {str(e)}")
    
    async def parse_wallet(self, chain: str, address: str) -> Optional[ParsedWalletData]:
        """Parst Wallet-Daten für eine bestimmte Blockchain"""
        try:
            if chain.lower() == 'ethereum':
                return await self._parse_ethereum_wallet(address)
            elif chain.lower() == 'bsc':
                return await self._parse_bsc_wallet(address)
            elif chain.lower() == 'solana':
                return await self._parse_solana_wallet(address)
            elif chain.lower() == 'sui':
                return await self._parse_sui_wallet(address)
            else:
                raise BlockchainException(f"Unsupported chain: {chain}")
        except Exception as e:
            logger.error(f"Error parsing wallet for {chain}: {e}")
            raise
    
    async def _parse_ethereum_wallet(self, address: str) -> Optional[ParsedWalletData]:
        """Parst Ethereum-Wallet-Daten"""
        try:
            # Alte Methode: async with self.ethereum_api:
            # Neue Methode:
            async with self.ethereum_provider:
                # Transaktionen abrufen
                # Alte Methode: transactions = await self.ethereum_api.get_transactions_by_address(address, limit=1)
                # Neue Methode:
                transactions = await self.ethereum_provider.get_transactions_by_address(address, limit=1)
                
                if not transactions:
                    return ParsedWalletData(
                        address=address,
                        chain='ethereum',
                        balance=0,
                        transactions_count=0
                    )
                
                # Balance abrufen (dies würde normalerweise über einen RPC-Aufruf erfolgen)
                # Hier vereinfacht
                balance = 0  # Placeholder
                
                return ParsedWalletData(
                    address=address,
                    chain='ethereum',
                    balance=balance,
                    transactions_count=len(transactions)
                )
        except Exception as e:
            logger.error(f"Error parsing Ethereum wallet: {e}")
            raise APIException(f"Error parsing Ethereum wallet: {str(e)}")
    
    async def _parse_bsc_wallet(self, address: str) -> Optional[ParsedWalletData]:
        """Parst BSC-Wallet-Daten"""
        try:
            # Alte Methode: async with self.bsc_api:
            # Neue Methode:
            async with self.bsc_provider:
                # Transaktionen abrufen
                # Alte Methode: transactions = await self.bsc_api.get_transactions_by_address(address, limit=1)
                # Neue Methode:
                transactions = await self.bsc_provider.get_transactions_by_address(address, limit=1)
                
                if not transactions:
                    return ParsedWalletData(
                        address=address,
                        chain='bsc',
                        balance=0,
                        transactions_count=0
                    )
                
                # Balance abrufen (dies würde normalerweise über einen RPC-Aufruf erfolgen)
                # Hier vereinfacht
                balance = 0  # Placeholder
                
                return ParsedWalletData(
                    address=address,
                    chain='bsc',
                    balance=balance,
                    transactions_count=len(transactions)
                )
        except Exception as e:
            logger.error(f"Error parsing BSC wallet: {e}")
            raise APIException(f"Error parsing BSC wallet: {str(e)}")
    
    async def _parse_solana_wallet(self, address: str) -> Optional[ParsedWalletData]:
        """Parst Solana-Wallet-Daten"""
        try:
            # Alte Methode: async with self.solana_api:
            # Neue Methode:
            async with self.solana_provider:
                # Account-Informationen abrufen
                # Alte Methode: account_info = await self.solana_api.get_account_info(address)
                # Neue Methode:
                account_info = await self.solana_provider.get_account_info(address)
                
                if not account_info:
                    return ParsedWalletData(
                        address=address,
                        chain='solana',
                        balance=0,
                        transactions_count=0
                    )
                
                # Balance aus den Account-Daten extrahieren
                lamports = account_info.get('lamports', 0)
                balance = lamports / 1_000_000_000  # SOL hat 9 Dezimalstellen
                
                # Transaktionen abrufen
                # Alte Methode: signatures = await self.solana_api.get_signatures_for_address(address, limit=1)
                # Neue Methode:
                signatures = await self.solana_provider.get_signatures_for_address(address, limit=1)
                
                return ParsedWalletData(
                    address=address,
                    chain='solana',
                    balance=balance,
                    transactions_count=len(signatures)
                )
        except Exception as e:
            logger.error(f"Error parsing Solana wallet: {e}")
            raise APIException(f"Error parsing Solana wallet: {str(e)}")
    
    async def _parse_sui_wallet(self, address: str) -> Optional[ParsedWalletData]:
        """Parst Sui-Wallet-Daten"""
        try:
            # Alte Methode: async with self.sui_api:
            # Neue Methode:
            async with self.sui_provider:
                # Balance abrufen
                # Alte Methode: balance = await self.sui_api.get_balance(address)
                # Neue Methode:
                balance = await self.sui_provider.get_balance(address)
                
                if balance is None:
                    return ParsedWalletData(
                        address=address,
                        chain='sui',
                        balance=0,
                        transactions_count=0
                    )
                
                # Alle Balances abrufen, um die Anzahl der Transaktionen zu schätzen
                # Alte Methode: all_balances = await self.sui_api.get_all_balances(address)
                # Neue Methode:
                all_balances = await self.sui_provider.get_all_balances(address)
                
                return ParsedWalletData(
                    address=address,
                    chain='sui',
                    balance=balance / 1_000_000_000,  # Sui hat 9 Dezimalstellen
                    transactions_count=len(all_balances)
                )
        except Exception as e:
            logger.error(f"Error parsing Sui wallet: {e}")
            raise APIException(f"Error parsing Sui wallet: {str(e)}")
    
    async def get_latest_block_number(self, chain: str) -> int:
        """Holt die neueste Blocknummer für eine Blockchain"""
        try:
            if chain.lower() == 'ethereum':
                # Dies würde normalerweise über einen RPC-Aufruf erfolgen
                # Hier vereinfacht
                return 0  # Placeholder
            elif chain.lower() == 'bsc':
                # Dies würde normalerweise über einen RPC-Aufruf erfolgen
                # Hier vereinfacht
                return 0  # Placeholder
            elif chain.lower() == 'solana':
                # Alte Methode: async with self.solana_api:
                # Neue Methode:
                async with self.solana_provider:
                    # Alte Methode: return await self.solana_api.get_latest_slot()
                    # Neue Methode:
                    return await self.solana_provider.get_latest_slot()
            elif chain.lower() == 'sui':
                # Alte Methode: async with self.sui_api:
                # Neue Methode:
                async with self.sui_provider:
                    # Alte Methode: return await self.sui_api.get_latest_checkpoint_sequence_number()
                    # Neue Methode:
                    return await self.sui_provider.get_latest_checkpoint_sequence_number()
            else:
                raise BlockchainException(f"Unsupported chain: {chain}")
        except Exception as e:
            logger.error(f"Error getting latest block number for {chain}: {e}")
            raise APIException(f"Error getting latest block number: {str(e)}")
    
    async def get_transactions_in_block_range(self, chain: str, start: int, end: int) -> List[Dict[str, Any]]:
        """Holt Transaktionen in einem Blockbereich für eine Blockchain"""
        try:
            if chain.lower() == 'ethereum':
                # Dies würde normalerweise über einen RPC-Aufruf erfolgen
                # Hier vereinfacht
                return []  # Placeholder
            elif chain.lower() == 'bsc':
                # Dies würde normalerweise über einen RPC-Aufruf erfolgen
                # Hier vereinfacht
                return []  # Placeholder
            elif chain.lower() == 'solana':
                # Alte Methode: async with self.solana_api:
                # Neue Methode:
                async with self.solana_provider:
                    # Alte Methode: return await self.solana_api.get_transactions_in_slot_range(start, end)
                    # Neue Methode:
                    return await self.solana_provider.get_transactions_in_slot_range(start, end)
            elif chain.lower() == 'sui':
                # Alte Methode: async with self.sui_api:
                # Neue Methode:
                async with self.sui_provider:
                    # Alte Methode: return await self.sui_api.get_transactions_in_checkpoint_range(start, end)
                    # Neue Methode:
                    return await self.sui_provider.get_transactions_in_checkpoint_range(start, end)
            else:
                raise BlockchainException(f"Unsupported chain: {chain}")
        except Exception as e:
            logger.error(f"Error getting transactions in block range for {chain}: {e}")
            raise APIException(f"Error getting transactions in block range: {str(e)}")
    
    async def parse_transaction(self, chain: str, tx_data: Dict[str, Any]) -> Optional[Transaction]:
        """Parst eine Transaktion für eine bestimmte Blockchain"""
        try:
            if chain.lower() == 'ethereum':
                return await self._parse_ethereum_transaction(tx_data)
            elif chain.lower() == 'bsc':
                return await self._parse_bsc_transaction(tx_data)
            elif chain.lower() == 'solana':
                return await self._parse_solana_transaction(tx_data)
            elif chain.lower() == 'sui':
                return await self._parse_sui_transaction(tx_data)
            else:
                raise BlockchainException(f"Unsupported chain: {chain}")
        except Exception as e:
            logger.error(f"Error parsing transaction for {chain}: {e}")
            raise
    
    async def _parse_ethereum_transaction(self, tx_data: Dict[str, Any]) -> Optional[Transaction]:
        """Parst eine Ethereum-Transaktion"""
        # Dies würde eine detaillierte Implementierung erfordern
        # Hier vereinfacht
        return None  # Placeholder
    
    async def _parse_bsc_transaction(self, tx_data: Dict[str, Any]) -> Optional[Transaction]:
        """Parst eine BSC-Transaktion"""
        # Dies würde eine detaillierte Implementierung erfordern
        # Hier vereinfacht
        return None  # Placeholder
    
    async def _parse_solana_transaction(self, tx_data: Dict[str, Any]) -> Optional[Transaction]:
        """Parst eine Solana-Transaktion"""
        # Dies würde eine detaillierte Implementierung erfordern
        # Hier vereinfacht
        return None  # Placeholder
    
    async def _parse_sui_transaction(self, tx_data: Dict[str, Any]) -> Optional[Transaction]:
        """Parst eine Sui-Transaktion"""
        # Dies würde eine detaillierte Implementierung erfordern
        # Hier vereinfacht
        return None  # Placeholder
