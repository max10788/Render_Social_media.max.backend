# workers/transaction_worker.py
import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, DatabaseException
from app.core.backend_crypto_tracker.processor.blockchain_parser import BlockchainParser
from app.core.backend_crypto_tracker.processor.database.models.transaction import Transaction
from app.core.backend_crypto_tracker.processor.database.manager import DatabaseManager

logger = get_logger(__name__)

class TransactionWorker:
    def __init__(self, db_manager: DatabaseManager, config: Dict[str, Any]):
        self.db_manager = db_manager
        self.config = config
        self.is_running = False
        self.parsers = {
            'ethereum': None,
            'bsc': None,
            'solana': None,
            'sui': None
        }
        self.last_processed_blocks = {
            'ethereum': 0,
            'bsc': 0,
            'solana': 0,
            'sui': 0
        }
        self.stats = {
            'total_transactions_processed': 0,
            'transactions_per_chain': {
                'ethereum': 0,
                'bsc': 0,
                'solana': 0,
                'sui': 0
            },
            'last_run_time': None
        }
    
    async def initialize(self):
        """Initialisiert die Blockchain-Parser"""
        try:
            # EVM Parser für Ethereum und BSC
            self.parsers['ethereum'] = BlockchainParser(
                rpc_url=self.config.get('ethereum_rpc'),
                chain='ethereum'
            )
            
            self.parsers['bsc'] = BlockchainParser(
                rpc_url=self.config.get('bsc_rpc'),
                chain='bsc'
            )
            
            # API-Clients für verschiedene Blockchains
            # Alte Initialisierung (ersetzen):
            # self.etherscan_api = EtherscanAPI(self.config.get('etherscan_api_key'))
            # self.bscscan_api = BscScanAPI(self.config.get('bscscan_api_key'))
            # self.solana_api = SolanaAPI(self.config.get('solana_rpc'))
            # self.sui_api = SuiAPI(self.config.get('sui_rpc'))
            
            # Neue Initialisierung (hinzufügen):
            self.ethereum_provider = EthereumProvider(self.config.get('etherscan_api_key'))
            self.bsc_provider = EthereumProvider(self.config.get('bscscan_api_key'))  # BSC verwendet auch EthereumProvider
            self.solana_provider = SolanaProvider(self.config.get('solana_rpc'))
            self.sui_provider = SuiProvider(self.config.get('sui_rpc'))
            
            # Lade den zuletzt verarbeiteten Block aus der Datenbank
            await self._load_last_processed_blocks()
            
            logger.info("Transaction worker initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing transaction worker: {e}")
            raise
    
    async def _load_last_processed_blocks(self):
        """Lädt den zuletzt verarbeiteten Block für jede Blockchain"""
        try:
            for chain in self.last_processed_blocks.keys():
                last_block = await self.db_manager.get_last_processed_block(chain)
                if last_block:
                    self.last_processed_blocks[chain] = last_block
                else:
                    # Aktuellen Block als Startpunkt verwenden
                    current_block = await self._get_current_block(chain)
                    self.last_processed_blocks[chain] = current_block
        except Exception as e:
            logger.error(f"Error loading last processed blocks: {e}")
    
    async def _get_current_block(self, chain: str) -> int:
        """Holt die aktuelle Blocknummer für eine Blockchain"""
        try:
            if chain in ['ethereum', 'bsc']:
                # Alte Methode: api = self.etherscan_api if chain == 'ethereum' else self.bscscan_api
                # Neue Methode:
                api = self.ethereum_provider if chain == 'ethereum' else self.bsc_provider
                async with api:
                    # Proxy-Methode, um die aktuelle Blocknummer zu erhalten
                    # Dies könnte über einen RPC-Aufruf effizienter sein
                    return await self.parsers[chain].get_latest_block_number()
            elif chain == 'solana':
                # Alte Methode: return await self.solana_api.get_latest_slot()
                # Neue Methode:
                return await self.solana_provider.get_latest_slot()
            elif chain == 'sui':
                # Alte Methode: return await self.sui_api.get_latest_checkpoint()
                # Neue Methode:
                return await self.sui_provider.get_latest_checkpoint()
            else:
                logger.warning(f"Unsupported chain: {chain}")
                return 0
        except Exception as e:
            logger.error(f"Error getting current block for {chain}: {e}")
            return 0
    
    async def start(self):
        """Startet den Transaction Worker"""
        if self.is_running:
            logger.warning("Transaction worker is already running")
            return
        
        self.is_running = True
        logger.info("Starting transaction worker")
        
        while self.is_running:
            try:
                await self._process_all_chains()
                self.stats['last_run_time'] = datetime.now()
                
                # Wartezeit zwischen den Durchläufen
                await asyncio.sleep(60)  # 1 Minute
            except Exception as e:
                logger.error(f"Error in transaction worker: {e}")
                await asyncio.sleep(30)  # Kürzere Wartezeit bei Fehlern
    
    async def stop(self):
        """Stoppt den Transaction Worker"""
        self.is_running = False
        logger.info("Stopping transaction worker")
    
    async def _process_all_chains(self):
        """Verarbeitet Transaktionen für alle unterstützten Blockchains"""
        tasks = []
        
        for chain in self.parsers.keys():
            if self.parsers[chain]:
                tasks.append(self._process_chain(chain))
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _process_chain(self, chain: str):
        """Verarbeitet Transaktionen für eine bestimmte Blockchain"""
        try:
            logger.debug(f"Processing transactions for {chain}")
            
            # Aktuellen Block holen
            current_block = await self._get_current_block(chain)
            
            if current_block <= self.last_processed_blocks[chain]:
                logger.debug(f"No new blocks for {chain}")
                return
            
            # Neue Transaktionen abrufen
            transactions = await self._fetch_new_transactions(
                chain, 
                self.last_processed_blocks[chain] + 1,
                current_block
            )
            
            if transactions:
                # Transaktionen verarbeiten und speichern
                processed_count = await self._process_and_save_transactions(chain, transactions)
                logger.info(f"Processed {processed_count} transactions for {chain}")
                
                # Statistiken aktualisieren
                self.stats['total_transactions_processed'] += processed_count
                self.stats['transactions_per_chain'][chain] += processed_count
            
            # Letzten verarbeiteten Block aktualisieren
            self.last_processed_blocks[chain] = current_block
            await self.db_manager.save_last_processed_block(chain, current_block)
            
        except Exception as e:
            logger.error(f"Error processing {chain} transactions: {e}")
    
    async def _fetch_new_transactions(self, chain: str, start_block: int, end_block: int) -> List[Dict]:
        """Holt neue Transaktionen für einen Blockbereich"""
        try:
            if chain in ['ethereum', 'bsc']:
                # Für EVM-Blockchains verwenden wir den Blockchain-Parser
                return await self.parsers[chain].get_transactions_in_block_range(start_block, end_block)
            elif chain == 'solana':
                # Alte Methode: return await self.solana_api.get_transactions_in_slot_range(start_block, end_block)
                # Neue Methode:
                return await self.solana_provider.get_transactions_in_slot_range(start_block, end_block)
            elif chain == 'sui':
                # Alte Methode: return await self.sui_api.get_transactions_in_checkpoint_range(start_block, end_block)
                # Neue Methode:
                return await self.sui_provider.get_transactions_in_checkpoint_range(start_block, end_block)
            else:
                logger.warning(f"Unsupported chain: {chain}")
                return []
        except Exception as e:
            logger.error(f"Error fetching transactions for {chain}: {e}")
            return []
    
    async def _process_and_save_transactions(self, chain: str, transactions: List[Dict]) -> int:
        """Verarbeitet und speichert Transaktionen"""
        processed_count = 0
        
        for tx_data in transactions:
            try:
                # Transaktion verarbeiten
                transaction = await self.parsers[chain].parse_transaction(tx_data)
                
                if transaction:
                    # In der Datenbank speichern
                    await self.db_manager.save_transaction(transaction)
                    processed_count += 1
                    
                    # Zusätzliche Verarbeitung für Token-Transaktionen
                    if transaction.is_token_transfer:
                        await self._process_token_transaction(chain, transaction)
            except Exception as e:
                logger.error(f"Error processing transaction: {e}")
        
        return processed_count
    
    async def _process_token_transaction(self, chain: str, transaction: Transaction):
        """Verarbeitet Token-spezifische Transaktionen"""
        try:
            # Hier könnten zusätzliche Analysen für Token-Transaktionen durchgeführt werden
            # Zum Beispiel: Aktualisierung von Token-Holdern, Volumen-Statistiken, etc.
            pass
        except Exception as e:
            logger.error(f"Error processing token transaction: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Gibt Statistiken des Transaction Workers zurück"""
        return {
            'is_running': self.is_running,
            'last_processed_blocks': self.last_processed_blocks.copy(),
            'stats': self.stats.copy(),
            'last_run_time': self.stats['last_run_time'].isoformat() if self.stats['last_run_time'] else None
        }
