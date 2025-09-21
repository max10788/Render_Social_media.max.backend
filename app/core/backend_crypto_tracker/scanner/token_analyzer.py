# scanner/token_analyzer.py
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
import time  # FEHLENDES IMPORT
import random  # FEHLENDES IMPORT
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from web3 import Web3
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, InvalidAddressException
from app.core.backend_crypto_tracker.config.scanner_config import scanner_config
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum_provider import EthereumProvider
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana_provider import SolanaProvider
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui_provider import SuiProvider
# Import all providers
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider

from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko_provider import CoinGeckoProvider
from app.core.backend_crypto_tracker.blockchain.aggregators.coinmarketcap_provider import CoinMarketCapProvider
from app.core.backend_crypto_tracker.blockchain.aggregators.cryptocompare_provider import CryptoCompareProvider

from app.core.backend_crypto_tracker.blockchain.exchanges.bitget_provider import BitgetProvider
from app.core.backend_crypto_tracker.blockchain.exchanges.kraken_provider import KrakenProvider
from app.core.backend_crypto_tracker.blockchain.exchanges.binance_provider import BinanceProvider
from app.core.backend_crypto_tracker.blockchain.exchanges.coinbase_provider import CoinbaseProvider

from app.core.backend_crypto_tracker.blockchain.onchain.bitquery_provider import BitqueryProvider
from app.core.backend_crypto_tracker.processor.database.models.token import Token
from app.core.backend_crypto_tracker.processor.database.models.wallet import WalletAnalysis, WalletTypeEnum

logger = get_logger(__name__)

@dataclass
class TokenAnalysisConfig:
    max_tokens_per_scan: int = 100
    max_market_cap: float = 5_000_000
    min_liquidity_threshold: float = 50_000
    whale_threshold_percentage: float = 5.0
    dev_threshold_percentage: float = 2.0
    sniper_time_threshold_seconds: int = 300
    rugpull_sell_threshold_percentage: float = 50.0
    max_holders_to_analyze: int = 100
    request_delay_seconds: float = 1.0

def retry_with_backoff(max_retries=3, base_delay=1, max_delay=60):
    """Decorator für Retry mit exponentiellem Backoff"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    retries += 1
                    
                    # Bei Rate-Limit-Fehlern länger warten
                    if "Rate limit exceeded" in str(e) or "429" in str(e):
                        delay = min(base_delay * (2 ** (retries - 1)) + random.uniform(0, 0.5), max_delay)
                    else:
                        delay = min(base_delay * (1.5 ** (retries - 1)) + random.uniform(0, 0.1), max_delay)
                    
                    logger.warning(f"Retry {retries}/{max_retries} after error: {str(e)}. Waiting {delay:.2f}s...")
                    await asyncio.sleep(delay)
            
            logger.error(f"Max retries ({max_retries}) exceeded. Last error: {str(last_exception)}")
            raise last_exception
        return wrapper
    return decorator

class TokenAnalyzer:
    def __init__(self, config: TokenAnalysisConfig = None):
        self.config = config or TokenAnalysisConfig()
        # Alte Initialisierung (ersetzen):
        # self.price_service = None
        # self.etherscan_api = None
        # self.solana_api = None
        # self.sui_api = None
        
        # Neue Initialisierung (hinzufügen):
        self.coingecko_provider = None
        self.ethereum_provider = None
        self.bsc_provider = None
        self.solana_provider = None
        self.sui_provider = None
        
        self.w3_eth = None
        self.w3_bsc = None
        
        # Konfiguration laden
        self.ethereum_rpc = scanner_config.rpc_config.ethereum_rpc
        self.bsc_rpc = scanner_config.rpc_config.bsc_rpc
        self.etherscan_key = scanner_config.rpc_config.etherscan_api_key
        self.bscscan_key = scanner_config.rpc_config.bscscan_api_key
        self.coingecko_key = scanner_config.rpc_config.coingecko_api_key
        
        # Rate-Limit-Tracking für CoinGecko
        self.coingecko_last_request_time = 0
        self.coingecko_min_interval = 1.2  # 50 Anfragen pro Minute = 1.2 Sekunden zwischen Anfragen
        self.coingecko_request_count = 0
        self.coingecko_reset_time = datetime.utcnow() + timedelta(minutes=1)
        
        # Bekannte Contract-Adressen
        self.known_contracts = scanner_config.rpc_config.known_contracts
        self.cex_wallets = scanner_config.rpc_config.cex_wallets
    
    async def __aenter__(self):
        # Alte Methode:
        # self.price_service = PriceService(self.coingecko_key)
        # self.etherscan_api = EtherscanAPI(self.etherscan_key, self.bscscan_key)
        # self.solana_api = SolanaAPIService()
        # self.sui_api = SuiAPIService()
        
        # Neue Methode:
        self.coingecko_provider = CoinGeckoProvider(self.coingecko_key)
        self.ethereum_provider = EthereumProvider(self.etherscan_key)
        self.bsc_provider = EthereumProvider(self.bscscan_key)  # BSC verwendet auch EthereumProvider
        self.solana_provider = SolanaProvider()
        self.sui_provider = SuiProvider()
        
        self.w3_eth = Web3(Web3.HTTPProvider(self.ethereum_rpc))
        self.w3_bsc = Web3(Web3.HTTPProvider(self.bsc_rpc))
        
        # Alte Methode:
        # await self.price_service.__aenter__()
        # await self.etherscan_api.__aenter__()
        # await self.solana_api.__aenter__()
        # await self.sui_api.__aenter__()
        
        # Neue Methode:
        await self.coingecko_provider.__aenter__()
        await self.ethereum_provider.__aenter__()
        await self.bsc_provider.__aenter__()
        await self.solana_provider.__aenter__()
        await self.sui_provider.__aenter__()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sicheres Schließen aller Ressourcen
        close_tasks = []
        
        # Alte Methode:
        # if self.price_service:
        #     close_tasks.append(self._safe_close(self.price_service, exc_type, exc_val, exc_tb, "price_service"))
        
        # if self.etherscan_api:
        #     close_tasks.append(self._safe_close(self.etherscan_api, exc_type, exc_val, exc_tb, "etherscan_api"))
        
        # if self.solana_api:
        #     close_tasks.append(self._safe_close(self.solana_api, exc_type, exc_val, exc_tb, "solana_api"))
        
        # if self.sui_api:
        #     close_tasks.append(self._safe_close(self.sui_api, exc_type, exc_val, exc_tb, "sui_api"))
        
        # Neue Methode:
        if self.coingecko_provider:
            close_tasks.append(self._safe_close(self.coingecko_provider, exc_type, exc_val, exc_tb, "coingecko_provider"))
        
        if self.ethereum_provider:
            close_tasks.append(self._safe_close(self.ethereum_provider, exc_type, exc_val, exc_tb, "ethereum_provider"))
        
        if self.bsc_provider:
            close_tasks.append(self._safe_close(self.bsc_provider, exc_type, exc_val, exc_tb, "bsc_provider"))
        
        if self.solana_provider:
            close_tasks.append(self._safe_close(self.solana_provider, exc_type, exc_val, exc_tb, "solana_provider"))
        
        if self.sui_provider:
            close_tasks.append(self._safe_close(self.sui_provider, exc_type, exc_val, exc_tb, "sui_provider"))
        
        # Alle Schließvorgänge parallel ausführen
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Web3-Verbindungen trennen
        if hasattr(self, 'w3_eth') and self.w3_eth:
            self.w3_eth = None
        
        if hasattr(self, 'w3_bsc') and self.w3_bsc:
            self.w3_bsc = None
    
    async def _safe_close(self, service, exc_type, exc_val, exc_tb, service_name):
        """Sicheres Schließen einer Service-Verbindung"""
        try:
            await service.__aexit__(exc_type, exc_val, exc_tb)
        except Exception as e:
            logger.warning(f"Error closing {service_name}: {str(e)}")
    
    async def _enforce_coingecko_rate_limit(self):
        """Stellt sicher, dass das CoinGecko Rate-Limit eingehalten wird"""
        current_time = time.time()
        
        # Wenn das Zeitfenster abgelaufen ist, setze den Zähler zurück
        if datetime.utcnow() >= self.coingecko_reset_time:
            self.coingecko_request_count = 0
            self.coingecko_reset_time = datetime.utcnow() + timedelta(minutes=1)
        
        # Wenn wir das Limit erreicht haben, warte bis zum nächsten Zeitfenster
        if self.coingecko_request_count >= 50:
            wait_time = (self.coingecko_reset_time - datetime.utcnow()).total_seconds()
            if wait_time > 0:
                logger.warning(f"CoinGecko rate limit reached. Waiting {wait_time:.2f}s for reset...")
                await asyncio.sleep(wait_time + 0.5)  # Etwas mehr Puffer
                self.coingecko_request_count = 0
                self.coingecko_reset_time = datetime.utcnow() + timedelta(minutes=1)
        
        # Wenn die letzte Anfrage zu kurz zurückliegt, warte
        time_since_last_request = current_time - self.coingecko_last_request_time
        min_interval = 1.5  # Etwas mehr Abstand zwischen Anfragen
        if time_since_last_request < min_interval:
            await asyncio.sleep(min_interval - time_since_last_request)
        
        # Aktualisiere die Tracking-Variablen
        self.coingecko_last_request_time = time.time()
        self.coingecko_request_count += 1
    
    async def scan_low_cap_tokens(self, max_tokens: int = None) -> List[Dict[str, Any]]:
        """Hauptfunktion zum Scannen von Low-Cap Tokens"""
        max_tokens = max_tokens or self.config.max_tokens_per_scan
        
        logger.info(f"Starting low-cap token scan (max {max_tokens} tokens)...")
        
        # Hole Low-Cap Tokens
        # Alte Methode: async with self.price_service:
        # Neue Methode:
        async with self.coingecko_provider:
            # Alte Methode: tokens = await self.price_service.get_low_cap_tokens(
            # Neue Methode:
            tokens = await self.coingecko_provider.get_low_cap_tokens(
                max_market_cap=self.config.max_market_cap,
                limit=max_tokens
            )
        
        if not tokens:
            logger.error("No tokens found")
            return []
        
        # Analysiere Tokens
        results = []
        for i, token in enumerate(tokens):
            try:
                logger.info(f"Processing token {i+1}/{len(tokens)}: {token.symbol}")
                
                # Rate limiting
                if i > 0:
                    await asyncio.sleep(self.config.request_delay_seconds)
                
                analysis = await self.analyze_token(token)
                if analysis:
                    results.append(analysis)
            except Exception as e:
                logger.error(f"Error analyzing {token.symbol}: {e}")
                continue
        
        logger.info(f"Analysis completed. {len(results)} tokens successfully analyzed.")
        return results
    
    async def analyze_token(self, token_data: Token) -> Optional[Dict[str, Any]]:
        """Vollständige Analyse eines Tokens"""
        logger.info(f"Analyzing token: {token_data.symbol} ({token_data.address})")
        
        try:
            # Hole Token-Holder
            holders = await self._fetch_token_holders(token_data.address, token_data.chain)
            
            if not holders:
                logger.warning(f"No holder data for {token_data.symbol}")
                return None
            
            # Analysiere Wallets
            wallet_analyses = await self._analyze_wallets(token_data, holders)
            
            # Berechne Token-Score
            score_data = self._calculate_token_score(token_data, wallet_analyses)
            
            return {
                'token_data': token_data,
                'wallet_analyses': wallet_analyses,
                'token_score': score_data['total_score'],
                'analysis_date': datetime.utcnow(),
                'metrics': score_data['metrics'],
                'risk_flags': score_data['risk_flags']
            }
        except Exception as e:
            logger.error(f"Error analyzing token {token_data.symbol}: {e}")
            return None
    
    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def analyze_custom_token(self, token_address: str, chain: str) -> Dict[str, Any]:
        """Analysiert einen einzelnen, benutzerdefinierten Token"""
        try:
            # 1. Token-Metadaten abrufen
            token_data = await self._fetch_custom_token_data(token_address, chain)
            if not token_data:
                raise ValueError(f"Token data could not be retrieved for {token_address} on {chain}")
            
            # 2. Holder-Analyse durchführen
            holders = await self._fetch_token_holders(token_address, chain)
            
            # 3. Wallet-Klassifizierung
            wallet_analyses = await self._analyze_wallets(token_data, holders)
            
            # 4. Score-Berechnung
            score_data = self._calculate_token_score(token_data, wallet_analyses)
            
            # 5. Ergebnis zusammenstellen
            analysis_result = {
                'token_info': {
                    'address': token_data.address,
                    'name': token_data.name,
                    'symbol': token_data.symbol,
                    'chain': chain,
                    'market_cap': token_data.market_cap,
                    'volume_24h': token_data.volume_24h,
                    'holders_count': len(holders),
                    'liquidity': token_data.liquidity
                },
                'score': score_data['total_score'],
                'metrics': score_data['metrics'],
                'risk_flags': score_data['risk_flags'],
                'wallet_analysis': {
                    'total_wallets': len(wallet_analyses),
                    'dev_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DEV_WALLET]),
                    'whale_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE_WALLET]),
                    'rugpull_suspects': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.RUGPULL_SUSPECT]),
                    'top_holders': [
                        {
                            'address': w.wallet_address,
                            'balance': w.balance,
                            'percentage': w.percentage_of_supply,
                            'type': w.wallet_type.value
                        }
                        for w in sorted(wallet_analyses, key=lambda x: x.balance, reverse=True)[:10]
                    ]
                }
            }
            
            return analysis_result
        except Exception as e:
            logger.error(f"Error analyzing custom token {token_address} on {chain}: {e}")
            raise
    
    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def _fetch_custom_token_data(self, token_address: str, chain: str) -> Optional[Token]:
        """Holt Token-Daten für verschiedene Chains mit Rate-Limit-Handling"""
        try:
            # Rate-Limit-Handling für CoinGecko
            await self._enforce_coingecko_rate_limit()
            
            # Alte Methode: async with self.price_service:
            # Neue Methode:
            async with self.coingecko_provider:
                # Alte Methode: price_data = await self.price_service.get_token_price(token_address, chain)
                # Neue Methode:
                price_data = await self.coingecko_provider.get_token_price(token_address, chain)
            
            # Erstelle Token-Objekt
            token = Token(
                address=token_address,
                name="",  # Wird später gefüllt
                symbol="",  # Wird später gefüllt
                chain=chain,
                market_cap=price_data.market_cap,
                volume_24h=price_data.volume_24h,
                liquidity=0,  # Wird später berechnet
                holders_count=0,  # Wird später geholt
                contract_verified=False,  # Wird später geprüft
                creation_date=None,  # Wird später geholt
                token_score=0  # Wird später berechnet
            )
            
            # Zusätzliche Token-Informationen abrufen
            if chain in ['ethereum', 'bsc']:
                return await self._fetch_evm_token_data(token)
            elif chain == 'solana':
                return await self._fetch_solana_token_data(token)
            elif chain == 'sui':
                return await self._fetch_sui_token_data(token)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching token data for {token_address} on {chain}: {e}")
            return None
    
    async def _fetch_evm_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für EVM-Chains"""
        try:
            # Wähle den richtigen Web3-Provider
            w3 = self.w3_eth if token.chain == 'ethereum' else self.w3_bsc
            
            # ERC20-ABI für grundlegende Funktionen
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [],
                    "name": "name",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "symbol",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "totalSupply",
                    "outputs": [{"name": "", "type": "uint256"}],
                    "type": "function"
                }
            ]
            
            # Erstelle Contract-Instanz
            contract = w3.eth.contract(address=token.address, abi=erc20_abi)
            
            # Hole Token-Name und Symbol
            try:
                token.name = contract.functions.name().call()
            except Exception:
                pass
            
            try:
                token.symbol = contract.functions.symbol().call()
            except Exception:
                pass
            
            # Prüfe, ob der Contract verifiziert ist
            try:
                if token.chain == 'ethereum':
                    # Alte Methode: token.contract_verified = await self.etherscan_api.is_contract_verified(token.address, 'ethereum')
                    # Neue Methode:
                    token.contract_verified = await self.ethereum_provider.is_contract_verified(token.address, 'ethereum')
                else:
                    # Alte Methode: token.contract_verified = await self.etherscan_api.is_contract_verified(token.address, 'bsc')
                    # Neue Methode:
                    token.contract_verified = await self.bsc_provider.is_contract_verified(token.address, 'bsc')
            except Exception:
                pass
            
            # Hole Erstellungsdatum des Contracts
            try:
                # Alte Methode: creation_tx = await self.etherscan_api.get_contract_creation_tx(token.address, token.chain)
                # Neue Methode:
                if token.chain == 'ethereum':
                    creation_tx = await self.ethereum_provider.get_contract_creation_tx(token.address, token.chain)
                else:
                    creation_tx = await self.bsc_provider.get_contract_creation_tx(token.address, token.chain)
                
                if creation_tx:
                    tx_receipt = w3.eth.get_transaction_receipt(creation_tx)
                    if tx_receipt:
                        block = w3.eth.get_block(tx_receipt.blockNumber)
                        token.creation_date = datetime.fromtimestamp(block.timestamp)
            except Exception:
                pass
            
            return token
        except Exception as e:
            logger.error(f"Error fetching EVM token data for {token.address}: {e}")
            return token
    
    async def _fetch_solana_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für Solana"""
        try:
            # Alte Methode: token_info = await self.solana_api.get_token_info(token.address)
            # Neue Methode:
            token_info = await self.solana_provider.get_token_info(token.address)
            
            if token_info:
                token.name = token_info.get('name', '')
                token.symbol = token_info.get('symbol', '')
                token.creation_date = token_info.get('creation_date')
            
            return token
        except Exception as e:
            logger.error(f"Error fetching Solana token data for {token.address}: {e}")
            return token
    
    async def _fetch_sui_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für Sui"""
        try:
            # Alte Methode: token_info = await self.sui_api.get_token_info(token.address)
            # Neue Methode:
            token_info = await self.sui_provider.get_token_info(token.address)
            
            if token_info:
                token.name = token_info.get('name', '')
                token.symbol = token_info.get('symbol', '')
                token.creation_date = token_info.get('creation_date')
            
            return token
        except Exception as e:
            logger.error(f"Error fetching Sui token data for {token.address}: {e}")
            return token
    
    async def _fetch_token_holders(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Holt Token-Holder für verschiedene Chains"""
        try:
            if chain in ['ethereum', 'bsc']:
                # Alte Methode: return await self.etherscan_api.get_token_holders(token_address, chain)
                # Neue Methode:
                if chain == 'ethereum':
                    return await self.ethereum_provider.get_token_holders(token_address, chain)
                else:
                    return await self.bsc_provider.get_token_holders(token_address, chain)
            elif chain == 'solana':
                # Alte Methode: return await self.solana_api.get_token_holders(token_address)
                # Neue Methode:
                return await self.solana_provider.get_token_holders(token_address)
            elif chain == 'sui':
                # Alte Methode: return await self.sui_api.get_token_holders(token_address)
                # Neue Methode:
                return await self.sui_provider.get_token_holders(token_address)
            else:
                return []
        except Exception as e:
            logger.error(f"Error fetching token holders for {token_address} on {chain}: {e}")
            return []
    
    async def _analyze_wallets(self, token_data: Token, holders: List[Dict[str, Any]]) -> List[WalletAnalysis]:
        """Analysiert die Wallets der Token-Holder"""
        wallet_analyses = []
        
        # Berechne die Gesamtmenge der Token
        total_supply = sum(float(h.get('TokenHolderQuantity', 0)) for h in holders)
        
        # Begrenze die Anzahl der zu analysierenden Wallets
        holders_to_analyze = holders[:self.config.max_holders_to_analyze]
        
        for holder in holders_to_analyze:
            try:
                balance = float(holder.get('TokenHolderQuantity', 0))
                percentage = (balance / total_supply) * 100 if total_supply > 0 else 0
                wallet_address = holder.get('TokenHolderAddress', '')
                
                # Hole Transaktionsdaten für die Wallet
                transaction_data = await self._fetch_wallet_transaction_data(wallet_address, token_data.chain)
                
                # Klassifiziere die Wallet
                wallet_type = self._classify_wallet(wallet_address, balance, percentage, transaction_data, token_data)
                
                # Erstelle Wallet-Analyse
                wallet_analysis = WalletAnalysis(
                    wallet_address=wallet_address,
                    wallet_type=wallet_type,
                    balance=balance,
                    percentage_of_supply=percentage,
                    transaction_count=transaction_data.get('tx_count', 0),
                    first_transaction=transaction_data.get('first_tx_time'),
                    last_transaction=transaction_data.get('last_tx_time'),
                    risk_score=self._calculate_wallet_risk_score(wallet_type, percentage, transaction_data)
                )
                
                wallet_analyses.append(wallet_analysis)
            except Exception as e:
                logger.error(f"Error analyzing wallet {holder.get('TokenHolderAddress', 'Unknown')}: {e}")
                continue
        
        return wallet_analyses
    
    async def _fetch_wallet_transaction_data(self, wallet_address: str, chain: str) -> Dict[str, Any]:
        """Holt Transaktionsdaten für eine Wallet"""
        try:
            if chain in ['ethereum', 'bsc']:
                # Alte Methode: return await self.etherscan_api.get_wallet_transactions(wallet_address, chain)
                # Neue Methode:
                if chain == 'ethereum':
                    return await self.ethereum_provider.get_wallet_transactions(wallet_address, chain)
                else:
                    return await self.bsc_provider.get_wallet_transactions(wallet_address, chain)
            elif chain == 'solana':
                # Alte Methode: return await self.solana_api.get_wallet_transactions(wallet_address)
                # Neue Methode:
                return await self.solana_provider.get_wallet_transactions(wallet_address)
            elif chain == 'sui':
                # Alte Methode: return await self.sui_api.get_wallet_transactions(wallet_address)
                # Neue Methode:
                return await self.sui_provider.get_wallet_transactions(wallet_address)
            else:
                return {}
        except Exception as e:
            logger.error(f"Error fetching transaction data for {wallet_address} on {chain}: {e}")
            return {}
    
    def _classify_wallet(self, wallet_address: str, balance: float, percentage: float,
                        transaction_data: Dict[str, Any], token_data: Token) -> WalletTypeEnum:
        """Klassifiziert Wallets basierend auf verschiedenen Kriterien"""
        # Burn Wallet Check
        if wallet_address.lower() in [addr.lower() for addr in self.known_contracts.get('burn_addresses', [])]:
            return WalletTypeEnum.BURN_WALLET
        
        # DEX Contract Check
        if wallet_address.lower() in [addr.lower() for addr in self.known_contracts.values() if isinstance(addr, str)]:
            return WalletTypeEnum.DEX_CONTRACT
        
        # CEX Wallet Check
        for exchange, wallets in self.cex_wallets.items():
            if wallet_address.lower() in [w.lower() for w in wallets]:
                return WalletTypeEnum.CEX_WALLET
        
        # Whale Wallet (>5% der Supply)
        if percentage > self.config.whale_threshold_percentage:
            return WalletTypeEnum.WHALE_WALLET
        
        # Dev Wallet Heuristik (frühe Transaktionen + hoher Anteil)
        tx_count = transaction_data.get('tx_count', 0)
        if percentage > self.config.dev_threshold_percentage and tx_count < 10:
            return WalletTypeEnum.DEV_WALLET
        
        # Sniper Wallet (sehr frühe Käufe)
        first_tx_time = transaction_data.get('first_tx_time')
        if first_tx_time and token_data.creation_date:
            time_diff = first_tx_time - token_data.creation_date
            if time_diff.total_seconds() < self.config.sniper_time_threshold_seconds:  # Erste 5 Minuten
                return WalletTypeEnum.SNIPER_WALLET
        
        # Rugpull Verdacht (plötzliche große Verkäufe)
        recent_sells = transaction_data.get('recent_large_sells', 0)
        if recent_sells > percentage * (self.config.rugpull_sell_threshold_percentage / 100):  # Verkauft mehr als 50% des Holdings
            return WalletTypeEnum.RUGPULL_SUSPECT
        
        return WalletTypeEnum.UNKNOWN
    
    def _calculate_wallet_risk_score(self, wallet_type: WalletTypeEnum, percentage: float, 
                                    transaction_data: Dict[str, Any]) -> float:
        """Berechnet einen Risiko-Score für eine Wallet"""
        risk_score = 0.0
        
        # Basis-Risiko basierend auf Wallet-Typ
        type_risk_scores = {
            WalletTypeEnum.BURN_WALLET: 0.0,
            WalletTypeEnum.DEX_CONTRACT: 0.0,
            WalletTypeEnum.CEX_WALLET: 0.0,
            WalletTypeEnum.WHALE_WALLET: 30.0,
            WalletTypeEnum.DEV_WALLET: 50.0,
            WalletTypeEnum.SNIPER_WALLET: 40.0,
            WalletTypeEnum.RUGPULL_SUSPECT: 90.0,
            WalletTypeEnum.UNKNOWN: 10.0
        }
        
        risk_score += type_risk_scores.get(wallet_type, 10.0)
        
        # Zusätzliches Risiko basierend auf dem prozentualen Anteil
        if percentage > 20:
            risk_score += 30.0
        elif percentage > 10:
            risk_score += 20.0
        elif percentage > 5:
            risk_score += 10.0
        
        # Zusätzliches Risiko für verdächtige Transaktionen
        recent_sells = transaction_data.get('recent_large_sells', 0)
        if recent_sells > 0:
            risk_score += min(recent_sells * 5, 30.0)
        
        return min(risk_score, 100.0)
    
    def _calculate_gini_coefficient(self, balances: List[float]) -> float:
        """Berechnet den Gini-Koeffizienten für Token-Verteilung"""
        if not balances or len(balances) < 2:
            return 0.0
        
        sorted_balances = sorted(balances)
        n = len(sorted_balances)
        cumsum = np.cumsum(sorted_balances)
        
        # Gini-Koeffizient berechnen
        return (2.0 * sum((i + 1) * balance for i, balance in enumerate(sorted_balances))) / (n * cumsum[-1]) - (n + 1) / n
    
    def _calculate_token_score(self, token_data: Token, wallet_analyses: List[WalletAnalysis]) -> Dict[str, Any]:
        """Berechnet einen Risiko-Score für den Token"""
        score = 100.0  # Start mit perfektem Score
        risk_flags = []
        
        # Marktkapitalisierung Score (niedrigere MC = höheres Risiko)
        if token_data.market_cap < 100000:  # < $100k
            score -= 30
            risk_flags.append("very_low_market_cap")
        elif token_data.market_cap < 500000:  # < $500k
            score -= 20
            risk_flags.append("low_market_cap")
        elif token_data.market_cap < 1000000:  # < $1M
            score -= 10
            risk_flags.append("moderate_market_cap")
        
        # Liquiditäts-Score
        if token_data.liquidity < self.config.min_liquidity_threshold:  # < $50k Liquidität
            score -= 25
            risk_flags.append("low_liquidity")
        elif token_data.liquidity < 100000:  # < $100k
            score -= 15
            risk_flags.append("moderate_liquidity")
        
        # Contract Verification
        if not token_data.contract_verified:
            score -= 15
            risk_flags.append("unverified_contract")
        
        # Wallet-Verteilungsanalyse
        total_supply_analyzed = sum(w.percentage_of_supply for w in wallet_analyses)
        whale_percentage = sum(w.percentage_of_supply for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE_WALLET)
        dev_percentage = sum(w.percentage_of_supply for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DEV_WALLET)
        rugpull_suspects = sum(1 for w in wallet_analyses if w.wallet_type == WalletTypeEnum.RUGPULL_SUSPECT)
        
        # Whale-Konzentration
        if whale_percentage > 50:
            score -= 40
            risk_flags.append("high_whale_concentration")
        elif whale_percentage > 30:
            score -= 25
            risk_flags.append("moderate_whale_concentration")
        elif whale_percentage > 15:
            score -= 10
            risk_flags.append("low_whale_concentration")
        
        # Dev Wallet Konzentration
        if dev_percentage > 20:
            score -= 30
            risk_flags.append("high_dev_concentration")
        elif dev_percentage > 10:
            score -= 15
            risk_flags.append("moderate_dev_concentration")
        
        # Rugpull Verdächtige
        if rugpull_suspects > 0:
            score -= rugpull_suspects * 20
            risk_flags.append("rugpull_suspects")
        
        # Gini-Koeffizient
        balances = [w.balance for w in wallet_analyses]
        gini = self._calculate_gini_coefficient(balances)
        if gini > 0.8:  # Sehr ungleiche Verteilung
            score -= 20
            risk_flags.append("very_uneven_distribution")
        elif gini > 0.6:
            score -= 10
            risk_flags.append("uneven_distribution")
        
        # Metriken sammeln
        metrics = {
            'total_holders_analyzed': len(wallet_analyses),
            'whale_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE_WALLET]),
            'dev_wallets': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DEV_WALLET]),
            'rugpull_suspects': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.RUGPULL_SUSPECT]),
            'gini_coefficient': gini,
            'whale_percentage': whale_percentage,
            'dev_percentage': dev_percentage
        }
        
        return {
            'total_score': max(0, min(100, score)),
            'metrics': metrics,
            'risk_flags': risk_flags
        }
