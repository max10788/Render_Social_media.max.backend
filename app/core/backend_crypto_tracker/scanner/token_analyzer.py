"""
Token Analyzer - Refactored to use new blockchain data system
Analyzes tokens using direct function imports instead of provider classes
"""

import asyncio
import logging
from datetime import datetime, timedelta
import time
import random
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from web3 import Web3
from dataclasses import dataclass
from enum import Enum
from functools import wraps
import os

# Core imports
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import (
    APIException, InvalidAddressException, ValidationException, CustomAnalysisException
)
from app.core.backend_crypto_tracker.config.scanner_config import scanner_config
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData
from app.core.backend_crypto_tracker.utils.cache import AnalysisCache
from app.core.backend_crypto_tracker.processor.database.models.token import Token
from app.core.backend_crypto_tracker.processor.database.models.wallet import WalletAnalysis, WalletTypeEnum
from app.core.backend_crypto_tracker.config.blockchain_api_keys import get_api_keys
from app.core.backend_crypto_tracker.utils.json_helpers import sanitize_float
from app.core.backend_crypto_tracker.utils.token_data_resolver import TokenDataResolver

# Import blockchain-specific functions - Ethereum/BSC
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_token_holders import get_token_holders as ethereum_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_address_transactions import get_address_transactions as ethereum_get_transactions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_token_price import get_token_price as ethereum_get_price
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_contract_abi import get_contract_abi as ethereum_get_abi

# Import blockchain-specific functions - Solana
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_token_holders import get_token_holders as solana_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_token_metadata import get_token_metadata as solana_get_metadata
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_token_price import get_token_price as solana_get_price
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_transaction_details import get_transaction_details as solana_get_transaction

# Import blockchain-specific functions - Sui
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_token_holders import get_token_holders as sui_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_coin_metadata import get_coin_metadata as sui_get_metadata
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_token_price import get_token_price as sui_get_price
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_transaction import get_transaction as sui_get_transaction

# Import onchain functions - Etherscan
from app.core.backend_crypto_tracker.blockchain.onchain.etherscan.is_contract_verified import is_contract_verified as etherscan_is_verified
from app.core.backend_crypto_tracker.blockchain.onchain.etherscan.get_contract_creation import get_contract_creation as etherscan_get_creation

# Import aggregators
from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko.get_token_market_data import get_token_market_data as coingecko_get_market_data

logger = get_logger(__name__)


@dataclass
class TokenAnalysisConfig:
    """Konfiguration für die Token-Analyse"""
    max_tokens_per_scan: int = 100
    max_market_cap: float = 5_000_000
    min_liquidity_threshold: float = 50_000
    whale_threshold_percentage: float = 5.0
    dev_threshold_percentage: float = 2.0
    sniper_time_threshold_seconds: int = 300
    rugpull_sell_threshold_percentage: float = 50.0
    max_holders_to_analyze: int = 100
    request_delay_seconds: float = 1.0
    enable_cache: bool = True
    cache_ttl_seconds: int = 300
    preferred_provider: str = "CoinGecko"


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
    """
    Token Analyzer - Refactored to use new blockchain data system
    Eliminates provider classes in favor of direct function imports
    """
    
    def __init__(self, config: TokenAnalysisConfig = None):
        self.config = config or TokenAnalysisConfig()
        self.logger = get_logger(__name__)
        
        # Cache-Einstellungen
        self.enable_cache = self.config.enable_cache
        self.cache_ttl = self.config.cache_ttl_seconds
        self.cache = AnalysisCache(max_size=1000, default_ttl=self.cache_ttl) if self.enable_cache else None
        
        # Web3-Verbindungen (für EVM-Chains)
        self.ethereum_rpc = scanner_config.rpc_config.ethereum_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
        self.bsc_rpc = scanner_config.rpc_config.bsc_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
        
        self.w3_eth = None
        self.w3_bsc = None
        
        # API-Schlüssel
        self.etherscan_key = scanner_config.rpc_config.etherscan_api_key
        self.bscscan_key = scanner_config.rpc_config.bscscan_api_key
        
        # Bekannte Contract-Adressen
        self.known_contracts = scanner_config.rpc_config.known_contracts
        self.cex_wallets = scanner_config.rpc_config.cex_wallets
        
        # Token-Resolver
        self.token_resolver = TokenDataResolver()
        
        # Initialisierungs-Flag
        self._initialized = False
        
        self.logger.info("TokenAnalyzer initialisiert mit neuem Blockchain-Daten-System")

    async def __aenter__(self):
        """Initialisiert asynchrone Ressourcen"""
        if not self._initialized:
            self.logger.debug("Initialisiere Web3-Verbindungen")
            
            # Web3-Verbindungen für EVM-Chains
            if self.ethereum_rpc:
                self.w3_eth = Web3(Web3.HTTPProvider(self.ethereum_rpc))
                self.logger.debug("Ethereum Web3 connection initialized")
            
            if self.bsc_rpc:
                self.w3_bsc = Web3(Web3.HTTPProvider(self.bsc_rpc))
                self.logger.debug("BSC Web3 connection initialized")
            
            self._initialized = True
            self.logger.info("TokenAnalyzer successfully initialized")
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Schließt asynchrone Ressourcen"""
        if self._initialized:
            # Web3-Verbindungen trennen
            if self.w3_eth:
                self.w3_eth = None
            
            if self.w3_bsc:
                self.w3_bsc = None
            
            self._initialized = False
            self.logger.debug("TokenAnalyzer resources closed")

    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def analyze_custom_token(self, token_address: str, chain: str, use_cache: Optional[bool] = None) -> Dict[str, Any]:
        """
        Zentrale Analyse-Methode für einen einzelnen Token
        Refactored to use direct blockchain function calls
        """
        self.logger.info(f"Starte Analyse für Token {token_address} auf Chain {chain}")
        
        # Cache-Verwaltung
        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        cache_key = f"token_analysis_{token_address}_{chain}"
        
        if should_use_cache and self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                self.logger.info(f"Returning cached analysis for {token_address}")
                return cached_result
        
        # Validierung
        if not token_address or not isinstance(token_address, str) or not token_address.strip():
            raise ValidationException("Token-Adresse muss ein nicht-leerer String sein", field="token_address")
        
        if not chain or not isinstance(chain, str) or not chain.strip():
            raise ValidationException("Chain muss ein nicht-leerer String sein", field="chain")
        
        chain = chain.lower().strip()
        
        try:
            # Schritt 1: Token-Daten abrufen
            token_data = await self.token_resolver.resolve_token_data(token_address, chain)
            
            if not token_data or token_data.name == "Unknown":
                self.logger.warning(f"Token {token_address} nicht gefunden auf {chain}")
                raise ValueError("Token data could not be retrieved")
            
            # Schritt 2: Holder-Daten abrufen (chain-spezifisch)
            holders = await self._get_token_holders(token_address, chain)
            
            # Schritt 3: Wallets analysieren
            wallet_analyses = await self._analyze_wallets(token_data, holders, chain)
            
            # Schritt 4: Token-Score berechnen
            score_result = self._calculate_token_score(token_data, wallet_analyses)
            
            # Schritt 5: Ergebnis zusammenstellen
            result = {
                'token_info': {
                    'address': token_data.address,
                    'name': token_data.name,
                    'symbol': token_data.symbol,
                    'chain': token_data.chain,
                    'market_cap': token_data.market_cap,
                    'volume_24h': token_data.volume_24h,
                    'holders_count': token_data.holders_count,
                    'liquidity': token_data.liquidity,
                    'contract_verified': token_data.contract_verified,
                    'creation_date': token_data.creation_date.isoformat() if token_data.creation_date else None
                },
                'score': score_result['total_score'],
                'metrics': score_result['metrics'],
                'risk_flags': score_result['risk_flags'],
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
                        for w in wallet_analyses[:10]  # Top 10 Holder
                    ]
                }
            }
            
            # Cache speichern
            if should_use_cache and self.cache:
                await self.cache.set(result, self.cache_ttl, cache_key)
            
            self.logger.info(f"Analyse für Token {token_address} erfolgreich abgeschlossen")
            return result
            
        except ValueError as e:
            if "Token data could not be retrieved" in str(e):
                self.logger.error(f"Token-Daten nicht verfügbar für {token_address} auf {chain}")
                # Minimales Ergebnis zurückgeben
                minimal_result = {
                    'token_info': {
                        'address': token_address,
                        'name': "Unknown",
                        'symbol': "UNKNOWN",
                        'chain': chain,
                        'market_cap': 0,
                        'volume_24h': 0,
                        'holders_count': 0,
                        'liquidity': 0
                    },
                    'score': 50.0,
                    'metrics': {
                        'total_holders_analyzed': 0,
                        'whale_wallets': 0,
                        'dev_wallets': 0,
                        'rugpull_suspects': 0,
                        'gini_coefficient': 0,
                        'whale_percentage': 0,
                        'dev_percentage': 0
                    },
                    'risk_flags': ["limited_data"],
                    'wallet_analysis': {
                        'total_wallets': 0,
                        'dev_wallets': 0,
                        'whale_wallets': 0,
                        'rugpull_suspects': 0,
                        'top_holders': []
                    }
                }
                
                if should_use_cache and self.cache:
                    await self.cache.set(minimal_result, self.cache_ttl, cache_key)
                
                return minimal_result
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unerwarteter Fehler bei Token-Analyse: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Unerwarteter Fehler: {str(e)}") from e

    async def _get_token_holders(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """
        Holt Token-Holder-Daten chain-spezifisch
        Neue Implementierung mit direkten Funktionsaufrufen
        """
        try:
            self.logger.debug(f"Fetching holders for {token_address} on {chain}")
            
            if chain in ['ethereum', 'bsc']:
                holders = await ethereum_get_holders(token_address)
            elif chain == 'solana':
                holders = await solana_get_holders(token_address)
            elif chain == 'sui':
                holders = await sui_get_holders(token_address)
            else:
                self.logger.warning(f"Unsupported chain: {chain}")
                return []
            
            self.logger.debug(f"Found {len(holders) if holders else 0} holders")
            return holders or []
            
        except Exception as e:
            self.logger.error(f"Error fetching holders for {token_address} on {chain}: {e}")
            return []

    async def _analyze_wallets(self, token_data: Token, holders: List[Dict[str, Any]], chain: str) -> List[WalletAnalysis]:
        """
        Analysiert die Wallets der Token-Holder
        Refactored to use direct transaction queries
        """
        wallet_analyses = []
        
        # Gesamtmenge berechnen
        total_supply = sum(float(h.get('balance', 0)) for h in holders)
        
        # Begrenze Anzahl der zu analysierenden Wallets
        holders_to_analyze = holders[:self.config.max_holders_to_analyze]
        
        for holder in holders_to_analyze:
            try:
                balance = float(holder.get('balance', 0))
                wallet_address = holder.get('address', '')
                percentage = (balance / total_supply) * 100 if total_supply > 0 else 0
                
                # Cache-Check
                cache_key = f"wallet_analysis_{wallet_address}_{token_data.address}"
                
                if self.cache:
                    cached_result = await self.cache.get(cache_key)
                    if cached_result:
                        wallet_analyses.append(cached_result)
                        continue
                
                # Transaktionsdaten abrufen (chain-spezifisch)
                transaction_data = await self._get_wallet_transactions(wallet_address, chain, token_data.address)
                
                # Wallet klassifizieren
                wallet_type = self._classify_wallet(wallet_address, balance, percentage, transaction_data, token_data)
                
                # Wallet-Analyse erstellen
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
                
                # Cache speichern
                if self.cache:
                    await self.cache.set(wallet_analysis, self.cache_ttl, cache_key)
                
            except Exception as e:
                self.logger.error(f"Error analyzing wallet {holder.get('address', 'Unknown')}: {e}")
                continue
        
        return wallet_analyses

    async def _get_wallet_transactions(self, wallet_address: str, chain: str, token_address: str) -> Dict[str, Any]:
        """
        Holt Transaktionsdaten für eine Wallet
        Neue Implementierung mit chain-spezifischen Funktionen
        """
        try:
            if chain in ['ethereum', 'bsc']:
                transactions = await ethereum_get_transactions(wallet_address, token_address)
            elif chain == 'solana':
                transactions = await solana_get_transaction(wallet_address)
            elif chain == 'sui':
                transactions = await sui_get_transaction(wallet_address)
            else:
                return {'tx_count': 0, 'first_tx_time': None, 'last_tx_time': None, 'recent_large_sells': 0}
            
            # Verarbeite Transaktionen
            if not transactions:
                return {'tx_count': 0, 'first_tx_time': None, 'last_tx_time': None, 'recent_large_sells': 0}
            
            tx_count = len(transactions)
            first_tx_time = min(tx.get('timestamp') for tx in transactions if tx.get('timestamp'))
            last_tx_time = max(tx.get('timestamp') for tx in transactions if tx.get('timestamp'))
            
            # Zähle große Verkäufe (letzte 7 Tage)
            recent_sells = 0
            seven_days_ago = datetime.now() - timedelta(days=7)
            for tx in transactions:
                if tx.get('timestamp') and tx.get('timestamp') > seven_days_ago:
                    if tx.get('type') == 'sell' and tx.get('value', 0) > 0:
                        recent_sells += 1
            
            return {
                'tx_count': tx_count,
                'first_tx_time': first_tx_time,
                'last_tx_time': last_tx_time,
                'recent_large_sells': recent_sells
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching transactions for {wallet_address} on {chain}: {e}")
            return {'tx_count': 0, 'first_tx_time': None, 'last_tx_time': None, 'recent_large_sells': 0}

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
        
        # Dev Wallet Heuristik
        tx_count = transaction_data.get('tx_count', 0)
        if percentage > self.config.dev_threshold_percentage and tx_count < 10:
            return WalletTypeEnum.DEV_WALLET
        
        # Sniper Wallet
        first_tx_time = transaction_data.get('first_tx_time')
        if first_tx_time and token_data.creation_date:
            time_diff = first_tx_time - token_data.creation_date
            if time_diff.total_seconds() < self.config.sniper_time_threshold_seconds:
                return WalletTypeEnum.SNIPER_WALLET
        
        # Rugpull Verdacht
        recent_sells = transaction_data.get('recent_large_sells', 0)
        if recent_sells > percentage * (self.config.rugpull_sell_threshold_percentage / 100):
            return WalletTypeEnum.RUGPULL_SUSPECT
        
        return WalletTypeEnum.UNKNOWN

    def _calculate_wallet_risk_score(self, wallet_type: WalletTypeEnum, percentage: float, 
                                    transaction_data: Dict[str, Any]) -> float:
        """Berechnet einen Risiko-Score für eine Wallet"""
        risk_score = 0.0
        
        # Basis-Risiko nach Wallet-Typ
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
        
        # Zusätzliches Risiko basierend auf Anteil
        if percentage > 20:
            risk_score += 30.0
        elif percentage > 10:
            risk_score += 20.0
        elif percentage > 5:
            risk_score += 10.0
        
        # Verdächtige Transaktionen
        recent_sells = transaction_data.get('recent_large_sells', 0)
        if recent_sells > 0:
            risk_score += min(recent_sells * 5, 30.0)
        
        return min(risk_score, 100.0)

    def _calculate_token_score(self, token_data: Token, wallet_analyses: List[WalletAnalysis]) -> Dict[str, Any]:
        """Berechnet einen Risiko-Score für den Token"""
        score = 100.0  # Start mit perfektem Score
        risk_flags = []
        
        # Marktkapitalisierung Score
        market_cap = sanitize_float(token_data.market_cap)
        if market_cap < 100000:  # < $100k
            score -= 30
            risk_flags.append("very_low_market_cap")
        elif market_cap < 500000:  # < $500k
            score -= 20
            risk_flags.append("low_market_cap")
        elif market_cap < 1000000:  # < $1M
            score -= 10
            risk_flags.append("moderate_market_cap")
        
        # Liquiditäts-Score
        liquidity = sanitize_float(token_data.liquidity)
        if liquidity < self.config.min_liquidity_threshold:
            score -= 25
            risk_flags.append("low_liquidity")
        elif liquidity < 100000:
            score -= 15
            risk_flags.append("moderate_liquidity")
        
        # Contract Verification
        if not token_data.contract_verified:
            score -= 15
            risk_flags.append("unverified_contract")
        
        # Wallet-Verteilungsanalyse
        if wallet_analyses:
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
            balances = [w.balance for w in wallet_analyses if w.balance > 0]
            gini = 0.0
            
            if len(balances) > 1:
                try:
                    gini = self._calculate_gini_coefficient(balances)
                    gini = sanitize_float(gini)
                except Exception as e:
                    logger.warning(f"Error calculating Gini coefficient: {e}")
                    gini = 0.0
            
            if gini > 0.8:
                score -= 20
                risk_flags.append("very_uneven_distribution")
            elif gini > 0.6:
                score -= 10
                risk_flags.append("uneven_distribution")
        else:
            score -= 25
            risk_flags.append("no_wallet_data")
            whale_percentage = 0.0
            dev_percentage = 0.0
            rugpull_suspects = 0
            gini = 0.0
        
        # Score im gültigen Bereich halten
        score = max(0.0, min(100.0, score))
        
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
            'total_score': sanitize_float(score),
            'metrics': metrics,
            'risk_flags': risk_flags
        }

    def _calculate_gini_coefficient(self, balances: List[float]) -> float:
        """Berechnet den Gini-Koeffizienten für Token-Verteilung"""
        if not balances or len(balances) < 2:
            return 0.0
        
        sorted_balances = sorted(balances)
        n = len(sorted_balances)
        cumsum = sum(sorted_balances)
        
        if cumsum <= 0:
            return 0.0
        
        gini = (2.0 * sum((i + 1) * balance for i, balance in enumerate(sorted_balances))) / (n * cumsum) - (n + 1) / n
        gini = max(0.0, min(1.0, gini))
        
        return sanitize_float(gini)

    def _perform_extended_risk_assessment(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """Führt eine erweiterte Risikobewertung durch"""
        risk_factors = []
        overall_risk = 50.0  # Neutraler Startwert
        
        # Basis-Score aus der Analyse
        base_score = analysis_result.get('score', 50.0)
        base_risk = 100.0 - float(base_score)
        overall_risk = (overall_risk + base_risk) / 2.0
        
        metrics = analysis_result.get('metrics', {})
        
        # Whale-Konzentration
        whale_percentage = float(metrics.get('whale_percentage', 0.0))
        if whale_percentage > 50:
            risk_factors.append({
                'factor': 'high_whale_concentration',
                'description': f'Hohe Whale-Konzentration ({whale_percentage:.1f}%)',
                'impact': 30
            })
            overall_risk = min(100.0, overall_risk + 30)
        elif whale_percentage > 20:
            risk_factors.append({
                'factor': 'moderate_whale_concentration',
                'description': f'Moderate Whale-Konzentration ({whale_percentage:.1f}%)',
                'impact': 15
            })
            overall_risk = min(100.0, overall_risk + 15)
        
        # Dev-Konzentration
        dev_percentage = float(metrics.get('dev_percentage', 0.0))
        if dev_percentage > 20:
            risk_factors.append({
                'factor': 'high_dev_concentration',
                'description': f'Hohe Entwickler-Konzentration ({dev_percentage:.1f}%)',
                'impact': 25
            })
            overall_risk = min(100.0, overall_risk + 25)
        elif dev_percentage > 10:
            risk_factors.append({
                'factor': 'moderate_dev_concentration',
                'description': f'Moderate Entwickler-Konzentration ({dev_percentage:.1f}%)',
                'impact': 12
            })
            overall_risk = min(100.0, overall_risk + 12)
        
        # Rugpull-Verdacht
        rugpull_suspects = int(metrics.get('rugpull_suspects', 0))
        if rugpull_suspects > 0:
            risk_factors.append({
                'factor': 'rugpull_suspects',
                'description': f'{rugpull_suspects} verdächtige Wallets entdeckt',
                'impact': rugpull_suspects * 20
            })
            overall_risk = min(100.0, overall_risk + rugpull_suspects * 20)
        
        # Gini-Koeffizient
        gini = float(metrics.get('gini_coefficient', 0.0))
        if gini > 0.8:
            risk_factors.append({
                'factor': 'very_uneven_distribution',
                'description': f'Sehr ungleiche Token-Verteilung (Gini: {gini:.2f})',
                'impact': 20
            })
            overall_risk = min(100.0, overall_risk + 20)
        elif gini > 0.6:
            risk_factors.append({
                'factor': 'uneven_distribution',
                'description': f'Ungleiche Token-Verteilung (Gini: {gini:.2f})',
                'impact': 10
            })
            overall_risk = min(100.0, overall_risk + 10)
        
        # Risikoflags
        risk_flags = analysis_result.get('risk_flags', [])
        
        if 'very_low_market_cap' in risk_flags:
            risk_factors.append({
                'factor': 'very_low_market_cap',
                'description': 'Sehr geringe Marktkapitalisierung',
                'impact': 30
            })
            overall_risk = min(100.0, overall_risk + 30)
        elif 'low_market_cap' in risk_flags:
            risk_factors.append({
                'factor': 'low_market_cap',
                'description': 'Geringe Marktkapitalisierung',
                'impact': 20
            })
            overall_risk = min(100.0, overall_risk + 20)
        
        if 'low_liquidity' in risk_flags:
            risk_factors.append({
                'factor': 'low_liquidity',
                'description': 'Geringe Liquidität',
                'impact': 25
            })
            overall_risk = min(100.0, overall_risk + 25)
        
        if 'unverified_contract' in risk_flags:
            risk_factors.append({
                'factor': 'unverified_contract',
                'description': 'Nicht verifizierter Smart Contract',
                'impact': 15
            })
            overall_risk = min(100.0, overall_risk + 15)
        
        # Score im gültigen Bereich
        overall_risk = max(0.0, min(100.0, overall_risk))
        
        # Risikostufe bestimmen
        if overall_risk >= 80:
            risk_level = 'critical'
        elif overall_risk >= 60:
            risk_level = 'high'
        elif overall_risk >= 40:
            risk_level = 'medium'
        elif overall_risk >= 20:
            risk_level = 'low'
        else:
            risk_level = 'minimal'
        
        # Empfehlung generieren
        if overall_risk >= 70:
            recommendation = 'Nicht empfohlen'
        elif overall_risk >= 50:
            recommendation = 'Hohe Vorsicht'
        elif overall_risk >= 30:
            recommendation = 'Mit Vorsicht'
        else:
            recommendation = 'Potenziell sicher'
        
        return {
            'overall_risk': round(overall_risk, 2),
            'risk_level': risk_level,
            'recommendation': recommendation,
            'risk_factors': risk_factors
        }

    async def analyze_token(self, token_address: str, chain: str) -> Dict[str, Any]:
        """
        Analysiert einen Token und gibt eine umfassende Bewertung zurück
        Wrapper-Methode mit erweiteter Risikobewertung
        """
        try:
            logger.info(f"Starte Analyse für Token {token_address} auf Chain {chain}")
            
            # Grundlegende Token-Analyse
            analysis_result = await self.analyze_custom_token(token_address, chain)
            
            if not analysis_result:
                logger.warning(f"Keine Analyseergebnisse für {token_address}")
                return {}
            
            # Erweiterte Risikobewertung
            try:
                extended_risk_assessment = self._perform_extended_risk_assessment(analysis_result)
                overall_risk_score = extended_risk_assessment.get('overall_risk', 50)
                
                complete_analysis = {
                    **analysis_result,
                    'extended_risk_assessment': extended_risk_assessment,
                    'overall_risk_score': overall_risk_score,
                    'analysis_timestamp': datetime.utcnow().isoformat(),
                    'analyzer_version': '2.0'
                }
                
                logger.info(f"Analyse für Token {token_address} abgeschlossen")
                return complete_analysis
                
            except Exception as risk_error:
                logger.warning(f"Fehler bei erweiterten Risikobewertung: {risk_error}")
                analysis_result['overall_risk_score'] = analysis_result.get('score', 50)
                analysis_result['risk_level'] = 'unknown'
                analysis_result['extended_risk_assessment'] = {
                    'overall_risk': analysis_result.get('score', 50),
                    'error': str(risk_error)
                }
                return analysis_result
                
        except Exception as e:
            logger.error(f"Fehler bei Token-Analyse: {e}")
            return {
                'error': str(e),
                'token_address': token_address,
                'chain': chain,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }

    async def scan_low_cap_tokens(self, max_tokens: int = None) -> List[Dict[str, Any]]:
        """
        Massenanalyse-Methode für mehrere Tokens
        Verwendet CoinGecko für Discovery
        """
        max_tokens = max_tokens or self.config.max_tokens_per_scan
        logger.info(f"Starting low-cap token scan (max {max_tokens} tokens)...")
        
        # Cache-Check
        cache_key = f"low_cap_tokens_{max_tokens}_{self.config.max_market_cap}"
        
        if self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                logger.info("Returning cached low-cap tokens data")
                return cached_result
        
        # Hole Low-Cap Tokens von CoinGecko
        try:
            tokens_data = await coingecko_get_market_data(
                max_market_cap=self.config.max_market_cap,
                limit=max_tokens
            )
            
            if not tokens_data:
                logger.error("No tokens found")
                return []
            
            # Analysiere Tokens
            results = []
            for i, token_info in enumerate(tokens_data):
                try:
                    logger.info(f"Processing token {i+1}/{len(tokens_data)}: {token_info.get('symbol')}")
                    
                    # Rate limiting
                    if i > 0:
                        await asyncio.sleep(self.config.request_delay_seconds)
                    
                    token_address = token_info.get('address')
                    chain = token_info.get('chain', 'ethereum')
                    
                    if not token_address:
                        continue
                    
                    analysis = await self.analyze_token(token_address, chain)
                    if analysis:
                        results.append(analysis)
                        
                except Exception as e:
                    logger.error(f"Error analyzing token: {e}")
                    continue
            
            # Cache speichern
            if self.cache:
                await self.cache.set(results, self.config.cache_ttl_seconds, cache_key)
            
            logger.info(f"Analysis completed. {len(results)} tokens successfully analyzed.")
            return results
            
        except Exception as e:
            logger.error(f"Error in scan_low_cap_tokens: {e}")
            return []

    async def close(self):
        """Schließt alle offenen Ressourcen"""
        await self.__aexit__(None, None, None)
        logger.debug("TokenAnalyzer resources closed successfully")
