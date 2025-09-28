import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
import time
import random
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from web3 import Web3
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
import os

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, InvalidAddressException
from app.core.backend_crypto_tracker.config.scanner_config import scanner_config
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum_provider import EthereumProvider
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana_provider import SolanaProvider
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui_provider import SuiProvider
from app.core.backend_crypto_tracker.utils.cache import AnalysisCache  # Angepasst: Verwende AnalysisCache statt TokenCache

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
from app.core.backend_crypto_tracker.blockchain.onchain.etherscan_provider import EtherscanProvider
from app.core.backend_crypto_tracker.config.blockchain_api_keys import get_api_keys

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
    enable_cache: bool = True
    cache_ttl_seconds: int = 300  # 5 Minuten
    preferred_provider: str = "CoinGecko"  # Neuer Parameter zur Auswahl des Providers

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
        
        # Logger initialisieren
        self.logger = get_logger(__name__)
        
        # Konfigurationswerte als Instanzattribute übernehmen
        self.enable_cache = self.config.enable_cache
        self.cache_ttl = self.config.cache_ttl_seconds
        
        # Provider-Initialisierung - Ersetzt APIManager durch BaseAPIProvider
        self.api_provider = None  # Wird in __aenter__ initialisiert
        self.ethereum_provider = None
        self.bsc_provider = None
        self.solana_provider = None
        self.sui_provider = None
        
        self.w3_eth = None
        self.w3_bsc = None
        
        # Cache initialisieren, falls aktiviert - angepasst für AnalysisCache
        self.cache = AnalysisCache(max_size=1000, default_ttl=self.config.cache_ttl_seconds) if self.config.enable_cache else None
        
        # Konfiguration laden
        self.ethereum_rpc = scanner_config.rpc_config.ethereum_rpc
        self.bsc_rpc = scanner_config.rpc_config.bsc_rpc
        self.etherscan_key = scanner_config.rpc_config.etherscan_api_key
        self.bscscan_key = scanner_config.rpc_config.bscscan_api_key
        
        # Bekannte Contract-Adressen
        self.known_contracts = scanner_config.rpc_config.known_contracts
        self.cex_wallets = scanner_config.rpc_config.cex_wallets

    async def __aenter__(self):
        # API-Manager initialisieren
        await self._initialize_api_manager()
        
        # Blockchain-Provider initialisieren mit GetBlock RPC-URL
        if os.getenv('ETHERSCAN_API_KEY'):
            # Verwende die GetBlock RPC-URL
            ethereum_rpc = self.ethereum_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
            self.ethereum_provider = EthereumProvider(self.etherscan_key, rpc_url=ethereum_rpc)
            logger.info("Ethereum provider initialized")
        else:
            logger.warning("Etherscan API key not provided, using limited functionality")
            # Verwende die GetBlock RPC-URL auch ohne API-Key
            ethereum_rpc = self.ethereum_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
            self.ethereum_provider = EthereumProvider(rpc_url=ethereum_rpc)
        
        if os.getenv('BSCSCAN_API_KEY'):
            # Verwende die GetBlock RPC-URL für BSC
            bsc_rpc = self.bsc_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
            self.bsc_provider = EthereumProvider(self.bscscan_key, rpc_url=bsc_rpc)
            logger.info("BSC provider initialized")
        else:
            logger.warning("BSCscan API key not provided, using limited functionality")
            # Verwende die GetBlock RPC-URL auch ohne API-Key
            bsc_rpc = self.bsc_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
            self.bsc_provider = EthereumProvider(rpc_url=bsc_rpc)
        
        if os.getenv('SOLANA_RPC_URL'):
            self.solana_provider = SolanaProvider()
            logger.info("Solana provider initialized")
        else:
            logger.warning("Solana RPC URL not provided, skipping this provider")
        
        if os.getenv('SUI_RPC_URL'):
            self.sui_provider = SuiProvider()
            logger.info("Sui provider initialized")
        else:
            logger.warning("Sui RPC URL not provided, skipping this provider")
        
        # Web3-Verbindungen initialisieren
        if self.ethereum_rpc:
            self.w3_eth = Web3(Web3.HTTPProvider(self.ethereum_rpc))
        
        if self.bsc_rpc:
            self.w3_bsc = Web3(Web3.HTTPProvider(self.bsc_rpc))
        
        # Provider-Sessions initialisieren
        if self.ethereum_provider:
            await self.ethereum_provider.__aenter__()
        
        if self.bsc_provider:
            await self.bsc_provider.__aenter__()
        
        if self.solana_provider:
            await self.solana_provider.__aenter__()
        
        if self.sui_provider:
            await self.sui_provider.__aenter__()
        
        return self
    
    async def _initialize_api_provider(self):
        """Initialisiert den API-Provider basierend auf der Konfiguration"""
        provider_name = self.config.preferred_provider.lower()
        
        # Erstelle den ausgewählten Provider
        if provider_name == "coingecko":
            self.api_provider = CoinGeckoProvider()
        elif provider_name == "coinmarketcap":
            self.api_provider = CoinMarketCapProvider()
        elif provider_name == "cryptocompare":
            self.api_provider = CryptoCompareProvider()
        elif provider_name == "bitget":
            self.api_provider = BitgetProvider()
        elif provider_name == "kraken":
            self.api_provider = KrakenProvider()
        elif provider_name == "binance":
            self.api_provider = BinanceProvider()
        elif provider_name == "coinbase":
            self.api_provider = CoinbaseProvider()
        elif provider_name == "bitquery":
            self.api_provider = BitqueryProvider()
        else:
            logger.warning(f"Unknown provider '{provider_name}', defaulting to CoinGecko")
            self.api_provider = CoinGeckoProvider()
        
        # Initialisiere den Provider
        await self.api_provider.__aenter__()
        logger.info(f"{self.config.preferred_provider} provider initialized")
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Sicheres Schließen aller Ressourcen
        close_tasks = []
        
        if self.api_provider:
            close_tasks.append(self._safe_close_api_provider())
        
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
            # Zusätzlich: Explizite close-Methode aufrufen, falls vorhanden
            if hasattr(service, 'close'):
                await service.close()
        except Exception as e:
            logger.warning(f"Error closing {service_name}: {str(e)}")
    
    async def _safe_close_api_provider(self):
        """Sicheres Schließen des API-Providers"""
        try:
            await self.api_provider.__aexit__(None, None, None)
            if hasattr(self.api_provider, 'close'):
                await self.api_provider.close()
        except Exception as e:
            logger.warning(f"Error closing API provider: {str(e)}")
    
    async def scan_low_cap_tokens(self, max_tokens: int = None) -> List[Dict[str, Any]]:
        """Hauptfunktion zum Scannen von Low-Cap Tokens"""
        max_tokens = max_tokens or self.config.max_tokens_per_scan
        
        logger.info(f"Starting low-cap token scan (max {max_tokens} tokens)...")
        
        # Cache-Schlüssel für diese Anfrage
        cache_key = f"low_cap_tokens_{max_tokens}_{self.config.max_market_cap}"
        
        # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
        if self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                logger.info(f"Returning cached low-cap tokens data")
                return cached_result
        
        # Hole Low-Cap Tokens über den API-Provider - Angepasst an BaseAPIProvider
        try:
            # BaseAPIProvider hat keine get_low_cap_tokens-Methode, also müssen wir eine alternative Implementierung verwenden
            # Hier verwenden wir get_token_price mit einer Liste bekannter Token-Adressen
            # In einer echten Implementierung müsste dies angepasst werden
            
            # Placeholder für die Token-Liste - in einer echten Implementierung würde dies anders gehandhabt
            known_tokens = [
                ("0x1234567890123456789012345678901234567890", "ethereum"),
                ("0x0987654321098765432109876543210987654321", "ethereum")
                # Weitere Token-Adressen würden hier stehen
            ]
            
            tokens = []
            for address, chain in known_tokens[:max_tokens]:
                try:
                    price_data = await self.api_provider.get_token_price(address, chain)
                    if price_data:
                        tokens.append(price_data)
                except Exception as e:
                    logger.warning(f"Error fetching token data for {address}: {e}")
                    continue
        except Exception as e:
            logger.error(f"Error fetching low-cap tokens: {e}")
            tokens = []
        
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
                
                analysis = await self.analyze_token(token.address, token.chain if hasattr(token, 'chain') else 'ethereum')
                if analysis:
                    results.append(analysis)
            except Exception as e:
                logger.error(f"Error analyzing {token.symbol}: {e}")
                continue
        
        # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
        if self.cache:
            await self.cache.set(results, self.config.cache_ttl_seconds, cache_key)
        
        logger.info(f"Analysis completed. {len(results)} tokens successfully analyzed.")
        return results
    
    async def analyze_token(self, token_address: str, chain: str) -> Dict[str, Any]:
        """Analysiert einen Token und gibt eine umfassende Bewertung zurück"""
        try:
            logger.info(f"Starte Analyse für Token {token_address} auf Chain {chain}")
            
            # Führe die grundlegende Token-Analyse durch
            analysis_result = await self.analyze_custom_token(token_address, chain)
            
            if not analysis_result:
                logger.warning(f"Keine Analyseergebnisse für {token_address}")
                return {}
            
            # Führe erweiterte Risikobewertung durch
            try:
                extended_risk_assessment = self._perform_extended_risk_assessment(analysis_result)
                
                # Korrigierter Zugriff auf overall_risk
                overall_risk_score = extended_risk_assessment.get('overall_risk', 50)
                
                # Vollständiges Ergebnis zusammenstellen
                complete_analysis = {
                    **analysis_result,
                    'extended_risk_assessment': extended_risk_assessment,
                    'overall_risk_score': overall_risk_score,
                    'analysis_timestamp': datetime.utcnow().isoformat(),
                    'analyzer_version': '2.0'
                }
                
                logger.info(f"Analyse für Token {token_address} auf Chain {chain} abgeschlossen")
                return complete_analysis
                
            except Exception as risk_error:
                logger.warning(f"Fehler bei der erweiterten Risikobewertung: {risk_error}")
                
                # Fallback: Verwende nur die Basis-Analyse
                analysis_result['overall_risk_score'] = analysis_result.get('score', 50)
                analysis_result['risk_level'] = 'unknown'
                analysis_result['extended_risk_assessment'] = {
                    'overall_risk': analysis_result.get('score', 50),
                    'error': str(risk_error)
                }
                
                return analysis_result
                
        except Exception as e:
            logger.error(f"Fehler bei der Token-Analyse: {e}")
            return {
                'error': str(e),
                'token_address': token_address,
                'chain': chain,
                'analysis_timestamp': datetime.utcnow().isoformat()
            }
    
    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def analyze_custom_token(self, token_address: str, chain: str, use_cache: Optional[bool] = None) -> Dict[str, Any]:
        """Zentrale Analyse-Methode für einen einzelnen Token"""
        self.logger.info(f"Starte Analyse für Token {token_address} auf Chain {chain}")
        
        # Bestimme, ob Cache verwendet werden soll
        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        
        # Cache-Schlüssel für diese Anfrage
        cache_key = f"lowcap_analyzer_custom_{token_address}_{chain}"
        
        # Prüfe, ob die Daten im Cache vorhanden sind
        if should_use_cache and self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                self.logger.info(f"Returning cached analysis for {token_address} on {chain}")
                return cached_result
        
        # Validierung der Eingabeparameter
        if not token_address or not isinstance(token_address, str) or not token_address.strip():
            error_msg = "Token-Adresse muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="token_address")
        
        if not chain or not isinstance(chain, str) or not chain.strip():
            error_msg = "Chain muss ein nicht-leerer String sein"
            self.logger.error(error_msg)
            raise ValidationException(error_msg, field="chain")
        
        # Normalisiere Chain-Name (kleinschreibung)
        chain = chain.lower().strip()
        
        # Prüfe, ob der TokenAnalyzer initialisiert ist
        if not self.api_provider:
            error_msg = "API-Provider ist nicht initialisiert. Verwenden Sie den Analyzer innerhalb eines async-Kontext-Managers (async with)."
            self.logger.error(error_msg)
            raise CustomAnalysisException(error_msg)
    
        try:
            # Hole Token-Daten vom API-Provider
            token_data = await self._fetch_custom_token_data(token_address, chain)
            
            if not token_data:
                raise ValueError("Token data could not be retrieved")
            
            # Hole Wallet-Daten
            holders = await self._fetch_token_holders(token_address, chain)
            
            # Analysiere Wallets
            wallet_analyses = await self._analyze_wallets(token_data, holders)
            
            # Berechne Token-Score
            score_result = self._calculate_token_score(token_data, wallet_analyses)
            
            # Erstelle Analyse-Ergebnis
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
            
            # Speichere das Ergebnis im Cache
            if should_use_cache and self.cache:
                await self.cache.set(result, self.cache_ttl, cache_key)
                
            self.logger.info(f"Analyse für Token {token_address} auf Chain {chain} abgeschlossen")
            return result
        except ValueError as e:
            # Spezielle Behandlung für "Token data could not be retrieved" Fehler
            if "Token data could not be retrieved" in str(e):
                self.logger.error(f"Konnte Tokendaten nicht abrufen für {token_address} auf {chain}: {str(e)}")
                # Erstelle ein minimales Analyseergebnis, auch wenn keine Token-Daten abgerufen werden konnten
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
                    'score': 50.0,  # Neutraler Score
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
                
                # Speichere das minimale Ergebnis im Cache
                if should_use_cache and self.cache:
                    await self.cache.set(minimal_result, self.cache_ttl, cache_key)
                
                return minimal_result
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}") from e
        except (APIException, NotFoundException) as e:
            self.logger.error(f"Externer Fehler bei der Token-Analyse: {str(e)}")
            raise CustomAnalysisException(f"Analyse fehlgeschlagen: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unerwarteter Fehler bei der Token-Analyse: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Unerwarteter Fehler bei der Analyse: {str(e)}") from e
    
    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def _fetch_custom_token_data(self, token_address: str, chain: str) -> Optional[Token]:
        """Holt Token-Daten für verschiedene Chains mit Rate-Limit-Handling"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"token_data_{token_address}_{chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached token data for {token_address}")
                    return cached_result
            
            # Hole Token-Daten vom API-Provider - Ersetzt APIManager durch BaseAPIProvider
            price_data = await self.api_provider.get_token_price(token_address, chain)
            
            # PRÜFEN, OB price_data None IST
            if price_data is None:
                logger.warning(f"Keine Preisdaten für Token {token_address} auf {chain} verfügbar")
                return None
            
            # Erstelle Token-Objekt mit getattr für sicheren Attributzugriff
            token = Token(
                address=token_address,
                name=getattr(price_data, 'name', ''),  # Verwende getattr mit Standardwert
                symbol=getattr(price_data, 'symbol', ''),  # Verwende getattr mit Standardwert
                chain=chain,
                market_cap=getattr(price_data, 'market_cap', 0),
                volume_24h=getattr(price_data, 'volume_24h', 0),
                liquidity=0,  # Wird später berechnet
                holders_count=0,  # Wird später geholt
                contract_verified=False,  # Wird später geprüft
                creation_date=None,  # Wird später geholt
                token_score=0  # Wird später berechnet
            )
            
            # Zusätzliche Token-Informationen abrufen
            if chain in ['ethereum', 'bsc']:
                token = await self._fetch_evm_token_data(token)
            elif chain == 'solana':
                token = await self._fetch_solana_token_data(token)
            elif chain == 'sui':
                token = await self._fetch_sui_token_data(token)
            
            # Speichere das Ergebnis im Cache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching token data for {token_address} on {chain}: {e}")
        return None
    
    async def _fetch_evm_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für EVM-Chains"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"evm_token_data_{token.address}_{token.chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached EVM token data for {token.address}")
                    return cached_result
            
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
                if token.chain == 'ethereum' and self.ethereum_provider:
                    token.contract_verified = await self.ethereum_provider.is_contract_verified(token.address, 'ethereum')
                elif token.chain == 'bsc' and self.bsc_provider:
                    token.contract_verified = await self.bsc_provider.is_contract_verified(token.address, 'bsc')
            except Exception:
                pass
            
            # Hole Erstellungsdatum des Contracts
            try:
                if token.chain == 'ethereum' and self.ethereum_provider:
                    creation_tx = await self.ethereum_provider.get_contract_creation_tx(token.address, token.chain)
                elif token.chain == 'bsc' and self.bsc_provider:
                    creation_tx = await self.bsc_provider.get_contract_creation_tx(token.address, token.chain)
                
                if creation_tx and w3:
                    tx_receipt = w3.eth.get_transaction_receipt(creation_tx)
                    if tx_receipt:
                        block = w3.eth.get_block(tx_receipt.blockNumber)
                        token.creation_date = datetime.fromtimestamp(block.timestamp)
            except Exception:
                pass
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching EVM token data for {token.address}: {e}")
            return token
    
    async def _fetch_solana_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für Solana"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"solana_token_data_{token.address}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached Solana token data for {token.address}")
                    return cached_result
            
            if self.solana_provider:
                token_info = await self.solana_provider.get_token_info(token.address)
                
                if token_info:
                    token.name = token_info.get('name', '')
                    token.symbol = token_info.get('symbol', '')
                    token.creation_date = token_info.get('creation_date')
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching Solana token data for {token.address}: {e}")
            return token
    
    async def _fetch_sui_token_data(self, token: Token) -> Token:
        """Holt zusätzliche Token-Daten für Sui"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"sui_token_data_{token.address}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached Sui token data for {token.address}")
                    return cached_result
            
            if self.sui_provider:
                token_info = await self.sui_provider.get_token_info(token.address)
                
                if token_info:
                    token.name = token_info.get('name', '')
                    token.symbol = token_info.get('symbol', '')
                    token.creation_date = token_info.get('creation_date')
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(token, self.config.cache_ttl_seconds, cache_key)
            
            return token
        except Exception as e:
            logger.error(f"Error fetching Sui token data for {token.address}: {e}")
            return token
    
    async def _fetch_token_holders(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Holt Wallets, die in den letzten 6 Stunden mit dem Smart Contract interagiert haben"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"active_wallets_{token_address}_{chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Verwende gecachte aktive Wallet-Daten für {token_address} auf {chain}")
                    return cached_result
            
            # Wähle den richtigen Provider basierend auf der Blockchain
            if chain.lower() == 'ethereum':
                provider = self.ethereum_provider
            elif chain.lower() == 'bsc':
                provider = self.bsc_provider
            elif chain.lower() == 'solana':
                provider = self.solana_provider
            elif chain.lower() == 'sui':
                provider = self.sui_provider
            else:
                logger.error(f"Unsupported blockchain: {chain}")
                return []
            
            if not provider:
                logger.error(f"No provider available for blockchain: {chain}")
                return []
            
            # Hole die Transaktionen des Smart Contracts der letzten 6 Stunden
            logger.info(f"Rufe Transaktionen für {token_address} auf {chain} der letzten 6 Stunden ab")
            
            try:
                # Hole Transaktionen über den Provider
                transactions = await provider.get_contract_transactions(token_address, hours=6)
                
                if not transactions:
                    logger.warning(f"Keine Transaktionen für {token_address} auf {chain} in den letzten 6 Stunden gefunden")
                    
                    # Erweitere den Zeitraum auf 24 Stunden, wenn in 6 Stunden keine Transaktionen gefunden wurden
                    logger.info(f"Versuche, Transaktionen der letzten 24 Stunden für {token_address} zu finden")
                    transactions = await provider.get_contract_transactions(token_address, hours=24)
                    
                    if not transactions:
                        logger.warning(f"Keine Transaktionen für {token_address} auf {chain} in den letzten 24 Stunden gefunden")
                        
                        # Letzter Versuch: Hole aktuelle Top-Holder als Fallback
                        try:
                            logger.info(f"Versuche, Top-Holder als Fallback für {token_address} zu finden")
                            if hasattr(provider, 'get_token_holders'):
                                holders = await provider.get_token_holders(token_address, chain)
                                
                                if holders:
                                    # Konvertiere in das erwartete Format
                                    active_wallets = []
                                    total_supply = 0
                                    
                                    for holder in holders:
                                        try:
                                            # Je nach Provider-Format anpassen
                                            if 'TokenHolderAddress' in holder and 'TokenHolderQuantity' in holder:
                                                address = holder['TokenHolderAddress']
                                                balance = float(holder['TokenHolderQuantity'])
                                            elif 'address' in holder and 'balance' in holder:
                                                address = holder['address']
                                                balance = float(holder['balance'])
                                            else:
                                                # Unbekanntes Format, überspringen
                                                continue
                                            
                                            active_wallets.append({
                                                'address': address,
                                                'balance': balance,
                                                'percentage': 0,  # Wird später berechnet
                                                'is_top_holder': True  # Markiere als Fallback
                                            })
                                            total_supply += balance
                                        except (ValueError, KeyError) as e:
                                            logger.warning(f"Ungültiges Holder-Format: {holder}, Fehler: {e}")
                                            continue
                                    
                                    # Berechne die Prozentsätze
                                    for wallet in active_wallets:
                                        if total_supply > 0:
                                            wallet['percentage'] = (wallet['balance'] / total_supply) * 100
                                        else:
                                            wallet['percentage'] = 0
                                    
                                    # Sortiere die Wallets nach Balance (absteigend)
                                    active_wallets.sort(key=lambda x: x['balance'], reverse=True)
                                    
                                    # Begrenze die Anzahl der Wallets
                                    active_wallets = active_wallets[:self.config.max_holders_to_analyze]
                                    
                                    # Speichere das Ergebnis im Cache
                                    if self.cache:
                                        await self.cache.set(active_wallets, self.config.cache_ttl_seconds, cache_key)
                                    
                                    logger.info(f"Top-Holder (Fallback) für {token_address}: {len(active_wallets)} Wallets gefunden")
                                    return active_wallets
                        except Exception as fallback_error:
                            logger.error(f"Fallback-Methode fehlgeschlagen: {fallback_error}")
                        
                        return []
                
                # Extrahiere die einzigartigen Wallet-Adressen aus den Transaktionen
                wallet_addresses = set()
                for tx in transactions:
                    # Füge Absender und Empfänger hinzu
                    if 'from' in tx and tx['from']:
                        wallet_addresses.add(tx['from'])
                    if 'to' in tx and tx['to']:
                        wallet_addresses.add(tx['to'])
                    # Bei Token-Transaktionen könnten auch andere Felder relevant sein
                    if 'contract_address' in tx and tx['contract_address']:
                        wallet_addresses.add(tx['contract_address'])
                
                logger.info(f"Gefundene aktive Wallet-Adressen: {len(wallet_addresses)}")
                
                # Für jede Wallet-Adresse, hole den aktuellen Token-Bestand
                active_wallets = []
                total_supply = 0
                
                for address in wallet_addresses:
                    try:
                        # Hole den Token-Bestand für diese Adresse
                        balance = await provider.get_token_balance(token_address, address)
                        if balance > 0:
                            active_wallets.append({
                                'address': address,
                                'balance': balance,
                                'percentage': 0,  # Wird später berechnet
                                'last_interaction': None  # Könnte später aus Transaktionen extrahiert werden
                            })
                            total_supply += balance
                    except Exception as e:
                        logger.warning(f"Fehler beim Abrufen des Token-Bestands für {address}: {e}")
                        continue
                
                # Berechne die Prozentsätze
                for wallet in active_wallets:
                    if total_supply > 0:
                        wallet['percentage'] = (wallet['balance'] / total_supply) * 100
                    else:
                        wallet['percentage'] = 0
                
                # Sortiere die Wallets nach Balance (absteigend)
                active_wallets.sort(key=lambda x: x['balance'], reverse=True)
                
                # Begrenze die Anzahl der Wallets
                active_wallets = active_wallets[:self.config.max_holders_to_analyze]
                
                # Speichere das Ergebnis im Cache
                if self.cache:
                    await self.cache.set(active_wallets, self.config.cache_ttl_seconds, cache_key)
                
                logger.info(f"Aktive Wallets für {token_address}: {len(active_wallets)} Wallets gefunden")
                return active_wallets
                
            except Exception as e:
                logger.error(f"Fehler beim Abrufen der aktiven Wallets für {token_address} auf {chain}: {e}")
                return []
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Token-Holder für {token_address} auf {chain}: {e}")
            return []
    
    async def _analyze_wallets(self, token_data: Token, holders: List[Dict[str, Any]]) -> List[WalletAnalysis]:
        """Analysiert die Wallets der Token-Holder"""
        wallet_analyses = []
        
        # Berechne die Gesamtmenge der Token
        total_supply = sum(float(h.get('balance', 0)) for h in holders)
        
        # Begrenze die Anzahl der zu analysierenden Wallets
        holders_to_analyze = holders[:self.config.max_holders_to_analyze]
        
        for holder in holders_to_analyze:
            try:
                # Passe das Datenformat an - je nachdem, ob es aktive Wallets oder Top-Holder sind
                if 'balance' in holder:
                    balance = float(holder.get('balance', 0))
                    wallet_address = holder.get('address', '')
                elif 'TokenHolderQuantity' in holder:
                    balance = float(holder.get('TokenHolderQuantity', 0))
                    wallet_address = holder.get('TokenHolderAddress', '')
                else:
                    logger.warning(f"Unbekanntes Holder-Format: {holder}")
                    continue
                    
                percentage = (balance / total_supply) * 100 if total_supply > 0 else 0
                
                # Cache-Schlüssel für diese Anfrage
                cache_key = f"wallet_analysis_{wallet_address}_{token_data.address}"
                
                # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
                if self.cache:
                    cached_result = await self.cache.get(cache_key)
                    if cached_result:
                        logger.info(f"Returning cached wallet analysis for {wallet_address}")
                        wallet_analyses.append(cached_result)
                        continue
                
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
                
                # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
                if self.cache:
                    await self.cache.set(wallet_analysis, self.config.cache_ttl_seconds, cache_key)
                    
            except Exception as e:
                logger.error(f"Error analyzing wallet {holder.get('address', holder.get('TokenHolderAddress', 'Unknown'))}: {e}")
                continue
        
        return wallet_analyses
    
    async def _fetch_wallet_transaction_data(self, wallet_address: str, chain: str) -> Dict[str, Any]:
        """Holt Transaktionsdaten für eine Wallet"""
        try:
            # Cache-Schlüssel für diese Anfrage
            cache_key = f"wallet_transactions_{wallet_address}_{chain}"
            
            # Prüfe, ob die Daten im Cache vorhanden sind - angepasst für AnalysisCache
            if self.cache:
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Returning cached wallet transactions for {wallet_address}")
                    return cached_result
            
            transaction_data = {}
            
            if chain in ['ethereum', 'bsc']:
                if chain == 'ethereum' and self.ethereum_provider:
                    transaction_data = await self.ethereum_provider.get_wallet_transactions(wallet_address, chain)
                elif chain == 'bsc' and self.bsc_provider:
                    transaction_data = await self.bsc_provider.get_wallet_transactions(wallet_address, chain)
            elif chain == 'solana' and self.solana_provider:
                transaction_data = await self.solana_provider.get_wallet_transactions(wallet_address)
            elif chain == 'sui' and self.sui_provider:
                transaction_data = await self.sui_provider.get_wallet_transactions(wallet_address)
            
            # Speichere das Ergebnis im Cache - angepasst für AnalysisCache
            if self.cache:
                await self.cache.set(transaction_data, self.config.cache_ttl_seconds, cache_key)
            
            return transaction_data
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
        
        # Wallet-Verteilungsanalyse - nur wenn Wallet-Analysen vorhanden sind
        if wallet_analyses:
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
        else:
            # Wenn keine Wallet-Analysen vorhanden sind, füge ein Risikoflag hinzu
            score -= 25  # Deutlicher Abzug für fehlende Wallet-Daten
            risk_flags.append("no_wallet_data")
            
            # Setze Standardwerte für die Metriken
            whale_percentage = 0
            dev_percentage = 0
            rugpull_suspects = 0
            gini = 0
        
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
    
    def _perform_extended_risk_assessment(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """Führt eine erweiterte Risikobewertung durch"""
        risk_factors = []
        overall_risk = 50  # Neutraler Startwert
        
        # Basis-Score aus der Analyse
        base_score = analysis_result.get('score', 50)
        
        # Umrechnung des Scores in Risiko (invers)
        base_risk = 100 - base_score
        overall_risk = (overall_risk + base_risk) / 2
        
        # Risikofaktoren aus den Metriken
        metrics = analysis_result.get('metrics', {})
        
        # Whale-Konzentration
        whale_percentage = metrics.get('whale_percentage', 0)
        if whale_percentage > 50:
            risk_factors.append({
                'factor': 'high_whale_concentration',
                'description': f'Hohe Whale-Konzentration ({whale_percentage:.1f}%)',
                'impact': 30
            })
            overall_risk = min(100, overall_risk + 30)
        elif whale_percentage > 20:
            risk_factors.append({
                'factor': 'moderate_whale_concentration',
                'description': f'Moderate Whale-Konzentration ({whale_percentage:.1f}%)',
                'impact': 15
            })
            overall_risk = min(100, overall_risk + 15)
        
        # Dev-Konzentration
        dev_percentage = metrics.get('dev_percentage', 0)
        if dev_percentage > 20:
            risk_factors.append({
                'factor': 'high_dev_concentration',
                'description': f'Hohe Entwickler-Konzentration ({dev_percentage:.1f}%)',
                'impact': 25
            })
            overall_risk = min(100, overall_risk + 25)
        elif dev_percentage > 10:
            risk_factors.append({
                'factor': 'moderate_dev_concentration',
                'description': f'Moderate Entwickler-Konzentration ({dev_percentage:.1f}%)',
                'impact': 12
            })
            overall_risk = min(100, overall_risk + 12)
        
        # Rugpull-Verdacht
        rugpull_suspects = metrics.get('rugpull_suspects', 0)
        if rugpull_suspects > 0:
            risk_factors.append({
                'factor': 'rugpull_suspects',
                'description': f'{rugpull_suspects} verdächtige Wallets entdeckt',
                'impact': rugpull_suspects * 20
            })
            overall_risk = min(100, overall_risk + rugpull_suspects * 20)
        
        # Gini-Koeffizient
        gini = metrics.get('gini_coefficient', 0)
        if gini > 0.8:
            risk_factors.append({
                'factor': 'very_uneven_distribution',
                'description': f'Sehr ungleiche Token-Verteilung (Gini: {gini:.2f})',
                'impact': 20
            })
            overall_risk = min(100, overall_risk + 20)
        elif gini > 0.6:
            risk_factors.append({
                'factor': 'uneven_distribution',
                'description': f'Ungleiche Token-Verteilung (Gini: {gini:.2f})',
                'impact': 10
            })
            overall_risk = min(100, overall_risk + 10)
        
        # Risikoflags aus der Basisanalyse
        risk_flags = analysis_result.get('risk_flags', [])
        
        # Marktkapitalisierung
        if 'very_low_market_cap' in risk_flags:
            risk_factors.append({
                'factor': 'very_low_market_cap',
                'description': 'Sehr geringe Marktkapitalisierung',
                'impact': 30
            })
            overall_risk = min(100, overall_risk + 30)
        elif 'low_market_cap' in risk_flags:
            risk_factors.append({
                'factor': 'low_market_cap',
                'description': 'Geringe Marktkapitalisierung',
                'impact': 20
            })
            overall_risk = min(100, overall_risk + 20)
        
        # Liquidität
        if 'low_liquidity' in risk_flags:
            risk_factors.append({
                'factor': 'low_liquidity',
                'description': 'Geringe Liquidität',
                'impact': 25
            })
            overall_risk = min(100, overall_risk + 25)
        
        # Verifizierung
        if 'unverified_contract' in risk_flags:
            risk_factors.append({
                'factor': 'unverified_contract',
                'description': 'Nicht verifizierter Smart Contract',
                'impact': 15
            })
            overall_risk = min(100, overall_risk + 15)
        
        # Bestimme die Risikostufe
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
        
        # Generiere eine Empfehlung
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
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Provider-Sessions."""
        close_tasks = []
        
        # Schließe API-Provider
        if self.api_provider:
            close_tasks.append(self._safe_close_api_provider())
        
        # Schließe Blockchain-Provider
        providers = [
            self.ethereum_provider,
            self.bsc_provider,
            self.solana_provider,
            self.sui_provider
        ]
        
        for provider in providers:
            if provider:
                close_tasks.append(self._safe_close_provider(provider, provider.__class__.__name__))
        
        # Führe alle Schließvorgänge parallel ausführen
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        logger.info("TokenAnalyzer resources closed successfully")

    async def _safe_close_provider(self, provider, provider_name):
        """Sicheres Schließen eines Providers"""
        try:
            if hasattr(provider, '__aexit__'):
                await provider.__aexit__(None, None, None)
            if hasattr(provider, 'close'):
                await provider.close()
        except Exception as e:
            logger.error(f"Error closing {provider_name}: {str(e)}")
