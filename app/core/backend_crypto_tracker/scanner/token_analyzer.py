"""
Token Analyzer - FINAL VERSION
Fully integrated with new 3-stage wallet classification system
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from functools import wraps
import random

from web3 import Web3

# Core imports
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import (
    ValidationException, CustomAnalysisException
)
from app.core.backend_crypto_tracker.config.scanner_config import scanner_config
from app.core.backend_crypto_tracker.utils.cache import AnalysisCache
from app.core.backend_crypto_tracker.processor.database.models.token import Token
from app.core.backend_crypto_tracker.processor.database.models.wallet import WalletAnalysis, WalletTypeEnum
from app.core.backend_crypto_tracker.utils.json_helpers import sanitize_float
from app.core.backend_crypto_tracker.utils.token_data_resolver import TokenDataResolver

# Import blockchain functions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_token_holders import get_token_holders as ethereum_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_address_transactions import get_address_transactions as ethereum_get_transactions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_token_holders import get_token_holders as solana_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_transaction_details import get_transaction_details as solana_get_transaction
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_token_holders import get_token_holders as sui_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_transaction import get_transaction as sui_get_transaction

# Import wallet classification system
from app.core.backend_crypto_tracker.scanner.wallet_classifier.core.stages_blockchain import Stage1_RawMetrics
from app.core.backend_crypto_tracker.scanner.wallet_classifier.core.stages import Stage2_DerivedMetrics, Stage3_ContextAnalysis
from app.core.backend_crypto_tracker.scanner.wallet_classifier.classes.dust_sweeper import DustSweeperAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifier.classes.hodler import HodlerAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifier.classes.mixer import MixerAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifier.classes.trader import TraderAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifier.classes.whale import WhaleAnalyzer

logger = get_logger(__name__)


# Configuration dataclass
from dataclasses import dataclass

@dataclass
class TokenAnalysisConfig:
    max_tokens_per_scan: int = 100
    max_market_cap: float = 5_000_000
    min_liquidity_threshold: float = 50_000
    whale_threshold_percentage: float = 5.0
    dev_threshold_percentage: float = 2.0
    max_holders_to_analyze: int = 100
    request_delay_seconds: float = 1.0
    enable_cache: bool = True
    cache_ttl_seconds: int = 300
    btc_price: float = 50000  # For USD conversion


def retry_with_backoff(max_retries=3, base_delay=1, max_delay=60):
    """Decorator for retry with exponential backoff"""
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
    Token Analyzer with integrated 3-stage wallet classification system
    """
    
    def __init__(self, config: TokenAnalysisConfig = None):
        self.config = config or TokenAnalysisConfig()
        self.logger = get_logger(__name__)
        
        # Cache
        self.enable_cache = self.config.enable_cache
        self.cache_ttl = self.config.cache_ttl_seconds
        self.cache = AnalysisCache(max_size=1000, default_ttl=self.cache_ttl) if self.enable_cache else None
        
        # Web3 connections
        self.ethereum_rpc = scanner_config.rpc_config.ethereum_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
        self.bsc_rpc = scanner_config.rpc_config.bsc_rpc or "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
        self.w3_eth = None
        self.w3_bsc = None
        
        # Known addresses
        self.known_contracts = scanner_config.rpc_config.known_contracts
        self.cex_wallets = scanner_config.rpc_config.cex_wallets
        
        # Token resolver
        self.token_resolver = TokenDataResolver()
        
        # Wallet classification system
        self.wallet_analyzers = {
            'dust_sweeper': DustSweeperAnalyzer(),
            'hodler': HodlerAnalyzer(),
            'mixer': MixerAnalyzer(),
            'trader': TraderAnalyzer(),
            'whale': WhaleAnalyzer()
        }
        
        self._initialized = False
        self.logger.info("TokenAnalyzer initialized with 3-stage wallet classification")

    async def __aenter__(self):
        if not self._initialized:
            self.logger.debug("Initializing Web3 connections")
            
            if self.ethereum_rpc:
                self.w3_eth = Web3(Web3.HTTPProvider(self.ethereum_rpc))
            
            if self.bsc_rpc:
                self.w3_bsc = Web3(Web3.HTTPProvider(self.bsc_rpc))
            
            self._initialized = True
            self.logger.info("TokenAnalyzer successfully initialized")
        
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._initialized:
            self.w3_eth = None
            self.w3_bsc = None
            self._initialized = False
            self.logger.debug("TokenAnalyzer resources closed")

    @retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
    async def analyze_custom_token(self, token_address: str, chain: str, use_cache: Optional[bool] = None) -> Dict[str, Any]:
        """Main token analysis method"""
        self.logger.info(f"Starting analysis for token {token_address} on {chain}")
        
        # Cache management
        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        cache_key = f"token_analysis_{token_address}_{chain}"
        
        if should_use_cache and self.cache:
            cached_result = await self.cache.get(cache_key)
            if cached_result:
                self.logger.info(f"Returning cached analysis for {token_address}")
                return cached_result
        
        # Validation
        if not token_address or not isinstance(token_address, str):
            raise ValidationException("Token address must be a non-empty string", field="token_address")
        
        if not chain or not isinstance(chain, str):
            raise ValidationException("Chain must be a non-empty string", field="chain")
        
        chain = chain.lower().strip()
        
        try:
            # Step 1: Get token data
            token_data = await self.token_resolver.resolve_token_data(token_address, chain)
            
            if not token_data or token_data.name == "Unknown":
                self.logger.warning(f"Token {token_address} not found on {chain}")
                raise ValueError("Token data could not be retrieved")
            
            # Step 2: Get holders
            holders = await self._get_token_holders(token_address, chain)
            
            # Step 3: Analyze wallets with 3-stage classification
            wallet_analyses = await self._analyze_wallets_3stage(token_data, holders, chain)
            
            # Step 4: Calculate token score
            score_result = self._calculate_token_score(token_data, wallet_analyses)
            
            # Step 5: Build result
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
                    'dust_sweepers': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DUST_SWEEPER]),
                    'hodlers': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.HODLER]),
                    'mixers': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.MIXER]),
                    'traders': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.TRADER]),
                    'whales': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE]),
                    'top_holders': [
                        {
                            'address': w.wallet_address,
                            'balance': w.balance,
                            'percentage': w.percentage_of_supply,
                            'type': w.wallet_type.value,
                            'risk_score': w.risk_score
                        }
                        for w in wallet_analyses[:10]
                    ]
                }
            }
            
            # Cache
            if should_use_cache and self.cache:
                await self.cache.set(result, self.cache_ttl, cache_key)
            
            self.logger.info(f"Analysis for {token_address} completed successfully")
            return result
            
        except ValueError as e:
            if "Token data could not be retrieved" in str(e):
                return self._create_minimal_result(token_address, chain, should_use_cache, cache_key)
            raise CustomAnalysisException(f"Analysis failed: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error in token analysis: {str(e)}", exc_info=True)
            raise CustomAnalysisException(f"Unexpected error: {str(e)}") from e

    async def _get_token_holders(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Get token holders chain-specifically"""
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
            self.logger.error(f"Error fetching holders: {e}")
            return []

    async def _analyze_wallets_3stage(
        self, 
        token_data: Token, 
        holders: List[Dict[str, Any]], 
        chain: str
    ) -> List[WalletAnalysis]:
        """
        НОВАЯ МЕТОД: 3-Stage Wallet Analysis Pipeline
        Stage 1: Raw Metrics -> Stage 2: Derived Metrics -> Stage 3: Context -> Classification
        """
        wallet_analyses = []
        
        total_supply = sum(float(h.get('balance', 0)) for h in holders)
        holders_to_analyze = holders[:self.config.max_holders_to_analyze]
        
        for holder in holders_to_analyze:
            try:
                balance = float(holder.get('balance', 0))
                wallet_address = holder.get('address', '')
                percentage = (balance / total_supply) * 100 if total_supply > 0 else 0
                
                # Cache check
                cache_key = f"wallet_3stage_{wallet_address}_{token_data.address}"
                if self.cache:
                    cached = await self.cache.get(cache_key)
                    if cached:
                        wallet_analyses.append(cached)
                        continue
                
                # Get blockchain data for this wallet
                blockchain_data = await self._get_wallet_blockchain_data(wallet_address, chain, token_data.address)
                
                # === 3-STAGE PIPELINE ===
                # Stage 1: Raw Metrics
                stage1_executor = Stage1_RawMetrics()
                raw_metrics = stage1_executor.execute(
                    blockchain_data,
                    config={'chain': chain},
                    blockchain=chain
                )
                
                # Stage 2: Derived Metrics
                stage2_executor = Stage2_DerivedMetrics()
                derived_metrics = stage2_executor.execute(
                    raw_metrics,
                    config={'btc_price': self.config.btc_price}
                )
                
                # Stage 3: Context (ohne DB erstmal)
                stage3_executor = Stage3_ContextAnalysis()
                context_metrics = stage3_executor.execute(
                    derived_metrics,
                    wallet_address,
                    context_db=None  # TODO: Add context DB later
                )
                
                # Combine all metrics
                all_metrics = {**raw_metrics, **derived_metrics, **context_metrics}
                
                # Classify wallet with all analyzers
                wallet_type, classification_score = self._classify_wallet_multistage(all_metrics)
                
                # Create WalletAnalysis
                wallet_analysis = WalletAnalysis(
                    wallet_address=wallet_address,
                    wallet_type=wallet_type,
                    balance=balance,
                    percentage_of_supply=percentage,
                    transaction_count=raw_metrics.get('tx_count', 0),
                    first_transaction=datetime.fromtimestamp(raw_metrics.get('first_seen', 0)) if raw_metrics.get('first_seen') else None,
                    last_transaction=datetime.fromtimestamp(raw_metrics.get('last_seen', 0)) if raw_metrics.get('last_seen') else None,
                    risk_score=classification_score * 100  # Convert to 0-100 scale
                )
                
                wallet_analyses.append(wallet_analysis)
                
                # Cache
                if self.cache:
                    await self.cache.set(wallet_analysis, self.cache_ttl, cache_key)
                
            except Exception as e:
                self.logger.error(f"Error analyzing wallet {holder.get('address')}: {e}")
                continue
        
        return wallet_analyses

    async def _get_wallet_blockchain_data(self, wallet_address: str, chain: str, token_address: str) -> Dict[str, Any]:
        """Get blockchain transaction data for wallet"""
        try:
            if chain in ['ethereum', 'bsc']:
                txs = await ethereum_get_transactions(wallet_address, token_address)
            elif chain == 'solana':
                txs = await solana_get_transaction(wallet_address)
            elif chain == 'sui':
                txs = await sui_get_transaction(wallet_address)
            else:
                return {'txs': [], 'balance': 0, 'address': wallet_address}
            
            # Format for Stage 1
            return {
                'address': wallet_address,
                'txs': txs or [],
                'balance': 0,  # Will be calculated from txs
                'inputs': [],   # Will be extracted from txs
                'outputs': []   # Will be extracted from txs
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching wallet data: {e}")
            return {'txs': [], 'balance': 0, 'address': wallet_address}

    def _classify_wallet_multistage(self, all_metrics: Dict[str, Any]) -> tuple:
        """
        Classify wallet using all 5 analyzers and return best match
        Returns: (WalletTypeEnum, confidence_score)
        """
        scores = {}
        
        # Run all classifiers
        for name, analyzer in self.wallet_analyzers.items():
            try:
                score = analyzer.compute_score(all_metrics)
                scores[name] = score
            except Exception as e:
                self.logger.warning(f"Error in {name} analyzer: {e}")
                scores[name] = 0.0
        
        # Find best match
        best_class = max(scores, key=scores.get)
        best_score = scores[best_class]
        
        # Map to WalletTypeEnum
        class_mapping = {
            'dust_sweeper': WalletTypeEnum.DUST_SWEEPER,
            'hodler': WalletTypeEnum.HODLER,
            'mixer': WalletTypeEnum.MIXER,
            'trader': WalletTypeEnum.TRADER,
            'whale': WalletTypeEnum.WHALE
        }
        
        # Check if score meets threshold
        analyzer = self.wallet_analyzers[best_class]
        if best_score >= analyzer.THRESHOLD:
            return class_mapping[best_class], best_score
        
        return WalletTypeEnum.UNKNOWN, best_score

    def _calculate_token_score(self, token_data: Token, wallet_analyses: List[WalletAnalysis]) -> Dict[str, Any]:
        """Calculate token risk score"""
        score = 100.0
        risk_flags = []
        
        # Market cap scoring
        market_cap = sanitize_float(token_data.market_cap)
        if market_cap < 100000:
            score -= 30
            risk_flags.append("very_low_market_cap")
        elif market_cap < 500000:
            score -= 20
            risk_flags.append("low_market_cap")
        
        # Liquidity scoring
        liquidity = sanitize_float(token_data.liquidity)
        if liquidity < self.config.min_liquidity_threshold:
            score -= 25
            risk_flags.append("low_liquidity")
        
        # Contract verification
        if not token_data.contract_verified:
            score -= 15
            risk_flags.append("unverified_contract")
        
        # Wallet distribution analysis
        if wallet_analyses:
            whale_percentage = sum(w.percentage_of_supply for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE)
            mixer_count = len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.MIXER])
            
            if whale_percentage > 50:
                score -= 40
                risk_flags.append("high_whale_concentration")
            
            if mixer_count > 2:
                score -= 30
                risk_flags.append("mixer_activity")
        
        score = max(0.0, min(100.0, score))
        
        metrics = {
            'total_holders_analyzed': len(wallet_analyses),
            'whales': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.WHALE]),
            'hodlers': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.HODLER]),
            'traders': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.TRADER]),
            'mixers': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.MIXER]),
            'dust_sweepers': len([w for w in wallet_analyses if w.wallet_type == WalletTypeEnum.DUST_SWEEPER])
        }
        
        return {
            'total_score': sanitize_float(score),
            'metrics': metrics,
            'risk_flags': risk_flags
        }

    def _create_minimal_result(self, token_address: str, chain: str, should_cache: bool, cache_key: str) -> Dict[str, Any]:
        """Create minimal result for unknown tokens"""
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
            'metrics': {'total_holders_analyzed': 0},
            'risk_flags': ["limited_data"],
            'wallet_analysis': {'total_wallets': 0, 'top_holders': []}
        }
        
        if should_cache and self.cache:
            asyncio.create_task(self.cache.set(minimal_result, self.cache_ttl, cache_key))
        
        return minimal_result

    async def close(self):
        """Close resources"""
        await self.__aexit__(None, None, None)
