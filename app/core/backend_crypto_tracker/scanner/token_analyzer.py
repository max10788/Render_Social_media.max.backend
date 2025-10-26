"""
Token Analyzer - FINAL VERSION with Frontend-Compatible Data Structure
✅ Fully integrated with new 3-stage wallet classification system
✅ Returns data in exact format expected by frontend
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
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_token_holders import execute_get_token_holders as ethereum_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_address_transactions import execute_get_address_transactions as ethereum_get_transactions
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_token_holders import execute_get_token_holders as solana_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_transaction_details import execute_get_transaction_details as solana_get_transaction
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_token_holders import execute_get_token_holders as sui_get_holders
from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_transaction import execute_get_transaction_details as sui_get_transaction

# Import wallet classification system
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.stages_blockchain import Stage1_RawMetrics
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.stages import Stage2_DerivedMetrics, Stage3_ContextAnalysis
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.classes.dust_sweeper import DustSweeperAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.classes.hodler import HodlerAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.classes.mixer import MixerAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.classes.trader import TraderAnalyzer
from app.core.backend_crypto_tracker.scanner.wallet_classifierr.classes.whale import WhaleAnalyzer

# ✅ NEW IMPORT: Wallet Data Transformer
from app.core.backend_crypto_tracker.scanner.wallet_data_transformer import WalletDataTransformer

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
    max_holders_to_analyze: int = 15
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
    ✅ Returns frontend-compatible wallet data structure
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
        
        # ✅ NEW: Wallet Data Transformer
        self.wallet_transformer = WalletDataTransformer()
        
        self._initialized = False
        self.logger.info("TokenAnalyzer initialized with 3-stage wallet classification")
        self.logger.info(f"⚠️ Max holders to classify: {self.config.max_holders_to_analyze}")

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
    async def analyze_custom_token(
        self, 
        token_address: str, 
        chain: str, 
        use_cache: Optional[bool] = None,
        wallet_source: str = "top_holders",
        recent_hours: Optional[int] = 3
    ) -> Dict[str, Any]:
        """
        Main token analysis method
        ✅ UPDATED: Returns frontend-compatible structure
        """
        self.logger.info(f"Starting analysis for token {token_address} on {chain}")
        self.logger.info(f"Wallet source: {wallet_source}, Recent hours: {recent_hours if wallet_source == 'recent_traders' else 'N/A'}")
        
        # Cache management
        should_use_cache = use_cache if use_cache is not None else self.enable_cache
        cache_key = f"token_analysis_{token_address}_{chain}_{wallet_source}_{recent_hours}"
        
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
            
            # Step 2: Get wallets based on source
            traders_data = None
            if wallet_source == "recent_traders":
                from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_recent_traders import execute_get_recent_traders
                
                self.logger.info(f"🔍 Getting recent traders (last {recent_hours}h)...")
                traders_data = await execute_get_recent_traders(token_address, chain, recent_hours)
                wallets = traders_data.get('traders', [])
                
                self.logger.info(f"✅ Found {len(wallets)} recent traders")
                
                holders = [
                    {
                        'address': w['address'],
                        'balance': w.get('total_bought', 0) - w.get('total_sold', 0),
                        'percentage': 0,
                        'tx_count': w.get('tx_count', 0),
                        'first_tx': w.get('first_tx'),
                        'last_tx': w.get('last_tx')
                    }
                    for w in wallets[:200]
                ]
            else:
                self.logger.info(f"🔍 Getting top token holders...")
                holders = await self._get_token_holders(token_address, chain)
            
            # Step 3: Analyze wallets - ✅ NEW METHOD
            wallet_results = await self._analyze_wallets(
                holders=holders,
                chain=chain,
                token_address=token_address,
                wallet_source=wallet_source,
                traders_data=traders_data
            )
            
            # Step 4: Calculate token score (use classified wallets only)
            # Convert back to WalletAnalysis format for scoring
            classified_wallet_objs = []
            for w in wallet_results['classified']:
                wallet_type = WalletTypeEnum[w['wallet_type'].upper()] if w['wallet_type'] != 'unclassified' else WalletTypeEnum.UNKNOWN
                wallet_obj = WalletAnalysis(
                    wallet_address=w['wallet_address'],
                    wallet_type=wallet_type,
                    balance=w['balance'],
                    percentage_of_supply=w['percentage_of_supply'],
                    transaction_count=w['transaction_count'],
                    risk_score=w['risk_score']
                )
                classified_wallet_objs.append(wallet_obj)
            
            score_result = self._calculate_token_score(token_data, classified_wallet_objs)
            
            # Step 5: Build frontend-compatible result
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
                'wallet_analysis': wallet_results  # ✅ This now contains classified/unclassified in correct format
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
                holders = await ethereum_get_holders(token_address, chain)
            elif chain == 'solana':
                holders = await solana_get_holders(token_address, chain)
            elif chain == 'sui':
                holders = await sui_get_holders(token_address, chain)
            else:
                self.logger.warning(f"Unsupported chain: {chain}")
                return []
            
            self.logger.debug(f"Found {len(holders) if holders else 0} holders")
            return holders or []
            
        except Exception as e:
            self.logger.error(f"Error fetching holders: {e}")
            return []

    async def _analyze_wallets(
        self,
        holders: List[Dict[str, Any]],
        chain: str,
        token_address: str,
        wallet_source: str = "top_holders",
        traders_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        ✅ COMPLETELY REWRITTEN: Analyze wallets and return frontend-compatible format
        
        Returns:
            {
                'classified': [Frontend-ready wallet objects with all fields],
                'unclassified': [Frontend-ready wallet objects with basic fields],
                'total': int
            }
        """
        if not holders:
            return {'classified': [], 'unclassified': [], 'total': 0}
        
        self.logger.info(f"Analyzing {len(holders)} wallets (source: {wallet_source})")
        
        # Calculate percentages
        total_supply = sum(float(h.get('balance', 0)) for h in holders)
        for holder in holders:
            balance = float(holder.get('balance', 0))
            holder['percentage'] = (balance / total_supply * 100) if total_supply > 0 else 0
        
        # ✅ Split: Top 30 for classification, rest unclassified
        max_to_classify = 30
        holders_to_classify = holders[:max_to_classify]
        holders_unclassified = holders[max_to_classify:]
        
        self.logger.info(f"Will classify: {len(holders_to_classify)}, Will skip: {len(holders_unclassified)}")
        
        # Classification results storage
        classification_results = {}
        
        # ✅ Classify Top 50 with 3-Stage Pipeline
        for idx, holder in enumerate(holders_to_classify, 1):
            wallet_address = holder.get('address', '').lower()
            
            try:
                self.logger.info(f"Classifying wallet {idx}/{len(holders_to_classify)}: {wallet_address}")
                
                # Get blockchain data with reduced limit
                blockchain_data = await self._get_wallet_blockchain_data(
                    wallet_address, 
                    chain, 
                    token_address,
                    limit=10
                )
                
                # Stage 1: Raw metrics
                stage1 = Stage1_RawMetrics()
                raw_metrics = stage1.execute(blockchain_data, config={})
                
                # Stage 2: Derived metrics
                stage2 = Stage2_DerivedMetrics()
                derived_metrics = stage2.execute(raw_metrics, config={})
                
                # Stage 3: Context analysis
                stage3 = Stage3_ContextAnalysis()
                context_metrics = stage3.execute(derived_metrics, wallet_address, context_db=None)
                
                # Combine all metrics
                all_metrics = {**raw_metrics, **derived_metrics, **context_metrics}
                
                # Classify wallet
                wallet_type, confidence_score = self._classify_wallet_multistage(all_metrics)
                
                # ✅ DEBUG: Log scores for debugging
                self.logger.debug(f"Wallet {wallet_address} metrics sample:")
                self.logger.debug(f"  - tx_count: {all_metrics.get('tx_count')}")
                self.logger.debug(f"  - total_value_usd: {all_metrics.get('total_value_usd')}")
                self.logger.debug(f"  - tx_per_month: {all_metrics.get('tx_per_month')}")
                self.logger.debug(f"  - holding_period_days: {all_metrics.get('holding_period_days')}")
                
                # Calculate risk score
                risk_score, risk_flags = self._calculate_wallet_risk(all_metrics, wallet_type)
                
                # Store classification result
                classification_results[wallet_address] = {
                    'wallet_type': wallet_type.value,
                    'confidence_score': confidence_score,
                    'risk_score': risk_score,
                    'risk_flags': risk_flags,
                    'metrics': all_metrics,
                    'classified': True
                }
                
                self.logger.info(f"✅ {wallet_address}: {wallet_type.value} (confidence: {confidence_score:.2f})")
                
                # Rate limiting
                await asyncio.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error classifying wallet {wallet_address}: {e}")
                classification_results[wallet_address] = {
                    'wallet_type': 'unknown',
                    'confidence_score': 0.0,
                    'risk_score': 0,
                    'risk_flags': ['classification_error'],
                    'classified': False
                }
        
        # ✅ Transform wallets to frontend format using WalletDataTransformer
        classified_wallets = []
        unclassified_wallets = []
        
        # Process classified wallets
        for holder in holders_to_classify:
            wallet_addr = holder.get('address', '').lower()
            classification = classification_results.get(wallet_addr)
            
            if classification and classification.get('classified'):
                # Prepare wallet data
                wallet_data = self._prepare_wallet_data(holder, traders_data)
                
                # Transform to frontend format
                transformed = self.wallet_transformer.transform_classified_wallet(
                    wallet_data=wallet_data,
                    token_address=token_address,
                    chain=chain,
                    classification_result=classification
                )
                classified_wallets.append(transformed)
        
        # Process unclassified wallets
        for holder in holders_unclassified:
            wallet_data = self._prepare_wallet_data(holder, traders_data)
            
            # Transform to frontend format (unclassified)
            transformed = self.wallet_transformer.transform_unclassified_wallet(
                wallet_data=wallet_data,
                token_address=token_address,
                chain=chain
            )
            unclassified_wallets.append(transformed)
        
        self.logger.info(f"✅ Classified: {len(classified_wallets)}, Unclassified: {len(unclassified_wallets)}")
        
        return {
            'classified': classified_wallets,
            'unclassified': unclassified_wallets,
            'total': len(classified_wallets) + len(unclassified_wallets)
        }

    def _prepare_wallet_data(
        self, 
        holder: Dict[str, Any], 
        traders_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Prepare wallet data for transformation.
        Combines holder data with trader data if available.
        """
        wallet_address = holder.get('address', '')
        
        wallet_data = {
            'address': wallet_address,
            'balance': holder.get('balance', 0),
            'percentage_of_supply': holder.get('percentage', 0),
            'transaction_count': holder.get('tx_count', 0),
            'first_transaction': holder.get('first_tx'),
            'last_transaction': holder.get('last_tx')
        }
        
        # Add trader-specific data if available
        if traders_data:
            trader_info = next(
                (t for t in traders_data.get('traders', []) 
                 if t['address'].lower() == wallet_address.lower()), 
                None
            )
            if trader_info:
                wallet_data.update({
                    'buy_count': trader_info.get('buy_count', 0),
                    'sell_count': trader_info.get('sell_count', 0),
                    'total_bought': trader_info.get('total_bought', 0),
                    'total_sold': trader_info.get('total_sold', 0),
                    'tx_count': trader_info.get('tx_count', 0),
                    'last_tx': trader_info.get('last_tx', 0)
                })
        
        return wallet_data

    def _calculate_wallet_risk(
        self, 
        metrics: Dict[str, Any], 
        wallet_type
    ) -> tuple:
        """
        Calculate risk score and flags for a wallet.
        
        Returns:
            (risk_score: int 0-100, risk_flags: List[str])
        """
        risk_score = 0
        risk_flags = []
        
        # Mixer wallets = high risk
        if wallet_type.value == 'MIXER':
            risk_score += 50
            risk_flags.append('Potential Mixer Activity')
        
        # Dust sweeper = medium risk
        if wallet_type.value == 'DUST_SWEEPER':
            risk_score += 30
            risk_flags.append('Dust Sweeper Pattern')
        
        # High transaction frequency
        tx_per_month = metrics.get('tx_per_month', 0)
        if tx_per_month > 100:
            risk_score += 20
            risk_flags.append('High Volume Trading')
        
        # Many exchange interactions
        exchange_count = metrics.get('exchange_interaction_count', 0)
        if exchange_count > 10:
            risk_score += 15
            risk_flags.append('Multiple Exchanges')
        
        # High turnover rate
        turnover = metrics.get('turnover_rate', 0)
        if turnover > 5:
            risk_score += 10
            risk_flags.append('High Turnover Rate')
        
        # Cap at 100
        risk_score = min(risk_score, 100)
        
        return risk_score, risk_flags
        
    async def _get_wallet_blockchain_data(
        self, 
        wallet_address: str, 
        chain: str, 
        token_address: str,
        limit: int = 40
    ) -> Dict[str, Any]:
        """Get blockchain transaction data for wallet"""
        try:
            if chain in ['ethereum', 'bsc']:
                from app.core.backend_crypto_tracker.blockchain.blockchain_specific.ethereum.get_address_transactions import execute_get_address_transactions
                txs = await execute_get_address_transactions(
                    wallet_address, 
                    limit=limit
                )
            elif chain == 'solana':
                from app.core.backend_crypto_tracker.blockchain.blockchain_specific.solana.get_transaction_details import execute_get_transaction_details
                txs = await execute_get_transaction_details(wallet_address)
            elif chain == 'sui':
                from app.core.backend_crypto_tracker.blockchain.blockchain_specific.sui.get_transaction import execute_get_transaction_details
                txs = await execute_get_transaction_details(wallet_address)
            else:
                return {'txs': [], 'balance': 0, 'address': wallet_address}
            
            return {
                'address': wallet_address,
                'txs': txs or [],
                'balance': 0,
                'inputs': [],
                'outputs': []
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching wallet data: {e}")
            return {'txs': [], 'balance': 0, 'address': wallet_address}

    def _classify_wallet_multistage(self, all_metrics: Dict[str, Any]) -> tuple:
        """
        ✅ OPTIMIZED VERSION - Calls AdaptiveClassifier once instead of 5 times
        
        This is more efficient because:
        - AdaptiveClassifier.classify() returns ALL probabilities at once
        - No need to call each analyzer separately
        - Same result, less computation
        """
        from app.core.backend_crypto_tracker.scanner.wallet_classifierr.core.adaptive_classifier import AdaptiveClassifier
        
        # Get all probabilities at once
        probabilities = AdaptiveClassifier.classify(all_metrics)
        
        # Log probabilities
        for class_name, prob in probabilities.items():
            self.logger.debug(f"    {class_name}: {prob:.4f}")
        
        # Find best match
        best_class_name = max(probabilities, key=probabilities.get)
        best_probability = probabilities[best_class_name]
        
        self.logger.info(f"  Best match: {best_class_name} with probability {best_probability:.4f}")
        
        # Map to WalletTypeEnum
        class_mapping = {
            'Dust Sweeper': WalletTypeEnum.DUST_SWEEPER,
            'Hodler': WalletTypeEnum.HODLER,
            'Mixer': WalletTypeEnum.MIXER,
            'Trader': WalletTypeEnum.TRADER,
            'Whale': WalletTypeEnum.WHALE
        }
        
        # Get threshold for this class
        analyzer_mapping = {
            'Dust Sweeper': 'dust_sweeper',
            'Hodler': 'hodler',
            'Mixer': 'mixer',
            'Trader': 'trader',
            'Whale': 'whale'
        }
        
        analyzer_key = analyzer_mapping.get(best_class_name)
        if analyzer_key:
            analyzer = self.wallet_analyzers[analyzer_key]
            threshold = analyzer.THRESHOLD
            
            # Check if probability meets threshold
            if best_probability >= threshold:
                self.logger.info(f"  ✅ CLASSIFIED as {best_class_name.upper()}")
                return class_mapping[best_class_name], best_probability
            else:
                self.logger.warning(f"  ❌ Probability {best_probability:.4f} below threshold {threshold} → UNKNOWN")
        
        return WalletTypeEnum.UNKNOWN, best_probability

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
            'wallet_analysis': {
                'classified': [],
                'unclassified': [],
                'total': 0
            }
        }
        
        if should_cache and self.cache:
            asyncio.create_task(self.cache.set(minimal_result, self.cache_ttl, cache_key))
        
        return minimal_result

    async def close(self):
        """Close resources"""
        await self.__aexit__(None, None, None)
