import asyncio
import logging
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import json
import hashlib
from collections import defaultdict

# Add these SQLAlchemy imports
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ENUM as SQLEnum

# Create the Base class for SQLAlchemy models
Base = declarative_base()

# Importiere die externen Dienste
from app.core.backend_crypto_tracker.services.multichain.chainalysis_service import ChainalysisIntegration
from app.core.backend_crypto_tracker.services.multichain.elliptic_service import EllipticIntegration
from app.core.backend_crypto_tracker.services.multichain.community_labels_service import CommunityLabelsAPI
from app.core.backend_crypto_tracker.utils.json_helpers import sanitize_float

# Importiere Konfigurationen
from app.core.backend_crypto_tracker.config.scanner_config import (
    WALLET_CLASSIFIER_CONFIG,
    ONCHAIN_ANALYSIS_CONFIG,
    SOURCE_WEIGHTS,
    load_config # Für chain-spezifische Konfigurationen
)
from app.core.backend_crypto_tracker.scanner.risk_assessor import AdvancedRiskAssessor, RiskAssessment, RiskLevel

logger = logging.getLogger(__name__)

# --- Enums und Dataclasses ---
class WalletTypeEnum(Enum):
    DEV_WALLET = "dev_wallet"
    LIQUIDITY_WALLET = "liquidity_wallet"
    WHALE_WALLET = "whale_wallet"
    DEX_CONTRACT = "dex_contract"
    BURN_WALLET = "burn_wallet"
    CEX_WALLET = "cex_wallet"
    SNIPER_WALLET = "sniper_wallet"
    RUGPULL_SUSPECT = "rugpull_suspect"
    UNKNOWN = "unknown"

class WalletAnalysis(Base):
    __tablename__ = "wallet_analyses"
    
    id = Column(Integer, primary_key=True, index=True)
    token_id = Column(Integer, ForeignKey("tokens.id"), nullable=False) # Assumes tokens table exists
    wallet_address = Column(String, nullable=False, index=True)
    wallet_type = Column(SQLEnum(WalletTypeEnum), nullable=False)
    balance = Column(Float, nullable=False)
    percentage_of_supply = Column(Float, nullable=False)
    transaction_count = Column(Integer)
    first_transaction = Column(DateTime)
    last_transaction = Column(DateTime)
    risk_score = Column(Float)
    analysis_date = Column(DateTime) # Default handled in code or DB
    
    # Relationship - String reference avoids circular import if Token is defined later/elsewhere
    # Ensure Token model is loaded before querying relationships
    token = relationship("Token", back_populates="wallet_analyses") # back_populates target must exist in Token
    
    # Neue Felder für erweiterte Metriken
    advanced_metrics = Column(JSON, nullable=True)  # Für institutionelle Risikometriken
    concentration_score = Column(Float, default=0.0)  # HHI + Gini
    liquidity_score = Column(Float, default=0.0)    # Amihud Ratio
    volatility_score = Column(Float, default=0.0)   # GARCH + Bollinger
    contract_entropy = Column(Float, default=0.0)    # Shannon Entropie
    whale_activity_score = Column(Float, default=0.0) # EWMA + Z-Score

# --- Klassen aus dem Originalcode ---
class OnChainAnalyzer:
    """Enhanced on-chain pattern analysis for wallet classification"""
    def __init__(self):
        # Korrigierte Zugriffe auf die Konfiguration
        self.dex_signatures = ONCHAIN_ANALYSIS_CONFIG.dex_signatures
        self.cex_patterns = ONCHAIN_ANALYSIS_CONFIG.cex_patterns
        self.dev_patterns = ONCHAIN_ANALYSIS_CONFIG.dev_patterns
        self.lp_patterns = ONCHAIN_ANALYSIS_CONFIG.lp_patterns
        self.rugpull_patterns = ONCHAIN_ANALYSIS_CONFIG.rugpull_patterns
    
    async def analyze_transaction_patterns(self, address: str, chain: str) -> Dict[str, Any]:
        """Comprehensive on-chain transaction pattern analysis"""
        patterns = {
            # Basic metrics
            'is_contract': await self._is_contract_address(address, chain),
            'transaction_count': 0,
            'unique_counterparties': 0,
            'avg_transaction_value': 0,
            'first_transaction_date': None,
            'last_transaction_date': None,
            # DEX interaction patterns
            'has_dex_interactions': False,
            'dex_signature_matches': [],
            'liquidity_operations': 0,
            'swap_operations': 0,
            # CEX interaction patterns
            'has_cex_interactions': False,
            'round_number_transactions': 0,
            'batch_transaction_patterns': 0,
            'withdrawal_sequences': 0,
            # Developer patterns
            'contract_creation': False,
            'admin_function_calls': 0,
            'early_activity_score': 0,
            'large_initial_balance': False,
            # Risk indicators
            'suspicious_patterns': [],
            'sudden_balance_changes': 0,
            'liquidity_removal_events': 0,
            # Advanced metrics
            'gas_usage_patterns': {},
            'transaction_timing_patterns': {},
            'counterparty_analysis': {},
            'token_interaction_count': 0
        }
        # In a real implementation, this would query blockchain APIs
        # For demonstration, we'll simulate some patterns
        patterns.update(await self._simulate_pattern_analysis(address, chain))
        return patterns
    
    async def _simulate_pattern_analysis(self, address: str, chain: str) -> Dict[str, Any]:
        """Simulate pattern analysis (replace with real blockchain queries)"""
        import random
        # Simulate different patterns based on address characteristics
        address_hash = hash(address) % 1000
        return {
            'transaction_count': random.randint(10, 50000),
            'unique_counterparties': random.randint(5, 1000),
            'has_dex_interactions': address_hash % 3 == 0,
            'has_cex_interactions': address_hash % 5 == 0,
            'contract_creation': address_hash % 10 == 0,
            'round_number_transactions': random.randint(0, 100),
            'dex_signature_matches': ['swap', 'addLiquidity'] if address_hash % 3 == 0 else [],
            'admin_function_calls': random.randint(0, 50) if address_hash % 10 == 0 else 0
        }
    
    async def _is_contract_address(self, address: str, chain: str) -> bool:
        """Check if address is a smart contract"""
        # In real implementation, would check if address has bytecode
        # For now, simulate based on address pattern
        return len(address) == 42 and address.startswith('0x') and hash(address) % 4 == 0
    
    def classify_by_patterns(self, patterns: Dict[str, Any], balance: float, total_supply: float) -> Tuple[WalletTypeEnum, float]:
        """Enhanced classification based on comprehensive pattern analysis"""
        confidence_scores = []
        classification_votes = []
        # DEX Contract Detection
        dex_score = self._analyze_dex_patterns(patterns)
        if dex_score > 0.7:
            classification_votes.append(WalletTypeEnum.DEX_CONTRACT)
            confidence_scores.append(dex_score)
        # CEX Wallet Detection
        cex_score = self._analyze_cex_patterns(patterns)
        if cex_score > 0.6:
            classification_votes.append(WalletTypeEnum.CEX_WALLET)
            confidence_scores.append(cex_score)
        # Developer Wallet Detection
        dev_score = self._analyze_dev_patterns(patterns, balance, total_supply)
        if dev_score > 0.6:
            classification_votes.append(WalletTypeEnum.DEV_WALLET)
            confidence_scores.append(dev_score)
        # Liquidity Provider Detection
        lp_score = self._analyze_lp_patterns(patterns)
        if lp_score > 0.5:
            classification_votes.append(WalletTypeEnum.LIQUIDITY_PROVIDER)
            confidence_scores.append(lp_score)
        # Whale Detection
        whale_score = self._analyze_whale_patterns(patterns, balance, total_supply)
        if whale_score > 0.6:
            classification_votes.append(WalletTypeEnum.WHALE_WALLET)
            confidence_scores.append(whale_score)
        # Rugpull Suspect Detection
        rugpull_score = self._analyze_rugpull_patterns(patterns, balance, total_supply)
        if rugpull_score > 0.7:
            classification_votes.append(WalletTypeEnum.RUGPULL_SUSPECT)
            confidence_scores.append(rugpull_score)
        # Team Wallet Detection
        team_score = self._analyze_team_patterns(patterns, balance, total_supply)
        if team_score > 0.6:
            classification_votes.append(WalletTypeEnum.TEAM_WALLET)
            confidence_scores.append(team_score)
        # Select best classification
        if classification_votes and confidence_scores:
            best_idx = confidence_scores.index(max(confidence_scores))
            return classification_votes[best_idx], confidence_scores[best_idx]
        # Default classification
        return WalletTypeEnum.UNKNOWN, 0.3
    
    def _analyze_dex_patterns(self, patterns: Dict[str, Any]) -> float:
        """Analyze DEX-specific patterns"""
        score = 0.0
        # Contract check
        if patterns.get('is_contract', False):
            score += 0.3
        # DEX signature matches
        dex_matches = patterns.get('dex_signature_matches', [])
        if dex_matches:
            score += min(0.4, len(dex_matches) * 0.1)
        # Liquidity operations
        if patterns.get('liquidity_operations', 0) > 10:
            score += 0.2
        # Swap operations
        if patterns.get('swap_operations', 0) > 100:
            score += 0.1
        return min(1.0, score)
    
    def _analyze_cex_patterns(self, patterns: Dict[str, Any]) -> float:
        """Analyze CEX-specific patterns"""
        score = 0.0
        # High transaction count
        tx_count = patterns.get('transaction_count', 0)
        if tx_count > self.cex_patterns['high_tx_count']:
            score += 0.3
        # Round number transactions
        round_tx = patterns.get('round_number_transactions', 0)
        if round_tx > 50:
            score += 0.2
        # Batch patterns
        if patterns.get('batch_transaction_patterns', 0) > 20:
            score += 0.2
        # Multiple counterparties
        if patterns.get('unique_counterparties', 0) > 500:
            score += 0.3
        return min(1.0, score)
    
    def _analyze_dev_patterns(self, patterns: Dict[str, Any], balance: float, total_supply: float) -> float:
        """Analyze developer wallet patterns"""
        score = 0.0
        # Contract creation
        if patterns.get('contract_creation', False):
            score += 0.4
        # Admin function calls
        if patterns.get('admin_function_calls', 0) > 10:
            score += 0.3
        # Large initial balance
        if total_supply and balance > 0:
            percentage = (balance / total_supply) * 100
            if percentage > 20:
                score += 0.3
            elif percentage > 10:
                score += 0.2
        # Early activity
        if patterns.get('early_activity_score', 0) > 0.7:
            score += 0.2
        return min(1.0, score)
    
    def _analyze_lp_patterns(self, patterns: Dict[str, Any]) -> float:
        """Analyze liquidity provider patterns"""
        score = 0.0
        # Liquidity operations
        liq_ops = patterns.get('liquidity_operations', 0)
        if liq_ops > 5:
            score += 0.4
        # DEX interactions
        if patterns.get('has_dex_interactions', False):
            score += 0.3
        # Token interaction diversity
        if patterns.get('token_interaction_count', 0) > 10:
            score += 0.3
        return min(1.0, score)
    
    def _analyze_whale_patterns(self, patterns: Dict[str, Any], balance: float, total_supply: float) -> float:
        """Analyze whale wallet patterns"""
        score = 0.0
        # Large absolute balance
        if balance > 10000000:  # 10M tokens
            score += 0.4
        elif balance > 1000000:  # 1M tokens
            score += 0.3
        # Large percentage of supply
        if total_supply and balance > 0:
            percentage = (balance / total_supply) * 100
            if percentage > 10:
                score += 0.4
            elif percentage > 5:
                score += 0.3
            elif percentage > 1:
                score += 0.2
        # Transaction patterns consistent with whales
        if patterns.get('transaction_count', 0) < 1000:  # Whales typically have fewer, larger transactions
            score += 0.2
        return min(1.0, score)
    
    def _analyze_rugpull_patterns(self, patterns: Dict[str, Any], balance: float, total_supply: float) -> float:
        """Analyze rugpull suspect patterns"""
        score = 0.0
        # Suspicious patterns
        suspicious_count = len(patterns.get('suspicious_patterns', []))
        if suspicious_count > 3:
            score += 0.5
        elif suspicious_count > 1:
            score += 0.3
        # Sudden balance changes
        if patterns.get('sudden_balance_changes', 0) > 5:
            score += 0.3
        # Liquidity removal events
        if patterns.get('liquidity_removal_events', 0) > 3:
            score += 0.4
        return min(1.0, score)
    
    def _analyze_team_patterns(self, patterns: Dict[str, Any], balance: float, total_supply: float) -> float:
        """Analyze team wallet patterns"""
        score = 0.0
        # Medium to large holdings (but not whale level)
        if total_supply and balance > 0:
            percentage = (balance / total_supply) * 100
            if 2 <= percentage <= 15:  # Team wallets typically hold 2-15%
                score += 0.4
        # Early activity but not contract creation
        if patterns.get('early_activity_score', 0) > 0.5 and not patterns.get('contract_creation', False):
            score += 0.3
        # Moderate transaction activity
        tx_count = patterns.get('transaction_count', 0)
        if 100 <= tx_count <= 5000:
            score += 0.3
        return min(1.0, score)

class MultiSourceClassifier:
    """Multi-source voting system for wallet classification"""
    def __init__(self):
        # Initialisiere die Dienste direkt, da sie ihre API-Schlüssel intern laden
        self.sources = [
            ChainalysisIntegration(),
            EllipticIntegration(),
            CommunityLabelsAPI(),
            OnChainAnalyzer()
        ]
        self.source_weights = SOURCE_WEIGHTS
        # Korrigierter Zugriff auf die Konfiguration
        self.confidence_thresholds = WALLET_CLASSIFIER_CONFIG.confidence_thresholds
    
    async def classify_with_confidence(self, address: str, chain: str, balance: float, total_supply: float) -> Tuple[WalletTypeEnum, float, List[str]]:
        """Classify wallet with confidence score using multiple sources"""
        results = []
        sources_used = []
        # Start with internal logic classification
        internal_result = await self._classify_with_internal_logic(address, chain, balance, total_supply)
        if internal_result:
            results.append({
                'source': 'InternalLogic',
                'result': internal_result,
                'weight': self.source_weights.get('InternalLogic', 0.3),
                'reliability': 0.8
            })
            sources_used.append('InternalLogic')
        # Collect results from external sources
        for source in self.sources:
            try:
                source_name = source.__class__.__name__
                result = None
                if isinstance(source, ChainalysisIntegration):
                    result = await self._get_chainalysis_classification(source, address, chain)
                elif isinstance(source, EllipticIntegration):
                    result = await self._get_elliptic_classification(source, address, chain)
                elif isinstance(source, CommunityLabelsAPI):
                    result = await self._get_community_classification(source, address, chain)
                elif isinstance(source, OnChainAnalyzer):
                    result = await self._get_onchain_classification(source, address, chain, balance, total_supply)
                if result:
                    results.append({
                        'source': source_name,
                        'result': result,
                        'weight': self.source_weights.get(source_name, 0.1),
                        'reliability': result.get('reliability', 0.7)
                    })
                    sources_used.append(source_name)
            except Exception as e:
                logger.warning(f"Source {source.__class__.__name__} failed: {e}")
        # Vote on results using enhanced voting system
        if not results:
            return WalletTypeEnum.UNKNOWN, 0.1, sources_used
        final_classification = self.vote_on_results(results)
        confidence = self._calculate_confidence(results)
        return final_classification, confidence, sources_used
    
    def vote_on_results(self, results: List[Dict]) -> WalletTypeEnum:
        """Enhanced weighted voting logic between different sources"""
        vote_scores = defaultdict(float)
        total_weight = 0
        # Calculate weighted scores for each wallet type
        for result_data in results:
            source_weight = result_data['weight']
            source_reliability = result_data.get('reliability', 0.7)
            result = result_data['result']
            # Adjust weight by source reliability
            adjusted_weight = source_weight * source_reliability
            total_weight += adjusted_weight
            if isinstance(result, dict) and 'wallet_type' in result:
                wallet_type = result['wallet_type']
                source_confidence = result.get('confidence', 0.5)
                # Final score = adjusted_weight * source_confidence
                vote_scores[wallet_type] += adjusted_weight * source_confidence
        if not vote_scores:
            return WalletTypeEnum.UNKNOWN
        # Normalize scores and apply minimum threshold
        normalized_scores = {
            wallet_type: score / total_weight if total_weight > 0 else 0
            for wallet_type, score in vote_scores.items()
        }
        # Get the highest scoring wallet type
        best_type, best_score = max(normalized_scores.items(), key=lambda x: x[1])
        # Apply minimum threshold - if score too low, classify as UNKNOWN
        if best_score < 0.3:
            return WalletTypeEnum.UNKNOWN
        return best_type
    
    def _calculate_confidence(self, results: List[Dict]) -> float:
        """Enhanced confidence calculation based on multiple factors"""
        if len(results) <= 1:
            return 0.4 if results else 0.1
        # Extract wallet types and their associated data
        classifications = []
        total_reliability = 0
        for result_data in results:
            result = result_data['result']
            source_reliability = result_data.get('reliability', 0.7)
            source_weight = result_data['weight']
            if isinstance(result, dict) and 'wallet_type' in result:
                classifications.append({
                    'wallet_type': result['wallet_type'],
                    'confidence': result.get('confidence', 0.5),
                    'reliability': source_reliability,
                    'weight': source_weight
                })
                total_reliability += source_reliability
        if not classifications:
            return 0.1
        # Calculate agreement metrics
        wallet_types = [c['wallet_type'] for c in classifications]
        most_common_type = max(set(wallet_types), key=wallet_types.count)
        agreement_count = wallet_types.count(most_common_type)
        agreement_ratio = agreement_count / len(wallet_types)
        # Calculate weighted confidence
        weighted_confidence = 0
        total_weight = 0
        for classification in classifications:
            if classification['wallet_type'] == most_common_type:
                weight = classification['weight'] * classification['reliability']
                weighted_confidence += classification['confidence'] * weight
                total_weight += weight
        if total_weight > 0:
            base_confidence = weighted_confidence / total_weight
        else:
            base_confidence = 0.5
        # Apply agreement bonus
        agreement_bonus = (agreement_ratio - 0.5) * 0.3 if agreement_ratio > 0.5 else 0
        # Apply source diversity bonus
        unique_sources = len(set(r['source'] for r in results))
        diversity_bonus = min(0.15, unique_sources * 0.03)
        # Apply reliability bonus
        avg_reliability = total_reliability / len(results)
        reliability_bonus = (avg_reliability - 0.7) * 0.2 if avg_reliability > 0.7 else 0
        final_confidence = base_confidence + agreement_bonus + diversity_bonus + reliability_bonus
        return min(1.0, max(0.1, final_confidence))
    
    async def _classify_with_internal_logic(self, address: str, chain: str, balance: float, total_supply: float) -> Optional[Dict]:
        """Internal classification logic based on basic patterns"""
        try:
            wallet_type, confidence = await self._determine_wallet_type_detailed(address, chain, balance, total_supply)
            return {
                'wallet_type': wallet_type,
                'confidence': confidence,
                'method': 'internal_logic'
            }
        except Exception as e:
            logger.error(f"Internal logic classification failed: {e}")
            return None
    
    async def _determine_wallet_type_detailed(self, address: str, chain: str, balance: float, total_supply: float) -> Tuple[WalletTypeEnum, float]:
        """Detailed wallet type determination with confidence scoring"""
        # Burn address patterns
        burn_patterns = [
            '0x0000000000000000000000000000000000000000',
            '0x000000000000000000000000000000000000dEaD',
            '11111111111111111111111111111111'  # Solana burn
        ]
        if address.lower() in [p.lower() for p in burn_patterns]:
            return WalletTypeEnum.BURN_WALLET, 0.98
        # Zero balance check
        if balance == 0:
            return WalletTypeEnum.UNKNOWN, 0.3
        # Large holder detection
        if total_supply and balance > 0:
            percentage = (balance / total_supply) * 100
            # Potential dev wallet - very large initial holding
            if percentage > 50:
                return WalletTypeEnum.DEV_WALLET, 0.85
            elif percentage > 20:
                return WalletTypeEnum.DEV_WALLET, 0.70
            elif percentage > 10:
                return WalletTypeEnum.WHALE_WALLET, 0.75
            elif percentage > 5:
                return WalletTypeEnum.WHALE_WALLET, 0.65
        # Absolute value whale detection
        if balance > 10000000:  # 10M tokens
            return WalletTypeEnum.WHALE_WALLET, 0.70
        elif balance > 1000000:  # 1M tokens
            return WalletTypeEnum.WHALE_WALLET, 0.60
        # Default classification
        return WalletTypeEnum.UNKNOWN, 0.4
    
    # Passe die `_get_*_classification` Methoden an, sodass sie die instanziierten Dienste verwenden:
    async def _get_chainalysis_classification(self, source: ChainalysisIntegration, address: str, chain: str) -> Optional[Dict]:
        """Get classification from Chainalysis"""
        try:
            asset = 'ETH' if chain == 'ethereum' else 'BNB' if chain == 'bsc' else 'SOL'
            risk_data = await source.get_address_risk(address, asset)
            sanctions_data = await source.screen_address(address)
            if not risk_data and not sanctions_data:
                return None
            # Parse Chainalysis response
            wallet_type = WalletTypeEnum.UNKNOWN
            confidence = 0.5
            if sanctions_data and sanctions_data.get('is_sanctioned', False):
                wallet_type = WalletTypeEnum.RUGPULL_SUSPECT
                confidence = 0.95
            elif risk_data:
                risk_score = risk_data.get('risk_score', 0)
                entity_type = risk_data.get('entity_type', '')
                if 'exchange' in entity_type.lower():
                    wallet_type = WalletTypeEnum.CEX_WALLET
                    confidence = 0.90
                elif 'dex' in entity_type.lower():
                    wallet_type = WalletTypeEnum.DEX_CONTRACT
                    confidence = 0.85
                elif risk_score > 80:
                    wallet_type = WalletTypeEnum.RUGPULL_SUSPECT
                    confidence = 0.80
            return {
                'wallet_type': wallet_type,
                'confidence': confidence,
                'reliability': 0.95,
                'source_data': {'risk_data': risk_data, 'sanctions_data': sanctions_data}
            }
        except Exception as e:
            logger.error(f"Chainalysis classification failed: {e}")
            return None
    
    async def _get_elliptic_classification(self, source: EllipticIntegration, address: str, chain: str) -> Optional[Dict]:
        """Get classification from Elliptic"""
        try:
            analysis = await source.get_wallet_analysis(address)
            labels = await source.get_entity_labels(address)
            if not analysis and not labels:
                return None
            wallet_type = WalletTypeEnum.UNKNOWN
            confidence = 0.5
            # Parse labels
            if labels:
                labels_lower = [label.lower() for label in labels]
                if any('exchange' in label for label in labels_lower):
                    wallet_type = WalletTypeEnum.CEX_WALLET
                    confidence = 0.85
                elif any('dex' in label or 'defi' in label for label in labels_lower):
                    wallet_type = WalletTypeEnum.DEX_CONTRACT
                    confidence = 0.80
                elif any('scam' in label or 'fraud' in label for label in labels_lower):
                    wallet_type = WalletTypeEnum.RUGPULL_SUSPECT
                    confidence = 0.85
            return {
                'wallet_type': wallet_type,
                'confidence': confidence,
                'reliability': 0.90,
                'source_data': {'analysis': analysis, 'labels': labels}
            }
        except Exception as e:
            logger.error(f"Elliptic classification failed: {e}")
            return None
    
    async def _get_community_classification(self, source: CommunityLabelsAPI, address: str, chain: str) -> Optional[Dict]:
        """Get classification from community sources"""
        try:
            labels = await source.get_community_labels(address, chain)
            if not labels:
                return None
            wallet_type = WalletTypeEnum.UNKNOWN
            confidence = 0.4  # Lower confidence for community sources
            labels_lower = [label.lower() for label in labels]
            if any('exchange' in label or 'binance' in label or 'coinbase' in label for label in labels_lower):
                wallet_type = WalletTypeEnum.CEX_WALLET
                confidence = 0.70
            elif any('uniswap' in label or 'pancake' in label or 'dex' in label for label in labels_lower):
                wallet_type = WalletTypeEnum.DEX_CONTRACT
                confidence = 0.75
            elif any('team' in label or 'dev' in label for label in labels_lower):
                wallet_type = WalletTypeEnum.DEV_WALLET
                confidence = 0.60
            return {
                'wallet_type': wallet_type,
                'confidence': confidence,
                'reliability': 0.60,
                'source_data': {'labels': labels}
            }
        except Exception as e:
            logger.error(f"Community classification failed: {e}")
            return None
    
    async def _get_onchain_classification(self, source: OnChainAnalyzer, address: str, chain: str, balance: float, total_supply: float) -> Optional[Dict]:
        """Get classification from on-chain analysis"""
        try:
            patterns = await source.analyze_transaction_patterns(address, chain)
            wallet_type, confidence = source.classify_by_patterns(patterns, balance, total_supply)
            return {
                'wallet_type': wallet_type,
                'confidence': confidence,
                'reliability': 0.75,
                'source_data': {'patterns': patterns}
            }
        except Exception as e:
            logger.error(f"On-chain classification failed: {e}")
            return None

class DynamicDataCollector:
    """Automated data collection for keeping classification data current"""
    def __init__(self):
        self.update_schedule = {
            'cex_addresses': 3600,      # CEX addresses hourly (seconds)
            'dex_contracts': 21600,     # DEX contracts every 6h
            'rugpull_database': 3600,   # Rugpull DB hourly
            'community_labels': 86400   # Community labels daily
        }
        self.last_updates = {}
        self.known_addresses = defaultdict(dict)
    
    async def update_known_addresses(self):
        """Update known addresses from various sources"""
        current_time = datetime.utcnow()
        tasks = []
        for data_type, interval in self.update_schedule.items():
            last_update = self.last_updates.get(data_type)
            if (not last_update or 
                (current_time - last_update).total_seconds() >= interval):
                if data_type == 'cex_addresses':
                    tasks.append(self._update_cex_addresses())
                elif data_type == 'dex_contracts':
                    tasks.append(self._update_dex_contracts())
                elif data_type == 'rugpull_database':
                    tasks.append(self._update_rugpull_database())
                elif data_type == 'community_labels':
                    tasks.append(self._update_community_labels())
                self.last_updates[data_type] = current_time
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _update_cex_addresses(self):
        """Update CEX addresses from various sources"""
        try:
            # Example: Update from multiple sources
            cex_sources = [
                "https://api.etherscan.io/api?module=account&action=txlist&tag=latest",
                "https://api.bscscan.com/api?module=account&action=txlist&tag=latest"
            ]
            # In real implementation, would fetch and parse CEX addresses
            logger.info("Updating CEX addresses...")
        except Exception as e:
            logger.error(f"Failed to update CEX addresses: {e}")
    
    async def _update_dex_contracts(self):
        """Update DEX contract addresses"""
        try:
            # CoinGecko API for current DEX list
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://api.coingecko.com/api/v3/exchanges/decentralized",
                    timeout=30.0
                )
                if response.status_code == 200:
                    dex_data = response.json()
                    # Process and update DEX contracts
                    logger.info(f"Updated {len(dex_data)} DEX contracts")
        except Exception as e:
            logger.error(f"Failed to update DEX contracts: {e}")
    
    async def _update_rugpull_database(self):
        """Update rugpull database"""
        try:
            # Would integrate with rugpull detection services
            logger.info("Updating rugpull database...")
        except Exception as e:
            logger.error(f"Failed to update rugpull database: {e}")
    
    async def _update_community_labels(self):
        """Update community labels"""
        try:
            # Would integrate with community labeling platforms
            logger.info("Updating community labels...")
        except Exception as e:
            logger.error(f"Failed to update community labels: {e}")

class EnhancedWalletClassifier:
    """Enhanced wallet classifier with multi-source analysis and dynamic updates"""
    def __init__(self):
        # api_keys werden nicht mehr benötigt, da sie in den Diensten geladen werden
        self.multi_source_classifier = MultiSourceClassifier()
        self.data_collector = DynamicDataCollector()
        self.advanced_risk_assessor = AdvancedRiskAssessor()
        self.cache = {}
        # Korrigierter Zugriff auf die Konfiguration
        self.cache_ttl = WALLET_CLASSIFIER_CONFIG.cache_ttl
        # Lade bekannte Adressen (könnte in eine eigene Datei oder DB ausgelagert werden)
        self.known_addresses = self._load_known_addresses()
        # ENTFERNT: self.chain_config = load_config()  # <--- FEHLERHAFTE ZEILE ENTFERNT
    
    async def __aenter__(self):
        """Asynchronous context manager entry"""
        # Initialisiere die MultiSourceClassifier-Komponenten, falls nötig
        # Da die MultiSourceClassifier-Klasse selbst keine __aenter__ hat, müssen wir hier nichts tun
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Asynchronous context manager exit"""
        # Hier könnten Aufräumarbeiten durchgeführt werden, falls nötig
        pass
    
    async def classify_wallet(self, wallet_address: str, chain: str,
                            balance: float, total_supply: float = None) -> WalletAnalysis:
        """Enhanced wallet classification with multiple sources and caching"""
        # Check cache first
        cache_key = self._generate_cache_key(wallet_address, chain, balance, total_supply)
        cached_result = self._get_cached_result(cache_key)
        if cached_result:
            return cached_result
        
        # Update dynamic data if needed
        await self.data_collector.update_known_addresses()
        
        # Check known addresses first
        known_type = self._check_known_addresses(wallet_address, chain)
        if known_type:
            result = WalletAnalysis(
                wallet_address=wallet_address,
                wallet_type=known_type,
                balance=balance,
                percentage_of_supply=(balance / total_supply * 100) if total_supply else 0,
                risk_score=self._calculate_risk_score(known_type, balance, total_supply, chain),  # chain hinzugefügt
                confidence_score=0.95,  # High confidence for known addresses
                analysis_date=datetime.utcnow(),
                sources_used=['known_addresses']
            )
        else:
            # Use multi-source classification
            wallet_type, confidence, sources_used = await self.multi_source_classifier.classify_with_confidence(
                wallet_address, chain, balance, total_supply
            )
            result = WalletAnalysis(
                wallet_address=wallet_address,
                wallet_type=wallet_type,
                balance=balance,
                percentage_of_supply=(balance / total_supply * 100) if total_supply else 0,
                risk_score=self._calculate_risk_score(wallet_type, balance, total_supply, chain),  # chain hinzugefügt
                confidence_score=confidence,
                analysis_date=datetime.utcnow(),
                sources_used=sources_used
            )
        
        # Cache the result
        self._cache_result(cache_key, result)
        return result
    
    async def classify_wallet_advanced(self, wallet_address: str, chain: str,
                                    balance: float, total_supply: float = None,
                                    transaction_history: List[Dict] = None,
                                    token_data: Dict[str, Any] = None) -> WalletAnalysis:
        """
        Erweiterte Wallet-Klassifizierung mit institutionellen Risikofunktionen
        
        Args:
            wallet_address: Die Wallet-Adresse
            chain: Die Blockchain
            balance: Wallet-Balance
            total_supply: Gesamtangebot des Tokens
            transaction_history: Historische Transaktionen (optional)
            token_data: Token-Daten für Risikobewertung (optional)
            
        Returns:
            Erweiterte WalletAnalysis mit Risikometriken
        """
        # Standard-Klassifizierung durchführen
        wallet_analysis = await self.classify_wallet(
            wallet_address, chain, balance, total_supply
        )
        
        # Erweiterte Risikometriken berechnen
        try:
            # Token-Daten für Risikobewertung vorbereiten
            if not token_data:
                token_data = {
                    'address': wallet_address,  # Vereinfacht für Demo
                    'chain': chain,
                    'market_cap': total_supply * 0.001 if total_supply else 0,  # Geschätzt
                    'liquidity': total_supply * 0.0005 if total_supply else 0,  # Geschätzt
                    'volume_24h': total_supply * 0.0002 if total_supply else 0,  # Geschätzt
                    'contract_verified': False  # Standardwert
                }
            
            # Wallet-Analysen für Risikobewertung vorbereiten
            wallet_analyses = [wallet_analysis]
            
            # Erweiterte Risikobewertung durchführen
            risk_assessment = await self.advanced_risk_assessor.assess_token_risk_advanced(
                token_data=token_data,
                wallet_analyses=wallet_analyses,
                transaction_history=transaction_history
            )
            
            # Erweiterte Metriken zur Wallet-Analyse hinzufügen
            institutional_metrics = risk_assessment.details.get('institutional_metrics', {})
            
            # Aktualisiere die Wallet-Analyse mit erweiterten Metriken
            wallet_analysis.concentration_score = institutional_metrics.get('concentration', {}).get('score', 0)
            wallet_analysis.liquidity_score = institutional_metrics.get('liquidity', {}).get('score', 0)
            wallet_analysis.volatility_score = institutional_metrics.get('volatility', {}).get('score', 0)
            wallet_analysis.contract_entropy = institutional_metrics.get('contract', {}).get('entropy', 0)
            wallet_analysis.whale_activity_score = institutional_metrics.get('whale_activity', {}).get('score', 0)
            wallet_analysis.advanced_metrics = institutional_metrics
            
            # Risikoscore aktualisieren
            wallet_analysis.risk_score = risk_assessment.overall_score
            
        except Exception as e:
            logger.error(f"Fehler bei erweiterter Wallet-Klassifizierung: {e}")
        
        return wallet_analysis
    
    def _check_known_addresses(self, address: str, chain: str) -> Optional[WalletTypeEnum]:
        """Check if address is in known addresses database"""
        chain_addresses = self.known_addresses.get(chain, {})
        return chain_addresses.get(address)
    
    def _generate_cache_key(self, address: str, chain: str, balance: float, total_supply: float) -> str:
        """Generate cache key for result caching"""
        key_data = f"{address}:{chain}:{balance}:{total_supply}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cached_result(self, cache_key: str) -> Optional[WalletAnalysis]:
        """Get cached result if still valid"""
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            if (datetime.utcnow() - cached_data['timestamp']).total_seconds() < self.cache_ttl:
                return cached_data['result']
            else:
                del self.cache[cache_key]
        return None
    
    def _cache_result(self, cache_key: str, result: WalletAnalysis):
        """Cache analysis result"""
        self.cache[cache_key] = {
            'result': result,
            'timestamp': datetime.utcnow()
        }
    
    def _calculate_risk_score(self, wallet_type: WalletTypeEnum, balance: float, total_supply: float, chain: str) -> float:
        """Enhanced risk score calculation"""
        base_scores = {
            WalletTypeEnum.DEV_WALLET: 80.0,
            WalletTypeEnum.RUGPULL_SUSPECT: 95.0,
            WalletTypeEnum.WHALE_WALLET: 60.0,
            WalletTypeEnum.DEX_CONTRACT: 10.0,
            WalletTypeEnum.CEX_WALLET: 20.0,
            WalletTypeEnum.BURN_WALLET: 0.0,
            WalletTypeEnum.TEAM_WALLET: 70.0,
            WalletTypeEnum.LIQUIDITY_PROVIDER: 25.0,
            WalletTypeEnum.UNKNOWN: 30.0
        }
        
        base_score = base_scores.get(wallet_type, 30.0)
        
        if total_supply and balance > 0:
            percentage = (balance / total_supply) * 100
            
            if wallet_type == WalletTypeEnum.DEV_WALLET:
                if percentage > 50:
                    base_score += 15
                elif percentage > 30:
                    base_score += 10
                elif percentage > 20:
                    base_score += 5
            elif wallet_type == WalletTypeEnum.WHALE_WALLET:
                if percentage > 20:
                    base_score += 25
                elif percentage > 10:
                    base_score += 15
                elif percentage > 5:
                    base_score += 10
        
        if balance > 0:
            if balance > 100000000:
                base_score += 15
            elif balance > 50000000:
                base_score += 10
            elif balance > 10000000:
                base_score += 5
        
        if wallet_type == WalletTypeEnum.RUGPULL_SUSPECT:
            base_score = max(base_score, 90.0)
        elif wallet_type == WalletTypeEnum.DEX_CONTRACT:
            base_score = min(base_score, 30.0)
        elif wallet_type == WalletTypeEnum.BURN_WALLET:
            base_score = 0.0
        
        chain_config = load_config(chain)
        min_scores = chain_config.get('min_scores', {})
        chain_min_score = min_scores.get(chain, 30)
        
        if wallet_type != WalletTypeEnum.BURN_WALLET and base_score > 0:
            base_score = max(base_score, chain_min_score)
        
        return sanitize_float(min(100.0, max(0.0, base_score)))
    
    def _load_known_addresses(self) -> Dict[str, Dict[str, WalletTypeEnum]]:
        """Lädt bekannte Adressen für alle unterstützten Chains."""
        return {
            'ethereum': self._load_known_ethereum_addresses(),
            'bsc': self._load_known_bsc_addresses(),
            'solana': self._load_known_solana_addresses(),
            'sui': self._load_known_sui_addresses()
        }
    
    def _load_known_ethereum_addresses(self) -> Dict[str, WalletTypeEnum]:
        """Load known Ethereum addresses"""
        return {
            # Uniswap
            '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D': WalletTypeEnum.DEX_CONTRACT,
            '0xE592427A0AEce92De3Edee1F18E0157C05861564': WalletTypeEnum.DEX_CONTRACT,
            # Binance
            '0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE': WalletTypeEnum.CEX_WALLET,
            '0xD551234Ae421e3BCBA99A0Da6d736074f22192FF': WalletTypeEnum.CEX_WALLET,
            # Coinbase
            '0x71660c4005BA85c37ccec55d0C4493E66Fe775d3': WalletTypeEnum.CEX_WALLET,
            '0x503828976D22510aad0201ac7EC88293211D23Da': WalletTypeEnum.CEX_WALLET,
            # Burn addresses
            '0x0000000000000000000000000000000000000000': WalletTypeEnum.BURN_WALLET,
            '0x000000000000000000000000000000000000dEaD': WalletTypeEnum.BURN_WALLET,
        }
    
    def _load_known_bsc_addresses(self) -> Dict[str, WalletTypeEnum]:
        """Load known BSC addresses"""
        return {
            # PancakeSwap
            '0x10ED43C718714eb63d5aA57B78B54704E256024E': WalletTypeEnum.DEX_CONTRACT,
            '0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73': WalletTypeEnum.DEX_CONTRACT,
            # Binance BSC
            '0x8894E0a0c962CB723c1976a4421c95949bE2D4E3': WalletTypeEnum.CEX_WALLET,
            # Burn addresses
            '0x0000000000000000000000000000000000000000': WalletTypeEnum.BURN_WALLET,
            '0x000000000000000000000000000000000000dEaD': WalletTypeEnum.BURN_WALLET,
        }
    
    def _load_known_solana_addresses(self) -> Dict[str, WalletTypeEnum]:
        """Lädt bekannte Solana Adressen"""
        return {
            # Bekannte DEX Adressen
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc': WalletTypeEnum.DEX_CONTRACT,  # Orca
            'SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt': WalletTypeEnum.DEX_CONTRACT, # Raydium AMM v3?
            # Bekannte CEX Adressen
            'EVMxfGDjANiCcvFgDJ7E8pfHgKqKpzqJJYxJt8o9V5i': WalletTypeEnum.CEX_WALLET, # FTX
            # Burn Addresses
            '11111111111111111111111111111111': WalletTypeEnum.BURN_WALLET,
        }
    
    def _load_known_sui_addresses(self) -> Dict[str, WalletTypeEnum]:
        """Lädt bekannte Sui Adressen"""
        return {
            # Bekannte System Adressen
            '0x0000000000000000000000000000000000000000000000000000000000000000': WalletTypeEnum.BURN_WALLET,
            # Weitere Sui-spezifische Adressen könnten hier hinzugefügt werden
        }

# Am Ende der Datei (vermutlich)
WalletClassifier = EnhancedWalletClassifier
