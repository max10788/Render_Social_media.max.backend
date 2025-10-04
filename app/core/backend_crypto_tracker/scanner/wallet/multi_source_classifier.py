"""
Multi-source voting system for wallet classification
"""
import logging
from typing import Tuple, List, Dict, Optional, Any
from collections import defaultdict
from scanner.models.wallet_models import WalletTypeEnum
from scanner.analyzers.onchain_analyzer import OnChainAnalyzer
from app.core.backend_crypto_tracker.services.multichain.chainalysis_service import ChainalysisIntegration
from app.core.backend_crypto_tracker.services.multichain.elliptic_service import EllipticIntegration
from app.core.backend_crypto_tracker.services.multichain.community_labels_service import CommunityLabelsAPI
from app.core.backend_crypto_tracker.config.scanner_config import (
    WALLET_CLASSIFIER_CONFIG, 
    SOURCE_WEIGHTS
)

logger = logging.getLogger(__name__)


class MultiSourceClassifier:
    """Multi-source voting system for wallet classification"""
    
    def __init__(self):
        # Initialize services (they load their API keys internally)
        self.sources = [
            ChainalysisIntegration(),
            EllipticIntegration(),
            CommunityLabelsAPI(),
            OnChainAnalyzer()
        ]
        self.source_weights = SOURCE_WEIGHTS
        self.confidence_thresholds = WALLET_CLASSIFIER_CONFIG.confidence_thresholds
    
    async def classify_with_confidence(self, address: str, chain: str, 
                                      balance: float, total_supply: float) -> Tuple[WalletTypeEnum, float, List[str]]:
        """Classify wallet with confidence score using multiple sources"""
        results = []
        sources_used = []
        
        # Start with internal logic classification
        internal_result = await self._classify_with_internal_logic(
            address, chain, balance, total_supply
        )
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
                    result = await self._get_onchain_classification(
                        source, address, chain, balance, total_supply
                    )
                
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
        
        # Apply minimum threshold
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
        
        base_confidence = weighted_confidence / total_weight if total_weight > 0 else 0.5
        
        # Apply bonuses
        agreement_bonus = (agreement_ratio - 0.5) * 0.3 if agreement_ratio > 0.5 else 0
        unique_sources = len(set(r['source'] for r in results))
        diversity_bonus = min(0.15, unique_sources * 0.03)
        avg_reliability = total_reliability / len(results)
        reliability_bonus = (avg_reliability - 0.7) * 0.2 if avg_reliability > 0.7 else 0
        
        final_confidence = base_confidence + agreement_bonus + diversity_bonus + reliability_bonus
        return min(1.0, max(0.1, final_confidence))
    
    async def _classify_with_internal_logic(self, address: str, chain: str, 
                                           balance: float, total_supply: float) -> Optional[Dict]:
        """Internal classification logic based on basic patterns"""
        try:
            wallet_type, confidence = await self._determine_wallet_type_detailed(
                address, chain, balance, total_supply
            )
            return {
                'wallet_type': wallet_type,
                'confidence': confidence,
                'method': 'internal_logic'
            }
        except Exception as e:
            logger.error(f"Internal logic classification failed: {e}")
            return None
    
    async def _determine_wallet_type_detailed(self, address: str, chain: str, 
                                             balance: float, total_supply: float) -> Tuple[WalletTypeEnum, float]:
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
        
        return WalletTypeEnum.UNKNOWN, 0.4
    
    async def _get_chainalysis_classification(self, source: ChainalysisIntegration, 
                                             address: str, chain: str) -> Optional[Dict]:
        """Get classification from Chainalysis"""
        try:
            asset = 'ETH' if chain == 'ethereum' else 'BNB' if chain == 'bsc' else 'SOL'
            risk_data = await source.get_address_risk(address, asset)
            sanctions_data = await source.screen_address(address)
            
            if not risk_data and not sanctions_data:
                return None
            
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
    
    async def _get_elliptic_classification(self, source: EllipticIntegration, 
                                          address: str, chain: str) -> Optional[Dict]:
        """Get classification from Elliptic"""
        try:
            analysis = await source.get_wallet_analysis(address)
            labels = await source.get_entity_labels(address)
            
            if not analysis and not labels:
                return None
            
            wallet_type = WalletTypeEnum.UNKNOWN
            confidence = 0.5
            
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
    
    async def _get_community_classification(self, source: CommunityLabelsAPI, 
                                           address: str, chain: str) -> Optional[Dict]:
        """Get classification from community sources"""
        try:
            labels = await source.get_community_labels(address, chain)
            
            if not labels:
                return None
            
            wallet_type = WalletTypeEnum.UNKNOWN
            confidence = 0.4
            labels_lower = [label.lower() for label in labels]
            
            if any('exchange' in label or 'binance' in label or 'coinbase' in label 
                   for label in labels_
