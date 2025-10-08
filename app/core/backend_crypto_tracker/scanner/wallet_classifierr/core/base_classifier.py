# wallet_classifier/core/base_classifier.py

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum
import numpy as np

class AnalysisStage(Enum):
    """Defines the depth of wallet analysis"""
    STAGE1 = "basic"      # Primary metrics only
    STAGE2 = "intermediate"  # Primary + Secondary metrics
    STAGE3 = "advanced"   # All metrics including context

class WalletClass(Enum):
    """Wallet behavior classifications"""
    DUST_SWEEPER = "dust_sweeper"
    HODLER = "hodler"
    MIXER = "mixer"
    TRADER = "trader"
    WHALE = "whale"
    UNKNOWN = "unknown"

@dataclass
class ClassificationScore:
    """Holds classification results with confidence scores"""
    wallet_class: WalletClass
    confidence: float
    stage: AnalysisStage
    metrics: Dict[str, float]
    sub_scores: Dict[str, float]
    
    def __repr__(self):
        return f"{self.wallet_class.value}: {self.confidence:.2%} (Stage {self.stage.value})"

@dataclass
class WalletData:
    """Container for wallet transaction data"""
    address: str
    transactions: List[Dict[str, Any]]
    balance: float
    first_seen: int  # Unix timestamp
    last_seen: int   # Unix timestamp
    total_received: float
    total_sent: float
    transaction_count: int
    unique_counterparties: int
    chain: str = "ethereum"  # Default to Ethereum
    
class BaseClassifier(ABC):
    """Abstract base class for all wallet classifiers"""
    
    def __init__(self):
        self.thresholds = self.get_thresholds()
        self.weights = self.get_weights()
        
    @abstractmethod
    def get_thresholds(self) -> Dict[str, float]:
        """Return classification thresholds for each stage"""
        pass
    
    @abstractmethod
    def get_weights(self) -> Dict[str, float]:
        """Return metric weights for scoring"""
        pass
    
    @abstractmethod
    def calculate_stage1_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """Calculate primary metrics (5 metrics)"""
        pass
    
    @abstractmethod
    def calculate_stage2_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """Calculate secondary metrics (3 metrics)"""
        pass
    
    @abstractmethod
    def calculate_stage3_metrics(self, wallet_data: WalletData) -> Dict[str, float]:
        """Calculate context metrics (4 metrics)"""
        pass
    
    def classify(self, wallet_data: WalletData, stage: AnalysisStage = AnalysisStage.STAGE2) -> ClassificationScore:
        """
        Main classification method
        
        Args:
            wallet_data: Wallet transaction data
            stage: Analysis depth level
            
        Returns:
            ClassificationScore with results
        """
        metrics = {}
        
        # Calculate metrics based on stage
        stage1_metrics = self.calculate_stage1_metrics(wallet_data)
        metrics.update(stage1_metrics)
        
        if stage in [AnalysisStage.STAGE2, AnalysisStage.STAGE3]:
            stage2_metrics = self.calculate_stage2_metrics(wallet_data)
            metrics.update(stage2_metrics)
            
        if stage == AnalysisStage.STAGE3:
            stage3_metrics = self.calculate_stage3_metrics(wallet_data)
            metrics.update(stage3_metrics)
        
        # Calculate confidence score
        confidence = self.calculate_confidence(metrics, stage)
        
        # Determine wallet class
        wallet_class = self.determine_class(confidence, stage)
        
        # Calculate sub-scores for detailed analysis
        sub_scores = self.calculate_sub_scores(metrics)
        
        return ClassificationScore(
            wallet_class=wallet_class,
            confidence=confidence,
            stage=stage,
            metrics=metrics,
            sub_scores=sub_scores
        )
    
    def calculate_confidence(self, metrics: Dict[str, float], stage: AnalysisStage) -> float:
        """
        Calculate overall confidence score based on metrics and weights
        """
        if stage == AnalysisStage.STAGE1:
            # Only use primary metrics
            primary_weight = 1.0
            secondary_weight = 0.0
            context_weight = 0.0
        elif stage == AnalysisStage.STAGE2:
            # Use primary and secondary metrics
            primary_weight = self.weights.get('primary', 0.7)
            secondary_weight = self.weights.get('secondary', 0.3)
            context_weight = 0.0
        else:  # STAGE3
            # Use all metrics
            primary_weight = self.weights.get('primary', 0.5)
            secondary_weight = self.weights.get('secondary', 0.3)
            context_weight = self.weights.get('context', 0.2)
        
        # Calculate weighted scores
        primary_score = self._calculate_metric_group_score(metrics, 'primary')
        secondary_score = self._calculate_metric_group_score(metrics, 'secondary')
        context_score = self._calculate_metric_group_score(metrics, 'context')
        
        total_score = (
            primary_score * primary_weight +
            secondary_score * secondary_weight +
            context_score * context_weight
        )
        
        return min(1.0, max(0.0, total_score))
    
    def _calculate_metric_group_score(self, metrics: Dict[str, float], group: str) -> float:
        """Calculate average score for a group of metrics"""
        group_metrics = {k: v for k, v in metrics.items() if group in k}
        if not group_metrics:
            return 0.0
        return np.mean(list(group_metrics.values()))
    
    def determine_class(self, confidence: float, stage: AnalysisStage) -> WalletClass:
        """Determine wallet class based on confidence and stage thresholds"""
        threshold = self.thresholds.get(stage.value, 0.5)
        
        if confidence >= threshold:
            return self.get_wallet_class()
        return WalletClass.UNKNOWN
    
    @abstractmethod
    def get_wallet_class(self) -> WalletClass:
        """Return the specific wallet class this classifier identifies"""
        pass
    
    def calculate_sub_scores(self, metrics: Dict[str, float]) -> Dict[str, float]:
        """Calculate detailed sub-scores for analysis"""
        sub_scores = {}
        
        # Group metrics by type
        primary_metrics = {k: v for k, v in metrics.items() if 'primary' in k}
        secondary_metrics = {k: v for k, v in metrics.items() if 'secondary' in k}
        context_metrics = {k: v for k, v in metrics.items() if 'context' in k}
        
        if primary_metrics:
            sub_scores['primary_avg'] = np.mean(list(primary_metrics.values()))
            sub_scores['primary_max'] = np.max(list(primary_metrics.values()))
            sub_scores['primary_min'] = np.min(list(primary_metrics.values()))
            
        if secondary_metrics:
            sub_scores['secondary_avg'] = np.mean(list(secondary_metrics.values()))
            
        if context_metrics:
            sub_scores['context_avg'] = np.mean(list(context_metrics.values()))
        
        return sub_scores
