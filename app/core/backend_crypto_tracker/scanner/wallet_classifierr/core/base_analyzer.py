# ============================================================================
# core/base_analyzer.py
# ============================================================================
"""Base class for all wallet class analyzers."""

from typing import Dict, Any, List
from .stages_blockchain import Stage1_RawMetrics
from .stages import Stage2_DerivedMetrics, Stage3_ContextAnalysis


class BaseWalletAnalyzer:
    """Base class for wallet classification."""
    
    CLASS_NAME = "Base"
    METRICS = {}
    THRESHOLD = 0.5
    WEIGHTS = {"primary": 0.6, "secondary": 0.3, "context": 0.1}
    
    def __init__(self):
        self.stage1 = Stage1_RawMetrics()
        self.stage2 = Stage2_DerivedMetrics()
        self.stage3 = Stage3_ContextAnalysis()
    
    def analyze(
        self,
        address: str,
        blockchain_data: Dict[str, Any],
        context_db: Any = None,
        config: Dict[str, Any] = None
    ) -> float:
        """
        Perform complete analysis through all stages.
        
        Args:
            address: Wallet address
            blockchain_data: Raw blockchain transaction data
            context_db: Optional context database
            config: Optional configuration
            
        Returns:
            Classification score [0, 1]
        """
        # Stage 1: Raw metrics
        raw_metrics = self.stage1.execute(blockchain_data, config)
        
        # Stage 2: Derived metrics
        derived_metrics = self.stage2.execute(raw_metrics, config)
        
        # Stage 3: Context
        context_metrics = self.stage3.execute(derived_metrics, address, context_db)
        
        # Combine all metrics
        all_metrics = {**raw_metrics, **derived_metrics, **context_metrics}
        
        # Compute class-specific score
        score = self.compute_score(all_metrics)
        
        return score
    
    def compute_score(self, metrics: Dict[str, Any]) -> float:
        """
        Compute classification score from metrics.
        Must be implemented by subclasses.
        
        Args:
            metrics: Combined metrics from all stages
            
        Returns:
            Score [0, 1]
        """
        raise NotImplementedError("Subclasses must implement compute_score()")
    
    def is_class(self, score: float) -> bool:
        """
        Determine if score indicates membership in this class.
        
        Args:
            score: Classification score
            
        Returns:
            True if wallet belongs to this class
        """
        return score >= self.THRESHOLD
    
    def _avg(self, values: List[float]) -> float:
        """Calculate average, handling empty lists."""
        return sum(values) / len(values) if values else 0
    
    def _normalize(self, value: float, min_val: float, max_val: float) -> float:
        """Normalize value to [0, 1]."""
        if max_val == min_val:
            return 0.5
        return max(0, min(1, (value - min_val) / (max_val - min_val)))
