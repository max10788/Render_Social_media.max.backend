# ============================================================================
# core/stages.py - ENHANCED WITH PHASE 1 DERIVED METRICS
# ============================================================================
"""
Stage 2 & 3: Derived Metrics and Context Analysis
Enhanced with Phase 1 derived features

NEW FEATURES IN STAGE 2:
✅ DEX trading ratios and intensity metrics
✅ Portfolio complexity indicators
✅ Bot likelihood combined scores

Stage1 is in stages_blockchain.py
This file contains only Stage2 and Stage3
"""

from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# STAGE 2: DERIVED METRICS (Calculated Indicators)
# ============================================================================

class Stage2_DerivedMetrics:
    """
    Calculate derived metrics from raw metrics.
    
    Input: Raw metrics from Stage 1 (including 10 new Phase 1 metrics)
    Output: Calculated indicators and ratios
    """
    
    # In stages.py - Stage2_DerivedMetrics.execute()
    def execute(self, raw_metrics: Dict[str, Any], config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        logger.info(f"🧮 Computing derived metrics from {len(raw_metrics)} raw metrics")
        
        # Log input raw metrics
        key_metrics = ['total_tx_count', 'sent_tx_count', 'received_tx_count', 'current_balance']
        for metric in key_metrics:
            if metric in raw_metrics:
                logger.info(f"📈 Raw {metric}: {raw_metrics[metric]}")
            else:
                logger.warning(f"❌ Raw {metric} NOT FOUND")
        
        derived = {}
        
        # Transaction ratios
        total_tx = raw_metrics.get('total_tx_count', 0)
        logger.info(f"🔄 Computing ratios with total_tx: {total_tx}")
        
        if total_tx > 0:
            derived['outgoing_tx_ratio'] = raw_metrics.get('sent_tx_count', 0) / total_tx
            derived['incoming_tx_ratio'] = raw_metrics.get('received_tx_count', 0) / total_tx
            logger.info(f"✅ Computed tx ratios: outgoing={derived['outgoing_tx_ratio']:.3f}, incoming={derived['incoming_tx_ratio']:.3f}")
        else:
            derived['outgoing_tx_ratio'] = 0
            derived['incoming_tx_ratio'] = 0
            logger.warning("⚠️ total_tx is 0, setting ratios to 0")
        
        # Activity metrics
        age_days = raw_metrics.get('age_days', 0)
        if age_days > 0:
            derived['tx_per_month'] = (total_tx / age_days) * 30
            derived['activity_rate'] = total_tx / age_days
        else:
            derived['tx_per_month'] = 0
            derived['activity_rate'] = 0
        
        # Value metrics
        total_sent = raw_metrics.get('total_value_sent', 0)
        total_received = raw_metrics.get('total_value_received', 0)
        
        if total_received > 0:
            derived['balance_retention_ratio'] = raw_metrics.get('current_balance', 0) / total_received
        else:
            derived['balance_retention_ratio'] = 0
        
        # Network metrics
        derived['counterparty_diversity'] = (
            raw_metrics.get('unique_senders', 0) + raw_metrics.get('unique_receivers', 0)
        ) / (2 * total_tx) if total_tx > 0 else 0
        
        # Input/Output analysis
        avg_inputs = raw_metrics.get('avg_inputs_per_tx', 0)
        avg_outputs = raw_metrics.get('avg_outputs_per_tx', 0)
        
        if avg_outputs > 0:
            derived['consolidation_rate'] = avg_inputs / avg_outputs
        else:
            derived['consolidation_rate'] = 0
        
        # Fan-in/Fan-out scores
        in_degree = raw_metrics.get('in_degree', 0)
        out_degree = raw_metrics.get('out_degree', 0)
        
        derived['fan_in_score'] = min(10, in_degree / 10) if in_degree > 0 else 0
        derived['fan_out_score'] = min(10, out_degree / 10) if out_degree > 0 else 0
        
        # Dormancy
        last_active = raw_metrics.get('last_active_days', 0)
        if age_days > 0:
            derived['dormancy_ratio'] = last_active / age_days
        else:
            derived['dormancy_ratio'] = 0
        
        # ===== 🆕 PHASE 1 DERIVED METRICS =====
        
        # DEX trading intensity (uses new dex_swap_count)
        dex_swaps = raw_metrics.get('dex_swap_count', 0)
        if total_tx > 0:
            derived['dex_trading_ratio'] = dex_swaps / total_tx  # NEW ⭐
        else:
            derived['dex_trading_ratio'] = 0
        
        # Portfolio complexity (uses new unique_tokens_held)
        tokens_held = raw_metrics.get('unique_tokens_held', 0)
        derived['portfolio_complexity'] = min(1.0, tokens_held / 20)  # NEW ⭐
        
        # Bot likelihood score (combines 3 new bot metrics)
        timing_score = raw_metrics.get('tx_timing_precision_score', 0)
        gas_score = raw_metrics.get('gas_price_optimization_score', 0)
        pattern_score = raw_metrics.get('automated_pattern_score', 0)
        derived['bot_likelihood_score'] = (timing_score + gas_score + pattern_score) / 3  # NEW ⭐
        
        # DEX trader classification (derived from DEX metrics)
        if dex_swaps > 0:
            derived['is_dex_active'] = True
            derived['dex_activity_level'] = 'high' if dex_swaps > 20 else 'medium' if dex_swaps > 5 else 'low'
        else:
            derived['is_dex_active'] = False
            derived['dex_activity_level'] = 'none'
    
        logger.info(f"✅ Computed {len(derived)} derived metrics")
        return derived


# ============================================================================
# STAGE 3: CONTEXT ANALYSIS (External Data Integration)
# ============================================================================

class Stage3_ContextAnalysis:
    """
    Add context from external sources.
    
    Input: Derived metrics from Stage 2
    Output: Context-enriched metrics with Phase 1 classifications
    """
    
    def execute(
        self,
        derived_metrics: Dict[str, Any],
        address: str,
        context_db: Any = None
    ) -> Dict[str, Any]:
        """
        Execute Stage 3 analysis with Phase 1 context enhancements.
        
        Args:
            derived_metrics: Output from Stage 2
            address: Wallet address
            context_db: Optional context database
            
        Returns:
            Dictionary of context metrics
        """
        context = {}
        
        # ===== EXISTING CONTEXT METRICS =====
        
        # Check known entities
        if context_db:
            known_entity = context_db.get_known_entity(address)
            if known_entity:
                context['known_entity'] = known_entity.get('name', 'Unknown')
                context['entity_type'] = known_entity.get('type', 'Unknown')
                context['risk_level'] = known_entity.get('risk_level', 0)
            else:
                context['known_entity'] = None
                context['entity_type'] = 'Unknown'
                context['risk_level'] = 0
            
            # Check exchange interactions
            context['exchange_interaction_count'] = context_db.count_exchange_interactions(address)
            
            # Check mixer interactions
            context['known_mixer_interaction'] = context_db.check_mixer_interaction(address)
        else:
            context['known_entity'] = None
            context['entity_type'] = 'Unknown'
            context['risk_level'] = 0
            context['exchange_interaction_count'] = 0
            context['known_mixer_interaction'] = False
        
        # Network position (if available)
        context['eigenvector_centrality'] = 0.0  # Placeholder
        context['betweenness_centrality'] = 0.0  # Placeholder
        
        # ===== 🆕 PHASE 1 CONTEXT ENHANCEMENTS =====
        
        # Enhanced bot detection with context
        bot_score = derived_metrics.get('bot_likelihood_score', 0)
        if bot_score > 0.7:
            context['likely_bot'] = True
            context['bot_confidence'] = 'High'
            context['bot_risk_level'] = 'Medium'  # NEW
        elif bot_score > 0.5:
            context['likely_bot'] = True
            context['bot_confidence'] = 'Medium'
            context['bot_risk_level'] = 'Low'  # NEW
        else:
            context['likely_bot'] = False
            context['bot_confidence'] = 'Low'
            context['bot_risk_level'] = 'None'  # NEW
        
        # DEX trader classification
        dex_ratio = derived_metrics.get('dex_trading_ratio', 0)
        dex_activity = derived_metrics.get('is_dex_active', False)
        
        if dex_ratio > 0.5:
            context['primary_dex_trader'] = True
            context['trading_profile'] = 'Active DEX Trader'  # NEW
        elif dex_ratio > 0.2:
            context['primary_dex_trader'] = False
            context['trading_profile'] = 'Moderate DEX User'  # NEW
        else:
            context['primary_dex_trader'] = False
            context['trading_profile'] = 'CEX or Non-Trader'  # NEW
        
        # Portfolio classification
        portfolio_complexity = derived_metrics.get('portfolio_complexity', 0)
        if portfolio_complexity > 0.5:
            context['diversified_portfolio'] = True
            context['portfolio_type'] = 'Diversified'  # NEW
        elif portfolio_complexity > 0.2:
            context['diversified_portfolio'] = False
            context['portfolio_type'] = 'Moderate'  # NEW
        else:
            context['diversified_portfolio'] = False
            context['portfolio_type'] = 'Concentrated'  # NEW
        
        # Combined classification hints (for adaptive_classifier)
        # These help the classifier make better decisions
        context['classification_hints'] = {
            'strong_dex_signal': dex_ratio > 0.4,
            'strong_hodler_signal': dex_ratio < 0.05 and portfolio_complexity > 0.3,
            'strong_bot_signal': bot_score > 0.7,
            'whale_indicator': derived_metrics.get('portfolio_complexity', 0) > 0.7
        }
        
        return context


# ============================================================================
# CONVENIENCE FUNCTION (For compatibility)
# ============================================================================

def execute_full_pipeline(
    blockchain_data: Dict[str, Any],
    address: str,
    context_db: Any = None,
    config: Optional[Dict[str, Any]] = None,
    blockchain: str = 'ethereum'
) -> Dict[str, Any]:
    """
    Execute complete 3-stage pipeline.
    
    This is a convenience function that imports Stage1 from stages_blockchain.py
    
    Args:
        blockchain_data: Raw transaction data
        address: Wallet address
        context_db: Optional context database
        config: Optional configuration
        blockchain: Blockchain name
        
    Returns:
        Complete metrics dictionary with all stages
    """
    # Import Stage1 from stages_blockchain
    from .stages_blockchain import Stage1_RawMetrics
    
    # Stage 1
    raw_metrics = Stage1_RawMetrics.execute(blockchain_data, config, blockchain)
    
    # Stage 2
    stage2 = Stage2_DerivedMetrics()
    derived_metrics = stage2.execute(raw_metrics, config)
    
    # Stage 3
    stage3 = Stage3_ContextAnalysis()
    context_metrics = stage3.execute(derived_metrics, address, context_db)
    
    # Combine all
    return {
        **raw_metrics,
        **derived_metrics,
        **context_metrics
    }


# ============================================================================
# VALIDATION (For compatibility)
# ============================================================================

def validate_new_metrics(metrics: Dict[str, Any]) -> Dict[str, bool]:
    """
    Validate that all 10 new Phase 1 metrics are present.
    
    Returns:
        Dictionary showing which metrics are present
    """
    from .stages_blockchain import validate_phase1_metrics
    return validate_phase1_metrics(metrics)
