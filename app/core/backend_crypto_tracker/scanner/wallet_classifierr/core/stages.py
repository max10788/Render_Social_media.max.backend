# ============================================================================
# core/stages.py
# ============================================================================
"""Logic for the 3 analysis stages."""

from typing import Dict, Any, List
from datetime import datetime


class AnalysisStage:
    """Base class for analysis stages."""
    
    def execute(self, address_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the analysis stage."""
        raise NotImplementedError("Subclasses must implement execute()")


class Stage1_RawMetrics(AnalysisStage):
    """Stage 1: Extract basic data from blockchain raw data."""
    
    def execute(self, address_data: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Extract raw metrics from blockchain data.
        
        Args:
            address_data: Dictionary containing transaction history
            config: Optional configuration
            
        Returns:
            Dictionary of raw metrics
        """
        txs = address_data.get('txs', [])
        inputs = address_data.get('inputs', [])
        outputs = address_data.get('outputs', [])
        
        if not txs:
            return self._empty_metrics()
        
        # Basic transaction metrics
        tx_count = len(txs)
        total_received = sum(inp.get('value', 0) for inp in inputs)
        total_sent = sum(out.get('value', 0) for out in outputs)
        current_balance = address_data.get('balance', 0)
        
        # Temporal metrics
        timestamps = [tx.get('timestamp', 0) for tx in txs if tx.get('timestamp')]
        first_seen = min(timestamps) if timestamps else 0
        last_seen = max(timestamps) if timestamps else 0
        current_time = int(datetime.now().timestamp())
        
        # Input/Output analysis
        inputs_per_tx = {}
        outputs_per_tx = {}
        for tx in txs:
            tx_hash = tx.get('hash')
            tx_inputs = [inp for inp in inputs if inp.get('tx_hash') == tx_hash]
            tx_outputs = [out for out in outputs if out.get('tx_hash') == tx_hash]
            inputs_per_tx[tx_hash] = len(tx_inputs)
            outputs_per_tx[tx_hash] = len(tx_outputs)
        
        avg_inputs = sum(inputs_per_tx.values()) / len(inputs_per_tx) if inputs_per_tx else 0
        avg_outputs = sum(outputs_per_tx.values()) / len(outputs_per_tx) if outputs_per_tx else 0
        
        return {
            'tx_count': tx_count,
            'total_received': total_received,
            'total_sent': total_sent,
            'current_balance': current_balance,
            'first_seen': first_seen,
            'last_seen': last_seen,
            'age_days': (current_time - first_seen) / 86400 if first_seen else 0,
            'avg_inputs_per_tx': avg_inputs,
            'avg_outputs_per_tx': avg_outputs,
            'inputs_per_tx': inputs_per_tx,
            'outputs_per_tx': outputs_per_tx,
            'input_values': [inp.get('value', 0) for inp in inputs],
            'output_values': [out.get('value', 0) for out in outputs],
            'timestamps': timestamps
        }
    
    def _empty_metrics(self) -> Dict[str, Any]:
        """Return empty metrics for addresses with no data."""
        return {
            'tx_count': 0,
            'total_received': 0,
            'total_sent': 0,
            'current_balance': 0,
            'first_seen': 0,
            'last_seen': 0,
            'age_days': 0,
            'avg_inputs_per_tx': 0,
            'avg_outputs_per_tx': 0,
            'inputs_per_tx': {},
            'outputs_per_tx': {},
            'input_values': [],
            'output_values': [],
            'timestamps': []
        }


class Stage2_DerivedMetrics(AnalysisStage):
    """Stage 2: Calculate derived metrics from raw data."""
    
    def execute(self, stage1_output: Dict[str, Any], config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Calculate derived metrics from Stage 1 output.
        
        Args:
            stage1_output: Output from Stage 1
            config: Optional configuration (e.g., BTC price, thresholds)
            
        Returns:
            Dictionary of derived metrics
        """
        from .utils import (
            convert_to_usd, calculate_entropy, calculate_gini_coefficient,
            is_round_amount, normalize_score
        )
        
        btc_price = config.get('btc_price', 50000) if config else 50000
        current_time = int(datetime.now().timestamp())
        
        # Holding metrics
        holding_period = (current_time - stage1_output['first_seen']) / 86400 if stage1_output['first_seen'] else 0
        balance_retention = (
            stage1_output['current_balance'] / stage1_output['total_received']
            if stage1_output['total_received'] > 0 else 0
        )
        
        # Activity metrics
        outgoing_count = sum(1 for v in stage1_output['outputs_per_tx'].values() if v > 0)
        outgoing_ratio = outgoing_count / stage1_output['tx_count'] if stage1_output['tx_count'] > 0 else 0
        
        # Consolidation detection
        consolidation_count = sum(
            1 for tx_hash, inp_count in stage1_output['inputs_per_tx'].items()
            if inp_count >= 5 and stage1_output['outputs_per_tx'].get(tx_hash, 0) == 1
        )
        consolidation_rate = consolidation_count / stage1_output['tx_count'] if stage1_output['tx_count'] > 0 else 0
        
        # Value analysis
        input_values_usd = [convert_to_usd(v, btc_price) for v in stage1_output['input_values']]
        avg_input_value = sum(input_values_usd) / len(input_values_usd) if input_values_usd else 0
        
        # Mixer-like patterns
        output_values = stage1_output['output_values']
        equal_outputs = 0
        if output_values:
            from collections import Counter
            value_counts = Counter(output_values)
            equal_outputs = sum(1 for count in value_counts.values() if count > 1)
        
        equal_output_proportion = equal_outputs / len(output_values) if output_values else 0
        
        round_amounts = sum(1 for v in output_values if is_round_amount(v))
        round_amounts_ratio = round_amounts / len(output_values) if output_values else 0
        
        # Timing analysis
        timing_entropy = calculate_entropy(stage1_output['timestamps']) if stage1_output['timestamps'] else 0
        
        # Portfolio metrics
        portfolio_concentration = calculate_gini_coefficient(output_values) if output_values else 0
        
        # Trader metrics
        tx_per_month = (
            stage1_output['tx_count'] / (holding_period / 30)
            if holding_period > 30 else stage1_output['tx_count']
        )
        
        turnover_rate = (
            stage1_output['total_sent'] / stage1_output['current_balance']
            if stage1_output['current_balance'] > 0 else 0
        )
        
        return {
            'holding_period_days': holding_period,
            'balance_retention_ratio': balance_retention,
            'outgoing_tx_ratio': outgoing_ratio,
            'consolidation_rate': consolidation_rate,
            'avg_input_value_usd': avg_input_value,
            'equal_output_proportion': equal_output_proportion,
            'round_amounts_ratio': round_amounts_ratio,
            'timing_entropy': timing_entropy,
            'portfolio_concentration': portfolio_concentration,
            'tx_per_month': tx_per_month,
            'turnover_rate': turnover_rate,
            'total_value_usd': convert_to_usd(stage1_output['current_balance'], btc_price),
            'net_inflow_usd': convert_to_usd(
                stage1_output['total_received'] - stage1_output['total_sent'],
                btc_price
            )
        }


class Stage3_ContextAnalysis(AnalysisStage):
    """Stage 3: Add external context data."""
    
    def execute(
        self,
        stage2_output: Dict[str, Any],
        address: str,
        context_db: Any
    ) -> Dict[str, Any]:
        """
        Add context from external databases and network analysis.
        
        Args:
            stage2_output: Output from Stage 2
            address: The wallet address being analyzed
            context_db: Database/service providing context (exchanges, mixers, etc.)
            
        Returns:
            Dictionary of context metrics
        """
        context = {}
        
        # Exchange interactions
        context['is_exchange'] = context_db.is_exchange(address) if context_db else False
        context['exchange_interaction_count'] = (
            context_db.count_exchange_interactions(address) if context_db else 0
        )
        
        # Mixer interactions
        context['in_mixer_cluster'] = context_db.in_mixer_cluster(address) if context_db else False
        context['known_mixer_interaction'] = (
            context_db.interacts_with_mixer(address) if context_db else False
        )
        context['tornado_cash_interaction'] = (
            context_db.tornado_cash_interaction(address) if context_db else False
        )
        
        # Network analysis
        if context_db:
            context['in_degree'] = context_db.get_in_degree(address)
            context['out_degree'] = context_db.get_out_degree(address)
            context['betweenness_centrality'] = context_db.get_betweenness(address)
            context['eigenvector_centrality'] = context_db.get_eigenvector(address)
        else:
            context['in_degree'] = 0
            context['out_degree'] = 0
            context['betweenness_centrality'] = 0
            context['eigenvector_centrality'] = 0
        
        # Smart contract interactions
        context['smart_contract_calls'] = (
            context_db.count_smart_contract_calls(address) if context_db else 0
        )
        context['dex_cex_interactions'] = (
            context_db.count_dex_cex_interactions(address) if context_db else 0
        )
        
        # Institutional/Whale markers
        context['institutional_wallet'] = (
            context_db.is_institutional(address) if context_db else False
        )
        context['whale_cluster_member'] = (
            context_db.in_whale_cluster(address) if context_db else False
        )
        
        # Cluster analysis
        context['cluster_size'] = context_db.get_cluster_size(address) if context_db else 1
        
        return context
