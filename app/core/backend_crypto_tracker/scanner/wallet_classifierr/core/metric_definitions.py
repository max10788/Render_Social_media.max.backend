# ============================================================================
# core/metric_definitions.py
# ============================================================================
"""Central definition of all 60 metrics (12 per class)."""

DUST_SWEEPER_METRICS = {
    "primary": [
        "input_count_per_tx",           # Avg number of inputs per transaction
        "avg_input_value_usd",          # Avg value of inputs (should be <100 USD)
        "tx_consolidation_rate",        # Ratio of consolidation transactions
        "inputs_gte_5_ratio",           # Ratio of txs with ≥5 inputs
        "single_output_ratio"           # Ratio of txs with exactly 1 output
    ],
    "secondary": [
        "dust_aggregation_frequency",   # Frequency of dust aggregation events
        "time_between_inputs_avg",      # Avg time between inputs
        "input_source_diversity"        # Diversity of input sources (entropy)
    ],
    "context": [
        "known_dust_service_interaction",  # Interaction with known dust services
        "change_address_reuse_ratio",      # Reuse of change addresses
        "cluster_size",                    # Size of address cluster
        "in_degree_centrality"             # Network in-degree
    ]
}

HODLER_METRICS = {
    "primary": [
        "holding_period_days",          # Days since first received (≥365)
        "balance_retention_ratio",      # Current balance / total received (≥0.9)
        "outgoing_tx_ratio",            # Outgoing / total txs (<0.1)
        "utxo_age_avg",                 # Avg age of UTXOs in days
        "last_outgoing_tx_age_days"     # Days since last outgoing tx
    ],
    "secondary": [
        "balance_stability_index",      # Variance of balance over time
        "inactive_days_ratio",          # Days inactive / total days
        "value_growth_vs_market"        # Growth vs. BTC market growth
    ],
    "context": [
        "exchange_interaction_count",   # Number of exchange interactions
        "smart_contract_calls",         # Number of smart contract calls
        "out_degree",                   # Network out-degree (low for hodlers)
        "isolation_score"               # How isolated the address is
    ]
}

MIXER_METRICS = {
    "primary": [
        "equal_output_proportion",      # Proportion of equal-value outputs
        "known_mixer_interaction",      # Binary: interacts with known mixers
        "coinjoin_frequency",           # Frequency of CoinJoin-like patterns
        "round_amounts_ratio",          # Ratio of round-number amounts
        "high_input_count_ratio"        # Ratio of txs with many inputs
    ],
    "secondary": [
        "timing_entropy",               # Entropy of transaction timing
        "output_uniformity_score",      # How uniform outputs are
        "path_complexity"               # Complexity of transaction paths
    ],
    "context": [
        "tornado_cash_interaction",     # Interaction with Tornado Cash
        "betweenness_centrality",       # Network betweenness
        "mixed_output_reuse_ratio",     # Reuse of mixed outputs
        "cluster_fragmentation"         # How fragmented the cluster is
    ]
}

TRADER_METRICS = {
    "primary": [
        "tx_count_per_month",           # Transactions per month (≥10)
        "bidirectional_flow_ratio",     # Ratio of bidirectional flows
        "exchange_interaction_freq",    # Frequency of exchange interactions
        "avg_tx_value_usd",             # Average transaction value
        "short_holding_time_ratio"      # Ratio of coins held <30 days
    ],
    "secondary": [
        "volatility_exposure",          # Exposure to price volatility
        "turnover_rate",                # Trading turnover rate
        "profit_loss_cycles"            # Number of profit/loss cycles
    ],
    "context": [
        "dex_cex_smart_contract_calls", # DEX/CEX smart contract interactions
        "out_degree",                   # High network out-degree
        "token_diversity",              # Number of different tokens traded
        "bridge_usage_count"            # Cross-chain bridge usage
    ]
}

WHALE_METRICS = {
    "primary": [
        "total_value_usd",              # Total value (top 1%)
        "single_tx_over_1m_count",      # Count of txs ≥$1M
        "portfolio_concentration",      # Gini coefficient of holdings
        "net_inflow_usd",               # Net inflow in USD
        "address_age_days"              # Age of the address
    ],
    "secondary": [
        "market_impact_estimate",       # Estimated market impact
        "liquidity_absorption_ratio",   # Ratio of liquidity absorbed
        "whale_cluster_membership"      # Member of known whale cluster
    ],
    "context": [
        "institutional_wallet_interaction",  # Interaction with institutions
        "governance_participation_count",    # DAO governance participation
        "cross_chain_presence",              # Presence on multiple chains
        "eigenvector_centrality"             # Network eigenvector centrality
    ]
}

