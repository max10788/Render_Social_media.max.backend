# app/core/backend_crypto_tracker/config/scanner_config.py
import os
from typing import Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum

class ConfidenceLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

@dataclass
class WalletClassifierConfig:
    cache_ttl: int = 3600  # 1 Stunde
    confidence_thresholds: Dict[ConfidenceLevel, float] = field(default_factory=lambda: {
        ConfidenceLevel.HIGH: 0.85,
        ConfidenceLevel.MEDIUM: 0.65,
        ConfidenceLevel.LOW: 0.45
    })

@dataclass
class OnChainAnalysisConfig:
    # --- DEX-Signaturen ---
    dex_signatures: Dict[str, str] = field(default_factory=lambda: {
        # --- Ethereum / EVM Chains ---
        # Uniswap V2
        'swapExactTokensForTokens': '0x38ed1739',
        'swapTokensForExactTokens': '0x8803dbee',
        'addLiquidity': '0xe8e33700',
        'removeLiquidity': '0xbaa2abde',
        # Uniswap V3
        'exactInputSingle': '0x414bf389', # swap
        'exactInput': '0xc04b8d59', # swap
        'mint': '0x3c8a7d8d', # add liquidity
        'burn': '0xa34123a7', # remove liquidity
        'collect': '0xfc6f7865', # collect fees
        # SushiSwap (ähnlich Uniswap V2)
        'sushi_swapExactTokensForTokens': '0x38ed1739',
        'sushi_addLiquidity': '0xe8e33700',
        # Balancer V2
        'swap': '0x52bbbe29',
        'joinPool': '0xb95cac28', # add liquidity
        'exitPool': '0x66dc5a08', # remove liquidity
        # Curve Finance
        'exchange': '0x3df02124', # swap
        'exchange_underlying': '0xa6417ed6', # swap
        'add_liquidity': '0x0b4c7e4d', # add liquidity
        'remove_liquidity': '0x5b36389c', # remove liquidity
        'remove_liquidity_imbalance': '0x2e7189ef', # remove liquidity
        'remove_liquidity_one_coin': '0x12aa3caf', # remove liquidity
        
        # --- Solana ---
        # Raydium AMM v3 / Raydium AMM v4
        'swapBaseIn': '66080e34', # swap
        'swapBaseOut': '931895e1', # swap
        'deposit': '273a0993', # add liquidity
        'withdraw': '26433c8c', # remove liquidity
        # Orca Whirlpool
        'swap': '0x00000000', # Placeholder, Orca uses program instructions, not function sigs
        'increaseLiquidity': '0x00000000', # Placeholder
        'decreaseLiquidity': '0x00000000', # Placeholder
        'collectFees': '0x00000000', # Placeholder
        
        # --- Sui ---
        # Placeholder - Sui uses Move Call objects, not signatures
        'sui_swap': '0x00000000', # Placeholder
        'sui_add_liquidity': '0x00000000', # Placeholder
        
        # --- Cross-Chain Bridges ---
        # Generic Bridge Swap
        'anySwapOut': '0xd22dc13a',
        'anySwapIn': '0x1d5c06e6',
        # Wormhole
        'completeTransfer': '0xc6878519',
        # LayerZero
        'send': '0x35b09a6e',
        'receive': '0x19ec6b95',
        
        # --- Yield Farming / Staking ---
        # Generic Staking
        'stake': '0xa694fc3a',
        'unstake': '0x2e1a7d4d',
        'claimRewards': '0x372500ab',
        # Generic Farming
        'deposit': '0xe2bbb158', # Often used for depositing LP tokens
        'withdraw': '0x441a34c9', # Often used for withdrawing LP tokens
        'harvest': '0x454a2ab3', # Claiming farming rewards
    })
    
    # --- CEX-Muster ---
    cex_patterns: Dict[str, Any] = field(default_factory=lambda: {
        # Transaktionshäufigkeit
        'high_tx_count': 10000, # Sehr hohe Anzahl an Transaktionen
        'tx_per_day_threshold': 500, # Viele Transaktionen pro Tag
        'batch_tx_threshold': 10, # Viele Transaktionen in kurzer Zeit
        
        # Betragsmuster
        'round_number_ratio': 0.3, # Anteil der Transaktionen mit runden Beträgen (z.B. 1.0, 10.0 ETH)
        'common_amounts': [1, 10, 100, 1000], # Typische Einzahlungsbeträge
        'small_amount_ratio': 0.7, # Anteil kleiner Beträge (Gebühren, Tests)
        
        # Zeitmuster
        'high_activity_hours': [0, 1, 2, 12, 18, 19, 20, 21, 22, 23], # Aktive Handelszeiten (UTC)
        'inter_arrival_time_seconds': 60, # Durchschnittliche Zeit zwischen Transaktionen in Sekunden (niedrig für aktiv)
        
        # Interaktionen
        'unique_counterparties_ratio': 0.9, # Interaktion mit sehr vielen verschiedenen Adressen
        'hot_cold_wallet_interactions': True, # Interaktion mit bekannten Hot/Cold Wallets
        'deposit_withdrawal_pairs': True, # Paare von Ein- und Auszahlungen
        
        # Netzwerkverhalten
        'gas_price_variance_low': True, # Konsistente, niedrige Gaspreise (automatisiert)
        'transaction_complexity_low': True, # Einfache Transaktionen (Transfer, nicht komplexe Contracts)
    })
    
    # --- Entwickler-Muster ---
    dev_patterns: Dict[str, Any] = field(default_factory=lambda: {
        # Contract Erstellung
        'contract_creation_early': True, # Contract wird kurz nach Token-Erstellung erstellt
        'contract_creation_tx_count': 1, # Typischerweise nur ein Creation-Tx
        'contract_admin_functions': ['renounceOwnership', 'transferOwnership', 'setFee', 'pause', 'unpause'],
        
        # Token-Aktivitäten
        'large_initial_mint': 0.5, # Anteil des Gesamtangebots, der an wenige Adressen geht
        'mint_burn_operations': True, # Minting/Burning von Tokens
        'token_distribution_skewed': True, # Ungleichmäßige Verteilung
        
        # Zeitliche Muster
        'early_activity': True, # Aktivität kurz nach Contract-Erstellung
        'test_transactions': True, # Kleine Test-Transaktionen vor dem Launch
        
        # Netzwerkverhalten
        'contract_interaction_ratio': 0.8, # Hohe Interaktion mit eigenem Contract
        'inter_team_transfers': True, # Transfers zu bekannten Team-Adressen (wenn bekannt)
        'contract_parameter_changes': True, # Änderungen an Contract-Parametern (Owner-only functions)
        
        # Risikomuster (negativ)
        'hidden_admin_keys': True, # Unbekannte Admin-Funktionen (schwer zu erkennen)
        'centralized_control': True, # Einzelne Adresse kontrolliert alles
    })
    
    # --- Liquidity Provider Muster ---
    lp_patterns: Dict[str, Any] = field(default_factory=lambda: {
        # Liquiditätsoperationen
        'add_remove_liquidity_ratio': 0.1, # Verhältnis von Add/Remove Liquidity Transaktionen
        'frequent_add_remove': True, # Häufige Hinzufügung/Entfernung von Liquidität
        'stable_pool_preference': True, # Bevorzugt Stablecoin-Pools (weniger IL)
        
        # Zeitliche Muster
        'daily_liquidity_ops': 1, # Durchschnittliche Liquiditätsoperationen pro Tag
        'rebalancing_behavior': True, # Anpassung der Positionen basierend auf Marktbedingungen
        'long_term_holding': True, # Liquidität wird über einen langen Zeitraum gehalten
        
        # Pool-Interaktionen
        'multiple_pool_interaction': True, # Interaktion mit mehreren Pools
        'pool_diversification': 5, # Mindestanzahl verschiedener Pools
        'yield_farming_claims': True, # Regelmäßiges Einsammeln von Farming-Belohnungen
        
        # Risikomuster
        'impermanent_loss_handling': True, # Verhalten bei Verlust (halten vs. entfernen)
        'correlation_risk': True, # Exposition gegenüber korrelierten Assets
    })
    
    # --- Rugpull-Muster ---
    rugpull_patterns: Dict[str, Any] = field(default_factory=lambda: {
        # Liquiditätsmuster
        'sudden_liquidity_removal': 0.9, # Anteil der Liquidität, der plötzlich entfernt wird
        'liquidity_removal_timing': 'post_launch', # Zeitpunkt der Liquiditätsentfernung
        'liquidity_concentration': 0.5, # Anteil der Liquidität in wenigen Adressen
        
        # Verkaufsmuster
        'large_sell_orders': True, # Große Verkäufe nach Preissteigerung
        'dump_pattern': True, # Schnelle, massive Verkäufe
        'transfer_to_burn_or_cex': True, # Transfer großer Mengen an Burn- oder CEX-Adressen
        
        # Contract-Muster
        'hidden_mint_function': True, # Unbekannte Mint-Funktionen
        'ownership_not_renounced': True, # Owner-Rechte nicht aufgegeben
        'mutable_contract': True, # Contract kann geändert werden
        
        # Netzwerkverhalten
        'interaction_with_scam_addresses': True, # Transaktionen mit bekannten Scam-Adressen
        'unusual_transaction_sequences': True, # Ungewöhnliche Abfolgen von Transaktionen
        'admin_function_abuse': True, # Missbrauch von Admin-Funktionen (z.B. Pausieren des Trades)
        
        # Zeitliche Muster
        'pre_rug_activity_spike': True, # Aktivitätsanstieg kurz vor dem Rugpull
        'post_launch_dump': True, # Dump kurz nach dem Launch/Liquidity Addition
    })

@dataclass
class SourceWeights:
    # Angepasste Werte, die sich zu 1.0 summieren
    ChainalysisIntegration: float = 0.35
    EllipticIntegration: float = 0.30
    CommunityLabelsAPI: float = 0.15
    OnChainAnalyzer: float = 0.10
    InternalLogic: float = 0.10
    
    def validate(self):
        total = sum(self.__dict__.values())
        if not (0.99 <= total <= 1.01):  # Kleine Toleranz für Rundungsfehler
            raise ValueError(f"Source weights must sum to 1.0, got {total}")

@dataclass
class RpcConfig:
    ethereum_rpc: str = "https://mainnet.infura.io/v3/YOUR_INFURA_KEY"
    bsc_rpc: str = "https://bsc-dataseed.binance.org/"
    solana_rpc: str = "https://api.mainnet-beta.solana.com"
    sui_rpc: str = "https://fullnode.mainnet.sui.io:443"
    
    # API Keys
    etherscan_api_key: str = "YOUR_ETHERSCAN_KEY"
    bscscan_api_key: str = "YOUR_BSCSCAN_KEY"
    coingecko_api_key: str = None
    helius_api_key: str = None
    sui_explorer_api_key: str = None
    
    # Bekannte Contract-Adressen
    known_contracts: Dict[str, Any] = field(default_factory=lambda: {
        'uniswap_v2_router': '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
        # ... restliche Contracts
    })
    
    # CEX Wallets
    cex_wallets: Dict[str, List[str]] = field(default_factory=lambda: {
        'binance': ['0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE'],
        # ... restliche Wallets
    })
    
    # Chain-spezifische Mindestscores
    min_scores: Dict[str, int] = field(default_factory=lambda: {
        'ethereum': 60,
        'bsc': 55,
        'solana': 50,
        'sui': 45
    })

@dataclass
class ScannerConfig:
    wallet_classifier: WalletClassifierConfig = field(default_factory=WalletClassifierConfig)
    onchain_analysis: OnChainAnalysisConfig = field(default_factory=OnChainAnalysisConfig)
    source_weights: SourceWeights = field(default_factory=SourceWeights)
    rpc_config: RpcConfig = field(default_factory=RpcConfig)
    
    def __post_init__(self):
        # Umgebungsvariablen überschreiben die Defaults
        self._load_from_env()
        # Validierung
        self.source_weights.validate()
    
    def _load_from_env(self):
        # Wallet Classifier
        self.wallet_classifier.cache_ttl = int(os.getenv('WALLET_CLASSIFIER_CACHE_TTL', self.wallet_classifier.cache_ttl))
        
        # RPC Config
        self.rpc_config.ethereum_rpc = os.getenv('ETHEREUM_RPC_URL', self.rpc_config.ethereum_rpc)
        self.rpc_config.etherscan_api_key = os.getenv('ETHERSCAN_API_KEY', self.rpc_config.etherscan_api_key)
        # ... restliche Umgebungsvariablen
        
        # Mindestscores
        self.rpc_config.min_scores['ethereum'] = int(os.getenv('ETHEREUM_MIN_SCORE', self.rpc_config.min_scores['ethereum']))
        # ... restliche Scores

# Globale Instanz
scanner_config = ScannerConfig()

# Konstanten für den Import in anderen Modulen
WALLET_CLASSIFIER_CONFIG = scanner_config.wallet_classifier
ONCHAIN_ANALYSIS_CONFIG = scanner_config.onchain_analysis
SOURCE_WEIGHTS = scanner_config.source_weights

def load_config(chain: str) -> Dict[str, Any]:
    """Lädt chain-spezifische Konfigurationen"""
    return {
        'min_score': scanner_config.rpc_config.min_scores.get(chain, 50),
        'rpc_url': getattr(scanner_config.rpc_config, f"{chain}_rpc", None),
        'api_key': getattr(scanner_config.rpc_config, f"{chain}_api_key", None),
        'known_contracts': scanner_config.rpc_config.known_contracts,
        'cex_wallets': scanner_config.rpc_config.cex_wallets
    }
