# app/core/backend_crypto_tracker/config/blockchain_api_keys.py

"""
API-Schlüssel-Konfiguration für verschiedene Blockchain-Provider.
Lädt alle Keys aus Umgebungsvariablen (.env-Datei).
"""

import os
from typing import Dict, Optional, List
from dataclasses import dataclass


@dataclass
class APIKeys:
    """Zentrale Verwaltung aller API-Schlüssel"""
    
    # Blockchain-Explorer APIs
    etherscan_api_key: Optional[str] = None
    bscscan_api_key: Optional[str] = None
    polygonscan_api_key: Optional[str] = None
    
    # Price & Market Data APIs
    coingecko_api_key: Optional[str] = None
    coinmarketcap_api_key: Optional[str] = None
    cryptocompare_api_key: Optional[str] = None
    
    # Exchange APIs
    binance_api_key: Optional[str] = None
    binance_secret_key: Optional[str] = None
    coinbase_api_key: Optional[str] = None
    coinbase_secret_key: Optional[str] = None
    kraken_api_key: Optional[str] = None
    kraken_secret_key: Optional[str] = None
    bitget_api_key: Optional[str] = None
    bitget_secret_key: Optional[str] = None
    
    # On-Chain Data APIs
    moralis_api_key: Optional[str] = None
    alchemy_api_key: Optional[str] = None
    infura_api_key: Optional[str] = None
    bitquery_api_key: Optional[str] = None
    
    # Blockchain RPCs
    ethereum_rpc: Optional[str] = None
    bsc_rpc: Optional[str] = None
    solana_rpc: Optional[str] = None
    sui_rpc: Optional[str] = None
    
    # Specialized APIs
    helius_api_key: Optional[str] = None  # Solana
    sui_explorer_api_key: Optional[str] = None
    chainalysis_api_key: Optional[str] = None
    elliptic_api_key: Optional[str] = None
    
    def __post_init__(self):
        """Lädt alle API-Schlüssel aus Umgebungsvariablen"""
        self._load_from_environment()
    
    def _load_from_environment(self):
        """Lädt API-Schlüssel aus .env-Datei"""
        
        # Blockchain Explorer APIs
        self.etherscan_api_key = os.getenv('ETHERSCAN_API_KEY')
        self.bscscan_api_key = os.getenv('BSCSCAN_API_KEY')
        self.polygonscan_api_key = os.getenv('POLYGONSCAN_API_KEY')
        
        # Price & Market Data APIs
        self.coingecko_api_key = os.getenv('COINGECKO_API_KEY')
        self.coinmarketcap_api_key = os.getenv('COINMARKETCAP_API_KEY')
        self.cryptocompare_api_key = os.getenv('CRYPTOCOMPARE_API_KEY')
        
        # Exchange APIs
        self.binance_api_key = os.getenv('BINANCE_API_KEY')
        self.binance_secret_key = os.getenv('BINANCE_SECRET_KEY')
        self.coinbase_api_key = os.getenv('COINBASE_API_KEY')
        self.coinbase_secret_key = os.getenv('COINBASE_SECRET_KEY')
        self.kraken_api_key = os.getenv('KRAKEN_API_KEY')
        self.kraken_secret_key = os.getenv('KRAKEN_SECRET_KEY')
        self.bitget_api_key = os.getenv('BITGET_API_KEY')
        self.bitget_secret_key = os.getenv('BITGET_SECRET_KEY')
        
        # On-Chain Data APIs
        self.moralis_api_key = os.getenv('MORALIS_API_KEY')
        self.alchemy_api_key = os.getenv('ALCHEMY_API_KEY')
        self.infura_api_key = os.getenv('INFURA_API_KEY')
        self.bitquery_api_key = os.getenv('BITQUERY_API_KEY')
        
        # Blockchain RPCs
        self.ethereum_rpc = os.getenv('ETHEREUM_RPC_URL', 'https://eth.public-rpc.com')
        self.bsc_rpc = os.getenv('BSC_RPC_URL', 'https://bsc-dataseed1.binance.org')
        self.solana_rpc = os.getenv('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com')
        self.sui_rpc = os.getenv('SUI_RPC_URL', 'https://fullnode.mainnet.sui.io:443')
        
        # Specialized APIs
        self.helius_api_key = os.getenv('HELIUS_API_KEY')
        self.sui_explorer_api_key = os.getenv('SUI_EXPLORER_API_KEY')
        self.chainalysis_api_key = os.getenv('CHAINALYSIS_API_KEY')
        self.elliptic_api_key = os.getenv('ELLIPTIC_API_KEY')
    
    def get_available_providers(self) -> Dict[str, List[str]]:
        """Gibt verfügbare Provider basierend auf konfigurierten API-Schlüsseln zurück"""
        available = {
            'blockchain_explorers': [],
            'price_providers': [],
            'exchange_providers': [],
            'onchain_providers': []
        }
        
        # Blockchain Explorers
        if self.etherscan_api_key:
            available['blockchain_explorers'].append('etherscan')
        if self.bscscan_api_key:
            available['blockchain_explorers'].append('bscscan')
        if self.polygonscan_api_key:
            available['blockchain_explorers'].append('polygonscan')
        
        # Price Providers
        if self.coingecko_api_key:
            available['price_providers'].append('coingecko')
        if self.coinmarketcap_api_key:
            available['price_providers'].append('coinmarketcap')
        if self.cryptocompare_api_key:
            available['price_providers'].append('cryptocompare')
        
        # Exchange Providers
        if self.binance_api_key and self.binance_secret_key:
            available['exchange_providers'].append('binance')
        if self.coinbase_api_key and self.coinbase_secret_key:
            available['exchange_providers'].append('coinbase')
        if self.kraken_api_key and self.kraken_secret_key:
            available['exchange_providers'].append('kraken')
        if self.bitget_api_key and self.bitget_secret_key:
            available['exchange_providers'].append('bitget')
        
        # On-Chain Providers
        if self.moralis_api_key:
            available['onchain_providers'].append('moralis')
        if self.alchemy_api_key:
            available['onchain_providers'].append('alchemy')
        if self.bitquery_api_key:
            available['onchain_providers'].append('bitquery')
        
        return available
    
    def validate_configuration(self) -> Dict[str, bool]:
        """Validiert die API-Konfiguration und gibt Statusbericht zurück"""
        status = {
            'has_blockchain_explorer': bool(self.etherscan_api_key or self.bscscan_api_key),
            'has_price_provider': bool(self.coingecko_api_key or self.coinmarketcap_api_key),
            'has_onchain_provider': bool(self.moralis_api_key or self.bitquery_api_key),
            'has_exchange_api': bool(
                (self.binance_api_key and self.binance_secret_key) or
                (self.coinbase_api_key and self.coinbase_secret_key)
            ),
            'ethereum_ready': bool(self.etherscan_api_key or self.alchemy_api_key),
            'bsc_ready': bool(self.bscscan_api_key),
            'solana_ready': bool(self.helius_api_key or self.solana_rpc),
            'sui_ready': bool(self.sui_explorer_api_key or self.sui_rpc),
        }
        
        return status
    
    def get_missing_keys(self) -> List[str]:
        """Gibt eine Liste fehlender wichtiger API-Schlüssel zurück"""
        missing = []
        
        # Kritische APIs
        if not self.etherscan_api_key:
            missing.append('ETHERSCAN_API_KEY (for Ethereum chain analysis)')
        
        if not (self.coingecko_api_key or self.coinmarketcap_api_key):
            missing.append('Price provider (COINGECKO_API_KEY or COINMARKETCAP_API_KEY)')
        
        if not (self.moralis_api_key or self.bitquery_api_key):
            missing.append('On-chain data provider (MORALIS_API_KEY or BITQUERY_API_KEY)')
        
        # Empfohlene APIs
        if not self.bscscan_api_key:
            missing.append('BSCSCAN_API_KEY (recommended for BSC support)')
        
        if not self.helius_api_key:
            missing.append('HELIUS_API_KEY (recommended for Solana support)')
        
        return missing


# Globale Instanz
api_keys = APIKeys()


def get_api_keys() -> APIKeys:
    """Gibt die globale APIKeys-Instanz zurück"""
    return api_keys


def print_configuration_status():
    """Druckt den Konfigurationsstatus aus"""
    status = api_keys.validate_configuration()
    missing = api_keys.get_missing_keys()
    available = api_keys.get_available_providers()
    
    print("\n" + "="*50)
    print("BLOCKCHAIN API CONFIGURATION STATUS")
    print("="*50)
    
    print("\n✅ Available Providers:")
    for category, providers in available.items():
        if providers:
            print(f"  {category}: {', '.join(providers)}")
    
    print("\n🔧 Configuration Status:")
    for key, value in status.items():
        status_icon = "✅" if value else "❌"
        print(f"  {status_icon} {key}: {'OK' if value else 'Missing'}")
    
    if missing:
        print("\n⚠️  Missing API Keys:")
        for key in missing:
            print(f"  - {key}")
    else:
        print("\n🎉 All critical API keys are configured!")
    
    print("="*50)


# Automatische Konfigurationsprüfung beim Import (nur in Development)
if __name__ == "__main__":
    print_configuration_status()
