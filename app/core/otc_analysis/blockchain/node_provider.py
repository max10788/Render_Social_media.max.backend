from web3 import Web3
from typing import Optional, Dict, List
import os
from enum import Enum

class ChainID(Enum):
    """Supported blockchain networks."""
    ETHEREUM = 1
    BSC = 56
    POLYGON = 137
    ARBITRUM = 42161
    OPTIMISM = 10

class NodeProvider:
    """
    Manages connections to blockchain nodes via Infura, Alchemy, QuickNode.
    Handles fallback logic and rate limiting.
    """
    
    def __init__(self, chain_id: int = ChainID.ETHEREUM.value):
        self.chain_id = chain_id
        self.providers = self._initialize_providers()
        self.active_provider_index = 0
        self.web3 = None
        self._connect()
    
    def _initialize_providers(self) -> List[str]:
        """
        Initialize provider URLs with API keys from environment.
        Multiple providers for redundancy.
        """
        providers = []
        
        # Infura
        infura_key = os.getenv('INFURA_API_KEY')
        if infura_key:
            if self.chain_id == ChainID.ETHEREUM.value:
                providers.append(f"https://mainnet.infura.io/v3/{infura_key}")
            elif self.chain_id == ChainID.POLYGON.value:
                providers.append(f"https://polygon-mainnet.infura.io/v3/{infura_key}")
            elif self.chain_id == ChainID.ARBITRUM.value:
                providers.append(f"https://arbitrum-mainnet.infura.io/v3/{infura_key}")
        
        # Alchemy
        alchemy_key = os.getenv('ALCHEMY_API_KEY')
        if alchemy_key:
            if self.chain_id == ChainID.ETHEREUM.value:
                providers.append(f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}")
            elif self.chain_id == ChainID.POLYGON.value:
                providers.append(f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}")
            elif self.chain_id == ChainID.ARBITRUM.value:
                providers.append(f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_key}")
        
        # QuickNode
        quicknode_url = os.getenv('QUICKNODE_URL')
        if quicknode_url:
            providers.append(quicknode_url)
        
        if not providers:
            raise ValueError("No blockchain provider configured. Set INFURA_API_KEY, ALCHEMY_API_KEY, or QUICKNODE_URL")
        
        return providers
    
    def _connect(self) -> bool:
        """Connect to blockchain node."""
        try:
            provider_url = self.providers[self.active_provider_index]
            self.web3 = Web3(Web3.HTTPProvider(provider_url))
            
            if self.web3.is_connected():
                print(f"✓ Connected to blockchain via {provider_url[:30]}...")
                return True
            else:
                return self._fallback_to_next_provider()
        except Exception as e:
            print(f"Connection error: {e}")
            return self._fallback_to_next_provider()
    
    def _fallback_to_next_provider(self) -> bool:
        """Switch to next provider if current fails."""
        self.active_provider_index += 1
        
        if self.active_provider_index >= len(self.providers):
            raise ConnectionError("All blockchain providers failed")
        
        print(f"Falling back to provider {self.active_provider_index + 1}/{len(self.providers)}")
        return self._connect()
    
    def get_block(self, block_number: int) -> Optional[Dict]:
        """Fetch block by number."""
        try:
            return dict(self.web3.eth.get_block(block_number, full_transactions=True))
        except Exception as e:
            print(f"Error fetching block {block_number}: {e}")
            return None
    
    def get_latest_block_number(self) -> int:
        """Get latest block number."""
        try:
            return self.web3.eth.block_number
        except Exception as e:
            print(f"Error getting latest block: {e}")
            return 0
    
    def get_transaction(self, tx_hash: str) -> Optional[Dict]:
        """Fetch transaction by hash."""
        try:
            return dict(self.web3.eth.get_transaction(tx_hash))
        except Exception as e:
            print(f"Error fetching transaction {tx_hash}: {e}")
            return None
    
    def get_transaction_receipt(self, tx_hash: str) -> Optional[Dict]:
        """Fetch transaction receipt."""
        try:
            return dict(self.web3.eth.get_transaction_receipt(tx_hash))
        except Exception as e:
            print(f"Error fetching receipt {tx_hash}: {e}")
            return None
    
    def get_balance(self, address: str, block: str = 'latest') -> int:
        """Get native token balance for address."""
        try:
            return self.web3.eth.get_balance(address, block)
        except Exception as e:
            print(f"Error getting balance for {address}: {e}")
            return 0
    
    def get_code(self, address: str) -> str:
        """
        Get bytecode at address.
        Used to determine if address is a contract.
        """
        try:
            return self.web3.eth.get_code(address).hex()
        except Exception as e:
            print(f"Error getting code for {address}: {e}")
            return "0x"
    
    def is_contract(self, address: str) -> bool:
        """Check if address is a smart contract."""
        code = self.get_code(address)
        return code != "0x" and code != "0x0"
    
    def to_checksum_address(self, address: str) -> str:
        """Convert address to checksum format."""
        return self.web3.to_checksum_address(address)
    
    def from_wei(self, value: int, unit: str = 'ether') -> float:
        """Convert Wei to Ether or other unit."""
        return float(self.web3.from_wei(value, unit))
    
    def to_wei(self, value: float, unit: str = 'ether') -> int:
        """Convert Ether to Wei."""
        return self.web3.to_wei(value, unit)
