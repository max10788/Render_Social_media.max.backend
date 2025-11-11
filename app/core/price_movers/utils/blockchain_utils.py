"""
Blockchain Utilities - Wallet Address Validation & Explorer URLs

Unterstützt:
- Solana (Base58)
- Ethereum/EVM (0x + Hex)
- Bitcoin (Base58)
"""

import re
import logging
from typing import Optional, Dict

from app.core.price_movers.utils.constants import (
    BlockchainNetwork,
    BLOCKCHAIN_EXPLORERS,
    WALLET_ADDRESS_PATTERNS
)


logger = logging.getLogger(__name__)


# ==================== WALLET ADDRESS VALIDATION ====================

def validate_wallet_address(address: str, blockchain: BlockchainNetwork) -> bool:
    """
    Validiert eine Wallet-Adresse für eine spezifische Blockchain
    
    Args:
        address: Wallet-Adresse
        blockchain: Blockchain Network (solana/ethereum/bsc/etc.)
        
    Returns:
        True wenn valide, False sonst
    """
    if not address:
        return False
    
    try:
        # Get pattern for blockchain
        pattern_config = WALLET_ADDRESS_PATTERNS.get(blockchain)
        
        if not pattern_config:
            logger.warning(f"No validation pattern for blockchain: {blockchain}")
            return True  # Fallback: Accept if no pattern defined
        
        # Check length
        expected_length = pattern_config.get('length')
        if expected_length and len(address) != expected_length:
            return False
        
        # Check prefix
        expected_prefix = pattern_config.get('prefix')
        if expected_prefix and not address.startswith(expected_prefix):
            return False
        
        # Blockchain-specific validation
        if blockchain == BlockchainNetwork.SOLANA:
            return validate_solana_address(address)
        elif blockchain in [BlockchainNetwork.ETHEREUM, BlockchainNetwork.BSC, BlockchainNetwork.POLYGON]:
            return validate_evm_address(address)
        
        # Default: Basic validation passed
        return True
        
    except Exception as e:
        logger.error(f"Wallet validation error: {e}")
        return False


def validate_solana_address(address: str) -> bool:
    """
    Validiert Solana Wallet-Adresse (Base58)
    
    Solana addresses are:
    - 32-44 characters long
    - Base58 encoded (no 0, O, I, l)
    
    Args:
        address: Solana address
        
    Returns:
        True wenn valide
    """
    if not address:
        return False
    
    # Length check
    if len(address) < 32 or len(address) > 44:
        return False
    
    # Base58 alphabet (no 0, O, I, l to avoid confusion)
    base58_pattern = r'^[1-9A-HJ-NP-Za-km-z]+$'
    
    if not re.match(base58_pattern, address):
        return False
    
    return True


def validate_evm_address(address: str) -> bool:
    """
    Validiert EVM-kompatible Wallet-Adresse (Ethereum, BSC, Polygon)
    
    EVM addresses are:
    - 42 characters (including 0x prefix)
    - Hexadecimal (0x + 40 hex chars)
    
    Args:
        address: EVM address
        
    Returns:
        True wenn valide
    """
    if not address:
        return False
    
    # Check format: 0x + 40 hex characters
    evm_pattern = r'^0x[a-fA-F0-9]{40}$'
    
    if not re.match(evm_pattern, address):
        return False
    
    return True


def validate_bitcoin_address(address: str) -> bool:
    """
    Validiert Bitcoin Wallet-Adresse
    
    Bitcoin addresses can be:
    - Legacy (P2PKH): Starts with 1
    - Script (P2SH): Starts with 3
    - SegWit (Bech32): Starts with bc1
    
    Args:
        address: Bitcoin address
        
    Returns:
        True wenn valide
    """
    if not address:
        return False
    
    # Legacy P2PKH (starts with 1)
    if address.startswith('1'):
        return 26 <= len(address) <= 35
    
    # Script P2SH (starts with 3)
    elif address.startswith('3'):
        return 26 <= len(address) <= 35
    
    # SegWit Bech32 (starts with bc1)
    elif address.startswith('bc1'):
        return 42 <= len(address) <= 62
    
    return False


# ==================== EXPLORER URL GENERATION ====================

def get_wallet_explorer_url(address: str, blockchain: BlockchainNetwork) -> Optional[str]:
    """
    Generiert Explorer-URL für eine Wallet-Adresse
    
    Args:
        address: Wallet-Adresse
        blockchain: Blockchain Network
        
    Returns:
        Explorer URL oder None
    """
    if not address:
        return None
    
    try:
        explorer_config = BLOCKCHAIN_EXPLORERS.get(blockchain)
        
        if not explorer_config:
            logger.warning(f"No explorer config for blockchain: {blockchain}")
            return None
        
        wallet_url_template = explorer_config.get('wallet_url')
        
        if not wallet_url_template:
            return None
        
        return wallet_url_template.format(address=address)
        
    except Exception as e:
        logger.error(f"Failed to generate wallet explorer URL: {e}")
        return None


def get_transaction_explorer_url(signature: str, blockchain: BlockchainNetwork) -> Optional[str]:
    """
    Generiert Explorer-URL für eine Transaktion
    
    Args:
        signature: Transaction hash/signature
        blockchain: Blockchain Network
        
    Returns:
        Explorer URL oder None
    """
    if not signature:
        return None
    
    try:
        explorer_config = BLOCKCHAIN_EXPLORERS.get(blockchain)
        
        if not explorer_config:
            logger.warning(f"No explorer config for blockchain: {blockchain}")
            return None
        
        # Try tx_url first, fallback to signature_url
        tx_url_template = explorer_config.get('tx_url') or explorer_config.get('signature_url')
        
        if not tx_url_template:
            return None
        
        # Format with appropriate parameter name
        try:
            return tx_url_template.format(signature=signature)
        except KeyError:
            # Try with 'hash' parameter instead
            try:
                return tx_url_template.format(hash=signature)
            except KeyError:
                logger.error(f"Transaction URL template has unknown parameters")
                return None
        
    except Exception as e:
        logger.error(f"Failed to generate transaction explorer URL: {e}")
        return None


def get_token_explorer_url(token_address: str, blockchain: BlockchainNetwork) -> Optional[str]:
    """
    Generiert Explorer-URL für einen Token
    
    Args:
        token_address: Token Contract Address / Mint
        blockchain: Blockchain Network
        
    Returns:
        Explorer URL oder None
    """
    if not token_address:
        return None
    
    try:
        explorer_config = BLOCKCHAIN_EXPLORERS.get(blockchain)
        
        if not explorer_config:
            return None
        
        token_url_template = explorer_config.get('token_url')
        
        if not token_url_template:
            return None
        
        # Format with appropriate parameter name
        try:
            return token_url_template.format(mint=token_address)
        except KeyError:
            # Try with 'address' parameter instead
            try:
                return token_url_template.format(address=token_address)
            except KeyError:
                logger.error(f"Token URL template has unknown parameters")
                return None
        
    except Exception as e:
        logger.error(f"Failed to generate token explorer URL: {e}")
        return None


# ==================== ADDRESS FORMATTING ====================

def shorten_address(address: str, start_chars: int = 8, end_chars: int = 6) -> str:
    """
    Kürzt eine Wallet-Adresse für Display
    
    z.B. 7xKXtg2CW87d97TXJSDpb4j5NzWZn9XsxUBmkVX
         → 7xKXtg2C...BmkVX
    
    Args:
        address: Full address
        start_chars: Anzahl Zeichen am Anfang
        end_chars: Anzahl Zeichen am Ende
        
    Returns:
        Gekürzte Adresse
    """
    if not address:
        return ""
    
    if len(address) <= (start_chars + end_chars + 3):
        return address
    
    return f"{address[:start_chars]}...{address[-end_chars:]}"


def detect_blockchain(address: str) -> Optional[BlockchainNetwork]:
    """
    Erkennt Blockchain anhand der Adress-Format
    
    Args:
        address: Wallet-Adresse
        
    Returns:
        BlockchainNetwork oder None
    """
    if not address:
        return None
    
    address_lower = address.lower()
    
    # Ethereum/EVM (0x prefix, 42 chars)
    if address_lower.startswith('0x') and len(address) == 42:
        return BlockchainNetwork.ETHEREUM  # Could also be BSC/Polygon
    
    # Bitcoin
    if address_lower.startswith(('1', '3', 'bc1')):
        # Check length for validation
        if 26 <= len(address) <= 62:
            return BlockchainNetwork.ETHEREUM  # Note: Bitcoin not in enum
    
    # Solana (Base58, 32-44 chars, no 0x prefix)
    if len(address) >= 32 and len(address) <= 44 and not address_lower.startswith('0x'):
        # Check if valid Base58
        if validate_solana_address(address):
            return BlockchainNetwork.SOLANA
    
    return None


def format_explorer_link(address: str, blockchain: Optional[BlockchainNetwork] = None) -> Dict[str, str]:
    """
    Generiert alle Explorer-Links für eine Adresse
    
    Args:
        address: Wallet-Adresse
        blockchain: Blockchain (optional, wird auto-detected wenn None)
        
    Returns:
        Dictionary mit Explorer-Infos
    """
    if not blockchain:
        blockchain = detect_blockchain(address)
    
    if not blockchain:
        return {
            'address': address,
            'blockchain': 'unknown',
            'wallet_url': None,
            'is_valid': False
        }
    
    wallet_url = get_wallet_explorer_url(address, blockchain)
    is_valid = validate_wallet_address(address, blockchain)
    
    explorer_config = BLOCKCHAIN_EXPLORERS.get(blockchain, {})
    
    return {
        'address': address,
        'address_short': shorten_address(address),
        'blockchain': blockchain.value,
        'explorer_name': explorer_config.get('name', 'Unknown'),
        'wallet_url': wallet_url,
        'is_valid': is_valid
    }


# ==================== UTILITY FUNCTIONS ====================

def is_contract_address(address: str, blockchain: BlockchainNetwork) -> bool:
    """
    Prüft ob Adresse ein Smart Contract ist (Heuristik)
    
    HINWEIS: Dies ist eine simple Heuristik. Für echte Contract-Detection
    muss die Blockchain abgefragt werden (z.B. eth_getCode für Ethereum)
    
    Args:
        address: Adresse
        blockchain: Blockchain
        
    Returns:
        True wenn wahrscheinlich ein Contract
    """
    # Für EVM: Contracts oft mit bestimmten Patterns
    if blockchain in [BlockchainNetwork.ETHEREUM, BlockchainNetwork.BSC, BlockchainNetwork.POLYGON]:
        # Simple Heuristik: Prüfe auf bekannte Contract-Patterns
        # Real implementation würde RPC call machen
        pass
    
    # Für Solana: Program Accounts haben spezielle Eigenschaften
    # Real implementation würde RPC call machen
    
    return False  # Fallback: Assume wallet


def normalize_address(address: str, blockchain: BlockchainNetwork) -> str:
    """
    Normalisiert eine Adresse (z.B. lowercase für EVM)
    
    Args:
        address: Adresse
        blockchain: Blockchain
        
    Returns:
        Normalisierte Adresse
    """
    if not address:
        return address
    
    # EVM: Lowercase (außer checksum addresses)
    if blockchain in [BlockchainNetwork.ETHEREUM, BlockchainNetwork.BSC, BlockchainNetwork.POLYGON]:
        # Keep original for checksum validation, but store lowercase
        return address.lower()
    
    # Solana: Case-sensitive, keine Änderung
    return address


# ==================== EXPORTS ====================

__all__ = [
    'validate_wallet_address',
    'validate_solana_address',
    'validate_evm_address',
    'validate_bitcoin_address',
    'get_wallet_explorer_url',
    'get_transaction_explorer_url',
    'get_token_explorer_url',
    'shorten_address',
    'detect_blockchain',
    'format_explorer_link',
    'is_contract_address',
    'normalize_address',
]
