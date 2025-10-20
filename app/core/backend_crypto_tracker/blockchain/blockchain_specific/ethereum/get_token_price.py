"""
Ethereum Token Price Fetcher
✅ FIXED: Multi-layer RPC URL fallback system
"""

from datetime import datetime
from typing import Optional, Dict, Any
import aiohttp
import os
from web3 import Web3
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)

# ERC20 ABI für balanceOf und decimals
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function"
    }
]


def get_rpc_url(chain: str) -> Optional[str]:
    """
    ✅ FIX: Multi-layer RPC URL resolution strategy
    Versucht in dieser Reihenfolge:
    1. Environment Variable (höchste Priorität)
    2. scanner_config (Fallback)
    3. Hardcoded Default URLs (letzter Fallback)
    """
    rpc_url = None
    
    # Layer 1: Environment Variables (höchste Priorität)
    if chain == 'ethereum':
        rpc_url = os.getenv('ETHEREUM_RPC_URL')
    elif chain == 'bsc':
        rpc_url = os.getenv('BSC_RPC_URL')
    
    if rpc_url:
        logger.info(f"Using RPC URL from environment for {chain}: {rpc_url[:50]}...")
        return rpc_url
    
    # Layer 2: scanner_config (Fallback)
    try:
        from app.core.backend_crypto_tracker.config.scanner_config import scanner_config
        
        if chain == 'ethereum':
            rpc_url = scanner_config.rpc_config.ethereum_rpc
        elif chain == 'bsc':
            rpc_url = scanner_config.rpc_config.bsc_rpc
        
        if rpc_url and rpc_url != "https://mainnet.infura.io/v3/YOUR_INFURA_KEY":
            logger.info(f"Using RPC URL from scanner_config for {chain}")
            return rpc_url
    except Exception as e:
        logger.warning(f"Could not load RPC from scanner_config: {e}")
    
    # Layer 3: Hardcoded defaults (letzter Fallback)
    default_rpcs = {
        'ethereum': "https://mainnet.infura.io/v3/ad87a00e831d4ac6996e0a847f689c13",
        'bsc': "https://bsc-dataseed.binance.org/"
    }
    
    rpc_url = default_rpcs.get(chain)
    
    if rpc_url:
        logger.info(f"Using default fallback RPC URL for {chain}")
        return rpc_url
    
    logger.error(f"No RPC URL available for chain: {chain}")
    return None


async def get_onchain_metadata(token_address: str, rpc_url: str) -> Optional[Dict[str, Any]]:
    """
    Holt Token-Metadaten direkt von der Blockchain
    ✅ IMPROVED: Bessere Fehlerbehandlung und Logging
    """
    try:
        logger.info(f"Attempting to connect to RPC: {rpc_url[:50]}...")
        
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not w3.is_connected():
            logger.warning(f"Could not connect to Ethereum RPC: {rpc_url[:50]}...")
            return None
        
        logger.info(f"Successfully connected to RPC!")
        
        # Erstelle Contract-Instanz
        checksum_address = Web3.to_checksum_address(token_address)
        contract = w3.eth.contract(
            address=checksum_address,
            abi=ERC20_ABI
        )
        
        # Hole Token-Metadaten mit Fehlerbehandlung
        try:
            name = contract.functions.name().call()
        except Exception as e:
            logger.warning(f"Could not fetch name: {e}")
            name = "Unknown"
        
        try:
            symbol = contract.functions.symbol().call()
        except Exception as e:
            logger.warning(f"Could not fetch symbol: {e}")
            symbol = "UNKNOWN"
        
        try:
            decimals = contract.functions.decimals().call()
        except Exception as e:
            logger.warning(f"Could not fetch decimals: {e}")
            decimals = 18  # Default für ERC20
        
        try:
            total_supply = contract.functions.totalSupply().call()
        except Exception as e:
            logger.warning(f"Could not fetch totalSupply: {e}")
            total_supply = 0
        
        logger.info(f"✅ Found token on-chain: {name} ({symbol}), decimals: {decimals}")
        
        return {
            'name': name,
            'symbol': symbol,
            'decimals': decimals,
            'total_supply': total_supply
        }
        
    except Exception as e:
        logger.warning(f"Could not fetch token metadata from RPC: {e}")
        return None


async def execute_get_token_price(token_address: str, chain: str) -> Optional[TokenPriceData]:
    """
    Ethereum-spezifische Token-Preisabfrage
    ✅ FIXED: Multi-layer RPC URL resolution
    
    Strategie:
    1. Holt On-Chain-Metadaten (Name, Symbol, Decimals) via RPC
    2. Versucht Preis von CoinGecko zu holen
    3. Gibt Token-Daten zurück - auch OHNE Preis wenn on-chain gefunden
    """
    try:
        logger.info(f"Fetching price for {token_address} on {chain}")
        
        # ✅ FIX: Hole RPC URL mit Multi-Layer-Strategie
        rpc_url = get_rpc_url(chain)
        
        if not rpc_url:
            logger.error(f"No RPC URL available for {chain}. Cannot fetch on-chain data.")
            onchain_metadata = None
        else:
            # Schritt 1: Hole On-Chain-Metadaten
            onchain_metadata = await get_onchain_metadata(token_address, rpc_url)
        
        # Schritt 2: Versuche Preis von CoinGecko zu holen
        platform_map = {
            'ethereum': 'ethereum',
            'bsc': 'binance-smart-chain',
            'polygon': 'polygon-pos',
            'avalanche': 'avalanche'
        }
        
        platform = platform_map.get(chain.lower(), 'ethereum')
        
        url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
        params = {
            'contract_addresses': token_address.lower(),
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true'
        }
        
        coingecko_data = None
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        token_data = data.get(token_address.lower(), {})
                        
                        if token_data and 'usd' in token_data:
                            logger.info(f"✅ Successfully fetched price for {token_address} from CoinGecko")
                            coingecko_data = token_data
                        else:
                            logger.warning(f"No price data found for {token_address} on CoinGecko")
                    elif response.status == 429:
                        logger.warning(f"Rate limit exceeded for CoinGecko API")
                    else:
                        logger.warning(f"CoinGecko API returned status {response.status}")
        except Exception as e:
            logger.warning(f"Error fetching from CoinGecko: {e}")
        
        # Schritt 3: Kombiniere Daten
        # WICHTIG: Gib Token-Daten zurück, auch wenn CoinGecko keinen Preis hat!
        if onchain_metadata:
            # ✅ Token existiert on-chain!
            logger.info(f"✅ Returning token data (on-chain found)")
            return TokenPriceData(
                token_address=token_address,
                chain=chain,
                name=onchain_metadata.get('name', 'Unknown'),
                symbol=onchain_metadata.get('symbol', 'UNKNOWN'),
                decimals=onchain_metadata.get('decimals'),
                price=float(coingecko_data.get('usd', 0)) if coingecko_data else 0.0,
                market_cap=float(coingecko_data.get('usd_market_cap', 0)) if coingecko_data else 0.0,
                volume_24h=float(coingecko_data.get('usd_24h_vol', 0)) if coingecko_data else 0.0,
                price_change_24h=float(coingecko_data.get('usd_24h_change', 0)) if coingecko_data else 0.0,
                source="Ethereum RPC + CoinGecko" if coingecko_data else "Ethereum RPC (no price)",
                last_updated=datetime.now()
            )
        elif coingecko_data:
            # Nur CoinGecko-Daten verfügbar
            logger.info(f"✅ Returning token data (CoinGecko only)")
            return TokenPriceData(
                token_address=token_address,
                chain=chain,
                price=float(coingecko_data.get('usd', 0)),
                market_cap=float(coingecko_data.get('usd_market_cap', 0)),
                volume_24h=float(coingecko_data.get('usd_24h_vol', 0)),
                price_change_24h=float(coingecko_data.get('usd_24h_change', 0)),
                source="CoinGecko",
                last_updated=datetime.now()
            )
        else:
            # ❌ Weder on-chain noch CoinGecko
            logger.warning(f"Token {token_address} not found on-chain or in CoinGecko")
            return None
                    
    except Exception as e:
        logger.error(f"Error fetching Ethereum token price: {e}", exc_info=True)
        return None
