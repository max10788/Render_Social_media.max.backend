"""
Ethereum Token Price Fetcher
✅ FIXED: Multi-RPC-Provider Fallback System mit Request Session
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
import aiohttp
import os
from web3 import Web3
from web3.providers import HTTPProvider
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


def get_rpc_urls(chain: str) -> List[str]:
    """
    ✅ NEW: Multi-RPC-Provider mit Fallback-Liste
    Gibt mehrere RPC-URLs zurück, die nacheinander versucht werden
    """
    rpc_urls = []
    
    # Layer 1: Environment Variables
    if chain == 'ethereum':
        env_rpc = os.getenv('ETHEREUM_RPC_URL')
        if env_rpc:
            rpc_urls.append(env_rpc)
            logger.info(f"Added RPC from environment: {env_rpc[:50]}...")
    elif chain == 'bsc':
        env_rpc = os.getenv('BSC_RPC_URL')
        if env_rpc:
            rpc_urls.append(env_rpc)
    
    # Layer 2: scanner_config
    try:
        from app.core.backend_crypto_tracker.config.scanner_config import scanner_config
        
        if chain == 'ethereum':
            config_rpc = scanner_config.rpc_config.ethereum_rpc
            if config_rpc and config_rpc not in rpc_urls and "YOUR_INFURA_KEY" not in config_rpc:
                rpc_urls.append(config_rpc)
                logger.info(f"Added RPC from scanner_config")
        elif chain == 'bsc':
            config_rpc = scanner_config.rpc_config.bsc_rpc
            if config_rpc and config_rpc not in rpc_urls:
                rpc_urls.append(config_rpc)
    except Exception as e:
        logger.warning(f"Could not load RPC from scanner_config: {e}")
    
    # Layer 3: Public Fallback RPCs
    public_rpcs = {
        'ethereum': [
            "https://eth.llamarpc.com",  # Sehr zuverlässig
            "https://rpc.ankr.com/eth",
            "https://ethereum.publicnode.com",
            "https://eth.drpc.org",
            "https://cloudflare-eth.com"
        ],
        'bsc': [
            "https://bsc-dataseed.binance.org/",
            "https://bsc-dataseed1.defibit.io/",
            "https://bsc.publicnode.com"
        ]
    }
    
    fallbacks = public_rpcs.get(chain, [])
    for fallback_rpc in fallbacks:
        if fallback_rpc not in rpc_urls:
            rpc_urls.append(fallback_rpc)
            logger.info(f"Added fallback RPC: {fallback_rpc[:50]}...")
    
    if not rpc_urls:
        logger.error(f"No RPC URLs available for chain: {chain}")
    
    return rpc_urls


async def get_onchain_metadata(token_address: str, rpc_urls: List[str]) -> Optional[Dict[str, Any]]:
    """
    ✅ IMPROVED: Versucht mehrere RPCs nacheinander
    """
    for i, rpc_url in enumerate(rpc_urls):
        try:
            logger.info(f"Attempt {i+1}/{len(rpc_urls)}: Connecting to {rpc_url[:50]}...")
            
            # ✅ FIX: HTTPProvider mit Request-Session für bessere Kompatibilität
            provider = HTTPProvider(
                rpc_url,
                request_kwargs={'timeout': 10}
            )
            w3 = Web3(provider)
            
            # Test connection
            if not w3.is_connected():
                logger.warning(f"RPC {i+1} not connected, trying next...")
                continue
            
            logger.info(f"✅ Successfully connected to RPC {i+1}!")
            
            # Hole die aktuelle Blocknummer als Connection-Test
            try:
                block_number = w3.eth.block_number
                logger.info(f"Current block number: {block_number}")
            except Exception as e:
                logger.warning(f"Could not fetch block number: {e}")
                continue
            
            # Erstelle Contract-Instanz
            checksum_address = Web3.to_checksum_address(token_address)
            contract = w3.eth.contract(
                address=checksum_address,
                abi=ERC20_ABI
            )
            
            # Hole Token-Metadaten mit Fehlerbehandlung
            metadata = {}
            
            try:
                metadata['name'] = contract.functions.name().call()
                logger.info(f"✅ Got name: {metadata['name']}")
            except Exception as e:
                logger.warning(f"Could not fetch name: {e}")
                metadata['name'] = "Unknown"
            
            try:
                metadata['symbol'] = contract.functions.symbol().call()
                logger.info(f"✅ Got symbol: {metadata['symbol']}")
            except Exception as e:
                logger.warning(f"Could not fetch symbol: {e}")
                metadata['symbol'] = "UNKNOWN"
            
            try:
                metadata['decimals'] = contract.functions.decimals().call()
                logger.info(f"✅ Got decimals: {metadata['decimals']}")
            except Exception as e:
                logger.warning(f"Could not fetch decimals: {e}")
                metadata['decimals'] = 18
            
            try:
                metadata['total_supply'] = contract.functions.totalSupply().call()
            except Exception as e:
                logger.warning(f"Could not fetch totalSupply: {e}")
                metadata['total_supply'] = 0
            
            # Wenn wir zumindest Name oder Symbol haben, ist es erfolgreich
            if metadata['name'] != "Unknown" or metadata['symbol'] != "UNKNOWN":
                logger.info(f"✅ Found token on-chain: {metadata['name']} ({metadata['symbol']})")
                return metadata
            else:
                logger.warning(f"Token found but has no valid metadata")
                # Versuche nächsten RPC
                continue
                
        except Exception as e:
            logger.warning(f"RPC {i+1} failed: {e}")
            if i < len(rpc_urls) - 1:
                logger.info(f"Trying next RPC...")
            continue
    
    logger.error(f"All RPCs failed for token {token_address}")
    return None


async def execute_get_token_price(token_address: str, chain: str) -> Optional[TokenPriceData]:
    """
    Ethereum-spezifische Token-Preisabfrage
    ✅ FIXED: Multi-RPC-Provider Fallback System
    
    Strategie:
    1. Versucht mehrere RPC-Provider nacheinander
    2. Holt On-Chain-Metadaten (Name, Symbol, Decimals)
    3. Versucht Preis von CoinGecko zu holen
    4. Gibt Token-Daten zurück - auch OHNE Preis wenn on-chain gefunden
    """
    try:
        logger.info(f"Fetching price for {token_address} on {chain}")
        
        # ✅ FIX: Hole Liste von RPC-URLs
        rpc_urls = get_rpc_urls(chain)
        
        if not rpc_urls:
            logger.error(f"No RPC URLs available for {chain}. Cannot fetch on-chain data.")
            onchain_metadata = None
        else:
            logger.info(f"Will try {len(rpc_urls)} RPC providers")
            # Schritt 1: Hole On-Chain-Metadaten (versucht alle RPCs)
            onchain_metadata = await get_onchain_metadata(token_address, rpc_urls)
        
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
                source="On-Chain RPC + CoinGecko" if coingecko_data else "On-Chain RPC (no price)",
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
