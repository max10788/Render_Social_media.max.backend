from typing import List, Dict, Any, Optional
import aiohttp
import os
from web3 import Web3
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)

# ERC20 ABI für Transfer Events
TRANSFER_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "from", "type": "address"},
        {"indexed": True, "name": "to", "type": "address"},
        {"indexed": False, "name": "value", "type": "uint256"}
    ],
    "name": "Transfer",
    "type": "event"
}


async def execute_get_token_holders(token_address: str, chain: str) -> List[Dict[str, Any]]:
    """
    Ethereum-spezifische Token-Holder-Abfrage
    Nutzt Etherscan API oder RPC für Token-Holder
    """
    try:
        logger.info(f"Fetching token holders for {token_address} on {chain}")
        
        # Versuch 1: Etherscan API (falls API Key vorhanden)
        etherscan_api_key = os.getenv('ETHERSCAN_API_KEY')
        
        if etherscan_api_key:
            holders = await _get_holders_from_etherscan(token_address, etherscan_api_key)
            if holders:
                logger.info(f"Found {len(holders)} holders from Etherscan")
                return holders
        
        # Versuch 2: CoinGecko API (Top Holders)
        holders = await _get_holders_from_coingecko(token_address, chain)
        if holders:
            logger.info(f"Found {len(holders)} holders from CoinGecko")
            return holders
        
        # Versuch 3: RPC (Transfer Events - sehr aufwändig)
        # Für Production besser externe APIs nutzen
        logger.warning(f"Could not fetch holders for {token_address} - no API available")
        return []
        
    except Exception as e:
        logger.error(f"Error fetching token holders: {e}")
        return []


async def _get_holders_from_etherscan(token_address: str, api_key: str) -> List[Dict[str, Any]]:
    """Holt Top Token Holders von Etherscan"""
    try:
        url = "https://api.etherscan.io/api"
        params = {
            'module': 'token',
            'action': 'tokenholderlist',
            'contractaddress': token_address,
            'page': 1,
            'offset': 100,  # Top 100 Holders
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get('status') == '1' and data.get('result'):
                        holders = []
                        for holder in data['result']:
                            holders.append({
                                'address': holder.get('TokenHolderAddress'),
                                'balance': float(holder.get('TokenHolderQuantity', 0)),
                                'percentage': 0  # Muss berechnet werden
                            })
                        return holders
                    else:
                        logger.warning(f"Etherscan API returned status: {data.get('message')}")
                        return []
                else:
                    logger.warning(f"Etherscan API returned HTTP {response.status}")
                    return []
                    
    except Exception as e:
        logger.warning(f"Error fetching from Etherscan: {e}")
        return []


async def _get_holders_from_coingecko(token_address: str, chain: str) -> List[Dict[str, Any]]:
    """
    Versucht Top Holders von CoinGecko zu holen
    Hinweis: CoinGecko bietet keine direkten Holder-Daten in der Free API
    """
    try:
        # CoinGecko hat keine direkte Token Holder API
        # Würde Pro API erfordern
        logger.debug(f"CoinGecko does not provide holder data in free tier")
        return []
        
    except Exception as e:
        logger.warning(f"Error fetching from CoinGecko: {e}")
        return []


async def _get_holders_from_rpc(token_address: str) -> List[Dict[str, Any]]:
    """
    Holt Token Holders via RPC (Transfer Events)
    WARNUNG: Sehr langsam und ressourcenintensiv!
    """
    try:
        rpc_url = os.getenv('ETHEREUM_RPC_URL')
        
        if not rpc_url:
            logger.warning("No RPC URL provided")
            return []
        
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        
        if not w3.is_connected():
            logger.warning("Could not connect to Ethereum RPC")
            return []
        
        # Dies würde alle Transfer-Events durchsuchen müssen
        # Für Production NICHT empfohlen - zu langsam!
        logger.warning("RPC-based holder fetching not implemented (too slow)")
        return []
        
    except Exception as e:
        logger.error(f"Error with RPC holder fetching: {e}")
        return []
