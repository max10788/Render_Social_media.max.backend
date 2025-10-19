from datetime import datetime
from typing import Optional
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
    }
]


async def execute_get_token_price(token_address: str, chain: str) -> Optional[TokenPriceData]:
    """
    Ethereum-spezifische Token-Preisabfrage
    1. Versucht RPC-Verbindung für On-Chain-Daten
    2. Fallback auf CoinGecko API
    """
    try:
        logger.info(f"Fetching price for {token_address} on {chain}")
        
        # Hole RPC URL aus Environment
        rpc_url = os.getenv('ETHEREUM_RPC_URL')
        
        if rpc_url:
            # Versuche On-Chain-Daten zu holen
            try:
                w3 = Web3(Web3.HTTPProvider(rpc_url))
                
                if w3.is_connected():
                    logger.info(f"Connected to Ethereum RPC: {rpc_url}")
                    
                    # Erstelle Contract-Instanz
                    contract = w3.eth.contract(
                        address=Web3.to_checksum_address(token_address),
                        abi=ERC20_ABI
                    )
                    
                    # Hole Token-Metadaten
                    try:
                        name = contract.functions.name().call()
                        symbol = contract.functions.symbol().call()
                        decimals = contract.functions.decimals().call()
                        
                        logger.info(f"Found token on-chain: {name} ({symbol}), decimals: {decimals}")
                        
                        # Preis muss von CoinGecko kommen (On-Chain hat keinen USD-Preis)
                        # Aber wir wissen jetzt, dass der Token existiert
                    except Exception as e:
                        logger.warning(f"Could not fetch token metadata from RPC: {e}")
                else:
                    logger.warning(f"Could not connect to Ethereum RPC")
            except Exception as e:
                logger.warning(f"Error using Ethereum RPC: {e}")
        
        # Fallback oder primäre Quelle: CoinGecko API
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
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    token_data = data.get(token_address.lower(), {})
                    
                    if token_data and 'usd' in token_data:
                        logger.info(f"Successfully fetched price for {token_address} from CoinGecko")
                        
                        return TokenPriceData(
                            price=float(token_data.get('usd', 0)),
                            market_cap=float(token_data.get('usd_market_cap', 0)),
                            volume_24h=float(token_data.get('usd_24h_vol', 0)),
                            price_change_percentage_24h=float(token_data.get('usd_24h_change', 0)),
                            source="CoinGecko",
                            last_updated=datetime.now()
                        )
                    else:
                        logger.warning(f"No price data found for {token_address} on CoinGecko")
                        return None
                        
                elif response.status == 429:
                    logger.warning(f"Rate limit exceeded for CoinGecko API")
                    return None
                else:
                    logger.warning(f"CoinGecko API returned status {response.status}")
                    return None
                    
    except Exception as e:
        logger.error(f"Error fetching Ethereum token price: {e}")
        return None
