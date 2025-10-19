from datetime import datetime
from typing import Optional
import aiohttp
import os
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


async def execute_get_token_price(token_address: str, chain: str) -> Optional[TokenPriceData]:
    """
    Solana-spezifische Token-Preisabfrage
    1. Versucht RPC-Verbindung für On-Chain-Daten
    2. Fallback auf CoinGecko API
    """
    try:
        logger.info(f"Fetching price for {token_address} on {chain}")
        
        # Hole RPC URL aus Environment
        rpc_url = os.getenv('SOLANA_RPC_URL')
        
        if rpc_url:
            try:
                # Solana RPC Call für Token-Account-Info
                async with aiohttp.ClientSession() as session:
                    payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getAccountInfo",
                        "params": [
                            token_address,
                            {"encoding": "jsonParsed"}
                        ]
                    }
                    
                    async with session.post(
                        rpc_url, 
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if data.get('result') and data['result'].get('value'):
                                logger.info(f"Found token on Solana chain: {token_address}")
                            else:
                                logger.warning(f"Token {token_address} not found on Solana chain")
                        else:
                            logger.warning(f"Solana RPC returned status {response.status}")
            except Exception as e:
                logger.warning(f"Error using Solana RPC: {e}")
        
        # CoinGecko API - Token Price by Contract Address (Solana)
        url = "https://api.coingecko.com/api/v3/simple/token_price/solana"
        params = {
            'contract_addresses': token_address,
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
        logger.error(f"Error fetching Solana token price: {e}")
        return None
