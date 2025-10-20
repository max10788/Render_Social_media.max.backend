from typing import List, Dict, Any
import aiohttp
import os
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_token_holders(token_address: str, chain: str) -> List[Dict[str, Any]]:
    """
    Holt Token Holders für einen ERC20 Token auf Ethereum
    Nutzt Etherscan API
    
    Args:
        token_address: ERC20 Token Contract Adresse
        chain: Blockchain ('ethereum' oder 'bsc')
    
    Returns:
        Liste von Holder-Dictionaries mit address, balance, percentage
    """
    try:
        logger.info(f"Fetching token holders for {token_address} on {chain}")
        
        # Hole API Key aus Environment
        if chain == 'ethereum':
            api_key = os.getenv('ETHERSCAN_API_KEY')
            base_url = "https://api.etherscan.io/api"
        elif chain == 'bsc':
            api_key = os.getenv('BSCSCAN_API_KEY')
            base_url = "https://api.bscscan.com/api"
        else:
            logger.warning(f"Unsupported chain for token holders: {chain}")
            return []
        
        if not api_key:
            logger.warning(f"No API key found for {chain} - cannot fetch holders")
            return []
        
        # Etherscan API: Token Holder List
        params = {
            'module': 'token',
            'action': 'tokenholderlist',
            'contractaddress': token_address,
            'page': 1,
            'offset': 100,  # Top 100 Holders
            'apikey': api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                base_url, 
                params=params, 
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Prüfe Etherscan Response Status
                    if data.get('status') == '1' and data.get('result'):
                        holders = []
                        total_supply = 0
                        
                        # Berechne Total Supply aus allen Holders
                        for holder in data['result']:
                            try:
                                balance = float(holder.get('TokenHolderQuantity', 0))
                                total_supply += balance
                            except (ValueError, TypeError):
                                continue
                        
                        # Erstelle Holder-Liste mit Percentage
                        for holder in data['result']:
                            try:
                                balance = float(holder.get('TokenHolderQuantity', 0))
                                percentage = (balance / total_supply * 100) if total_supply > 0 else 0
                                
                                holders.append({
                                    'address': holder.get('TokenHolderAddress', ''),
                                    'balance': balance,
                                    'percentage': round(percentage, 4)
                                })
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Error parsing holder data: {e}")
                                continue
                        
                        logger.info(f"Successfully fetched {len(holders)} holders for {token_address}")
                        return holders
                    
                    elif data.get('status') == '0':
                        # Etherscan Error
                        error_msg = data.get('message', 'Unknown error')
                        logger.warning(f"Etherscan API error: {error_msg}")
                        
                        if 'rate limit' in error_msg.lower():
                            logger.warning("Rate limit exceeded - waiting before retry")
                        
                        return []
                    
                    else:
                        logger.warning(f"Unexpected Etherscan response: {data}")
                        return []
                
                elif response.status == 429:
                    logger.warning(f"Rate limit exceeded for {chain} API")
                    return []
                
                else:
                    logger.warning(f"{chain} API returned HTTP {response.status}")
                    return []
                    
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching token holders: {e}")
        return []
    
    except Exception as e:
        logger.error(f"Error fetching token holders for {token_address}: {e}")
        return []
