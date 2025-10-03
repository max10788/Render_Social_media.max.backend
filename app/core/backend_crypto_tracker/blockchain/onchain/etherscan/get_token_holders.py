"""
Token holders retrieval implementation for Etherscan
"""
import asyncio
import aiohttp
import os
from typing import Any, Dict, List, Optional
from app.core.backend_crypto_tracker.utils.logger import get_logger
from .etherscan_provider import EtherscanProvider

logger = get_logger(__name__)

async def get_token_holders(self: EtherscanProvider, token_address: str, chain: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Holt Token-Holder für einen ERC20/BEP20-Token"""
    try:
        # Bestimme die richtige API-URL basierend auf der Chain
        if chain.lower() == 'ethereum':
            base_url = "https://api.etherscan.io/api"
            api_key = self.api_key or os.getenv('ETHERSCAN_API_KEY')
        elif chain.lower() == 'bsc':
            base_url = "https://api.bscscan.com/api"
            api_key = self.api_key or os.getenv('BSCSCAN_API_KEY')
        else:
            logger.warning(f"Unsupported chain for Etherscan: {chain}")
            return []

        if not api_key:
            logger.warning(f"No API key provided for {chain} scan")
            return await _get_holders_from_transfers(self, token_address, base_url, limit)

        # Versuche zuerst die direkte Token-Holder-API
        params = {
            'module': 'token',
            'action': 'tokenholderlist',
            'contractaddress': token_address,
            'page': '1',
            'offset': str(min(limit, 10000)),
            'sort': 'desc',
            'apikey': api_key
        }

        if not self.session:
            self.session = aiohttp.ClientSession()

        async with self.session.get(base_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if data.get('status') == '1' and data.get('message') == 'OK':
                    holders = []
                    for holder in data.get('result', []):
                        holders.append({
                            'address': holder.get('TokenHolderAddress'),
                            'balance': holder.get('TokenHolderQuantity'),
                            'percentage': 0
                        })
                    logger.info(f"Retrieved {len(holders)} token holders from {chain}scan API")
                    return holders
                else:
                    logger.warning(f"API returned error: {data.get('message', 'Unknown error')}")
                    return await _get_holders_from_transfers(self, token_address, base_url, limit)
            else:
                logger.warning(f"HTTP error {response.status} from {chain}scan")
                return await _get_holders_from_transfers(self, token_address, base_url, limit)

    except asyncio.CancelledError:
        logger.warning("Token holders request was cancelled")
        return []
    except Exception as e:
        logger.error(f"Error retrieving token holders from {chain}scan: {e}")
        return []

async def _get_holders_from_transfers(self: EtherscanProvider, token_address: str, base_url: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Analysiert Token-Transfers um Holder zu ermitteln (Fallback-Methode)"""
    try:
        api_key = self.api_key or ""
        params = {
            'module': 'account',
            'action': 'tokentx',
            'contractaddress': token_address,
            'sort': 'desc',
            'apikey': api_key
        }

        if not self.session:
            self.session = aiohttp.ClientSession()

        async with self.session.get(base_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if data.get('status') == '1' and data.get('result'):
                    balances = {}
                    decimals = 18  # Standardwert
                    
                    # Versuche, die Decimals aus der ersten Transaktion zu ermitteln
                    for tx in data['result']:
                        try:
                            decimals = int(tx.get('tokenDecimal', 18))
                            break
                        except (ValueError, TypeError):
                            continue
                    
                    # Analysiere alle Transaktionen
                    for tx in data['result']:
                        from_addr = tx.get('from')
                        to_addr = tx.get('to')
                        value = int(tx.get('value', 0))
                        token_amount = value / (10 ** decimals)
                        
                        if from_addr not in balances:
                            balances[from_addr] = 0
                        if to_addr not in balances:
                            balances[to_addr] = 0
                        balances[from_addr] -= token_amount
                        balances[to_addr] += token_amount

                    positive_balances = {
                        addr: bal for addr, bal in balances.items()
                        if bal > 0
                    }
                    sorted_holders = sorted(
                        positive_balances.items(),
                        key=lambda x: x[1],
                        reverse=True
                    )[:limit]

                    holders = []
                    for address, balance in sorted_holders:
                        holders.append({
                            'address': address,
                            'balance': balance,
                            'percentage': 0
                        })

                    logger.info(f"Calculated {len(holders)} token holders from transfer analysis")
                    return holders
                else:
                    logger.warning(f"No transfer data available: {data.get('message', 'Unknown')}")
            else:
                logger.warning(f"HTTP error {response.status} from {base_url}")
    except asyncio.CancelledError:
        logger.warning("Transfer analysis request was cancelled")
        return []
    except Exception as e:
        logger.error(f"Error analyzing transfers for holders: {e}")
    return []

# Füge die Methode zur Klasse hinzu
EtherscanProvider.get_token_holders = get_token_holders
