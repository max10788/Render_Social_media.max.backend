"""
Contract creation transaction retrieval implementation
"""
import asyncio
import aiohttp
import os
from typing import Optional
from app.core.backend_crypto_tracker.utils.logger import get_logger
from .etherscan_provider import EtherscanProvider

logger = get_logger(__name__)

async def get_contract_creation_tx(self: EtherscanProvider, contract_address: str, chain: str) -> Optional[str]:
    """Holt die Contract-Erstellungs-Transaktion"""
    try:
        base_url = "https://api.etherscan.io/api" if chain.lower() == 'ethereum' else "https://api.bscscan.com/api"
        api_key = self.api_key or ""
        params = {
            'module': 'contract',
            'action': 'getcontractcreation',
            'contractaddresses': contract_address,
            'apikey': api_key
        }

        if not self.session:
            self.session = aiohttp.ClientSession()

        async with self.session.get(base_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if data.get('status') == '1' and data.get('result'):
                    return data['result'][0].get('txhash')
    except asyncio.CancelledError:
        logger.warning("Contract creation tx request was cancelled")
        return None
    except Exception as e:
        logger.error(f"Error getting contract creation tx: {e}")
    return None

# Füge die Methode zur Klasse hinzu
EtherscanProvider.get_contract_creation_tx = get_contract_creation_tx
