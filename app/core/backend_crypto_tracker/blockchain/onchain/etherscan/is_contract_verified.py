"""
Contract verification check implementation
"""
import asyncio
import aiohttp
import os
from typing import Optional
from app.core.backend_crypto_tracker.utils.logger import get_logger
from .etherscan_provider import EtherscanProvider

logger = get_logger(__name__)

async def is_contract_verified(self: EtherscanProvider, contract_address: str, chain: str) -> bool:
    """Prüft, ob ein Contract verifiziert ist"""
    try:
        base_url = "https://api.etherscan.io/api" if chain.lower() == 'ethereum' else "https://api.bscscan.com/api"
        api_key = self.api_key or ""
        params = {
            'module': 'contract',
            'action': 'getsourcecode',
            'address': contract_address,
            'apikey': api_key
        }

        if not self.session:
            self.session = aiohttp.ClientSession()

        async with self.session.get(base_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if data.get('status') == '1' and data.get('result'):
                    source_code = data['result'][0].get('SourceCode', '')
                    return len(source_code.strip()) > 0
    except asyncio.CancelledError:
        logger.warning("Contract verification request was cancelled")
        return False
    except Exception as e:
        logger.error(f"Error checking contract verification: {e}")
    return False

# Füge die Methode zur Klasse hinzu
EtherscanProvider.is_contract_verified = is_contract_verified
