"""
Wallet transactions retrieval implementation
"""
import asyncio
import aiohttp
import os
from datetime import datetime
from typing import Any, Dict
from app.core.backend_crypto_tracker.utils.logger import get_logger
from .etherscan_provider import EtherscanProvider

logger = get_logger(__name__)

async def get_wallet_transactions(self: EtherscanProvider, wallet_address: str, chain: str) -> Dict[str, Any]:
    """Holt Transaktionsdaten für eine Wallet"""
    try:
        base_url = "https://api.etherscan.io/api" if chain.lower() == 'ethereum' else "https://api.bscscan.com/api"
        api_key = self.api_key or ""
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': wallet_address,
            'sort': 'desc',
            'apikey': api_key
        }

        if not self.session:
            self.session = aiohttp.ClientSession()

        async with self.session.get(base_url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if data.get('status') == '1' and data.get('result'):
                    transactions = data['result']
                    if transactions:
                        first_tx = datetime.fromtimestamp(int(transactions[-1].get('timeStamp', 0)))
                        last_tx = datetime.fromtimestamp(int(transactions[0].get('timeStamp', 0)))
                        return {
                            'tx_count': len(transactions),
                            'first_tx_time': first_tx,
                            'last_tx_time': last_tx,
                            'recent_large_sells': 0
                        }
    except asyncio.CancelledError:
        logger.warning("Wallet transactions request was cancelled")
        return {
            'tx_count': 0,
            'first_tx_time': None,
            'last_tx_time': None,
            'recent_large_sells': 0
        }
    except Exception as e:
        logger.error(f"Error getting wallet transactions: {e}")
    return {
        'tx_count': 0,
        'first_tx_time': None,
        'last_tx_time': None,
        'recent_large_sells': 0
    }

# Füge die Methode zur Klasse hinzu
EtherscanProvider.get_wallet_transactions = get_wallet_transactions
