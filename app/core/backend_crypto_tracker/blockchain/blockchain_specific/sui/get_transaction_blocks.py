 =============================================================================
# DATEI 1: app/core/backend_crypto_tracker/blockchain/blockchain_specific/sui/get_transaction_blocks.py
# =============================================================================

from datetime import datetime
from typing import Any, Dict, List, Optional
import httpx
import os
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_transaction_blocks(
    address: str = None,
    digest: str = None,
    limit: int = 100,
    provider=None  # Optional, wird ignoriert - für Kompatibilität
) -> Optional[Dict[str, Any]]:
    """
    Holt Transaction Blocks entweder für eine Adresse oder einen einzelnen Digest
    OHNE Provider - nutzt direkte HTTP-Calls
    
    Args:
        address: Sui wallet address (zum Abrufen mehrerer Transaktionen)
        digest: Single transaction digest (zum Abrufen eines spezifischen Blocks)
        limit: Maximale Anzahl Transaktionen zu abrufen (nur bei address)
        provider: DEPRECATED - wird ignoriert
    
    Returns:
        Transaction Block Daten oder Liste von Blocks
    """
    try:
        # Fall 1: Einzelne Digest abrufen
        if digest:
            return await _get_single_transaction_block(digest)
        
        # Fall 2: Transaktionen für eine Adresse abrufen
        if address:
            return await _get_address_transaction_blocks(address, limit)
        
        logger.error("Either 'address' or 'digest' parameter required")
        return None
        
    except Exception as e:
        logger.error(f"Error in execute_get_transaction_blocks: {e}")
        return None


async def _make_sui_rpc_call(method: str, params: list) -> Optional[Dict]:
    """Macht einen direkten RPC-Call an Sui"""
    try:
        sui_rpc_url = os.getenv('SUI_RPC_URL', 'https://fullnode.mainnet.sui.io:443')
        
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': method,
            'params': params
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(sui_rpc_url, json=payload)
            response.raise_for_status()
            return response.json()
            
    except httpx.HTTPError as e:
        logger.error(f"HTTP error in Sui RPC call: {e}")
        return None
    except Exception as e:
        logger.error(f"Error in Sui RPC call: {e}")
        return None


async def _get_single_transaction_block(digest: str) -> Optional[Dict[str, Any]]:
    """Holt einen einzelnen Transaction Block"""
    try:
        params = [
            digest,
            {
                'showInput': True,
                'showRawInput': True,
                'showEffects': True,
                'showEvents': True,
                'showObjectChanges': True,
                'showBalanceChanges': True
            }
        ]
        
        data = await _make_sui_rpc_call('sui_getTransactionBlock', params)
        
        if data and data.get('result'):
            tx_block = data['result']
            return {
                'digest': digest,
                'transaction_block': tx_block.get('transaction'),
                'effects': tx_block.get('effects'),
                'events': tx_block.get('events'),
                'object_changes': tx_block.get('objectChanges'),
                'balance_changes': tx_block.get('balanceChanges'),
                'timestamp_ms': tx_block.get('timestampMs'),
                'checkpoint': tx_block.get('checkpoint'),
                'last_updated': datetime.now()
            }
    except Exception as e:
        logger.error(f"Error fetching single transaction block for {digest}: {e}")
    
    return None


async def _get_address_transaction_blocks(
    address: str,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Holt alle Transaction Blocks für eine Adresse
    """
    try:
        # Schritt 1: Transaktions-Digests für diese Adresse abrufen
        digests = await _get_transaction_digests_for_address(address, limit)
        
        if not digests:
            logger.info(f"No transactions found for address {address}")
            return []
        
        logger.info(f"Found {len(digests)} transaction digests for {address}")
        
        # Schritt 2: Detailinformationen für jeden Digest abrufen
        transactions = []
        for digest in digests:
            try:
                tx_block = await _get_single_transaction_block(digest)
                if tx_block:
                    transactions.append(tx_block)
            except Exception as e:
                logger.warning(f"Error fetching transaction block for digest {digest}: {e}")
                continue
        
        logger.info(f"Successfully fetched {len(transactions)} transaction blocks for {address}")
        return transactions
        
    except Exception as e:
        logger.error(f"Error fetching address transaction blocks: {e}")
        return []


async def _get_transaction_digests_for_address(
    address: str,
    limit: int = 100
) -> List[str]:
    """
    Holt die Transaction Digests für eine Adresse
    Nutzt queryTransactionBlocks RPC-Methode
    """
    try:
        params = [
            {
                'filter': {
                    'FromAddress': address
                },
                'options': {
                    'showInput': False,
                    'showEffects': False,
                    'showEvents': False
                }
            },
            None,  # cursor für Pagination
            min(limit, 100)  # Sui hat oft ein Maximum von 100
        ]
        
        data = await _make_sui_rpc_call('suix_queryTransactionBlocks', params)
        
        if data and data.get('result'):
            result = data['result']
            digests = [tx.get('digest') for tx in result.get('data', []) if tx.get('digest')]
            return digests
        
        return []
        
    except Exception as e:
        logger.error(f"Error fetching transaction digests for address {address}: {e}")
        return []
