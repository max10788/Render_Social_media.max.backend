from datetime import datetime
from typing import Any, Dict, List, Optional
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_transaction_blocks(
    provider, 
    address: str = None,
    digest: str = None,
    limit: int = 100
) -> Optional[Dict[str, Any]]:
    """
    Holt Transaction Blocks entweder für eine Adresse oder einen einzelnen Digest
    
    Args:
        provider: Sui RPC provider
        address: Sui wallet address (zum Abrufen mehrerer Transaktionen)
        digest: Single transaction digest (zum Abrufen eines spezifischen Blocks)
        limit: Maximale Anzahl Transaktionen zu abrufen (nur bei address)
    
    Returns:
        Transaction Block Daten oder Liste von Blocks
    """
    try:
        # Fall 1: Einzelne Digest abrufen
        if digest:
            return await _get_single_transaction_block(provider, digest)
        
        # Fall 2: Transaktionen für eine Adresse abrufen
        if address:
            return await _get_address_transaction_blocks(provider, address, limit)
        
        logger.error("Either 'address' or 'digest' parameter required")
        return None
        
    except Exception as e:
        logger.error(f"Error in execute_get_transaction_blocks: {e}")
        return None


async def _get_single_transaction_block(
    provider, 
    digest: str
) -> Optional[Dict[str, Any]]:
    """Holt einen einzelnen Transaction Block"""
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'sui_getTransactionBlock',
            'params': [
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
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
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
    provider,
    address: str,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Holt alle Transaction Blocks für eine Adresse
    """
    try:
        # Schritt 1: Transaktions-Digests für diese Adresse abrufen
        digests = await _get_transaction_digests_for_address(provider, address, limit)
        
        if not digests:
            logger.info(f"No transactions found for address {address}")
            return []
        
        logger.info(f"Found {len(digests)} transaction digests for {address}")
        
        # Schritt 2: Detailinformationen für jeden Digest abrufen
        transactions = []
        for digest in digests:
            try:
                tx_block = await _get_single_transaction_block(provider, digest)
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
    provider,
    address: str,
    limit: int = 100
) -> List[str]:
    """
    Holt die Transaction Digests für eine Adresse
    Nutzt queryTransactionBlocks RPC-Methode
    """
    try:
        params = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'suix_queryTransactionBlocks',
            'params': [
                {
                    'filter': {
                        'FromAddress': address
                    },
                    'order': 'descending',
                    'limit': min(limit, 100)  # Sui hat oft ein Maximum von 100
                },
                None  # cursor für Pagination
            ]
        }
        
        data = await provider._make_post_request(provider.base_url, params)
        
        if data and data.get('result'):
            result = data['result']
            digests = [tx.get('digest') for tx in result.get('data', []) if tx.get('digest')]
            return digests
        
        return []
        
    except Exception as e:
        logger.error(f"Error fetching transaction digests for address {address}: {e}")
        return []
