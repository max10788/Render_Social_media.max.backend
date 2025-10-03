from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


async def execute_get_contract_transactions(provider, contract_address: str, hours: int = 24) -> List[Dict]:
    """Holt die Transaktionen eines Smart-Contracts der letzten Stunden"""
    try:
        if provider.etherscan_provider:
            # Berechne den Zeitstempel vor X Stunden
            since_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp())
            
            # Hole Transaktionen über Etherscan
            transactions = await provider.etherscan_provider.get_contract_transactions(
                contract_address, 
                start_block=0,  # Etherscan findet automatisch den richtigen Block
                end_block=99999999,
                sort='desc'
            )
            
            # Filtere Transaktionen nach Zeit
            filtered_transactions = []
            for tx in transactions:
                if tx.get('timeStamp') and int(tx['timeStamp']) >= since_timestamp:
                    filtered_transactions.append({
                        'from': tx.get('from'),
                        'to': tx.get('to'),
                        'hash': tx.get('hash'),
                        'timeStamp': tx.get('timeStamp')
                    })
            
            return filtered_transactions
        else:
            # Fallback: Direkte Abfrage über RPC (langsamer)
            return await _get_contract_transactions_via_rpc(provider, contract_address, hours)
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Contract-Transaktionen: {e}")
        return []


async def _get_contract_transactions_via_rpc(provider, contract_address: str, hours: int) -> List[Dict]:
    """Fallback-Methode über RPC-Abfragen"""
    try:
        # Berechne den Block vor X Stunden
        latest_block = provider.w3.eth.block_number
        blocks_per_hour = 240  # Ca. 15 Sekunden pro Block
        start_block = max(0, latest_block - (hours * blocks_per_hour))
        
        transactions = []
        
        # Hole alle Transaktionen in diesem Blockbereich
        for block_num in range(start_block, latest_block + 1):
            block = provider.w3.eth.get_block(block_num, full_transactions=True)
            
            for tx in block.transactions:
                # Prüfe, ob die Transaktion den Contract betrifft
                if (tx.to and tx.to.lower() == contract_address.lower()) or \
                   (tx.input and tx.input.startswith('0xa9059cbb')):  # ERC20 Transfer
                    transactions.append({
                        'from': tx['from'],
                        'to': tx['to'],
                        'hash': tx.hash.hex(),
                        'timeStamp': block.timestamp
                    })
        
        return transactions
    except Exception as e:
        logger.error(f"Fehler bei der RPC-Transaktionsabfrage: {e}")
        return []
