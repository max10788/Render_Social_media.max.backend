"""
Wallet token transactions retrieval implementation
"""
import asyncio
import aiohttp
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from app.core.backend_crypto_tracker.utils.logger import get_logger
from .etherscan_provider import EtherscanProvider

logger = get_logger(__name__)

async def get_wallet_token_transactions(
    self: EtherscanProvider, 
    wallet_address: str, 
    token_address: Optional[str] = None,
    hours: int = 24,
    start_block: Optional[int] = None,
    sort: str = 'desc'
) -> List[Dict]:
    """
    Holt die Token-Transaktionen einer Wallet, optional gefiltert nach einem bestimmten Token
    """
    try:
        # Bestimme die richtige API-URL basierend auf der Chain
        base_url = self.base_url
        api_key = self.api_key
        
        # Berechne den Zeitstempel basierend auf Parametern
        if start_block is not None:
            # Prüfe auf ungültige Blocknummer
            if start_block <= 0:
                logger.error(f"Ungültige Blocknummer: {start_block}. Verwende stattdessen Zeitraum.")
                since_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp())
            else:
                # Hole Zeitstempel für Start-Block
                since_timestamp = await _get_block_timestamp(self, start_block)
                if since_timestamp == 0:
                    logger.error(f"Konnte Zeitstempel für Block {start_block} nicht ermitteln. Verwende stattdessen Zeitraum.")
                    since_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp())
        else:
            # Berechne Zeitstempel vor X Stunden
            since_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp())
        
        # Logge die Parameter für Debugging
        logger.info(f"Suche Token-Transaktionen für Wallet {wallet_address} seit {'Block ' + str(start_block) if start_block else str(hours) + ' Stunden'}")
        
        # Hole Token-Transfers über die Etherscan-API mit Zeitstempel-Filter
        params = {
            'module': 'account',
            'action': 'tokentx',
            'address': wallet_address,
            'starttimestamp': str(since_timestamp),
            'endtimestamp': str(int(datetime.now().timestamp())),
            'sort': sort,
            'apikey': api_key
        }
        
        # Wenn eine Token-Adresse angegeben ist, füge sie hinzu
        if token_address:
            params['contractaddress'] = token_address
            logger.info(f"Filtere nach Token: {token_address}")
        
        # Führe den API-Aufluf durch
        data = await _make_request(self, base_url, params)
        
        if data and data.get('status') == '1' and data.get('result'):
            transactions = data.get('result', [])
            logger.info(f"Etherscan API gab {len(transactions)} Token-Transaktionen zurück")
            
            # Bereite die Transaktionsdaten auf
            result_transactions = []
            for tx in transactions:
                result_transactions.append({
                    'from': tx.get('from'),
                    'to': tx.get('to'),
                    'hash': tx.get('hash'),
                    'timeStamp': int(tx.get('timeStamp', 0)),
                    'contract_address': tx.get('contractAddress'),
                    'value': int(tx.get('value', 0)),
                    'tokenSymbol': tx.get('tokenSymbol'),
                    'tokenDecimal': int(tx.get('tokenDecimal', 18))
                })
            
            logger.info(f"Verarbeite {len(result_transactions)} Token-Transaktionen seit {'Block ' + str(start_block) if start_block else str(hours) + ' Stunden'}")
            return result_transactions
        else:
            error_msg = data.get('message', 'Unknown error') if data else 'No response'
            logger.warning(f"Keine Token-Transaktionsdaten von Etherscan API erhalten: {error_msg}")
            
            # Versuche Fallback ohne Zeitstempel-Filter
            logger.info("Versuche Fallback ohne Zeitstempel-Filter...")
            fallback_params = {
                'module': 'account',
                'action': 'tokentx',
                'address': wallet_address,
                'sort': 'desc',
                'apikey': api_key
            }
            
            if token_address:
                fallback_params['contractaddress'] = token_address
            
            fallback_data = await _make_request(self, base_url, fallback_params)
            
            if fallback_data and fallback_data.get('status') == '1' and fallback_data.get('result'):
                all_transactions = fallback_data.get('result', [])
                logger.info(f"Fallback: Etherscan API gab {len(all_transactions)} Token-Transaktionen zurück (ungefiltert)")
                
                # Manuell nach Zeit filtern
                result_transactions = []
                for tx in all_transactions:
                    tx_timestamp = int(tx.get('timeStamp', 0))
                    if tx_timestamp >= since_timestamp:
                        result_transactions.append({
                            'from': tx.get('from'),
                            'to': tx.get('to'),
                            'hash': tx.get('hash'),
                            'timeStamp': tx_timestamp,
                            'contract_address': tx.get('contractAddress'),
                            'value': int(tx.get('value', 0)),
                            'tokenSymbol': tx.get('tokenSymbol'),
                            'tokenDecimal': int(tx.get('tokenDecimal', 18))
                        })
                
                logger.info(f"Fallback: Gefiltert {len(result_transactions)} Token-Transaktionen seit {'Block ' + str(start_block) if start_block else str(hours) + ' Stunden'}")
                return result_transactions
            else:
                logger.error("Sowohl Hauptversuch als auch Fallback fehlgeschlagen")
                return []
            
    except asyncio.CancelledError:
        logger.warning("Wallet token transactions request was cancelled")
        return []
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Wallet-Token-Transaktionen: {e}")
        return []

async def _get_block_timestamp(self: EtherscanProvider, block_number: int) -> int:
    """
    Holt den Zeitstempel eines Blocks über die Etherscan API
    
    Args:
        block_number: Blocknummer
        
    Returns:
        Unix-Zeitstempel des Blocks oder 0 bei Fehler
    """
    try:
        # Prüfe auf ungültige Blocknummer
        if block_number <= 0:
            logger.error(f"Ungültige Blocknummer: {block_number}")
            return 0
            
        params = {
            'module': 'proxy',
            'action': 'eth_getBlockByNumber',
            'tag': hex(block_number),
            'boolean': 'true',
            'apikey': self.api_key
        }
        
        data = await _make_request(self, self.base_url, params)
        
        if data and data.get('result'):
            timestamp_hex = data['result'].get('timestamp')
            if timestamp_hex:
                return int(timestamp_hex, 16)
        
        logger.warning(f"Block {block_number} nicht gefunden oder kein Zeitstempel verfügbar")
        return 0
        
    except asyncio.CancelledError:
        logger.warning(f"Block timestamp request for block {block_number} was cancelled")
        return 0
    except Exception as e:
        logger.error(f"Fehler beim Abrufen des Block-Zeitstempels für Block {block_number}: {e}")
        return 0

async def _make_request(self: EtherscanProvider, url: str, params: Dict) -> Dict:
    """Hilfsmethode für API-Anfragen"""
    try:
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        async with self.session.get(url, params=params) as response:
            return await response.json()
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return {}

# Füge die Methode zur Klasse hinzu
EtherscanProvider.get_wallet_token_transactions = get_wallet_token_transactions
