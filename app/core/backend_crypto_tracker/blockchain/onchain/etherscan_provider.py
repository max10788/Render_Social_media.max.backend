"""
Etherscan API provider implementation for token holders and on-chain data.
"""
import asyncio
import aiohttp
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider

logger = get_logger(__name__)

class EtherscanProvider(BaseAPIProvider):
    """Etherscan API Provider für On-Chain-Daten"""
    def __init__(self, api_key: Optional[str] = None):
        if api_key is None:
            api_key = os.getenv('ETHERSCAN_API_KEY')
        super().__init__("Etherscan", "https://api.etherscan.io/api", api_key)
        self.min_request_interval = 0.2  # 5 RPS für kostenlose API

    async def get_token_price(self, token_address: str, chain: str):
        """Implementiert abstrakte Methode, aber nicht verwendet für Preise"""
        logger.warning("Etherscan provider not optimized for token prices")
        return None

    def get_rate_limits(self) -> Dict[str, int]:
        """Rate Limits für Etherscan API"""
        return {
            "requests_per_second": 5,
            "requests_per_minute": 300,
            "requests_per_hour": 18000
        }

    async def get_token_holders(self, token_address: str, chain: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Holt Token-Holder für einen ERC20-Token"""
        try:
            # Bestimme die richtige API-URL basierend auf der Chain
            if chain.lower() == 'ethereum':
                base_url = "https://api.etherscan.io/api"
                api_key = os.getenv('ETHERSCAN_API_KEY')
            elif chain.lower() == 'bsc':
                base_url = "https://api.bscscan.com/api"
                api_key = os.getenv('BSCSCAN_API_KEY') or os.getenv('ETHERSCAN_API_KEY')
            else:
                logger.warning(f"Unsupported chain for Etherscan: {chain}")
                return []

            if not api_key:
                logger.warning(f"No API key provided for {chain} scan")
                return await self._get_holders_from_transfers(token_address, base_url, limit)

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
                                'TokenHolderAddress': holder.get('TokenHolderAddress'),
                                'TokenHolderQuantity': holder.get('TokenHolderQuantity'),
                                'percentage': 0
                            })
                        logger.info(f"Retrieved {len(holders)} token holders from {chain}scan API")
                        return holders
                    else:
                        logger.warning(f"API returned error: {data.get('message', 'Unknown error')}")
                        return await self._get_holders_from_transfers(token_address, base_url, limit)
                else:
                    logger.warning(f"HTTP error {response.status} from {chain}scan")
                    return await self._get_holders_from_transfers(token_address, base_url, limit)

        except Exception as e:
            logger.error(f"Error retrieving token holders from {chain}scan: {e}")
            return []

    async def _get_holders_from_transfers(self, token_address: str, base_url: str, limit: int = 100) -> List[Dict[str, Any]]:
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
                        for tx in data['result']:
                            from_addr = tx.get('from')
                            to_addr = tx.get('to')
                            value = int(tx.get('value', 0))
                            decimals = int(tx.get('tokenDecimal', 18))
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
                                'TokenHolderAddress': address,
                                'TokenHolderQuantity': str(int(balance * (10 ** decimals))),
                                'percentage': 0
                            })

                        logger.info(f"Calculated {len(holders)} token holders from transfer analysis")
                        return holders
                    else:
                        logger.warning(f"No transfer data available: {data.get('message', 'Unknown')}")
                else:
                    logger.warning(f"HTTP error {response.status} from {base_url}")
        except Exception as e:
            logger.error(f"Error analyzing transfers for holders: {e}")
        return []

    async def get_contract_creation_tx(self, contract_address: str, chain: str) -> Optional[str]:
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
        except Exception as e:
            logger.error(f"Error getting contract creation tx: {e}")
        return None

    async def is_contract_verified(self, contract_address: str, chain: str) -> bool:
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
        except Exception as e:
            logger.error(f"Error checking contract verification: {e}")
        return False

    async def get_wallet_transactions(self, wallet_address: str, chain: str) -> Dict[str, Any]:
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
        except Exception as e:
            logger.error(f"Error getting wallet transactions: {e}")
        return {
            'tx_count': 0,
            'first_tx_time': None,
            'last_tx_time': None,
            'recent_large_sells': 0
        }

    async def get_contract_transactions(
        self, 
        contract_address: str, 
        hours: int = 24, 
        start_block: Optional[int] = None,
        end_block: Optional[int] = None
    ) -> List[Dict]:
        """
        Holt die Transaktionen eines Smart-Contracts der letzten Stunden
        oder in einem bestimmten Blockbereich
        
        Args:
            contract_address: Contract-Adresse
            hours: Zeitraum in Stunden (wenn start_block nicht angegeben)
            start_block: Optionale Start-Blocknummer
            end_block: Optionale End-Blocknummer (wird ignoriert, nur für Kompatibilität)
            
        Returns:
            Liste der Transaktionen
        """
        try:
            # Berechne den Zeitstempel basierend auf Parametern
            if start_block is not None:
                # Hole Zeitstempel für Start-Block
                since_timestamp = await self._get_block_timestamp(start_block)
                if since_timestamp == 0:
                    logger.error(f"Konnte Zeitstempel für Block {start_block} nicht ermitteln")
                    return []
            else:
                # Berechne Zeitstempel vor X Stunden
                since_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp())
            
            # Logge die Parameter für Debugging
            logger.info(f"Suche Transaktionen für Contract {contract_address} seit {'Block ' + str(start_block) if start_block else str(hours) + ' Stunden'}")
            
            # Hole Token-Transfers über die Etherscan-API
            params = {
                'module': 'account',
                'action': 'tokentx',
                'contractaddress': contract_address,
                'sort': 'desc',
                'apikey': self.api_key
            }
            
            # Führe den API-Aufruf durch
            data = await self._make_request(self.base_url, params)
            
            if data and data.get('status') == '1' and data.get('result'):
                transactions = data.get('result', [])
                logger.info(f"Etherscan API gab {len(transactions)} Transaktionen zurück")
                
                # Filtere Transaktionen nach Zeit
                filtered_transactions = []
                for tx in transactions:
                    tx_timestamp = int(tx.get('timeStamp', 0))
                    if tx_timestamp >= since_timestamp:
                        filtered_transactions.append({
                            'from': tx.get('from'),
                            'to': tx.get('to'),
                            'hash': tx.get('hash'),
                            'timeStamp': tx_timestamp,
                            'contract_address': contract_address,
                            'value': int(tx.get('value', 0)),
                            'tokenSymbol': tx.get('tokenSymbol'),
                            'tokenDecimal': int(tx.get('tokenDecimal', 18))
                        })
                
                logger.info(f"Gefiltert {len(filtered_transactions)} Transaktionen seit {'Block ' + str(start_block) if start_block else str(hours) + ' Stunden'}")
                return filtered_transactions
            else:
                error_msg = data.get('message', 'Unknown error') if data else 'No response'
                logger.warning(f"Keine Transaktionsdaten von Etherscan API erhalten: {error_msg}")
                return []
                
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Contract-Transaktionen: {e}")
            return []

    async def _get_block_timestamp(self, block_number: int) -> int:
        """
        Holt den Zeitstempel eines Blocks über die Etherscan API
        
        Args:
            block_number: Blocknummer
            
        Returns:
            Unix-Zeitstempel des Blocks oder 0 bei Fehler
        """
        try:
            params = {
                'module': 'proxy',
                'action': 'eth_getBlockByNumber',
                'tag': hex(block_number),
                'boolean': 'true',
                'apikey': self.api_key
            }
            
            data = await self._make_request(self.base_url, params)
            
            if data and data.get('result'):
                timestamp_hex = data['result'].get('timestamp')
                if timestamp_hex:
                    return int(timestamp_hex, 16)
            
            logger.warning(f"Block {block_number} nicht gefunden oder kein Zeitstempel verfügbar")
            return 0
            
        except Exception as e:
            logger.error(f"Fehler beim Abrufen des Block-Zeitstempels für Block {block_number}: {e}")
            return 0
