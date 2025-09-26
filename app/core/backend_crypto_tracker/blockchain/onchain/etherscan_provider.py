# app/core/backend_crypto_tracker/blockchain/onchain/etherscan_provider.py

"""
Etherscan API provider implementation for token holders and on-chain data.
"""

import asyncio
import aiohttp
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider

logger = get_logger(__name__)


class EtherscanProvider(BaseAPIProvider):
    """Etherscan API Provider für On-Chain-Daten"""
    
    def __init__(self, api_key: Optional[str] = None):
        # Automatisch API-Key aus Umgebungsvariable laden
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
        """
        Holt Token-Holder für einen ERC20-Token
        
        Args:
            token_address: Token-Contract-Adresse
            chain: Blockchain (ethereum, bsc)
            limit: Maximale Anzahl an Holders
            
        Returns:
            Liste der Token-Holder mit Adressen und Balances
        """
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
            
            # Versuche zuerst die direkte Token-Holder-API (falls verfügbar)
            params = {
                'module': 'token',
                'action': 'tokenholderlist',
                'contractaddress': token_address,
                'page': '1',
                'offset': str(limit),
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
                                'percentage': 0  # Wird später berechnet
                            })
                        
                        logger.info(f"Retrieved {len(holders)} token holders from {chain}scan API")
                        return holders
                    else:
                        logger.warning(f"API returned error: {data.get('message', 'Unknown error')}")
                        # Fallback zu Transfer-Analyse
                        return await self._get_holders_from_transfers(token_address, base_url, limit)
                else:
                    logger.warning(f"HTTP error {response.status} from {chain}scan")
                    return await self._get_holders_from_transfers(token_address, base_url, limit)
        
        except Exception as e:
            logger.error(f"Error retrieving token holders from {chain}scan: {e}")
            return []
    
    async def _get_holders_from_transfers(self, token_address: str, base_url: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Analysiert Token-Transfers um Holder zu ermitteln (Fallback-Methode)
        """
        try:
            api_key = self.api_key or ""
            
            # Hole die letzten Token-Transfers
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
                        # Analysiere Transfers um Balances zu berechnen
                        balances = {}
                        
                        for tx in data['result']:
                            from_addr = tx.get('from')
                            to_addr = tx.get('to')
                            value = int(tx.get('value', 0))
                            decimals = int(tx.get('tokenDecimal', 18))
                            
                            # Konvertiere zu Token-Einheiten
                            token_amount = value / (10 ** decimals)
                            
                            # Update Balances
                            if from_addr not in balances:
                                balances[from_addr] = 0
                            if to_addr not in balances:
                                balances[to_addr] = 0
                            
                            balances[from_addr] -= token_amount
                            balances[to_addr] += token_amount
                        
                        # Filtere positive Balances und sortiere
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
                                'percentage': 0  # Wird später berechnet
                            })
                        
                        logger.info(f"Calculated {len(holders)} token holders from transfer analysis")
                        return holders
                    else:
                        logger.warning(f"No transfer data available: {data.get('message', 'Unknown')}")
        
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
                                'recent_large_sells': 0  # Placeholder - erweiterte Analyse nötig
                            }
        
        except Exception as e:
            logger.error(f"Error getting wallet transactions: {e}")
        
        return {
            'tx_count': 0,
            'first_tx_time': None,
            'last_tx_time': None,
            'recent_large_sells': 0
        }
