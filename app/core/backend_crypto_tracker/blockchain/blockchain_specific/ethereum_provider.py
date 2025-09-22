"""
Ethereum blockchain API provider implementation.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class EthereumProvider(BaseAPIProvider):
    """Ethereum Blockchain API-Anbieter"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Ethereum", "https://api.etherscan.io/api", api_key, "ETHERSCAN_API_KEY")
        
        # Initialisiere den CoinGecko Provider für zusätzliche Funktionalitäten
        from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko_provider import CoinGeckoProvider
        self.coingecko_provider = CoinGeckoProvider()
    
    async def get_address_balance(self, address: str) -> Optional[Dict[str, Any]]:
        """Holt den Kontostand einer Ethereum-Adresse"""
        try:
            url = self.base_url
            params = {
                'module': 'account',
                'action': 'balance',
                'address': address,
                'tag': 'latest',
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1':
                balance_wei = int(data.get('result', 0))
                return {
                    'address': address,
                    'balance': balance_wei / 10**18,  # Wei zu ETH
                    'balance_wei': balance_wei,
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Ethereum address balance: {e}")
        
        return None
    
    async def get_address_transactions(self, address: str, start_block: int = 0, end_block: int = 99999999, sort: str = 'asc') -> Optional[List[Dict[str, Any]]]:
        """Holt Transaktionen für eine Ethereum-Adresse"""
        try:
            url = self.base_url
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': start_block,
                'endblock': end_block,
                'sort': sort,
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1' and data.get('result'):
                transactions = []
                for tx in data['result']:
                    transactions.append({
                        'tx_hash': tx.get('hash'),
                        'block_number': int(tx.get('blockNumber', 0)),
                        'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                        'from_address': tx.get('from'),
                        'to_address': tx.get('to'),
                        'value': int(tx.get('value', 0)) / 10**18,
                        'gas': int(tx.get('gas', 0)),
                        'gas_price': int(tx.get('gasPrice', 0)) / 10**9,  # Wei zu Gwei
                        'gas_used': int(tx.get('gasUsed', 0)),
                        'contract_address': tx.get('contractAddress'),
                        'nonce': int(tx.get('nonce', 0)),
                        'transaction_index': int(tx.get('transactionIndex', 0)),
                        'confirmations': int(tx.get('confirmations', 0))
                    })
                
                return transactions
        except Exception as e:
            logger.error(f"Error fetching Ethereum address transactions: {e}")
        
        return None
    
    async def get_token_transfers(self, address: str, contract_address: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Holt Token-Transfers für eine Adresse"""
        try:
            url = self.base_url
            params = {
                'module': 'account',
                'action': 'tokentx',
                'address': address,
                'sort': 'desc',
                'apikey': self.api_key
            }
            
            if contract_address:
                params['contractaddress'] = contract_address
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1' and data.get('result'):
                transfers = []
                for tx in data['result']:
                    transfers.append({
                        'tx_hash': tx.get('hash'),
                        'block_number': int(tx.get('blockNumber', 0)),
                        'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                        'from_address': tx.get('from'),
                        'to_address': tx.get('to'),
                        'contract_address': tx.get('contractAddress'),
                        'token_symbol': tx.get('tokenSymbol'),
                        'token_name': tx.get('tokenName'),
                        'token_decimal': int(tx.get('tokenDecimal', 18)),
                        'value': int(tx.get('value', 0)) / (10 ** int(tx.get('tokenDecimal', 18))),
                        'transaction_index': int(tx.get('transactionIndex', 0)),
                        'gas': int(tx.get('gas', 0)),
                        'gas_price': int(tx.get('gasPrice', 0)) / 10**9,
                        'gas_used': int(tx.get('gasUsed', 0)),
                        'confirmations': int(tx.get('confirmations', 0))
                    })
                
                return transfers
        except Exception as e:
            logger.error(f"Error fetching Ethereum token transfers: {e}")
        
        return None
    
    async def get_contract_abi(self, contract_address: str) -> Optional[Dict[str, Any]]:
        """Holt das ABI eines Smart Contracts"""
        try:
            url = self.base_url
            params = {
                'module': 'contract',
                'action': 'getabi',
                'address': contract_address,
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1':
                return {
                    'contract_address': contract_address,
                    'abi': data.get('result'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Ethereum contract ABI: {e}")
        
        return None
    
    # In der get_token_price-Methode (ca. Zeile 200), füge eine Antwortprüfung hinzu:
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Versuche zuerst, den Preis über CoinGecko zu erhalten (genauere Daten)
            coingecko_price = await self.coingecko_provider.get_token_price(token_address, chain)
            if coingecko_price:
                return coingecko_price
                
            # Fallback auf Etherscan
            url = self.base_url
            params = {
                'module': 'stats',
                'action': 'tokenprice',
                'contractaddress': token_address,
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            # Prüfe, ob die Antwort gültig ist
            if not data or data.get('status') != '1':
                logger.warning(f"Ungültige Antwort von Etherscan für Token {token_address}")
                return None
                
            result = data.get('result', {})
            if not result or not result.get('ethusd'):
                logger.warning(f"Keine Preisdaten von Etherscan für Token {token_address}")
                return None
                
            return TokenPriceData(
                price=float(result.get('ethusd', 0)),
                market_cap=0,  # Nicht verfügbar
                volume_24h=0,  # Nicht verfügbar
                price_change_percentage_24h=0,  # Nicht verfügbar
                source=self.name,
                last_updated=datetime.now()
            )
        except Exception as e:
            logger.error(f"Error fetching Ethereum token price: {e}")
            return None
    
    async def get_token_holders(self, token_address: str, chain: str = 'ethereum', limit: int = 100) -> List[Dict[str, Any]]:
        """
        Holt die Top-Token-Halter für einen bestimmten Token.
        Nutzt CoinGecko Pro On-Chain API, falls verfügbar, sonst GeckoTerminal oder Etherscan.
        
        Args:
            token_address: Die Token-Vertragsadresse
            chain: Die Blockchain (Standard: ethereum)
            limit: Maximale Anzahl an Haltern, die abgerufen werden sollen
            
        Returns:
            Eine Liste von Dictionaries mit Halter-Informationen
        """
        try:
            # Versuche zuerst, die Halter über CoinGecko zu erhalten
            coingecko_holders = await self.coingecko_provider.get_token_holders(token_address, chain, limit)
            if coingecko_holders:
                return coingecko_holders
            
            # Fallback: Versuche, Halter über Etherscan-Token-Transfers zu ermitteln
            return await self._get_holders_from_etherscan(token_address, limit)
                
        except Exception as e:
            logger.error(f"Error fetching token holders for {token_address} on {chain}: {e}")
            return []
    
    async def _get_holders_from_etherscan(self, token_address: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Ermittelt Token-Halter durch Analyse von Token-Transfers über Etherscan.
        Dies ist eine Fallback-Methode, die weniger genau ist als dedizierte APIs.
        
        Args:
            token_address: Die Token-Vertragsadresse
            limit: Maximale Anzahl an Haltern, die abgerufen werden sollen
            
        Returns:
            Eine Liste von Dictionaries mit Halter-Informationen
        """
        try:
            # Hole Token-Transfers
            url = self.base_url
            params = {
                'module': 'account',
                'action': 'tokentx',
                'contractaddress': token_address,
                'sort': 'desc',
                'apikey': self.api_key
            }
            
            data = await self._make_request(url, params)
            
            if data and data.get('status') == '1' and data.get('result'):
                # Analysiere die Transfers, um die größten Halter zu ermitteln
                balances = {}
                
                for tx in data['result']:
                    from_address = tx.get('from')
                    to_address = tx.get('to')
                    value = int(tx.get('value', 0))
                    decimals = int(tx.get('tokenDecimal', 18))
                    
                    # Konvertiere in tatsächlichen Token-Wert
                    token_value = value / (10 ** decimals)
                    
                    # Aktualisiere den Saldo des Absenders
                    if from_address not in balances:
                        balances[from_address] = 0
                    balances[from_address] -= token_value
                    
                    # Aktualisiere den Saldo des Empfängers
                    if to_address not in balances:
                        balances[to_address] = 0
                    balances[to_address] += token_value
                
                # Filtere Null-Beträge und negative Salden
                balances = {addr: bal for addr, bal in balances.items() if bal > 0}
                
                # Sortiere nach Balance (absteigend)
                sorted_balances = sorted(balances.items(), key=lambda x: x[1], reverse=True)
                
                # Berechne den Gesamtbestand für Prozentangaben
                total_supply = sum(bal for _, bal in sorted_balances)
                
                # Erstelle die Ergebnisliste
                holders = []
                for address, balance in sorted_balances[:limit]:
                    percentage = (balance / total_supply * 100) if total_supply > 0 else 0
                    holders.append({
                        'address': address,
                        'amount': balance,
                        'percentage': percentage
                    })
                
                return holders
                
        except Exception as e:
            logger.error(f"Error fetching holders from Etherscan: {e}")
        
        return []
    
    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_second": 5, "requests_per_minute": 300}
    
    async def close(self):
        """Schließt alle offenen Ressourcen wie Client-Sessions."""
        if hasattr(self, 'client_session') and self.client_session:
            await self.client_session.close()
            logger.info("EthereumProvider client session closed successfully")
        
        # Schließe auch den CoinGeckoProvider
        if hasattr(self, 'coingecko_provider'):
            await self.coingecko_provider.close()
