"""
Ethereum blockchain API provider implementation.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from web3 import Web3

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.onchain.etherscan_provider import EtherscanProvider
from app.core.backend_crypto_tracker.blockchain.aggregators.coingecko_provider import CoinGeckoProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)


class EthereumProvider:
    def __init__(self, api_key=None, rpc_url=None):
        self.api_key = api_key
        self.etherscan_provider = EtherscanProvider(api_key) if api_key else None
        self.w3 = None
        self.base_url = "https://api.etherscan.io/api"
        self.session = None
        self.coingecko_provider = CoinGeckoProvider()
        
        # Zuverlässige Ethereum RPC-URLs (nur echte Ethereum-Endpunkte)
        self.rpc_urls = [
            # Zuerst die übergebene URL oder Umgebungsvariable
            rpc_url if rpc_url else os.getenv('ETHEREUM_RPC_URL'),
            # Öffentliche Ethereum RPCs
            "https://ethereum.publicnode.com",
            "https://eth.llamarpc.com",
            "https://eth.meowrpc.com",
            "https://1rpc.io/eth",
            "https://rpc.ankr.com/eth",
            "https://cloudflare-eth.com",
            # Infura mit API-Key
            "https://mainnet.infura.io/v3/1JTTMXUDJ2D2DKAW9BU6PED8NZJD7G4G9V",
            # GetBlock
            "https://go.getblock.io/79261441b53344bfbb3b8bdf37fe4047"
        ]
        
        # Filtere None-Werte heraus
        self.rpc_urls = [url for url in self.rpc_urls if url]
        
        logger.info(f"Available Ethereum RPC URLs: {len(self.rpc_urls)} configured")
    
    async def __aenter__(self):
        # Versuche Verbindung mit allen verfügbaren RPC-URLs
        for i, rpc_url in enumerate(self.rpc_urls):
            try:
                logger.info(f"Attempting to connect to Ethereum node #{i+1}: {rpc_url}")
                self.w3 = Web3(Web3.HTTPProvider(rpc_url))
                
                if self.w3.is_connected():
                    latest_block = self.w3.eth.block_number
                    # Zusätzliche Prüfung: Sicherstellen, dass wir mit dem richtigen Netzwerk verbunden sind
                    chain_id = self.w3.eth.chain_id
                    logger.info(f"Connected to node. Chain ID: {chain_id}, Latest block: {latest_block}")
                    
                    # Ethereum Mainnet hat Chain ID 1
                    if chain_id == 1:
                        logger.info(f"Successfully connected to Ethereum mainnet. Latest block: {latest_block}")
                        self.rpc_url = rpc_url  # Speichere die funktionierende URL
                        break
                    else:
                        logger.warning(f"Connected to wrong network (Chain ID: {chain_id}), expected Ethereum (1)")
                else:
                    logger.warning(f"Connection failed to Ethereum node: {rpc_url}")
            except Exception as e:
                logger.error(f"Error connecting to Ethereum node {rpc_url}: {e}")
                continue
        else:
            # Wenn alle Verbindungsversuche fehlschlagen
            logger.error("All connection attempts failed for Ethereum nodes")
            self.w3 = None
        
        if self.etherscan_provider:
            await self.etherscan_provider.__aenter__()
        if self.coingecko_provider:
            await self.coingecko_provider.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.etherscan_provider:
            await self.etherscan_provider.__aexit__(exc_type, exc_val, exc_tb)
        if self.coingecko_provider:
            await self.coingecko_provider.__aexit__(exc_type, exc_val, exc_tb)
    
    async def _make_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Hilfsmethode für HTTP-Anfragen"""
        if not self.session:
            import aiohttp
            self.session = aiohttp.ClientSession()
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"HTTP error {response.status}: {await response.text()}")
                    return {}
        except Exception as e:
            logger.error(f"Request error: {e}")
            return {}
    
    async def get_address_balance(self, address: str) -> Optional[Dict[str, Any]]:
        """Holt den Kontostand einer Ethereum-Adresse"""
        try:
            # Prüfe, ob wir eine Web3-Verbindung haben
            if not self.w3 or not self.w3.is_connected():
                logger.error("No Web3 connection available for balance lookup")
                return None
                
            params = {
                'module': 'account',
                'action': 'balance',
                'address': address,
                'tag': 'latest',
                'apikey': self.api_key
            }
            
            data = await self._make_request(self.base_url, params)
            
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

    async def get_contract_transactions(self, contract_address: str, hours: int = 24) -> List[Dict]:
        """Holt die Transaktionen eines Smart-Contracts der letzten Stunden"""
        try:
            # Prüfe, ob wir eine Web3-Verbindung haben
            if not self.w3 or not self.w3.is_connected():
                logger.error("No Web3 connection available for contract transaction lookup")
                return []
            
            if self.etherscan_provider:
                # Berechne den Zeitstempel vor X Stunden
                since_timestamp = int((datetime.now() - timedelta(hours=hours)).timestamp())
                
                # Hole Transaktionen über Etherscan
                transactions = await self.etherscan_provider.get_contract_transactions(
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
                return await self._get_contract_transactions_via_rpc(contract_address, hours)
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Contract-Transaktionen: {e}")
            return []
    
    async def _get_contract_transactions_via_rpc(self, contract_address: str, hours: int) -> List[Dict]:
        """Fallback-Methode über RPC-Abfragen"""
        try:
            # Prüfe, ob wir eine Web3-Verbindung haben
            if not self.w3 or not self.w3.is_connected():
                logger.error("No Web3 connection available for RPC transaction lookup")
                return []
            
            # Berechne den Block vor X Stunden
            latest_block = self.w3.eth.block_number
            blocks_per_hour = 240  # Ca. 15 Sekunden pro Block
            start_block = max(0, latest_block - (hours * blocks_per_hour))
            
            transactions = []
            
            # Hole alle Transaktionen in diesem Blockbereich
            for block_num in range(start_block, latest_block + 1):
                try:
                    block = self.w3.eth.get_block(block_num, full_transactions=True)
                    
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
                except Exception as e:
                    logger.warning(f"Error processing block {block_num}: {e}")
                    continue
            
            return transactions
        except Exception as e:
            logger.error(f"Fehler bei der RPC-Transaktionsabfrage: {e}")
            return []
            
    async def get_address_transactions(self, address: str, start_block: int = 0, end_block: int = 99999999, sort: str = 'asc') -> Optional[List[Dict[str, Any]]]:
        """Holt Transaktionen für eine Ethereum-Adresse"""
        try:
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': start_block,
                'endblock': end_block,
                'sort': sort,
                'apikey': self.api_key
            }
            
            data = await self._make_request(self.base_url, params)
            
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
    
    async def get_token_balance(self, token_address: str, wallet_address: str) -> float:
        """Holt den Token-Bestand einer Wallet"""
        try:
            # Prüfe, ob wir eine Web3-Verbindung haben
            if not self.w3 or not self.w3.is_connected():
                logger.error("No Web3 connection available for token balance lookup")
                return 0
            
            # ERC20-ABI für balanceOf
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [{"name": "_owner", "type": "address"}],
                    "name": "balanceOf",
                    "outputs": [{"name": "balance", "type": "uint256"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "decimals",
                    "outputs": [{"name": "", "type": "uint8"}],
                    "type": "function"
                }
            ]
            
            # Erstelle Contract-Instanz
            contract = self.w3.eth.contract(address=token_address, abi=erc20_abi)
            
            # Hole Decimals und Balance
            decimals = await contract.functions.decimals().call()
            balance_raw = await contract.functions.balanceOf(wallet_address).call()
            
            # Konvertiere in lesbare Zahl
            return balance_raw / (10 ** decimals)
        except Exception as e:
            logger.error(f"Fehler beim Abrufen des Token-Bestands: {e}")
            return 0
    
    async def get_token_transfers(self, address: str, contract_address: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Holt Token-Transfers für eine Adresse"""
        try:
            params = {
                'module': 'account',
                'action': 'tokentx',
                'address': address,
                'sort': 'desc',
                'apikey': self.api_key
            }
            
            if contract_address:
                params['contractaddress'] = contract_address
            
            data = await self._make_request(self.base_url, params)
            
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
            params = {
                'module': 'contract',
                'action': 'getabi',
                'address': contract_address,
                'apikey': self.api_key
            }
            
            data = await self._make_request(self.base_url, params)
            
            if data and data.get('status') == '1':
                return {
                    'contract_address': contract_address,
                    'abi': data.get('result'),
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching Ethereum contract ABI: {e}")
        
        return None
    
    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        """Holt Token-Preisdaten - NUR für interne Berechnungen, nicht für Top-Coins"""
        try:
            # Diese Methode sollte nur für interne Berechnungen verwendet werden
            # und nicht für die Abfrage von allgemeinen Börsendaten
            
            # Versuche, den Preis über DEX-Daten zu ermitteln
            dex_price = await self._get_token_price_from_dex(token_address)
            if dex_price and dex_price > 0:
                return TokenPriceData(
                    price=dex_price,
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=0,  # Nicht verfügbar
                    price_change_percentage_24h=0,  # Nicht verfügbar
                    source="DEX",
                    last_updated=datetime.now()
                )
            
            # Fallback: Keine Preisdaten verfügbar
            logger.warning(f"Keine Preisdaten für Token {token_address} verfügbar")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching token price: {e}")
            return None
    
    async def _get_token_price_from_dex(self, token_address: str) -> Optional[float]:
        """Holt Token-Preis von dezentralen Börsen"""
        try:
            # Prüfe, ob wir eine Web3-Verbindung haben
            if not self.w3 or not self.w3.is_connected():
                logger.error("No Web3 connection available for DEX price lookup")
                return None
            
            # Uniswap V2 Factory Address
            factory_address = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
            factory_abi = [
                {
                    "inputs": [{"internalType": "address", "name": "tokenA", "type": "address"}, {"internalType": "address", "name": "tokenB", "type": "address"}],
                    "name": "getPair",
                    "outputs": [{"internalType": "address", "name": "pair", "type": "address"}],
                    "stateMutability": "view",
                    "type": "function"
                }
            ]
            
            factory_contract = self.w3.eth.contract(address=factory_address, abi=factory_abi)
            
            # WETH Address
            weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            
            # Finde das Paar
            pair_address = await factory_contract.functions.getPair(token_address, weth_address).call()
            
            if pair_address != "0x0000000000000000000000000000000000000000":
                # Hole Reserven
                pair_abi = [
                    {
                        "inputs": [],
                        "name": "getReserves",
                        "outputs": [
                            {"internalType": "uint112", "name": "reserve0", "type": "uint112"},
                            {"internalType": "uint112", "name": "reserve1", "type": "uint112"}
                        ],
                        "stateMutability": "view",
                        "type": "function"
                    },
                    {
                        "inputs": [],
                        "name": "token0",
                        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                        "stateMutability": "view",
                        "type": "function"
                    },
                    {
                        "inputs": [],
                        "name": "token1",
                        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
                        "stateMutability": "view",
                        "type": "function"
                    }
                ]
                
                pair_contract = self.w3.eth.contract(address=pair_address, abi=pair_abi)
                
                # Hole Token-Adressen und Reserven
                token0 = await pair_contract.functions.token0().call()
                token1 = await pair_contract.functions.token1().call()
                reserves = await pair_contract.functions.getReserves().call()
                
                # Bestimme, welche Reserven zu welchem Token gehören
                if token0.lower() == token_address.lower():
                    token_reserve = reserves[0]
                    weth_reserve = reserves[1]
                else:
                    token_reserve = reserves[1]
                    weth_reserve = reserves[0]
                
                # Hole WETH-Preis (vereinfacht)
                weth_price_usd = 2000  # Fallback-Wert, in einer echten Implementierung würde dies von einer API geholt
                
                # Berechne Token-Preis
                if token_reserve > 0:
                    token_price = (weth_reserve * weth_price_usd) / token_reserve
                    return token_price
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting token price from DEX: {e}")
            return None
    
    async def get_token_holders(self, token_address: str, chain: str) -> List[Dict[str, Any]]:
        """Holt die Top-Holder eines Tokens durch Analyse der Transaktionen"""
        try:
            logger.info(f"Starte Analyse der Top-Holder für Token {token_address}")
            
            # Prüfe, ob wir eine Web3-Verbindung haben
            if not self.w3 or not self.w3.is_connected():
                logger.error("No Web3 connection available for token holder analysis")
                return []
            
            # Schritt 1: Hole die Transaktionen des Smart-Contracts der letzten 24 Stunden
            logger.info(f"Rufe Transaktionen für {token_address} auf {chain} ab")
            transactions = await self.get_contract_transactions(token_address, hours=24)
            
            if not transactions:
                logger.warning(f"Keine Transaktionen für {token_address} gefunden")
                # Versuche, Holder über Etherscan direkt zu bekommen
                return await self._get_holders_from_etherscan(token_address)
            
            # Schritt 2: Extrahiere die einzigartigen Wallet-Adressen aus den Transaktionen
            wallet_addresses = set()
            for tx in transactions:
                # Extrahiere Absender und Empfänger
                if 'from' in tx and tx['from']:
                    wallet_addresses.add(tx['from'])
                if 'to' in tx and tx['to']:
                    wallet_addresses.add(tx['to'])
            
            logger.info(f"Gefundene aktive Wallet-Adressen: {len(wallet_addresses)}")
            
            # Schritt 3: Für jede Wallet-Adresse, hole den aktuellen Token-Bestand
            holders = []
            total_supply = 0
            
            for address in wallet_addresses:
                try:
                    # Hole den Token-Bestand für diese Adresse
                    balance = await self.get_token_balance(token_address, address)
                    if balance > 0:
                        holders.append({
                            'address': address,
                            'balance': balance,
                            'percentage': 0,  # Wird später berechnet
                            'last_interaction': None  # Könnte später aus Transaktionen extrahiert werden
                        })
                        total_supply += balance
                except Exception as e:
                    logger.warning(f"Fehler beim Abrufen des Token-Bestands für {address}: {e}")
                    continue
            
            # Schritt 4: Berechne die Prozentsätze
            for holder in holders:
                if total_supply > 0:
                    holder['percentage'] = (holder['balance'] / total_supply) * 100
                else:
                    holder['percentage'] = 0
            
            # Schritt 5: Sortiere die Wallets nach Balance (absteigend)
            holders.sort(key=lambda x: x['balance'], reverse=True)
            
            # Schritt 6: Begrenze die Anzahl der Wallets
            holders = holders[:100]  # Top 100 Holder
            
            logger.info(f"Top-Holder für {token_address}: {len(holders)} Wallets gefunden")
            return holders
            
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Token-Holder: {e}")
            # Fallback: Versuche, Holder über Etherscan direkt zu bekommen
            return await self._get_holders_from_etherscan(token_address)
    
    async def get_active_wallets(self, token_address: str, hours: int = 6) -> List[Dict[str, Any]]:
        """Holt die Wallets, die in den letzten X Stunden aktiv waren"""
        try:
            logger.info(f"Starte Analyse der aktiven Wallets für Token {token_address} der letzten {hours} Stunden")
            
            # Prüfe, ob wir eine Web3-Verbindung haben
            if not self.w3 or not self.w3.is_connected():
                logger.error("No Web3 connection available for active wallet analysis")
                return []
            
            # Hole die Transaktionen des Smart-Contracts der letzten X Stunden
            transactions = await self.get_contract_transactions(token_address, hours=hours)
            
            if not transactions:
                logger.warning(f"Keine Transaktionen für {token_address} in den letzten {hours} Stunden gefunden")
                return []
            
            # Extrahiere die einzigartigen Wallet-Adressen aus den Transaktionen
            wallet_addresses = set()
            for tx in transactions:
                # Extrahiere Absender und Empfänger
                if 'from' in tx and tx['from']:
                    wallet_addresses.add(tx['from'])
                if 'to' in tx and tx['to']:
                    wallet_addresses.add(tx['to'])
            
            # Für jede Wallet-Adresse, hole zusätzliche Informationen
            active_wallets = []
            for address in wallet_addresses:
                try:
                    # Hole den Token-Bestand für diese Adresse
                    balance = await self.get_token_balance(token_address, address)
                    
                    # Finde die letzte Transaktion dieser Wallet
                    last_tx = None
                    for tx in transactions:
                        if tx.get('from') == address or tx.get('to') == address:
                            last_tx = tx
                            break
                    
                    active_wallets.append({
                        'address': address,
                        'balance': balance,
                        'last_transaction': last_tx,
                        'last_transaction_timestamp': datetime.fromtimestamp(int(last_tx.get('timeStamp'))) if last_tx else None
                    })
                except Exception as e:
                    logger.warning(f"Fehler beim Abrufen der Daten für Wallet {address}: {e}")
                    continue
            
            # Sortiere die Wallets nach dem Zeitpunkt der letzten Transaktion (neueste zuerst)
            active_wallets.sort(key=lambda x: x.get('last_transaction_timestamp', datetime.min), reverse=True)
            
            logger.info(f"Aktive Wallets für {token_address} in den letzten {hours} Stunden: {len(active_wallets)} Wallets gefunden")
            return active_wallets
            
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der aktiven Wallets: {e}")
            return []
    
    async def _get_holders_from_etherscan(self, token_address: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fallback-Methode: Ermittelt Token-Halter durch Analyse von Token-Transfers über Etherscan.
        Dies ist eine Fallback-Methode, die weniger genau ist als die primäre Methode.
        """
        try:
            # Hole Token-Transfers
            params = {
                'module': 'account',
                'action': 'tokentx',
                'contractaddress': token_address,
                'sort': 'desc',
                'apikey': self.api_key
            }
            
            data = await self._make_request(self.base_url, params)
            
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
        if hasattr(self, 'session') and self.session:
            # Schließe zuerst den Connector
            if hasattr(self.session, 'connector') and self.session.connector:
                await self.session.connector.close()
            # Dann schließe die Session
            await self.session.close()
            logger.info("EthereumProvider client session closed successfully")
        
        # Schließe auch den CoinGeckoProvider
        if hasattr(self, 'coingecko_provider'):
            await self.coingecko_provider.close()
