"""
Blockchain Provider Classes for Transaction Controller
Wraps existing blockchain API functionality into provider classes
"""
import os
import aiohttp
from typing import Dict, Any, List, Optional
from datetime import datetime
from app.core.backend_crypto_tracker.utils.logger import get_logger

logger = get_logger(__name__)


class EthereumProvider:
    """
    Ethereum/BSC blockchain provider using Etherscan/BscScan API
    Wraps existing functionality from blockchain/onchain/etherscan and blockchain_specific/ethereum
    """

    def __init__(self, chain: str = 'ethereum'):
        self.chain = chain
        self.api_key = os.getenv('ETHERSCAN_API_KEY') if chain == 'ethereum' else os.getenv('BSCSCAN_API_KEY')
        self.base_url = "https://api.etherscan.io/api" if chain == 'ethereum' else "https://api.bscscan.com/api"
        self.session = None

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def _make_request(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Make API request to Etherscan/BscScan"""
        await self._ensure_session()

        try:
            params['apikey'] = self.api_key
            async with self.session.get(self.base_url, params=params, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Etherscan API error: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Etherscan API request failed: {e}")
            return None

    async def get_transaction_by_hash(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """Get transaction details by hash"""
        try:
            params = {
                'module': 'proxy',
                'action': 'eth_getTransactionByHash',
                'txhash': tx_hash
            }

            data = await self._make_request(params)
            if data and data.get('result'):
                tx = data['result']
                return {
                    'tx_hash': tx.get('hash', ''),
                    'from_address': tx.get('from', '').lower(),
                    'to_address': tx.get('to', '').lower() if tx.get('to') else None,
                    'value': float(int(tx.get('value', '0x0'), 16)) / 1e18,
                    'block_number': int(tx.get('blockNumber', '0x0'), 16) if tx.get('blockNumber') else None,
                    'block_hash': tx.get('blockHash'),
                    'gas_price': float(int(tx.get('gasPrice', '0x0'), 16)) / 1e9,  # in Gwei
                    'gas_used': int(tx.get('gas', '0x0'), 16),
                    'status': 'success',  # Need to get receipt for actual status
                    'timestamp': datetime.utcnow()
                }
            return None
        except Exception as e:
            logger.error(f"Error getting transaction {tx_hash}: {e}")
            return None

    async def get_transactions_by_address(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get transactions for an address"""
        try:
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': start_block,
                'endblock': end_block,
                'page': 1,
                'offset': limit,
                'sort': 'desc'
            }

            data = await self._make_request(params)
            if data and data.get('status') == '1' and data.get('result'):
                transactions = []
                for tx in data['result']:
                    transactions.append({
                        'tx_hash': tx.get('hash', ''),
                        'from_address': tx.get('from', '').lower(),
                        'to_address': tx.get('to', '').lower() if tx.get('to') else None,
                        'value': float(tx.get('value', 0)) / 1e18,
                        'block_number': int(tx.get('blockNumber', 0)),
                        'block_hash': tx.get('blockHash'),
                        'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                        'gas_used': int(tx.get('gasUsed', 0)),
                        'gas_price': float(tx.get('gasPrice', 0)) / 1e9,
                        'fee': (int(tx.get('gasUsed', 0)) * float(tx.get('gasPrice', 0))) / 1e18,
                        'status': 'success' if tx.get('isError') == '0' else 'failed',
                        'method': tx.get('functionName', '').split('(')[0] if tx.get('functionName') else None
                    })
                return transactions
            return []
        except Exception as e:
            logger.error(f"Error getting transactions for {address}: {e}")
            return []

    async def get_token_transfers(self, token_address: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get token transfer events"""
        try:
            params = {
                'module': 'account',
                'action': 'tokentx',
                'contractaddress': token_address,
                'page': 1,
                'offset': limit,
                'sort': 'desc'
            }

            data = await self._make_request(params)
            if data and data.get('status') == '1' and data.get('result'):
                transfers = []
                for tx in data['result']:
                    decimals = int(tx.get('tokenDecimal', 18))
                    value = float(tx.get('value', 0)) / (10 ** decimals)

                    transfers.append({
                        'tx_hash': tx.get('hash', ''),
                        'from_address': tx.get('from', '').lower(),
                        'to_address': tx.get('to', '').lower(),
                        'value': 0,  # Native value
                        'token_address': token_address.lower(),
                        'token_amount': value,
                        'timestamp': datetime.fromtimestamp(int(tx.get('timeStamp', 0))),
                        'block_number': int(tx.get('blockNumber', 0)),
                        'gas_used': int(tx.get('gasUsed', 0)),
                        'gas_price': float(tx.get('gasPrice', 0)) / 1e9,
                        'status': 'success'
                    })
                return transfers
            return []
        except Exception as e:
            logger.error(f"Error getting token transfers for {token_address}: {e}")
            return []

    async def get_internal_transactions(self, tx_hash: str) -> List[Dict[str, Any]]:
        """Get internal transactions for a transaction hash"""
        try:
            params = {
                'module': 'account',
                'action': 'txlistinternal',
                'txhash': tx_hash
            }

            data = await self._make_request(params)
            if data and data.get('status') == '1' and data.get('result'):
                internals = []
                for tx in data['result']:
                    internals.append({
                        'from': tx.get('from', '').lower(),
                        'to': tx.get('to', '').lower(),
                        'value': float(tx.get('value', 0)) / 1e18,
                        'type': tx.get('type', 'call'),
                        'gas': int(tx.get('gas', 0)),
                        'gasUsed': int(tx.get('gasUsed', 0)),
                        'isError': tx.get('isError') == '1'
                    })
                return internals
            return []
        except Exception as e:
            logger.error(f"Error getting internal transactions for {tx_hash}: {e}")
            return []

    async def get_transaction_receipt(self, tx_hash: str) -> Dict[str, Any]:
        """Get transaction receipt with logs"""
        try:
            params = {
                'module': 'proxy',
                'action': 'eth_getTransactionReceipt',
                'txhash': tx_hash
            }

            data = await self._make_request(params)
            if data and data.get('result'):
                receipt = data['result']
                logs = receipt.get('logs', [])

                return {
                    'status': int(receipt.get('status', '0x0'), 16),
                    'logs': logs,
                    'gasUsed': int(receipt.get('gasUsed', '0x0'), 16),
                    'blockNumber': int(receipt.get('blockNumber', '0x0'), 16)
                }
            return {'status': 0, 'logs': [], 'gasUsed': 0, 'blockNumber': 0}
        except Exception as e:
            logger.error(f"Error getting transaction receipt for {tx_hash}: {e}")
            return {'status': 0, 'logs': [], 'gasUsed': 0, 'blockNumber': 0}

    async def close(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()


class SolanaProvider:
    """
    Solana blockchain provider using RPC API
    Minimal implementation - extends as needed
    """

    def __init__(self):
        self.rpc_url = os.getenv('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com')
        self.session = None

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def _make_rpc_request(self, method: str, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Make JSON-RPC request to Solana"""
        await self._ensure_session()

        try:
            payload = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': method,
                'params': params
            }

            async with self.session.post(self.rpc_url, json=payload, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Solana RPC error: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Solana RPC request failed: {e}")
            return None

    async def get_transaction(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """Get transaction details by signature"""
        try:
            data = await self._make_rpc_request('getTransaction', [tx_hash, {'encoding': 'json'}])

            if data and data.get('result'):
                result = data['result']
                return {
                    'tx_hash': tx_hash,
                    'signature': tx_hash,
                    'slot': result.get('slot'),
                    'block_number': result.get('slot'),
                    'timestamp': datetime.fromtimestamp(result.get('blockTime', 0)) if result.get('blockTime') else datetime.utcnow(),
                    'status': 'success' if not result.get('meta', {}).get('err') else 'failed',
                    'fee': result.get('meta', {}).get('fee', 0) / 1e9,  # lamports to SOL
                    'metadata': result
                }
            return None
        except Exception as e:
            logger.error(f"Error getting Solana transaction {tx_hash}: {e}")
            return None

    async def get_account_info(self, address: str) -> Dict[str, Any]:
        """Get account info - returns basic structure"""
        try:
            data = await self._make_rpc_request('getAccountInfo', [address, {'encoding': 'jsonParsed'}])

            if data and data.get('result'):
                result = data['result']
                return {
                    'address': address,
                    'lamports': result.get('value', {}).get('lamports', 0),
                    'owner': result.get('value', {}).get('owner', ''),
                    'transactions': []  # Placeholder - would need getConfirmedSignaturesForAddress2
                }
            return {'address': address, 'lamports': 0, 'owner': '', 'transactions': []}
        except Exception as e:
            logger.error(f"Error getting Solana account info for {address}: {e}")
            return {'address': address, 'lamports': 0, 'owner': '', 'transactions': []}

    async def get_token_transfers(self, token_address: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get token transfers - placeholder implementation"""
        logger.warning("Solana get_token_transfers not fully implemented")
        return []

    async def close(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()


class SuiProvider:
    """
    Sui blockchain provider using JSON-RPC API
    Minimal implementation - extends as needed
    """

    def __init__(self):
        self.rpc_url = os.getenv('SUI_RPC_URL', 'https://fullnode.mainnet.sui.io:443')
        self.session = None

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def _make_rpc_request(self, method: str, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Make JSON-RPC request to Sui"""
        await self._ensure_session()

        try:
            payload = {
                'jsonrpc': '2.0',
                'id': 1,
                'method': method,
                'params': params
            }

            async with self.session.post(self.rpc_url, json=payload, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.error(f"Sui RPC error: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Sui RPC request failed: {e}")
            return None

    async def get_transaction(self, tx_digest: str) -> Optional[Dict[str, Any]]:
        """Get transaction details by digest"""
        try:
            data = await self._make_rpc_request('sui_getTransactionBlock', [tx_digest, {'showInput': True, 'showEffects': True}])

            if data and data.get('result'):
                result = data['result']
                timestamp_ms = result.get('timestampMs', 0)

                return {
                    'tx_hash': tx_digest,
                    'digest': tx_digest,
                    'timestamp': datetime.fromtimestamp(int(timestamp_ms) / 1000) if timestamp_ms else datetime.utcnow(),
                    'status': result.get('effects', {}).get('status', {}).get('status', 'unknown'),
                    'gas_used': result.get('effects', {}).get('gasUsed', {}).get('computationCost', 0),
                    'metadata': result
                }
            return None
        except Exception as e:
            logger.error(f"Error getting Sui transaction {tx_digest}: {e}")
            return None

    async def get_account_info(self, address: str) -> Dict[str, Any]:
        """Get account info - returns basic structure"""
        try:
            # Get objects owned by address
            data = await self._make_rpc_request('suix_getOwnedObjects', [address, {'limit': 10}])

            if data and data.get('result'):
                result = data['result']
                return {
                    'address': address,
                    'objects': result.get('data', []),
                    'transactions': []  # Placeholder
                }
            return {'address': address, 'objects': [], 'transactions': []}
        except Exception as e:
            logger.error(f"Error getting Sui account info for {address}: {e}")
            return {'address': address, 'objects': [], 'transactions': []}

    async def get_token_transfers(self, token_address: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get token transfers - placeholder implementation"""
        logger.warning("Sui get_token_transfers not fully implemented")
        return []

    async def close(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
