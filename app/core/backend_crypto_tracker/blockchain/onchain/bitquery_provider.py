"""
Bitquery API provider implementation.
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.blockchain.exchanges.base_provider import BaseAPIProvider
from app.core.backend_crypto_tracker.blockchain.data_models.token_price_data import TokenPriceData

logger = get_logger(__name__)

class BitqueryProvider(BaseAPIProvider):
    """Bitquery API-Anbieter (GraphQL) - beste on-chain Daten"""
    def __init__(self, api_key: Optional[str] = None):
        super().__init__("Bitquery", "https://graphql.bitquery.io", api_key, "BITQUERY_API_KEY")
        self.min_request_interval = 0.5  # Höheres Rate-Limiting für GraphQL

    async def _make_post_request(self, url: str, payload: Dict, headers: Dict = None) -> Dict:
        """
        Führt eine POST-Anfrage durch und loggt die direkte Antwort.
        Args:
            url: Die URL für die API-Anfrage
            payload: Die Payload für die Anfrage
            headers: Die Header für die Anfrage
        Returns:
            Die JSON-Antwort als Dictionary
        """
        try:
            # Führe die eigentliche Anfrage durch
            response = await super()._make_post_request(url, payload, headers)
            # Logge die direkte API-Antwort
            logger.info(f"Bitquery API Response - URL: {url}")
            logger.info(f"Payload: {json.dumps(payload, indent=2)}")
            logger.info(f"Headers: {headers}")
            logger.info(f"Response: {json.dumps(response, indent=2)}")
            return response
        except Exception as e:
            # Logge den Fehler bei der Anfrage
            logger.error(f"Bitquery API Request Failed - URL: {url}")
            logger.error(f"Payload: {json.dumps(payload, indent=2)}")
            logger.error(f"Headers: {headers}")
            logger.error(f"Error: {str(e)}")
            raise

    async def get_token_price(self, token_address: str, chain: str) -> Optional[TokenPriceData]:
        try:
            # Bitquery verwendet GraphQL-Abfragen
            query = self._build_price_query(token_address, chain)
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            if data.get('data') and data['data'].get('ethereum'):
                token_data = data['data']['ethereum']['dexTrades'][0]
                return TokenPriceData(
                    price=float(token_data.get('quotePrice', 0)),
                    market_cap=0,  # Nicht verfügbar
                    volume_24h=0,  # Nicht verfügbar
                    price_change_percentage_24h=0,  # Nicht verfügbar
                    source=self.name,
                    last_updated=datetime.now()
                )
        except Exception as e:
            logger.error(f"Error fetching from Bitquery: {e}")
        return None

    async def get_dex_trades(self, token_address: str, chain: str, hours: int = 24) -> Optional[List[Dict[str, Any]]]:
        """Holt DEX-Trades für einen Token"""
        try:
            query = self._build_dex_trades_query(token_address, chain, hours)
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            if data.get('data') and data['data'].get('ethereum'):
                trades = data['data']['ethereum']['dexTrades']
                dex_trades = []
                for trade in trades:
                    dex_trades.append({
                        'transaction_hash': trade.get('transaction', {}).get('hash'),
                        'timestamp': datetime.fromtimestamp(int(trade.get('block', {}).get('timestamp', {}).get('unixtime'))),
                        'buyer': trade.get('buyAddress'),
                        'seller': trade.get('sellAddress'),
                        'price': float(trade.get('quotePrice')),
                        'amount': float(trade.get('tradeAmount')),
                        'amount_usd': float(trade.get('tradeAmount', {}).get('inUSD')),
                        'pool_address': trade.get('exchange', {}).get('address')
                    })
                return dex_trades
        except Exception as e:
            logger.error(f"Error fetching DEX trades from Bitquery: {e}")
        return None

    async def get_token_liquidity(self, token_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """Holt Liquiditätsdaten für einen Token"""
        try:
            query = self._build_liquidity_query(token_address, chain)
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            if data.get('data') and data['data'].get('ethereum'):
                pools = data['data']['ethereum']['dexTrades']
                # Aggregiere Liquiditätsdaten
                total_liquidity_usd = 0
                unique_traders = set()
                pool_data = {}
                for pool in pools:
                    pool_address = pool.get('exchange', {}).get('address')
                    if pool_address not in pool_data:
                        pool_data[pool_address] = {
                            'liquidity_usd': 0,
                            'trader_count': 0
                        }
                    pool_data[pool_address]['liquidity_usd'] += float(pool.get('tradeAmount', {}).get('inUSD', 0))
                    unique_traders.add(pool.get('buyAddress'))
                    unique_traders.add(pool.get('sellAddress'))

                for pool_address in pool_data:
                    pool_data[pool_address]['trader_count'] = len(unique_traders)
                    total_liquidity_usd += pool_data[pool_address]['liquidity_usd']

                return {
                    'total_liquidity_usd': total_liquidity_usd,
                    'unique_traders_24h': len(unique_traders),
                    'pool_data': pool_data,
                    'last_updated': datetime.now()
                }
        except Exception as e:
            logger.error(f"Error fetching token liquidity from Bitquery: {e}")
        return None

    async def get_wallet_activity(self, wallet_address: str, chain: str, hours: int = 24) -> Optional[List[Dict[str, Any]]]:
        """Holt Wallet-Aktivitäten für eine bestimmte Adresse"""
        try:
            query = self._build_wallet_activity_query(wallet_address, chain, hours)
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            data = await self._make_post_request(self.base_url, {'query': query}, headers)
            if data.get('data') and data['data'].get('ethereum'):
                transfers = data['data']['ethereum']['transfers']
                wallet_activity = []
                for transfer in transfers:
                    wallet_activity.append({
                        'transaction_hash': transfer.get('transaction', {}).get('hash'),
                        'timestamp': datetime.fromtimestamp(int(transfer.get('block', {}).get('timestamp', {}).get('unixtime'))),
                        'sender': transfer.get('sender', {}).get('address'),
                        'receiver': transfer.get('receiver', {}).get('address'),
                        'amount': float(transfer.get('amount')),
                        'amount_usd': float(transfer.get('amount', {}).get('inUSD', 0)),
                        'token_symbol': transfer.get('currency', {}).get('symbol')
                    })
                return wallet_activity
        except Exception as e:
            logger.error(f"Error fetching wallet activity from Bitquery: {e}")
        return None

    # --- NEUE METHODE: get_token_holders_graphql ---
    async def get_token_holders_graphql(self, contract_address: str, chain: str) -> List[Dict[str, Any]]:
        """
        Holt Token-Holder über Bitquery GraphQL API.
        Args:
            contract_address: Die Token-Contract-Adresse
            chain: Die Blockchain (z.B. 'ethereum')
        Returns:
            Liste der Token-Holder mit Adressen und Balances
        """
        try:
            query = self._build_token_holders_query(contract_address, chain)
            headers = {
                'Content-Type': 'application/json',
                'X-API-KEY': self.api_key if self.api_key else ''
            }
            data = await self._make_post_request(self.base_url, {'query': query}, headers)

            holders = []
            if data.get('data') and data['data'].get(chain.lower()):
                # Beispiel-Struktur anpassen - Bitquery-Abfrage kann variieren
                # Diese Abfrage ist ein Beispiel, da die exakte Struktur abhängt von der Token-Implementierung
                # und der Art, wie Bitquery Token-Besitz indiziert.
                # Eine mögliche Abfrage könnte z.B. nach `ethereum.erc20Balances` suchen.
                balances = data['data'][chain.lower()].get('erc20Balances', [])
                for balance_entry in balances:
                    holders.append({
                        'TokenHolderAddress': balance_entry.get('owner', {}).get('address'),
                        'TokenHolderQuantity': str(balance_entry.get('value', 0)),
                        'percentage': 0  # Wird später berechnet
                    })
            logger.info(f"Retrieved {len(holders)} token holders from Bitquery for {contract_address} on {chain}")
            return holders
        except Exception as e:
            logger.error(f"Error fetching token holders from Bitquery: {e}")
            return []

    def _build_token_holders_query(self, contract_address: str, chain: str) -> str:
        """
        Erstellt eine GraphQL-Abfrage für Token-Halter.
        Hinweis: Diese Abfrage ist ein Beispiel und muss möglicherweise an die spezifischen Anforderungen
        und die Token-Implementierung angepasst werden. Bitquery indiziert Besitzstände nicht immer direkt.
        """
        # Beispiel-Abfrage - kann je nach Token-Standard und Bitquery-Indizierung stark variieren
        # Dies ist eine vereinfachte Version, die möglicherweise nicht für alle Tokens funktioniert.
        # Eine echte Implementierung könnte komplexere Abfragen mit `ethereum.erc20Transfers`
        # und manueller Berechnung der Balances erfordern.
        return f"""
        {{
          {chain.lower()} {{
            erc20Balances(
              currency: {{is: "{contract_address}"}}
              options: {{limit: 100, desc: "value"}}
            ) {{
              owner {{
                address
              }}
              value
            }}
          }}
        }}
        """

    # --- Bestehende Abfragemethoden ---
    def _build_price_query(self, token_address: str, chain: str) -> str:
        """Erstellt eine GraphQL-Abfrage für den Token-Preis"""
        # Für dieses Beispiel verwenden wir eine einfache Ethereum-DEX-Abfrage
        return """
        {
          ethereum {
            dexTrades(
              options: {limit: 1, desc: "block.timestamp.timeInterval"}
              baseCurrency: {is: "%s"}
              quoteCurrency: {is: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"}
            ) {
              tradeAmount(in: USD)
              quotePrice
              block {
                timestamp {
                  timeInterval(minute: 24)
                }
              }
            }
          }
        }
        """ % token_address

    def _build_dex_trades_query(self, token_address: str, chain: str, hours: int) -> str:
        """Erstellt eine GraphQL-Abfrage für DEX-Trades"""
        return """
        {
          ethereum {
            dexTrades(
              options: {desc: "block.timestamp.unixtime", limit: 100}
              baseCurrency: {is: "%s"}
              time: {since: "%s"}
            ) {
              transaction {
                hash
              }
              block {
                timestamp {
                  unixtime
                }
              }
              buyAddress
              sellAddress
              quotePrice
              tradeAmount
              tradeAmount(in: USD)
              exchange {
                address
              }
            }
          }
        }
        """ % (token_address, f"utc_now-{hours}h")

    def _build_liquidity_query(self, token_address: str, chain: str) -> str:
        """Erstellt eine GraphQL-Abfrage für Liquiditätsdaten"""
        return """
        {
          ethereum {
            dexTrades(
              options: {desc: "block.timestamp.unixtime", limit: 1000}
              baseCurrency: {is: "%s"}
              time: {since: "utc_now-24h"}
            ) {
              tradeAmount(in: USD)
              buyAddress
              sellAddress
              exchange {
                address
              }
            }
          }
        }
        """ % token_address

    def _build_wallet_activity_query(self, wallet_address: str, chain: str, hours: int) -> str:
        """Erstellt eine GraphQL-Abfrage für Wallet-Aktivitäten"""
        return """
        {
          ethereum {
            transfers(
              options: {desc: "block.timestamp.unixtime", limit: 100}
              amount: {gt: 0}
              time: {since: "%s"}
            ) {
              transaction {
                hash
              }
              block {
                timestamp {
                  unixtime
                }
              }
              sender {
                address
              }
              receiver {
                address
              }
              amount
              amount(in: USD)
              currency {
                symbol
              }
            }
          }
        }
        """ % f"utc_now-{hours}h"

    def get_rate_limits(self) -> Dict[str, int]:
        return {"requests_per_minute": 60, "requests_per_hour": 3600}
