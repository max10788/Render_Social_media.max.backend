"""
High Volume Wallet Discovery - Counterparty Analysis
====================================================

✅ FEATURES:
- Same counterparty extraction as SimpleLastTxAnalyzer
- Moralis ERC20 transfers for volume calculation
- Volume-focused scoring (not OTC-specific)
- Saves to DB with 'high_volume_wallet' entity type

Version: 1.0
Date: 2025-01-12
"""

from typing import List, Dict, Optional
from datetime import datetime
import logging
from app.core.otc_analysis.discovery.wallet_tagger import WalletTagger

logger = logging.getLogger(__name__)


class HighVolumeAnalyzer:
    """
    Discovers high-volume wallets through counterparty analysis.
    
    Similar to SimpleLastTxAnalyzer but focused on volume, not OTC patterns.
    """
    
    def __init__(
        self, 
        db, 
        transaction_extractor, 
        wallet_profiler, 
        price_oracle, 
        wallet_stats_api=None
    ):
        self.db = db
        self.transaction_extractor = transaction_extractor
        self.wallet_profiler = wallet_profiler
        self.price_oracle = price_oracle
        self.wallet_stats_api = wallet_stats_api
        self.wallet_tagger = WalletTagger()  # ✅ NEU
    
    def discover_high_volume_counterparties(
        self,
        source_address: str,
        num_transactions: int = 5,
        filter_known_entities: bool = True
    ) -> List[Dict]:
        """
        Extract counterparties from last N transactions.
        
        Same logic as SimpleLastTxAnalyzer but returns unique counterparties.
        
        Args:
            source_address: Address to analyze
            num_transactions: Number of recent transactions to check
            filter_known_entities: Filter out known exchanges/protocols
            
        Returns:
            List of unique counterparties with metadata
        """
        source_normalized = source_address.lower().strip()
        
        logger.info(
            f"🔍 Extracting counterparties from last {num_transactions} "
            f"transactions of {source_normalized[:10]}..."
        )
        
        try:
            # Fetch transactions (LIMITED immediately)
            transactions = self.transaction_extractor.extract_wallet_transactions(
                source_address,
                include_internal=True,
                include_tokens=True
            )[:num_transactions * 2]  # Buffer for sorting
            
            if not transactions:
                logger.warning(f"⚠️ No transactions found")
                return []
            
            # Sort and take recent N
            recent_txs = sorted(
                transactions,
                key=lambda x: x.get('timestamp', datetime.min),
                reverse=True
            )[:num_transactions]
            
            logger.info(f"📊 Analyzing {len(recent_txs)} recent transactions")
            
            # Extract counterparties
            counterparties_data = {}
            filtered_count = 0
            
            for tx in recent_txs:
                # Determine counterparty
                from_addr = str(tx.get('from_address', '')).lower().strip()
                to_addr = str(tx.get('to_address', '')).lower().strip()
                
                if from_addr == source_normalized and to_addr:
                    counterparty = to_addr
                    label = tx.get('to_address_label')
                    entity = tx.get('to_address_entity')
                    is_known = tx.get('to_is_known_entity', False)
                elif to_addr == source_normalized and from_addr:
                    counterparty = from_addr
                    label = tx.get('from_address_label')
                    entity = tx.get('from_address_entity')
                    is_known = tx.get('from_is_known_entity', False)
                else:
                    continue
                
                # Filter known entities
                if filter_known_entities and is_known:
                    filtered_count += 1
                    logger.info(
                        f"   ⏭️  Skipping {counterparty[:10]}... "
                        f"(known: {label or entity})"
                    )
                    continue
                
                # Track counterparty
                if counterparty not in counterparties_data:
                    counterparties_data[counterparty] = {
                        'address': counterparty,
                        'tx_count': 0,
                        'total_volume': 0,
                        'first_seen': tx['timestamp'],
                        'last_seen': tx['timestamp'],
                        'moralis_label': label,
                        'moralis_entity': entity,
                        'is_known_entity': is_known
                    }
                
                # Update stats
                cp_data = counterparties_data[counterparty]
                cp_data['tx_count'] += 1
                
                if tx.get('usd_value'):
                    cp_data['total_volume'] += tx['usd_value']
                
                if tx['timestamp'] < cp_data['first_seen']:
                    cp_data['first_seen'] = tx['timestamp']
                if tx['timestamp'] > cp_data['last_seen']:
                    cp_data['last_seen'] = tx['timestamp']
            
            logger.info(
                f"📋 Found {len(counterparties_data)} unique counterparties "
                f"(filtered {filtered_count} known entities)"
            )
            
            return list(counterparties_data.values())
            
        except Exception as e:
            logger.error(f"❌ Error extracting counterparties: {e}", exc_info=True)
            return []
    
    def analyze_volume_profile(self, counterparty_address: str) -> Optional[Dict]:
        """
        Analyze a counterparty for high-volume characteristics.
        
        Similar to SimpleLastTxAnalyzer.analyze_counterparty but volume-focused.
        
        Args:
            counterparty_address: Address to analyze
            
        Returns:
            Analysis result with volume metrics
        """
        logger.info(f"🔬 Analyzing volume profile {counterparty_address[:10]}...")
        
        try:
            # ================================================================
            # PRIORITY 1: MORALIS ERC20 TRANSFERS FOR VOLUME
            # ================================================================
            
            if self.wallet_stats_api:
                logger.info(f"   🚀 PRIORITY 1: Fetching ERC20 transfers via Moralis")
                
                transfers = self._get_moralis_erc20_transfers(
                    counterparty_address, 
                    limit=100
                )
                
                if transfers and len(transfers) > 0:
                    logger.info(f"   📊 Found {len(transfers)} ERC20 transfers")
                    
                    # Calculate volume from transfers
                    total_volume_usd = 0
                    token_diversity = set()
                    unique_counterparties = set()
                    large_transfers = 0
                    
                    normalized_address = counterparty_address.lower().strip()
                    
                    for transfer in transfers:
                        from_addr = str(transfer.get('from_address', '')).lower().strip()
                        to_addr = str(transfer.get('to_address', '')).lower().strip()
                        token_symbol = transfer.get('token_symbol', '')
                        value = float(transfer.get('value', 0) or 0)
                        decimals = int(transfer.get('token_decimals', 18) or 18)
                        
                        # Convert value
                        value_decimal = value / (10 ** decimals)
                        
                        # USD Value for stablecoins
                        if token_symbol in ['USDT', 'USDC', 'DAI', 'BUSD']:
                            value_usd = value_decimal
                        else:
                            value_usd = value_decimal * 1  # Placeholder
                        
                        total_volume_usd += value_usd
                        
                        # Track metrics
                        token_diversity.add(token_symbol)
                        
                        if from_addr == normalized_address:
                            unique_counterparties.add(to_addr)
                        elif to_addr == normalized_address:
                            unique_counterparties.add(from_addr)
                        
                        if value_usd >= 100_000:  # $100K+ = large
                            large_transfers += 1
                    
                    # Create profile
                    profile = {
                        'address': counterparty_address,
                        'total_volume_usd': total_volume_usd,
                        'unique_counterparties': len(unique_counterparties),
                        'token_diversity': len(token_diversity),
                        'transfer_count': len(transfers),
                        'avg_transfer_usd': total_volume_usd / len(transfers),
                        'large_transfer_count': large_transfers,
                        'first_seen': transfers[-1].get('block_timestamp') if transfers else None,
                        'last_seen': transfers[0].get('block_timestamp') if transfers else None,
                        'data_quality': 'high',
                        'profile_method': 'moralis_erc20_transfers'
                    }
                    
                    result = {
                        'address': counterparty_address,
                        'total_volume': total_volume_usd,
                        'transaction_count': len(transfers),
                        'avg_transaction': total_volume_usd / len(transfers),
                        'first_seen': profile['first_seen'],
                        'last_seen': profile['last_seen'],
                        'profile': profile,
                        'strategy': 'moralis_erc20_transfers',
                        'data_quality': 'high'
                    }
                    
                    logger.info(
                        f"✅ Moralis Analysis: "
                        f"${total_volume_usd:,.0f} volume, "
                        f"{len(token_diversity)} tokens, "
                        f"{large_transfers} large transfers"
                    )
                    
                    return result
                else:
                    logger.warning(f"   ⚠️ No ERC20 transfers found via Moralis")
            
            # ================================================================
            # FALLBACK: Transaction Processing
            # ================================================================
            
            logger.info(f"   📊 FALLBACK: Processing transactions manually")
            
            # Fetch transactions
            transactions = self.transaction_extractor.extract_wallet_transactions(
                counterparty_address,
                include_internal=True,
                include_tokens=True
            )
            
            if not transactions:
                logger.warning(f"⚠️ No transactions for {counterparty_address[:10]}")
                return None
            
            # Enrich with USD
            transactions = self.transaction_extractor.enrich_with_usd_value(
                transactions,
                self.price_oracle,
                max_transactions=50
            )
            
            # Create profile
            profile = self.wallet_profiler.create_profile(
                counterparty_address,
                transactions,
                labels={}
            )
            
            result = {
                'address': counterparty_address,
                'total_volume': profile.get('total_volume_usd', 0),
                'transaction_count': len(transactions),
                'avg_transaction': profile.get('avg_transaction_usd', 0),
                'first_seen': profile.get('first_seen'),
                'last_seen': profile.get('last_seen'),
                'profile': profile,
                'strategy': 'transaction_processing',
                'data_quality': profile.get('data_quality', 'unknown')
            }
            
            logger.info(
                f"✅ Transaction Processing: "
                f"${result['total_volume']:,.0f} volume"
            )
            
            # ✅ NEU: Generate comprehensive tags
            logger.info(f"   🏷️  Generating characteristic tags...")
            
            categorized_tags = self.wallet_tagger.generate_comprehensive_tags(
                address=counterparty_address,
                transactions=cp_transactions,
                profile=profile,
                scoring_metrics={
                    'total_volume': result['total_volume'],
                    'avg_transaction': result['avg_transaction'],
                    'tx_count': result['transaction_count'],
                    'token_diversity': profile.get('token_diversity', 0),
                    'unique_counterparties': profile.get('unique_counterparties', 0),
                    'large_transfer_count': profile.get('large_transfer_count', 0)
                }
            )
            
            # Add categorized tags to result
            result['categorized_tags'] = categorized_tags
            
            logger.info(
                f"✅ Analysis complete with {len(categorized_tags['all'])} tags "
                f"across {len([k for k in categorized_tags.keys() if k != 'all'])} categories"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing {counterparty_address[:10]}: {e}", exc_info=True)
            return None
    
    def _get_moralis_erc20_transfers(
        self, 
        address: str, 
        limit: int = 100
    ) -> Optional[list]:
        """
        Fetch ERC20 transfers via Moralis API.
        
        Same as SimpleLastTxAnalyzer implementation.
        """
        if not self.wallet_stats_api or not hasattr(self.wallet_stats_api, 'moralis_available'):
            logger.warning(f"   ⚠️ Moralis API not available")
            return None
        
        try:
            import requests
            
            url = f"https://deep-index.moralis.io/api/v2.2/{address}/erc20/transfers"
            
            # Get Moralis key from wallet_stats_api
            if not hasattr(self.wallet_stats_api, 'moralis_key'):
                logger.warning(f"   ⚠️ Moralis key not found")
                return None
            
            response = requests.get(
                url,
                headers={
                    'X-API-Key': self.wallet_stats_api.moralis_key,
                    'accept': 'application/json'
                },
                params={'limit': limit},
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                transfers = data.get('result', [])
                
                logger.info(f"   ✅ Fetched {len(transfers)} ERC20 transfers from Moralis")
                
                # Track success
                if hasattr(self.wallet_stats_api, 'api_error_tracker'):
                    self.wallet_stats_api.api_error_tracker.track_call('moralis', success=True)
                
                return transfers
            
            elif response.status_code == 429:
                logger.warning(f"   ⏱️  Moralis rate limit")
                if hasattr(self.wallet_stats_api, 'api_error_tracker'):
                    self.wallet_stats_api.api_error_tracker.track_call('moralis', success=False, error='rate_limit')
            
            else:
                logger.warning(f"   ❌ Moralis API failed: HTTP {response.status_code}")
                if hasattr(self.wallet_stats_api, 'api_error_tracker'):
                    self.wallet_stats_api.api_error_tracker.track_call('moralis', success=False, error=f'http_{response.status_code}')
            
            return None
            
        except Exception as e:
            logger.warning(f"   ❌ Moralis ERC20 error: {type(e).__name__}")
            if hasattr(self.wallet_stats_api, 'api_error_tracker'):
                self.wallet_stats_api.api_error_tracker.track_call('moralis', success=False, error=type(e).__name__)
            return None


# Export
__all__ = ['HighVolumeAnalyzer']
