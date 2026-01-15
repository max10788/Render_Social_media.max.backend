"""
High Volume Wallet Discovery - Counterparty Analysis
====================================================

✅ ENHANCED v2.0 - Balance + Activity Integration:
- Current balance tracking via BalanceFetcher
- Temporal activity analysis via ActivityAnalyzer
- Combined scoring via BalanceScorer
- Intelligent classification (active vs dormant whales)

✅ FEATURES:
- Same counterparty extraction as SimpleLastTxAnalyzer
- Moralis ERC20 transfers for volume calculation
- Volume-focused scoring (not OTC-specific)
- Saves to DB with 'high_volume_wallet' entity type
- **NEW**: Balance-aware classification
- **NEW**: Activity pattern detection
- **NEW**: Risk assessment based on current state

Version: 2.0
Date: 2025-01-15
"""

from typing import List, Dict, Optional
from datetime import datetime
import logging

# ✨ NEW IMPORTS
from app.core.otc_analysis.blockchain.balance_fetcher import BalanceFetcher
from app.core.otc_analysis.discovery.activity_analyzer import ActivityAnalyzer
from app.core.otc_analysis.discovery.balance_scorer import BalanceScorer
from app.core.otc_analysis.discovery.wallet_tagger import WalletTagger

logger = logging.getLogger(__name__)


class HighVolumeAnalyzer:
    """
    Discovers high-volume wallets through counterparty analysis.
    
    ✨ ENHANCED v2.0:
    Now includes balance and activity analysis for accurate classification.
    Prevents misclassification of dormant/depleted wallets.
    """
    
    def __init__(
        self, 
        db, 
        transaction_extractor, 
        wallet_profiler, 
        price_oracle, 
        wallet_stats_api=None,
        balance_fetcher=None,  # ✨ NEW
        activity_analyzer=None,  # ✨ NEW
        balance_scorer=None  # ✨ NEW
    ):
        self.db = db
        self.transaction_extractor = transaction_extractor
        self.wallet_profiler = wallet_profiler
        self.price_oracle = price_oracle
        self.wallet_stats_api = wallet_stats_api
        self.wallet_tagger = WalletTagger()
        
        # ✨ NEW: Balance & Activity Services
        self.balance_fetcher = balance_fetcher or BalanceFetcher(
            cache_manager=None,
            price_oracle=price_oracle
        )
        self.activity_analyzer = activity_analyzer or ActivityAnalyzer(
            dormancy_threshold_days=90
        )
        self.balance_scorer = balance_scorer or BalanceScorer(
            min_active_balance_usd=10_000
        )
        
        logger.info("✅ HighVolumeAnalyzer v2.0 initialized with Balance + Activity tracking")
    
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
    
    def analyze_volume_profile(
        self, 
        counterparty_address: str,
        include_balance_analysis: bool = True,  # ✨ NEW
        include_activity_analysis: bool = True  # ✨ NEW
    ) -> Optional[Dict]:
        """
        Analyze a counterparty for high-volume characteristics.
        
        ✨ ENHANCED v2.0:
        Now includes balance and activity analysis for accurate classification.
        
        Args:
            counterparty_address: Address to analyze
            include_balance_analysis: Fetch current balance (default: True)
            include_activity_analysis: Analyze activity patterns (default: True)
            
        Returns:
            Analysis result with volume metrics + balance + activity + categorized tags
        """
        logger.info(f"🔬 Analyzing volume profile {counterparty_address[:10]}...")
        
        try:
            # ================================================================
            # STEP 1: GET VOLUME DATA (Moralis or Fallback)
            # ================================================================
            
            volume_result = self._get_volume_data(counterparty_address)
            
            if not volume_result:
                logger.warning(f"⚠️ No volume data for {counterparty_address[:10]}")
                return None
            
            # ================================================================
            # ✨ STEP 2: GET CURRENT BALANCE (NEW!)
            # ================================================================
            
            balance_data = None
            
            if include_balance_analysis:
                logger.info(f"   💰 Fetching current balance...")
                
                try:
                    balance_data = self.balance_fetcher.get_total_balance_usd(
                        counterparty_address,
                        use_cache=True
                    )
                    
                    logger.info(
                        f"   ✅ Balance: ${balance_data['total_balance_usd']:,.2f} "
                        f"(Native: ${balance_data['native_balance_usd']:,.2f}, "
                        f"Tokens: ${balance_data['token_balance_usd']:,.2f})"
                    )
                    
                except Exception as balance_error:
                    logger.warning(f"   ⚠️ Balance fetch failed: {balance_error}")
                    balance_data = None
            
            # ================================================================
            # ✨ STEP 3: ANALYZE ACTIVITY PATTERNS (NEW!)
            # ================================================================
            
            activity_analysis = None
            
            if include_activity_analysis and volume_result.get('transactions'):
                logger.info(f"   📅 Analyzing activity patterns...")
                
                try:
                    activity_analysis = self.activity_analyzer.get_temporal_analysis(
                        volume_result['transactions'],
                        current_time=datetime.now()
                    )
                    
                    pattern = activity_analysis['pattern']['pattern']
                    score = activity_analysis['activity_score']
                    lifecycle = activity_analysis['lifecycle_stage']
                    
                    logger.info(
                        f"   ✅ Activity: {pattern} pattern, "
                        f"{lifecycle} stage, "
                        f"score: {score:.1f}/100"
                    )
                    
                except Exception as activity_error:
                    logger.warning(f"   ⚠️ Activity analysis failed: {activity_error}")
                    activity_analysis = None
            
            # ================================================================
            # ✨ STEP 4: COMBINED SCORING (NEW!)
            # ================================================================
            
            combined_scoring = None
            
            if balance_data and activity_analysis:
                logger.info(f"   🎯 Calculating combined score...")
                
                try:
                    # Get balance status
                    current_balance = balance_data['total_balance_usd']
                    historical_volume = volume_result['total_volume']
                    
                    balance_status = self.balance_scorer.classify_balance_status(
                        current_balance,
                        historical_volume
                    )
                    
                    # Calculate balance health
                    balance_health = self.balance_scorer.score_balance_health(
                        balance_status,
                        current_balance
                    )
                    
                    # Combine balance + activity
                    combined_scoring = self.balance_scorer.combine_balance_and_activity(
                        balance_health_score=balance_health,
                        activity_score=activity_analysis['activity_score'],
                        activity_pattern=activity_analysis['pattern'],
                        balance_status=balance_status,
                        historical_volume_usd=historical_volume
                    )
                    
                    logger.info(
                        f"   ✅ Combined: {combined_scoring['combined_score']:.1f}/100 "
                        f"(classification: {combined_scoring['final_classification']})"
                    )
                    
                except Exception as scoring_error:
                    logger.warning(f"   ⚠️ Combined scoring failed: {scoring_error}")
                    combined_scoring = None
            
            # ================================================================
            # STEP 5: GENERATE COMPREHENSIVE TAGS
            # ================================================================
            
            logger.info(f"   🏷️  Generating characteristic tags...")
            
            categorized_tags = self.wallet_tagger.generate_comprehensive_tags(
                address=counterparty_address,
                transactions=volume_result.get('transactions', []),
                profile=volume_result.get('profile', {}),
                scoring_metrics={
                    'total_volume': volume_result['total_volume'],
                    'avg_transaction': volume_result['avg_transaction'],
                    'tx_count': volume_result['transaction_count'],
                    'token_diversity': volume_result.get('profile', {}).get('token_diversity', 0),
                    'unique_counterparties': volume_result.get('profile', {}).get('unique_counterparties', 0),
                    'large_transfer_count': volume_result.get('profile', {}).get('large_transfer_count', 0)
                }
            )
            
            # ================================================================
            # ✨ STEP 6: BUILD ENHANCED RESULT (NEW!)
            # ================================================================
            
            result = {
                # Original volume data
                'address': counterparty_address,
                'total_volume': volume_result['total_volume'],
                'transaction_count': volume_result['transaction_count'],
                'avg_transaction': volume_result['avg_transaction'],
                'first_seen': volume_result['first_seen'],
                'last_seen': volume_result['last_seen'],
                'profile': volume_result['profile'],
                'strategy': volume_result['strategy'],
                'data_quality': volume_result['data_quality'],
                'categorized_tags': categorized_tags,
                
                # ✨ NEW: Balance data
                'balance_analysis': balance_data,
                
                # ✨ NEW: Activity data
                'activity_analysis': activity_analysis,
                
                # ✨ NEW: Combined scoring
                'combined_scoring': combined_scoring,
                
                # ✨ NEW: Enhanced classification
                'enhanced_classification': self._determine_enhanced_classification(
                    volume_result,
                    balance_data,
                    activity_analysis,
                    combined_scoring
                )
            }
            
            logger.info(
                f"✅ Analysis complete: "
                f"${result['total_volume']:,.0f} volume, "
                f"{len(categorized_tags['all'])} tags"
            )
            
            if combined_scoring:
                logger.info(
                    f"   📊 Enhanced: {result['enhanced_classification']['classification']} "
                    f"(risk: {result['enhanced_classification']['risk_level']})"
                )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error analyzing {counterparty_address[:10]}: {e}", exc_info=True)
            return None
    
    # ========================================================================
    # ✨ NEW HELPER METHODS
    # ========================================================================
    
    def _get_volume_data(self, address: str) -> Optional[Dict]:
        """
        Get volume data via Moralis or fallback.
        
        Extracted from original analyze_volume_profile for cleaner structure.
        """
        # ================================================================
        # PRIORITY 1: MORALIS ERC20 TRANSFERS FOR VOLUME
        # ================================================================
        
        if self.wallet_stats_api:
            logger.info(f"   🚀 PRIORITY 1: Fetching ERC20 transfers via Moralis")
            
            transfers = self._get_moralis_erc20_transfers(address, limit=100)
            
            if transfers and len(transfers) > 0:
                logger.info(f"   📊 Found {len(transfers)} ERC20 transfers")
                
                # Calculate volume from transfers
                total_volume_usd = 0
                token_diversity = set()
                unique_counterparties = set()
                large_transfers = 0
                
                normalized_address = address.lower().strip()
                
                # Convert transfers to transaction format
                transactions_for_analysis = []
                
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
                    
                    # ✨ Convert to transaction format for activity analysis
                    transactions_for_analysis.append({
                        'from_address': from_addr,
                        'to_address': to_addr,
                        'token_symbol': token_symbol,
                        'timestamp': transfer.get('block_timestamp'),
                        'usd_value': value_usd
                    })
                
                # Create profile
                profile = {
                    'address': address,
                    'total_volume_usd': total_volume_usd,
                    'unique_counterparties': len(unique_counterparties),
                    'token_diversity': len(token_diversity),
                    'transfer_count': len(transfers),
                    'avg_transfer_usd': total_volume_usd / len(transfers) if len(transfers) > 0 else 0,
                    'large_transfer_count': large_transfers,
                    'first_seen': transfers[-1].get('block_timestamp') if transfers else None,
                    'last_seen': transfers[0].get('block_timestamp') if transfers else None,
                    'data_quality': 'high',
                    'profile_method': 'moralis_erc20_transfers'
                }
                
                return {
                    'address': address,
                    'total_volume': total_volume_usd,
                    'transaction_count': len(transfers),
                    'avg_transaction': total_volume_usd / len(transfers) if len(transfers) > 0 else 0,
                    'first_seen': profile['first_seen'],
                    'last_seen': profile['last_seen'],
                    'profile': profile,
                    'strategy': 'moralis_erc20_transfers',
                    'data_quality': 'high',
                    'transactions': transactions_for_analysis  # ✨ For activity analysis
                }
            else:
                logger.warning(f"   ⚠️ No ERC20 transfers found via Moralis")
        
        # ================================================================
        # FALLBACK: Transaction Processing
        # ================================================================
        
        logger.info(f"   📊 FALLBACK: Processing transactions manually")
        
        # Fetch transactions
        transactions = self.transaction_extractor.extract_wallet_transactions(
            address,
            include_internal=True,
            include_tokens=True
        )
        
        if not transactions:
            logger.warning(f"⚠️ No transactions for {address[:10]}")
            return None
        
        # Enrich with USD
        transactions = self.transaction_extractor.enrich_with_usd_value(
            transactions,
            self.price_oracle,
            max_transactions=50
        )
        
        # Create profile
        profile = self.wallet_profiler.create_profile(
            address,
            transactions,
            labels={}
        )
        
        return {
            'address': address,
            'total_volume': profile.get('total_volume_usd', 0),
            'transaction_count': len(transactions),
            'avg_transaction': profile.get('avg_transaction_usd', 0),
            'first_seen': profile.get('first_seen'),
            'last_seen': profile.get('last_seen'),
            'profile': profile,
            'strategy': 'transaction_processing',
            'data_quality': profile.get('data_quality', 'unknown'),
            'transactions': transactions  # ✨ For activity analysis
        }
    
    def _determine_enhanced_classification(
        self,
        volume_result: Dict,
        balance_data: Optional[Dict],
        activity_analysis: Optional[Dict],
        combined_scoring: Optional[Dict]
    ) -> Dict[str, any]:
        """
        Determine enhanced classification using all available data.
        
        ✨ NEW METHOD:
        Combines volume, balance, and activity for accurate classification.
        """
        # Base classification from volume
        total_volume = volume_result['total_volume']
        
        # If we have combined scoring, use that
        if combined_scoring:
            return {
                'classification': combined_scoring['final_classification'],
                'confidence': combined_scoring['combined_score'] / 100,
                'risk_level': combined_scoring['risk_assessment']['level'],
                'risk_factors': combined_scoring['risk_assessment']['factors'],
                'method': 'combined_scoring'
            }
        
        # Fallback to volume-only classification
        if total_volume >= 100_000_000:
            base_class = 'mega_whale'
        elif total_volume >= 10_000_000:
            base_class = 'whale'
        elif total_volume >= 1_000_000:
            base_class = 'high_volume_trader'
        else:
            base_class = 'moderate_volume'
        
        # Adjust for activity if available
        if activity_analysis:
            pattern = activity_analysis['pattern']['pattern']
            if pattern == 'dormant':
                base_class += '_dormant'
            elif pattern in ['sustained', 'active']:
                base_class += '_active'
        
        return {
            'classification': base_class,
            'confidence': 0.5,  # Lower confidence without full scoring
            'risk_level': 'medium',
            'risk_factors': ['Classification based on volume only'],
            'method': 'volume_only'
        }
    
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
