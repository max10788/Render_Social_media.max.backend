"""
Wallet Tagger - Generate comprehensive characteristic tags for wallets
======================================================================

✅ MULTI-DIMENSIONAL TAGGING:
- Volume & Size Tags
- Activity & Frequency Tags
- Token Preference Tags
- Trading Behavior Tags
- Network Pattern Tags
- Risk Profile Tags
- Temporal Pattern Tags

Version: 1.0
Date: 2025-01-12
"""

from typing import Dict, List, Set
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class WalletTagger:
    """
    Generates comprehensive tags for wallet characterization.
    
    Tags are organized into categories:
    - volume: Volume-related characteristics
    - activity: Activity frequency and patterns
    - tokens: Token preferences and diversity
    - behavior: Trading behavior patterns
    - network: Counterparty and network patterns
    - risk: Risk profile indicators
    - temporal: Time-based patterns
    """
    
    def __init__(self):
        logger.info("✅ WalletTagger initialized")
    
    def generate_comprehensive_tags(
        self,
        address: str,
        transactions: List[Dict],
        profile: Dict,
        scoring_metrics: Dict
    ) -> Dict[str, List[str]]:
        """
        Generate comprehensive tags across all categories.
        
        Args:
            address: Wallet address
            transactions: List of transactions
            profile: Wallet profile from analyzer
            scoring_metrics: Metrics from volume scorer
            
        Returns:
            Dict with categorized tags:
            {
                'volume': [...],
                'activity': [...],
                'tokens': [...],
                'behavior': [...],
                'network': [...],
                'risk': [...],
                'temporal': [...],
                'all': [...]  # Combined list
            }
        """
        logger.info(f"🏷️  Generating comprehensive tags for {address[:10]}...")
        
        try:
            categorized_tags = {
                'volume': self._generate_volume_tags(scoring_metrics),
                'activity': self._generate_activity_tags(transactions, profile),
                'tokens': self._generate_token_tags(transactions, profile),
                'behavior': self._generate_behavior_tags(transactions, profile),
                'network': self._generate_network_tags(transactions, profile),
                'risk': self._generate_risk_tags(transactions, profile, scoring_metrics),
                'temporal': self._generate_temporal_tags(transactions)
            }
            
            # Combine all tags
            all_tags = []
            for category, tags in categorized_tags.items():
                all_tags.extend(tags)
            
            categorized_tags['all'] = list(set(all_tags))  # Remove duplicates
            
            # Log summary
            logger.info(f"   ✅ Generated {len(categorized_tags['all'])} unique tags:")
            for category, tags in categorized_tags.items():
                if category != 'all' and tags:
                    logger.info(f"      • {category}: {len(tags)} tags")
            
            return categorized_tags
            
        except Exception as e:
            logger.error(f"❌ Error generating tags: {e}", exc_info=True)
            return {
                'volume': [],
                'activity': [],
                'tokens': [],
                'behavior': [],
                'network': [],
                'risk': [],
                'temporal': [],
                'all': ['error_tagging']
            }
    
    def _generate_volume_tags(self, metrics: Dict) -> List[str]:
        """Generate volume-related tags."""
        tags = []
        
        total_volume = metrics.get('total_volume', 0)
        avg_transaction = metrics.get('avg_transaction', 0)
        large_transfer_count = metrics.get('large_transfer_count', 0)
        
        # Total volume tiers
        if total_volume >= 1_000_000_000:  # $1B+
            tags.extend(['billion_club', 'ultra_whale'])
        elif total_volume >= 100_000_000:  # $100M+
            tags.extend(['mega_whale', 'ultra_high_volume'])
        elif total_volume >= 50_000_000:  # $50M+
            tags.extend(['whale', 'very_high_volume'])
        elif total_volume >= 10_000_000:  # $10M+
            tags.extend(['large_wallet', 'high_volume'])
        elif total_volume >= 5_000_000:  # $5M+
            tags.extend(['medium_large_wallet', 'significant_volume'])
        elif total_volume >= 1_000_000:  # $1M+
            tags.extend(['medium_wallet', 'notable_volume'])
        
        # Average transaction size
        if avg_transaction >= 10_000_000:  # $10M+ avg
            tags.extend(['massive_transactions', 'institutional_scale'])
        elif avg_transaction >= 1_000_000:  # $1M+ avg
            tags.extend(['large_transactions', 'institutional_size'])
        elif avg_transaction >= 500_000:  # $500K+ avg
            tags.extend(['substantial_transactions', 'professional_scale'])
        elif avg_transaction >= 100_000:  # $100K+ avg
            tags.extend(['medium_large_transactions', 'serious_trader'])
        elif avg_transaction >= 50_000:  # $50K+ avg
            tags.extend(['medium_transactions', 'active_trader_scale'])
        elif avg_transaction >= 10_000:  # $10K+ avg
            tags.extend(['small_medium_transactions', 'retail_plus'])
        
        # Large transfer patterns
        if large_transfer_count >= 50:
            tags.append('frequent_large_mover')
        elif large_transfer_count >= 20:
            tags.append('regular_large_mover')
        elif large_transfer_count >= 10:
            tags.append('occasional_large_mover')
        elif large_transfer_count >= 5:
            tags.append('selective_large_mover')
        
        return tags
    
    def _generate_activity_tags(self, transactions: List[Dict], profile: Dict) -> List[str]:
        """Generate activity frequency and pattern tags."""
        tags = []
        
        tx_count = profile.get('transfer_count', 0) or len(transactions)
        
        # Transaction frequency
        if tx_count >= 1000:
            tags.extend(['hyperactive', 'very_high_frequency'])
        elif tx_count >= 500:
            tags.extend(['very_active', 'high_frequency'])
        elif tx_count >= 200:
            tags.extend(['active_trader', 'frequent_transactions'])
        elif tx_count >= 100:
            tags.extend(['regular_trader', 'moderate_frequency'])
        elif tx_count >= 50:
            tags.extend(['occasional_trader', 'selective_activity'])
        elif tx_count >= 20:
            tags.extend(['infrequent_trader', 'low_frequency'])
        else:
            tags.extend(['rare_trader', 'minimal_activity'])
        
        # Activity intensity (if timestamps available)
        if transactions:
            try:
                # Calculate activity span
                timestamps = [tx.get('timestamp') or tx.get('block_timestamp') for tx in transactions]
                timestamps = [t for t in timestamps if t]
                
                if len(timestamps) >= 2:
                    # Convert string timestamps to datetime if needed
                    dt_timestamps = []
                    for ts in timestamps:
                        if isinstance(ts, str):
                            try:
                                dt_timestamps.append(datetime.fromisoformat(ts.replace('Z', '+00:00')))
                            except:
                                continue
                        elif isinstance(ts, datetime):
                            dt_timestamps.append(ts)
                    
                    if len(dt_timestamps) >= 2:
                        first = min(dt_timestamps)
                        last = max(dt_timestamps)
                        span_days = (last - first).days
                        
                        if span_days > 0:
                            tx_per_day = tx_count / span_days
                            
                            if tx_per_day >= 10:
                                tags.append('daily_multiple_transactions')
                            elif tx_per_day >= 1:
                                tags.append('daily_active')
                            elif tx_per_day >= 0.5:
                                tags.append('every_other_day')
                            elif tx_per_day >= 0.14:  # Weekly
                                tags.append('weekly_active')
                            else:
                                tags.append('sporadic_activity')
                        
                        # Account age
                        if span_days >= 730:  # 2 years
                            tags.append('veteran_account')
                        elif span_days >= 365:  # 1 year
                            tags.append('mature_account')
                        elif span_days >= 180:  # 6 months
                            tags.append('established_account')
                        elif span_days >= 90:  # 3 months
                            tags.append('developing_account')
                        else:
                            tags.append('new_account')
            except Exception as e:
                logger.debug(f"Could not analyze activity timing: {e}")
        
        return tags
    
    def _generate_token_tags(self, transactions: List[Dict], profile: Dict) -> List[str]:
        """Generate token preference and diversity tags."""
        tags = []
        
        token_diversity = profile.get('token_diversity', 0)
        
        # Token diversity
        if token_diversity >= 20:
            tags.extend(['highly_diversified', 'multi_asset_portfolio'])
        elif token_diversity >= 10:
            tags.extend(['well_diversified', 'broad_portfolio'])
        elif token_diversity >= 5:
            tags.extend(['diversified', 'varied_assets'])
        elif token_diversity >= 3:
            tags.extend(['multi_token', 'selective_portfolio'])
        elif token_diversity >= 2:
            tags.extend(['dual_token', 'focused_portfolio'])
        else:
            tags.extend(['single_token', 'concentrated_portfolio'])
        
        # Analyze token types (if data available)
        if transactions:
            token_symbols = set()
            stablecoin_count = 0
            defi_token_count = 0
            
            for tx in transactions:
                token = tx.get('token_symbol', '')
                if token:
                    token_symbols.add(token.upper())
                    
                    # Stablecoins
                    if token.upper() in ['USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDP', 'FRAX']:
                        stablecoin_count += 1
                    
                    # DeFi tokens (examples)
                    if token.upper() in ['UNI', 'AAVE', 'CRV', 'COMP', 'MKR', 'SNX', 'SUSHI']:
                        defi_token_count += 1
            
            # Stablecoin preference
            if stablecoin_count > len(transactions) * 0.7:
                tags.extend(['stablecoin_heavy', 'fiat_proxy'])
            elif stablecoin_count > len(transactions) * 0.4:
                tags.extend(['stablecoin_user', 'stable_preference'])
            
            # DeFi exposure
            if defi_token_count > len(transactions) * 0.5:
                tags.extend(['defi_native', 'protocol_heavy'])
            elif defi_token_count > len(transactions) * 0.2:
                tags.extend(['defi_user', 'protocol_exposure'])
            
            # ETH vs ERC20
            eth_count = sum(1 for tx in transactions if tx.get('token_symbol', 'ETH') == 'ETH')
            if eth_count > len(transactions) * 0.7:
                tags.append('eth_focused')
            elif eth_count < len(transactions) * 0.3:
                tags.append('erc20_focused')
            else:
                tags.append('mixed_assets')
        
        return tags
    
    def _generate_behavior_tags(self, transactions: List[Dict], profile: Dict) -> List[str]:
        """Generate trading behavior pattern tags."""
        tags = []
        
        if not transactions:
            return tags
        
        # Analyze inflow vs outflow
        inflow_count = 0
        outflow_count = 0
        inflow_volume = 0
        outflow_volume = 0
        
        address_lower = profile.get('address', '').lower()
        
        for tx in transactions:
            from_addr = str(tx.get('from_address', '')).lower()
            to_addr = str(tx.get('to_address', '')).lower()
            value = tx.get('usd_value', 0) or tx.get('value_usd', 0) or 0
            
            if to_addr == address_lower:
                inflow_count += 1
                inflow_volume += value
            elif from_addr == address_lower:
                outflow_count += 1
                outflow_volume += value
        
        total_count = inflow_count + outflow_count
        
        if total_count > 0:
            inflow_ratio = inflow_count / total_count
            
            # Accumulator vs Distributor
            if inflow_ratio >= 0.7:
                tags.extend(['accumulator', 'net_buyer', 'capital_inflow'])
            elif inflow_ratio >= 0.55:
                tags.extend(['slight_accumulator', 'buying_bias'])
            elif inflow_ratio >= 0.45:
                tags.extend(['balanced_trader', 'neutral_flow'])
            elif inflow_ratio >= 0.3:
                tags.extend(['slight_distributor', 'selling_bias'])
            else:
                tags.extend(['distributor', 'net_seller', 'capital_outflow'])
            
            # Volume-based patterns
            total_volume = inflow_volume + outflow_volume
            if total_volume > 0:
                inflow_vol_ratio = inflow_volume / total_volume
                
                if abs(inflow_vol_ratio - 0.5) < 0.1:
                    tags.append('volume_balanced')
                elif inflow_vol_ratio > 0.6:
                    tags.append('large_inflows')
                elif inflow_vol_ratio < 0.4:
                    tags.append('large_outflows')
        
        # Transaction size consistency
        if len(transactions) >= 5:
            values = [tx.get('usd_value', 0) or tx.get('value_usd', 0) or 0 for tx in transactions]
            values = [v for v in values if v > 0]
            
            if len(values) >= 3:
                import statistics
                avg = statistics.mean(values)
                
                try:
                    stdev = statistics.stdev(values)
                    cv = stdev / avg if avg > 0 else 0  # Coefficient of variation
                    
                    if cv < 0.3:
                        tags.extend(['consistent_sizing', 'systematic_trader'])
                    elif cv > 1.5:
                        tags.extend(['variable_sizing', 'opportunistic_trader'])
                except:
                    pass
        
        return tags
    
    def _generate_network_tags(self, transactions: List[Dict], profile: Dict) -> List[str]:
        """Generate counterparty and network pattern tags."""
        tags = []
        
        unique_counterparties = profile.get('unique_counterparties', 0)
        
        # Counterparty diversity
        if unique_counterparties >= 100:
            tags.extend(['wide_network', 'many_counterparties'])
        elif unique_counterparties >= 50:
            tags.extend(['broad_network', 'diverse_counterparties'])
        elif unique_counterparties >= 20:
            tags.extend(['moderate_network', 'multiple_counterparties'])
        elif unique_counterparties >= 10:
            tags.extend(['limited_network', 'few_counterparties'])
        elif unique_counterparties >= 5:
            tags.extend(['narrow_network', 'selective_counterparties'])
        else:
            tags.extend(['minimal_network', 'very_few_counterparties'])
        
        # Analyze counterparty concentration
        if transactions and unique_counterparties > 0:
            counterparty_counts = {}
            address_lower = profile.get('address', '').lower()
            
            for tx in transactions:
                from_addr = str(tx.get('from_address', '')).lower()
                to_addr = str(tx.get('to_address', '')).lower()
                
                if from_addr == address_lower and to_addr:
                    counterparty_counts[to_addr] = counterparty_counts.get(to_addr, 0) + 1
                elif to_addr == address_lower and from_addr:
                    counterparty_counts[from_addr] = counterparty_counts.get(from_addr, 0) + 1
            
            if counterparty_counts:
                max_count = max(counterparty_counts.values())
                concentration = max_count / len(transactions)
                
                if concentration >= 0.5:
                    tags.extend(['concentrated_counterparty', 'primary_partner'])
                elif concentration >= 0.3:
                    tags.extend(['preferred_counterparty', 'regular_partner'])
                else:
                    tags.append('distributed_counterparties')
        
        return tags
    
    def _generate_risk_tags(
        self, 
        transactions: List[Dict], 
        profile: Dict, 
        metrics: Dict
    ) -> List[str]:
        """Generate risk profile tags."""
        tags = []
        
        avg_transaction = metrics.get('avg_transaction', 0)
        total_volume = metrics.get('total_volume', 0)
        tx_count = metrics.get('tx_count', 0)
        
        # Position sizing risk
        if tx_count > 0:
            avg_as_percent = (avg_transaction / total_volume * 100) if total_volume > 0 else 0
            
            if avg_as_percent >= 50:
                tags.extend(['concentrated_bets', 'high_risk_sizing'])
            elif avg_as_percent >= 20:
                tags.extend(['large_positions', 'elevated_risk'])
            elif avg_as_percent <= 1:
                tags.extend(['diversified_sizing', 'low_risk_sizing'])
        
        # Volume risk tiers
        if total_volume >= 50_000_000:
            tags.append('systemic_risk_potential')
        elif total_volume >= 10_000_000:
            tags.append('material_market_impact')
        
        # Transaction frequency risk
        if tx_count >= 500:
            tags.append('high_churn_risk')
        
        return tags
    
    def _generate_temporal_tags(self, transactions: List[Dict]) -> List[str]:
        """Generate time-based pattern tags."""
        tags = []
        
        if not transactions or len(transactions) < 5:
            return tags
        
        try:
            # Extract timestamps
            timestamps = []
            for tx in transactions:
                ts = tx.get('timestamp') or tx.get('block_timestamp')
                if ts:
                    if isinstance(ts, str):
                        try:
                            timestamps.append(datetime.fromisoformat(ts.replace('Z', '+00:00')))
                        except:
                            continue
                    elif isinstance(ts, datetime):
                        timestamps.append(ts)
            
            if len(timestamps) < 5:
                return tags
            
            # Analyze recency
            now = datetime.now(timestamps[0].tzinfo) if timestamps[0].tzinfo else datetime.now()
            last_tx = max(timestamps)
            days_since_last = (now - last_tx).days
            
            if days_since_last <= 1:
                tags.append('active_today')
            elif days_since_last <= 7:
                tags.append('active_this_week')
            elif days_since_last <= 30:
                tags.append('active_this_month')
            elif days_since_last <= 90:
                tags.append('recent_activity')
            else:
                tags.append('dormant_recently')
            
            # Analyze hour patterns (if enough data)
            hours = [ts.hour for ts in timestamps]
            
            if len(hours) >= 10:
                # Business hours (9-17)
                business_hours = sum(1 for h in hours if 9 <= h < 17)
                business_ratio = business_hours / len(hours)
                
                if business_ratio >= 0.7:
                    tags.append('business_hours_trader')
                elif business_ratio <= 0.3:
                    tags.append('off_hours_trader')
                
                # Night trading (22-6)
                night_hours = sum(1 for h in hours if h >= 22 or h < 6)
                night_ratio = night_hours / len(hours)
                
                if night_ratio >= 0.5:
                    tags.append('night_trader')
        
        except Exception as e:
            logger.debug(f"Could not analyze temporal patterns: {e}")
        
        return tags
    
    def get_tag_descriptions(self) -> Dict[str, str]:
        """Get descriptions for all possible tags."""
        return {
            # Volume tags
            'billion_club': 'Total volume exceeds $1 billion',
            'ultra_whale': 'Extremely high volume participant',
            'mega_whale': 'Total volume exceeds $100 million',
            'whale': 'Total volume exceeds $50 million',
            'large_wallet': 'Total volume exceeds $10 million',
            
            # Activity tags
            'hyperactive': '1000+ transactions',
            'very_active': '500+ transactions',
            'active_trader': '200+ transactions',
            'daily_active': 'Transacts approximately daily',
            
            # Token tags
            'highly_diversified': 'Trades 20+ different tokens',
            'stablecoin_heavy': 'Primarily uses stablecoins (70%+)',
            'defi_native': 'Heavy DeFi protocol usage',
            
            # Behavior tags
            'accumulator': 'Net buyer - more inflows than outflows',
            'distributor': 'Net seller - more outflows than inflows',
            'systematic_trader': 'Consistent transaction sizing',
            
            # Network tags
            'wide_network': 'Interacts with 100+ unique addresses',
            'concentrated_counterparty': 'Trades primarily with one address',
            
            # Risk tags
            'concentrated_bets': 'Large average position sizes',
            'systemic_risk_potential': 'Volume large enough for market impact',
            
            # Temporal tags
            'active_today': 'Transacted in last 24 hours',
            'business_hours_trader': 'Primarily trades 9 AM - 5 PM',
            'night_trader': 'Primarily trades 10 PM - 6 AM',
        }


# Export
__all__ = ['WalletTagger']
