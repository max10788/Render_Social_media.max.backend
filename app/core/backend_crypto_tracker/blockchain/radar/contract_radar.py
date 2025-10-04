# blockchain/radar/contract_radar.py
"""
Contract analysis and monitoring system.
"""
import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import json

from ..onchain.etherscan import (
    get_contract_abi,
    get_contract_creation,
    get_contract_market_cap,
    get_internal_transactions
)
from ..onchain.bitquery import get_token_holders
from ..aggregators.coingecko import get_token_market_data
from ..utils.error_handling import handle_api_error

@dataclass
class ContractAnalysis:
    """Data class for contract analysis results."""
    address: str
    network: str
    is_verified: bool
    creator_address: Optional[str] = None
    creation_tx: Optional[str] = None
    creation_block: Optional[int] = None
    abi_available: bool = False
    function_count: int = 0
    event_count: int = 0
    total_supply: Optional[float] = None
    market_cap: Optional[float] = None
    holder_count: Optional[int] = None
    top_holders: List[Dict] = field(default_factory=list)
    risk_score: float = 0.0
    risk_factors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

class ContractRadar:
    """
    Advanced contract analysis and monitoring system.
    """
    
    def __init__(
        self,
        etherscan_api_key: Optional[str] = None,
        bitquery_api_key: Optional[str] = None,
        coingecko_api_key: Optional[str] = None
    ):
        """
        Initialize ContractRadar with API keys.
        
        Args:
            etherscan_api_key: Etherscan API key
            bitquery_api_key: Bitquery API key
            coingecko_api_key: CoinGecko API key
        """
        self.etherscan_key = etherscan_api_key
        self.bitquery_key = bitquery_api_key
        self.coingecko_key = coingecko_api_key
        self.analysis_cache = {}
        
    def analyze_contract(
        self,
        contract_address: str,
        network: str = "mainnet",
        deep_analysis: bool = True
    ) -> ContractAnalysis:
        """
        Perform comprehensive contract analysis.
        
        Args:
            contract_address: Contract address to analyze
            network: Blockchain network
            deep_analysis: Perform deep analysis including holders
            
        Returns:
            ContractAnalysis object with comprehensive data
        """
        # Check cache
        cache_key = f"{network}:{contract_address}"
        if cache_key in self.analysis_cache:
            cached = self.analysis_cache[cache_key]
            # Cache valid for 1 hour
            if (datetime.now() - datetime.fromisoformat(cached.timestamp)).seconds < 3600:
                return cached
        
        analysis = ContractAnalysis(
            address=contract_address,
            network=network,
            is_verified=False
        )
        
        # Get contract ABI and verification status
        if self.etherscan_key:
            abi_data = get_contract_abi(contract_address, network, self.etherscan_key)
            analysis.is_verified = abi_data.get("verified", False)
            analysis.abi_available = bool(abi_data.get("abi"))
            analysis.function_count = abi_data.get("function_count", 0)
            analysis.event_count = abi_data.get("event_count", 0)
            
            # Get creation details
            creation_data = get_contract_creation(contract_address, network, self.etherscan_key)
            if creation_data.get("found"):
                analysis.creator_address = creation_data.get("creator_address")
                analysis.creation_tx = creation_data.get("creation_tx_hash")
                analysis.creation_block = creation_data.get("deployed_at_block")
            
            # Get market cap and supply
            market_data = get_contract_market_cap(contract_address, network, self.etherscan_key)
            analysis.total_supply = market_data.get("total_supply")
            analysis.market_cap = market_data.get("market_cap_usd")
            analysis.metadata["symbol"] = market_data.get("symbol")
            analysis.metadata["name"] = market_data.get("name")
            analysis.metadata["decimals"] = market_data.get("decimals")
        
        # Deep analysis with additional data sources
        if deep_analysis:
            # Get holder distribution
            if self.bitquery_key:
                holders = get_token_holders(
                    contract_address,
                    network,
                    limit=20,
                    api_key=self.bitquery_key
                )
                analysis.top_holders = holders[:10]
                analysis.holder_count = len(holders)
                
                # Calculate holder concentration
                if holders:
                    top_10_percentage = sum(h.get("percentage", 0) for h in holders[:10])
                    analysis.metadata["holder_concentration"] = top_10_percentage
            
            # Get market data from CoinGecko
            if self.coingecko_key and analysis.metadata.get("symbol"):
                try:
                    market_info = get_token_market_data(
                        analysis.metadata["symbol"].lower(),
                        api_key=self.coingecko_key
                    )
                    analysis.metadata["price_usd"] = market_info.price
                    analysis.metadata["volume_24h"] = market_info.volume_24h
                    analysis.metadata["price_change_24h"] = market_info.price_change_24h
                except:
                    pass  # Token might not be listed
        
        # Calculate risk score
        analysis.risk_score = self._calculate_risk_score(analysis)
        
        # Cache the analysis
        self.analysis_cache[cache_key] = analysis
        
        return analysis
    
    def _calculate_risk_score(self, analysis: ContractAnalysis) -> float:
        """
        Calculate risk score based on various factors.
        
        Args:
            analysis: ContractAnalysis object
            
        Returns:
            Risk score from 0 (low risk) to 100 (high risk)
        """
        risk_score = 0.0
        risk_factors = []
        
        # Check verification status
        if not analysis.is_verified:
            risk_score += 30
            risk_factors.append("Contract not verified")
        
        # Check holder concentration
        concentration = analysis.metadata.get("holder_concentration", 0)
        if concentration > 50:
            risk_score += 25
            risk_factors.append(f"High holder concentration: {concentration:.1f}%")
        elif concentration > 30:
            risk_score += 15
            risk_factors.append(f"Moderate holder concentration: {concentration:.1f}%")
        
        # Check liquidity
        volume_24h = analysis.metadata.get("volume_24h", 0)
        if volume_24h < 10000:
            risk_score += 20
            risk_factors.append("Low trading volume")
        elif volume_24h < 100000:
            risk_score += 10
            risk_factors.append("Moderate trading volume")
        
        # Check age of contract
        if analysis.creation_block:
            # Rough estimate: ~12 seconds per block
            blocks_since_creation = 18000000 - analysis.creation_block  # Approximate current block
            days_since_creation = (blocks_since_creation * 12) / 86400
            if days_since_creation < 7:
                risk_score += 15
                risk_factors.append("Very new contract")
            elif days_since_creation < 30:
                risk_score += 10
                risk_factors.append("Relatively new contract")
        
        # Check for common vulnerabilities in function names
        if analysis.function_count > 0:
            # This would need actual ABI analysis in production
            pass
        
        analysis.risk_factors = risk_factors
        return min(risk_score, 100)  # Cap at 100
    
    def monitor_contracts(
        self,
        contracts: List[str],
        network: str = "mainnet",
        interval: int = 300  # 5 minutes
    ):
        """
        Monitor multiple contracts for changes.
        
        Args:
            contracts: List of contract addresses
            network: Blockchain network
            interval: Check interval in seconds
        """
        while True:
            for contract in contracts:
                try:
                    analysis = self.analyze_contract(contract, network, deep_analysis=False)
                    # Here you would typically:
                    # 1. Compare with previous analysis
                    # 2. Send alerts if significant changes
                    # 3. Store in database
                    print(f"Monitored {contract}: Risk Score = {analysis.risk_score}")
                except Exception as e:
                    print(f"Error monitoring {contract}: {e}")
            
            asyncio.sleep(interval)
    
    def find_similar_contracts(
        self,
        reference_contract: str,
        network: str = "mainnet",
        similarity_threshold: float = 0.8
    ) -> List[Dict[str, Any]]:
        """
        Find contracts similar to a reference contract.
        
        Args:
            reference_contract: Reference contract address
            network: Blockchain network
            similarity_threshold: Similarity threshold (0-1)
            
        Returns:
            List of similar contracts with similarity scores
        """
        # Get reference contract analysis
        ref_analysis = self.analyze_contract(reference_contract, network)
        
        similar_contracts = []
        
        # In production, this would query a database of analyzed contracts
        # For now, return empty list as placeholder
        
        return similar_contracts
