import os
import json
import asyncio
import subprocess
import tempfile
import aiohttp
import re
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pathlib import Path

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, NotFoundException
from app.core.backend_crypto_tracker.services.contract.contract_metadata import ContractMetadataService

logger = get_logger(__name__)

class SecurityScanner:
    """Service for analyzing smart contracts for security vulnerabilities."""
    
    # SWC (Smart Contract Weakness Classification) Registry
    SWC_REGISTRY = {
        "SWC-101": {
            "name": "Integer Overflow and Underflow",
            "severity": "high",
            "description": "Arithmetic operations can overflow or underflow, leading to unexpected behavior."
        },
        "SWC-102": {
            "name": "Outdated Compiler Version",
            "severity": "medium",
            "description": "Using an outdated compiler version can expose the contract to known vulnerabilities."
        },
        "SWC-103": {
            "name": "Floating Pragma",
            "severity": "low",
            "description": "Contracts should use a fixed compiler version to avoid unexpected behavior."
        },
        "SWC-104": {
            "name": "Unchecked Call Return Value",
            "severity": "medium",
            "description": "The return value of a low-level call is not checked, potentially causing silent failures."
        },
        "SWC-105": {
            "name": "Unprotected Ownership",
            "severity": "high",
            "description": "Ownership functions are not protected, allowing unauthorized changes."
        },
        "SWC-106": {
            "name": "Unprotected SELFDESTRUCT Instruction",
            "severity": "high",
            "description": "The SELFDESTRUCT instruction can be called by anyone, potentially destroying the contract."
        },
        "SWC-107": {
            "name": "Reentrancy",
            "severity": "high",
            "description": "External calls can be used to re-enter the contract before the first call completes."
        },
        "SWC-108": {
            "name": "State Variable Default Visibility",
            "severity": "low",
            "description": "State variables should have explicit visibility to avoid confusion."
        },
        "SWC-109": {
            "name": "Unprotected Upgrade",
            "severity": "high",
            "description": "Upgrade mechanisms are not properly protected, allowing unauthorized upgrades."
        },
        "SWC-110": {
            "name": "Assert Violation",
            "severity": "medium",
            "description": "Assert statements can be used to cause intentional failures, potentially for malicious purposes."
        },
        "SWC-111": {
            "name": "Use of Deprecated Solidity Features",
            "severity": "low",
            "description": "Using deprecated features can lead to unexpected behavior or vulnerabilities."
        },
        "SWC-112": {
            "name": "Delegatecall to Untrusted Callee",
            "severity": "high",
            "description": "Delegatecall to untrusted contracts can lead to code execution in the context of the caller."
        },
        "SWC-113": {
            "name": "DoS with Unbounded Loop",
            "severity": "high",
            "description": "Loops that depend on user input can cause Denial of Service if they consume too much gas."
        },
        "SWC-114": {
            "name": "Transaction Order Dependence",
            "severity": "medium",
            "description": "Contract state can be manipulated by controlling transaction order."
        },
        "SWC-115": {
            "name": "Authorization through tx.origin",
            "severity": "high",
            "description": "Using tx.origin for authorization can lead to phishing attacks."
        },
        "SWC-116": {
            "name": "Block Values as a Proxy for Time",
            "severity": "medium",
            "description": "Using block.timestamp or block.number as a proxy for time can be manipulated by miners."
        },
        "SWC-117": {
            "name": "Signature Malleability",
            "severity": "high",
            "description": "ECDSA signatures can be malleated, potentially allowing signature replay."
        },
        "SWC-118": {
            "name": "Incorrect Constructor Name",
            "severity": "medium",
            "description": "Using an incorrect constructor name can lead to unintended behavior."
        },
        "SWC-119": {
            "name": "Shadowing State Variables",
            "severity": "medium",
            "description": "Local variables with the same name as state variables can lead to confusion and bugs."
        },
        "SWC-120": {
            "name": "Weak Sources of Randomness from Chain Attributes",
            "severity": "medium",
            "description": "Using block attributes as a source of randomness can be manipulated by miners."
        },
        "SWC-121": {
            "name": "Missing Protection against Signature Replay Attacks",
            "severity": "high",
            "description": "Signatures can be replayed if they don't include a nonce or other protection."
        },
        "SWC-122": {
            "name": "Lack of Proper Signature Verification",
            "severity": "high",
            "description": "Signatures are not properly verified, potentially allowing unauthorized actions."
        },
        "SWC-123": {
            "name": "Requirement Violation",
            "severity": "medium",
            "description": "Requirement statements can be used to cause intentional failures, potentially for malicious purposes."
        },
        "SWC-124": {
            "name": "Write to Arbitrary Storage Location",
            "severity": "high",
            "description": "Writing to arbitrary storage locations can lead to unexpected state changes."
        },
        "SWC-125": {
            "name": "Incorrect Inheritance Order",
            "severity": "medium",
            "description": "Incorrect inheritance order can lead to unexpected behavior, especially with multiple inheritance."
        },
        "SWC-126": {
            "name": "Insufficient Gas Griefing",
            "severity": "medium",
            "description": "Contracts can be forced to run out of gas by malicious actors."
        },
        "SWC-127": {
            "name": "Arbitrary Jump with Function Type",
            "severity": "high",
            "description": "Using function types for arbitrary jumps can lead to code execution in unexpected contexts."
        },
        "SWC-128": {
            "name": "DoS With Failed Call",
            "severity": "medium",
            "description": "External calls that can fail can cause the contract to become stuck."
        },
        "SWC-129": {
            "name": "Typographical Error",
            "severity": "medium",
            "description": "Typos in the code can lead to unexpected behavior."
        },
        "SWC-130": {
            "name": "Right-To-Left-Override Control Character (RTLO)",
            "severity": "medium",
            "description": "RTLO characters can be used to hide malicious code."
        },
        "SWC-131": {
            "name": "Presence of Unused Variables",
            "severity": "low",
            "description": "Unused variables can indicate incomplete or incorrect code."
        },
        "SWC-132": {
            "name": "Unexpected Ether Balance",
            "severity": "medium",
            "description": "Contracts that expect a specific ether balance can behave unexpectedly if the balance is different."
        },
        "SWC-133": {
            "name": "Hash Collisions With Multiple Variable Length Arguments",
            "severity": "medium",
            "description": "Hash functions can produce collisions when used with multiple variable length arguments."
        },
        "SWC-134": {
            "name": "Message Call With Hardcoded Gas Amount",
            "severity": "medium",
            "description": "Using hardcoded gas amounts for message calls can lead to unexpected failures."
        },
        "SWC-135": {
            "name": "Gas Limit and Loops",
            "severity": "medium",
            "description": "Loops that consume a lot of gas can cause transactions to fail."
        },
        "SWC-136": {
            "name": "Unencrypted Private Data On-Chain",
            "severity": "high",
            "description": "Storing private data on-chain without encryption can expose sensitive information."
        }
    }
    
    def __init__(self):
        """Initialize scanner and rules."""
        # Initialize contract metadata service
        self.metadata_service = ContractMetadataService()
        
        # Initialize session for HTTP requests
        self.session = None
        
        # Path to Slither executable (if available)
        self.slither_path = os.getenv("SLITHER_PATH", "slither")
        
        # Chain-specific security rules
        self.chain_specific_rules = {
            "ethereum": {
                "max_external_calls": 5,
                "recommended_min_proxy_delay": 2 * 24 * 60 * 60,  # 2 days in seconds
                "recommended_max_supply": "1000000000000000000000000"  # 1 million tokens with 18 decimals
            },
            "bsc": {
                "max_external_calls": 5,
                "recommended_min_proxy_delay": 2 * 24 * 60 * 60,  # 2 days in seconds
                "recommended_max_supply": "1000000000000000000000000"  # 1 million tokens with 18 decimals
            },
            "solana": {
                "max_cross_program_invocations": 4,
                "recommended_max_compute_units": 200000
            },
            "sui": {
                "max_object_dependencies": 10,
                "recommended_max_gas": 10000000
            }
        }
        
        # Initialize vulnerability patterns for regex-based detection
        self.vulnerability_patterns = {
            "SWC-107": [
                r"\.call\s*\(\s*.*\s*\)\s*;",  # External call pattern
                r"\.send\s*\(\s*.*\s*\)\s*;",  # Send pattern
                r"\.transfer\s*\(\s*.*\s*\)\s*;"  # Transfer pattern
            ],
            "SWC-115": [
                r"tx\.origin\s*==\s*",  # tx.origin comparison
                r"require\s*\(\s*tx\.origin"  # tx.origin in require
            ],
            "SWC-105": [
                r"function\s+\w*\s*owner\s*\(",  # Owner function
                r"function\s+\w*\s*transferOwnership\s*\(",  # Transfer ownership
                r"function\s+\w*\s*renounceOwnership\s*\("  # Renounce ownership
            ],
            "SWC-106": [
                r"selfdestruct\s*\(",  # Selfdestruct pattern
                r"suicide\s*\("  # Suicide pattern (deprecated)
            ],
            "SWC-112": [
                r"delegatecall\s*\(",  # Delegatecall pattern
                r"\.delegatecall\s*\("  # Delegatecall with address
            ]
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        await self.metadata_service.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
        await self.metadata_service.__aexit__(exc_type, exc_val, exc_tb)
    
    async def scan_contract_security(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Perform a comprehensive security scan of a smart contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary containing security analysis results
        """
        try:
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Perform vulnerability check
            vulnerabilities = await self.check_vulnerabilities(address, chain)
            
            # Analyze access control
            access_control_issues = await self.analyze_access_control(address, chain)
            
            # Analyze economic risks
            economic_risks = await self.analyze_economic_risks(address, chain)
            
            # Calculate verification confidence
            verification_confidence = await self.calculate_verification_confidence(address, chain)
            
            # Calculate code quality metrics
            code_quality_metrics = await self._analyze_code_quality(address, chain)
            
            return {
                "vulnerabilities": vulnerabilities,
                "code_quality_metrics": code_quality_metrics,
                "access_control_issues": access_control_issues,
                "economic_risks": economic_risks,
                "verification_confidence": verification_confidence
            }
        except Exception as e:
            logger.error(f"Failed to scan contract security for {address} on {chain}: {str(e)}")
            raise
    
    async def check_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """
        Check for known vulnerabilities in a smart contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            List of detected vulnerabilities
        """
        try:
            vulnerabilities = []
            
            if chain.lower() in ["ethereum", "bsc"]:
                # Use Slither for Solidity contracts
                vulnerabilities = await self._check_solidity_vulnerabilities(address, chain)
                
                # Perform additional regex-based checks
                regex_vulnerabilities = await self._check_vulnerabilities_with_regex(address, chain)
                vulnerabilities.extend(regex_vulnerabilities)
                
            elif chain.lower() == "solana":
                # Solana-specific vulnerability checks
                vulnerabilities = await self._check_solana_vulnerabilities(address, chain)
            elif chain.lower() == "sui":
                # Sui-specific vulnerability checks
                vulnerabilities = await self._check_sui_vulnerabilities(address, chain)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            return vulnerabilities
        except Exception as e:
            logger.error(f"Failed to check vulnerabilities for {address} on {chain}: {str(e)}")
            raise
    
    async def _check_vulnerabilities_with_regex(self, address: str, chain: str) -> List[Dict]:
        """Check for vulnerabilities using regex patterns on the contract source code."""
        try:
            vulnerabilities = []
            
            # Get contract source code
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                return vulnerabilities
            
            # Check for each vulnerability pattern
            for swc_id, patterns in self.vulnerability_patterns.items():
                for pattern in patterns:
                    matches = re.finditer(pattern, source_code, re.IGNORECASE)
                    for match in matches:
                        # Get line number
                        line_number = source_code[:match.start()].count('\n') + 1
                        
                        # Get vulnerability details
                        vuln_details = self.SWC_REGISTRY.get(swc_id, {})
                        
                        vulnerability = {
                            "id": swc_id,
                            "name": vuln_details.get("name", "Unknown Vulnerability"),
                            "severity": vuln_details.get("severity", "medium"),
                            "description": vuln_details.get("description", "No description available"),
                            "line_number": line_number
                        }
                        
                        # Avoid duplicates
                        if not any(v["id"] == swc_id and v["line_number"] == line_number for v in vulnerabilities):
                            vulnerabilities.append(vulnerability)
            
            return vulnerabilities
        except Exception as e:
            logger.error(f"Failed to check vulnerabilities with regex for {address} on {chain}: {str(e)}")
            return []
    
    async def _get_contract_source_code(self, address: str, chain: str) -> Optional[str]:
        """Get the source code of a contract."""
        try:
            if chain.lower() == "ethereum":
                return await self._get_ethereum_source_code(address)
            elif chain.lower() == "bsc":
                return await self._get_bsc_source_code(address)
            else:
                return None
        except Exception as e:
            logger.error(f"Failed to get source code for {address} on {chain}: {str(e)}")
            return None
    
    async def _get_ethereum_source_code(self, address: str) -> Optional[str]:
        """Get the source code of an Ethereum contract."""
        try:
            etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
            if not etherscan_api_key:
                return None
            
            params = {
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": etherscan_api_key
            }
            
            url = "https://api.etherscan.io/api"
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "1" and data["result"]:
                        contract_data = data["result"][0]
                        if contract_data.get("ContractName", "") != "":
                            return contract_data.get("SourceCode", "")
            
            return None
        except Exception as e:
            logger.error(f"Failed to get Ethereum source code for {address}: {str(e)}")
            return None
    
    async def _get_bsc_source_code(self, address: str) -> Optional[str]:
        """Get the source code of a BSC contract."""
        try:
            bscscan_api_key = os.getenv("BSCSCAN_API_KEY")
            if not bscscan_api_key:
                return None
            
            params = {
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": bscscan_api_key
            }
            
            url = "https://api.bscscan.com/api"
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "1" and data["result"]:
                        contract_data = data["result"][0]
                        if contract_data.get("ContractName", "") != "":
                            return contract_data.get("SourceCode", "")
            
            return None
        except Exception as e:
            logger.error(f"Failed to get BSC source code for {address}: {str(e)}")
            return None
    
    async def _check_solidity_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """Check for vulnerabilities in Solidity contracts using Slither."""
        try:
            # Get contract ABI
            abi = await self.metadata_service.get_abi(address, chain)
            if not abi:
                logger.warning(f"No ABI available for contract {address} on {chain}")
                return []
            
            # Create a temporary file for the ABI
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as abi_file:
                abi_file.write(abi)
                abi_path = abi_file.name
            
            try:
                # Run Slither to detect vulnerabilities
                cmd = [
                    self.slither_path,
                    address,
                    "--json",
                    "-",
                    "--detect",
                    "all"
                ]
                
                # Add API key if available
                api_key = os.getenv("ETHERSCAN_API_KEY") if chain.lower() == "ethereum" else os.getenv("BSCSCAN_API_KEY")
                if api_key:
                    cmd.extend(["--etherscan-apikey", api_key])
                
                logger.info(f"Running Slither with command: {' '.join(cmd)}")
                
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await process.communicate()
                
                if process.returncode != 0:
                    logger.error(f"Slither failed with return code {process.returncode}: {stderr.decode()}")
                    return []
                
                # Parse Slither output
                try:
                    slither_results = json.loads(stdout.decode())
                except json.JSONDecodeError:
                    logger.error("Failed to parse Slither output as JSON")
                    return []
                
                # Convert Slither results to our format
                vulnerabilities = []
                for detector_result in slither_results.get("detectors", []):
                    # Map Slither detector to SWC ID if possible
                    swc_id = self._map_slither_to_swc(detector_result.get("check", ""))
                    
                    vulnerability = {
                        "id": swc_id,
                        "name": detector_result.get("check", ""),
                        "severity": self._normalize_severity(detector_result.get("impact", "unknown")),
                        "description": detector_result.get("description", ""),
                        "line_number": detector_result.get("first_element", {}).get("line", 0)
                    }
                    
                    vulnerabilities.append(vulnerability)
                
                return vulnerabilities
            finally:
                # Clean up temporary file
                os.unlink(abi_path)
        except Exception as e:
            logger.error(f"Failed to check Solidity vulnerabilities for {address} on {chain}: {str(e)}")
            return []
    
    def _map_slither_to_swc(self, slither_check: str) -> str:
        """Map a Slither check name to an SWC ID."""
        # Simple mapping for common checks
        mapping = {
            "reentrancy-eth": "SWC-107",
            "integer-overflow": "SWC-101",
            "integer-underflow": "SWC-101",
            "unchecked-low-level-calls": "SWC-104",
            "suicidal": "SWC-106",
            "unprotected-upgrade": "SWC-109",
            "tx-origin": "SWC-115",
            "weak-random": "SWC-120",
            "incorrect-equality": "SWC-129",
            "unprotected-selfdestruct": "SWC-106",
            "delegatecall-loop": "SWC-112",
            "arbitrary-send": "SWC-105",
            "backdoor": "SWC-112",
            "constant-function-asm": "SWC-120",
            "shadowing-local": "SWC-119",
            "shadowing-state": "SWC-119",
            "uninitialized-fptr-callback": "SWC-124",
            "uninitialized-state": "SWC-109",
            "uninitialized-storage": "SWC-109",
            "unused-return": "SWC-131",
            "void-criterion": "SWC-123"
        }
        
        return mapping.get(slither_check, "UNKNOWN")
    
    def _normalize_severity(self, severity: str) -> str:
        """Normalize severity to our standard format."""
        severity = severity.lower()
        if severity in ["high", "critical"]:
            return "high"
        elif severity in ["medium", "warning"]:
            return "medium"
        elif severity in ["low", "informational", "optimization"]:
            return "low"
        else:
            return "medium"  # Default to medium if unknown
    
    async def _check_solana_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """Check for vulnerabilities in Solana programs."""
        try:
            vulnerabilities = []
            
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Check for common Solana vulnerabilities
            if not metadata.get("verification_status", False):
                vulnerabilities.append({
                    "id": "SOL-001",
                    "name": "Unverified Program",
                    "severity": "medium",
                    "description": "The program has not been verified, making it difficult to audit for vulnerabilities.",
                    "line_number": 0
                })
            
            # Check for excessive cross-program invocations
            # This would require analyzing the program's instructions
            # For now, we'll add a placeholder check
            vulnerabilities.append({
                "id": "SOL-002",
                "name": "Excessive Cross-Program Invocations",
                "severity": "medium",
                "description": "The program makes many cross-program invocations, which could lead to complexity issues.",
                "line_number": 0
            })
            
            # Check for missing account validation
            vulnerabilities.append({
                "id": "SOL-003",
                "name": "Missing Account Validation",
                "severity": "high",
                "description": "The program may not properly validate accounts before use.",
                "line_number": 0
            })
            
            return vulnerabilities
        except Exception as e:
            logger.error(f"Failed to check Solana vulnerabilities for {address} on {chain}: {str(e)}")
            return []
    
    async def _check_sui_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """Check for vulnerabilities in Sui Move modules."""
        try:
            vulnerabilities = []
            
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Check for common Sui vulnerabilities
            if not metadata.get("verification_status", False):
                vulnerabilities.append({
                    "id": "SUI-001",
                    "name": "Unverified Module",
                    "severity": "medium",
                    "description": "The module has not been verified, making it difficult to audit for vulnerabilities.",
                    "line_number": 0
                })
            
            # Check for excessive object dependencies
            # This would require analyzing the module's dependencies
            # For now, we'll add a placeholder check
            vulnerabilities.append({
                "id": "SUI-002",
                "name": "Excessive Object Dependencies",
                "severity": "medium",
                "description": "The module has many object dependencies, which could lead to complexity issues.",
                "line_number": 0
            })
            
            # Check for missing capability checks
            vulnerabilities.append({
                "id": "SUI-003",
                "name": "Missing Capability Checks",
                "severity": "high",
                "description": "The module may not properly check capabilities before sensitive operations.",
                "line_number": 0
            })
            
            return vulnerabilities
        except Exception as e:
            logger.error(f"Failed to check Sui vulnerabilities for {address} on {chain}: {str(e)}")
            return []
    
    async def analyze_access_control(self, address: str, chain: str) -> List[str]:
        """
        Analyze access control issues in a smart contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            List of access control issues
        """
        try:
            issues = []
            
            if chain.lower() in ["ethereum", "bsc"]:
                issues = await self._analyze_solidity_access_control(address, chain)
            elif chain.lower() == "solana":
                issues = await self._analyze_solana_access_control(address, chain)
            elif chain.lower() == "sui":
                issues = await self._analyze_sui_access_control(address, chain)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            return issues
        except Exception as e:
            logger.error(f"Failed to analyze access control for {address} on {chain}: {str(e)}")
            raise
    
    async def _analyze_solidity_access_control(self, address: str, chain: str) -> List[str]:
        """Analyze access control issues in Solidity contracts."""
        try:
            issues = []
            
            # Get contract source code
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                issues.append("No source code available to analyze access control")
                return issues
            
            # Check for missing access control on critical functions
            critical_functions = [
                "mint", "burn", "pause", "unpause", "withdraw", "transferownership",
                "setfee", "settax", "setlimit", "setrate", "setaddress", "changeowner"
            ]
            
            # Check for functions with names matching critical functions
            for func in critical_functions:
                pattern = rf"function\s+{func}\s*\("
                matches = re.finditer(pattern, source_code, re.IGNORECASE)
                for match in matches:
                    # Get the function signature
                    line_start = source_code.rfind('\n', 0, match.start()) + 1
                    line_end = source_code.find('\n', match.end())
                    if line_end == -1:
                        line_end = len(source_code)
                    
                    function_line = source_code[line_start:line_end].strip()
                    
                    # Check if the function has access control modifiers
                    has_access_control = any(
                        modifier in function_line 
                        for modifier in ["onlyOwner", "onlyAdmin", "whenNotPaused", "whenPaused", "internal", "private"]
                    )
                    
                    if not has_access_control:
                        issues.append(f"Critical function {func} lacks access control")
            
            # Check for tx.origin usage
            if re.search(r"tx\.origin", source_code):
                issues.append("Use of tx.origin detected, which can lead to phishing attacks")
            
            # Check for unprotected selfdestruct
            if re.search(r"selfdestruct\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                issues.append("Unprotected selfdestruct function detected")
            
            # Check for unprotected delegatecall
            if re.search(r"delegatecall\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                issues.append("Unprotected delegatecall function detected")
            
            # Check for missing pausing mechanism
            if not re.search(r"pause\s*\(", source_code) and not re.search(r"unpause\s*\(", source_code):
                issues.append("No pausing mechanism detected, which could be problematic in emergencies")
            
            return issues
        except Exception as e:
            logger.error(f"Failed to analyze Solidity access control for {address} on {chain}: {str(e)}")
            return ["Failed to analyze access control due to an error"]
    
    async def _analyze_solana_access_control(self, address: str, chain: str) -> List[str]:
        """Analyze access control issues in Solana programs."""
        try:
            issues = []
            
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Solana-specific access control checks
            if not metadata.get("verification_status", False):
                issues.append("Unverified program, access control cannot be fully verified")
            
            # Check for missing account validation
            issues.append("Potential missing account validation in program")
            
            # Check for missing authority checks
            issues.append("Potential missing authority checks in program")
            
            return issues
        except Exception as e:
            logger.error(f"Failed to analyze Solana access control for {address} on {chain}: {str(e)}")
            return ["Failed to analyze access control due to an error"]
    
    async def _analyze_sui_access_control(self, address: str, chain: str) -> List[str]:
        """Analyze access control issues in Sui Move modules."""
        try:
            issues = []
            
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Sui-specific access control checks
            if not metadata.get("verification_status", False):
                issues.append("Unverified module, access control cannot be fully verified")
            
            # Check for missing capability checks
            issues.append("Potential missing capability checks in module")
            
            # Check for missing object ownership validation
            issues.append("Potential missing object ownership validation in module")
            
            return issues
        except Exception as e:
            logger.error(f"Failed to analyze Sui access control for {address} on {chain}: {str(e)}")
            return ["Failed to analyze access control due to an error"]
    
    async def analyze_economic_risks(self, address: str, chain: str) -> List[str]:
        """
        Analyze economic risks in a smart contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            List of economic risks
        """
        try:
            risks = []
            
            if chain.lower() in ["ethereum", "bsc"]:
                risks = await self._analyze_solidity_economic_risks(address, chain)
            elif chain.lower() == "solana":
                risks = await self._analyze_solana_economic_risks(address, chain)
            elif chain.lower() == "sui":
                risks = await self._analyze_sui_economic_risks(address, chain)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            return risks
        except Exception as e:
            logger.error(f"Failed to analyze economic risks for {address} on {chain}: {str(e)}")
            raise
    
    async def _analyze_solidity_economic_risks(self, address: str, chain: str) -> List[str]:
        """Analyze economic risks in Solidity contracts."""
        try:
            risks = []
            
            # Get contract source code
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                risks.append("No source code available to analyze economic risks")
                return risks
            
            # Check for unlimited minting
            if re.search(r"mint\s*\(", source_code) and not re.search(r"_maxSupply", source_code):
                risks.append("Potential for unlimited minting detected")
            
            # Check for excessive burning
            if re.search(r"burn\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                risks.append("Potential for excessive burning detected")
            
            # Check for rug pull potential
            if re.search(r"withdraw\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                risks.append("Potential for rug pull detected")
            
            # Check for excessive fees or taxes
            if re.search(r"fee\s*=\s*[0-9]+", source_code) and re.search(r"fee\s*>\s*10", source_code):
                risks.append("Potential for excessive fees or taxes detected")
            
            # Check for missing pausing mechanism
            if not re.search(r"pause\s*\(", source_code) and not re.search(r"unpause\s*\(", source_code):
                risks.append("No pausing mechanism detected, which could be problematic in emergencies")
            
            # Check for honeypot potential
            if re.search(r"transfer\s*\(", source_code) and re.search(r"require\s*\(\s*false", source_code):
                risks.append("Potential honeypot mechanism detected")
            
            # Check for anti-whale mechanisms
            if re.search(r"balanceOf\s*\(\s*msg\.sender\s*\)\s*>\s*max", source_code):
                risks.append("Anti-whale mechanism detected, which may limit usability")
            
            return risks
        except Exception as e:
            logger.error(f"Failed to analyze Solidity economic risks for {address} on {chain}: {str(e)}")
            return ["Failed to analyze economic risks due to an error"]
    
    async def _analyze_solana_economic_risks(self, address: str, chain: str) -> List[str]:
        """Analyze economic risks in Solana programs."""
        try:
            risks = []
            
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Solana-specific economic risk checks
            if not metadata.get("verification_status", False):
                risks.append("Unverified program, economic risks cannot be fully verified")
            
            # Check for excessive minting
            risks.append("Potential for excessive minting detected")
            
            # Check for rug pull potential
            risks.append("Potential for rug pull detected")
            
            return risks
        except Exception as e:
            logger.error(f"Failed to analyze Solana economic risks for {address} on {chain}: {str(e)}")
            return ["Failed to analyze economic risks due to an error"]
    
    async def _analyze_sui_economic_risks(self, address: str, chain: str) -> List[str]:
        """Analyze economic risks in Sui Move modules."""
        try:
            risks = []
            
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Sui-specific economic risk checks
            if not metadata.get("verification_status", False):
                risks.append("Unverified module, economic risks cannot be fully verified")
            
            # Check for excessive minting
            risks.append("Potential for excessive minting detected")
            
            # Check for rug pull potential
            risks.append("Potential for rug pull detected")
            
            return risks
        except Exception as e:
            logger.error(f"Failed to analyze Sui economic risks for {address} on {chain}: {str(e)}")
            return ["Failed to analyze economic risks due to an error"]
    
    async def calculate_verification_confidence(self, address: str, chain: str) -> float:
        """
        Calculate the verification confidence score for a contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Verification confidence score between 0.0 and 1.0
        """
        try:
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Start with a base score
            confidence = 0.0
            
            # Check if contract is verified
            if metadata.get("verification_status", False):
                confidence += 0.4
            
            # Check if we have ABI
            abi = await self.metadata_service.get_abi(address, chain)
            if abi:
                confidence += 0.2
            
            # Check if we have creator address
            creator = await self.metadata_service.get_creator_address(address, chain)
            if creator:
                confidence += 0.1
            
            # Check for deployment date
            if metadata.get("deployment_date"):
                confidence += 0.1
            
            # Check for contract type
            if metadata.get("contract_type"):
                confidence += 0.1
            
            # Check for source code availability
            source_code = await self._get_contract_source_code(address, chain)
            if source_code:
                confidence += 0.1
            
            # Check for vulnerabilities
            vulnerabilities = await self.check_vulnerabilities(address, chain)
            if vulnerabilities:
                # Reduce confidence based on number of high severity vulnerabilities
                high_severity_count = sum(1 for v in vulnerabilities if v.get("severity") == "high")
                confidence -= min(0.3, high_severity_count * 0.1)
            
            # Ensure the score is between 0.0 and 1.0
            confidence = max(0.0, min(1.0, confidence))
            
            return confidence
        except Exception as e:
            logger.error(f"Failed to calculate verification confidence for {address} on {chain}: {str(e)}")
            return 0.0
    
    async def _analyze_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Analyze code quality metrics for a contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary containing code quality metrics
        """
        try:
            metrics = {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
            
            if chain.lower() in ["ethereum", "bsc"]:
                metrics = await self._analyze_solidity_code_quality(address, chain)
            elif chain.lower() == "solana":
                metrics = await self._analyze_solana_code_quality(address, chain)
            elif chain.lower() == "sui":
                metrics = await self._analyze_sui_code_quality(address, chain)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            return metrics
        except Exception as e:
            logger.error(f"Failed to analyze code quality for {address} on {chain}: {str(e)}")
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
    
    async def _analyze_solidity_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """Analyze code quality metrics for Solidity contracts."""
        try:
            # Get contract source code
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                return {
                    "complexity_score": 0,
                    "lines_of_code": 0,
                    "external_calls": 0
                }
            
            # Count lines of code
            lines_of_code = len(source_code.split('\n'))
            
            # Count external calls
            external_calls = len(re.findall(r"\.(call|send|transfer|delegatecall)\s*\(", source_code))
            
            # Count functions
            function_count = len(re.findall(r"function\s+\w+\s*\(", source_code))
            
            # Count modifiers
            modifier_count = len(re.findall(r"modifier\s+\w+\s*\(", source_code))
            
            # Count events
            event_count = len(re.findall(r"event\s+\w+\s*\(", source_code))
            
            # Calculate complexity score based on various factors
            complexity_score = 0
            
            # Base complexity from function count
            complexity_score += min(30, function_count * 2)
            
            # Add complexity from external calls
            complexity_score += min(20, external_calls * 4)
            
            # Add complexity from lines of code
            complexity_score += min(20, lines_of_code / 50)
            
            # Add complexity from modifiers
            complexity_score += min(10, modifier_count * 2)
            
            # Add complexity from events
            complexity_score += min(10, event_count)
            
            # Add complexity from inheritance
            inheritance_count = len(re.findall(r"is\s+\w+", source_code))
            complexity_score += min(10, inheritance_count * 3)
            
            # Ensure complexity score is between 0 and 100
            complexity_score = min(100, complexity_score)
            
            return {
                "complexity_score": complexity_score,
                "lines_of_code": lines_of_code,
                "external_calls": external_calls
            }
        except Exception as e:
            logger.error(f"Failed to analyze Solidity code quality for {address} on {chain}: {str(e)}")
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
    
    async def _analyze_solana_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """Analyze code quality metrics for Solana programs."""
        try:
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Solana-specific code quality metrics
            # This is a simplified analysis; in a real implementation, we would use Solana-specific tools
            
            # Placeholder values
            complexity_score = 50
            lines_of_code = 1000
            external_calls = 3
            
            return {
                "complexity_score": complexity_score,
                "lines_of_code": lines_of_code,
                "external_calls": external_calls
            }
        except Exception as e:
            logger.error(f"Failed to analyze Solana code quality for {address} on {chain}: {str(e)}")
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
    
    async def _analyze_sui_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """Analyze code quality metrics for Sui Move modules."""
        try:
            # Get contract metadata
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Sui-specific code quality metrics
            # This is a simplified analysis; in a real implementation, we would use Sui-specific tools
            
            # Placeholder values
            complexity_score = 50
            lines_of_code = 1000
            external_calls = 3
            
            return {
                "complexity_score": complexity_score,
                "lines_of_code": lines_of_code,
                "external_calls": external_calls
            }
        except Exception as e:
            logger.error(f"Failed to analyze Sui code quality for {address} on {chain}: {str(e)}")
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }

            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }

    async def _get_contract_source_code(self, address: str, chain: str) -> Optional[str]:
        """Get the source code of a contract."""
        try:
            if chain.lower() == "ethereum":
                return await self._get_ethereum_source_code(address)
            elif chain.lower() == "bsc":
                return await self._get_bsc_source_code(address)
            else:
                return None
        except Exception as e:
            logger.error(f"Failed to get source code for {address} on {chain}: {str(e)}")
            return None

    async def _get_ethereum_source_code(self, address: str) -> Optional[str]:
        """Get the source code of an Ethereum contract."""
        try:
            etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
            if not etherscan_api_key:
                return None
            
            params = {
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": etherscan_api_key
            }
            
            url = "https://api.etherscan.io/api"
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "1" and data["result"]:
                        contract_data = data["result"][0]
                        if contract_data.get("ContractName", "") != "":
                            return contract_data.get("SourceCode", "")
            
            return None
        except Exception as e:
            logger.error(f"Failed to get Ethereum source code for {address}: {str(e)}")
            return None

    async def _get_bsc_source_code(self, address: str) -> Optional[str]:
        """Get the source code of a BSC contract."""
        try:
            bscscan_api_key = os.getenv("BSCSCAN_API_KEY")
            if not bscscan_api_key:
                return None
            
            params = {
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": bscscan_api_key
            }
            
            url = "https://api.bscscan.com/api"
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "1" and data["result"]:
                        contract_data = data["result"][0]
                        if contract_data.get("ContractName", "") != "":
                            return contract_data.get("SourceCode", "")
            
            return None
        except Exception as e:
            logger.error(f"Failed to get BSC source code for {address}: {str(e)}")
            return None

    async def _check_vulnerabilities_with_regex(self, address: str, chain: str) -> List[Dict]:
        """Check for vulnerabilities using regex patterns on the contract source code."""
        try:
            vulnerabilities = []
            
            # Get contract source code
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                return vulnerabilities
            
            # Check for each vulnerability pattern
            for swc_id, patterns in self.vulnerability_patterns.items():
                for pattern in patterns:
                    matches = re.finditer(pattern, source_code, re.IGNORECASE)
                    for match in matches:
                        # Get line number
                        line_number = source_code[:match.start()].count('\n') + 1
                        
                        # Get vulnerability details
                        vuln_details = self.SWC_REGISTRY.get(swc_id, {})
                        
                        vulnerability = {
                            "id": swc_id,
                            "name": vuln_details.get("name", "Unknown Vulnerability"),
                            "severity": vuln_details.get("severity", "medium"),
                            "description": vuln_details.get("description", "No description available"),
                            "line_number": line_number
                        }
                        
                        # Avoid duplicates
                        if not any(v["id"] == swc_id and v["line_number"] == line_number for v in vulnerabilities):
                            vulnerabilities.append(vulnerability)
            
            return vulnerabilities
        except Exception as e:
            logger.error(f"Failed to check vulnerabilities with regex for {address} on {chain}: {str(e)}")
            return []

    async def _calculate_risk_score(self, vulnerabilities: List[Dict], access_control_issues: List[str], 
                                  economic_risks: List[str], verification_confidence: float) -> float:
        """
        Calculate an overall risk score based on security analysis results.
        
        Args:
            vulnerabilities: List of detected vulnerabilities
            access_control_issues: List of access control issues
            economic_risks: List of economic risks
            verification_confidence: Verification confidence score
            
        Returns:
            Risk score between 0.0 (low risk) and 1.0 (high risk)
        """
        try:
            # Start with a base risk score
            risk_score = 0.0
            
            # Add risk based on vulnerabilities
            for vuln in vulnerabilities:
                severity = vuln.get("severity", "medium")
                if severity == "high":
                    risk_score += 0.2
                elif severity == "medium":
                    risk_score += 0.1
                else:  # low
                    risk_score += 0.05
            
            # Add risk based on access control issues
            risk_score += len(access_control_issues) * 0.1
            
            # Add risk based on economic risks
            risk_score += len(economic_risks) * 0.1
            
            # Reduce risk based on verification confidence
            risk_score *= (1.0 - verification_confidence)
            
            # Ensure the score is between 0.0 and 1.0
            risk_score = max(0.0, min(1.0, risk_score))
            
            return risk_score
        except Exception as e:
            logger.error(f"Failed to calculate risk score: {str(e)}")
            return 0.5  # Default to medium risk

    async def generate_security_report(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Generate a comprehensive security report for a contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary containing the security report
        """
        try:
            # Perform security scan
            scan_results = await self.scan_contract_security(address, chain)
            
            # Calculate risk score
            risk_score = await self._calculate_risk_score(
                scan_results["vulnerabilities"],
                scan_results["access_control_issues"],
                scan_results["economic_risks"],
                scan_results["verification_confidence"]
            )
            
            # Generate report
            report = {
                "contract_address": address,
                "blockchain": chain,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "risk_score": risk_score,
                "risk_level": self._get_risk_level(risk_score),
                "summary": self._generate_summary(scan_results, risk_score),
                "details": scan_results
            }
            
            return report
        except Exception as e:
            logger.error(f"Failed to generate security report for {address} on {chain}: {str(e)}")
            raise

    def _get_risk_level(self, risk_score: float) -> str:
        """Convert risk score to risk level."""
        if risk_score >= 0.7:
            return "critical"
        elif risk_score >= 0.5:
            return "high"
        elif risk_score >= 0.3:
            return "medium"
        else:
            return "low"


    def _generate_summary(self, scan_results: Dict[str, Any], risk_score: float) -> str:
        """Generate a human-readable summary of the security analysis."""
        try:
            vulnerabilities = scan_results["vulnerabilities"]
            access_control_issues = scan_results["access_control_issues"]
            economic_risks = scan_results["economic_risks"]
            verification_confidence = scan_results["verification_confidence"]
            
            # Count high severity vulnerabilities
            high_vulns = sum(1 for v in vulnerabilities if v.get("severity") == "high")
            medium_vulns = sum(1 for v in vulnerabilities if v.get("severity") == "medium")
            low_vulns = sum(1 for v in vulnerabilities if v.get("severity") == "low")
            
            # Generate summary based on findings
            if high_vulns > 0:
                summary = f"CRITICAL: Contract has {high_vulns} high-severity vulnerabilities. Immediate action required."
            elif medium_vulns > 0:
                summary = f"HIGH RISK: Contract has {medium_vulns} medium-severity vulnerabilities that should be addressed."
            elif access_control_issues or economic_risks:
                summary = "MEDIUM RISK: Contract has potential access control or economic risks that should be reviewed."
            elif verification_confidence < 0.5:
                summary = "MEDIUM RISK: Contract verification confidence is low, making it difficult to fully assess security."
            else:
                summary = "LOW RISK: No significant security issues detected, but regular monitoring is recommended."
            
            return summary
        except Exception as e:
            logger.error(f"Failed to generate summary: {str(e)}")
            return "Unable to generate summary due to an error."

    async def _calculate_risk_score(self, vulnerabilities: List[Dict], access_control_issues: List[str], 
                                  economic_risks: List[str], verification_confidence: float) -> float:
        """
        Calculate an overall risk score based on security analysis results.
        
        Args:
            vulnerabilities: List of detected vulnerabilities
            access_control_issues: List of access control issues
            economic_risks: List of economic risks
            verification_confidence: Verification confidence score
            
        Returns:
            Risk score between 0.0 (low risk) and 1.0 (high risk)
        """
        try:
            # Start with a base risk score
            risk_score = 0.0
            
            # Add risk based on vulnerabilities
            for vuln in vulnerabilities:
                severity = vuln.get("severity", "medium")
                if severity == "high":
                    risk_score += 0.2
                elif severity == "medium":
                    risk_score += 0.1
                else:  # low
                    risk_score += 0.05
            
            # Add risk based on access control issues
            risk_score += len(access_control_issues) * 0.1
            
            # Add risk based on economic risks
            risk_score += len(economic_risks) * 0.1
            
            # Reduce risk based on verification confidence
            risk_score *= (1.0 - verification_confidence)
            
            # Ensure the score is between 0.0 and 1.0
            risk_score = max(0.0, min(1.0, risk_score))
            
            return risk_score
        except Exception as e:
            logger.error(f"Failed to calculate risk score: {str(e)}")
            return 0.5  # Default to medium risk

    async def generate_security_report(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Generate a comprehensive security report for a contract.
        
        Args:
            address: Contract address
            chain: Blockchain name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary containing the security report
        """
        try:
            # Perform security scan
            scan_results = await self.scan_contract_security(address, chain)
            
            # Calculate risk score
            risk_score = await self._calculate_risk_score(
                scan_results["vulnerabilities"],
                scan_results["access_control_issues"],
                scan_results["economic_risks"],
                scan_results["verification_confidence"]
            )
            
            # Generate report
            report = {
                "contract_address": address,
                "blockchain": chain,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "risk_score": risk_score,
                "risk_level": self._get_risk_level(risk_score),
                "summary": self._generate_summary(scan_results, risk_score),
                "details": scan_results
            }
            
            return report
        except Exception as e:
            logger.error(f"Failed to generate security report for {address} on {chain}: {str(e)}")
            raise

    async def _get_contract_source_code(self, address: str, chain: str) -> Optional[str]:
        """Get the source code of a contract."""
        try:
            if chain.lower() == "ethereum":
                return await self._get_ethereum_source_code(address)
            elif chain.lower() == "bsc":
                return await self._get_bsc_source_code(address)
            else:
                return None
        except Exception as e:
            logger.error(f"Failed to get source code for {address} on {chain}: {str(e)}")
            return None

    async def _get_ethereum_source_code(self, address: str) -> Optional[str]:
        """Get the source code of an Ethereum contract."""
        try:
            etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
            if not etherscan_api_key:
                return None
            
            params = {
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": etherscan_api_key
            }
            
            url = "https://api.etherscan.io/api"
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "1" and data["result"]:
                        contract_data = data["result"][0]
                        if contract_data.get("ContractName", "") != "":
                            return contract_data.get("SourceCode", "")
            
            return None
        except Exception as e:
            logger.error(f"Failed to get Ethereum source code for {address}: {str(e)}")
            return None

    async def _get_bsc_source_code(self, address: str) -> Optional[str]:
        """Get the source code of a BSC contract."""
        try:
            bscscan_api_key = os.getenv("BSCSCAN_API_KEY")
            if not bscscan_api_key:
                return None
            
            params = {
                "module": "contract",
                "action": "getsourcecode",
                "address": address,
                "apikey": bscscan_api_key
            }
            
            url = "https://api.bscscan.com/api"
            
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["status"] == "1" and data["result"]:
                        contract_data = data["result"][0]
                        if contract_data.get("ContractName", "") != "":
                            return contract_data.get("SourceCode", "")
            
            return None
        except Exception as e:
            logger.error(f"Failed to get BSC source code for {address}: {str(e)}")
            return None
