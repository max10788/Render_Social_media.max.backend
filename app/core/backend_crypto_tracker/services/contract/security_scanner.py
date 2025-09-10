import os
import json
import asyncio
import subprocess
import tempfile
import aiohttp
import re
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
from pathlib import Path
from functools import lru_cache

from app.core.backend_crypto_tracker.utils.logger import get_logger
from app.core.backend_crypto_tracker.utils.exceptions import APIException, NotFoundException, SecurityScanException
from app.core.backend_crypto_tracker.services.contract.contract_metadata import ContractMetadataService

logger = get_logger(__name__)

class SecurityScanner:
    """Service für die Analyse von Smart Contracts auf Sicherheitslücken."""
    
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
    
    def __init__(self, cache_ttl: int = 3600):
        """
        Initialisiere Scanner und Regeln.
        
        Args:
            cache_ttl: Cache Time-To-Live in Sekunden
        """
        # Initialisiere Contract-Metadaten-Service
        self.metadata_service = ContractMetadataService()
        
        # Initialisiere Session für HTTP-Anfragen
        self.session = None
        
        # Pfad zum Slither-Executable (falls verfügbar)
        self.slither_path = os.getenv("SLITHER_PATH", "slither")
        
        # Cache-TTL
        self.cache_ttl = cache_ttl
        
        # Chain-spezifische Sicherheitsregeln
        self.chain_specific_rules = {
            "ethereum": {
                "max_external_calls": int(os.getenv("ETH_MAX_EXTERNAL_CALLS", "5")),
                "recommended_min_proxy_delay": int(os.getenv("ETH_MIN_PROXY_DELAY", "172800")),  # 2 Tage in Sekunden
                "recommended_max_supply": os.getenv("ETH_MAX_SUPPLY", "1000000000000000000000000")  # 1 Million Tokens mit 18 Dezimalstellen
            },
            "bsc": {
                "max_external_calls": int(os.getenv("BSC_MAX_EXTERNAL_CALLS", "5")),
                "recommended_min_proxy_delay": int(os.getenv("BSC_MIN_PROXY_DELAY", "172800")),  # 2 Tage in Sekunden
                "recommended_max_supply": os.getenv("BSC_MAX_SUPPLY", "1000000000000000000000000")  # 1 Million Tokens mit 18 Dezimalstellen
            },
            "solana": {
                "max_cross_program_invocations": int(os.getenv("SOL_MAX_CPI", "4")),
                "recommended_max_compute_units": int(os.getenv("SOL_MAX_COMPUTE", "200000"))
            },
            "sui": {
                "max_object_dependencies": int(os.getenv("SUI_MAX_OBJECTS", "10")),
                "recommended_max_gas": int(os.getenv("SUI_MAX_GAS", "10000000"))
            }
        }
        
        # Initialisiere Vulnerability-Patterns für Regex-basierte Erkennung
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
        
        # Cache für Analyseergebnisse
        self._cache = {}
    
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
    
    def _get_cache_key(self, method_name: str, *args) -> str:
        """Erzeuge einen Cache-Schlüssel für eine Methode mit ihren Argumenten."""
        return f"{method_name}:{':'.join(str(arg) for arg in args)}"
    
    def _is_cache_valid(self, timestamp: float) -> bool:
        """Prüfe, ob ein Cache-Eintrag noch gültig ist."""
        return (datetime.now().timestamp() - timestamp) < self.cache_ttl
    
    async def scan_contract_security(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Führe eine umfassende Sicherheitsanalyse eines Smart Contracts durch.
        
        Args:
            address: Contract-Adresse
            chain: Blockchain-Name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary mit den Ergebnissen der Sicherheitsanalyse
        """
        try:
            # Prüfe Cache
            cache_key = self._get_cache_key("scan_contract_security", address, chain)
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key]["timestamp"]):
                logger.info(f"Using cached security scan results for {address} on {chain}")
                return self._cache[cache_key]["data"]
            
            logger.info(f"Starting security scan for contract {address} on {chain}")
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Führe Vulnerability-Check durch
            vulnerabilities = await self.check_vulnerabilities(address, chain)
            
            # Analysiere Zugriffskontrollen
            access_control_issues = await self.analyze_access_control(address, chain)
            
            # Analysiere wirtschaftliche Risiken
            economic_risks = await self.analyze_economic_risks(address, chain)
            
            # Berechne Verifizierungs-Konfidenz
            verification_confidence = await self.calculate_verification_confidence(address, chain)
            
            # Berechne Code-Qualitäts-Metriken
            code_quality_metrics = await self._analyze_code_quality(address, chain)
            
            result = {
                "vulnerabilities": vulnerabilities,
                "code_quality_metrics": code_quality_metrics,
                "access_control_issues": access_control_issues,
                "economic_risks": economic_risks,
                "verification_confidence": verification_confidence
            }
            
            # Speichere im Cache
            self._cache[cache_key] = {
                "timestamp": datetime.now().timestamp(),
                "data": result
            }
            
            logger.info(f"Completed security scan for contract {address} on {chain}")
            return result
        except Exception as e:
            error_msg = f"Failed to scan contract security for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            raise SecurityScanException(error_msg) from e
    
    async def check_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """
        Prüfe auf bekannte Vulnerabilities in einem Smart Contract.
        
        Args:
            address: Contract-Adresse
            chain: Blockchain-Name (ethereum, bsc, solana, sui)
            
        Returns:
            Liste der erkannten Vulnerabilities
        """
        try:
            # Prüfe Cache
            cache_key = self._get_cache_key("check_vulnerabilities", address, chain)
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key]["timestamp"]):
                logger.info(f"Using cached vulnerability check results for {address} on {chain}")
                return self._cache[cache_key]["data"]
            
            logger.info(f"Checking vulnerabilities for contract {address} on {chain}")
            
            vulnerabilities = []
            
            if chain.lower() in ["ethereum", "bsc"]:
                # Verwende Slither für Solidity-Contracts
                vulnerabilities = await self._check_solidity_vulnerabilities(address, chain)
                
                # Führe zusätzliche Regex-basierte Prüfungen durch
                regex_vulnerabilities = await self._check_vulnerabilities_with_regex(address, chain)
                vulnerabilities.extend(regex_vulnerabilities)
                
            elif chain.lower() == "solana":
                # Solana-spezifische Vulnerability-Prüfungen
                vulnerabilities = await self._check_solana_vulnerabilities(address, chain)
            elif chain.lower() == "sui":
                # Sui-spezifische Vulnerability-Prüfungen
                vulnerabilities = await self._check_sui_vulnerabilities(address, chain)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            # Entferne Duplikate basierend auf ID und Zeilennummer
            unique_vulnerabilities = []
            seen = set()
            for vuln in vulnerabilities:
                key = (vuln.get("id", ""), vuln.get("line_number", 0))
                if key not in seen:
                    seen.add(key)
                    unique_vulnerabilities.append(vuln)
            
            # Speichere im Cache
            self._cache[cache_key] = {
                "timestamp": datetime.now().timestamp(),
                "data": unique_vulnerabilities
            }
            
            logger.info(f"Found {len(unique_vulnerabilities)} vulnerabilities for contract {address} on {chain}")
            return unique_vulnerabilities
        except Exception as e:
            error_msg = f"Failed to check vulnerabilities for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            raise SecurityScanException(error_msg) from e
    
    async def _check_vulnerabilities_with_regex(self, address: str, chain: str) -> List[Dict]:
        """Prüfe auf Vulnerabilities mit Regex-Patterns im Contract-Quellcode."""
        try:
            vulnerabilities = []
            
            # Hole Contract-Quellcode
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                return vulnerabilities
            
            # Prüfe für jedes Vulnerability-Pattern
            for swc_id, patterns in self.vulnerability_patterns.items():
                for pattern in patterns:
                    matches = re.finditer(pattern, source_code, re.IGNORECASE)
                    for match in matches:
                        # Hole Zeilennummer
                        line_number = source_code[:match.start()].count('\n') + 1
                        
                        # Hole Vulnerability-Details
                        vuln_details = self.SWC_REGISTRY.get(swc_id, {})
                        
                        vulnerability = {
                            "id": swc_id,
                            "name": vuln_details.get("name", "Unknown Vulnerability"),
                            "severity": vuln_details.get("severity", "medium"),
                            "description": vuln_details.get("description", "No description available"),
                            "line_number": line_number
                        }
                        
                        vulnerabilities.append(vulnerability)
            
            return vulnerabilities
        except Exception as e:
            error_msg = f"Failed to check vulnerabilities with regex for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return []
    
    async def _get_contract_source_code(self, address: str, chain: str) -> Optional[str]:
        """Hole den Quellcode eines Contracts."""
        try:
            # Prüfe Cache
            cache_key = self._get_cache_key("_get_contract_source_code", address, chain)
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key]["timestamp"]):
                return self._cache[cache_key]["data"]
            
            source_code = None
            
            if chain.lower() == "ethereum":
                source_code = await self._get_ethereum_source_code(address)
            elif chain.lower() == "bsc":
                source_code = await self._get_bsc_source_code(address)
            
            # Speichere im Cache
            self._cache[cache_key] = {
                "timestamp": datetime.now().timestamp(),
                "data": source_code
            }
            
            return source_code
        except Exception as e:
            error_msg = f"Failed to get source code for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return None
    
    async def _get_ethereum_source_code(self, address: str) -> Optional[str]:
        """Hole den Quellcode eines Ethereum-Contracts."""
        try:
            etherscan_api_key = os.getenv("ETHERSCAN_API_KEY")
            if not etherscan_api_key:
                logger.warning("ETHERSCAN_API_KEY not set, cannot fetch Ethereum source code")
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
            error_msg = f"Failed to get Ethereum source code for {address}: {str(e)}"
            logger.error(error_msg)
            return None
    
    async def _get_bsc_source_code(self, address: str) -> Optional[str]:
        """Hole den Quellcode eines BSC-Contracts."""
        try:
            bscscan_api_key = os.getenv("BSCSCAN_API_KEY")
            if not bscscan_api_key:
                logger.warning("BSCSCAN_API_KEY not set, cannot fetch BSC source code")
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
            error_msg = f"Failed to get BSC source code for {address}: {str(e)}"
            logger.error(error_msg)
            return None
    
    async def _check_solidity_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """Prüfe auf Vulnerabilities in Solidity-Contracts mit Slither."""
        try:
            # Hole Contract-ABI
            abi = await self.metadata_service.get_abi(address, chain)
            if not abi:
                logger.warning(f"No ABI available for contract {address} on {chain}")
                return []
            
            # Erstelle eine temporäre Datei für die ABI
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as abi_file:
                abi_file.write(abi)
                abi_path = abi_file.name
            
            try:
                # Führe Slither aus, um Vulnerabilities zu erkennen
                cmd = [
                    self.slither_path,
                    address,
                    "--json",
                    "-",
                    "--detect",
                    "all"
                ]
                
                # Füge API-Schlüssel hinzu, falls verfügbar
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
                
                # Parse Slither-Ausgabe
                try:
                    slither_results = json.loads(stdout.decode())
                except json.JSONDecodeError:
                    logger.error("Failed to parse Slither output as JSON")
                    return []
                
                # Konvertiere Slither-Ergebnisse in unser Format
                vulnerabilities = []
                for detector_result in slither_results.get("detectors", []):
                    # Mappe Slither-Detector zu SWC-ID, falls möglich
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
                # Räume temporäre Datei auf
                os.unlink(abi_path)
        except Exception as e:
            error_msg = f"Failed to check Solidity vulnerabilities for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return []
    
    def _map_slither_to_swc(self, slither_check: str) -> str:
        """Mappe einen Slither-Check-Namen zu einer SWC-ID."""
        # Einfache Mapping für gängige Checks
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
        """Normalisiere Schweregrad zu unserem Standardformat."""
        severity = severity.lower()
        if severity in ["high", "critical"]:
            return "high"
        elif severity in ["medium", "warning"]:
            return "medium"
        elif severity in ["low", "informational", "optimization"]:
            return "low"
        else:
            return "medium"  # Standard auf medium, falls unbekannt
    
    async def _check_solana_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """Prüfe auf Vulnerabilities in Solana-Programmen."""
        try:
            vulnerabilities = []
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Prüfe auf gängige Solana-Vulnerabilities
            if not metadata.get("verification_status", False):
                vulnerabilities.append({
                    "id": "SOL-001",
                    "name": "Unverified Program",
                    "severity": "medium",
                    "description": "The program has not been verified, making it difficult to audit for vulnerabilities.",
                    "line_number": 0
                })
            
            # Prüfe auf übermäßige Cross-Program-Invocations
            # Dies würde die Analyse der Anweisungen des Programms erfordern
            # Für jetzt fügen wir eine Platzhalter-Prüfung hinzu
            vulnerabilities.append({
                "id": "SOL-002",
                "name": "Excessive Cross-Program Invocations",
                "severity": "medium",
                "description": "The program makes many cross-program invocations, which could lead to complexity issues.",
                "line_number": 0
            })
            
            # Prüfe auf fehlende Account-Validierung
            vulnerabilities.append({
                "id": "SOL-003",
                "name": "Missing Account Validation",
                "severity": "high",
                "description": "The program may not properly validate accounts before use.",
                "line_number": 0
            })
            
            return vulnerabilities
        except Exception as e:
            error_msg = f"Failed to check Solana vulnerabilities for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return []
    
    async def _check_sui_vulnerabilities(self, address: str, chain: str) -> List[Dict]:
        """Prüfe auf Vulnerabilities in Sui Move-Modulen."""
        try:
            vulnerabilities = []
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Prüfe auf gängige Sui-Vulnerabilities
            if not metadata.get("verification_status", False):
                vulnerabilities.append({
                    "id": "SUI-001",
                    "name": "Unverified Module",
                    "severity": "medium",
                    "description": "The module has not been verified, making it difficult to audit for vulnerabilities.",
                    "line_number": 0
                })
            
            # Prüfe auf übermäßige Objekt-Abhängigkeiten
            # Dies würde die Analyse der Abhängigkeiten des Moduls erfordern
            # Für jetzt fügen wir eine Platzhalter-Prüfung hinzu
            vulnerabilities.append({
                "id": "SUI-002",
                "name": "Excessive Object Dependencies",
                "severity": "medium",
                "description": "The module has many object dependencies, which could lead to complexity issues.",
                "line_number": 0
            })
            
            # Prüfe auf fehlende Capability-Checks
            vulnerabilities.append({
                "id": "SUI-003",
                "name": "Missing Capability Checks",
                "severity": "high",
                "description": "The module may not properly check capabilities before sensitive operations.",
                "line_number": 0
            })
            
            return vulnerabilities
        except Exception as e:
            error_msg = f"Failed to check Sui vulnerabilities for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return []
    
    async def analyze_access_control(self, address: str, chain: str) -> List[str]:
        """
        Analysiere Zugriffskontrollprobleme in einem Smart Contract.
        
        Args:
            address: Contract-Adresse
            chain: Blockchain-Name (ethereum, bsc, solana, sui)
            
        Returns:
            Liste der Zugriffskontrollprobleme
        """
        try:
            # Prüfe Cache
            cache_key = self._get_cache_key("analyze_access_control", address, chain)
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key]["timestamp"]):
                logger.info(f"Using cached access control analysis for {address} on {chain}")
                return self._cache[cache_key]["data"]
            
            logger.info(f"Analyzing access control for contract {address} on {chain}")
            
            issues = []
            
            if chain.lower() in ["ethereum", "bsc"]:
                issues = await self._analyze_solidity_access_control(address, chain)
            elif chain.lower() == "solana":
                issues = await self._analyze_solana_access_control(address, chain)
            elif chain.lower() == "sui":
                issues = await self._analyze_sui_access_control(address, chain)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            # Speichere im Cache
            self._cache[cache_key] = {
                "timestamp": datetime.now().timestamp(),
                "data": issues
            }
            
            logger.info(f"Found {len(issues)} access control issues for contract {address} on {chain}")
            return issues
        except Exception as e:
            error_msg = f"Failed to analyze access control for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            raise SecurityScanException(error_msg) from e
    
    async def _analyze_solidity_access_control(self, address: str, chain: str) -> List[str]:
        """Analysiere Zugriffskontrollprobleme in Solidity-Contracts."""
        try:
            issues = []
            
            # Hole Contract-Quellcode
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                issues.append("No source code available to analyze access control")
                return issues
            
            # Prüfe auf fehlende Zugriffskontrolle bei kritischen Funktionen
            critical_functions = [
                "mint", "burn", "pause", "unpause", "withdraw", "transferownership",
                "setfee", "settax", "setlimit", "setrate", "setaddress", "changeowner"
            ]
            
            # Prüfe auf Funktionen mit Namen, die mit kritischen Funktionen übereinstimmen
            for func in critical_functions:
                pattern = rf"function\s+{func}\s*\("
                matches = re.finditer(pattern, source_code, re.IGNORECASE)
                for match in matches:
                    # Hole die Funktionssignatur
                    line_start = source_code.rfind('\n', 0, match.start()) + 1
                    line_end = source_code.find('\n', match.end())
                    if line_end == -1:
                        line_end = len(source_code)
                    
                    function_line = source_code[line_start:line_end].strip()
                    
                    # Prüfe, ob die Funktion Zugriffskontroll-Modifikatoren hat
                    has_access_control = any(
                        modifier in function_line 
                        for modifier in ["onlyOwner", "onlyAdmin", "whenNotPaused", "whenPaused", "internal", "private"]
                    )
                    
                    if not has_access_control:
                        issues.append(f"Critical function {func} lacks access control")
            
            # Prüfe auf tx.origin-Nutzung
            if re.search(r"tx\.origin", source_code):
                issues.append("Use of tx.origin detected, which can lead to phishing attacks")
            
            # Prüfe auf ungeschützten selfdestruct
            if re.search(r"selfdestruct\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                issues.append("Unprotected selfdestruct function detected")
            
            # Prüfe auf ungeschützten delegatecall
            if re.search(r"delegatecall\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                issues.append("Unprotected delegatecall function detected")
            
            # Prüfe auf fehlenden Pausierungsmechanismus
            if not re.search(r"pause\s*\(", source_code) and not re.search(r"unpause\s*\(", source_code):
                issues.append("No pausing mechanism detected, which could be problematic in emergencies")
            
            return issues
        except Exception as e:
            error_msg = f"Failed to analyze Solidity access control for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return ["Failed to analyze access control due to an error"]
    
    async def _analyze_solana_access_control(self, address: str, chain: str) -> List[str]:
        """Analysiere Zugriffskontrollprobleme in Solana-Programmen."""
        try:
            issues = []
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Solana-spezifische Zugriffskontrollprüfungen
            if not metadata.get("verification_status", False):
                issues.append("Unverified program, access control cannot be fully verified")
            
            # Prüfe auf fehlende Account-Validierung
            issues.append("Potential missing account validation in program")
            
            # Prüfe auf fehlende Authority-Checks
            issues.append("Potential missing authority checks in program")
            
            return issues
        except Exception as e:
            error_msg = f"Failed to analyze Solana access control for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return ["Failed to analyze access control due to an error"]
    
    async def _analyze_sui_access_control(self, address: str, chain: str) -> List[str]:
        """Analysiere Zugriffskontrollprobleme in Sui Move-Modulen."""
        try:
            issues = []
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Sui-spezifische Zugriffskontrollprüfungen
            if not metadata.get("verification_status", False):
                issues.append("Unverified module, access control cannot be fully verified")
            
            # Prüfe auf fehlende Capability-Checks
            issues.append("Potential missing capability checks in module")
            
            # Prüfe auf fehlende Objekt-Eigentümer-Validierung
            issues.append("Potential missing object ownership validation in module")
            
            return issues
        except Exception as e:
            error_msg = f"Failed to analyze Sui access control for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return ["Failed to analyze access control due to an error"]
    
    async def analyze_economic_risks(self, address: str, chain: str) -> List[str]:
        """
        Analysiere wirtschaftliche Risiken in einem Smart Contract.
        
        Args:
            address: Contract-Adresse
            chain: Blockchain-Name (ethereum, bsc, solana, sui)
            
        Returns:
            Liste der wirtschaftlichen Risiken
        """
        try:
            # Prüfe Cache
            cache_key = self._get_cache_key("analyze_economic_risks", address, chain)
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key]["timestamp"]):
                logger.info(f"Using cached economic risks analysis for {address} on {chain}")
                return self._cache[cache_key]["data"]
            
            logger.info(f"Analyzing economic risks for contract {address} on {chain}")
            
            risks = []
            
            if chain.lower() in ["ethereum", "bsc"]:
                risks = await self._analyze_solidity_economic_risks(address, chain)
            elif chain.lower() == "solana":
                risks = await self._analyze_solana_economic_risks(address, chain)
            elif chain.lower() == "sui":
                risks = await self._analyze_sui_economic_risks(address, chain)
            else:
                raise ValueError(f"Unsupported blockchain: {chain}")
            
            # Speichere im Cache
            self._cache[cache_key] = {
                "timestamp": datetime.now().timestamp(),
                "data": risks
            }
            
            logger.info(f"Found {len(risks)} economic risks for contract {address} on {chain}")
            return risks
        except Exception as e:
            error_msg = f"Failed to analyze economic risks for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            raise SecurityScanException(error_msg) from e
    
    async def _analyze_solidity_economic_risks(self, address: str, chain: str) -> List[str]:
        """Analysiere wirtschaftliche Risiken in Solidity-Contracts."""
        try:
            risks = []
            
            # Hole Contract-Quellcode
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                risks.append("No source code available to analyze economic risks")
                return risks
            
            # Prüfe auf unbegrenztes Minting
            if re.search(r"mint\s*\(", source_code) and not re.search(r"_maxSupply", source_code):
                risks.append("Potential for unlimited minting detected")
            
            # Prüfe auf übermäßiges Burning
            if re.search(r"burn\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                risks.append("Potential for excessive burning detected")
            
            # Prüfe auf Rug-Pull-Potenzial
            if re.search(r"withdraw\s*\(", source_code) and not re.search(r"onlyOwner", source_code):
                risks.append("Potential for rug pull detected")
            
            # Prüfe auf übermäßige Gebühren oder Steuern
            if re.search(r"fee\s*=\s*[0-9]+", source_code) and re.search(r"fee\s*>\s*10", source_code):
                risks.append("Potential for excessive fees or taxes detected")
            
            # Prüfe auf fehlenden Pausierungsmechanismus
            if not re.search(r"pause\s*\(", source_code) and not re.search(r"unpause\s*\(", source_code):
                risks.append("No pausing mechanism detected, which could be problematic in emergencies")
            
            # Prüfe auf Honeypot-Potenzial
            if re.search(r"transfer\s*\(", source_code) and re.search(r"require\s*\(\s*false", source_code):
                risks.append("Potential honeypot mechanism detected")
            
            # Prüfe auf Anti-Whale-Mechanismen
            if re.search(r"balanceOf\s*\(\s*msg\.sender\s*\)\s*>\s*max", source_code):
                risks.append("Anti-whale mechanism detected, which may limit usability")
            
            return risks
        except Exception as e:
            error_msg = f"Failed to analyze Solidity economic risks for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return ["Failed to analyze economic risks due to an error"]
    
    async def _analyze_solana_economic_risks(self, address: str, chain: str) -> List[str]:
        """Analysiere wirtschaftliche Risiken in Solana-Programmen."""
        try:
            risks = []
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Solana-spezifische wirtschaftliche Risikoprüfungen
            if not metadata.get("verification_status", False):
                risks.append("Unverified program, economic risks cannot be fully verified")
            
            # Prüfe auf übermäßiges Minting
            risks.append("Potential for excessive minting detected")
            
            # Prüfe auf Rug-Pull-Potenzial
            risks.append("Potential for rug pull detected")
            
            return risks
        except Exception as e:
            error_msg = f"Failed to analyze Solana economic risks for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return ["Failed to analyze economic risks due to an error"]
    
    async def _analyze_sui_economic_risks(self, address: str, chain: str) -> List[str]:
        """Analysiere wirtschaftliche Risiken in Sui Move-Modulen."""
        try:
            risks = []
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Sui-spezifische wirtschaftliche Risikoprüfungen
            if not metadata.get("verification_status", False):
                risks.append("Unverified module, economic risks cannot be fully verified")
            
            # Prüfe auf übermäßiges Minting
            risks.append("Potential for excessive minting detected")
            
            # Prüfe auf Rug-Pull-Potenzial
            risks.append("Potential for rug pull detected")
            
            return risks
        except Exception as e:
            error_msg = f"Failed to analyze Sui economic risks for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return ["Failed to analyze economic risks due to an error"]
    
    async def calculate_verification_confidence(self, address: str, chain: str) -> float:
        """
        Berechne den Verifizierungs-Konfidenz-Score für einen Contract.
        
        Args:
            address: Contract-Adresse
            chain: Blockchain-Name (ethereum, bsc, solana, sui)
            
        Returns:
            Verifizierungs-Konfidenz-Score zwischen 0.0 und 1.0
        """
        try:
            # Prüfe Cache
            cache_key = self._get_cache_key("calculate_verification_confidence", address, chain)
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key]["timestamp"]):
                logger.info(f"Using cached verification confidence for {address} on {chain}")
                return self._cache[cache_key]["data"]
            
            logger.info(f"Calculating verification confidence for contract {address} on {chain}")
            
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Beginne mit einem Basis-Score
            confidence = 0.0
            
            # Prüfe, ob der Contract verifiziert ist
            if metadata.get("verification_status", False):
                confidence += 0.4
            
            # Prüfe, ob wir eine ABI haben
            abi = await self.metadata_service.get_abi(address, chain)
            if abi:
                confidence += 0.2
            
            # Prüfe, ob wir eine Creator-Adresse haben
            creator = await self.metadata_service.get_creator_address(address, chain)
            if creator:
                confidence += 0.1
            
            # Prüfe auf Deploymentsdatum
            if metadata.get("deployment_date"):
                confidence += 0.1
            
            # Prüfe auf Contract-Typ
            if metadata.get("contract_type"):
                confidence += 0.1
            
            # Prüfe auf Verfügbarkeit des Quellcodes
            source_code = await self._get_contract_source_code(address, chain)
            if source_code:
                confidence += 0.1
            
            # Prüfe auf Vulnerabilities
            vulnerabilities = await self.check_vulnerabilities(address, chain)
            if vulnerabilities:
                # Reduziere Konfidenz basierend auf der Anzahl der hochschweren Vulnerabilities
                high_severity_count = sum(1 for v in vulnerabilities if v.get("severity") == "high")
                confidence -= min(0.3, high_severity_count * 0.1)
            
            # Stelle sicher, dass der Score zwischen 0.0 und 1.0 liegt
            confidence = max(0.0, min(1.0, confidence))
            
            # Speichere im Cache
            self._cache[cache_key] = {
                "timestamp": datetime.now().timestamp(),
                "data": confidence
            }
            
            logger.info(f"Calculated verification confidence {confidence} for contract {address} on {chain}")
            return confidence
        except Exception as e:
            error_msg = f"Failed to calculate verification confidence for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return 0.0
    
    async def _analyze_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Analysiere Code-Qualitätsmetriken für einen Contract.
        
        Args:
            address: Contract-Adresse
            chain: Blockchain-Name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary mit Code-Qualitätsmetriken
        """
        try:
            # Prüfe Cache
            cache_key = self._get_cache_key("_analyze_code_quality", address, chain)
            if cache_key in self._cache and self._is_cache_valid(self._cache[cache_key]["timestamp"]):
                logger.info(f"Using cached code quality analysis for {address} on {chain}")
                return self._cache[cache_key]["data"]
            
            logger.info(f"Analyzing code quality for contract {address} on {chain}")
            
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
            
            # Speichere im Cache
            self._cache[cache_key] = {
                "timestamp": datetime.now().timestamp(),
                "data": metrics
            }
            
            logger.info(f"Analyzed code quality for contract {address} on {chain}")
            return metrics
        except Exception as e:
            error_msg = f"Failed to analyze code quality for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
    
    async def _analyze_solidity_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """Analysiere Code-Qualitätsmetriken für Solidity-Contracts."""
        try:
            # Hole Contract-Quellcode
            source_code = await self._get_contract_source_code(address, chain)
            if not source_code:
                return {
                    "complexity_score": 0,
                    "lines_of_code": 0,
                    "external_calls": 0
                }
            
            # Zähle Codezeilen
            lines_of_code = len(source_code.split('\n'))
            
            # Zähle externe Aufrufe
            external_calls = len(re.findall(r"\.(call|send|transfer|delegatecall)\s*\(", source_code))
            
            # Zähle Funktionen
            function_count = len(re.findall(r"function\s+\w+\s*\(", source_code))
            
            # Zähle Modifikatoren
            modifier_count = len(re.findall(r"modifier\s+\w+\s*\(", source_code))
            
            # Zähle Events
            event_count = len(re.findall(r"event\s+\w+\s*\(", source_code))
            
            # Berechne Komplexitäts-Score basierend auf verschiedenen Faktoren
            complexity_score = 0
            
            # Basis-Komplexität aus Funktionsanzahl
            complexity_score += min(30, function_count * 2)
            
            # Füge Komplexität aus externen Aufrufen hinzu
            complexity_score += min(20, external_calls * 4)
            
            # Füge Komplexität aus Codezeilen hinzu
            complexity_score += min(20, lines_of_code / 50)
            
            # Füge Komplexität aus Modifikatoren hinzu
            complexity_score += min(10, modifier_count * 2)
            
            # Füge Komplexität aus Events hinzu
            complexity_score += min(10, event_count)
            
            # Füge Komplexität aus Vererbung hinzu
            inheritance_count = len(re.findall(r"is\s+\w+", source_code))
            complexity_score += min(10, inheritance_count * 3)
            
            # Stelle sicher, dass der Komplexitäts-Score zwischen 0 und 100 liegt
            complexity_score = min(100, complexity_score)
            
            return {
                "complexity_score": complexity_score,
                "lines_of_code": lines_of_code,
                "external_calls": external_calls
            }
        except Exception as e:
            error_msg = f"Failed to analyze Solidity code quality for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
    
    async def _analyze_solana_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """Analysiere Code-Qualitätsmetriken für Solana-Programme."""
        try:
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Solana-spezifische Code-Qualitätsmetriken
            # Dies ist eine vereinfachte Analyse; in einer echten Implementierung würden wir Solana-spezifische Tools verwenden
            
            # Platzhalter-Werte
            complexity_score = 50
            lines_of_code = 1000
            external_calls = 3
            
            return {
                "complexity_score": complexity_score,
                "lines_of_code": lines_of_code,
                "external_calls": external_calls
            }
        except Exception as e:
            error_msg = f"Failed to analyze Solana code quality for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
    
    async def _analyze_sui_code_quality(self, address: str, chain: str) -> Dict[str, Any]:
        """Analysiere Code-Qualitätsmetriken für Sui Move-Module."""
        try:
            # Hole Contract-Metadaten
            metadata = await self.metadata_service.get_contract_metadata(address, chain)
            
            # Sui-spezifische Code-Qualitätsmetriken
            # Dies ist eine vereinfachte Analyse; in einer echten Implementierung würden wir Sui-spezifische Tools verwenden
            
            # Platzhalter-Werte
            complexity_score = 50
            lines_of_code = 1000
            external_calls = 3
            
            return {
                "complexity_score": complexity_score,
                "lines_of_code": lines_of_code,
                "external_calls": external_calls
            }
        except Exception as e:
            error_msg = f"Failed to analyze Sui code quality for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            return {
                "complexity_score": 0,
                "lines_of_code": 0,
                "external_calls": 0
            }
    
    async def _calculate_risk_score(self, vulnerabilities: List[Dict], access_control_issues: List[str], 
                                  economic_risks: List[str], verification_confidence: float) -> float:
        """
        Berechne einen Gesamt-Risiko-Score basierend auf den Sicherheitsanalyse-Ergebnissen.
        
        Args:
            vulnerabilities: Liste der erkannten Vulnerabilities
            access_control_issues: Liste der Zugriffskontrollprobleme
            economic_risks: Liste der wirtschaftlichen Risiken
            verification_confidence: Verifizierungs-Konfidenz-Score
            
        Returns:
            Risiko-Score zwischen 0.0 (geringes Risiko) und 1.0 (hohes Risiko)
        """
        try:
            # Beginne mit einem Basis-Risiko-Score
            risk_score = 0.0
            
            # Füge Risiko basierend auf Vulnerabilities hinzu
            for vuln in vulnerabilities:
                severity = vuln.get("severity", "medium")
                if severity == "high":
                    risk_score += 0.2
                elif severity == "medium":
                    risk_score += 0.1
                else:  # low
                    risk_score += 0.05
            
            # Füge Risiko basierend auf Zugriffskontrollproblemen hinzu
            risk_score += len(access_control_issues) * 0.1
            
            # Füge Risiko basierend auf wirtschaftlichen Risiken hinzu
            risk_score += len(economic_risks) * 0.1
            
            # Reduziere Risiko basierend auf Verifizierungs-Konfidenz
            risk_score *= (1.0 - verification_confidence)
            
            # Stelle sicher, dass der Score zwischen 0.0 und 1.0 liegt
            risk_score = max(0.0, min(1.0, risk_score))
            
            return risk_score
        except Exception as e:
            error_msg = f"Failed to calculate risk score: {str(e)}"
            logger.error(error_msg)
            return 0.5  # Standard auf mittleres Risiko
    
    async def generate_security_report(self, address: str, chain: str) -> Dict[str, Any]:
        """
        Erstelle einen umfassenden Sicherheitsbericht für einen Contract.
        
        Args:
            address: Contract-Adresse
            chain: Blockchain-Name (ethereum, bsc, solana, sui)
            
        Returns:
            Dictionary mit dem Sicherheitsbericht
        """
        try:
            logger.info(f"Generating security report for contract {address} on {chain}")
            
            # Führe Sicherheitsanalyse durch
            scan_results = await self.scan_contract_security(address, chain)
            
            # Berechne Risiko-Score
            risk_score = await self._calculate_risk_score(
                scan_results["vulnerabilities"],
                scan_results["access_control_issues"],
                scan_results["economic_risks"],
                scan_results["verification_confidence"]
            )
            
            # Erstelle Bericht
            report = {
                "contract_address": address,
                "blockchain": chain,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "risk_score": risk_score,
                "risk_level": self._get_risk_level(risk_score),
                "summary": self._generate_summary(scan_results, risk_score),
                "details": scan_results
            }
            
            logger.info(f"Generated security report for contract {address} on {chain}")
            return report
        except Exception as e:
            error_msg = f"Failed to generate security report for {address} on {chain}: {str(e)}"
            logger.error(error_msg)
            raise SecurityScanException(error_msg) from e
    
    def _get_risk_level(self, risk_score: float) -> str:
        """Konvertiere Risiko-Score in Risiko-Level."""
        if risk_score >= 0.7:
            return "critical"
        elif risk_score >= 0.5:
            return "high"
        elif risk_score >= 0.3:
            return "medium"
        else:
            return "low"

    def _generate_summary(self, scan_results: Dict[str, Any], risk_score: float) -> str:
        """Erstelle eine menschenlesbare Zusammenfassung der Sicherheitsanalyse."""
        try:
            vulnerabilities = scan_results["vulnerabilities"]
            access_control_issues = scan_results["access_control_issues"]
            economic_risks = scan_results["economic_risks"]
            verification_confidence = scan_results["verification_confidence"]
            
            # Zähle hochschwere Vulnerabilities
            high_vulns = sum(1 for v in vulnerabilities if v.get("severity") == "high")
            medium_vulns = sum(1 for v in vulnerabilities if v.get("severity") == "medium")
            low_vulns = sum(1 for v in vulnerabilities if v.get("severity") == "low")
            
            # Erstelle Zusammenfassung basierend auf den Ergebnissen
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
            error_msg = f"Failed to generate summary: {str(e)}"
            logger.error(error_msg)
            return "Unable to generate summary due to an error."
