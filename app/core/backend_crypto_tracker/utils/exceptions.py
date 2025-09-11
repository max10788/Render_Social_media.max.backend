# utils/exceptions.py
from typing import Any, Dict, Optional

class BaseCryptoTrackerException(Exception):
    """Base exception for the application"""
    
    def __init__(
        self, 
        message: str, 
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        self.original_exception = original_exception
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for API responses"""
        result = {
            "error": self.message,
            "error_code": self.error_code
        }
        if self.details:
            result["details"] = self.details
        return result

class ConfigurationException(BaseCryptoTrackerException):
    """Exception for configuration errors"""
    pass

class DatabaseException(BaseCryptoTrackerException):
    """Exception for database errors"""
    pass

class APIException(BaseCryptoTrackerException):
    """Exception for external API errors"""
    pass

class ScannerException(BaseCryptoTrackerException):
    """Exception for scanner-related errors"""
    pass

class BlockchainException(BaseCryptoTrackerException):
    """Exception for blockchain-related errors"""
    pass

class CustomAnalysisException(BlockchainException):
    """Base exception für Custom Analysis Fehler"""
    pass

class UnsupportedChainException(CustomAnalysisException):
    """Exception für nicht unterstützte Chains"""
    
    def __init__(self, chain: str, supported_chains: list):
        super().__init__(
            f"Chain '{chain}' is not supported. Supported chains: {', '.join(supported_chains)}",
            error_code="UNSUPPORTED_CHAIN",
            details={"chain": chain, "supported_chains": supported_chains}
        )

class InvalidAddressException(CustomAnalysisException):
    """Exception für ungültige Adressen"""
    
    def __init__(self, address: str, chain: str, reason: str = None):
        message = f"Invalid address '{address}' for chain '{chain}'"
        if reason:
            message += f": {reason}"
        super().__init__(
            message,
            error_code="INVALID_ADDRESS",
            details={"address": address, "chain": chain, "reason": reason}
        )

class RateLimitExceededException(APIException):
    """Exception wenn Rate Limit überschritten"""
    
    def __init__(self, service: str, limit: int, period: str):
        super().__init__(
            f"Rate limit exceeded for service '{service}'. Limit: {limit} requests per {period}.",
            error_code="RATE_LIMIT_EXCEEDED",
            details={"service": service, "limit": limit, "period": period}
        )

class AnalysisTimeoutException(CustomAnalysisException):
    """Exception bei Analyse Timeout"""
    
    def __init__(self, analysis_type: str, timeout_seconds: int):
        super().__init__(
            f"Analysis '{analysis_type}' timed out after {timeout_seconds} seconds.",
            error_code="ANALYSIS_TIMEOUT",
            details={"analysis_type": analysis_type, "timeout_seconds": timeout_seconds}
        )

class InsufficientDataException(CustomAnalysisException):
    """Exception wenn nicht genügend Daten für Analyse vorhanden"""
    
    def __init__(self, analysis_type: str, data_points_required: int, data_points_available: int):
        super().__init__(
            f"Insufficient data for analysis '{analysis_type}'. Required: {data_points_required}, Available: {data_points_available}.",
            error_code="INSUFFICIENT_DATA",
            details={
                "analysis_type": analysis_type, 
                "data_points_required": data_points_required,
                "data_points_available": data_points_available
            }
        )

class NotFoundException(BaseCryptoTrackerException):
    """Exception for when a resource is not found"""
    
    def __init__(self, resource_type: str, identifier: str = None):
        if identifier:
            message = f"{resource_type} with identifier '{identifier}' not found"
        else:
            message = f"{resource_type} not found"
        
        super().__init__(
            message,
            error_code="NOT_FOUND",
            details={"resource_type": resource_type, "identifier": identifier}
        )

class SecurityScanException(ScannerException):
    """Exception für Sicherheitsanalyse-Fehler"""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            message,
            error_code="SECURITY_SCAN_ERROR",
            details=details
        )
