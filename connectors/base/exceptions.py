"""
Custom exception classes for the connector framework.

This module defines the exception hierarchy used throughout the connector
framework for consistent error handling and reporting.
"""

from typing import Any, Dict, Optional


class ConnectorError(Exception):
    """Base exception class for all connector-related errors."""
    
    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the connector error.
        
        Args:
            message: Error message
            error_code: Optional error code
            details: Optional error details
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}


class ConfigurationError(ConnectorError):
    """Raised when connector configuration is invalid."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "CONFIG_ERROR", details)


class ValidationError(ConnectorError):
    """Raised when data validation fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "VALIDATION_ERROR", details)


class ExtractionError(ConnectorError):
    """Raised when data extraction fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "EXTRACTION_ERROR", details)


class TransformationError(ConnectorError):
    """Raised when data transformation fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "TRANSFORMATION_ERROR", details)


class LoadingError(ConnectorError):
    """Raised when data loading fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "LOADING_ERROR", details)


class SchemaError(ConnectorError):
    """Raised when schema validation fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "SCHEMA_ERROR", details)


class AuthenticationError(ConnectorError):
    """Raised when authentication fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "AUTH_ERROR", details)


class RateLimitError(ConnectorError):
    """Raised when rate limits are exceeded."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "RATE_LIMIT_ERROR", details)


class NetworkError(ConnectorError):
    """Raised when network operations fail."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "NETWORK_ERROR", details)


class TimeoutError(ConnectorError):
    """Raised when operations timeout."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, "TIMEOUT_ERROR", details)
