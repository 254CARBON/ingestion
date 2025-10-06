"""
Base connector framework for 254Carbon ingestion layer.

This module provides the foundational classes and utilities for building
data connectors that extract, transform, and load market data from various
sources into the 254Carbon platform.
"""

from .base_connector import BaseConnector, ConnectorConfig, ExtractionResult, TransformationResult
from .contract_validator import ContractValidator
from .connector_registry import ConnectorRegistry
from .exceptions import (
    ConnectorError,
    ExtractionError,
    TransformationError,
    ValidationError,
    ConfigurationError,
)
from .utils import (
    retry_with_backoff,
    exponential_backoff,
    validate_config,
    setup_logging,
)

__all__ = [
    "BaseConnector",
    "ConnectorConfig",
    "ExtractionResult",
    "TransformationResult",
    "ContractValidator",
    "ConnectorRegistry",
    "ConnectorError",
    "ExtractionError",
    "TransformationError",
    "ValidationError",
    "ConfigurationError",
    "retry_with_backoff",
    "exponential_backoff",
    "validate_config",
    "setup_logging",
]

__version__ = "1.0.0"
