"""
Utility functions for the connector framework.

This module provides common utilities used across the connector framework,
including retry logic, backoff strategies, configuration validation, and logging setup.
"""

import asyncio
import logging
import os
import random
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import structlog
import yaml
from pydantic import BaseModel, ValidationError

from .exceptions import ConfigurationError, ValidationError

T = TypeVar('T')


def setup_logging(service_name: str) -> structlog.BoundLogger:
    """
    Set up structured logging for a service.
    
    Args:
        service_name: Name of the service
        
    Returns:
        structlog.BoundLogger: Configured logger
    """
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger(service_name)


def exponential_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True
) -> float:
    """
    Calculate exponential backoff delay.
    
    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        jitter: Whether to add random jitter
        
    Returns:
        float: Delay in seconds
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    
    if jitter:
        # Add up to 25% jitter
        jitter_amount = delay * 0.25 * random.random()
        delay += jitter_amount
    
    return delay


def retry_with_backoff(
    func: Callable[..., T],
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
    max_backoff: float = 60.0,
    exceptions: tuple = (Exception,),
    **kwargs
) -> T:
    """
    Retry a function with exponential backoff.
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        backoff_seconds: Base backoff delay in seconds
        max_backoff: Maximum backoff delay in seconds
        exceptions: Tuple of exceptions to catch and retry
        **kwargs: Additional arguments to pass to the function
        
    Returns:
        T: Result of the function
        
    Raises:
        Exception: Last exception if all retries fail
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func(**kwargs)
        except exceptions as e:
            last_exception = e
            
            if attempt == max_retries:
                break
            
            delay = exponential_backoff(attempt, backoff_seconds, max_backoff)
            time.sleep(delay)
    
    raise last_exception


async def async_retry_with_backoff(
    func: Callable[..., T],
    max_retries: int = 3,
    backoff_seconds: float = 1.0,
    max_backoff: float = 60.0,
    exceptions: tuple = (Exception,),
    **kwargs
) -> T:
    """
    Retry an async function with exponential backoff.
    
    Args:
        func: Async function to retry
        max_retries: Maximum number of retry attempts
        backoff_seconds: Base backoff delay in seconds
        max_backoff: Maximum backoff delay in seconds
        exceptions: Tuple of exceptions to catch and retry
        **kwargs: Additional arguments to pass to the function
        
    Returns:
        T: Result of the function
        
    Raises:
        Exception: Last exception if all retries fail
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func(**kwargs)
        except exceptions as e:
            last_exception = e
            
            if attempt == max_retries:
                break
            
            delay = exponential_backoff(attempt, backoff_seconds, max_backoff)
            await asyncio.sleep(delay)
    
    raise last_exception


def validate_config(config: Dict[str, Any], model_class: BaseModel) -> BaseModel:
    """
    Validate configuration against a Pydantic model.
    
    Args:
        config: Configuration dictionary
        model_class: Pydantic model class
        
    Returns:
        BaseModel: Validated configuration instance
        
    Raises:
        ConfigurationError: If validation fails
    """
    try:
        return model_class(**config)
    except ValidationError as e:
        raise ConfigurationError(f"Configuration validation failed: {e}") from e


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.
    
    Args:
        file_path: Path to the YAML file
        
    Returns:
        Dict[str, Any]: Configuration dictionary
        
    Raises:
        ConfigurationError: If file cannot be loaded or parsed
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise ConfigurationError(f"Configuration file not found: {file_path}")
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid YAML in configuration file: {e}") from e


def get_env_var(key: str, default: Optional[str] = None, required: bool = False) -> str:
    """
    Get environment variable with validation.
    
    Args:
        key: Environment variable name
        default: Default value if not set
        required: Whether the variable is required
        
    Returns:
        str: Environment variable value
        
    Raises:
        ConfigurationError: If required variable is not set
    """
    value = os.getenv(key, default)
    
    if required and value is None:
        raise ConfigurationError(f"Required environment variable not set: {key}")
    
    return value


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes into human-readable string.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        str: Formatted string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds into human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        str: Formatted string
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into chunks of specified size.
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List[List[Any]]: List of chunks
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def safe_get(dictionary: Dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Safely get a value from a dictionary with nested key support.
    
    Args:
        dictionary: Dictionary to search
        key: Key to search for (supports dot notation for nested keys)
        default: Default value if key not found
        
    Returns:
        Any: Value or default
    """
    keys = key.split('.')
    current = dictionary
    
    for k in keys:
        if isinstance(current, dict) and k in current:
            current = current[k]
        else:
            return default
    
    return current


def merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries.
    
    Args:
        dict1: First dictionary
        dict2: Second dictionary
        
    Returns:
        Dict[str, Any]: Merged dictionary
    """
    result = dict1.copy()
    
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result
