"""
Connector Operator for Airflow.

This operator provides a way to execute connector operations
(extract, transform, load) within Airflow DAGs.
"""

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class ConnectorOperator(BaseOperator):
    """
    Airflow operator for executing connector operations.
    
    This operator provides a way to execute connector operations
    (extract, transform, load) within Airflow DAGs.
    """
    
    @apply_defaults
    def __init__(
        self,
        connector_config: Dict[str, Any],
        operation: str,
        **kwargs
    ):
        """
        Initialize the connector operator.
        
        Args:
            connector_config: Connector configuration
            operation: Operation to perform (extract, transform, load)
            **kwargs: Additional operator arguments
        """
        super().__init__(**kwargs)
        self.connector_config = connector_config
        self.operation = operation
        
        # Validate operation
        if operation not in ['extract', 'transform', 'load']:
            raise ValueError(f"Invalid operation: {operation}")
        
        # Set task ID if not provided
        if not self.task_id:
            self.task_id = f"{operation}_{connector_config.get('name', 'unknown')}"
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the connector operation.
        
        Args:
            context: Airflow context
            
        Returns:
            Any: Operation result
        """
        try:
            logging.info(f"Starting {self.operation} operation for connector: {self.connector_config.get('name')}")
            
            # Run async operation
            result = asyncio.run(self._execute_async(context))
            
            logging.info(f"Completed {self.operation} operation for connector: {self.connector_config.get('name')}")
            return result
            
        except Exception as e:
            error_msg = f"Failed to execute {self.operation} operation: {str(e)}"
            logging.error(error_msg)
            logging.error(traceback.format_exc())
            raise AirflowException(error_msg)
    
    async def _execute_async(self, context: Dict[str, Any]) -> Any:
        """
        Execute the connector operation asynchronously.
        
        Args:
            context: Airflow context
            
        Returns:
            Any: Operation result
        """
        try:
            # Import connector class dynamically
            connector_class = self._get_connector_class()
            
            # Create connector instance
            connector = connector_class(self.connector_config)
            
            # Execute operation
            if self.operation == 'extract':
                result = await connector.extract()
            elif self.operation == 'transform':
                # For transform, we need extraction result
                extraction_result = self._get_extraction_result(context)
                result = await connector.transform(extraction_result)
            elif self.operation == 'load':
                # For load, we need transformation result
                transformation_result = self._get_transformation_result(context)
                result = await connector.load(transformation_result)
            
            # Store result in XCom for downstream tasks
            context['task_instance'].xcom_push(
                key=f'{self.operation}_result',
                value=result
            )
            
            return result
            
        except Exception as e:
            logging.error(f"Async execution failed: {e}")
            raise
    
    def _get_connector_class(self):
        """Get connector class dynamically."""
        try:
            market = self.connector_config.get('market', '').lower()
            
            if market == 'miso':
                from connectors.miso.connector import MISOConnector
                return MISOConnector
            elif market == 'caiso':
                from connectors.caiso.connector import CAISOConnector
                return CAISOConnector
            else:
                raise ValueError(f"Unknown market: {market}")
                
        except Exception as e:
            logging.error(f"Failed to get connector class: {e}")
            raise
    
    def _get_extraction_result(self, context: Dict[str, Any]) -> Any:
        """Get extraction result from XCom."""
        try:
            # Look for extraction result in XCom
            extraction_result = context['task_instance'].xcom_pull(
                task_ids=f"extract_{self.connector_config.get('name', 'unknown')}",
                key='extract_result'
            )
            
            if not extraction_result:
                raise ValueError("No extraction result found in XCom")
            
            return extraction_result
            
        except Exception as e:
            logging.error(f"Failed to get extraction result: {e}")
            raise
    
    def _get_transformation_result(self, context: Dict[str, Any]) -> Any:
        """Get transformation result from XCom."""
        try:
            # Look for transformation result in XCom
            transformation_result = context['task_instance'].xcom_pull(
                task_ids=f"transform_{self.connector_config.get('name', 'unknown')}",
                key='transform_result'
            )
            
            if not transformation_result:
                raise ValueError("No transformation result found in XCom")
            
            return transformation_result
            
        except Exception as e:
            logging.error(f"Failed to get transformation result: {e}")
            raise
    
    def on_kill(self) -> None:
        """Handle task kill."""
        logging.info(f"Connector operation {self.operation} was killed")
        super().on_kill()