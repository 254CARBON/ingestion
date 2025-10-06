"""
Quality Check Sensor for Airflow.

This sensor provides data quality checks for connector operations
within Airflow DAGs.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class QualityCheckSensor(BaseSensorOperator):
    """
    Airflow sensor for data quality checks.
    
    This sensor provides data quality checks for connector operations
    within Airflow DAGs.
    """
    
    @apply_defaults
    def __init__(
        self,
        connector_config: Dict[str, Any],
        quality_threshold: float = 0.8,
        **kwargs
    ):
        """
        Initialize the quality check sensor.
        
        Args:
            connector_config: Connector configuration
            quality_threshold: Minimum quality threshold
            **kwargs: Additional sensor arguments
        """
        super().__init__(**kwargs)
        self.connector_config = connector_config
        self.quality_threshold = quality_threshold
        
        # Set task ID if not provided
        if not self.task_id:
            self.task_id = f"quality_check_{connector_config.get('name', 'unknown')}"
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """
        Check if quality conditions are met.
        
        Args:
            context: Airflow context
            
        Returns:
            bool: True if quality check passes, False otherwise
        """
        try:
            logging.info(f"Performing quality check for connector: {self.connector_config.get('name')}")
            
            # Run async quality check
            result = asyncio.run(self._check_quality_async(context))
            
            if result:
                logging.info(f"Quality check passed for connector: {self.connector_config.get('name')}")
            else:
                logging.warning(f"Quality check failed for connector: {self.connector_config.get('name')}")
            
            return result
            
        except Exception as e:
            logging.error(f"Quality check failed: {e}")
            return False
    
    async def _check_quality_async(self, context: Dict[str, Any]) -> bool:
        """
        Perform quality check asynchronously.
        
        Args:
            context: Airflow context
            
        Returns:
            bool: True if quality check passes, False otherwise
        """
        try:
            # Get connector results from XCom
            extraction_result = self._get_extraction_result(context)
            transformation_result = self._get_transformation_result(context)
            load_result = self._get_load_result(context)
            
            # Perform quality checks
            quality_score = await self._calculate_quality_score(
                extraction_result, transformation_result, load_result
            )
            
            # Store quality score in XCom
            context['task_instance'].xcom_push(
                key='quality_score',
                value=quality_score
            )
            
            # Check if quality meets threshold
            return quality_score >= self.quality_threshold
            
        except Exception as e:
            logging.error(f"Async quality check failed: {e}")
            return False
    
    def _get_extraction_result(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get extraction result from XCom."""
        try:
            return context['task_instance'].xcom_pull(
                task_ids=f"extract_{self.connector_config.get('name', 'unknown')}",
                key='extract_result'
            )
        except Exception:
            return None
    
    def _get_transformation_result(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get transformation result from XCom."""
        try:
            return context['task_instance'].xcom_pull(
                task_ids=f"transform_{self.connector_config.get('name', 'unknown')}",
                key='transform_result'
            )
        except Exception:
            return None
    
    def _get_load_result(self, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get load result from XCom."""
        try:
            return context['task_instance'].xcom_pull(
                task_ids=f"load_{self.connector_config.get('name', 'unknown')}",
                key='load_result'
            )
        except Exception:
            return None
    
    async def _calculate_quality_score(
        self,
        extraction_result: Optional[Dict[str, Any]],
        transformation_result: Optional[Dict[str, Any]],
        load_result: Optional[Dict[str, Any]]
    ) -> float:
        """
        Calculate overall quality score.
        
        Args:
            extraction_result: Extraction result
            transformation_result: Transformation result
            load_result: Load result
            
        Returns:
            float: Quality score between 0.0 and 1.0
        """
        try:
            score = 1.0
            
            # Check extraction quality
            if extraction_result:
                extraction_score = self._check_extraction_quality(extraction_result)
                score *= extraction_score
            else:
                score *= 0.0
            
            # Check transformation quality
            if transformation_result:
                transformation_score = self._check_transformation_quality(transformation_result)
                score *= transformation_score
            else:
                score *= 0.0
            
            # Check load quality
            if load_result:
                load_score = self._check_load_quality(load_result)
                score *= load_score
            else:
                score *= 0.0
            
            return max(0.0, min(1.0, score))
            
        except Exception as e:
            logging.error(f"Failed to calculate quality score: {e}")
            return 0.0
    
    def _check_extraction_quality(self, extraction_result: Dict[str, Any]) -> float:
        """Check extraction quality."""
        try:
            score = 1.0
            
            # Check record count
            record_count = extraction_result.get('record_count', 0)
            if record_count == 0:
                score *= 0.0
            elif record_count < 10:
                score *= 0.5
            elif record_count < 100:
                score *= 0.8
            
            # Check metadata completeness
            metadata = extraction_result.get('metadata', {})
            if not metadata:
                score *= 0.5
            
            # Check data types
            data_types = metadata.get('data_types', [])
            if not data_types:
                score *= 0.7
            
            return score
            
        except Exception as e:
            logging.error(f"Failed to check extraction quality: {e}")
            return 0.0
    
    def _check_transformation_quality(self, transformation_result: Dict[str, Any]) -> float:
        """Check transformation quality."""
        try:
            score = 1.0
            
            # Check record count
            record_count = transformation_result.get('record_count', 0)
            if record_count == 0:
                score *= 0.0
            elif record_count < 10:
                score *= 0.5
            elif record_count < 100:
                score *= 0.8
            
            # Check validation errors
            validation_errors = transformation_result.get('validation_errors', [])
            if validation_errors:
                error_count = len(validation_errors)
                if error_count > 10:
                    score *= 0.3
                elif error_count > 5:
                    score *= 0.6
                elif error_count > 0:
                    score *= 0.8
            
            # Check success rate
            success_rate = transformation_result.get('metadata', {}).get('success_rate', 1.0)
            score *= success_rate
            
            return score
            
        except Exception as e:
            logging.error(f"Failed to check transformation quality: {e}")
            return 0.0
    
    def _check_load_quality(self, load_result: Dict[str, Any]) -> float:
        """Check load quality."""
        try:
            score = 1.0
            
            # Check if load was successful
            if not load_result:
                score *= 0.0
            
            # Check for errors
            if isinstance(load_result, dict) and 'error' in load_result:
                score *= 0.0
            
            return score
            
        except Exception as e:
            logging.error(f"Failed to check load quality: {e}")
            return 0.0
    
    def on_kill(self) -> None:
        """Handle sensor kill."""
        logging.info(f"Quality check sensor for connector {self.connector_config.get('name')} was killed")
        super().on_kill()