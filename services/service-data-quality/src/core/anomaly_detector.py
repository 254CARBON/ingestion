"""
Anomaly detection for data quality monitoring.

This module provides anomaly detection algorithms for identifying
data quality issues in market data streams.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import deque
from uuid import uuid4

import structlog
import yaml
import numpy as np
from pydantic import BaseModel, Field


class AnomalyType:
    """Anomaly type constants."""
    PRICE_SPIKE = "price_spike"
    VOLUME_ANOMALY = "volume_anomaly"
    MISSING_DATA = "missing_data"
    SCHEMA_DRIFT = "schema_drift"
    VALIDATION_ERROR = "validation_error"
    OUTLIER = "outlier"
    DUPLICATE = "duplicate"


class Anomaly(BaseModel):
    """Detected anomaly."""

    anomaly_id: str = Field(..., description="Unique anomaly identifier")
    anomaly_type: str = Field(..., description="Type of anomaly")
    severity: str = Field(..., description="Severity level (low, medium, high, critical)")
    market: str = Field(..., description="Market where anomaly was detected")
    data_type: str = Field(..., description="Type of data")
    field_name: Optional[str] = Field(None, description="Field where anomaly was detected")
    expected_value: Optional[Any] = Field(None, description="Expected value")
    actual_value: Optional[Any] = Field(None, description="Actual value")
    deviation: Optional[float] = Field(None, description="Deviation from expected")
    description: str = Field(..., description="Anomaly description")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    detected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AnomalyDetectorConfig(BaseModel):
    """Configuration for anomaly detector."""

    price_spike_threshold: float = Field(3.0, description="Standard deviations for price spike")
    volume_spike_threshold: float = Field(3.0, description="Standard deviations for volume spike")
    missing_data_window_minutes: int = Field(30, description="Window for missing data detection")
    outlier_threshold: float = Field(2.5, description="Standard deviations for outlier detection")
    duplicate_window_seconds: int = Field(60, description="Window for duplicate detection")
    min_samples_for_detection: int = Field(10, description="Minimum samples before detection")


class AnomalyDetector:
    """
    Anomaly detector for data quality monitoring.

    Detects various types of anomalies in market data streams including
    price spikes, volume anomalies, missing data, and schema drift.
    """

    def __init__(self, config_path: str = "configs/data_quality_rules.yaml"):
        """
        Initialize anomaly detector.

        Args:
            config_path: Path to data quality configuration
        """
        self.config_path = config_path
        self.logger = structlog.get_logger(__name__)

        # Load configuration
        self.config = self._load_config()
        self.detector_config = AnomalyDetectorConfig(**self.config.get("anomaly_detection", {}))

        # Historical data buffers for statistical analysis
        self.price_buffers: Dict[str, deque] = {}
        self.volume_buffers: Dict[str, deque] = {}
        self.last_seen: Dict[str, datetime] = {}
        self.seen_records: Dict[str, set] = {}

        # Detection statistics
        self.stats = {
            "total_records_analyzed": 0,
            "anomalies_detected": 0,
            "anomalies_by_type": {},
            "anomalies_by_market": {},
            "anomalies_by_severity": {}
        }

        self.logger.info("Anomaly detector initialized", config_path=config_path)

    def _load_config(self) -> Dict[str, Any]:
        """Load data quality configuration."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info("Data quality configuration loaded", config_path=self.config_path)
            return config
        except Exception as e:
            self.logger.error("Failed to load data quality configuration",
                            error=str(e), config_path=self.config_path)
            return {}

    async def detect_anomalies(self, data: Dict[str, Any]) -> List[Anomaly]:
        """
        Detect anomalies in data.

        Args:
            data: Data to analyze

        Returns:
            List[Anomaly]: Detected anomalies
        """
        try:
            self.stats["total_records_analyzed"] += 1

            anomalies = []
            market = data.get("market", "UNKNOWN")
            data_type = data.get("data_type", "unknown")

            # Price spike detection
            price_anomalies = await self._detect_price_spikes(data, market)
            anomalies.extend(price_anomalies)

            # Volume anomaly detection
            volume_anomalies = await self._detect_volume_anomalies(data, market)
            anomalies.extend(volume_anomalies)

            # Missing data detection
            missing_anomalies = await self._detect_missing_data(data, market)
            anomalies.extend(missing_anomalies)

            # Schema drift detection
            schema_anomalies = await self._detect_schema_drift(data, market, data_type)
            anomalies.extend(schema_anomalies)

            # Duplicate detection
            duplicate_anomalies = await self._detect_duplicates(data, market)
            anomalies.extend(duplicate_anomalies)

            # Validation error detection
            validation_anomalies = await self._detect_validation_errors(data)
            anomalies.extend(validation_anomalies)

            # Update statistics
            for anomaly in anomalies:
                self.stats["anomalies_detected"] += 1
                self.stats["anomalies_by_type"][anomaly.anomaly_type] = (
                    self.stats["anomalies_by_type"].get(anomaly.anomaly_type, 0) + 1
                )
                self.stats["anomalies_by_market"][anomaly.market] = (
                    self.stats["anomalies_by_market"].get(anomaly.market, 0) + 1
                )
                self.stats["anomalies_by_severity"][anomaly.severity] = (
                    self.stats["anomalies_by_severity"].get(anomaly.severity, 0) + 1
                )

            if anomalies:
                self.logger.info("Anomalies detected",
                               count=len(anomalies),
                               market=market,
                               data_type=data_type)

            return anomalies

        except Exception as e:
            self.logger.error("Failed to detect anomalies", error=str(e))
            return []

    async def _detect_price_spikes(self, data: Dict[str, Any], market: str) -> List[Anomaly]:
        """Detect price spikes using statistical analysis."""
        anomalies = []

        try:
            price = data.get("price")
            if price is None:
                return anomalies

            price = float(price)
            buffer_key = f"{market}_{data.get('delivery_location', 'unknown')}"

            # Initialize buffer if needed
            if buffer_key not in self.price_buffers:
                self.price_buffers[buffer_key] = deque(maxlen=100)

            buffer = self.price_buffers[buffer_key]

            # Need minimum samples for statistical detection
            if len(buffer) >= self.detector_config.min_samples_for_detection:
                prices = list(buffer)
                mean_price = np.mean(prices)
                std_price = np.std(prices)

                if std_price > 0:
                    z_score = abs((price - mean_price) / std_price)

                    if z_score > self.detector_config.price_spike_threshold:
                        severity = "critical" if z_score > 5.0 else "high" if z_score > 4.0 else "medium"

                        anomalies.append(Anomaly(
                            anomaly_id=str(uuid4()),
                            anomaly_type=AnomalyType.PRICE_SPIKE,
                            severity=severity,
                            market=market,
                            data_type=data.get("data_type", "unknown"),
                            field_name="price",
                            expected_value=mean_price,
                            actual_value=price,
                            deviation=z_score,
                            description=f"Price spike detected: {price} deviates {z_score:.2f} std from mean {mean_price:.2f}",
                            metadata={
                                "mean": mean_price,
                                "std": std_price,
                                "z_score": z_score,
                                "sample_size": len(buffer)
                            }
                        ))

            # Add current price to buffer
            buffer.append(price)

        except Exception as e:
            self.logger.error("Failed to detect price spikes", error=str(e))

        return anomalies

    async def _detect_volume_anomalies(self, data: Dict[str, Any], market: str) -> List[Anomaly]:
        """Detect volume anomalies using statistical analysis."""
        anomalies = []

        try:
            quantity = data.get("quantity")
            if quantity is None:
                return anomalies

            quantity = float(quantity)
            buffer_key = f"{market}_{data.get('delivery_location', 'unknown')}"

            # Initialize buffer if needed
            if buffer_key not in self.volume_buffers:
                self.volume_buffers[buffer_key] = deque(maxlen=100)

            buffer = self.volume_buffers[buffer_key]

            # Need minimum samples for statistical detection
            if len(buffer) >= self.detector_config.min_samples_for_detection:
                volumes = list(buffer)
                mean_volume = np.mean(volumes)
                std_volume = np.std(volumes)

                if std_volume > 0:
                    z_score = abs((quantity - mean_volume) / std_volume)

                    if z_score > self.detector_config.volume_spike_threshold:
                        severity = "high" if z_score > 4.0 else "medium"

                        anomalies.append(Anomaly(
                            anomaly_id=str(uuid4()),
                            anomaly_type=AnomalyType.VOLUME_ANOMALY,
                            severity=severity,
                            market=market,
                            data_type=data.get("data_type", "unknown"),
                            field_name="quantity",
                            expected_value=mean_volume,
                            actual_value=quantity,
                            deviation=z_score,
                            description=f"Volume anomaly detected: {quantity} deviates {z_score:.2f} std from mean {mean_volume:.2f}",
                            metadata={
                                "mean": mean_volume,
                                "std": std_volume,
                                "z_score": z_score,
                                "sample_size": len(buffer)
                            }
                        ))

            # Add current volume to buffer
            buffer.append(quantity)

        except Exception as e:
            self.logger.error("Failed to detect volume anomalies", error=str(e))

        return anomalies

    async def _detect_missing_data(self, data: Dict[str, Any], market: str) -> List[Anomaly]:
        """Detect missing data patterns."""
        anomalies = []

        try:
            location_key = f"{market}_{data.get('delivery_location', 'unknown')}"
            current_time = datetime.now(timezone.utc)

            # Check if we've seen data recently
            if location_key in self.last_seen:
                last_seen_time = self.last_seen[location_key]
                time_diff = (current_time - last_seen_time).total_seconds() / 60  # minutes

                if time_diff > self.detector_config.missing_data_window_minutes:
                    anomalies.append(Anomaly(
                        anomaly_id=str(uuid4()),
                        anomaly_type=AnomalyType.MISSING_DATA,
                        severity="medium",
                        market=market,
                        data_type=data.get("data_type", "unknown"),
                        field_name=None,
                        expected_value=None,
                        actual_value=None,
                        deviation=time_diff,
                        description=f"Missing data detected: no data for {time_diff:.1f} minutes",
                        metadata={
                            "last_seen": last_seen_time.isoformat(),
                            "gap_minutes": time_diff
                        }
                    ))

            # Update last seen time
            self.last_seen[location_key] = current_time

        except Exception as e:
            self.logger.error("Failed to detect missing data", error=str(e))

        return anomalies

    async def _detect_schema_drift(self, data: Dict[str, Any], market: str, data_type: str) -> List[Anomaly]:
        """Detect schema drift or unexpected fields."""
        anomalies = []

        try:
            # Define expected schemas for each data type
            expected_schemas = {
                "market_price": ["market", "delivery_location", "delivery_date", "delivery_hour", "price"],
                "trade": ["market", "delivery_location", "delivery_date", "delivery_hour", "price", "quantity"],
                "curve": ["market", "curve_type", "delivery_date", "delivery_hour", "price"]
            }

            expected_fields = expected_schemas.get(data_type, [])
            actual_fields = set(data.keys())

            # Check for missing expected fields
            missing_fields = [f for f in expected_fields if f not in actual_fields]

            if missing_fields:
                anomalies.append(Anomaly(
                    anomaly_id=str(uuid4()),
                    anomaly_type=AnomalyType.SCHEMA_DRIFT,
                    severity="high",
                    market=market,
                    data_type=data_type,
                    field_name=",".join(missing_fields),
                    expected_value=expected_fields,
                    actual_value=list(actual_fields),
                    deviation=len(missing_fields),
                    description=f"Schema drift detected: missing fields {missing_fields}",
                    metadata={
                        "missing_fields": missing_fields,
                        "expected_fields": expected_fields
                    }
                ))

        except Exception as e:
            self.logger.error("Failed to detect schema drift", error=str(e))

        return anomalies

    async def _detect_duplicates(self, data: Dict[str, Any], market: str) -> List[Anomaly]:
        """Detect duplicate records."""
        anomalies = []

        try:
            event_id = data.get("event_id")
            if not event_id:
                return anomalies

            market_key = f"{market}_{data.get('data_type', 'unknown')}"

            # Initialize seen records set if needed
            if market_key not in self.seen_records:
                self.seen_records[market_key] = set()

            # Check for duplicate
            if event_id in self.seen_records[market_key]:
                anomalies.append(Anomaly(
                    anomaly_id=str(uuid4()),
                    anomaly_type=AnomalyType.DUPLICATE,
                    severity="low",
                    market=market,
                    data_type=data.get("data_type", "unknown"),
                    field_name="event_id",
                    expected_value=None,
                    actual_value=event_id,
                    deviation=None,
                    description=f"Duplicate record detected: event_id {event_id}",
                    metadata={"event_id": event_id}
                ))

            # Add to seen records (limit size to prevent memory issues)
            if len(self.seen_records[market_key]) > 10000:
                # Remove oldest half
                self.seen_records[market_key] = set(list(self.seen_records[market_key])[5000:])

            self.seen_records[market_key].add(event_id)

        except Exception as e:
            self.logger.error("Failed to detect duplicates", error=str(e))

        return anomalies

    async def _detect_validation_errors(self, data: Dict[str, Any]) -> List[Anomaly]:
        """Detect validation errors in data."""
        anomalies = []

        try:
            # Check for validation error markers
            validation_errors = data.get("_validation_errors", [])

            if validation_errors:
                for error in validation_errors:
                    anomalies.append(Anomaly(
                        anomaly_id=str(uuid4()),
                        anomaly_type=AnomalyType.VALIDATION_ERROR,
                        severity="medium",
                        market=data.get("market", "UNKNOWN"),
                        data_type=data.get("data_type", "unknown"),
                        field_name=None,
                        expected_value=None,
                        actual_value=None,
                        deviation=None,
                        description=f"Validation error: {error}",
                        metadata={"validation_error": error}
                    ))

            # Check quality score if present
            quality_score = data.get("quality_score")
            if quality_score is not None and float(quality_score) < 0.5:
                anomalies.append(Anomaly(
                    anomaly_id=str(uuid4()),
                    anomaly_type=AnomalyType.VALIDATION_ERROR,
                    severity="high",
                    market=data.get("market", "UNKNOWN"),
                    data_type=data.get("data_type", "unknown"),
                    field_name="quality_score",
                    expected_value=0.9,
                    actual_value=quality_score,
                    deviation=0.9 - quality_score,
                    description=f"Low quality score detected: {quality_score}",
                    metadata={"quality_score": quality_score}
                ))

        except Exception as e:
            self.logger.error("Failed to detect validation errors", error=str(e))

        return anomalies

    async def detect_outliers(self, data: Dict[str, Any], field: str) -> Optional[Anomaly]:
        """
        Detect outliers in a specific field using IQR method.

        Args:
            data: Data to analyze
            field: Field to check for outliers

        Returns:
            Optional[Anomaly]: Detected outlier anomaly or None
        """
        try:
            value = data.get(field)
            if value is None:
                return None

            value = float(value)
            market = data.get("market", "UNKNOWN")
            buffer_key = f"{market}_{field}"

            # Get historical data for this field
            if buffer_key in self.price_buffers:  # Reuse price buffer structure
                buffer = list(self.price_buffers[buffer_key])

                if len(buffer) >= self.detector_config.min_samples_for_detection:
                    q1 = np.percentile(buffer, 25)
                    q3 = np.percentile(buffer, 75)
                    iqr = q3 - q1

                    lower_bound = q1 - (1.5 * iqr)
                    upper_bound = q3 + (1.5 * iqr)

                    if value < lower_bound or value > upper_bound:
                        return Anomaly(
                            anomaly_id=str(uuid4()),
                            anomaly_type=AnomalyType.OUTLIER,
                            severity="medium",
                            market=market,
                            data_type=data.get("data_type", "unknown"),
                            field_name=field,
                            expected_value=f"[{lower_bound:.2f}, {upper_bound:.2f}]",
                            actual_value=value,
                            deviation=None,
                            description=f"Outlier detected in {field}: {value} outside bounds [{lower_bound:.2f}, {upper_bound:.2f}]",
                            metadata={
                                "q1": q1,
                                "q3": q3,
                                "iqr": iqr,
                                "lower_bound": lower_bound,
                                "upper_bound": upper_bound
                            }
                        )

        except Exception as e:
            self.logger.error("Failed to detect outliers", field=field, error=str(e))

        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get anomaly detector statistics."""
        return {
            **self.stats,
            "buffer_sizes": {
                "price_buffers": len(self.price_buffers),
                "volume_buffers": len(self.volume_buffers),
                "seen_records": sum(len(s) for s in self.seen_records.values())
            },
            "anomaly_rate": (
                self.stats["anomalies_detected"] / max(1, self.stats["total_records_analyzed"])
            )
        }

    def reset_stats(self) -> None:
        """Reset anomaly detector statistics."""
        self.stats = {
            "total_records_analyzed": 0,
            "anomalies_detected": 0,
            "anomalies_by_type": {},
            "anomalies_by_market": {},
            "anomalies_by_severity": {}
        }
