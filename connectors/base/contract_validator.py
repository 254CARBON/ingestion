"""
Contract validation utilities for the connector framework.

This module provides utilities for validating data contracts, schemas,
and ensuring compatibility between different versions of data formats.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

from .exceptions import SchemaError, ValidationError
from .utils import setup_logging


class ContractValidator:
    """
    Validates data contracts and schemas.
    
    This class provides functionality to validate data against Avro schemas,
    check schema compatibility, and ensure data integrity.
    """
    
    def __init__(self, schema_dir: Optional[str] = None):
        """
        Initialize the contract validator.
        
        Args:
            schema_dir: Directory containing schema files
        """
        self.schema_dir = Path(schema_dir) if schema_dir else Path("schemas")
        self.logger = setup_logging(self.__class__.__name__)
        self._schemas: Dict[str, avro.schema.Schema] = {}
    
    def load_schema(self, schema_name: str) -> avro.schema.Schema:
        """
        Load an Avro schema from file.
        
        Args:
            schema_name: Name of the schema file (without extension)
            
        Returns:
            avro.schema.Schema: Loaded Avro schema
            
        Raises:
            SchemaError: If schema cannot be loaded
        """
        if schema_name in self._schemas:
            return self._schemas[schema_name]
        
        schema_path = self.schema_dir / f"{schema_name}.avsc"
        
        if not schema_path.exists():
            raise SchemaError(f"Schema file not found: {schema_path}")
        
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_json = json.load(f)
            
            schema = avro.schema.parse(json.dumps(schema_json))
            self._schemas[schema_name] = schema
            return schema
            
        except Exception as e:
            raise SchemaError(f"Failed to load schema {schema_name}: {e}") from e
    
    def validate_record(self, record: Dict[str, Any], schema_name: str) -> bool:
        """
        Validate a record against a schema.
        
        Args:
            record: Record to validate
            schema_name: Name of the schema to validate against
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            schema = self.load_schema(schema_name)
            avro.io.Validate(schema, record)
            return True
        except Exception as e:
            self.logger.warning(f"Record validation failed: {e}")
            return False
    
    def validate_batch(self, records: List[Dict[str, Any]], schema_name: str) -> List[str]:
        """
        Validate a batch of records against a schema.
        
        Args:
            records: List of records to validate
            schema_name: Name of the schema to validate against
            
        Returns:
            List[str]: List of validation error messages
        """
        errors = []
        schema = self.load_schema(schema_name)
        
        for i, record in enumerate(records):
            try:
                avro.io.Validate(schema, record)
            except Exception as e:
                errors.append(f"Record {i}: {e}")
        
        return errors
    
    def check_schema_compatibility(
        self,
        old_schema_name: str,
        new_schema_name: str
    ) -> Dict[str, Any]:
        """
        Check compatibility between two schema versions.
        
        Args:
            old_schema_name: Name of the old schema
            new_schema_name: Name of the new schema
            
        Returns:
            Dict[str, Any]: Compatibility report
        """
        try:
            old_schema = self.load_schema(old_schema_name)
            new_schema = self.load_schema(new_schema_name)
            
            # Basic compatibility checks
            compatibility = {
                "compatible": True,
                "errors": [],
                "warnings": [],
                "changes": []
            }
            
            # Check field additions/removals
            old_fields = {field.name for field in old_schema.fields}
            new_fields = {field.name for field in new_schema.fields}
            
            added_fields = new_fields - old_fields
            removed_fields = old_fields - new_fields
            
            if removed_fields:
                compatibility["compatible"] = False
                compatibility["errors"].append(f"Removed fields: {removed_fields}")
            
            if added_fields:
                compatibility["changes"].append(f"Added fields: {added_fields}")
            
            # Check field type changes
            for field in new_schema.fields:
                if field.name in old_fields:
                    old_field = next(f for f in old_schema.fields if f.name == field.name)
                    if old_field.type != field.type:
                        compatibility["compatible"] = False
                        compatibility["errors"].append(
                            f"Field {field.name} type changed from {old_field.type} to {field.type}"
                        )
            
            return compatibility
            
        except Exception as e:
            return {
                "compatible": False,
                "errors": [f"Schema compatibility check failed: {e}"],
                "warnings": [],
                "changes": []
            }
    
    def get_schema_info(self, schema_name: str) -> Dict[str, Any]:
        """
        Get information about a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            Dict[str, Any]: Schema information
        """
        try:
            schema = self.load_schema(schema_name)
            
            return {
                "name": schema_name,
                "type": schema.type,
                "fields": [
                    {
                        "name": field.name,
                        "type": str(field.type),
                        "default": field.default,
                        "doc": field.doc
                    }
                    for field in schema.fields
                ],
                "doc": schema.doc,
                "namespace": schema.namespace
            }
            
        except Exception as e:
            return {
                "name": schema_name,
                "error": str(e)
            }
    
    def list_schemas(self) -> List[str]:
        """
        List all available schemas.
        
        Returns:
            List[str]: List of schema names
        """
        if not self.schema_dir.exists():
            return []
        
        return [
            f.stem for f in self.schema_dir.glob("*.avsc")
        ]
    
    def validate_schema_file(self, schema_path: str) -> Dict[str, Any]:
        """
        Validate a schema file.
        
        Args:
            schema_path: Path to the schema file
            
        Returns:
            Dict[str, Any]: Validation result
        """
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_json = json.load(f)
            
            schema = avro.schema.parse(json.dumps(schema_json))
            
            return {
                "valid": True,
                "schema_type": schema.type,
                "fields": len(schema.fields),
                "namespace": schema.namespace
            }
            
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }
    
    def generate_sample_record(self, schema_name: str) -> Dict[str, Any]:
        """
        Generate a sample record based on a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            Dict[str, Any]: Sample record
        """
        try:
            schema = self.load_schema(schema_name)
            sample = {}
            
            for field in schema.fields:
                if field.default is not None:
                    sample[field.name] = field.default
                elif field.type == "string":
                    sample[field.name] = f"sample_{field.name}"
                elif field.type == "int":
                    sample[field.name] = 0
                elif field.type == "long":
                    sample[field.name] = 0
                elif field.type == "float":
                    sample[field.name] = 0.0
                elif field.type == "double":
                    sample[field.name] = 0.0
                elif field.type == "boolean":
                    sample[field.name] = False
                elif field.type == "null":
                    sample[field.name] = None
                else:
                    sample[field.name] = None
            
            return sample
            
        except Exception as e:
            self.logger.error(f"Failed to generate sample record: {e}")
            return {}
