#!/usr/bin/env python3
"""
Schema Validation Script.

This script validates Avro schemas for compatibility and correctness.
"""

import json
import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import avro.schema


def setup_logging() -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('schema_validation.log')
        ]
    )


def validate_avro_schema(schema_path: str) -> bool:
    """
    Validate Avro schema file.
    
    Args:
        schema_path: Path to Avro schema file
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        with open(schema_path, 'r') as f:
            schema_data = json.load(f)
        
        # Parse schema
        schema = avro.schema.parse(json.dumps(schema_data))
        
        # Basic validation
        if not schema.name:
            logging.error(f"Schema missing name: {schema_path}")
            return False
        
        if not schema.fields:
            logging.error(f"Schema missing fields: {schema_path}")
            return False
        
        # Validate field types
        for field in schema.fields:
            if not field.type:
                logging.error(f"Field '{field.name}' missing type in schema: {schema_path}")
                return False
        
        logging.info(f"Schema validation passed: {schema_path}")
        return True
        
    except Exception as e:
        logging.error(f"Schema validation failed for {schema_path}: {e}")
        return False


def validate_schema_compatibility(schema1_path: str, schema2_path: str) -> bool:
    """
    Validate schema compatibility between two schemas.
    
    Args:
        schema1_path: Path to first schema
        schema2_path: Path to second schema
        
    Returns:
        bool: True if compatible, False otherwise
    """
    try:
        with open(schema1_path, 'r') as f:
            schema1_data = json.load(f)
        
        with open(schema2_path, 'r') as f:
            schema2_data = json.load(f)
        
        schema1 = avro.schema.parse(json.dumps(schema1_data))
        schema2 = avro.schema.parse(json.dumps(schema2_data))
        
        # Check if schemas are compatible
        # This is a simplified check - in production, use proper schema registry
        if schema1.name != schema2.name:
            logging.error(f"Schema names don't match: {schema1.name} vs {schema2.name}")
            return False
        
        # Check field compatibility
        schema1_fields = {f.name: f.type for f in schema1.fields}
        schema2_fields = {f.name: f.type for f in schema2.fields}
        
        # Check for removed fields
        removed_fields = set(schema1_fields.keys()) - set(schema2_fields.keys())
        if removed_fields:
            logging.error(f"Fields removed in {schema2_path}: {removed_fields}")
            return False
        
        # Check for type changes
        for field_name in schema1_fields:
            if field_name in schema2_fields:
                if schema1_fields[field_name] != schema2_fields[field_name]:
                    logging.error(f"Field type changed for '{field_name}': {schema1_fields[field_name]} -> {schema2_fields[field_name]}")
                    return False
        
        logging.info(f"Schema compatibility check passed: {schema1_path} vs {schema2_path}")
        return True
        
    except Exception as e:
        logging.error(f"Schema compatibility check failed: {e}")
        return False


def scan_schema_files(schemas_dir: str) -> List[str]:
    """
    Scan directory for Avro schema files.
    
    Args:
        schemas_dir: Path to schemas directory
        
    Returns:
        List[str]: List of schema file paths
    """
    schema_files = []
    
    if not os.path.exists(schemas_dir):
        logging.error(f"Schemas directory not found: {schemas_dir}")
        return schema_files
    
    # Scan for .avsc files
    for root, dirs, files in os.walk(schemas_dir):
        for file in files:
            if file.endswith('.avsc'):
                schema_path = os.path.join(root, file)
                schema_files.append(schema_path)
    
    return schema_files


def validate_all_schemas(schemas_dir: str) -> Dict[str, Any]:
    """
    Validate all schemas in directory.
    
    Args:
        schemas_dir: Path to schemas directory
        
    Returns:
        Dict[str, Any]: Validation results
    """
    results = {
        'total_schemas': 0,
        'valid_schemas': 0,
        'invalid_schemas': 0,
        'schema_details': [],
        'errors': []
    }
    
    # Scan for schema files
    schema_files = scan_schema_files(schemas_dir)
    results['total_schemas'] = len(schema_files)
    
    if not schema_files:
        logging.warning("No schema files found")
        return results
    
    # Validate each schema
    for schema_path in schema_files:
        try:
            is_valid = validate_avro_schema(schema_path)
            
            schema_detail = {
                'path': schema_path,
                'valid': is_valid,
                'errors': []
            }
            
            if is_valid:
                results['valid_schemas'] += 1
            else:
                results['invalid_schemas'] += 1
                results['errors'].append(f"Invalid schema: {schema_path}")
            
            results['schema_details'].append(schema_detail)
            
        except Exception as e:
            results['invalid_schemas'] += 1
            results['errors'].append(f"Error validating {schema_path}: {e}")
            logging.error(f"Error validating schema {schema_path}: {e}")
    
    return results


def check_schema_evolution(schemas_dir: str) -> Dict[str, Any]:
    """
    Check schema evolution compatibility.
    
    Args:
        schemas_dir: Path to schemas directory
        
    Returns:
        Dict[str, Any]: Evolution check results
    """
    results = {
        'compatibility_checks': 0,
        'compatible_schemas': 0,
        'incompatible_schemas': 0,
        'compatibility_details': [],
        'errors': []
    }
    
    # Scan for schema files
    schema_files = scan_schema_files(schemas_dir)
    
    if len(schema_files) < 2:
        logging.info("Not enough schemas for compatibility check")
        return results
    
    # Check compatibility between all pairs
    for i, schema1_path in enumerate(schema_files):
        for schema2_path in schema_files[i+1:]:
            try:
                is_compatible = validate_schema_compatibility(schema1_path, schema2_path)
                
                compatibility_detail = {
                    'schema1': schema1_path,
                    'schema2': schema2_path,
                    'compatible': is_compatible
                }
                
                results['compatibility_checks'] += 1
                
                if is_compatible:
                    results['compatible_schemas'] += 1
                else:
                    results['incompatible_schemas'] += 1
                    results['errors'].append(f"Incompatible schemas: {schema1_path} vs {schema2_path}")
                
                results['compatibility_details'].append(compatibility_detail)
                
            except Exception as e:
                results['errors'].append(f"Error checking compatibility: {e}")
                logging.error(f"Error checking compatibility: {e}")
    
    return results


def generate_validation_report(validation_results: Dict[str, Any], evolution_results: Dict[str, Any], output_path: str) -> None:
    """
    Generate validation report.
    
    Args:
        validation_results: Schema validation results
        evolution_results: Schema evolution results
        output_path: Path to output report file
    """
    try:
        report = {
            'generated_at': datetime.now().isoformat(),
            'generator': 'validate_schemas.py',
            'version': '1.0.0',
            'validation_results': validation_results,
            'evolution_results': evolution_results,
            'summary': {
                'total_schemas': validation_results['total_schemas'],
                'valid_schemas': validation_results['valid_schemas'],
                'invalid_schemas': validation_results['invalid_schemas'],
                'compatibility_checks': evolution_results['compatibility_checks'],
                'compatible_schemas': evolution_results['compatible_schemas'],
                'incompatible_schemas': evolution_results['incompatible_schemas']
            }
        }
        
        # Write report file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logging.info(f"Validation report saved to: {output_path}")
        
        # Print summary
        print(f"\nSchema Validation Summary:")
        print(f"Total schemas: {report['summary']['total_schemas']}")
        print(f"Valid schemas: {report['summary']['valid_schemas']}")
        print(f"Invalid schemas: {report['summary']['invalid_schemas']}")
        print(f"Compatibility checks: {report['summary']['compatibility_checks']}")
        print(f"Compatible schemas: {report['summary']['compatible_schemas']}")
        print(f"Incompatible schemas: {report['summary']['incompatible_schemas']}")
        
    except Exception as e:
        logging.error(f"Failed to generate validation report: {e}")
        raise


def main():
    """Main function."""
    setup_logging()
    
    try:
        # Get configuration
        schemas_dir = os.getenv('SCHEMAS_DIR', 'events/avro')
        output_path = os.getenv('VALIDATION_REPORT_PATH', 'schema_validation_report.json')
        
        logging.info(f"Validating schemas in directory: {schemas_dir}")
        logging.info(f"Output report path: {output_path}")
        
        # Validate all schemas
        validation_results = validate_all_schemas(schemas_dir)
        
        # Check schema evolution
        evolution_results = check_schema_evolution(schemas_dir)
        
        # Generate report
        generate_validation_report(validation_results, evolution_results, output_path)
        
        # Check if validation passed
        if validation_results['invalid_schemas'] > 0:
            logging.error("Schema validation failed")
            sys.exit(1)
        
        if evolution_results['incompatible_schemas'] > 0:
            logging.warning("Schema compatibility issues found")
        
        logging.info("Schema validation completed successfully")
        
    except Exception as e:
        logging.error(f"Schema validation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()