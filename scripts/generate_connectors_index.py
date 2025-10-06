#!/usr/bin/env python3
"""
Generate Connectors Index Script.

This script scans the connectors directory and generates an index file
containing all connector configurations for dynamic DAG generation.
"""

import json
import os
import sys
import yaml
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional


def setup_logging() -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('connectors_index_generation.log')
        ]
    )


def validate_connector_config(connector_config: Dict[str, Any]) -> bool:
    """
    Validate connector configuration.
    
    Args:
        connector_config: Connector configuration
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_fields = ['name', 'version', 'market', 'mode', 'enabled', 'output_topic', 'schema']
    
    try:
        for field in required_fields:
            if field not in connector_config:
                logging.error(f"Missing required field '{field}' in connector config")
                return False
        
        # Validate schedule format for batch mode
        if connector_config.get('mode') == 'batch' and 'schedule' in connector_config:
            schedule = connector_config['schedule']
            if not schedule or schedule == '':
                logging.error(f"Empty schedule for batch connector: {connector_config['name']}")
                return False
        
        # Validate schema file exists
        schema_path = connector_config.get('schema')
        if schema_path and not os.path.exists(schema_path):
            logging.warning(f"Schema file not found: {schema_path}")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to validate connector config: {e}")
        return False


def load_connector_config(connector_path: str) -> Optional[Dict[str, Any]]:
    """
    Load connector configuration from file.
    
    Args:
        connector_path: Path to connector configuration file
        
    Returns:
        Optional[Dict[str, Any]]: Connector configuration or None if failed
    """
    try:
        with open(connector_path, 'r') as f:
            connector_config = yaml.safe_load(f)
        
        # Add metadata
        connector_config['config_path'] = connector_path
        connector_config['connector_dir'] = os.path.dirname(connector_path)
        connector_config['loaded_at'] = datetime.now().isoformat()
        
        return connector_config
        
    except Exception as e:
        logging.error(f"Failed to load connector config {connector_path}: {e}")
        return None


def scan_connectors_directory(connectors_dir: str) -> List[Dict[str, Any]]:
    """
    Scan connectors directory for configuration files.
    
    Args:
        connectors_dir: Path to connectors directory
        
    Returns:
        List[Dict[str, Any]]: List of connector configurations
    """
    connectors = []
    
    if not os.path.exists(connectors_dir):
        logging.error(f"Connectors directory not found: {connectors_dir}")
        return connectors
    
    # Scan for connector.yaml files
    for root, dirs, files in os.walk(connectors_dir):
        # Skip base directory
        if 'base' in root:
            continue
            
        for file in files:
            if file == 'connector.yaml':
                connector_path = os.path.join(root, file)
                
                # Load connector configuration
                connector_config = load_connector_config(connector_path)
                
                if connector_config and validate_connector_config(connector_config):
                    connectors.append(connector_config)
                    logging.info(f"Added connector: {connector_config['name']} ({connector_config['market']})")
                else:
                    logging.warning(f"Skipped invalid connector: {connector_path}")
    
    return connectors


def generate_connectors_index(connectors: List[Dict[str, Any]], output_path: str) -> None:
    """
    Generate connectors index file.
    
    Args:
        connectors: List of connector configurations
        output_path: Path to output index file
    """
    try:
        # Create index data
        index_data = {
            "generated_at": datetime.now().isoformat(),
            "generator": "generate_connectors_index.py",
            "version": "1.0.0",
            "connectors_count": len(connectors),
            "connectors": connectors,
            "summary": {
                "total_connectors": len(connectors),
                "enabled_connectors": len([c for c in connectors if c.get('enabled', True)]),
                "disabled_connectors": len([c for c in connectors if not c.get('enabled', True)]),
                "markets": list(set([c['market'] for c in connectors])),
                "modes": list(set([c['mode'] for c in connectors]))
            }
        }
        
        # Write index file
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(index_data, f, indent=2)
        
        logging.info(f"Generated connectors index with {len(connectors)} connectors")
        logging.info(f"Index file saved to: {output_path}")
        
        # Print summary
        print(f"\nConnectors Index Generation Summary:")
        print(f"Total connectors: {index_data['summary']['total_connectors']}")
        print(f"Enabled connectors: {index_data['summary']['enabled_connectors']}")
        print(f"Disabled connectors: {index_data['summary']['disabled_connectors']}")
        print(f"Markets: {', '.join(index_data['summary']['markets'])}")
        print(f"Modes: {', '.join(index_data['summary']['modes'])}")
        
    except Exception as e:
        logging.error(f"Failed to generate connectors index: {e}")
        raise


def main():
    """Main function."""
    setup_logging()
    
    try:
        # Get configuration
        connectors_dir = os.getenv('CONNECTORS_DIR', 'connectors')
        output_path = os.getenv('CONNECTORS_INDEX_PATH', 'connectors_index.json')
        
        logging.info(f"Scanning connectors directory: {connectors_dir}")
        logging.info(f"Output path: {output_path}")
        
        # Scan connectors directory
        connectors = scan_connectors_directory(connectors_dir)
        
        if not connectors:
            logging.warning("No valid connectors found")
            return
        
        # Generate index
        generate_connectors_index(connectors, output_path)
        
        logging.info("Connectors index generation completed successfully")
        
    except Exception as e:
        logging.error(f"Connectors index generation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()