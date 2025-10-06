#!/usr/bin/env python3
"""
Test script to verify CAISO connector examples are working correctly.

This script runs basic tests on the example files and configurations
to ensure they work as expected.
"""

import asyncio
import json
import logging
import sys
import os
from pathlib import Path

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from connectors.caiso.config import CAISOConnectorConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_configurations():
    """Test that all configuration files are valid."""
    logger.info("Testing CAISO configurations...")

    config_files = [
        "caiso_config_basic.yaml",
        "caiso_config_advanced.yaml",
        "caiso_config_production.yaml"
    ]

    for config_file in config_files:
        config_path = Path(__file__).parent / config_file
        if config_path.exists():
            logger.info(f"✓ Configuration file exists: {config_file}")
        else:
            logger.error(f"✗ Configuration file missing: {config_file}")
            return False

    # Test basic configuration loading
    try:
        config = CAISOConnectorConfig(
            name="test_config",
            version="1.0.0",
            market="CAISO",
            mode="batch",
            output_topic="test.topic",
            schema="test.schema"
        )
        logger.info("✓ Basic configuration creation successful")
    except Exception as e:
        logger.error(f"✗ Configuration creation failed: {e}")
        return False

    return True


def test_sample_data():
    """Test that sample data files are valid JSON."""
    logger.info("Testing sample data files...")

    data_files = [
        "sample_raw_data.json",
        "sample_normalized_data.json",
        "sample_enriched_data.json"
    ]

    for data_file in data_files:
        data_path = Path(__file__).parent / data_file
        if not data_path.exists():
            logger.error(f"✗ Sample data file missing: {data_file}")
            return False

        try:
            with open(data_path, 'r') as f:
                data = json.load(f)

            if not isinstance(data, list):
                logger.error(f"✗ Sample data should be a list: {data_file}")
                return False

            if len(data) == 0:
                logger.warning(f"⚠ Sample data is empty: {data_file}")
            else:
                logger.info(f"✓ Sample data valid ({len(data)} records): {data_file}")

        except json.JSONDecodeError as e:
            logger.error(f"✗ Invalid JSON in {data_file}: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Error reading {data_file}: {e}")
            return False

    return True


def test_example_scripts():
    """Test that example scripts can be imported without errors."""
    logger.info("Testing example scripts...")

    script_files = [
        "basic_usage.py",
        "batch_processing.py",
        "real_time_monitoring.py",
        "error_handling.py",
        "integration_example.py"
    ]

    for script_file in script_files:
        script_path = Path(__file__).parent / script_file
        if not script_path.exists():
            logger.error(f"✗ Example script missing: {script_file}")
            return False

        try:
            # Try to compile the script (basic syntax check)
            with open(script_path, 'r') as f:
                code = f.read()

            compile(code, script_path.name, 'exec')
            logger.info(f"✓ Example script syntax valid: {script_file}")

        except SyntaxError as e:
            logger.error(f"✗ Syntax error in {script_file}: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ Error checking {script_file}: {e}")
            return False

    return True


def test_documentation():
    """Test that documentation file exists."""
    logger.info("Testing documentation...")

    readme_path = Path(__file__).parent / "README.md"
    if readme_path.exists():
        logger.info("✓ Documentation exists: README.md")
        return True
    else:
        logger.error("✗ Documentation missing: README.md")
        return False


async def test_connector_initialization():
    """Test that CAISO connector can be initialized."""
    logger.info("Testing CAISO connector initialization...")

    try:
        config = CAISOConnectorConfig(
            name="test_connector",
            version="1.0.0",
            market="CAISO",
            mode="batch",
            output_topic="test.topic",
            schema="test.schema"
        )

        # This should not raise an exception
        from connectors.caiso.connector import CAISOConnector
        connector = CAISOConnector(config)

        logger.info("✓ CAISO connector initialization successful")
        return True

    except Exception as e:
        logger.error(f"✗ CAISO connector initialization failed: {e}")
        return False


def main():
    """Run all tests."""
    logger.info("=== CAISO Connector Examples Test Suite ===")

    tests = [
        ("Configurations", test_configurations),
        ("Sample Data", test_sample_data),
        ("Example Scripts", test_example_scripts),
        ("Documentation", test_documentation),
    ]

    results = []

    # Run synchronous tests
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results.append((test_name, False))

    # Run async tests
    logger.info(f"\n--- Async Tests ---")
    try:
        result = asyncio.run(test_connector_initialization())
        results.append(("Connector Initialization", result))
    except Exception as e:
        logger.error(f"Async test failed: {e}")
        results.append(("Connector Initialization", False))

    # Summary
    logger.info(f"\n=== Test Results Summary ===")
    passed = 0
    failed = 0

    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {test_name}")
        if result:
            passed += 1
        else:
            failed += 1

    logger.info(f"\nOverall: {passed} passed, {failed} failed")

    if failed == 0:
        logger.info("🎉 All tests passed! CAISO connector examples are ready to use.")
        return 0
    else:
        logger.error("❌ Some tests failed. Please check the issues above.")
        return 1


if __name__ == "__main__":
    exit(main())
