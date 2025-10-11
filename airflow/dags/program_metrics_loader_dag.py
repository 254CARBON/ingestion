"""
Airflow DAG for program metrics loader automation.

Loads market snapshots into ClickHouse and runs smoke tests.
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable


# Default arguments
default_args = {
    "owner": "analytics-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "program_metrics_loader",
    default_args=default_args,
    description="Load program metrics snapshots and run smoke tests",
    schedule_interval="@hourly",  # Run hourly for load/gen data
    catchup=False,
    max_active_runs=1,
    tags=["program-metrics", "clickhouse", "analytics"],
)


def get_clickhouse_url() -> str:
    """Get ClickHouse URL from Airflow variables."""
    return Variable.get("clickhouse_url", default_var="http://localhost:8123")


def get_snapshot_base_dir() -> str:
    """Get snapshot base directory from Airflow variables."""
    return Variable.get("snapshot_base_dir", default_var="/home/m/254/carbon/analytics/fixtures/program-mvp")


def get_scenario_service_url() -> str:
    """Get scenario service URL from Airflow variables."""
    return Variable.get("scenario_service_url", default_var="http://localhost:8000")


# Market configurations
MARKETS = [
    {
        "name": "wecc",
        "display_name": "WECC/CAISO",
        "schedule": "@hourly",
        "files": ["load_demand.csv", "generation_actual.csv"],
    },
    {
        "name": "miso", 
        "display_name": "MISO",
        "schedule": "@hourly",
        "files": ["load_demand.csv", "generation_actual.csv"],
    },
    {
        "name": "ercot",
        "display_name": "ERCOT", 
        "schedule": "@hourly",
        "files": ["load_demand.csv", "generation_actual.csv"],
    },
]

# Create tasks for each market
market_tasks = {}

for market in MARKETS:
    market_name = market["name"]
    
    # File sensors for snapshot availability
    file_sensors = {}
    for file_type in market["files"]:
        sensor_task_id = f"wait_for_{market_name}_{file_type.replace('.csv', '')}"
        file_sensors[file_type] = FileSensor(
            task_id=sensor_task_id,
            filepath=f"{get_snapshot_base_dir()}/{market_name}/{file_type}",
            fs_conn_id="fs_default",
            poke_interval=30,
            timeout=300,
            dag=dag,
        )
    
    # Loader task
    loader_task_id = f"load_{market_name}_snapshots"
    loader_task = BashOperator(
        task_id=loader_task_id,
        bash_command=f"""
        python /home/m/254/carbon/data-processing/services/program-metrics-loader/loader.py \
            --snapshot-dir {get_snapshot_base_dir()}/{market_name} \
            --clickhouse-url {get_clickhouse_url()} \
            --database markets
        """,
        dag=dag,
    )
    
    # Smoke test task
    smoke_test_task_id = f"smoke_test_{market_name}_apis"
    smoke_test_task = BashOperator(
        task_id=smoke_test_task_id,
        bash_command=f"""
        # Test RA API
        curl -f "{get_scenario_service_url()}/api/v1/programs/ra?market={market_name}" || exit 1
        
        # Test RPS API
        curl -f "{get_scenario_service_url()}/api/v1/programs/rps?market={market_name}&compliance_year=2024" || exit 1
        
        # Test GHG API
        curl -f "{get_scenario_service_url()}/api/v1/programs/ghg?market={market_name}&scope=scope1" || exit 1
        
        # Test IRP API
        curl -f "{get_scenario_service_url()}/api/v1/programs/irp?market={market_name}&planning_horizon_year=2024" || exit 1
        
        echo "All API smoke tests passed for {market_name}"
        """,
        dag=dag,
    )
    
    # Set up dependencies
    for file_type, sensor in file_sensors.items():
        sensor >> loader_task
    
    loader_task >> smoke_test_task
    
    market_tasks[market_name] = {
        "file_sensors": file_sensors,
        "loader": loader_task,
        "smoke_test": smoke_test_task,
    }


# Monthly tasks for REC ledger updates
monthly_markets = ["wecc", "miso", "ercot"]

for market_name in monthly_markets:
    # REC ledger file sensor
    rec_sensor_task_id = f"wait_for_{market_name}_rec_ledger"
    rec_sensor = FileSensor(
        task_id=rec_sensor_task_id,
        filepath=f"{get_snapshot_base_dir()}/{market_name}/rec_ledger.csv",
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=600,
        dag=dag,
    )
    
    # REC ledger loader
    rec_loader_task_id = f"load_{market_name}_rec_ledger"
    rec_loader = BashOperator(
        task_id=rec_loader_task_id,
        bash_command=f"""
        python /home/m/254/carbon/data-processing/services/program-metrics-loader/loader.py \
            --snapshot-dir {get_snapshot_base_dir()}/{market_name} \
            --clickhouse-url {get_clickhouse_url()} \
            --database markets
        """,
        dag=dag,
    )
    
    rec_sensor >> rec_loader


# Quarterly tasks for emission factors
quarterly_task = BashOperator(
    task_id="load_emission_factors",
    bash_command=f"""
    # Load emission factors for all markets
    for market in wecc miso ercot; do
        python /home/m/254/carbon/data-processing/services/program-metrics-loader/loader.py \
            --snapshot-dir {get_snapshot_base_dir()}/$market \
            --clickhouse-url {get_clickhouse_url()} \
            --database markets
    done
    """,
    dag=dag,
)


# Data quality validation task
def validate_data_quality(**context) -> None:
    """Validate data quality across all markets."""
    import subprocess
    import json
    
    clickhouse_url = get_clickhouse_url()
    
    validation_queries = [
        {
            "name": "ra_hourly_count",
            "query": "SELECT count() FROM markets.ra_hourly WHERE market = 'wecc'",
            "min_count": 100,
        },
        {
            "name": "rps_compliance_count", 
            "query": "SELECT count() FROM markets.rps_compliance_annual WHERE market = 'miso'",
            "min_count": 1,
        },
        {
            "name": "ghg_inventory_count",
            "query": "SELECT count() FROM markets.ghg_inventory_hourly WHERE market = 'ercot'",
            "min_count": 100,
        },
    ]
    
    results = {}
    for validation in validation_queries:
        try:
            # Execute query using curl
            cmd = [
                "curl", "-s",
                f"{clickhouse_url}/?query={validation['query']}&default_format=JSON"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                count = data["data"][0][0] if data["data"] else 0
                results[validation["name"]] = {
                    "count": count,
                    "passed": count >= validation["min_count"],
                }
            else:
                results[validation["name"]] = {
                    "count": 0,
                    "passed": False,
                    "error": result.stderr,
                }
        except Exception as e:
            results[validation["name"]] = {
                "count": 0,
                "passed": False,
                "error": str(e),
            }
    
    # Log results
    print("Data quality validation results:")
    for name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        print(f"  {name}: {status} (count: {result['count']})")
        if not result["passed"] and "error" in result:
            print(f"    Error: {result['error']}")
    
    # Fail if any validation failed
    failed_validations = [name for name, result in results.items() if not result["passed"]]
    if failed_validations:
        raise Exception(f"Data quality validation failed: {failed_validations}")


data_quality_task = PythonOperator(
    task_id="validate_data_quality",
    python_callable=validate_data_quality,
    dag=dag,
)

# Set up final dependencies
all_smoke_tests = [market_tasks[market]["smoke_test"] for market in market_tasks]
all_smoke_tests >> data_quality_task

# Documentation
dag.doc_md = """
# Program Metrics Loader DAG

This DAG automates the loading of program metrics snapshots into ClickHouse
and runs smoke tests to verify data quality and API functionality.

## Schedule
- **Hourly**: Load and generation data for all markets
- **Monthly**: REC ledger updates
- **Quarterly**: Emission factors updates

## Tasks
1. **File Sensors**: Wait for snapshot files to be available
2. **Loaders**: Load CSV snapshots into ClickHouse
3. **Smoke Tests**: Verify API endpoints return valid data
4. **Data Quality**: Validate data completeness and quality

## Configuration
Set these Airflow variables:
- `clickhouse_url`: ClickHouse connection URL
- `snapshot_base_dir`: Base directory for snapshot files
- `scenario_service_url`: Scenario service API URL

## Monitoring
- Task failures trigger email notifications
- Data quality validation ensures data completeness
- Smoke tests verify API functionality
"""
