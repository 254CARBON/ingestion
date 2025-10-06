"""
CAISO Market Data Ingestion DAG.

This DAG handles the ingestion of CAISO market data using the CAISO connector.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from airflow.plugins.connector_operator import ConnectorOperator
from airflow.plugins.quality_check_sensor import QualityCheckSensor

# Default arguments
default_args = {
    'owner': 'platform',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create DAG
dag = DAG(
    'ingest_caiso_batch',
    default_args=default_args,
    description='Ingest CAISO market data',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['ingestion', 'caiso', 'batch'],
)

# Task 1: Check CAISO connector health
health_check = PythonOperator(
    task_id='health_check',
    python_callable=check_caiso_health,
    dag=dag,
)

# Task 2: Run CAISO connector
run_caiso_connector = ConnectorOperator(
    task_id='run_caiso_connector',
    connector_name='caiso',
    mode='batch',
    dag=dag,
)

# Task 3: Quality check
quality_check = QualityCheckSensor(
    task_id='quality_check',
    topic='ingestion.caiso.raw.v1',
    timeout=300,
    poke_interval=30,
    dag=dag,
)

# Task 4: Update metrics
update_metrics = PythonOperator(
    task_id='update_metrics',
    python_callable=update_caiso_metrics,
    dag=dag,
)

# Task 5: Cleanup
cleanup = BashOperator(
    task_id='cleanup',
    bash_command='echo "Cleaning up temporary files"',
    dag=dag,
)

# Set task dependencies
health_check >> run_caiso_connector >> quality_check >> update_metrics >> cleanup


def check_caiso_health():
    """Check CAISO connector health."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # This would call the connector registry service
        logger.info("Checking CAISO connector health")
        
        # Simulate health check
        import requests
        response = requests.get('http://connector-registry:8500/connectors/caiso/health', timeout=10)
        
        if response.status_code == 200:
            logger.info("CAISO connector is healthy")
            return True
        else:
            logger.error(f"CAISO connector health check failed: {response.status_code}")
            raise Exception("CAISO connector is unhealthy")
            
    except Exception as e:
        logger.error(f"CAISO health check failed: {e}")
        raise


def update_caiso_metrics():
    """Update CAISO connector metrics."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Updating CAISO connector metrics")
        
        # This would update metrics in the monitoring system
        # For now, just log the action
        
    except Exception as e:
        logger.error(f"Failed to update CAISO metrics: {e}")
        raise
