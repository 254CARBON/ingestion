"""
Maintenance DAG for cleanup and system maintenance tasks.

This DAG handles various maintenance tasks including cleanup,
index regeneration, and system health checks.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    'owner': 'platform',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
}

# Create DAG
dag = DAG(
    'maintenance',
    default_args=default_args,
    description='System maintenance and cleanup tasks',
    schedule_interval='0 0 * * 0',  # Weekly on Sunday at midnight
    max_active_runs=1,
    tags=['maintenance', 'cleanup', 'system'],
)

# Task Group: Cleanup tasks
with TaskGroup('cleanup', dag=dag) as cleanup_group:
    
    # Cleanup old logs
    cleanup_logs = BashOperator(
        task_id='cleanup_logs',
        bash_command='find /opt/airflow/logs -name "*.log" -mtime +7 -delete',
    )
    
    # Cleanup temporary files
    cleanup_temp = BashOperator(
        task_id='cleanup_temp',
        bash_command='find /tmp -name "airflow_*" -mtime +1 -delete',
    )
    
    # Cleanup old data files
    cleanup_data = BashOperator(
        task_id='cleanup_data',
        bash_command='find /opt/airflow/data -name "*.tmp" -mtime +1 -delete',
    )

# Task Group: Index regeneration
with TaskGroup('index_regeneration', dag=dag) as index_group:
    
    # Regenerate connector index
    regenerate_index = PythonOperator(
        task_id='regenerate_connector_index',
        python_callable=regenerate_connector_index,
    )
    
    # Validate schemas
    validate_schemas = PythonOperator(
        task_id='validate_schemas',
        python_callable=validate_all_schemas,
    )

# Task Group: Health checks
with TaskGroup('health_checks', dag=dag) as health_group:
    
    # Check service health
    check_services = PythonOperator(
        task_id='check_services',
        python_callable=check_all_services,
    )
    
    # Check connector health
    check_connectors = PythonOperator(
        task_id='check_connectors',
        python_callable=check_all_connectors,
    )

# Task Group: Metrics and reporting
with TaskGroup('metrics_reporting', dag=dag) as metrics_group:
    
    # Generate weekly report
    generate_report = PythonOperator(
        task_id='generate_weekly_report',
        python_callable=generate_weekly_report,
    )
    
    # Update system metrics
    update_system_metrics = PythonOperator(
        task_id='update_system_metrics',
        python_callable=update_system_metrics,
    )

# Final notification
send_notification = PythonOperator(
    task_id='send_notification',
    python_callable=send_maintenance_notification,
    dag=dag,
)

# Set task dependencies
[cleanup_group, index_group, health_group, metrics_group] >> send_notification


def regenerate_connector_index():
    """Regenerate the connector index."""
    import logging
    import subprocess
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Regenerating connector index")
        
        # Run the index generation script
        result = subprocess.run(
            ['python', 'scripts/generate_connectors_index.py'],
            capture_output=True,
            text=True,
            cwd='/opt/airflow'
        )
        
        if result.returncode == 0:
            logger.info("Connector index regenerated successfully")
        else:
            logger.error(f"Failed to regenerate index: {result.stderr}")
            raise Exception("Index regeneration failed")
            
    except Exception as e:
        logger.error(f"Error regenerating connector index: {e}")
        raise


def validate_all_schemas():
    """Validate all Avro schemas."""
    import logging
    import subprocess
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Validating all schemas")
        
        # Run schema validation
        result = subprocess.run(
            ['python', 'scripts/validate_schemas.py'],
            capture_output=True,
            text=True,
            cwd='/opt/airflow'
        )
        
        if result.returncode == 0:
            logger.info("All schemas validated successfully")
        else:
            logger.error(f"Schema validation failed: {result.stderr}")
            raise Exception("Schema validation failed")
            
    except Exception as e:
        logger.error(f"Error validating schemas: {e}")
        raise


def check_all_services():
    """Check health of all services."""
    import logging
    import requests
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Checking all services")
        
        services = [
            'connector-registry:8500',
            'normalization:8510',
            'enrichment:8511',
            'aggregation:8512',
            'projection:8513'
        ]
        
        unhealthy_services = []
        
        for service in services:
            try:
                response = requests.get(f'http://{service}/health', timeout=10)
                if response.status_code != 200:
                    unhealthy_services.append(service)
            except Exception:
                unhealthy_services.append(service)
        
        if unhealthy_services:
            logger.warning(f"Unhealthy services: {unhealthy_services}")
        else:
            logger.info("All services are healthy")
            
    except Exception as e:
        logger.error(f"Error checking services: {e}")
        raise


def check_all_connectors():
    """Check health of all connectors."""
    import logging
    import requests
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Checking all connectors")
        
        # Get list of connectors from registry
        response = requests.get('http://connector-registry:8500/connectors/', timeout=10)
        
        if response.status_code == 200:
            connectors = response.json().get('connectors', [])
            
            unhealthy_connectors = []
            
            for connector in connectors:
                connector_name = connector['name']
                try:
                    health_response = requests.get(
                        f'http://connector-registry:8500/connectors/{connector_name}/health',
                        timeout=10
                    )
                    if health_response.status_code != 200:
                        unhealthy_connectors.append(connector_name)
                except Exception:
                    unhealthy_connectors.append(connector_name)
            
            if unhealthy_connectors:
                logger.warning(f"Unhealthy connectors: {unhealthy_connectors}")
            else:
                logger.info("All connectors are healthy")
        else:
            logger.error("Failed to get connector list")
            raise Exception("Failed to get connector list")
            
    except Exception as e:
        logger.error(f"Error checking connectors: {e}")
        raise


def generate_weekly_report():
    """Generate weekly system report."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Generating weekly report")
        
        # This would generate a comprehensive weekly report
        # For now, just log the action
        
    except Exception as e:
        logger.error(f"Error generating weekly report: {e}")
        raise


def update_system_metrics():
    """Update system metrics."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Updating system metrics")
        
        # This would update system-wide metrics
        # For now, just log the action
        
    except Exception as e:
        logger.error(f"Error updating system metrics: {e}")
        raise


def send_maintenance_notification():
    """Send maintenance completion notification."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Sending maintenance notification")
        
        # This would send a notification about maintenance completion
        # For now, just log the action
        
    except Exception as e:
        logger.error(f"Error sending notification: {e}")
        raise
