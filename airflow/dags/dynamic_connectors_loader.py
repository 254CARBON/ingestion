"""
Dynamic Connectors Loader DAG.

This DAG dynamically loads and creates DAGs for all connectors based on
their configuration files, enabling automatic discovery and management.
"""

import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from plugins.connector_operator import ConnectorOperator
from plugins.quality_check_sensor import QualityCheckSensor


# Default arguments for all DAGs
default_args = {
    'owner': 'platform',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
    'sla': timedelta(hours=2)  # SLA for connector execution
}

# DAG configuration
dag_config = {
    'dag_id': 'dynamic_connectors_loader',
    'default_args': default_args,
    'description': 'Dynamic connector discovery and DAG generation',
    'schedule_interval': '@hourly',
    'max_active_runs': 1,
    'catchup': False,
    'tags': ['connectors', 'dynamic', 'discovery']
}

# Create the main DAG
dag = DAG(**dag_config)


def load_connectors_index() -> Dict[str, Any]:
    """
    Load connectors index from file.
    
    Returns:
        Dict[str, Any]: Connectors index data
    """
    try:
        index_path = Variable.get("connectors_index_path", default_var="/opt/airflow/connectors_index.json")
        
        if os.path.exists(index_path):
            with open(index_path, 'r') as f:
                index_data = json.load(f)
            logging.info(f"Loaded connectors index with {len(index_data.get('connectors', []))} connectors")
            return index_data
        else:
            logging.warning(f"Connectors index file not found: {index_path}")
            return {"connectors": []}
            
    except Exception as e:
        logging.error(f"Failed to load connectors index: {e}")
        return {"connectors": []}


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


def create_connector_dag(connector_config: Dict[str, Any]) -> Optional[DAG]:
    """
    Create a DAG for a specific connector.
    
    Args:
        connector_config: Connector configuration
        
    Returns:
        Optional[DAG]: Created DAG or None if failed
    """
    try:
        if not validate_connector_config(connector_config):
            return None
        
        connector_name = connector_config['name']
        market = connector_config['market']
        mode = connector_config['mode']
        
        # Create DAG ID
        dag_id = f"ingest_{market.lower()}_{mode}"
        
        # Create DAG arguments
        dag_args = default_args.copy()
        dag_args['sla'] = timedelta(hours=1)  # Connector-specific SLA
        
        # Create DAG
        connector_dag = DAG(
            dag_id=dag_id,
            default_args=dag_args,
            description=f"Data ingestion for {market} market via {connector_name} connector",
            schedule_interval=connector_config.get('schedule', '@hourly'),
            max_active_runs=1,
            catchup=False,
            tags=['connector', market.lower(), mode, connector_name]
        )
        
        # Create tasks
        with connector_dag:
            # Data extraction task
            extract_task = ConnectorOperator(
                task_id=f"extract_{connector_name}",
                connector_config=connector_config,
                operation="extract",
                dag=connector_dag
            )
            
            # Data transformation task
            transform_task = ConnectorOperator(
                task_id=f"transform_{connector_name}",
                connector_config=connector_config,
                operation="transform",
                dag=connector_dag
            )
            
            # Data loading task
            load_task = ConnectorOperator(
                task_id=f"load_{connector_name}",
                connector_config=connector_config,
                operation="load",
                dag=connector_dag
            )
            
            # Data quality check
            quality_check = QualityCheckSensor(
                task_id=f"quality_check_{connector_name}",
                connector_config=connector_config,
                timeout=300,
                poke_interval=30,
                dag=connector_dag
            )
            
            # Set task dependencies
            extract_task >> transform_task >> load_task >> quality_check
            
            # Add SLA monitoring
            extract_task.sla = timedelta(minutes=30)
            transform_task.sla = timedelta(minutes=15)
            load_task.sla = timedelta(minutes=10)
            quality_check.sla = timedelta(minutes=5)
        
        logging.info(f"Created DAG for connector: {connector_name}")
        return connector_dag
        
    except Exception as e:
        logging.error(f"Failed to create DAG for connector {connector_config.get('name', 'unknown')}: {e}")
        return None


def generate_connectors_index(**context) -> None:
    """
    Generate connectors index from connector configurations.
    
    Args:
        **context: Airflow context
    """
    try:
        connectors_dir = Variable.get("connectors_dir", default_var="/opt/airflow/connectors")
        index_path = Variable.get("connectors_index_path", default_var="/opt/airflow/connectors_index.json")
        
        connectors = []
        
        # Scan connectors directory
        for root, dirs, files in os.walk(connectors_dir):
            for file in files:
                if file == 'connector.yaml':
                    connector_path = os.path.join(root, file)
                    
                    try:
                        import yaml
                        with open(connector_path, 'r') as f:
                            connector_config = yaml.safe_load(f)
                        
                        # Add connector path to config
                        connector_config['config_path'] = connector_path
                        connector_config['connector_dir'] = root
                        
                        # Validate connector
                        if validate_connector_config(connector_config):
                            connectors.append(connector_config)
                            logging.info(f"Added connector: {connector_config['name']}")
                        else:
                            logging.warning(f"Skipped invalid connector: {connector_path}")
                            
                    except Exception as e:
                        logging.error(f"Failed to load connector config {connector_path}: {e}")
                        continue
        
        # Create index data
        index_data = {
            "generated_at": datetime.now().isoformat(),
            "connectors_count": len(connectors),
            "connectors": connectors
        }
        
        # Write index file
        os.makedirs(os.path.dirname(index_path), exist_ok=True)
        with open(index_path, 'w') as f:
            json.dump(index_data, f, indent=2)
        
        logging.info(f"Generated connectors index with {len(connectors)} connectors")
        
        # Store in Airflow Variable for other DAGs
        Variable.set("connectors_index", json.dumps(index_data))
        
    except Exception as e:
        logging.error(f"Failed to generate connectors index: {e}")
        raise


def load_and_create_dags(**context) -> None:
    """
    Load connectors index and create DAGs dynamically.
    
    Args:
        **context: Airflow context
    """
    try:
        # Load connectors index
        index_data = load_connectors_index()
        connectors = index_data.get('connectors', [])
        
        created_dags = 0
        failed_dags = 0
        
        # Create DAGs for each connector
        for connector_config in connectors:
            if connector_config.get('enabled', True):
                dag = create_connector_dag(connector_config)
                if dag:
                    created_dags += 1
                else:
                    failed_dags += 1
            else:
                logging.info(f"Skipped disabled connector: {connector_config['name']}")
        
        logging.info(f"DAG creation completed: {created_dags} created, {failed_dags} failed")
        
        # Store results in XCom
        context['task_instance'].xcom_push(
            key='dag_creation_results',
            value={
                'created_dags': created_dags,
                'failed_dags': failed_dags,
                'total_connectors': len(connectors)
            }
        )
        
    except Exception as e:
        logging.error(f"Failed to load and create DAGs: {e}")
        raise


def monitor_connector_health(**context) -> None:
    """
    Monitor health of all connectors.
    
    Args:
        **context: Airflow context
    """
    try:
        # Load connectors index
        index_data = load_connectors_index()
        connectors = index_data.get('connectors', [])
        
        health_status = {
            'total_connectors': len(connectors),
            'healthy_connectors': 0,
            'unhealthy_connectors': 0,
            'connector_details': []
        }
        
        # Check health of each connector
        for connector_config in connectors:
            connector_name = connector_config['name']
            market = connector_config['market']
            
            try:
                # Simulate health check (in production, this would call the connector registry)
                health_status['healthy_connectors'] += 1
                health_status['connector_details'].append({
                    'name': connector_name,
                    'market': market,
                    'status': 'healthy',
                    'last_run': '2024-01-01T00:00:00Z'  # Placeholder
                })
                
            except Exception as e:
                health_status['unhealthy_connectors'] += 1
                health_status['connector_details'].append({
                    'name': connector_name,
                    'market': market,
                    'status': 'unhealthy',
                    'error': str(e)
                })
        
        logging.info(f"Health check completed: {health_status['healthy_connectors']} healthy, {health_status['unhealthy_connectors']} unhealthy")
        
        # Store results in XCom
        context['task_instance'].xcom_push(
            key='health_status',
            value=health_status
        )
        
    except Exception as e:
        logging.error(f"Failed to monitor connector health: {e}")
        raise


def cleanup_old_dags(**context) -> None:
    """
    Clean up old DAGs that are no longer needed.
    
    Args:
        **context: Airflow context
    """
    try:
        # This would typically involve:
        # 1. Comparing current connectors with existing DAGs
        # 2. Identifying DAGs that are no longer needed
        # 3. Archiving or removing old DAGs
        
        logging.info("DAG cleanup completed (placeholder implementation)")
        
    except Exception as e:
        logging.error(f"Failed to cleanup old DAGs: {e}")
        raise


# Define tasks
with dag:
    # Task 1: Generate connectors index
    generate_index_task = PythonOperator(
        task_id='generate_connectors_index',
        python_callable=generate_connectors_index,
        dag=dag
    )
    
    # Task 2: Load and create DAGs
    create_dags_task = PythonOperator(
        task_id='load_and_create_dags',
        python_callable=load_and_create_dags,
        dag=dag
    )
    
    # Task 3: Monitor connector health
    health_check_task = PythonOperator(
        task_id='monitor_connector_health',
        python_callable=monitor_connector_health,
        dag=dag
    )
    
    # Task 4: Cleanup old DAGs
    cleanup_task = PythonOperator(
        task_id='cleanup_old_dags',
        python_callable=cleanup_old_dags,
        dag=dag
    )
    
    # Task 5: Generate connector index script
    generate_index_script = BashOperator(
        task_id='generate_index_script',
        bash_command='python /opt/airflow/scripts/generate_connectors_index.py',
        dag=dag
    )
    
    # Task 6: Validate schemas
    validate_schemas_task = BashOperator(
        task_id='validate_schemas',
        bash_command='python /opt/airflow/scripts/validate_schemas.py',
        dag=dag
    )
    
    # Set task dependencies
    generate_index_script >> generate_index_task >> create_dags_task
    create_dags_task >> health_check_task >> cleanup_task
    validate_schemas_task >> create_dags_task