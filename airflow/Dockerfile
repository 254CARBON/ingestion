FROM apache/airflow:2.7.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt

# Install project dependencies
COPY pyproject.toml /tmp/pyproject.toml
RUN pip install --no-cache-dir -e /tmp

# Copy Airflow configuration
COPY airflow_settings.yaml /opt/airflow/airflow.cfg

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Create necessary directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/logs

# Expose port
EXPOSE 8080

# Default command
CMD ["webserver"]
