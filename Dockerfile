# Use the official Apache Airflow image
FROM apache/airflow:2.10.2-python3.11

USER root

# Install system dependencies for ODBC and SQL Server driver
RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    gnupg \
    unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir python-dotenv requests pyodbc

# Set Airflow home (optional, default is /opt/airflow)
ENV AIRFLOW_HOME=/opt/airflow

# Copy DAGs into the container (optional if using volumes)
# COPY dags/ $AIRFLOW_HOME/dags/

USER airflow
