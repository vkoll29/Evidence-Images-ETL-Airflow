# Use an official Python base image
FROM python:3.9-slim

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Apache Airflow with extras (including PostgreSQL and other databases)
RUN pip install 'apache-airflow[postgres,celery,redis]'

# Set up the Airflow database (if using SQLite, this step can be skipped)
# Replace "your_database_connection_string" with your actual database connection string
# For example, for PostgreSQL: postgresql+psycopg2://username:password@host:port/database_name
# For SQLite: sqlite:////usr/local/airflow/airflow.db
# RUN airflow db init

# Expose the ports used by Airflow web server and worker (if using LocalExecutor or CeleryExecutor)
EXPOSE 8080
EXPOSE 8793

# Initialize the database and start the web server (replace "LocalExecutor" with "CeleryExecutor" if using Celery)
CMD ["bash", "-c", "airflow db init && airflow webserver --port 8080"]
