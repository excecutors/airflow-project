# Airflow base image (lightweight)
FROM apache/airflow:2.9.2

# Switch to root to install dependencies
USER root

# Install OS-level packages
RUN apt-get update && apt-get install -y \
    python3-dev gcc build-essential && \
    apt-get clean

# Switch back to Airflow user
USER airflow

# Copy project requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

