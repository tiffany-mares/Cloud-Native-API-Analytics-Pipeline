# ============================================
# Airflow Docker Image
# Cloud-Native API Analytics Pipeline
# ============================================

FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set Python path for src imports
ENV PYTHONPATH="/opt/airflow/src:/opt/airflow:${PYTHONPATH}"
