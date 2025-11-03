# Dockerfile
FROM apache/airflow:2.9.2

# Copy requirements
COPY requirements.txt /requirements.txt

# Install packages
RUN pip install --no-cache-dir -r /requirements.txt