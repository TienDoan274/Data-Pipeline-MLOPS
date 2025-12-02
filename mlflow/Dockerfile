# mlflow/Dockerfile
FROM ghcr.io/mlflow/mlflow:v2.9.2

USER root

# Install psycopg2 for PostgreSQL support
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    boto3==1.34.14

USER nobody

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=30s \
    CMD curl -f http://localhost:5000/health || exit 1