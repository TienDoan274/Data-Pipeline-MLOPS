# Data Pipeline MLOps – E2E Medallion, Streaming Alerts, Dashboard, Recommendation API (MLflow)

## Brief Description
- **Objective**: End-to-end data system for e-commerce, including batch processing, streaming, real-time alerts, and Recommendation API.
- **Main Components**:
  - **Medallion ETL (Bronze/Silver/Gold)** using Airflow, stored on MinIO (S3-compatible).
  - **Micro-batch pipeline** exports metrics for Dashboard with auto-refresh via Redis pub/sub.
  - **CDC** from PostgreSQL → Kafka (Debezium) → Flink detects alerts and sends to Telegram.
  - **Trino** for querying data on the data lake (MinIO).
  - **ML Offline**: backfill ML data + train CF model + log & register model to **MLflow**.
  - **Recommendation API** (FastAPI) loads model from **MLflow Model Registry**, caches results with Redis.

## Architecture and Services
- **Airflow**: `airflow-webserver`, `airflow-scheduler`, `airflow-postgres` (metadata).
- **Data Source**: `source-postgres` (E-commerce DB: `customers`, `products`, `orders`).
- **Data Lake**: `minio` (+ `minio-init` creates buckets `bronze`, `silver`, `gold`, `trino`, `mlflow` artifacts).
- **Streaming**: `zookeeper`, `kafka`, `kafka-ui`.
- **CDC**: `debezium` reads changes from `source-postgres` and pushes to Kafka topic `cdc.public.orders`.
- **Query**: `trino` (catalog connects to MinIO and source Postgres).
- **Dashboard**: `dashboard` (Streamlit) reads metrics from `gold` and receives update signals via Redis.
- **Flink**: `flink-jobmanager`, `flink-taskmanager` run Python jobs in `flink-jobs/` to detect alerts & send to Telegram.
- **Redis**: cache & real-time notification for Dashboard and Recommendation API.
- **MLflow Tracking**:
  - `mlflow-postgres`: metadata store.
  - `mlflow-server`: MLflow 2.9.2 server, stores artifacts on MinIO (S3).
- **Recommendation API**: `recommendation-api` (FastAPI) reads CF model from **MLflow Model Registry**.

## Main Code Components
- **Airflow DAGs** (`dags/`):
  - **`medallion_ml_pipeline.py` → DAG `medallion_ml_pipeline`**:
    - Daily ETL Postgres → MinIO (Bronze → Silver → Gold).
    - Prepares ML data (user–product interactions) and saves to `gold/ml-data/...`.
    - Can trigger training pipeline/batch training (combined with ML scripts in `scripts/`).
  - **`micro_batch_dashboard.py` → DAG `micro_batch_dashboard_simple`**:
    - `extract_today_metrics`: fetches today's orders from Postgres, writes to `gold/dashboard/orders_today`.
    - `compute_dashboard_metrics`: creates tables `overall_metrics`, `top_products`, `category_stats`, `regional_stats`, `hourly_stats` in `gold/dashboard/metrics/*.parquet`.
    - `notify_dashboard`: publishes event to Redis channel `dashboard:updates` + updates key `dashboard:last_update`.
- **Dashboard** (`dashboard/app.py`):
  - Reads metrics from MinIO bucket `gold/dashboard/metrics/*.parquet`.
  - Auto-refresh based on Redis key `dashboard:last_update`.
  - Displays KPIs, hourly charts, Top products, Category/Region analysis.
- **Flink jobs** (`flink-jobs/`):
  - `flink_job_alert_detection_simple.py`: reads Kafka CDC topic (JSON unwrapped), applies rules:
    - Large order total (high value),
    - Unusual quantity (> 50),
    - Negative price,
    - Quantity <= 0,
    - Writes results to topic `flink-alerts`.
  - `flink_job_telegram_sender.py`: reads `flink-alerts` and sends alerts to Telegram (Bot token + Chat ID injected from env).
- **ML Offline & Scripts** (`scripts/` – latest code):
  - **`setup_source_db.py`**:
    - Creates schema `customers`, `products`, `orders` in `source-postgres`.
    - Generates initial sample data for e-commerce.
  - **`ingest_data.py`**:
    - Generates additional 30 days of orders data (starting from 2025-12-02).
    - Intentionally creates problematic records (negative price, zero quantity, high value, high quantity) to test CDC + Flink alerts.
  - **`create_data_for_recommend.py`**:
    - Backfills **train/eval** data for Recommendation over the last 30 days.
    - Pipeline:
      - Extracts orders by date from Postgres.
      - Cleans (removes nulls, fixes negative prices, removes quantity <= 0, recalculates total).
      - Extracts unique `(customer_id, product_name)` interactions.
      - Splits train/eval by customer (GroupShuffleSplit 80/20).
      - Saves `train` and `eval` to MinIO bucket `gold` as:
        - `gold/ml-data/train/date=YYYY-MM-DD/interactions.parquet`
        - `gold/ml-data/eval/date=YYYY-MM-DD/interactions.parquet` (eval is accumulated).
  - **`train_past_data_for_recommend.py`** (training pipeline & model registration with MLflow):
    - Reads 30 days of training data from `gold/ml-data/train/...`.
    - Builds user–item matrix (user × product) and calculates item–item similarity (cosine).
    - Saves model artifacts (matrices, ID mappings, metadata) to MinIO `gold/ml-data/models/...`.
    - Logs params/metrics/artifacts to **MLflow Tracking**:
      - Number of interactions, users/products, sparsity, similarity statistics, model size.
    - Registers model to **MLflow Model Registry**:
      - Model name: `collaborative_filtering_model`.
      - Auto-creates/updates version, transitions stage to **Production**, and adds tags/description.
  - **`test_cdc_alerts.py`**:
    - Inserts a series of test orders (order_id `TSTTT_*`) into Postgres with conditions:
      - High value (total >= 50,000),
      - High quantity (> 50),
      - Negative price,
      - Zero quantity.
    - Used to verify end-to-end: Postgres → Debezium CDC → Kafka → Flink Alert → Kafka `flink-alerts` → Telegram.

- **Recommendation API** (`recommendation/main.py` – FastAPI + MLflow):
  - Uses **MLflow Tracking Server** and **Model Registry**:
    - `MLFLOW_TRACKING_URI=http://mlflow-server:5000`.
    - Artifacts stored on MinIO (S3).
  - On service startup:
    - Connects to MLflow, reads model `collaborative_filtering_model` stage `Production`.
    - Loads all necessary artifacts (matrices, user/product mappings, metadata).
  - **Endpoints**:
    - `GET /health`: health information and model metadata (version, n_users, n_products, sparsity, MLflow URI, etc.).
    - `GET /recommend/{customer_id}?top_n=`: product recommendations for user, with Redis cache (24h TTL).
    - `GET /similar/products/{product_id}?top_n=`: find similar products by product ID using item–item similarity.
    - `GET /similar/{product_name}?top_n=`: find similar products by product name (backward compatible).
    - `GET /available-products`: list all products with product IDs and popularity.
    - `GET /available-customers`: list all customers in training data with purchase counts.
    - `GET /check/customer/{customer_id}`: check if customer exists in training data.
    - `GET /recommend/fallback/{customer_id}`: get popular products for unknown customers.
    - `POST /reload`: reload latest model from MLflow (Production stage) and flush Redis cache.

## System Requirements
- **Required**:
  - Docker, Docker Compose.
- **Available Ports** (according to current `docker-compose.yml`):
  - 8080 (Airflow Web),
  - 5433 (Airflow Postgres),
  - 5434 (Source Postgres),
  - 9000/9001 (MinIO API/Console),
  - 8081 (Trino UI),
  - 9080 (Kafka UI),
  - 9092/9093 (Kafka),
  - 8083 (Debezium),
  - 8501 (Dashboard),
  - 6379 (Redis),
  - 5000 (MLflow),
  - 5431 (MLflow Postgres),
  - 8000 (Recommendation API).
- **Optional**:
  - Telegram account (BotFather) & Chat ID to receive alerts.

## Overall Pipeline Diagram

<img width="3057" height="1799" alt="Untitled diagram-2025-11-09-050911" src="https://github.com/user-attachments/assets/72957b33-442e-4be6-87ea-b62ef8ccd600" />

## Quick Start

### 1. Clone repo and start the entire stack
```bash
docker-compose up -d --build
```

### 2. Initialize source data (PostgreSQL e-commerce)
```bash
python scripts/setup_source_db.py
```

- Connects to `localhost:5434` (mapped to container `source-postgres`).
- Creates tables `customers`, `products`, `orders` and generates initial sample data.

### 3. Add 30 days of orders data for sufficient volume + CDC/Flink test cases
```bash
python scripts/ingest_data.py
```

- Generates additional 30 days of orders with various scenarios: high value, high quantity, negative price, zero quantity.

### 4. Backfill ML data for Recommendation (Gold bucket)
```bash
python scripts/create_data_for_recommend.py
```

- Creates train/eval interactions for the last 30 days in:
  - `gold/ml-data/train/date=YYYY-MM-DD/interactions.parquet`
  - `gold/ml-data/eval/date=YYYY-MM-DD/interactions.parquet`

### 5. Train CF model from Gold + log & register to MLflow
```bash
python scripts/train_past_data_for_recommend.py
```

- Creates user–item matrix, item–item similarity.
- Saves model artifacts to MinIO `gold/ml-data/models/...`.
- Logs run to MLflow and registers model `collaborative_filtering_model` stage `Production`.

### 6. Reload model at Recommendation API (after training completes)
```bash
curl -X POST http://localhost:8000/reload
```

### 7. Access main services

- Airflow Web: `http://localhost:8080` (user/pass: `airflow/airflow`).
- MinIO Console: `http://localhost:9001` (minioadmin/minioadmin).
- Trino UI: `http://localhost:8081`.
- Kafka UI: `http://localhost:9080`.
- Debezium: `http://localhost:8083`.
- Dashboard: `http://localhost:8501`.
- MLflow UI: `http://localhost:5000`.
- Recommendation API (docs/health): `http://localhost:8000` or `http://localhost:8000/docs`.

## CDC Setup (Debezium)

### 1. Ensure source Postgres has logical replication enabled
(Already configured in compose; if additional customization needed, see `scripts` or `init-scripts`).

### 2. Create Debezium connector
```bash
chmod +x scripts/setup_debezium_connection.sh
./scripts/setup_debezium_connection.sh
```

- Connector streams `orders` table to Kafka (topic `cdc.public.orders`).
- Can verify in Kafka UI `http://localhost:9080`.

## Run Flink Jobs (Alerts & Telegram)

### 1. Prepare checkpoints directory in container (first time):
```bash
docker exec -u root flink-jobmanager bash -c "mkdir -p /tmp/flink-checkpoints && chown -R flink:flink /tmp/flink-checkpoints && chmod 755 /tmp/flink-checkpoints"
docker exec -u root flink-taskmanager bash -c "mkdir -p /tmp/flink-checkpoints && chown -R flink:flink /tmp/flink-checkpoints && chmod 755 /tmp/flink-checkpoints"
```

### 2. Create alerts topic (if not exists):
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic flink-alerts --partitions 1 --replication-factor 1
```

### 3. Submit Job 1 – Alert Detection:
```bash
docker exec flink-jobmanager /opt/flink/bin/flink run \
  -py /opt/flink/jobs/flink_job_alert_detection_simple.py \
  --bootstrap kafka:9092 \
  --in-topic cdc.public.orders \
  --out-topic flink-alerts
```

### 4. Configure Telegram and submit Job 2 – Telegram Sender:

- Create bot via `@BotFather`, obtain `TELEGRAM_BOT_TOKEN`.
- Get `TELEGRAM_CHAT_ID` by sending `/start` to the bot, then call `getUpdates` API.
- Set environment variables (in `.env` or Docker Compose):
```bash
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

Submit job:
```bash
docker exec flink-jobmanager /opt/flink/bin/flink run \
  -d \
  -py /opt/flink/jobs/flink_job_telegram_sender.py \
  --bootstrap kafka:9092 \
  --topic flink-alerts
```

### 5. Quick test CDC + Flink Alerts pipeline:
```bash
python scripts/test_cdc_alerts.py
```

- Check Kafka topic:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic flink-alerts --from-beginning
```

## Airflow DAGs

- **`medallion_ml_pipeline`**:
  - Batch ETL Postgres → MinIO following Medallion architecture.
  - Can be extended to trigger ML backfill/training.
- **`micro_batch_dashboard_simple`**:
  - Runs by default ~every minute, updates metrics and publishes signal to Redis.
  - Enable in Airflow UI for Dashboard auto-refresh.

## Dashboard (Streamlit)

- Reads data from MinIO bucket `gold/dashboard/metrics/*.parquet`.
- Auto-refreshes based on Redis key `dashboard:last_update`.
- Displays:
  - Overall KPIs,
  - Hourly charts,
  - Top products,
  - Category/Region analysis.

## Recommendation API (FastAPI + MLflow)

- **Main Environment Variables** (see in `docker-compose.yml`):
  - `MLFLOW_TRACKING_URI=http://mlflow-server:5000`
  - `MLFLOW_S3_ENDPOINT_URL=http://minio:9000`
  - `AWS_ACCESS_KEY_ID=minioadmin`, `AWS_SECRET_ACCESS_KEY=minioadmin`
  - Redis: `REDIS_HOST=redis`, `REDIS_PORT=6379`
  - Postgres DB: `DB_HOST=source-postgres`, `DB_PORT=5432`, `DB_NAME=ecommerce`, `DB_USER=app_user`, `DB_PASSWORD=app_password`
  - Model: `MODEL_NAME=collaborative_filtering_model`, `MODEL_STAGE=Production`
  
- **Endpoints**:
  - `GET /health` – service status, model metadata, MLflow URI.
  - `GET /recommend/{customer_id}?top_n=` – recommendations by user (using CF + Redis cache).
  - `GET /similar/products/{product_id}?top_n=` – similar products by product ID based on item–item similarity.
  - `GET /similar/{product_name}?top_n=` – similar products by product name (backward compatible).
  - `GET /available-products?limit=&search=&sort=` – list all products with IDs and popularity.
  - `GET /available-customers?limit=&search=&sort=&min_purchases=` – list all customers in training data.
  - `GET /check/customer/{customer_id}` – check if customer exists in training data.
  - `GET /recommend/fallback/{customer_id}?top_n=` – popular products fallback for unknown customers.
  - `POST /reload` – reload latest model from MLflow + flush cache.
  
- **Quick API Examples**:
  - Check health:
```bash
    curl http://localhost:8000/health
```
  - Get recommendations for user `CUST0001` (Top-10):
```bash
    curl "http://localhost:8000/recommend/CUST0001?top_n=10"
```
  - Get similar products by ID:
```bash
    curl "http://localhost:8000/similar/products/PROD0000?top_n=5"
```
  - Get similar products by name:
```bash
    curl "http://localhost:8000/similar/Widget%20A?top_n=10"
```
  - List available products:
```bash
    curl "http://localhost:8000/available-products?limit=20&sort=popularity"
```
  - Check customer status:
```bash
    curl "http://localhost:8000/check/customer/CUST0001"
```

## Data and Storage Format

- **Storage**: Parquet on MinIO (S3) in buckets:
  - `bronze`: raw data from Postgres or other sources.
  - `silver`: cleaned/normalized data.
  - `gold`: aggregated/ready for analytics & ML data.
- **Partitioning**:
  - Example: `orders/date=YYYY-MM-DD/data.parquet` in Bronze/Silver/Gold.
  - ML data:
    - `gold/ml-data/train/date=YYYY-MM-DD/interactions.parquet`
    - `gold/ml-data/eval/date=YYYY-MM-DD/interactions.parquet`
    - `gold/ml-data/models/date=YYYY-MM-DD/model_*.pkl`
- **Dashboard metrics**:
  - `gold/dashboard/metrics/*.parquet`.

## Query with Trino

- Catalogs in `trino/catalog` already point to:
  - MinIO (data lake),
  - Source Postgres.
- Access UI: `http://localhost:8081`, select catalog/schema to query.

## Important Directories & Files

- `docker-compose.yml`: defines entire stack.
- `dags/medallion_ml_pipeline.py`, `dags/micro_batch_dashboard.py`: main DAGs.
- `dashboard/app.py`: Streamlit Dashboard application.
- `flink-jobs/*.py`: Flink jobs for alert detection and Telegram sending.
- `scripts/setup_source_db.py`, `scripts/ingest_data.py`, `scripts/create_data_for_recommend.py`, `scripts/train_past_data_for_recommend.py`, `scripts/test_cdc_alerts.py`: utilities for data initialization, ML backfill, training & CDC/alerts testing.
- `trino/catalog/*.properties`: MinIO and Postgres connection configuration.
- `mlflow/Dockerfile`: MLflow server image.
- `recommendation/main.py`: Recommendation API (FastAPI + MLflow).
- `recommendation/Dockerfile`: Recommendation API Docker image.

## Python Dependencies (dev/local)

- See `requirements.txt`:
  - pandas, numpy, scikit-learn, sqlalchemy, psycopg2-binary, boto3, mlflow, fastapi, uvicorn, redis, etc.
- Container images already have necessary runtime dependencies installed, only need pip install when running scripts locally.

## Security Notes

- Example values in `docker-compose.yml` (MinIO access keys, DB passwords, Telegram tokens, etc.) are for **demo/local** only.
- For production deployment, use:
  - Secret Manager,
  - Network policies (private subnet, security groups),
  - TLS for MinIO, MLflow, API.

## Quick Troubleshooting

- **Airflow doesn't see DAGs**:
  - Check `dags/` mount and logs in `airflow-scheduler` container.
- **Dashboard has no data**:
  - Enable DAG `micro_batch_dashboard_simple`.
  - Check MinIO bucket `gold/dashboard/metrics/`.
- **No Telegram notifications**:
  - Verify `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` and check logs of `flink_job_telegram_sender.py`.
- **Recommendation API reports model not loaded**:
  - Check MLflow UI for existing run & registered model `collaborative_filtering_model` stage `Production`.
  - Rerun `scripts/train_past_data_for_recommend.py` and call `POST /reload`.
- **Product ID format issues**:
  - Product IDs follow format `PROD0000` (4 digits with leading zeros).
  - Ensure training script generates proper product ID mappings.
  - Check model artifacts include `product_id_to_name.pkl` and `product_name_to_id.pkl`.

## Model Training Workflow

1. **Data Collection**: Run `scripts/ingest_data.py` to generate orders data.
2. **ML Data Preparation**: Run `scripts/create_data_for_recommend.py` to create train/eval splits.
3. **Model Training**: Run `scripts/train_past_data_for_recommend.py` to train and register model.
4. **API Deployment**: API automatically loads Production model on startup.
5. **Model Updates**: After training new model, call `POST /reload` to update API without restart.

