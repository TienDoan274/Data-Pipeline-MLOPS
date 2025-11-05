Data Pipeline MLOps – E2E Medallion, Streaming Alerts, Dashboard, Recommendation API

Mô tả ngắn
- Hệ thống dữ liệu end-to-end cho e-commerce, bao gồm:
  - Trích xuất – làm sạch – tổng hợp theo mô hình Medallion (Bronze/Silver/Gold) chạy bằng Airflow, lưu trên MinIO (S3).
  - Pipeline micro-batch 30s xuất số liệu cho Dashboard, tự động refresh qua Redis pub/sub.
  - CDC từ PostgreSQL → Kafka qua Debezium, Flink phát hiện cảnh báo và gửi Telegram.
  - Trino để truy vấn dữ liệu trên data lake.
  - Recommendation API (FastAPI) tải model từ Weights & Biases (W&B), cache bằng Redis.

Kiến trúc và dịch vụ
- Airflow: `airflow-webserver`, `airflow-scheduler`, `airflow-postgres` (metadata). Dùng `LocalExecutor` và mount `dags/`.
- Nguồn dữ liệu: `source-postgres` (DB e-commerce: `customers`, `products`, `orders`).
- Data Lake: `minio` (+ `minio-init` tạo bucket `bronze`, `silver`, `gold`, `trino`).
- Streaming: `zookeeper`, `kafka`, `kafka-ui`.
- CDC: `debezium` đọc thay đổi từ `source-postgres` đưa vào Kafka topic.
- Query: `trino` (kết nối MinIO qua `trino/catalog`).
- Dashboard: `dashboard` (Streamlit) đọc metric từ `gold` và nhận thông báo qua Redis.
- Flink: `flink-jobmanager`, `flink-taskmanager` chạy các job Python trong `flink-jobs/`.
- Redis: cache/notification realtime cho Dashboard và Recommendation API.
- W&B self-hosted: `wandb-mysql`, `wandb-server` để lưu model artifact và metadata.
- Recommendation API: `recommendation-api` (FastAPI) phục vụ gợi ý, nạp model từ W&B.

Các thành phần code chính
- Airflow DAGs (`dags/`):
  - `medallion_pipeline.py`: ETL hằng ngày Postgres → MinIO (Bronze → Silver → Gold) với các bước:
    - extract_from_postgres: đọc `orders` theo ngày, ghi Parquet vào bucket `bronze`.
    - clean_orders: áp rule chất lượng dữ liệu (loại thiếu `order_id`, quantity <= 0, giá âm, chuẩn hóa `total`...), ghi `silver`.
    - create_aggregations: tổng hợp `daily_summary`, `category_performance`, `regional_performance` vào `gold`.
    - validate_pipeline: log thống kê, cảnh báo nếu tỉ lệ reject > 10%.
  - `micro_batch_dashboard.py` (simple): mỗi ~phút chạy 3 task:
    - extract_today_metrics: lấy orders hôm nay từ Postgres, ghi `gold/dashboard/orders_today`.
    - compute_dashboard_metrics: tạo các bảng `overall_metrics`, `top_products`, `category_stats`, `regional_stats`, `hourly_stats` vào `gold/dashboard/metrics/*.parquet`.
    - notify_dashboard: publish sự kiện lên Redis channel `dashboard:updates` và cập nhật key `dashboard:last_update`.
- Dashboard (`dashboard/app.py`): Streamlit đọc metric từ MinIO, tự refresh khi có Redis marker. Giao diện gồm Top KPIs, biểu đồ theo giờ, sản phẩm top, phân tích Category/Region.
- Flink jobs (`flink-jobs/`):
  - `flink_job_alert_detection_simple.py`: đọc Kafka topic CDC (JSON unwrapped), lọc rule đơn giản (đơn giá trị lớn, số lượng bất thường, giá âm, quantity <= 0) và ghi ra topic `flink-alerts`.
  - `flink_job_telegram_sender.py`: đọc `flink-alerts` và gửi cảnh báo tới Telegram bằng bot token và chat id.
- Recommendation API (`recommendation/main.py`): FastAPI nạp model CF từ W&B artifact registry, cung cấp endpoints:
  - GET `/health`, `/`: health.
  - GET `/recommend/{customer_id}?top_n=`: gợi ý theo user, có cache Redis 24h.
  - GET `/similar/{product_name}?top_n=`: sản phẩm tương tự.
  - POST `/reload`: reload model từ W&B và xóa cache.

Yêu cầu hệ thống
- Docker, Docker Compose
- Ports trống: 8080 (Airflow), 8081 (Trino UI), 8083 (Debezium), 8084 (W&B), 8501 (Dashboard), 9000/9001 (MinIO), 9092/9093 (Kafka), 9080 (Kafka UI), 6379 (Redis), 5433/5434 (Postgres), 3306 (MySQL)
- Tùy chọn: tài khoản Telegram (BotFather) để nhận cảnh báo; W&B API key

Khởi chạy nhanh
1) Clone repo và bật stack
```
docker-compose up -d --build
```

2) Khởi tạo dữ liệu nguồn (PostgreSQL e-commerce)
```
python scripts/setup_source_db.py
```
Script sẽ kết nối `localhost:5434` (map tới container `source-postgres`), tạo bảng `customers`, `products`, `orders` và sinh dữ liệu 1 tháng gần đây.

3) Truy cập các dịch vụ
- Airflow Web: http://localhost:8080 (user/pass mặc định: airflow/airflow)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- Trino UI: http://localhost:8081
- Kafka UI: http://localhost:9080
- Debezium: http://localhost:8083
- Dashboard: http://localhost:8501
- W&B: http://localhost:8084
- Recommendation API: http://localhost:8000

Thiết lập CDC (Debezium)
1) Bật logical replication cho Postgres nguồn nếu cần (xem `scripts/enable_postgres_cdc.sh`).
2) Tạo connector Debezium:
```
chmod +x scripts/setup_debezium_connection.sh
./scripts/setup_debezium_connection.sh
```
Connector sẽ stream bảng `orders` sang Kafka (ví dụ topic `cdc.public.orders`). Kiểm tra trên Kafka UI.

Chạy Flink jobs (cảnh báo & Telegram)
1) Chuẩn bị thư mục checkpoints trong container (lần đầu, tùy OS có thể bỏ qua nếu đã có):
```
docker exec -u root flink-jobmanager bash -c "mkdir -p /tmp/flink-checkpoints && chown -R flink:flink /tmp/flink-checkpoints && chmod 755 /tmp/flink-checkpoints"
docker exec -u root flink-taskmanager bash -c "mkdir -p /tmp/flink-checkpoints && chown -R flink:flink /tmp/flink-checkpoints && chmod 755 /tmp/flink-checkpoints"
```
2) Tạo topic alerts (nếu chưa có):
```
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic flink-alerts --partitions 1 --replication-factor 1
```
3) Submit Job 1 – Alert Detection:
```
docker exec flink-jobmanager /opt/flink/bin/flink run \
  -py /opt/flink/jobs/flink_job_alert_detection_simple.py \
  --bootstrap kafka:9092 \
  --in-topic cdc.public.orders \
  --out-topic flink-alerts
```
4) Cấu hình Telegram và submit Job 2 – Telegram Sender:
- Tạo bot qua `@BotFather`, lấy `TELEGRAM_BOT_TOKEN`.
- Lấy `TELEGRAM_CHAT_ID` bằng cách nhắn `/start` tới bot rồi gọi API `getUpdates`.
- Đặt biến môi trường trong `.env` hoặc docker-compose (đã wire sẵn):
```
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```
Submit job:
```
docker exec flink-jobmanager /opt/flink/bin/flink run \
  -py -d /opt/flink/jobs/flink_job_telegram_sender.py \
  --bootstrap kafka:9092 \
  --topic flink-alerts
```

Airflow DAGs
- `medallion_postgres_minio_pipeline` (daily): bật trong Airflow UI.
- `micro_batch_dashboard_simple` (mặc định mỗi ~phút): bật để Dashboard tự cập nhật.

Dashboard (Streamlit)
- Ứng dụng đọc metric từ MinIO bucket `gold/dashboard/metrics/*.parquet`.
- Tự động refresh theo Redis key `dashboard:last_update` được publish bởi DAG.
- Màn hình gồm KPIs, chart theo giờ, Top products, Category/Region.

Recommendation API (FastAPI + W&B)
- Biến môi trường chính (đã cấu hình trong compose):
  - `WANDB_BASE_URL=http://wandb-server:8080`
  - `WANDB_API_KEY=<key>`
  - Redis: `REDIS_HOST=redis`, `REDIS_PORT=6379`
- Endpoints:
  - `GET /health` – tình trạng và kích thước model.
  - `GET /recommend/{customer_id}?top_n=` – gợi ý theo user (cache 24h ở Redis).
  - `GET /similar/{product_name}?top_n=` – sản phẩm tương tự.
  - `POST /reload` – reload model từ W&B, flush cache.

Dữ liệu và định dạng lưu trữ
- Storage: Parquet trên MinIO (S3) tại các bucket `bronze`, `silver`, `gold`.
- Tổ chức theo partition key: ví dụ `orders/date=YYYY-MM-DD/data.parquet` ở Bronze/Silver/Gold.
- Metric Dashboard ở `gold/dashboard/metrics/*.parquet`.

Truy vấn với Trino
- Cấu hình catalogs ở `trino/catalog` đã trỏ tới MinIO và Postgres nguồn.
- Truy cập UI: http://localhost:8081, chọn catalog/schema để query.

Thư mục & tệp quan trọng
- `docker-compose.yml`: định nghĩa toàn bộ stack.
- `dags/medallion_pipeline.py`, `dags/micro_batch_dashboard.py`: các DAG chính.
- `dashboard/app.py`: ứng dụng Streamlit.
- `flink-jobs/*.py`: job Flink phát hiện cảnh báo và gửi Telegram.
- `scripts/*.sh`, `scripts/setup_source_db.py`: tiện ích khởi tạo dữ liệu, CDC.
- `trino/catalog/*.properties`: cấu hình kết nối MinIO, Postgres.

Phụ thuộc Python (dev/local)
- Xem `requirements.txt` (scikit-learn, pandas, numpy, scipy, matplotlib, seaborn, sqlalchemy, psycopg2-binary, boto3...). Containers đã cài sẵn những thứ cần thiết cho runtime.

Ghi chú bảo mật
- Các biến ví dụ (W&B API key, Telegram) trong compose chỉ phục vụ demo/local. Khi triển khai thật, thay bằng secret manager và network/policy phù hợp.

Khắc phục sự cố nhanh
- Airflow không thấy DAG: kiểm tra mount `dags/` và log trong container scheduler.
- Dashboard không có dữ liệu: bật DAG `micro_batch_dashboard_simple` và kiểm tra MinIO bucket `gold/dashboard/metrics/`.
- Không nhận Telegram: kiểm tra biến môi trường bot, chat id, và log job `flink_job_telegram_sender.py`.
- Recommendation API báo model chưa nạp: kiểm tra `WANDB_API_KEY`, artifact tồn tại trong registry và log khởi động.

License
- Dự án dùng cho mục đích học tập/demo.
