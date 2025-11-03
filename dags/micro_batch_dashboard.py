# dags/micro_batch_dashboard_simple.py
"""
SIMPLE Micro-batch pipeline
- Fixed 30-second schedule
- Publishes to Redis pub/sub when done
- Dashboard auto-refreshes on notification
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import boto3
from io import BytesIO
from sqlalchemy import create_engine, text
import redis
import json

logger = logging.getLogger(__name__)

# MinIO client
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# Redis client
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)


def get_db_engine():
    """PostgreSQL connection"""
    return create_engine('postgresql://app_user:app_password@source-postgres:5432/ecommerce')


def extract_today_metrics(**context):
    """Extract today's orders for dashboard"""
    today = datetime.now().date()
    logger.info(f"📊 Extracting metrics for {today}")
    
    engine = get_db_engine()
    
    query = text("""
        SELECT 
            order_id, order_date, customer_id, product_id, category,
            product_name, price, quantity, total, status,
            payment_method, region, created_at, updated_at
        FROM orders
        WHERE DATE(order_date) = CURRENT_DATE
        ORDER BY order_date DESC
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    if df.empty:
        logger.warning(f"⚠️  No data for today")
        context['ti'].xcom_push(key='today_count', value=0)
        return
    
    logger.info(f"📦 Extracted {len(df)} orders for today")
    
    df['_extraction_time'] = datetime.now()
    df['date'] = str(today)
    
    # Save to MinIO
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    key = f'dashboard/orders_today/data.parquet'
    s3_client.put_object(Bucket='gold', Key=key, Body=parquet_buffer.getvalue())
    
    logger.info(f"✅ Uploaded to s3://gold/{key}")
    context['ti'].xcom_push(key='today_count', value=len(df))


def compute_dashboard_metrics(**context):
    """Compute aggregated metrics for dashboard"""
    logger.info("📊 Computing dashboard metrics...")
    
    try:
        obj = s3_client.get_object(Bucket='gold', Key='dashboard/orders_today/data.parquet')
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except:
        logger.warning("⚠️  No data found")
        return
    
    df_valid = df[df['status'].isin(['completed', 'processing'])]
    
    # Overall metrics
    metrics = {
        'total_orders': len(df_valid),
        'total_revenue': float(df_valid['total'].sum()),
        'avg_order_value': float(df_valid['total'].mean()) if len(df_valid) > 0 else 0,
        'unique_customers': df_valid['customer_id'].nunique(),
        'last_updated': datetime.now().isoformat()
    }
    
    # Top products
    top_products = df_valid.groupby('product_name').agg({
        'order_id': 'count',
        'quantity': 'sum',
        'total': 'sum'
    }).reset_index()
    top_products.columns = ['product_name', 'order_count', 'quantity_sold', 'revenue']
    top_products = top_products.sort_values('revenue', ascending=False).head(10)
    
    # Category stats
    category_stats = df_valid.groupby('category').agg({
        'order_id': 'count',
        'total': 'sum',
        'quantity': 'sum'
    }).reset_index()
    category_stats.columns = ['category', 'order_count', 'revenue', 'quantity_sold']
    
    # Regional stats
    regional_stats = df_valid.groupby('region').agg({
        'order_id': 'count',
        'total': 'sum'
    }).reset_index()
    regional_stats.columns = ['region', 'order_count', 'revenue']
    
    # Hourly stats
    df_valid['hour'] = pd.to_datetime(df_valid['order_date']).dt.hour
    hourly_stats = df_valid.groupby('hour').agg({
        'order_id': 'count',
        'total': 'sum'
    }).reset_index()
    hourly_stats.columns = ['hour', 'order_count', 'revenue']
    
    # Save all metrics to MinIO
    metrics_data = {
        'overall_metrics': pd.DataFrame([metrics]),
        'top_products': top_products,
        'category_stats': category_stats,
        'regional_stats': regional_stats,
        'hourly_stats': hourly_stats
    }
    
    for name, data_df in metrics_data.items():
        parquet_buffer = BytesIO()
        data_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        
        key = f'dashboard/metrics/{name}.parquet'
        s3_client.put_object(Bucket='gold', Key=key, Body=parquet_buffer.getvalue())
        logger.info(f"✅ Saved {name}")
    
    logger.info(f"✅ Metrics computed - Revenue: ${metrics['total_revenue']:,.2f}, Orders: {metrics['total_orders']}")
    
    # Store metrics for notification
    context['ti'].xcom_push(key='metrics', value=metrics)


def notify_dashboard(**context):
    """
    Publish notification to Redis pub/sub channel
    Dashboard will listen and auto-refresh
    """
    try:
        # Get metrics from previous task
        metrics = context['ti'].xcom_pull(key='metrics', task_ids='compute_dashboard_metrics')
        
        # Create notification
        notification = {
            'event': 'data_updated',
            'timestamp': datetime.now().isoformat(),
            'run_id': context['dag_run'].run_id,
            'metrics': metrics if metrics else {}
        }
        
        # Publish to Redis pub/sub channel
        redis_client.publish('dashboard:updates', json.dumps(notification))
        logger.info(f"📢 Published notification to dashboard")
        
        # Also update last update marker (for dashboard to check)
        redis_client.set('dashboard:last_update', datetime.now().isoformat())
        redis_client.expire('dashboard:last_update', 3600)  # 1 hour TTL
        
        # Cache metrics for quick dashboard access
        redis_client.set('dashboard:latest_metrics', json.dumps(metrics if metrics else {}))
        redis_client.expire('dashboard:latest_metrics', 300)  # 5 min TTL
        
        logger.info("✅ Dashboard notification sent")
        
    except Exception as e:
        logger.error(f"❌ Failed to notify dashboard: {e}")


# DAG definition - runs every 30 seconds
default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    'micro_batch_dashboard_simple',
    default_args=default_args,
    description='Simple micro-batch - 30s schedule with Redis pub/sub notification',
    schedule_interval='*/1 * * * *',  # Every 30 seconds
    start_date=datetime(2025, 10, 27),
    catchup=False,
    max_active_runs=1,
    tags=['dashboard', 'micro-batch', 'simple'],
) as dag:
    
    extract = PythonOperator(
        task_id='extract_today_metrics',
        python_callable=extract_today_metrics,
    )
    
    compute = PythonOperator(
        task_id='compute_dashboard_metrics',
        python_callable=compute_dashboard_metrics,
    )
    
    notify = PythonOperator(
        task_id='notify_dashboard',
        python_callable=notify_dashboard,
    )
    
    extract >> compute >> notify