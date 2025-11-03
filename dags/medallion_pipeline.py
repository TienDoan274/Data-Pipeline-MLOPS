# dags/medallion_postgres_to_minio.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import boto3
from io import BytesIO
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# MinIO S3 client
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

def get_db_engine():
    """Create PostgreSQL engine"""
    return create_engine('postgresql://app_user:app_password@source-postgres:5432/ecommerce')

# dags/medallion_pipeline.py - FIX query
def extract_orders_from_postgres(**context):
    """Bronze Layer: Extract orders from PostgreSQL"""
    execution_date = context['ds']
    logger.info(f"📊 Extracting orders for {execution_date}")
    
    engine = get_db_engine()
    
    # ← Match exact column names from database
    query = text("""
        SELECT 
            order_id,
            order_date,
            customer_id,
            product_id,
            category,
            product_name,
            price,
            quantity,
            total,
            status,
            payment_method,
            region,
            created_at,
            updated_at
        FROM orders
        WHERE DATE(order_date) = :execution_date
        ORDER BY order_date
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'execution_date': execution_date})
    
    if df.empty:
        logger.warning(f"⚠️  No data found for {execution_date}")
        context['ti'].xcom_push(key='bronze_count', value=0)
        return
    
    logger.info(f"📦 Extracted {len(df)} orders from PostgreSQL")
    
    # Add bronze metadata
    df['_ingestion_timestamp'] = datetime.now()
    df['_execution_date'] = execution_date
    df['_source_system'] = 'postgresql'
    
    # Write to MinIO
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    bucket = 'bronze'
    key = f'orders/date={execution_date}/data.parquet'
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue()
    )
    
    logger.info(f"✅ Uploaded {len(df)} records to s3://{bucket}/{key}")
    
    context['ti'].xcom_push(key='bronze_count', value=len(df))
    context['ti'].xcom_push(key='s3_path', value=f's3://{bucket}/{key}')
    context['ti'].xcom_push(key='total_revenue', value=float(df['total'].sum()))

def clean_orders_in_minio(**context):
    """
    Silver Layer: Read from Bronze, clean, write to Silver
    """
    execution_date = context['ds']
    logger.info(f"🔧 Cleaning orders for {execution_date}")
    
    # Read from Bronze
    bronze_bucket = 'bronze'
    bronze_key = f'orders/date={execution_date}/data.parquet'
    
    try:
        obj = s3_client.get_object(Bucket=bronze_bucket, Key=bronze_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"⚠️  No bronze data found for {execution_date}")
        return
    
    logger.info(f"📊 Read {len(df)} records from Bronze")
    
    # Data Quality Rules
    initial_count = len(df)
    
    # Remove records with missing order_id
    df = df[df['order_id'].notna()]
    
    # Remove invalid quantities
    df = df[df['quantity'] > 0]
    
    # Fix negative prices (data error)
    df['price'] = df['price'].abs()
    
    # Remove records with missing status
    df = df[df['status'].notna() & (df['status'] != '')]
    
    # Recalculate total (fix calculation errors)
    df['total_calculated'] = df['price'] * df['quantity']
    df['total_original'] = df['total']
    df['total'] = df['total_calculated']
    
    # Add data quality flags
    df['_has_calculation_error'] = (
        abs(df['total_calculated'] - df['total_original']) > 0.01
    )
    
    # Add cleaning metadata
    df['_cleaned_timestamp'] = datetime.now()
    df['_records_rejected'] = initial_count - len(df)
    
    # Drop temporary columns
    df = df.drop(['total_calculated', 'total_original'], axis=1)
    
    rejected_count = initial_count - len(df)
    logger.info(f"🔧 Cleaned: {len(df)} records (rejected: {rejected_count})")
    
    # Write to Silver
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    silver_bucket = 'silver'
    silver_key = f'orders/date={execution_date}/data.parquet'
    
    s3_client.put_object(
        Bucket=silver_bucket,
        Key=silver_key,
        Body=parquet_buffer.getvalue()
    )
    
    logger.info(f"✅ Uploaded to s3://{silver_bucket}/{silver_key}")
    
    context['ti'].xcom_push(key='silver_count', value=len(df))
    context['ti'].xcom_push(key='rejected_count', value=rejected_count)

def create_gold_aggregations(**context):
    """
    Gold Layer: Create business aggregations
    """
    execution_date = context['ds']
    logger.info(f"📊 Creating aggregations for {execution_date}")
    
    # Read from Silver
    silver_bucket = 'silver'
    silver_key = f'orders/date={execution_date}/data.parquet'
    
    try:
        obj = s3_client.get_object(Bucket=silver_bucket, Key=silver_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"⚠️  No silver data found for {execution_date}")
        return
    
    # 1. Daily Sales Summary
    daily_summary = pd.DataFrame([{
        'date': execution_date,
        'total_orders': len(df),
        'total_revenue': df['total'].sum(),
        'total_quantity': df['quantity'].sum(),
        'avg_order_value': df['total'].mean(),
        'unique_customers': df['customer_id'].nunique(),
        '_created_at': datetime.now()
    }])
    
    # 2. Category Performance
    category_stats = df.groupby('category').agg({
        'order_id': 'count',
        'total': 'sum',
        'quantity': 'sum'
    }).reset_index()
    category_stats.columns = ['category', 'order_count', 'revenue', 'quantity_sold']
    category_stats['date'] = execution_date
    category_stats['_created_at'] = datetime.now()
    
    # 3. Regional Performance
    regional_stats = df.groupby('region').agg({
        'order_id': 'count',
        'total': 'sum'
    }).reset_index()
    regional_stats.columns = ['region', 'order_count', 'revenue']
    regional_stats['date'] = execution_date
    regional_stats['_created_at'] = datetime.now()
    
    # Upload aggregations to Gold
    gold_bucket = 'gold'
    
    aggregations = {
        'daily_summary': daily_summary,
        'category_performance': category_stats,
        'regional_performance': regional_stats
    }
    
    for name, agg_df in aggregations.items():
        parquet_buffer = BytesIO()
        agg_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        
        gold_key = f'{name}/date={execution_date}/data.parquet'
        
        s3_client.put_object(
            Bucket=gold_bucket,
            Key=gold_key,
            Body=parquet_buffer.getvalue()
        )
        
        logger.info(f"✅ Created {name}: s3://{gold_bucket}/{gold_key}")
    
    logger.info(f"📈 Summary - Revenue: ${daily_summary['total_revenue'].iloc[0]:,.2f}, "
                f"Orders: {daily_summary['total_orders'].iloc[0]}")

def validate_pipeline(**context):
    """
    Validation: Check data quality across layers
    """
    execution_date = context['ds']
    ti = context['ti']
    
    bronze_count = ti.xcom_pull(task_ids='extract_from_postgres', key='bronze_count') or 0
    silver_count = ti.xcom_pull(task_ids='clean_orders', key='silver_count') or 0
    rejected = ti.xcom_pull(task_ids='clean_orders', key='rejected_count') or 0
    
    logger.info(f"🔍 Pipeline Validation for {execution_date}")
    logger.info(f"  Bronze: {bronze_count} records")
    logger.info(f"  Silver: {silver_count} records")
    logger.info(f"  Rejected: {rejected} records ({rejected/bronze_count*100:.1f}%)")
    
    # Quality threshold
    if bronze_count > 0 and rejected / bronze_count > 0.1:
        logger.warning(f"⚠️  High rejection rate: {rejected/bronze_count*100:.1f}%")
    
    logger.info("✅ Validation complete")

# DAG Definition
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'medallion_postgres_minio_pipeline',
    default_args=default_args,
    description='Incremental ETL: PostgreSQL → MinIO (Bronze/Silver/Gold)',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,  # Enable backfilling
    max_active_runs=10000,
    tags=['minio', 'postgresql', 'medallion', 'etl'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_orders_from_postgres,
    )
    
    clean_task = PythonOperator(
        task_id='clean_orders',
        python_callable=clean_orders_in_minio,
    )
    
    aggregate_task = PythonOperator(
        task_id='create_aggregations',
        python_callable=create_gold_aggregations,
    )
    
    validate_task = PythonOperator(
        task_id='validate_pipeline',
        python_callable=validate_pipeline,
    )
    
    extract_task >> clean_task >> aggregate_task >> validate_task