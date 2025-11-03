# dags/recommendation_feature_pipeline.py
"""
Recommendation Feature Pipeline
Extract user-item interactions from Gold layer for Collaborative Filtering
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import boto3
from io import BytesIO
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Clients
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)


def get_db_engine():
    return create_engine('postgresql://app_user:app_password@source-postgres:5432/ecommerce')


def extract_interactions_from_gold(**context):
    """
    Extract user-product interactions from Gold layer
    
    Output: (customer_id, product_name) pairs
    """
    logger.info("=" * 80)
    logger.info("📊 STEP 1: Extracting User-Product Interactions")
    logger.info("=" * 80)
    
    engine = get_db_engine()
    
    # Query to get all purchases
    query = text("""
        SELECT DISTINCT
            customer_id,
            product_name
        FROM orders
        WHERE status IN ('completed', 'processing')
          AND customer_id IS NOT NULL
          AND product_name IS NOT NULL
        ORDER BY customer_id, product_name
    """)
    
    with engine.connect() as conn:
        interactions = pd.read_sql(query, conn)
    
    logger.info(f"✅ Extracted {len(interactions)} interactions")
    logger.info(f"   Unique customers: {interactions['customer_id'].nunique()}")
    logger.info(f"   Unique products: {interactions['product_name'].nunique()}")
    
    # Calculate statistics
    purchases_per_user = interactions.groupby('customer_id').size()
    users_per_product = interactions.groupby('product_name').size()
    
    logger.info(f"\n📊 Statistics:")
    logger.info(f"   Avg purchases per user: {purchases_per_user.mean():.2f}")
    logger.info(f"   Avg users per product: {users_per_product.mean():.2f}")
    logger.info(f"   Max purchases (user): {purchases_per_user.max()}")
    logger.info(f"   Max users (product): {users_per_product.max()}")
    
    # Save raw interactions
    context['ti'].xcom_push(key='interactions', value=interactions.to_json())
    context['ti'].xcom_push(key='n_interactions', value=len(interactions))


def filter_min_support(**context):
    """
    Filter products and users with minimum support
    
    Rules:
    - Products: At least MIN_USERS_PER_PRODUCT users
    - Users: At least MIN_PRODUCTS_PER_USER products
    """
    logger.info("\n" + "=" * 80)
    logger.info("🔍 STEP 2: Filtering by Minimum Support")
    logger.info("=" * 80)
    
    # Configuration
    MIN_USERS_PER_PRODUCT = 3  # Product must be bought by >= 3 users
    MIN_PRODUCTS_PER_USER = 2  # User must buy >= 2 products
    
    logger.info(f"   Min users per product: {MIN_USERS_PER_PRODUCT}")
    logger.info(f"   Min products per user: {MIN_PRODUCTS_PER_USER}")
    
    # Load interactions
    interactions_json = context['ti'].xcom_pull(key='interactions', task_ids='extract_interactions')
    interactions = pd.read_json(interactions_json)
    
    logger.info(f"\n📊 Before filtering:")
    logger.info(f"   Interactions: {len(interactions)}")
    logger.info(f"   Customers: {interactions['customer_id'].nunique()}")
    logger.info(f"   Products: {interactions['product_name'].nunique()}")
    
    # Iterative filtering (products and users influence each other)
    prev_size = 0
    iteration = 0
    
    while len(interactions) != prev_size:
        prev_size = len(interactions)
        iteration += 1
        
        logger.info(f"\n   Iteration {iteration}:")
        
        # Filter products with low support
        product_counts = interactions.groupby('product_name').size()
        valid_products = product_counts[product_counts >= MIN_USERS_PER_PRODUCT].index
        interactions = interactions[interactions['product_name'].isin(valid_products)]
        
        logger.info(f"      After product filter: {len(interactions)} interactions")
        
        # Filter users with few purchases
        user_counts = interactions.groupby('customer_id').size()
        valid_users = user_counts[user_counts >= MIN_PRODUCTS_PER_USER].index
        interactions = interactions[interactions['customer_id'].isin(valid_users)]
        
        logger.info(f"      After user filter: {len(interactions)} interactions")
    
    logger.info(f"\n📊 After filtering:")
    logger.info(f"   Interactions: {len(interactions)}")
    logger.info(f"   Customers: {interactions['customer_id'].nunique()}")
    logger.info(f"   Products: {interactions['product_name'].nunique()}")
    
    # Calculate sparsity
    n_users = interactions['customer_id'].nunique()
    n_products = interactions['product_name'].nunique()
    n_interactions = len(interactions)
    sparsity = 1 - (n_interactions / (n_users * n_products))
    
    logger.info(f"\n📊 Matrix Properties:")
    logger.info(f"   Possible interactions: {n_users * n_products:,}")
    logger.info(f"   Actual interactions: {n_interactions:,}")
    logger.info(f"   Sparsity: {sparsity:.2%}")
    
    context['ti'].xcom_push(key='filtered_interactions', value=interactions.to_json())
    context['ti'].xcom_push(key='n_users', value=n_users)
    context['ti'].xcom_push(key='n_products', value=n_products)


def save_to_minio(**context):
    """Save filtered interactions to MinIO for training"""
    logger.info("\n" + "=" * 80)
    logger.info("💾 STEP 3: Saving to MinIO")
    logger.info("=" * 80)
    
    # Load filtered interactions
    interactions_json = context['ti'].xcom_pull(
        key='filtered_interactions', 
        task_ids='filter_min_support'
    )
    interactions = pd.read_json(interactions_json)
    
    # Add metadata
    interactions['extraction_date'] = datetime.now().date()
    
    # Save as parquet
    parquet_buffer = BytesIO()
    interactions.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    # Create key with date partition
    date_str = datetime.now().strftime('%Y-%m-%d')
    key = f'ml-data/recommendation/interactions/date={date_str}/interactions.parquet'
    
    s3_client.put_object(
        Bucket='gold',
        Key=key,
        Body=parquet_buffer.getvalue()
    )
    
    logger.info(f"✅ Saved to s3://gold/{key}")
    logger.info(f"   Size: {len(interactions)} interactions")
    
    # Also save statistics
    stats = {
        'extraction_date': date_str,
        'n_interactions': len(interactions),
        'n_users': interactions['customer_id'].nunique(),
        'n_products': interactions['product_name'].nunique(),
        'avg_purchases_per_user': interactions.groupby('customer_id').size().mean(),
        'avg_users_per_product': interactions.groupby('product_name').size().mean(),
    }
    
    stats_df = pd.DataFrame([stats])
    stats_buffer = BytesIO()
    stats_df.to_parquet(stats_buffer, index=False)
    stats_buffer.seek(0)
    
    stats_key = f'ml-data/recommendation/stats/date={date_str}/stats.parquet'
    s3_client.put_object(
        Bucket='gold',
        Key=stats_key,
        Body=stats_buffer.getvalue()
    )
    
    logger.info(f"✅ Saved statistics to s3://gold/{stats_key}")
    logger.info("\n" + "=" * 80)
    logger.info("✅ FEATURE PIPELINE COMPLETED!")
    logger.info("=" * 80)


def create_sample_data(**context):
    """
    Create sample interactions data for testing
    (Only for demo - remove in production)
    """
    logger.info("📦 Creating sample data for testing...")
    
    # Sample purchases
    sample_data = pd.DataFrame([
        {'customer_id': 1, 'product_name': 'Laptop'},
        {'customer_id': 1, 'product_name': 'Mouse'},
        {'customer_id': 1, 'product_name': 'Keyboard'},
        {'customer_id': 2, 'product_name': 'Laptop'},
        {'customer_id': 2, 'product_name': 'Monitor'},
        {'customer_id': 3, 'product_name': 'Mouse'},
        {'customer_id': 3, 'product_name': 'Keyboard'},
        {'customer_id': 3, 'product_name': 'USB_Hub'},
        {'customer_id': 4, 'product_name': 'Laptop'},
        {'customer_id': 4, 'product_name': 'Mouse'},
        {'customer_id': 5, 'product_name': 'Keyboard'},
        {'customer_id': 5, 'product_name': 'Monitor'},
        {'customer_id': 5, 'product_name': 'USB_Hub'},
    ])
    
    logger.info(f"✅ Created {len(sample_data)} sample interactions")
    
    context['ti'].xcom_push(key='interactions', value=sample_data.to_json())
    context['ti'].xcom_push(key='n_interactions', value=len(sample_data))


# DAG Definition
default_args = {
    'owner': 'ml-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'recommendation_feature_pipeline',
    default_args=default_args,
    description='Prepare user-product interactions for Collaborative Filtering',
    schedule=timedelta(days=1),  # Daily
    start_date=datetime(2025, 10, 27),
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'recommendation', 'collaborative-filtering'],
) as dag:
    
    # Option 1: Extract from database (production)
    extract = PythonOperator(
        task_id='extract_interactions',
        python_callable=extract_interactions_from_gold,
    )
    
    # Option 2: Create sample data (for testing)
    # Uncomment for demo/testing
    # extract = PythonOperator(
    #     task_id='extract_interactions',
    #     python_callable=create_sample_data,
    # )
    
    filter_task = PythonOperator(
        task_id='filter_min_support',
        python_callable=filter_min_support,
    )
    
    save = PythonOperator(
        task_id='save_to_minio',
        python_callable=save_to_minio,
    )
    
    extract >> filter_task >> save