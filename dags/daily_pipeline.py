# dags/medallion_ml_pipeline.py
"""
Enhanced Medallion Pipeline with ML Training & Evaluation
- Daily ETL: Bronze → Silver → Gold
- Train/Eval Split: 80/20 with daily accumulation
- Model Training: After aggregations complete
- Evaluation: On accumulated eval set
- Checkpoint: Best model persistence
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import boto3
from io import BytesIO
from sqlalchemy import create_engine, text
import pickle
import json
import wandb
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split

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


# ============================================================================
# PHASE 1: MEDALLION ETL (Bronze → Silver → Gold)
# ============================================================================

def extract_orders_from_postgres(**context):
    """Bronze Layer: Extract daily orders"""
    execution_date = context['ds']
    logger.info(f"📊 [BRONZE] Extracting orders for {execution_date}")
    
    engine = get_db_engine()
    
    query = text("""
        SELECT 
            order_id, order_date, customer_id, product_id,
            category, product_name, price, quantity, total,
            status, payment_method, region, created_at, updated_at
        FROM orders
        WHERE DATE(order_date) = :execution_date
            AND status IN ('completed', 'processing')  -- Only successful orders
        ORDER BY order_date
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'execution_date': execution_date})
    
    if df.empty:
        logger.warning(f"⚠️  No data found for {execution_date}")
        context['ti'].xcom_push(key='bronze_count', value=0)
        return
    
    logger.info(f"📦 Extracted {len(df)} orders")
    
    # Add metadata
    df['_ingestion_timestamp'] = datetime.now()
    df['_execution_date'] = execution_date
    
    # Write to Bronze
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    bucket = 'bronze'
    key = f'orders/date={execution_date}/data.parquet'
    
    s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
    
    logger.info(f"✅ Uploaded to s3://{bucket}/{key}")
    context['ti'].xcom_push(key='bronze_count', value=len(df))


def clean_orders_in_minio(**context):
    """Silver Layer: Clean and validate"""
    execution_date = context['ds']
    logger.info(f"🔧 [SILVER] Cleaning orders for {execution_date}")
    
    # Read from Bronze
    bronze_key = f'orders/date={execution_date}/data.parquet'
    
    try:
        obj = s3_client.get_object(Bucket='bronze', Key=bronze_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except:
        logger.warning(f"⚠️  No bronze data for {execution_date}")
        return
    
    initial_count = len(df)
    
    # Cleaning rules
    df = df[df['order_id'].notna()]
    df = df[df['quantity'] > 0]
    df['price'] = df['price'].abs()
    df = df[df['status'].notna() & (df['status'] != '')]
    
    # Recalculate total
    df['total'] = df['price'] * df['quantity']
    df['_cleaned_timestamp'] = datetime.now()
    
    rejected = initial_count - len(df)
    logger.info(f"🔧 Cleaned: {len(df)} records (rejected: {rejected})")
    
    # Write to Silver
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    silver_key = f'orders/date={execution_date}/data.parquet'
    s3_client.put_object(Bucket='silver', Key=silver_key, Body=parquet_buffer.getvalue())
    
    context['ti'].xcom_push(key='silver_count', value=len(df))


def create_gold_aggregations(**context):
    """Gold Layer: Business aggregations"""
    execution_date = context['ds']
    logger.info(f"📊 [GOLD] Creating aggregations for {execution_date}")
    
    # Read from Silver
    silver_key = f'orders/date={execution_date}/data.parquet'
    
    try:
        obj = s3_client.get_object(Bucket='silver', Key=silver_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except:
        return
    
    # Aggregations
    daily_summary = pd.DataFrame([{
        'date': execution_date,
        'total_orders': len(df),
        'total_revenue': df['total'].sum(),
        'unique_customers': df['customer_id'].nunique(),
        'unique_products': df['product_name'].nunique(),
        '_created_at': datetime.now()
    }])
    
    category_stats = df.groupby('category').agg({
        'order_id': 'count',
        'total': 'sum'
    }).reset_index()
    category_stats.columns = ['category', 'order_count', 'revenue']
    category_stats['date'] = execution_date
    
    # Upload to Gold
    for name, agg_df in [('daily_summary', daily_summary), ('category_performance', category_stats)]:
        buffer = BytesIO()
        agg_df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        key = f'{name}/date={execution_date}/data.parquet'
        s3_client.put_object(Bucket='gold', Key=key, Body=buffer.getvalue())
        logger.info(f"✅ Created {name}")


# ============================================================================
# PHASE 2: ML DATA PREPARATION (Train/Eval Split)
# ============================================================================

def prepare_ml_interactions(**context):
    """
    Extract user-product interactions from Gold layer
    Split into Train (80%) and Eval (20%) sets
    Eval set accumulates daily for continuous evaluation
    """
    execution_date = context['ds']
    logger.info(f"\n{'='*80}")
    logger.info(f"🎯 [ML PREP] Preparing interactions for {execution_date}")
    logger.info(f"{'='*80}")
    
    # Read from Silver (cleaned orders)
    silver_key = f'orders/date={execution_date}/data.parquet'
    
    try:
        obj = s3_client.get_object(Bucket='silver', Key=silver_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except:
        logger.warning("No data for ML preparation")
        return
    
    # Extract interactions (customer_id, product_name)
    interactions = df[['customer_id', 'product_name']].drop_duplicates()
    
    logger.info(f"📊 Extracted {len(interactions)} unique interactions")
    logger.info(f"   Customers: {interactions['customer_id'].nunique()}")
    logger.info(f"   Products: {interactions['product_name'].nunique()}")
    
    # Add metadata
    interactions['date'] = execution_date
    interactions['timestamp'] = datetime.now()
    
    # ========================================================================
    # TRAIN/EVAL SPLIT: 80/20
    # ========================================================================
    
    # Stratified split by customer (ensure each customer in train has samples)
    train_interactions, eval_interactions = train_test_split(
        interactions,
        test_size=0.2,
        random_state=42,
        stratify=interactions['customer_id'].map(
            lambda x: x if interactions['customer_id'].value_counts()[x] > 1 else 'other'
        )
    )
    
    logger.info(f"\n📊 Train/Eval Split:")
    logger.info(f"   Train: {len(train_interactions)} ({len(train_interactions)/len(interactions)*100:.1f}%)")
    logger.info(f"   Eval:  {len(eval_interactions)} ({len(eval_interactions)/len(interactions)*100:.1f}%)")
    
    # ========================================================================
    # SAVE TRAIN SET (for today's training)
    # ========================================================================
    
    train_buffer = BytesIO()
    train_interactions.to_parquet(train_buffer, index=False)
    train_buffer.seek(0)
    
    train_key = f'ml-data/recommendation/train/date={execution_date}/interactions.parquet'
    s3_client.put_object(Bucket='gold', Key=train_key, Body=train_buffer.getvalue())
    
    logger.info(f"✅ Saved train set: s3://gold/{train_key}")
    
    # ========================================================================
    # ACCUMULATE EVAL SET (continuous evaluation)
    # ========================================================================
    
    # Load previous eval sets
    accumulated_eval = eval_interactions.copy()
    
    try:
        # List all previous eval files
        response = s3_client.list_objects_v2(
            Bucket='gold',
            Prefix='ml-data/recommendation/eval/'
        )
        
        if 'Contents' in response:
            logger.info(f"\n📦 Loading previous eval sets...")
            
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    prev_obj = s3_client.get_object(Bucket='gold', Key=obj['Key'])
                    prev_df = pd.read_parquet(BytesIO(prev_obj['Body'].read()))
                    accumulated_eval = pd.concat([accumulated_eval, prev_df], ignore_index=True)
            
            # Remove duplicates (keep latest)
            accumulated_eval = accumulated_eval.drop_duplicates(
                subset=['customer_id', 'product_name'],
                keep='last'
            )
            
            logger.info(f"   Accumulated eval set: {len(accumulated_eval)} interactions")
    except Exception as e:
        logger.warning(f"   No previous eval data or error: {e}")
    
    # Save accumulated eval set
    eval_buffer = BytesIO()
    accumulated_eval.to_parquet(eval_buffer, index=False)
    eval_buffer.seek(0)
    
    eval_key = f'ml-data/recommendation/eval/date={execution_date}/interactions.parquet'
    s3_client.put_object(Bucket='gold', Key=eval_key, Body=eval_buffer.getvalue())
    
    logger.info(f"✅ Saved accumulated eval set: s3://gold/{eval_key}")
    
    # Save statistics
    stats = {
        'date': execution_date,
        'train_size': len(train_interactions),
        'eval_size': len(eval_interactions),
        'accumulated_eval_size': len(accumulated_eval),
        'n_customers_train': int(train_interactions['customer_id'].nunique()),
        'n_products_train': int(train_interactions['product_name'].nunique()),
        'n_customers_eval': int(accumulated_eval['customer_id'].nunique()),
        'n_products_eval': int(accumulated_eval['product_name'].nunique()),
    }
    
    context['ti'].xcom_push(key='ml_stats', value=stats)
    
    logger.info(f"\n📊 ML Data Statistics:")
    for k, v in stats.items():
        logger.info(f"   {k}: {v}")


# ============================================================================
# PHASE 3: MODEL TRAINING
# ============================================================================

def train_recommendation_model(**context):
    """
    Train Item-Based Collaborative Filtering model
    Load checkpoint if exists, otherwise train from scratch
    """
    execution_date = context['ds']
    logger.info(f"\n{'='*80}")
    logger.info(f"🧠 [TRAINING] Training model for {execution_date}")
    logger.info(f"{'='*80}")
    
    # ========================================================================
    # LOAD TRAINING DATA
    # ========================================================================
    
    train_key = f'ml-data/recommendation/train/date={execution_date}/interactions.parquet'
    
    try:
        obj = s3_client.get_object(Bucket='gold', Key=train_key)
        train_df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except:
        logger.error("❌ No training data found!")
        return
    
    logger.info(f"📦 Loaded training data: {len(train_df)} interactions")
    
    # ========================================================================
    # CHECK FOR EXISTING CHECKPOINT
    # ========================================================================
    
    checkpoint_key = 'ml-data/recommendation/checkpoints/best_model.pkl'
    checkpoint_exists = False
    previous_metrics = None
    
    try:
        obj = s3_client.get_object(Bucket='gold', Key=checkpoint_key)
        checkpoint = pickle.loads(obj['Body'].read())
        
        previous_metrics = checkpoint.get('metrics', {})
        checkpoint_exists = True
        
        logger.info(f"\n✅ Found checkpoint:")
        logger.info(f"   Version: {checkpoint.get('version')}")
        logger.info(f"   Date: {checkpoint.get('trained_date')}")
        logger.info(f"   Eval Coverage: {previous_metrics.get('eval_coverage', 0):.2%}")
        
    except:
        logger.info("\n📝 No checkpoint found - training from scratch")
    
    # ========================================================================
    # BUILD USER-ITEM MATRIX
    # ========================================================================
    
    logger.info(f"\n🔨 Building user-item matrix...")
    
    # Create mappings
    unique_users = train_df['customer_id'].unique()
    unique_products = train_df['product_name'].unique()
    
    user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
    item_to_idx = {item: idx for idx, item in enumerate(unique_products)}
    idx_to_user = {idx: user for user, idx in user_to_idx.items()}
    idx_to_item = {idx: item for item, idx in item_to_idx.items()}
    
    # Build binary matrix
    n_users = len(user_to_idx)
    n_items = len(item_to_idx)
    user_item_matrix = np.zeros((n_users, n_items))
    
    for _, row in train_df.iterrows():
        user_idx = user_to_idx[row['customer_id']]
        item_idx = item_to_idx[row['product_name']]
        user_item_matrix[user_idx, item_idx] = 1
    
    sparsity = 1 - (np.count_nonzero(user_item_matrix) / (n_users * n_items))
    
    logger.info(f"   Matrix shape: {user_item_matrix.shape}")
    logger.info(f"   Sparsity: {sparsity:.2%}")
    
    # ========================================================================
    # COMPUTE ITEM SIMILARITY
    # ========================================================================
    
    logger.info(f"\n🔍 Computing item-item similarity...")
    
    item_user_matrix = user_item_matrix.T
    item_similarity_matrix = cosine_similarity(item_user_matrix)
    np.fill_diagonal(item_similarity_matrix, 0)
    
    logger.info(f"   Similarity matrix: {item_similarity_matrix.shape}")
    logger.info(f"   Mean similarity: {item_similarity_matrix.mean():.4f}")
    
    # ========================================================================
    # SAVE MODEL ARTIFACTS
    # ========================================================================
    
    model_version = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    model_artifacts = {
        'version': model_version,
        'trained_date': execution_date,
        'user_item_matrix': user_item_matrix,
        'item_similarity_matrix': item_similarity_matrix,
        'user_to_idx': user_to_idx,
        'item_to_idx': item_to_idx,
        'idx_to_user': idx_to_user,
        'idx_to_item': idx_to_item,
        'n_train_interactions': len(train_df),
        'sparsity': float(sparsity),
    }
    
    # Save to MinIO
    model_buffer = BytesIO()
    pickle.dump(model_artifacts, model_buffer)
    model_buffer.seek(0)
    
    model_key = f'ml-data/recommendation/models/date={execution_date}/model_{model_version}.pkl'
    s3_client.put_object(Bucket='gold', Key=model_key, Body=model_buffer.getvalue())
    
    logger.info(f"✅ Saved model: s3://gold/{model_key}")
    
    context['ti'].xcom_push(key='model_key', value=model_key)
    context['ti'].xcom_push(key='model_version', value=model_version)


# ============================================================================
# PHASE 4: MODEL EVALUATION
# ============================================================================

def evaluate_model(**context):
    """
    Evaluate model on accumulated eval set
    Compare with checkpoint and save if better
    """
    execution_date = context['ds']
    model_key = context['ti'].xcom_pull(key='model_key', task_ids='train_model')
    
    logger.info(f"\n{'='*80}")
    logger.info(f"📊 [EVALUATION] Evaluating model for {execution_date}")
    logger.info(f"{'='*80}")
    
    # ========================================================================
    # LOAD MODEL
    # ========================================================================
    
    obj = s3_client.get_object(Bucket='gold', Key=model_key)
    model = pickle.loads(obj['Body'].read())
    
    user_item_matrix = model['user_item_matrix']
    item_similarity_matrix = model['item_similarity_matrix']
    user_to_idx = model['user_to_idx']
    item_to_idx = model['item_to_idx']
    
    # ========================================================================
    # LOAD ACCUMULATED EVAL SET
    # ========================================================================
    
    eval_key = f'ml-data/recommendation/eval/date={execution_date}/interactions.parquet'
    
    try:
        obj = s3_client.get_object(Bucket='gold', Key=eval_key)
        eval_df = pd.read_parquet(BytesIO(obj['Body'].read()))
    except:
        logger.error("❌ No eval data found!")
        return
    
    logger.info(f"📦 Loaded eval data: {len(eval_df)} interactions")
    
    # ========================================================================
    # EVALUATE METRICS
    # ========================================================================
    
    logger.info(f"\n🎯 Computing evaluation metrics...")
    
    # 1. Coverage: % of eval users/products covered by training
    eval_users_in_train = eval_df['customer_id'].isin(user_to_idx.keys()).sum()
    eval_products_in_train = eval_df['product_name'].isin(item_to_idx.keys()).sum()
    
    user_coverage = eval_users_in_train / len(eval_df) if len(eval_df) > 0 else 0
    product_coverage = eval_products_in_train / len(eval_df) if len(eval_df) > 0 else 0
    overall_coverage = (eval_users_in_train + eval_products_in_train) / (2 * len(eval_df)) if len(eval_df) > 0 else 0
    
    # 2. Recommendation Quality
    # Sample 100 users from eval set that exist in training
    eval_users_available = eval_df[eval_df['customer_id'].isin(user_to_idx.keys())]['customer_id'].unique()
    
    if len(eval_users_available) > 0:
        sample_size = min(100, len(eval_users_available))
        sample_users = np.random.choice(eval_users_available, sample_size, replace=False)
        
        total_precision = 0
        valid_users = 0
        
        for customer_id in sample_users:
            # Get user's eval purchases
            eval_purchases = set(eval_df[eval_df['customer_id'] == customer_id]['product_name'].values)
            
            # Generate recommendations
            user_idx = user_to_idx[customer_id]
            user_purchases = np.where(user_item_matrix[user_idx] > 0)[0]
            
            if len(user_purchases) == 0:
                continue
            
            # Calculate scores
            scores = np.zeros(len(item_to_idx))
            for item_idx in range(len(item_to_idx)):
                for purchased_idx in user_purchases:
                    scores[item_idx] += item_similarity_matrix[item_idx, purchased_idx]
            
            # Filter out training purchases
            for purchased_idx in user_purchases:
                scores[purchased_idx] = -999
            
            # Top 10 recommendations
            top_10_indices = np.argsort(scores)[::-1][:10]
            top_10_products = [model['idx_to_item'][str(idx)] for idx in top_10_indices if scores[idx] > 0]
            
            # Precision@10: % of recommended items in eval set
            hits = len(set(top_10_products) & eval_purchases)
            precision = hits / len(top_10_products) if len(top_10_products) > 0 else 0
            
            total_precision += precision
            valid_users += 1
        
        avg_precision = total_precision / valid_users if valid_users > 0 else 0
    else:
        avg_precision = 0
    
    # 3. Model Statistics
    n_users = len(user_to_idx)
    n_items = len(item_to_idx)
    sparsity = model['sparsity']
    
    metrics = {
        'date': execution_date,
        'model_version': model['version'],
        
        # Coverage metrics
        'eval_size': len(eval_df),
        'eval_user_coverage': float(user_coverage),
        'eval_product_coverage': float(product_coverage),
        'eval_coverage': float(overall_coverage),
        
        # Quality metrics
        'precision_at_10': float(avg_precision),
        
        # Model stats
        'n_users': int(n_users),
        'n_items': int(n_items),
        'sparsity': float(sparsity),
        
        'evaluated_at': datetime.now().isoformat()
    }
    
    logger.info(f"\n📊 Evaluation Results:")
    logger.info(f"   Eval Coverage: {metrics['eval_coverage']:.2%}")
    logger.info(f"   User Coverage: {metrics['eval_user_coverage']:.2%}")
    logger.info(f"   Product Coverage: {metrics['eval_product_coverage']:.2%}")
    logger.info(f"   Precision@10: {metrics['precision_at_10']:.4f}")
    logger.info(f"   Model Size: {n_users} users × {n_items} items")
    logger.info(f"   Sparsity: {sparsity:.2%}")
    
    # ========================================================================
    # COMPARE WITH CHECKPOINT AND SAVE IF BETTER
    # ========================================================================
    
    should_save_checkpoint = False
    
    try:
        # Load checkpoint metrics
        checkpoint_key = 'ml-data/recommendation/checkpoints/best_model.pkl'
        obj = s3_client.get_object(Bucket='gold', Key=checkpoint_key)
        checkpoint = pickle.loads(obj['Body'].read())
        
        prev_metrics = checkpoint.get('metrics', {})
        prev_coverage = prev_metrics.get('eval_coverage', 0)
        prev_precision = prev_metrics.get('precision_at_10', 0)
        
        logger.info(f"\n📊 Checkpoint Comparison:")
        logger.info(f"   Previous Coverage: {prev_coverage:.2%} → Current: {metrics['eval_coverage']:.2%}")
        logger.info(f"   Previous Precision: {prev_precision:.4f} → Current: {metrics['precision_at_10']:.4f}")
        
        # Save if coverage improved OR (coverage same AND precision improved)
        if (metrics['eval_coverage'] > prev_coverage or 
            (abs(metrics['eval_coverage'] - prev_coverage) < 0.01 and 
             metrics['precision_at_10'] > prev_precision)):
            
            should_save_checkpoint = True
            logger.info(f"\n✅ NEW BEST MODEL! Saving checkpoint...")
        else:
            logger.info(f"\n⚠️  No improvement - keeping previous checkpoint")
            
    except:
        # No checkpoint exists
        should_save_checkpoint = True
        logger.info(f"\n✅ First model - saving as checkpoint...")
    
    # Save checkpoint if better
    if should_save_checkpoint:
        checkpoint_data = {
            'version': model['version'],
            'trained_date': execution_date,
            'model_key': model_key,
            'metrics': metrics,
            'model_artifacts': model  # Include full model
        }
        
        checkpoint_buffer = BytesIO()
        pickle.dump(checkpoint_data, checkpoint_buffer)
        checkpoint_buffer.seek(0)
        
        checkpoint_key = 'ml-data/recommendation/checkpoints/best_model.pkl'
        s3_client.put_object(Bucket='gold', Key=checkpoint_key, Body=checkpoint_buffer.getvalue())
        
        logger.info(f"✅ Saved checkpoint: s3://gold/{checkpoint_key}")
    
    # ========================================================================
    # SAVE EVALUATION RESULTS
    # ========================================================================
    
    results_df = pd.DataFrame([metrics])
    results_buffer = BytesIO()
    results_df.to_parquet(results_buffer, index=False)
    results_buffer.seek(0)
    
    results_key = f'ml-data/recommendation/evaluations/date={execution_date}/metrics.parquet'
    s3_client.put_object(Bucket='gold', Key=results_key, Body=results_buffer.getvalue())
    
    logger.info(f"✅ Saved evaluation results: s3://gold/{results_key}")
    
    context['ti'].xcom_push(key='evaluation_metrics', value=metrics)
    context['ti'].xcom_push(key='is_best_model', value=should_save_checkpoint)


# ============================================================================
# PHASE 5: MODEL REGISTRATION (WANDB)
# ============================================================================

def register_to_wandb(**context):
    """Register best model to WandB if evaluation passed"""
    execution_date = context['ds']
    is_best = context['ti'].xcom_pull(key='is_best_model', task_ids='evaluate_model')
    
    if not is_best:
        logger.info("⏭️  Skipping WandB registration - not best model")
        return 'skip_wandb'
    
    logger.info(f"\n{'='*80}")
    logger.info(f"📦 [WANDB] Registering model to WandB")
    logger.info(f"{'='*80}")
    
    # Load checkpoint
    checkpoint_key = 'ml-data/recommendation/checkpoints/best_model.pkl'
    obj = s3_client.get_object(Bucket='gold', Key=checkpoint_key)
    checkpoint = pickle.loads(obj['Body'].read())
    
    model = checkpoint['model_artifacts']
    metrics = checkpoint['metrics']
    
    # Initialize WandB
    wandb.init(
        project="ecommerce-recommendation",
        job_type="model-registration",
        name=f"cf_model_{execution_date}"
    )
    
    # Create artifact
    import tempfile
    import os
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Save model files
        np.save(os.path.join(tmp_dir, 'user_item_matrix.npy'), model['user_item_matrix'])
        np.save(os.path.join(tmp_dir, 'item_similarity_matrix.npy'), model['item_similarity_matrix'])
        
        with open(os.path.join(tmp_dir, 'user_to_idx.pkl'), 'wb') as f:
            pickle.dump(model['user_to_idx'], f)
        
        with open(os.path.join(tmp_dir, 'item_to_idx.pkl'), 'wb') as f:
            pickle.dump(model['item_to_idx'], f)
        
        with open(os.path.join(tmp_dir, 'idx_to_user.pkl'), 'wb') as f:
            pickle.dump(model['idx_to_user'], f)
        
        with open(os.path.join(tmp_dir, 'idx_to_item.pkl'), 'wb') as f:
            pickle.dump(model['idx_to_item'], f)
        
        # Save metadata
        metadata = {
            'version': model['version'],
            'trained_date': execution_date,
            'metrics': metrics,
            'n_users': model['user_item_matrix'].shape[0],
            'n_items': model['user_item_matrix'].shape[1],
            'sparsity': model['sparsity']
        }
        
        with open(os.path.join(tmp_dir, 'metadata.json'), 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Create WandB artifact
        artifact = wandb.Artifact(
            name="collaborative_filtering_model",
            type="model",
            description=f"Best CF model - Coverage: {metrics['eval_coverage']:.2%}, Precision@10: {metrics['precision_at_10']:.4f}",
            metadata=metadata
        )
        
        artifact.add_dir(tmp_dir)
        
        # Log artifact
        wandb.log_artifact(artifact)
        
        # Link to production
        artifact.link("production")
        
        logger.info("✅ Model registered to WandB:production")
    
    wandb.finish()
    
    return 'wandb_registered'


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'ml-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'medallion_ml_pipeline',
    default_args=default_args,
    description='Medallion ETL + ML Training with Checkpoint Management',
    schedule='@daily',
    start_date=datetime(2025, 10, 27),
    catchup=True,  # Enable backfilling
    max_active_runs=1,
    tags=['medallion', 'ml', 'recommendation', 'training'],
) as dag:
    
    # ========================================================================
    # PHASE 1: MEDALLION ETL
    # ========================================================================
    
    extract = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_orders_from_postgres,
    )
    
    clean = PythonOperator(
        task_id='clean_orders',
        python_callable=clean_orders_in_minio,
    )
    
    aggregate = PythonOperator(
        task_id='create_aggregations',
        python_callable=create_gold_aggregations,
    )
    
    # ========================================================================
    # PHASE 2: ML DATA PREPARATION
    # ========================================================================
    
    ml_prep = PythonOperator(
        task_id='prepare_ml_data',
        python_callable=prepare_ml_interactions,
    )
    
    # ========================================================================
    # PHASE 3: MODEL TRAINING
    # ========================================================================
    
    train = PythonOperator(
        task_id='train_model',
        python_callable=train_recommendation_model,
    )
    
    # ========================================================================
    # PHASE 4: MODEL EVALUATION
    # ========================================================================
    
    evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )
    
    # ========================================================================
    # PHASE 5: WANDB REGISTRATION (Conditional)
    # ========================================================================
    
    register_wandb = PythonOperator(
        task_id='register_to_wandb',
        python_callable=register_to_wandb,
    )
    
    skip_wandb = DummyOperator(
        task_id='skip_wandb'
    )
    
    end = DummyOperator(
        task_id='end'
    )
    
    # ========================================================================
    # DAG FLOW
    # ========================================================================
    
    # Medallion ETL
    extract >> clean >> aggregate
    
    # ML Pipeline (runs after aggregations)
    aggregate >> ml_prep >> train >> evaluate
    
    # Conditional WandB registration
    evaluate >> [register_wandb, skip_wandb] >> end