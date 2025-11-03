# dags/recommendation_training_pipeline.py
"""
Recommendation Model Training Pipeline
Train Item-Based Collaborative Filtering model and register in MLflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
import boto3
from io import BytesIO
import pickle
import json
import mlflow
import mlflow.pyfunc
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

# Clients
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

# MLflow configuration
mlflow.set_tracking_uri("http://mlflow-server:5000")
mlflow.set_experiment("recommendation_collaborative_filtering")


def load_interactions_from_minio(**context):
    """Load processed interactions from MinIO"""
    logger.info("=" * 80)
    logger.info("📦 STEP 1: Loading Interactions from MinIO")
    logger.info("=" * 80)
    
    # Get latest data
    date_str = datetime.now().strftime('%Y-%m-%d')
    key = f'ml-data/recommendation/interactions/date={date_str}/interactions.parquet'
    
    try:
        obj = s3_client.get_object(Bucket='gold', Key=key)
        interactions = pd.read_parquet(BytesIO(obj['Body'].read()))
        
        logger.info(f"✅ Loaded from s3://gold/{key}")
        logger.info(f"   Interactions: {len(interactions)}")
        logger.info(f"   Customers: {interactions['customer_id'].nunique()}")
        logger.info(f"   Products: {interactions['product_name'].nunique()}")
        
        context['ti'].xcom_push(key='interactions', value=interactions.to_json())
        
    except Exception as e:
        logger.error(f"❌ Failed to load data: {e}")
        raise


def build_user_item_matrix(**context):
    """Build binary user-item interaction matrix"""
    logger.info("\n" + "=" * 80)
    logger.info("🔨 STEP 2: Building User-Item Matrix")
    logger.info("=" * 80)
    
    # Load interactions
    interactions_json = context['ti'].xcom_pull(
        key='interactions',
        task_ids='load_interactions'
    )
    interactions = pd.read_json(interactions_json)
    
    # Create mappings
    unique_users = interactions['customer_id'].unique()
    unique_products = interactions['product_name'].unique()
    
    user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
    item_to_idx = {item: idx for idx, item in enumerate(unique_products)}
    
    idx_to_user = {idx: user for user, idx in user_to_idx.items()}
    idx_to_item = {idx: item for item, idx in item_to_idx.items()}
    
    logger.info(f"   Users: {len(user_to_idx)}")
    logger.info(f"   Products: {len(item_to_idx)}")
    
    # Build matrix
    n_users = len(user_to_idx)
    n_items = len(item_to_idx)
    
    user_item_matrix = np.zeros((n_users, n_items))
    
    for _, row in interactions.iterrows():
        user_idx = user_to_idx[row['customer_id']]
        item_idx = item_to_idx[row['product_name']]
        user_item_matrix[user_idx, item_idx] = 1
    
    logger.info(f"   Matrix shape: {user_item_matrix.shape}")
    logger.info(f"   Non-zero entries: {np.count_nonzero(user_item_matrix)}")
    
    sparsity = 1 - (np.count_nonzero(user_item_matrix) / (n_users * n_items))
    logger.info(f"   Sparsity: {sparsity:.2%}")
    
    # Save to context
    context['ti'].xcom_push(key='user_item_matrix', value=user_item_matrix.tolist())
    context['ti'].xcom_push(key='user_to_idx', value=user_to_idx)
    context['ti'].xcom_push(key='item_to_idx', value=item_to_idx)
    context['ti'].xcom_push(key='idx_to_user', value=idx_to_user)
    context['ti'].xcom_push(key='idx_to_item', value=idx_to_item)


def compute_item_similarity(**context):
    """Compute item-item similarity matrix"""
    logger.info("\n" + "=" * 80)
    logger.info("🔍 STEP 3: Computing Item Similarity Matrix")
    logger.info("=" * 80)
    
    # Load matrix
    user_item_matrix = np.array(context['ti'].xcom_pull(
        key='user_item_matrix',
        task_ids='build_matrix'
    ))
    
    # Transpose to get item-user matrix
    item_user_matrix = user_item_matrix.T
    
    logger.info(f"   Computing cosine similarities...")
    logger.info(f"   Item-user matrix shape: {item_user_matrix.shape}")
    
    # Compute similarity
    item_similarity_matrix = cosine_similarity(item_user_matrix)
    
    # Set diagonal to 0 (item not similar to itself)
    np.fill_diagonal(item_similarity_matrix, 0)
    
    logger.info(f"✅ Similarity matrix computed")
    logger.info(f"   Shape: {item_similarity_matrix.shape}")
    logger.info(f"   Mean similarity: {item_similarity_matrix.mean():.4f}")
    logger.info(f"   Max similarity: {item_similarity_matrix.max():.4f}")
    
    # Find top similar pairs
    item_to_idx = context['ti'].xcom_pull(key='item_to_idx', task_ids='build_matrix')
    idx_to_item = context['ti'].xcom_pull(key='idx_to_item', task_ids='build_matrix')
    
    logger.info(f"\n   Top 5 Similar Product Pairs:")
    n_items = item_similarity_matrix.shape[0]
    similar_pairs = []
    
    for i in range(n_items):
        for j in range(i+1, n_items):
            similarity = item_similarity_matrix[i, j]
            if similarity > 0:
                similar_pairs.append((
                    idx_to_item[str(i)],
                    idx_to_item[str(j)],
                    similarity
                ))
    
    similar_pairs.sort(key=lambda x: x[2], reverse=True)
    
    for i, (item1, item2, sim) in enumerate(similar_pairs[:5], 1):
        logger.info(f"      {i}. {item1} <-> {item2}: {sim:.4f}")
    
    context['ti'].xcom_push(key='item_similarity_matrix', value=item_similarity_matrix.tolist())


def evaluate_model(**context):
    """Evaluate recommendation model"""
    logger.info("\n" + "=" * 80)
    logger.info("📊 STEP 4: Evaluating Model")
    logger.info("=" * 80)
    
    # Load data
    user_item_matrix = np.array(context['ti'].xcom_pull(
        key='user_item_matrix',
        task_ids='build_matrix'
    ))
    item_similarity_matrix = np.array(context['ti'].xcom_pull(
        key='item_similarity_matrix',
        task_ids='compute_similarity'
    ))
    
    # Simple evaluation: Coverage and Diversity
    n_users, n_items = user_item_matrix.shape
    
    # Coverage: % of products that can be recommended
    # Products with at least 1 similar product
    has_similarity = (item_similarity_matrix.sum(axis=1) > 0).sum()
    coverage = has_similarity / n_items
    
    logger.info(f"   Coverage: {coverage:.2%}")
    logger.info(f"   ({has_similarity}/{n_items} products can be recommended)")
    
    # Diversity: Average pairwise similarity
    # Lower is better (more diverse recommendations)
    avg_similarity = item_similarity_matrix[item_similarity_matrix > 0].mean()
    
    logger.info(f"   Average similarity: {avg_similarity:.4f}")
    logger.info(f"   (Lower = more diverse recommendations)")
    
    # Sparsity
    sparsity = 1 - (np.count_nonzero(user_item_matrix) / (n_users * n_items))
    logger.info(f"   Data sparsity: {sparsity:.2%}")
    
    metrics = {
        'coverage': float(coverage),
        'avg_similarity': float(avg_similarity),
        'sparsity': float(sparsity),
        'n_users': int(n_users),
        'n_items': int(n_items),
        'n_interactions': int(np.count_nonzero(user_item_matrix))
    }
    
    context['ti'].xcom_push(key='metrics', value=metrics)
    
    return metrics


def log_to_mlflow(**context):
    """Log model and metrics to MLflow"""
    logger.info("\n" + "=" * 80)
    logger.info("📝 STEP 5: Logging to MLflow")
    logger.info("=" * 80)
    
    # Load all artifacts
    user_item_matrix = np.array(context['ti'].xcom_pull(
        key='user_item_matrix',
        task_ids='build_matrix'
    ))
    item_similarity_matrix = np.array(context['ti'].xcom_pull(
        key='item_similarity_matrix',
        task_ids='compute_similarity'
    ))
    user_to_idx = context['ti'].xcom_pull(key='user_to_idx', task_ids='build_matrix')
    item_to_idx = context['ti'].xcom_pull(key='item_to_idx', task_ids='build_matrix')
    idx_to_user = context['ti'].xcom_pull(key='idx_to_user', task_ids='build_matrix')
    idx_to_item = context['ti'].xcom_pull(key='idx_to_item', task_ids='build_matrix')
    metrics = context['ti'].xcom_pull(key='metrics', task_ids='evaluate_model')
    
    # Start MLflow run
    with mlflow.start_run(run_name=f"cf_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        
        # Log parameters
        mlflow.log_param("algorithm", "item_based_collaborative_filtering")
        mlflow.log_param("similarity_metric", "cosine")
        mlflow.log_param("n_users", metrics['n_users'])
        mlflow.log_param("n_items", metrics['n_items'])
        mlflow.log_param("n_interactions", metrics['n_interactions'])
        
        # Log metrics
        mlflow.log_metric("coverage", metrics['coverage'])
        mlflow.log_metric("avg_similarity", metrics['avg_similarity'])
        mlflow.log_metric("sparsity", metrics['sparsity'])
        
        # Save artifacts
        import tempfile
        import os
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Save matrices
            np.save(os.path.join(tmp_dir, 'user_item_matrix.npy'), user_item_matrix)
            np.save(os.path.join(tmp_dir, 'item_similarity_matrix.npy'), item_similarity_matrix)
            
            # Save mappings
            with open(os.path.join(tmp_dir, 'user_to_idx.pkl'), 'wb') as f:
                pickle.dump(user_to_idx, f)
            
            with open(os.path.join(tmp_dir, 'item_to_idx.pkl'), 'wb') as f:
                pickle.dump(item_to_idx, f)
            
            with open(os.path.join(tmp_dir, 'idx_to_user.pkl'), 'wb') as f:
                pickle.dump(idx_to_user, f)
            
            with open(os.path.join(tmp_dir, 'idx_to_item.pkl'), 'wb') as f:
                pickle.dump(idx_to_item, f)
            
            # Save metadata
            metadata = {
                'trained_at': datetime.now().isoformat(),
                'algorithm': 'item_based_cf',
                'n_users': metrics['n_users'],
                'n_items': metrics['n_items'],
                'n_interactions': metrics['n_interactions'],
                'sparsity': metrics['sparsity']
            }
            
            with open(os.path.join(tmp_dir, 'metadata.json'), 'w') as f:
                json.dump(metadata, f, indent=2)
            
            # Log all artifacts
            mlflow.log_artifacts(tmp_dir)
        
        # Log tags
        mlflow.set_tag("model_type", "recommendation")
        mlflow.set_tag("algorithm", "collaborative_filtering")
        mlflow.set_tag("data_date", datetime.now().strftime('%Y-%m-%d'))
        mlflow.set_tag("team", "ml-team")
        
        # Get run info
        run = mlflow.active_run()
        logger.info(f"✅ Logged to MLflow")
        logger.info(f"   Run ID: {run.info.run_id}")
        logger.info(f"   Experiment: {run.info.experiment_id}")
        
        context['ti'].xcom_push(key='mlflow_run_id', value=run.info.run_id)


# DAG Definition
default_args = {
    'owner': 'ml-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'recommendation_training_pipeline',
    default_args=default_args,
    description='Train Collaborative Filtering recommendation model',
    schedule=timedelta(days=7),  # Weekly
    start_date=datetime(2025, 10, 27),
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'recommendation', 'training'],
) as dag:
    
    load = PythonOperator(
        task_id='load_interactions',
        python_callable=load_interactions_from_minio,
    )
    
    build_matrix = PythonOperator(
        task_id='build_matrix',
        python_callable=build_user_item_matrix,
    )
    
    compute_sim = PythonOperator(
        task_id='compute_similarity',
        python_callable=compute_item_similarity,
    )
    
    evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )
    
    log_mlflow = PythonOperator(
        task_id='log_to_mlflow',
        python_callable=log_to_mlflow,
    )
    
    load >> build_matrix >> compute_sim >> evaluate >> log_mlflow