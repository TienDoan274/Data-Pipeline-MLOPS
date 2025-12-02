# scripts/train_and_register_model.py
# SIMPLIFIED VERSION - NO PYFUNC WRAPPER

"""
Complete Training & MLflow Registration Pipeline
=================================================
Train collaborative filtering model from Gold bucket data
and register to MLflow Model Registry
"""

import pandas as pd
import numpy as np
import boto3
from io import BytesIO
from datetime import datetime, timedelta
import logging
import os
import pickle
import json
import mlflow
from sklearn.metrics.pairwise import cosine_similarity
import tempfile
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Training configuration
TRAINING_DATE = "2025-12-02"
WINDOW_DAYS = 30

# MinIO/S3
MINIO_CONFIG = {
    'endpoint_url': 'http://localhost:9000',
    'aws_access_key_id': 'minioadmin',
    'aws_secret_access_key': 'minioadmin',
    'region_name': 'us-east-1'
}

GOLD_BUCKET = 'gold'

# MLflow
MLFLOW_TRACKING_URI = 'http://localhost:5000'
MLFLOW_EXPERIMENT_NAME = "ecommerce-recommendation"
MLFLOW_S3_ENDPOINT_URL = 'http://localhost:9000'

# Set environment variables
os.environ['MLFLOW_TRACKING_URI'] = MLFLOW_TRACKING_URI
os.environ['MLFLOW_S3_ENDPOINT_URL'] = MLFLOW_S3_ENDPOINT_URL
os.environ['AWS_ACCESS_KEY_ID'] = MINIO_CONFIG['aws_access_key_id']
os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_CONFIG['aws_secret_access_key']

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# S3 Client
s3_client = boto3.client('s3', **MINIO_CONFIG)

# ============================================================================
# STEP 1: LOAD TRAINING DATA
# ============================================================================

def load_training_data(execution_date, window_days):
    """Load and combine training data from Gold bucket"""
    
    logger.info("="*80)
    logger.info("📊 STEP 1: LOADING TRAINING DATA")
    logger.info("="*80)
    
    execution_dt = datetime.strptime(execution_date, '%Y-%m-%d')
    start_date = (execution_dt - timedelta(days=window_days - 1)).strftime('%Y-%m-%d')
    
    logger.info(f"   Date range: {start_date} to {execution_date}")
    logger.info(f"   Window: {window_days} days")
    
    train_dfs = []
    
    for i in range(window_days):
        date = (execution_dt - timedelta(days=i)).strftime('%Y-%m-%d')
        train_key = f"ml-data/train/date={date}/interactions.parquet"
        
        try:
            obj = s3_client.get_object(Bucket=GOLD_BUCKET, Key=train_key)
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            
            if len(df) > 0:
                train_dfs.append(df)
                logger.info(f"   ✅ {date}: {len(df)} interactions")
        except Exception as e:
            logger.debug(f"   ⏭️  {date}: {e}")
    
    if not train_dfs:
        raise ValueError("❌ No training data found!")
    
    train_df = pd.concat(train_dfs, ignore_index=True)
    train_df['customer_id'] = train_df['customer_id'].astype(str)
    train_df['product_name'] = train_df['product_name'].astype(str)
    train_df = train_df.drop_duplicates(subset=['customer_id', 'product_name'], keep='last')
    
    n_interactions = len(train_df)
    n_users = train_df['customer_id'].nunique()
    n_products = train_df['product_name'].nunique()
    
    logger.info(f"\n   📈 Training data summary:")
    logger.info(f"      Files loaded: {len(train_dfs)}")
    logger.info(f"      Total interactions: {n_interactions:,}")
    logger.info(f"      Unique customers: {n_users:,}")
    logger.info(f"      Unique products: {n_products:,}")
    
    return train_df, n_interactions, n_users, n_products, start_date

# ============================================================================
# STEP 2: BUILD USER-ITEM MATRIX
# ============================================================================

def build_user_item_matrix(train_df):
    """Build user-item interaction matrix"""
    
    logger.info("\n" + "="*80)
    logger.info("🔧 STEP 2: BUILDING USER-ITEM MATRIX")
    logger.info("="*80)
    
    users = sorted(train_df['customer_id'].unique())
    products = sorted(train_df['product_name'].unique())
    
    logger.info(f"   Users: {len(users)}")
    logger.info(f"   Products: {len(products)}")
    
    user_to_idx = {user: idx for idx, user in enumerate(users)}
    item_to_idx = {item: idx for idx, item in enumerate(products)}
    idx_to_user = {str(idx): user for user, idx in user_to_idx.items()}
    idx_to_item = {str(idx): item for item, idx in item_to_idx.items()}
    
    # NEW: Create product_id to product_name mapping
    # Product ID = PROD{index:04d} (e.g., PROD0000, PROD0001, ...)
    product_id_to_name = {f"PROD{idx:04d}": item for idx, item in enumerate(products)}
    product_name_to_id = {item: f"PROD{idx:04d}" for idx, item in enumerate(products)}
    
    user_item_matrix = np.zeros((len(users), len(products)), dtype=np.float32)
    
    logger.info(f"\n   Matrix shape: {user_item_matrix.shape}")
    logger.info(f"   Populating matrix...")
    
    populated = 0
    for idx, row in train_df.iterrows():
        customer_id = str(row['customer_id'])
        product_name = str(row['product_name'])
        
        if customer_id in user_to_idx and product_name in item_to_idx:
            user_idx = user_to_idx[customer_id]
            item_idx = item_to_idx[product_name]
            user_item_matrix[user_idx, item_idx] = 1.0
            populated += 1
    
    n_nonzero = np.count_nonzero(user_item_matrix)
    
    logger.info(f"   Populated entries: {populated:,}")
    logger.info(f"   Non-zero entries: {n_nonzero:,}")
    
    if n_nonzero == 0:
        raise ValueError("❌ Matrix is all zeros!")
    
    sparsity = 1 - (n_nonzero / user_item_matrix.size)
    
    logger.info(f"   Sparsity: {sparsity:.2%}")
    logger.info(f"   Memory: {user_item_matrix.nbytes / 1024 / 1024:.2f} MB")
    
    return (user_item_matrix, user_to_idx, item_to_idx, idx_to_user, idx_to_item, 
            sparsity, product_id_to_name, product_name_to_id)


# ============================================================================
# STEP 3: COMPUTE ITEM SIMILARITY
# ============================================================================

def compute_item_similarity(user_item_matrix):
    """Compute item-item similarity matrix"""
    
    logger.info("\n" + "="*80)
    logger.info("🧮 STEP 3: COMPUTING ITEM SIMILARITY")
    logger.info("="*80)
    
    item_similarity_matrix = cosine_similarity(user_item_matrix.T)
    np.fill_diagonal(item_similarity_matrix, 0)
    
    sim_stats = {
        'mean': float(item_similarity_matrix.mean()),
        'std': float(item_similarity_matrix.std()),
        'min': float(item_similarity_matrix.min()),
        'max': float(item_similarity_matrix.max()),
        'median': float(np.median(item_similarity_matrix))
    }
    
    logger.info(f"   Shape: {item_similarity_matrix.shape}")
    logger.info(f"   Statistics:")
    logger.info(f"      Mean: {sim_stats['mean']:.4f}")
    logger.info(f"      Max: {sim_stats['max']:.4f}")
    logger.info(f"      Median: {sim_stats['median']:.4f}")
    logger.info(f"   Non-zero entries: {np.count_nonzero(item_similarity_matrix):,}")
    
    return item_similarity_matrix, sim_stats

# ============================================================================
# STEP 4: SAVE TO GOLD BUCKET
# ============================================================================

def save_model_to_gold(model_artifacts, execution_date):
    """Save model to Gold bucket"""
    
    logger.info("\n" + "="*80)
    logger.info("💾 STEP 4: SAVING TO GOLD BUCKET")
    logger.info("="*80)
    
    model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_key = f"ml-data/models/date={execution_date}/model_{model_version}.pkl"
    
    model_bytes = BytesIO()
    pickle.dump(model_artifacts, model_bytes)
    file_size = model_bytes.getbuffer().nbytes / 1024 / 1024
    model_bytes.seek(0)
    
    s3_client.upload_fileobj(model_bytes, GOLD_BUCKET, model_key)
    
    logger.info(f"   ✅ Saved: s3://{GOLD_BUCKET}/{model_key}")
    logger.info(f"   File size: {file_size:.2f} MB")
    
    return model_key, model_version, file_size

# ============================================================================
# STEP 5: LOG TO MLFLOW (SIMPLIFIED)
# ============================================================================

def log_to_mlflow(model_artifacts, execution_date, window_days, start_date,
                  n_interactions, n_users, n_products, sparsity, sim_stats, file_size):
    """Log artifacts to MLflow (simplified)"""
    
    logger.info("\n" + "="*80)
    logger.info("📤 STEP 5: LOGGING TO MLFLOW")
    logger.info("="*80)
    
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    with mlflow.start_run(run_name=f"training_{execution_date}") as run:
        
        # Log parameters
        mlflow.log_param("execution_date", execution_date)
        mlflow.log_param("window_days", window_days)
        mlflow.log_param("window_start", start_date)
        mlflow.log_param("algorithm", "collaborative_filtering")
        mlflow.log_param("similarity_metric", "cosine")
        
        # Log metrics
        mlflow.log_metric("n_interactions", n_interactions)
        mlflow.log_metric("n_users", n_users)
        mlflow.log_metric("n_products", n_products)
        mlflow.log_metric("matrix_sparsity", sparsity)
        mlflow.log_metric("model_size_mb", file_size)
        
        for key, value in sim_stats.items():
            mlflow.log_metric(f"similarity_{key}", value)
        
        # Log artifacts
        with tempfile.TemporaryDirectory() as tmpdir:
            np.save(f"{tmpdir}/user_item_matrix.npy", model_artifacts['user_item_matrix'])
            np.save(f"{tmpdir}/item_similarity_matrix.npy", model_artifacts['item_similarity_matrix'])
            
            with open(f"{tmpdir}/user_to_idx.pkl", 'wb') as f:
                pickle.dump(model_artifacts['user_to_idx'], f)
            with open(f"{tmpdir}/item_to_idx.pkl", 'wb') as f:
                pickle.dump(model_artifacts['item_to_idx'], f)
            with open(f"{tmpdir}/idx_to_user.pkl", 'wb') as f:
                pickle.dump(model_artifacts['idx_to_user'], f)
            with open(f"{tmpdir}/idx_to_item.pkl", 'wb') as f:
                pickle.dump(model_artifacts['idx_to_item'], f)
            
            # NEW: Save product ID mappings
            with open(f"{tmpdir}/product_id_to_name.pkl", 'wb') as f:
                pickle.dump(model_artifacts['product_id_to_name'], f)
            with open(f"{tmpdir}/product_name_to_id.pkl", 'wb') as f:
                pickle.dump(model_artifacts['product_name_to_id'], f)
            
            with open(f"{tmpdir}/metadata.json", 'w') as f:
                json.dump(model_artifacts['metadata'], f, indent=2)
            
            mlflow.log_artifacts(tmpdir)
        
        run_id = run.info.run_id
        logger.info(f"   ✅ Logged to MLflow")
        logger.info(f"   Run ID: {run_id}")
        
        return run_id

# ============================================================================
# STEP 6: REGISTER TO MODEL REGISTRY
# ============================================================================

# scripts/train_and_register_model.py
# REPLACE register_to_mlflow function with this:

def register_to_mlflow(run_id, execution_date, n_users, n_products, sparsity):
    """Register model to MLflow Model Registry"""
    
    logger.info("\n" + "="*80)
    logger.info("📝 STEP 6: REGISTERING TO MODEL REGISTRY")
    logger.info("="*80)
    
    try:
        client = mlflow.MlflowClient()
        model_name = "collaborative_filtering_model"
        
        # Create or get model
        try:
            client.create_registered_model(model_name)
            logger.info(f"   ✅ Created model: {model_name}")
        except:
            logger.info(f"   ℹ️  Model {model_name} already exists")
        
        # Get experiment and artifact location
        run = client.get_run(run_id)
        artifact_uri = run.info.artifact_uri
        
        # CORRECT: Use full artifact URI from MinIO/S3
        source = f"{artifact_uri}"  # This will be like: s3://mlflow/artifacts/1/{run_id}/artifacts
        
        logger.info(f"   Source URI: {source}")
        
        # Create model version
        model_version = client.create_model_version(
            name=model_name,
            source=source,
            run_id=run_id
        )
        
        logger.info(f"   ✅ Created version: {model_version.version}")
        
        # Transition to Production
        client.transition_model_version_stage(
            name=model_name,
            version=model_version.version,
            stage="Production",
            archive_existing_versions=True
        )
        
        logger.info(f"   ✅ Transitioned to Production")
        
        # Update description
        description = (
            f"Collaborative Filtering Model\n"
            f"Date: {execution_date}\n"
            f"Users: {n_users:,}\n"
            f"Products: {n_products:,}\n"
            f"Sparsity: {sparsity:.2%}"
        )
        
        client.update_model_version(
            name=model_name,
            version=model_version.version,
            description=description
        )
        
        # Add tags
        tags = {
            "training_date": execution_date,
            "n_users": str(n_users),
            "n_products": str(n_products),
            "sparsity": f"{sparsity:.4f}"
        }
        
        for key, value in tags.items():
            client.set_model_version_tag(
                name=model_name,
                version=model_version.version,
                key=key,
                value=value
            )
        
        logger.info(f"   ✅ Description and tags updated")
        
        return model_version.version
        
    except Exception as e:
        logger.error(f"   ❌ Registration failed: {e}")
        raise

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main pipeline"""
    
    try:
        logger.info("\n" + "="*80)
        logger.info("🚀 STARTING TRAINING PIPELINE")
        logger.info("="*80)
        
        # Load data
        train_df, n_interactions, n_users, n_products, start_date = load_training_data(
            TRAINING_DATE, WINDOW_DAYS
        )
        
        # Build matrix - NOW RETURNS 8 VALUES
        (user_item_matrix, user_to_idx, item_to_idx, idx_to_user, idx_to_item, 
         sparsity, product_id_to_name, product_name_to_id) = build_user_item_matrix(train_df)
        
        # Compute similarity
        item_similarity_matrix, sim_stats = compute_item_similarity(user_item_matrix)
        
        # Package - ADD NEW MAPPINGS
        model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_artifacts = {
            'user_item_matrix': user_item_matrix,
            'item_similarity_matrix': item_similarity_matrix,
            'user_to_idx': user_to_idx,
            'item_to_idx': item_to_idx,
            'idx_to_user': idx_to_user,
            'idx_to_item': idx_to_item,
            'product_id_to_name': product_id_to_name,  # NEW
            'product_name_to_id': product_name_to_id,  # NEW
            'metadata': {
                'version': model_version,
                'training_date': TRAINING_DATE,
                'window_days': WINDOW_DAYS,
                'window_start': start_date,
                'n_users': n_users,
                'n_products': n_products,
                'n_interactions': n_interactions,
                'sparsity': sparsity,
                'similarity_stats': sim_stats
            }
        }
        
        # Save to Gold
        model_key, model_version, file_size = save_model_to_gold(model_artifacts, TRAINING_DATE)
        
        # Log to MLflow
        run_id = log_to_mlflow(
            model_artifacts, TRAINING_DATE, WINDOW_DAYS, start_date,
            n_interactions, n_users, n_products, sparsity, sim_stats, file_size
        )
        
        # Register
        registry_version = register_to_mlflow(run_id, TRAINING_DATE, n_users, n_products, sparsity)
        
        # Summary
        logger.info("\n" + "="*80)
        logger.info("🎉 PIPELINE COMPLETED")
        logger.info("="*80)
        logger.info(f"\n📊 Summary:")
        logger.info(f"   Users: {n_users:,}")
        logger.info(f"   Products: {n_products:,}")
        logger.info(f"   Interactions: {n_interactions:,}")
        logger.info(f"   Sparsity: {sparsity:.2%}")
        logger.info(f"\n📦 Locations:")
        logger.info(f"   Gold: s3://{GOLD_BUCKET}/{model_key}")
        logger.info(f"   MLflow Run: {run_id}")
        logger.info(f"   Registry Version: {registry_version}")
        logger.info(f"\n🔄 Next:")
        logger.info(f"   curl -X POST http://localhost:8000/reload")
        logger.info(f"   curl http://localhost:8000/recommend/CUST0001?top_n=10")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)