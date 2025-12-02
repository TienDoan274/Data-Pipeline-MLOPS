#!/usr/bin/env python3
"""
Backfill ML Training Data
==========================
Create training and validation data for the last 30 days
Start date: 2025-10-27 (going backwards)

This script:
1. Extract orders from PostgreSQL for each day
2. Clean data (Bronze → Silver)
3. Create ML train/eval split
4. Save to MinIO Gold bucket
"""

import pandas as pd
import numpy as np
import boto3
from io import BytesIO
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sklearn.model_selection import GroupShuffleSplit
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Date range
END_DATE = datetime(2025, 12, 31)  # Start from this date
NUM_DAYS = 30  # Go back 30 days

# PostgreSQL
DB_CONFIG = {
    'host': 'localhost',  # Change if remote
    'port': '5434',
    'database': 'ecommerce',
    'user': 'app_user',
    'password': 'app_password'
}

# MinIO/S3
MINIO_CONFIG = {
    'endpoint_url': 'http://localhost:9000',
    'aws_access_key_id': 'minioadmin',
    'aws_secret_access_key': 'minioadmin',
    'region_name': 'us-east-1'
}

GOLD_BUCKET = 'gold'
TRAIN_RATIO = 0.8  # 80% train, 20% eval

# S3 Client
s3_client = boto3.client('s3', **MINIO_CONFIG)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_db_connection():
    """Create PostgreSQL connection"""
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)

# ============================================================================
# EXTRACT & CLEAN
# ============================================================================

def extract_and_clean_orders(date_str):
    """Extract and clean orders for a specific date"""
    logger.info(f"📥 Processing date: {date_str}")
    
    try:
        engine = get_db_connection()
        
        # Extract orders for this date
        query = f"""
        SELECT 
            customer_id,
            product_name,
            price,
            quantity,
            total
        FROM orders
        WHERE DATE(order_date) = '{date_str}'
        """
        
        df = pd.read_sql(query, engine)
        
        if len(df) == 0:
            logger.warning(f"   ⚠️  No orders found for {date_str}")
            return None
        
        logger.info(f"   Extracted: {len(df)} orders")
        
        # Clean data
        initial_count = len(df)
        
        # Remove nulls
        df = df.dropna(subset=['customer_id', 'product_name'])
        
        # Fix negative prices
        df['price'] = df['price'].abs()
        
        # Ensure quantity > 0
        df = df[df['quantity'] > 0]
        
        # Recalculate total
        df['total'] = df['price'] * df['quantity']
        
        # Remove duplicates (keep only customer_id and product_name for ML)
        df = df[['customer_id', 'product_name']].drop_duplicates()
        
        cleaned_count = len(df)
        removed_count = initial_count - cleaned_count
        
        logger.info(f"   Cleaned: {cleaned_count} unique interactions")
        logger.info(f"   Removed: {removed_count} invalid/duplicate records")
        
        return df
        
    except Exception as e:
        logger.error(f"   ❌ Failed to process {date_str}: {e}")
        return None

# ============================================================================
# CREATE TRAIN/EVAL SPLIT
# ============================================================================

def create_train_eval_split(df, date_str):
    """Split data into train and eval sets"""
    
    if df is None or len(df) == 0:
        return None, None
    
    # Stratified split by customer
    splitter = GroupShuffleSplit(n_splits=1, train_size=TRAIN_RATIO, random_state=42)
    
    train_idx, eval_idx = next(splitter.split(df, groups=df['customer_id']))
    
    train_df = df.iloc[train_idx]
    eval_df = df.iloc[eval_idx]
    
    logger.info(f"   Train: {len(train_df)} interactions")
    logger.info(f"   Eval: {len(eval_df)} interactions")
    
    return train_df, eval_df

# ============================================================================
# SAVE TO MINIO
# ============================================================================

def save_to_minio(df, key):
    """Save DataFrame to MinIO as parquet"""
    
    if df is None or len(df) == 0:
        return False
    
    try:
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        
        file_size = parquet_buffer.getbuffer().nbytes / 1024
        
        parquet_buffer.seek(0)
        
        s3_client.upload_fileobj(
            parquet_buffer,
            GOLD_BUCKET,
            key
        )
        
        logger.info(f"   ✅ Saved: s3://{GOLD_BUCKET}/{key} ({file_size:.2f} KB)")
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Failed to save {key}: {e}")
        return False

# ============================================================================
# ACCUMULATE EVAL DATA
# ============================================================================

def accumulate_eval_data(new_eval_df, date_str):
    """Accumulate eval data (keep growing eval set)"""
    
    eval_key = f"ml-data/eval/date={date_str}/interactions.parquet"
    
    # Load existing eval data
    eval_dfs = [new_eval_df]
    
    try:
        # List all previous eval data
        response = s3_client.list_objects_v2(
            Bucket=GOLD_BUCKET,
            Prefix='ml-data/eval/'
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'] != eval_key and obj['Key'].endswith('.parquet'):
                    try:
                        existing_obj = s3_client.get_object(Bucket=GOLD_BUCKET, Key=obj['Key'])
                        existing_df = pd.read_parquet(BytesIO(existing_obj['Body'].read()))
                        eval_dfs.append(existing_df)
                    except Exception:
                        pass
    except Exception:
        pass
    
    # Combine all eval data
    accumulated_eval = pd.concat(eval_dfs, ignore_index=True)
    
    # Remove duplicates (keep latest)
    accumulated_eval = accumulated_eval.drop_duplicates(
        subset=['customer_id', 'product_name'],
        keep='last'
    )
    
    logger.info(f"   Accumulated eval: {len(accumulated_eval)} total interactions")
    
    return accumulated_eval

# ============================================================================
# MAIN BACKFILL PROCESS
# ============================================================================

def backfill_ml_data():
    """Backfill ML data for the last N days"""
    
    logger.info("="*80)
    logger.info(f"🚀 BACKFILLING ML DATA")
    logger.info(f"   End Date: {END_DATE.strftime('%Y-%m-%d')}")
    logger.info(f"   Days: {NUM_DAYS}")
    logger.info(f"   Start Date: {(END_DATE - timedelta(days=NUM_DAYS-1)).strftime('%Y-%m-%d')}")
    logger.info("="*80)
    
    stats = {
        'total_days': NUM_DAYS,
        'processed': 0,
        'failed': 0,
        'no_data': 0,
        'total_train_interactions': 0,
        'total_eval_interactions': 0
    }
    
    # Process each day (going backwards from end_date)
    for i in range(NUM_DAYS):
        date = END_DATE - timedelta(days=i)
        date_str = date.strftime('%Y-%m-%d')
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing Day {i+1}/{NUM_DAYS}: {date_str}")
        logger.info(f"{'='*80}")
        
        # 1. Extract and clean
        df = extract_and_clean_orders(date_str)
        
        if df is None:
            stats['no_data'] += 1
            continue
        
        # 2. Create train/eval split
        train_df, eval_df = create_train_eval_split(df, date_str)
        
        if train_df is None:
            stats['failed'] += 1
            continue
        
        # 3. Save train data
        train_key = f"ml-data/train/date={date_str}/interactions.parquet"
        if save_to_minio(train_df, train_key):
            stats['total_train_interactions'] += len(train_df)
        
        # 4. Accumulate and save eval data
        accumulated_eval = accumulate_eval_data(eval_df, date_str)
        
        eval_key = f"ml-data/eval/date={date_str}/interactions.parquet"
        if save_to_minio(accumulated_eval, eval_key):
            stats['total_eval_interactions'] = len(accumulated_eval)
        
        stats['processed'] += 1
    
    # Print summary
    logger.info("\n" + "="*80)
    logger.info("✅ BACKFILL COMPLETED")
    logger.info("="*80)
    logger.info(f"   Total days: {stats['total_days']}")
    logger.info(f"   Processed: {stats['processed']}")
    logger.info(f"   Failed: {stats['failed']}")
    logger.info(f"   No data: {stats['no_data']}")
    logger.info(f"   Total train interactions: {stats['total_train_interactions']:,}")
    logger.info(f"   Final eval interactions: {stats['total_eval_interactions']:,}")
    logger.info("="*80)
    
    return stats

# ============================================================================
# VERIFY DATA
# ============================================================================

def verify_backfill():
    """Verify backfilled data"""
    
    logger.info("\n🔍 Verifying backfilled data...")
    
    try:
        # List train files
        train_response = s3_client.list_objects_v2(
            Bucket=GOLD_BUCKET,
            Prefix='ml-data/train/'
        )
        
        train_files = [obj['Key'] for obj in train_response.get('Contents', []) 
                      if obj['Key'].endswith('.parquet')]
        
        logger.info(f"   ✅ Train files: {len(train_files)}")
        
        # List eval files
        eval_response = s3_client.list_objects_v2(
            Bucket=GOLD_BUCKET,
            Prefix='ml-data/eval/'
        )
        
        eval_files = [obj['Key'] for obj in eval_response.get('Contents', [])
                     if obj['Key'].endswith('.parquet')]
        
        logger.info(f"   ✅ Eval files: {len(eval_files)}")
        
        # Sample a few files
        logger.info("\n   Sample train files:")
        for key in sorted(train_files)[:5]:
            logger.info(f"      - {key}")
        
        if len(train_files) > 5:
            logger.info(f"      ... and {len(train_files) - 5} more")
        
        logger.info("\n   Sample eval files:")
        for key in sorted(eval_files)[:5]:
            logger.info(f"      - {key}")
        
        if len(eval_files) > 5:
            logger.info(f"      ... and {len(eval_files) - 5} more")
        
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Verification failed: {e}")
        return False

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    try:
        # Run backfill
        stats = backfill_ml_data()
        
        # Verify
        verify_backfill()
        
        logger.info("\n✅ All done! You can now train the model.")
        logger.info("\nNext steps:")
        logger.info("   1. Check data in MinIO: docker exec minio-client mc ls local/gold/ml-data/")
        logger.info("   2. Train model: Use the training script or Airflow DAG")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Backfill interrupted by user")
    except Exception as e:
        logger.error(f"\n❌ Backfill failed: {e}")
        raise