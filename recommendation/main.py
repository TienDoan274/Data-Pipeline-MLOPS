# recommendation/main.py
"""
Recommendation API - FastAPI Service with WandB
Serve collaborative filtering recommendations
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import numpy as np
import pickle
import wandb
import logging
from functools import lru_cache
import redis
import json
import os
from pathlib import Path
import tempfile

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Product Recommendation API",
    description="Collaborative Filtering Recommendation System with WandB",
    version="1.0.0"
)

# Redis client for caching
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    decode_responses=True
)

# WandB setup
WANDB_BASE_URL = os.getenv('WANDB_BASE_URL', 'http://wandb-server:8080')
WANDB_API_KEY = os.getenv('WANDB_API_KEY')
os.environ['WANDB_BASE_URL'] = WANDB_BASE_URL

# Initialize WandB
if WANDB_API_KEY:
    wandb.login(key=WANDB_API_KEY)


# ============================================================================
# MODELS
# ============================================================================

class RecommendationResponse(BaseModel):
    """Response model for recommendations"""
    customer_id: int
    recommendations: List[dict]
    model_version: str
    cached: bool = False


class SimilarProductsResponse(BaseModel):
    """Response model for similar products"""
    product_name: str
    similar_products: List[dict]
    model_version: str


# ============================================================================
# MODEL LOADER
# ============================================================================

class RecommenderModel:
    """Wrapper for collaborative filtering model"""
    
    def __init__(self):
        self.item_similarity_matrix = None
        self.user_item_matrix = None
        self.user_to_idx = None
        self.item_to_idx = None
        self.idx_to_user = None
        self.idx_to_item = None
        self.model_version = None
        self.loaded = False
    
    def load_from_wandb(
        self, 
        project: str = "ecommerce-recommendation",
        model_name: str = "collaborative_filtering_model",
        alias: str = "production"
    ):
        """Load model from WandB registry"""
        logger.info(f"📦 Loading model from WandB: {project}/{model_name}:{alias}")
        
        try:
            # Initialize WandB run for inference
            run = wandb.init(
                project=project,
                job_type="inference",
                reinit=True
            )
            
            # Download model artifact
            artifact_name = f"{model_name}:{alias}"
            logger.info(f"   Downloading artifact: {artifact_name}")
            
            artifact = run.use_artifact(artifact_name, type='model')
            artifact_dir = artifact.download()
            
            self.model_version = artifact.version
            
            logger.info(f"   Loading version: v{self.model_version}")
            logger.info(f"   Artifact path: {artifact_dir}")
            
            # Load matrices
            self.user_item_matrix = np.load(f"{artifact_dir}/user_item_matrix.npy")
            self.item_similarity_matrix = np.load(f"{artifact_dir}/item_similarity_matrix.npy")
            
            # Load mappings
            with open(f"{artifact_dir}/user_to_idx.pkl", 'rb') as f:
                self.user_to_idx = pickle.load(f)
            
            with open(f"{artifact_dir}/item_to_idx.pkl", 'rb') as f:
                self.item_to_idx = pickle.load(f)
            
            with open(f"{artifact_dir}/idx_to_user.pkl", 'rb') as f:
                self.idx_to_user = pickle.load(f)
            
            with open(f"{artifact_dir}/idx_to_item.pkl", 'rb') as f:
                self.idx_to_item = pickle.load(f)
            
            self.loaded = True
            
            # Finish WandB run
            wandb.finish()
            
            logger.info(f"✅ Model loaded successfully")
            logger.info(f"   Users: {len(self.user_to_idx)}")
            logger.info(f"   Products: {len(self.item_to_idx)}")
            logger.info(f"   Sparsity: {1 - np.count_nonzero(self.user_item_matrix) / self.user_item_matrix.size:.2%}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load model from WandB: {e}")
            raise
    
    def recommend(self, customer_id: int, top_n: int = 10) -> List[dict]:
        """Generate recommendations for a customer"""
        
        if not self.loaded:
            raise ValueError("Model not loaded")
        
        # Check if user exists
        if customer_id not in self.user_to_idx:
            logger.warning(f"Customer {customer_id} not found in training data")
            return []
        
        # Get user's purchases
        user_idx = self.user_to_idx[customer_id]
        user_purchases = self.user_item_matrix[user_idx]
        purchased_item_indices = np.where(user_purchases > 0)[0]
        
        if len(purchased_item_indices) == 0:
            logger.warning(f"Customer {customer_id} has no purchase history")
            return []
        
        # Calculate scores based on item similarity
        n_items = self.item_similarity_matrix.shape[0]
        scores = np.zeros(n_items)
        
        for item_idx in range(n_items):
            score = 0
            for purchased_idx in purchased_item_indices:
                score += self.item_similarity_matrix[item_idx, purchased_idx]
            scores[item_idx] = score
        
        # Filter out purchased items
        for item_idx in purchased_item_indices:
            scores[item_idx] = -999
        
        # Get top N
        top_indices = np.argsort(scores)[::-1][:top_n]
        
        recommendations = []
        for rank, item_idx in enumerate(top_indices, 1):
            if scores[item_idx] > 0:
                recommendations.append({
                    'rank': rank,
                    'product_name': self.idx_to_item[str(item_idx)],
                    'score': float(scores[item_idx])
                })
        
        return recommendations
    
    def get_similar_products(self, product_name: str, top_n: int = 10) -> List[dict]:
        """Get products similar to given product"""
        
        if not self.loaded:
            raise ValueError("Model not loaded")
        
        if product_name not in self.item_to_idx:
            logger.warning(f"Product '{product_name}' not found")
            return []
        
        item_idx = self.item_to_idx[product_name]
        similarities = self.item_similarity_matrix[item_idx]
        
        # Exclude the product itself
        similarities[item_idx] = -999
        
        # Get top N
        top_indices = np.argsort(similarities)[::-1][:top_n]
        
        similar_products = []
        for rank, idx in enumerate(top_indices, 1):
            if similarities[idx] > 0:
                similar_products.append({
                    'rank': rank,
                    'product_name': self.idx_to_item[str(idx)],
                    'similarity': float(similarities[idx])
                })
        
        return similar_products


# ============================================================================
# GLOBAL MODEL INSTANCE
# ============================================================================

model = RecommenderModel()


@lru_cache()
def get_model():
    """Get singleton model instance"""
    if not model.loaded:
        model.load_from_wandb()
    return model


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Load model on startup"""
    logger.info("🚀 Starting Recommendation API with WandB")
    logger.info(f"   WandB Base URL: {WANDB_BASE_URL}")
    
    try:
        get_model()
        logger.info("✅ Model loaded successfully")
    except Exception as e:
        logger.error(f"❌ Failed to load model: {e}")
        logger.warning("⚠️  API will start but recommendations will fail until model is loaded")


@app.get("/")
async def root():
    """Health check"""
    return {
        "service": "Recommendation API",
        "status": "running",
        "model_loaded": model.loaded,
        "model_version": model.model_version,
        "wandb_base_url": WANDB_BASE_URL
    }


@app.get("/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy" if model.loaded else "unhealthy",
        "model_version": model.model_version,
        "n_users": len(model.user_to_idx) if model.loaded else 0,
        "n_products": len(model.item_to_idx) if model.loaded else 0,
        "redis_connected": redis_client.ping() if redis_client else False
    }


@app.get("/recommend/{customer_id}", response_model=RecommendationResponse)
async def get_recommendations(
    customer_id: int,
    top_n: int = Query(default=10, ge=1, le=50, description="Number of recommendations")
):
    """
    Get product recommendations for a customer
    
    - **customer_id**: Customer ID
    - **top_n**: Number of recommendations to return (1-50)
    """
    
    # Check cache first
    cache_key = f"rec:user:{customer_id}:top{top_n}"
    
    try:
        cached_result = redis_client.get(cache_key)
        
        if cached_result:
            logger.info(f"✅ Cache HIT for customer {customer_id}")
            recommendations = json.loads(cached_result)
            return RecommendationResponse(
                customer_id=customer_id,
                recommendations=recommendations,
                model_version=model.model_version,
                cached=True
            )
    except Exception as e:
        logger.warning(f"Redis error: {e}")
    
    # Cache miss - generate recommendations
    logger.info(f"🔍 Cache MISS for customer {customer_id} - generating...")
    
    try:
        recommender = get_model()
        recommendations = recommender.recommend(customer_id, top_n)
        
        if not recommendations:
            raise HTTPException(
                status_code=404,
                detail=f"No recommendations for customer {customer_id}. User may be new or have no purchase history."
            )
        
        # Cache the result (24 hours)
        try:
            redis_client.setex(
                cache_key,
                86400,  # 24 hours
                json.dumps(recommendations)
            )
        except Exception as e:
            logger.warning(f"Failed to cache result: {e}")
        
        return RecommendationResponse(
            customer_id=customer_id,
            recommendations=recommendations,
            model_version=model.model_version,
            cached=False
        )
        
    except Exception as e:
        logger.error(f"❌ Error generating recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/similar/{product_name}", response_model=SimilarProductsResponse)
async def get_similar_products(
    product_name: str,
    top_n: int = Query(default=10, ge=1, le=50, description="Number of similar products")
):
    """
    Get products similar to given product
    
    - **product_name**: Product name
    - **top_n**: Number of similar products to return (1-50)
    """
    
    try:
        recommender = get_model()
        similar_products = recommender.get_similar_products(product_name, top_n)
        
        if not similar_products:
            raise HTTPException(
                status_code=404,
                detail=f"Product '{product_name}' not found or has no similar products"
            )
        
        return SimilarProductsResponse(
            product_name=product_name,
            similar_products=similar_products,
            model_version=model.model_version
        )
        
    except Exception as e:
        logger.error(f"❌ Error finding similar products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reload")
async def reload_model():
    """Reload model from WandB (for updates)"""
    try:
        logger.info("🔄 Reloading model from WandB...")
        model.load_from_wandb()
        
        # Clear cache
        try:
            redis_client.flushdb()
            logger.info("🗑️  Cache cleared")
        except Exception as e:
            logger.warning(f"Failed to clear cache: {e}")
        
        return {
            "status": "success",
            "model_version": model.model_version,
            "message": "Model reloaded successfully from WandB"
        }
    except Exception as e:
        logger.error(f"❌ Failed to reload model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get model statistics"""
    if not model.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    return {
        "model_version": model.model_version,
        "n_users": len(model.user_to_idx),
        "n_products": len(model.item_to_idx),
        "n_interactions": int(np.count_nonzero(model.user_item_matrix)),
        "sparsity": float(1 - (np.count_nonzero(model.user_item_matrix) / 
                              (len(model.user_to_idx) * len(model.item_to_idx)))),
        "avg_user_purchases": float(np.count_nonzero(model.user_item_matrix) / len(model.user_to_idx)),
        "wandb_base_url": WANDB_BASE_URL
    }


@app.get("/products")
async def list_products(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of products")
):
    """List available products"""
    if not model.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    products = list(model.item_to_idx.keys())[:limit]
    return {
        "total_products": len(model.item_to_idx),
        "products": products,
        "showing": len(products)
    }


# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )