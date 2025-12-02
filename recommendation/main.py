# recommendation/main.py (COMPLETE REPLACEMENT FOR MLFLOW)

"""
Recommendation API - FastAPI Service with MLflow
Serve collaborative filtering recommendations
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import numpy as np
import pickle
import mlflow
import mlflow.pyfunc
import logging
from functools import lru_cache
import redis
import json
import os
import tempfile

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="Product Recommendation API",
    description="Collaborative Filtering Recommendation System with MLflow",
    version="2.0.0"
)

# Redis client for caching
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    decode_responses=True
)

# MLflow setup
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow-server:5000')
MLFLOW_S3_ENDPOINT_URL = os.getenv('MLFLOW_S3_ENDPOINT_URL', 'http://minio:9000')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')

# Set environment variables for MLflow
os.environ['MLFLOW_TRACKING_URI'] = MLFLOW_TRACKING_URI
os.environ['MLFLOW_S3_ENDPOINT_URL'] = MLFLOW_S3_ENDPOINT_URL
os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# ============================================================================
# MODELS
# ============================================================================

class RecommendationResponse(BaseModel):
    """Response model for recommendations"""
    customer_id: str
    recommendations: List[dict]
    model_version: str
    model_stage: str
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
        self.product_id_to_name = None
        self.product_name_to_id = None
        self.model_version = None
        self.model_stage = None
        self.loaded = False
    
    def load_from_mlflow(
        self, 
        model_name: str = "collaborative_filtering_model",
        stage: str = "Production"
    ):
        """Load model from MLflow Model Registry"""
        logger.info(f"📦 Loading model from MLflow: {model_name}/{stage}")
        
        try:
            # Initialize MLflow client
            client = mlflow.MlflowClient()
            
            # Get model version by stage
            logger.info(f"   Fetching model: {model_name} (stage={stage})")
            
            model_versions = client.search_model_versions(f"name='{model_name}'")
            
            production_versions = [
                v for v in model_versions 
                if v.current_stage == stage
            ]
            
            if not production_versions:
                raise ValueError(f"No model found in '{stage}' stage")
            
            # Get latest production version
            latest_version = sorted(
                production_versions,
                key=lambda x: int(x.version),
                reverse=True
            )[0]
            
            self.model_version = latest_version.version
            self.model_stage = latest_version.current_stage
            run_id = latest_version.run_id
            
            logger.info(f"   Model Version: {self.model_version}")
            logger.info(f"   Stage: {self.model_stage}")
            logger.info(f"   Run ID: {run_id}")
            
            # Download artifacts
            with tempfile.TemporaryDirectory() as tmpdir:
                logger.info(f"   Downloading artifacts...")
                
                artifact_path = client.download_artifacts(run_id, "", tmpdir)
                
                # Load matrices
                self.user_item_matrix = np.load(f"{artifact_path}/user_item_matrix.npy")
                self.item_similarity_matrix = np.load(f"{artifact_path}/item_similarity_matrix.npy")
                
                # Load mappings
                with open(f"{artifact_path}/user_to_idx.pkl", 'rb') as f:
                    self.user_to_idx = pickle.load(f)
                
                with open(f"{artifact_path}/item_to_idx.pkl", 'rb') as f:
                    self.item_to_idx = pickle.load(f)
                
                with open(f"{artifact_path}/idx_to_user.pkl", 'rb') as f:
                    self.idx_to_user = pickle.load(f)
                
                with open(f"{artifact_path}/idx_to_item.pkl", 'rb') as f:
                    self.idx_to_item = pickle.load(f)
                
                # Load product ID mappings
                try:
                    with open(f"{artifact_path}/product_id_to_name.pkl", 'rb') as f:
                        self.product_id_to_name = pickle.load(f)
                    
                    with open(f"{artifact_path}/product_name_to_id.pkl", 'rb') as f:
                        self.product_name_to_id = pickle.load(f)
                    
                    logger.info(f"   ✅ Product ID mappings loaded")
                except FileNotFoundError:
                    # Fallback: Create mappings from existing data
                    logger.warning(f"   ⚠️  Product ID mappings not found, creating from item_to_idx")
                    products = sorted(self.item_to_idx.keys())
                    self.product_id_to_name = {f"PROD{idx:04d}": name for idx, name in enumerate(products)}
                    self.product_name_to_id = {name: f"PROD{idx:04d}" for idx, name in enumerate(products)}
            
            self.loaded = True
            
            logger.info(f"✅ Model loaded successfully")
            logger.info(f"   Users: {len(self.user_to_idx)}")
            logger.info(f"   Products: {len(self.item_to_idx)}")
            logger.info(f"   Product IDs: {len(self.product_id_to_name)}")
            logger.info(f"   Sparsity: {1 - np.count_nonzero(self.user_item_matrix) / self.user_item_matrix.size:.2%}")
            
        except Exception as e:
            logger.error(f"❌ Failed to load model from MLflow: {e}")
            raise
    
    def recommend(self, customer_id: str, top_n: int = 10) -> List[dict]:
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
            if scores[item_idx] >= 0:  # Accept >= 0
                product_name = self.idx_to_item[str(item_idx)]
                product_id = self.product_name_to_id.get(product_name, 'UNKNOWN')
                
                recommendations.append({
                    'rank': rank,
                    'product_id': product_id,
                    'product_name': product_name,
                    'score': float(scores[item_idx])
                })
        
        # FALLBACK: If no recommendations, return popular products
        if len(recommendations) == 0:
            logger.warning(f"No similar products for {customer_id}, using popularity fallback")
            
            # Get popular products
            product_popularity = self.user_item_matrix.sum(axis=0)
            popular_indices = np.argsort(product_popularity)[::-1]
            
            # Filter out purchased items
            fallback_indices = [
                idx for idx in popular_indices 
                if idx not in purchased_item_indices
            ][:top_n]
            
            for rank, item_idx in enumerate(fallback_indices, 1):
                product_name = self.idx_to_item[str(item_idx)]
                product_id = self.product_name_to_id.get(product_name, 'UNKNOWN')
                
                recommendations.append({
                    'rank': rank,
                    'product_id': product_id,
                    'product_name': product_name,
                    'score': 0.0,
                    'fallback': True
                })
        
        return recommendations
    
    def get_similar_products(self, product_name: str, top_n: int = 10) -> List[dict]:
        """Get products similar to given product name"""
        
        if not self.loaded:
            raise ValueError("Model not loaded")
        
        # Check if product exists
        if product_name not in self.item_to_idx:
            logger.warning(f"Product '{product_name}' not found")
            return []
        
        item_idx = self.item_to_idx[product_name]
        similarities = self.item_similarity_matrix[item_idx].copy()
        
        # Exclude the product itself
        similarities[item_idx] = -999
        
        # Get top N
        top_indices = np.argsort(similarities)[::-1][:top_n]
        
        similar_products = []
        for rank, idx in enumerate(top_indices, 1):
            if similarities[idx] >= 0:  # Accept >= 0
                similar_product_name = self.idx_to_item[str(idx)]
                product_id = self.product_name_to_id.get(similar_product_name, 'UNKNOWN')
                
                similar_products.append({
                    'rank': rank,
                    'product_id': product_id,
                    'product_name': similar_product_name,
                    'similarity': float(similarities[idx])
                })
        
        # FALLBACK: If no similar products, return popular products
        if len(similar_products) == 0:
            logger.warning(f"No similar products for '{product_name}', using popularity fallback")
            
            product_popularity = self.user_item_matrix.sum(axis=0)
            popular_indices = np.argsort(product_popularity)[::-1]
            
            # Filter out the product itself
            fallback_indices = [
                idx for idx in popular_indices 
                if idx != item_idx
            ][:top_n]
            
            for rank, idx in enumerate(fallback_indices, 1):
                similar_product_name = self.idx_to_item[str(idx)]
                product_id = self.product_name_to_id.get(similar_product_name, 'UNKNOWN')
                
                similar_products.append({
                    'rank': rank,
                    'product_id': product_id,
                    'product_name': similar_product_name,
                    'similarity': 0.0,
                    'fallback': True
                })
        
        return similar_products
    
    def get_similar_by_product_id(self, product_id: str, top_n: int = 10) -> List[dict]:
        """Get products similar to given product ID"""
        
        if not self.loaded:
            raise ValueError("Model not loaded")
        
        # Convert product ID to product name
        if product_id not in self.product_id_to_name:
            logger.warning(f"Product ID '{product_id}' not found")
            return []
        
        product_name = self.product_id_to_name[product_id]
        
        # Use existing method
        return self.get_similar_products(product_name, top_n)


# ============================================================================
# GLOBAL MODEL INSTANCE
# ============================================================================

model = RecommenderModel()


@lru_cache()
def get_model():
    """Get singleton model instance"""
    if not model.loaded:
        model.load_from_mlflow()
    return model


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Load model on startup"""
    logger.info("🚀 Starting Recommendation API with MLflow")
    logger.info(f"   MLflow Tracking URI: {MLFLOW_TRACKING_URI}")
    
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
        "model_stage": model.model_stage,
        "mlflow_tracking_uri": MLFLOW_TRACKING_URI
    }


@app.get("/health")
async def health_check():
    """Detailed health check"""
    return {
        "status": "healthy" if model.loaded else "unhealthy",
        "model_version": model.model_version,
        "model_stage": model.model_stage,
        "n_users": len(model.user_to_idx) if model.loaded else 0,
        "n_products": len(model.item_to_idx) if model.loaded else 0,
        "redis_connected": redis_client.ping() if redis_client else False
    }


@app.get("/recommend/{customer_id}", response_model=RecommendationResponse)
async def get_recommendations(
    customer_id: str,
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
                model_version=str(model.model_version),
                model_stage=model.model_stage,
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
            model_version=str(model.model_version),
            model_stage=model.model_stage,
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
            model_version=str(model.model_version)
        )
        
    except Exception as e:
        logger.error(f"❌ Error finding similar products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reload")
async def reload_model():
    """Reload model from MLflow (for updates)"""
    try:
        logger.info("🔄 Reloading model from MLflow...")
        model.load_from_mlflow()
        
        # Clear cache
        try:
            redis_client.flushdb()
            logger.info("🗑️  Cache cleared")
        except Exception as e:
            logger.warning(f"Failed to clear cache: {e}")
        
        return {
            "status": "success",
            "model_version": str(model.model_version),
            "model_stage": model.model_stage,
            "message": "Model reloaded successfully from MLflow"
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
        "model_version": str(model.model_version),
        "model_stage": model.model_stage,
        "n_users": len(model.user_to_idx),
        "n_products": len(model.item_to_idx),
        "n_interactions": int(np.count_nonzero(model.user_item_matrix)),
        "sparsity": float(1 - (np.count_nonzero(model.user_item_matrix) / 
                              (len(model.user_to_idx) * len(model.item_to_idx)))),
        "avg_user_purchases": float(np.count_nonzero(model.user_item_matrix) / len(model.user_to_idx)),
        "mlflow_tracking_uri": MLFLOW_TRACKING_URI
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

@app.get("/available-products")
async def list_available_products(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of products"),
    search: str = Query(default=None, description="Search products by name"),
    sort: str = Query(default="name", description="Sort by: name, popularity")
):
    """
    List all available products with product IDs
    """
    if not model.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        # Get all products
        all_products = list(model.item_to_idx.keys())
        
        # Calculate popularity
        product_popularity = {}
        for product in all_products:
            item_idx = model.item_to_idx[product]
            popularity = int(model.user_item_matrix[:, item_idx].sum())
            product_popularity[product] = popularity
        
        # Filter by search
        if search:
            search_lower = search.lower()
            filtered_products = [
                p for p in all_products 
                if search_lower in p.lower()
            ]
        else:
            filtered_products = all_products
        
        # Sort
        if sort == "popularity":
            filtered_products = sorted(
                filtered_products,
                key=lambda p: product_popularity[p],
                reverse=True
            )
        else:
            filtered_products = sorted(filtered_products)
        
        # Limit
        limited_products = filtered_products[:limit]
        
        # Build response with product IDs
        products_with_details = []
        for product in limited_products:
            product_id = model.product_name_to_id.get(product, 'UNKNOWN')
            products_with_details.append({
                'product_id': product_id,  # NEW
                'product_name': product,
                'popularity': product_popularity[product],
                'can_get_similar': True
            })
        
        return {
            "total_products": len(model.item_to_idx),
            "filtered_products": len(filtered_products),
            "showing": len(limited_products),
            "products": products_with_details,
            "usage": "Use product_id in: GET /similar/products/{product_id}?top_n=10"
        }
        
    except Exception as e:
        logger.error(f"❌ Error listing products: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class SimilarProductsByIdResponse(BaseModel):
    """Response model for similar products by ID"""
    product_id: str
    product_name: str
    similar_products: List[dict]
    model_version: str


@app.get("/similar/products/{product_id}", response_model=SimilarProductsByIdResponse)
async def get_similar_products_by_id(
    product_id: str,
    top_n: int = Query(default=10, ge=1, le=50, description="Number of similar products")
):
    """
    Get products similar to given product ID
    
    - **product_id**: Product ID (e.g., PROD0000)
    - **top_n**: Number of similar products to return (1-50)
    """
    
    try:
        recommender = get_model()
        
        # Check if product ID exists
        if product_id not in recommender.product_id_to_name:
            raise HTTPException(
                status_code=404,
                detail=f"Product ID '{product_id}' not found. "
                       f"Use /available-products to see all product IDs."
            )
        
        product_name = recommender.product_id_to_name[product_id]
        similar_products = recommender.get_similar_by_product_id(product_id, top_n)
        
        if not similar_products:
            raise HTTPException(
                status_code=404,
                detail=f"No similar products found for product ID '{product_id}'"
            )
        
        return SimilarProductsByIdResponse(
            product_id=product_id,
            product_name=product_name,
            similar_products=similar_products,
            model_version=str(model.model_version)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error finding similar products: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/available-customers")
async def list_available_customers(
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of customers"),
    search: str = Query(default=None, description="Search customers by ID"),
    sort: str = Query(default="id", description="Sort by: id, purchases")
):
    """
    List all available customers that can be used in /recommend endpoint
    
    - **limit**: Maximum number of customers to return (1-1000)
    - **search**: Filter customers by ID (partial match)
    - **sort**: Sort by 'id' (alphabetical) or 'purchases' (most active)
    """
    if not model.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        # Get all customers
        all_customers = list(model.user_to_idx.keys())
        
        # Calculate purchase count for each customer
        customer_purchases = {}
        for customer in all_customers:
            user_idx = model.user_to_idx[customer]
            # Count how many products this customer purchased
            purchase_count = int(model.user_item_matrix[user_idx, :].sum())
            customer_purchases[customer] = purchase_count
        
        # Filter by search term
        if search:
            search_upper = search.upper()
            filtered_customers = [
                c for c in all_customers 
                if search_upper in c.upper()
            ]
        else:
            filtered_customers = all_customers
        
        # Sort customers
        if sort == "purchases":
            filtered_customers = sorted(
                filtered_customers,
                key=lambda c: customer_purchases[c],
                reverse=True
            )
        else:  # sort by id (default)
            filtered_customers = sorted(filtered_customers)
        
        # Limit results
        limited_customers = filtered_customers[:limit]
        
        # Build response with details
        customers_with_details = []
        for customer in limited_customers:
            customers_with_details.append({
                'customer_id': customer,
                'purchase_count': customer_purchases[customer],
                'can_get_recommendations': customer_purchases[customer] > 0
            })
        
        return {
            "total_customers": len(model.user_to_idx),
            "filtered_customers": len(filtered_customers),
            "showing": len(limited_customers),
            "customers": customers_with_details,
            "usage": f"Use customer_id in: GET /recommend/{{customer_id}}?top_n=10"
        }
        
    except Exception as e:
        logger.error(f"❌ Error listing customers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/search")
async def search_products_and_customers(
    query: str = Query(..., description="Search query"),
    type: str = Query(default="both", description="Search type: products, customers, or both"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximum results per type")
):
    """
    Search for products and/or customers
    
    - **query**: Search term (case-insensitive)
    - **type**: What to search - 'products', 'customers', or 'both' (default)
    - **limit**: Maximum results per type (1-100)
    """
    if not model.loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    try:
        results = {}
        query_lower = query.lower()
        
        # Search products
        if type in ["products", "both"]:
            matching_products = [
                {
                    'product_name': p,
                    'similarity_api': f"/similar/{p}"
                }
                for p in model.item_to_idx.keys()
                if query_lower in p.lower()
            ][:limit]
            
            results['products'] = {
                'count': len(matching_products),
                'items': matching_products
            }
        
        # Search customers
        if type in ["customers", "both"]:
            matching_customers = [
                {
                    'customer_id': c,
                    'recommendation_api': f"/recommend/{c}"
                }
                for c in model.user_to_idx.keys()
                if query_lower in c.lower()
            ][:limit]
            
            results['customers'] = {
                'count': len(matching_customers),
                'items': matching_customers
            }
        
        return {
            "query": query,
            "search_type": type,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"❌ Error searching: {e}")
        raise HTTPException(status_code=500, detail=str(e))



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