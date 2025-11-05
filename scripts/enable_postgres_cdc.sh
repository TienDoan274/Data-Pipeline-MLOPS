#!/bin/bash
# debug_and_fix_wandb.sh
# Debug and fix WandB "empty connection scheme" error

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_header "🔍 WandB Environment Debug & Fix"

# ============================================================================
# STEP 1: Check current environment variables
# ============================================================================

print_header "1️⃣  Checking Current Environment Variables"

if docker ps -q -f name=wandb-server > /dev/null 2>&1; then
    echo ""
    print_info "Current WandB environment variables:"
    echo ""
    
    docker inspect wandb-server --format='{{range .Config.Env}}{{println .}}{{end}}' | \
        grep -E "BUCKET|AWS|MYSQL|HOST|LICENSE" | sort
    
    echo ""
    print_warning "Checking for empty or problematic values..."
    
    # Check BUCKET specifically
    BUCKET_VALUE=$(docker inspect wandb-server --format='{{range .Config.Env}}{{println .}}{{end}}' | \
        grep "^BUCKET=" | cut -d'=' -f2)
    
    echo "BUCKET value: [$BUCKET_VALUE]"
    
    if [ -z "$BUCKET_VALUE" ]; then
        print_error "BUCKET is empty!"
    elif [ "$BUCKET_VALUE" = "wandb" ]; then
        print_success "BUCKET value is correct"
    else
        print_warning "BUCKET has unexpected value: $BUCKET_VALUE"
    fi
    
else
    print_warning "wandb-server container not found"
fi

# ============================================================================
# STEP 2: Stop and remove container
# ============================================================================

print_header "2️⃣  Stopping and Removing WandB Containers"

echo ""
read -p "Stop and remove wandb-server container? (y/n): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Stopping containers..."
    docker-compose stop wandb-server recommendation-api 2>/dev/null || true
    
    print_info "Removing containers..."
    docker-compose rm -f wandb-server recommendation-api 2>/dev/null || true
    
    print_success "Containers removed"
fi

# ============================================================================
# STEP 3: Verify MinIO bucket exists
# ============================================================================

print_header "3️⃣  Verifying MinIO Bucket"

echo ""
print_info "Checking MinIO bucket 'wandb'..."

# Configure mc
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true

# Check if bucket exists
if docker exec minio mc ls local/wandb 2>/dev/null; then
    print_success "Bucket 'wandb' exists"
else
    print_warning "Bucket 'wandb' does not exist, creating..."
    docker exec minio mc mb local/wandb --ignore-existing
    print_success "Bucket created"
fi

# Set public policy
print_info "Setting bucket policy..."
docker exec minio mc anonymous set download local/wandb 2>/dev/null || true

# ============================================================================
# STEP 4: Create explicit environment file
# ============================================================================

print_header "4️⃣  Creating Explicit Environment File"

echo ""
print_info "Creating .env.wandb with all variables..."

cat > .env.wandb << 'EOF'
# WandB Server Environment Variables
# Created by debug_and_fix_wandb.sh

# Database Configuration
MYSQL_HOST=wandb-mysql
MYSQL_PORT=3306
MYSQL_DATABASE=wandb_local
MYSQL_USER=wandb
MYSQL_PASSWORD=wandb_pass

# Storage Configuration (CRITICAL!)
BUCKET=wandb
BUCKET_QUEUE=internal://

# AWS/S3 Configuration (for MinIO)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_S3_ENDPOINT_URL=http://minio:9000
AWS_S3_FORCE_PATH_STYLE=true

# WandB Settings
LICENSE=
LOCAL_RESTORE=true
HOST=http://localhost:8084

# Security Settings
GORILLA_CSRF_SECURE=false
GORILLA_SESSION_SECURE=false

# Additional Settings
DISABLE_CODE_SAVING=false
EOF

print_success "Created .env.wandb"

echo ""
print_info "Contents:"
cat .env.wandb

# ============================================================================
# STEP 5: Start with explicit environment
# ============================================================================

print_header "5️⃣  Starting WandB with Explicit Environment"

echo ""
read -p "Start WandB server now? (y/n): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Starting wandb-mysql..."
    docker-compose up -d wandb-mysql
    
    print_info "Waiting for MySQL (30 seconds)..."
    sleep 30
    
    print_info "Starting wandb-server with explicit environment..."
    
    # Start with explicit env file
    docker run -d \
        --name wandb-server \
        --network mlops_airflow-network \
        -p 8084:8080 \
        --env-file .env.wandb \
        -v wandb-data:/vol \
        --health-cmd="curl -f http://localhost:8080/healthz" \
        --health-interval=30s \
        --health-timeout=10s \
        --health-retries=5 \
        --health-start-period=60s \
        --restart always \
        wandb/local:latest
    
    print_success "WandB server started"
    
    echo ""
    print_info "Waiting for server to initialize (60 seconds)..."
    sleep 60
    
    # Check logs
    echo ""
    print_info "Checking logs for errors..."
    docker logs wandb-server 2>&1 | tail -20
fi

# ============================================================================
# STEP 6: Verify it worked
# ============================================================================

print_header "6️⃣  Verifying WandB Server"

echo ""
print_info "Checking container status..."

if docker ps | grep -q wandb-server; then
    print_success "Container is running"
    
    # Check health
    HEALTH=$(docker inspect wandb-server --format='{{.State.Health.Status}}' 2>/dev/null || echo "unknown")
    echo "Health status: $HEALTH"
    
    # Check for errors in logs
    echo ""
    print_info "Checking for 'empty connection scheme' error..."
    
    if docker logs wandb-server 2>&1 | grep -q "empty connection scheme"; then
        print_error "Still has 'empty connection scheme' error"
        echo ""
        echo "This means BUCKET variable is still problematic."
        echo "Check the logs:"
        docker logs wandb-server 2>&1 | grep -A 5 -B 5 "empty connection scheme"
    else
        print_success "No 'empty connection scheme' error found"
    fi
    
    # Check if settings loaded
    if docker logs wandb-server 2>&1 | grep -q "Settings loaded successfully"; then
        print_success "Settings loaded successfully!"
    else
        print_warning "Settings might not have loaded yet"
    fi
    
    # Test health endpoint
    echo ""
    print_info "Testing health endpoint..."
    sleep 5
    
    if curl -s -f http://localhost:8084/healthz > /dev/null 2>&1; then
        print_success "Health check passed!"
        RESPONSE=$(curl -s http://localhost:8084/healthz)
        echo "Response: $RESPONSE"
    else
        print_warning "Health check failed (server might still be starting)"
    fi
    
else
    print_error "Container is not running"
    echo ""
    echo "Check why it stopped:"
    docker logs wandb-server 2>&1 | tail -30
fi

# ============================================================================
# STEP 7: Alternative - Use docker run with inline env
# ============================================================================

print_header "7️⃣  Alternative: Direct Docker Run"

echo ""
echo "If above didn't work, try this manual command:"
echo ""
echo "# Stop current container"
echo "docker stop wandb-server && docker rm wandb-server"
echo ""
echo "# Run with inline environment variables"
cat << 'EOFCMD'
docker run -d \
  --name wandb-server \
  --network mlops_airflow-network \
  -p 8084:8080 \
  -e MYSQL_HOST=wandb-mysql \
  -e MYSQL_PORT=3306 \
  -e MYSQL_DATABASE=wandb_local \
  -e MYSQL_USER=wandb \
  -e MYSQL_PASSWORD=wandb_pass \
  -e BUCKET=wandb \
  -e BUCKET_QUEUE=internal:// \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_S3_ENDPOINT_URL=http://minio:9000 \
  -e AWS_S3_FORCE_PATH_STYLE=true \
  -e LICENSE= \
  -e LOCAL_RESTORE=true \
  -e HOST=http://localhost:8084 \
  -e GORILLA_CSRF_SECURE=false \
  -e GORILLA_SESSION_SECURE=false \
  -v mlops_wandb-data:/vol \
  --restart always \
  wandb/local:latest
EOFCMD

# ============================================================================
# Summary
# ============================================================================

print_header "📊 Summary"

echo ""
echo "Debug complete. Current status:"
echo ""

docker ps | grep wandb || echo "No WandB containers running"

echo ""
print_header "🔍 Next Steps"

echo ""
echo "1. Check logs:"
echo "   docker logs wandb-server"
echo ""
echo "2. Check health:"
echo "   curl http://localhost:8084/healthz"
echo ""
echo "3. Access UI:"
echo "   http://localhost:8084"
echo ""
echo "4. Get login credentials:"
echo "   docker logs wandb-server | grep -A 3 'Login at'"
echo ""

print_header "✅ Done!"