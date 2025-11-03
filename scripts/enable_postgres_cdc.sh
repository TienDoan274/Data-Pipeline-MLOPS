# scripts/enable_postgres_cdc.sh
#!/bin/bash

echo "🔧 Enabling PostgreSQL CDC (Write-Ahead Log)..."

# Enable logical replication
docker exec source-postgres bash -c "
cat >> /var/lib/postgresql/data/postgresql.conf << EOF

# CDC Configuration
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
EOF
"

# Restart PostgreSQL
echo "🔄 Restarting PostgreSQL..."
docker restart source-postgres
sleep 15

# Verify
echo "✅ Verifying CDC settings..."
docker exec source-postgres psql -U app_user -d ecommerce -c "SHOW wal_level;"

echo ""
echo "✅ PostgreSQL CDC enabled!"