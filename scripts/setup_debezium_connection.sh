#!/bin/bash

echo "📡 Setting up Debezium CDC connector..."

# Wait for Debezium Connect to be ready
echo "⏳ Waiting for Debezium Connect to be ready..."
sleep 10

# Check if Debezium is ready
until curl -s http://localhost:8083/ > /dev/null 2>&1; do
  echo "   Still waiting for Debezium..."
  sleep 5
done
echo "✅ Debezium Connect is ready!"

# Delete old connector if exists
echo ""
echo "🗑️  Checking for existing connector..."
EXISTING=$(curl -s http://localhost:8083/connectors | grep -o "postgres-orders-cdc" || echo "")

if [ ! -z "$EXISTING" ]; then
  echo "   Found existing connector, deleting..."
  curl -X DELETE http://localhost:8083/connectors/postgres-orders-cdc
  echo ""
  echo "   ✅ Old connector deleted"
  sleep 2
else
  echo "   No existing connector found"
fi

# Register new PostgreSQL connector with proper configuration
echo ""
echo "🔧 Registering new PostgreSQL CDC connector..."
echo ""

RESPONSE=$(curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-orders-cdc",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "source-postgres",
      "database.port": "5432",
      "database.user": "app_user",
      "database.password": "app_password",
      "database.dbname": "ecommerce",
      "database.server.name": "ecommerce_db",
      "table.include.list": "public.orders",
      "plugin.name": "pgoutput",
      "publication.autocreate.mode": "filtered",
      "slot.name": "debezium_orders_slot_v3",
      "topic.prefix": "cdc",
      "heartbeat.interval.ms": "10000",
      "snapshot.mode": "initial",
      
      "decimal.handling.mode": "double",
      "binary.handling.mode": "bytes",
      "time.precision.mode": "adaptive_time_microseconds",
      "include.schema.changes": "false",
      "provide.transaction.metadata": "false",
      
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "true",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false"
    }
  }')

echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

# Check if connector was created successfully
if echo "$RESPONSE" | grep -q "error"; then
  echo ""
  echo "❌ Failed to create connector!"
  echo "   Check the error message above"
  exit 1
fi

echo ""
echo "✅ Debezium connector registered successfully!"

# Wait a bit for connector to initialize
echo ""
echo "⏳ Waiting for connector to initialize..."
sleep 5

# Check connector status
echo ""
echo "📊 Connector Status:"
curl -s http://localhost:8083/connectors/postgres-orders-cdc/status | jq '.'

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Setup Complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 Useful Commands:"
echo ""
echo "Check connector status:"
echo "  curl http://localhost:8083/connectors/postgres-orders-cdc/status | jq"
echo ""
echo "View connector config:"
echo "  curl http://localhost:8083/connectors/postgres-orders-cdc | jq"
echo ""
echo "List all connectors:"
echo "  curl http://localhost:8083/connectors | jq"
echo ""
echo "Delete connector:"
echo "  curl -X DELETE http://localhost:8083/connectors/postgres-orders-cdc"
echo ""
echo "View topics:"
echo "  Kafka UI: http://localhost:9080"
echo ""
echo "Monitor Kafka messages:"
echo "  docker exec -it kafka kafka-console-consumer.sh \\"
echo "    --bootstrap-server localhost:9092 \\"
echo "    --topic cdc.public.orders \\"
echo "    --from-beginning"
echo ""