# scripts/fix_cdc_and_test.sh
#!/bin/bash

set -e

echo "🔧 Fixing CDC and Testing Alert Flow"
echo "="*60

# 1. Check Debezium connector
echo "1️⃣ Checking Debezium CDC Connector..."
echo ""

CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/postgres-orders-cdc/status 2>/dev/null)

if [ -z "$CONNECTOR_STATUS" ]; then
    echo "❌ Debezium connector not found!"
    echo ""
    echo "Register connector:"
    echo "  ./scripts/setup_debezium.sh"
    exit 1
fi

CONNECTOR_STATE=$(echo "$CONNECTOR_STATUS" | jq -r '.connector.state')
TASK_STATE=$(echo "$CONNECTOR_STATUS" | jq -r '.tasks[0].state // "NONE"')

echo "Connector state: $CONNECTOR_STATE"
echo "Task state: $TASK_STATE"

if [ "$CONNECTOR_STATE" != "RUNNING" ] || [ "$TASK_STATE" != "RUNNING" ]; then
    echo ""
    echo "❌ Connector not running properly!"
    echo ""
    echo "Full status:"
    echo "$CONNECTOR_STATUS" | jq '.'
    echo ""
    echo "Restart connector:"
    echo "  curl -X POST http://localhost:8083/connectors/postgres-orders-cdc/restart"
    exit 1
fi

echo "✅ CDC connector is running"

# 2. Check PostgreSQL replication
echo ""
echo "2️⃣ Checking PostgreSQL Replication Slot..."

SLOT_INFO=$(docker exec source-postgres psql -U app_user -d ecommerce -t -c "
SELECT slot_name, active, restart_lsn 
FROM pg_replication_slots 
WHERE slot_name LIKE 'debezium%';
")

if [ -z "$SLOT_INFO" ]; then
    echo "❌ Replication slot not found!"
    echo ""
    echo "This means CDC is not properly configured."
    echo "Re-run Debezium setup script."
    exit 1
fi

echo "$SLOT_INFO"
echo "✅ Replication slot exists"

# 3. Count existing orders
echo ""
echo "3️⃣ Counting Existing Orders..."

ORDER_COUNT=$(docker exec source-postgres psql -U app_user -d ecommerce -t -c "
SELECT COUNT(*) FROM orders;
" | xargs)

echo "Current orders in database: $ORDER_COUNT"

# 4. Insert test order and watch
echo ""
echo "4️⃣ Inserting Test Order (High-Value)..."
echo ""

TEST_ORDER_ID="CDC_TEST_$(date +%s)"

docker exec source-postgres psql -U app_user -d ecommerce -c "
INSERT INTO orders VALUES (
    '$TEST_ORDER_ID',
    NOW(),
    'CUST_CDC_TEST',
    'PROD_CDC_TEST',
    'Electronics',
    'High-Value Test Item',
    35000.00,
    1,
    35000.00,
    'processing',
    'credit_card',
    'North America',
    NOW(),
    NOW()
);
"

echo "✅ Inserted order: $TEST_ORDER_ID"

# 5. Wait and check CDC topic
echo ""
echo "5️⃣ Checking CDC Topic (waiting 5 seconds)..."
sleep 5

CDC_MESSAGE=$(timeout 3 docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic cdc.public.orders \
    --from-beginning \
    --max-messages 1 2>/dev/null)

if [ -z "$CDC_MESSAGE" ]; then
    echo "❌ No message in CDC topic!"
    echo ""
    echo "Debug CDC:"
    echo "  1. Check Debezium logs:"
    echo "     docker logs debezium | tail -50"
    echo ""
    echo "  2. Check connector status:"
    echo "     curl http://localhost:8083/connectors/postgres-orders-cdc/status | jq"
    echo ""
    echo "  3. Restart Debezium:"
    echo "     docker restart debezium"
    exit 1
fi

echo "✅ CDC message found in Kafka!"
echo ""
echo "Sample CDC message:"
echo "$CDC_MESSAGE" | jq -r 'if type == "object" then {order_id, customer_id, total} else . end' 2>/dev/null || echo "$CDC_MESSAGE"

# 6. Wait for Flink processing
echo ""
echo "6️⃣ Waiting for Flink to Process (10 seconds)..."
sleep 10

# 7. Check alerts topic
echo ""
echo "7️⃣ Checking Flink Alerts Topic..."

ALERT_MESSAGE=$(timeout 3 docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flink-alerts \
    --from-beginning \
    --max-messages 1 2>/dev/null)

if [ -z "$ALERT_MESSAGE" ]; then
    echo "⚠️  No alert in flink-alerts topic"
    echo ""
    echo "Job 1 (Alert Detection) is not producing alerts."
    echo ""
    echo "Check Job 1 logs:"
    docker logs flink-taskmanager 2>&1 | tail -50
    exit 1
fi

echo "✅ Alert found in Kafka!"
echo ""
echo "Alert:"
echo "$ALERT_MESSAGE" | jq '.' 2>/dev/null || echo "$ALERT_MESSAGE"

# 8. Check Telegram logs
echo ""
echo "8️⃣ Checking Telegram Activity..."

sleep 5

TELEGRAM_LOGS=$(docker logs flink-taskmanager 2>&1 | grep -E "(Sent|✅|Telegram.*$TEST_ORDER_ID)" | tail -10)

if [ -z "$TELEGRAM_LOGS" ]; then
    echo "⚠️  No Telegram activity in logs"
    echo ""
    echo "But alert is in Kafka, so Job 2 should pick it up."
    echo ""
    echo "Check Job 2 logs:"
    docker logs flink-taskmanager 2>&1 | tail -100 | grep -i telegram
else
    echo "✅ Telegram activity found:"
    echo "$TELEGRAM_LOGS"
fi

# 9. Send more test orders
echo ""
echo "9️⃣ Sending Additional Test Orders..."
echo ""

# Test 2: Suspicious quantity
TEST_ID_2="CDC_SUSPICIOUS_$(date +%s)"
docker exec source-postgres psql -U app_user -d ecommerce -c "
INSERT INTO orders VALUES (
    '$TEST_ID_2',
    NOW(),
    'CUST_FRAUD',
    'PROD_PHONE',
    'Electronics',
    'iPhone Bulk Order',
    1200.00,
    100,
    120000.00,
    'processing',
    'credit_card',
    'Asia',
    NOW(),
    NOW()
);
" > /dev/null
echo "✅ Test 2: Suspicious quantity ($TEST_ID_2)"

sleep 2

# Test 3: Negative price
TEST_ID_3="CDC_NEGATIVE_$(date +%s)"
docker exec source-postgres psql -U app_user -d ecommerce -c "
INSERT INTO orders VALUES (
    '$TEST_ID_3',
    NOW(),
    'CUST_ERROR',
    'PROD_ERROR',
    'Electronics',
    'Data Error',
    -500.00,
    1,
    -500.00,
    'error',
    'none',
    'System',
    NOW(),
    NOW()
);
" > /dev/null
echo "✅ Test 3: Negative price ($TEST_ID_3)"

sleep 2

# Test 4: Zero quantity
TEST_ID_4="CDC_ZERO_QTY_$(date +%s)"
docker exec source-postgres psql -U app_user -d ecommerce -c "
INSERT INTO orders VALUES (
    '$TEST_ID_4',
    NOW(),
    'CUST_BUG',
    'PROD_BUG',
    'Clothing',
    'Zero Quantity Bug',
    100.00,
    0,
    0.00,
    'processing',
    'card',
    'Europe',
    NOW(),
    NOW()
);
" > /dev/null
echo "✅ Test 4: Zero quantity ($TEST_ID_4)"

# 10. Wait and check final results
echo ""
echo "🔟 Waiting 10 seconds for all alerts..."
sleep 10

echo ""
echo "Checking all alerts in Kafka:"
timeout 5 docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flink-alerts \
    --from-beginning \
    --max-messages 20 2>/dev/null | jq -r 'if type == "object" then "  ✓ \(.alert_type): \(.order_id)" else . end'

# Summary
echo ""
echo "="*60
echo "🎯 Test Complete!"
echo "="*60
echo ""
echo "📱 Check your Telegram for 4 alerts:"
echo "   1. High-Value Order ($35,000)"
echo "   2. Suspicious Quantity (100 items)"
echo "   3. Negative Price Data Quality Alert"
echo "   4. Zero Quantity Data Quality Alert"
echo ""
echo "🔍 Monitor live:"
echo "   docker logs -f flink-taskmanager | grep -E '(Sent|Alert|Telegram)'"
echo ""
echo "📊 View all alerts:"
echo "   docker exec kafka kafka-console-consumer \\"
echo "     --bootstrap-server localhost:9092 \\"
echo "     --topic flink-alerts \\"
echo "     --from-beginning"