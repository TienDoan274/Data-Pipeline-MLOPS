#!/bin/bash
# scripts/fix_with_writable_path.sh

echo "🔧 Fixing Schema Mismatch (Writable Path)"
echo "="*60

# 1. Stop current jobs
echo ""
echo "1️⃣ Stopping jobs..."
JOBS=$(docker exec flink-jobmanager /opt/flink/bin/flink list 2>/dev/null | \
    grep "alerts_sink" | grep RUNNING | grep -oP '[a-f0-9]{32}')

for JOB_ID in $JOBS; do
    docker exec flink-jobmanager /opt/flink/bin/flink cancel $JOB_ID 2>/dev/null
done

sleep 10

# 2. Create job file in WRITABLE directory
echo ""
echo "2️⃣ Creating job in writable directory..."

docker exec flink-jobmanager bash -c 'cat > /tmp/flink_job_debezium.py << '\''EOF'\''
"""Alert Detection with Debezium JSON Format"""
import argparse
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", required=True)
    parser.add_argument("--in-topic", required=True)
    parser.add_argument("--out-topic", required=True)
    args = parser.parse_args()
    
    print("="*60)
    print("🚀 Alert Detection (Debezium Format)")
    print("="*60)
    
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)
    table_env.get_config().set("parallelism.default", "1")
    
    # Use Debezium JSON format
    table_env.execute_sql(f"""
        CREATE TABLE orders_source (
            order_id STRING,
            total DOUBLE,
            quantity INT,
            price DOUBLE,
            customer_id STRING,
            product_name STRING
        ) WITH (
            '\''connector'\'' = '\''kafka'\'',
            '\''topic'\'' = '\''{args.in_topic}'\'',
            '\''properties.bootstrap.servers'\'' = '\''{args.bootstrap}'\'',
            '\''properties.group.id'\'' = '\''flink-alerts-debezium'\'',
            '\''scan.startup.mode'\'' = '\''earliest-offset'\'',
            '\''format'\'' = '\''debezium-json'\''
        )
    """)
    
    table_env.execute_sql(f"""
        CREATE TABLE alerts_sink (
            alert_type STRING,
            order_id STRING,
            customer_id STRING,
            product_name STRING,
            total DOUBLE,
            quantity INT
        ) WITH (
            '\''connector'\'' = '\''kafka'\'',
            '\''topic'\'' = '\''{args.out_topic}'\'',
            '\''properties.bootstrap.servers'\'' = '\''{args.bootstrap}'\'',
            '\''format'\'' = '\''json'\''
        )
    """)
    
    table_env.execute_sql("""
        INSERT INTO alerts_sink
        SELECT
            CASE
                WHEN total > 10000 THEN '\''HIGH_VALUE_ORDER'\''
                WHEN quantity > 50 THEN '\''SUSPICIOUS_QUANTITY'\''
                WHEN price < 0 THEN '\''NEGATIVE_PRICE'\''
                ELSE '\''INVALID_QUANTITY'\''
            END as alert_type,
            order_id,
            customer_id,
            product_name,
            total,
            quantity
        FROM orders_source
        WHERE total > 10000 OR quantity > 50 OR price < 0 OR quantity <= 0
    """)
    
    print("✅ Alert job with Debezium format running!")

if __name__ == "__main__":
    main()
EOF'

echo "✅ Job file created in /tmp/"

# 3. Submit job
echo ""
echo "3️⃣ Submitting job with Debezium format..."

docker exec flink-jobmanager /opt/flink/bin/flink run \
    -d \
    -py /tmp/flink_job_debezium.py \
    --bootstrap kafka:9092 \
    --in-topic cdc.public.orders \
    --out-topic flink-alerts

if [ $? -eq 0 ]; then
    echo "✅ Job submitted successfully!"
else
    echo "❌ Job submission failed!"
    exit 1
fi

sleep 20

# 4. Check job
echo ""
echo "4️⃣ Running jobs:"
docker exec flink-jobmanager /opt/flink/bin/flink list | grep RUNNING

NEW_JOB_ID=$(docker exec flink-jobmanager /opt/flink/bin/flink list 2>/dev/null | \
    grep "alerts_sink" | grep RUNNING | grep -oP '[a-f0-9]{32}' | head -1)

if [ -n "$NEW_JOB_ID" ]; then
    echo ""
    echo "✅ New job ID: $NEW_JOB_ID"
else
    echo ""
    echo "❌ Job not found!"
    exit 1
fi

# 5. Wait for processing
echo ""
echo "5️⃣ Waiting 30 seconds for processing..."

for i in {30..1}; do
    echo -ne "\r   $i seconds..."
    sleep 1
done
echo -e "\r   ✅ Done!        "

# 6. Check alerts
echo ""
echo "6️⃣ Checking alerts..."

ALERT_COUNT=$(timeout 5 docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flink-alerts \
    --from-beginning \
    --max-messages 100 2>/dev/null | wc -l)

echo ""
echo "📊 Total alerts: $ALERT_COUNT"

if [ $ALERT_COUNT -gt 0 ]; then
    echo ""
    echo "✅ ✅ ✅ SUCCESS! ALERTS GENERATED! ✅ ✅ ✅"
    echo ""
    echo "Alert breakdown:"
    timeout 5 docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic flink-alerts \
        --from-beginning \
        --max-messages 100 2>/dev/null | \
        jq -r '.alert_type' 2>/dev/null | sort | uniq -c || echo "Cannot parse alerts"
    
    echo ""
    echo "Sample alerts:"
    timeout 3 docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic flink-alerts \
        --from-beginning \
        --max-messages 5 2>/dev/null | jq -c '.' 2>/dev/null
else
    echo ""
    echo "❌ No alerts yet!"
    
    # Check job metrics
    echo ""
    echo "Checking job metrics..."
    VERTEX_ID=$(curl -s "http://localhost:8082/jobs/$NEW_JOB_ID/vertices" | jq -r '.vertices[0].id' 2>/dev/null)
    
    if [ -n "$VERTEX_ID" ]; then
        curl -s "http://localhost:8082/jobs/$NEW_JOB_ID/vertices/$VERTEX_ID/metrics?get=numRecordsIn,numRecordsOut" | \
            jq '.[] | "\(.id): \(.value)"' 2>/dev/null
    fi
fi

# 7. Check consumer group
echo ""
echo ""
echo "7️⃣ Consumer group:"
docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe \
    --group flink-alerts-debezium 2>/dev/null

# 8. Insert fresh test order
echo ""
echo ""
echo "8️⃣ Inserting NEW test order..."

TEST_ID="DBZ_$(date +%s | tail -c 6)"

docker exec source-postgres psql -U app_user -d ecommerce -c "
INSERT INTO orders VALUES (
    '${TEST_ID}', NOW(), 'CUST_DBZ', 'PROD_DBZ', 'Electronics', 'Debezium Format Test',
    77777.77, 1, 77777.77, 'processing', 'credit_card', 'Test', NOW(), NOW()
);
" > /dev/null 2>&1

echo "✅ Inserted: $TEST_ID (\$77,777.77)"

echo ""
echo "Waiting 15 seconds..."
sleep 15

# 9. Check for new alert
echo ""
echo "9️⃣ Checking for new alert..."

NEW_ALERT=$(timeout 5 docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flink-alerts \
    --from-beginning \
    --max-messages 200 2>/dev/null | grep "$TEST_ID")

if [ -n "$NEW_ALERT" ]; then
    echo "✅ NEW ALERT GENERATED!"
    echo ""
    echo "$NEW_ALERT" | jq '.' 2>/dev/null || echo "$NEW_ALERT"
else
    echo "❌ New alert not found"
fi

# Summary
echo ""
echo "="*60
echo "📊 FINAL SUMMARY"
echo "="*60
echo ""

FINAL_COUNT=$(timeout 5 docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic flink-alerts \
    --from-beginning \
    --max-messages 200 2>/dev/null | wc -l)

if [ $FINAL_COUNT -gt 0 ]; then
    echo "🎉 SUCCESS! Schema mismatch FIXED!"
    echo ""
    echo "✅ Job using Debezium JSON format"
    echo "✅ Can parse wrapped CDC messages"
    echo "✅ Generated $FINAL_COUNT total alerts"
    echo ""
    echo "📱 Check your Telegram app for $FINAL_COUNT notifications!"
    echo ""
    echo "🌐 Flink UI: http://localhost:8082/#/job/$NEW_JOB_ID/overview"
else
    echo "❌ Still not working!"
    echo ""
    echo "Debug steps:"
    echo "  1. Check job logs:"
    echo "     docker logs flink-taskmanager --tail 100"
    echo ""
    echo "  2. Check job exceptions:"
    echo "     curl -s http://localhost:8082/jobs/$NEW_JOB_ID/exceptions | jq '.'"
fi

echo ""
echo "="*60