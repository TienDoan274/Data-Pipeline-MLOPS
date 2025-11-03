# scripts/debug_flink_job1.sh
#!/bin/bash

echo "🔬 Deep Debug Flink Job 1 (Alert Detection)"
echo "="*60

# 1. Check if Job 1 is actually running
echo "1️⃣ Checking Job 1 Status..."
echo ""

JOB1_INFO=$(docker exec flink-jobmanager /opt/flink/bin/flink list 2>/dev/null | grep -i "alert.*detection")

if [ -z "$JOB1_INFO" ]; then
    echo "❌ Job 1 not found!"
    exit 1
fi

echo "$JOB1_INFO"
JOB1_ID=$(echo "$JOB1_INFO" | grep -oP '[a-f0-9]{32}')
echo ""
echo "Job 1 ID: $JOB1_ID"

# 2. Get Job 1 details from Flink API
echo ""
echo "2️⃣ Job 1 Details from Flink API..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

JOB1_DETAIL=$(curl -s http://localhost:8082/jobs/$JOB1_ID)

echo "State: $(echo "$JOB1_DETAIL" | jq -r '.state')"
echo "Start time: $(echo "$JOB1_DETAIL" | jq -r '.["start-time"]')"
echo "Duration: $(echo "$JOB1_DETAIL" | jq -r '.duration')"

# 3. Check vertices (operators)
echo ""
echo "3️⃣ Job 1 Vertices (Operators)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "$JOB1_DETAIL" | jq -r '.vertices[] | "  \(.name) - \(.status)"'

# 4. Check if any data is being read
echo ""
echo "4️⃣ Checking Kafka Source Metrics..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SOURCE_VERTEX=$(echo "$JOB1_DETAIL" | jq -r '.vertices[] | select(.name | contains("Source")) | .id')

if [ -n "$SOURCE_VERTEX" ]; then
    METRICS=$(curl -s "http://localhost:8082/jobs/$JOB1_ID/vertices/$SOURCE_VERTEX/metrics?get=numRecordsIn,numRecordsOut")
    
    echo "Source vertex metrics:"
    echo "$METRICS" | jq -r '.[] | "  \(.id): \(.value)"'
    
    RECORDS_IN=$(echo "$METRICS" | jq -r '.[] | select(.id == "numRecordsIn") | .value')
    RECORDS_OUT=$(echo "$METRICS" | jq -r '.[] | select(.id == "numRecordsOut") | .value')
    
    echo ""
    if [ "$RECORDS_IN" = "0" ]; then
        echo "❌ No records read from Kafka!"
        echo "   Job is running but not consuming messages"
    else
        echo "✅ Records read: $RECORDS_IN"
        echo "✅ Records out: $RECORDS_OUT"
    fi
else
    echo "⚠️  Could not find Source vertex"
fi

# 5. Check for exceptions
echo ""
echo "5️⃣ Checking for Exceptions in Job 1..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

EXCEPTIONS=$(curl -s "http://localhost:8082/jobs/$JOB1_ID/exceptions")
EXCEPTION_COUNT=$(echo "$EXCEPTIONS" | jq -r '.["all-exceptions"] | length')

if [ "$EXCEPTION_COUNT" -gt 0 ]; then
    echo "❌ Found $EXCEPTION_COUNT exceptions:"
    echo ""
    echo "$EXCEPTIONS" | jq -r '.["all-exceptions"][] | "[\(.timestamp | todate)] \(.exception)"' | head -10
else
    echo "✅ No exceptions"
fi

# 6. Check TaskManager logs for Job 1
echo ""
echo "6️⃣ TaskManager Logs for Job 1..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "Looking for Alert Detection activity..."
ALERT_LOGS=$(docker logs flink-taskmanager 2>&1 | grep -i "alert.*detection" | tail -20)

if [ -z "$ALERT_LOGS" ]; then
    echo "⚠️  No Alert Detection logs found"
else
    echo "$ALERT_LOGS"
fi

# 7. Check for Kafka consumer group
echo ""
echo "7️⃣ Checking Kafka Consumer Group..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

CONSUMER_GROUPS=$(docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --list | grep -i "flink\|alert")

if [ -z "$CONSUMER_GROUPS" ]; then
    echo "❌ No Flink consumer groups found!"
    echo "   Job is not creating a consumer"
else
    echo "✅ Found consumer groups:"
    echo "$CONSUMER_GROUPS"
    
    # Get details for each group
    for GROUP in $CONSUMER_GROUPS; do
        echo ""
        echo "Consumer group: $GROUP"
        docker exec kafka kafka-consumer-groups \
            --bootstrap-server localhost:9092 \
            --describe \
            --group $GROUP 2>/dev/null || echo "  (No data)"
    done
fi

# 8. Check Python process in TaskManager
echo ""
echo "8️⃣ Checking Python Processes in TaskManager..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

PYTHON_PROCS=$(docker exec flink-taskmanager ps aux | grep -i python | grep -v grep)

if [ -z "$PYTHON_PROCS" ]; then
    echo "⚠️  No Python processes found"
else
    echo "$PYTHON_PROCS"
fi

# 9. Test Flink Table API connectivity
echo ""
echo "9️⃣ Testing Flink Table API..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

docker exec flink-taskmanager python3 << 'PYEOF'
try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)
    
    # Try to create a simple table
    table_env.execute_sql("""
        CREATE TABLE test_table (
            id STRING,
            value INT
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '1',
            'fields.id.kind' = 'sequence',
            'fields.id.start' = '1',
            'fields.id.end' = '10'
        )
    """)
    
    print("✅ Flink Table API working")
    print("✅ Can create tables")
    
except Exception as e:
    print(f"❌ Flink Table API error: {e}")
    import traceback
    traceback.print_exc()
PYEOF

# 10. Get full Job 1 logs
echo ""
echo "🔟 Full Job 1 Logs (last 100 lines)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

docker logs flink-taskmanager 2>&1 | tail -100 | grep -v "Reflections\|AppInfoParser"

# Summary
echo ""
echo "="*60
echo "📊 Debug Summary"
echo "="*60
echo ""

if [ "$RECORDS_IN" = "0" ] || [ -z "$RECORDS_IN" ]; then
    echo "❌ CRITICAL: Job 1 is NOT reading from Kafka"
    echo ""
    echo "Possible causes:"
    echo "  1. Kafka connection issue"
    echo "  2. Topic name mismatch"
    echo "  3. Consumer group issue"
    echo "  4. Flink Table API error"
    echo ""
    echo "🔧 Next steps:"
    echo "  1. Check Job 1 file exists:"
    echo "     docker exec flink-taskmanager ls -la /opt/flink/jobs/"
    echo ""
    echo "  2. View full job submission output:"
    echo "     docker exec flink-jobmanager /opt/flink/bin/flink run \\"
    echo "       -py /opt/flink/jobs/flink_job_alert_detection.py \\"
    echo "       --bootstrap kafka:9092 \\"
    echo "       --in-topic cdc.public.orders \\"
    echo "       --out-topic flink-alerts"
    echo ""
    echo "  3. Check if job can connect to Kafka:"
    echo "     docker exec flink-taskmanager nc -zv kafka 9092"
else
    echo "✅ Job 1 is reading from Kafka!"
    echo "   Records in: $RECORDS_IN"
    echo "   Records out: $RECORDS_OUT"
    echo ""
    
    if [ "$RECORDS_OUT" = "0" ]; then
        echo "⚠️  No alerts being produced"
        echo "   This means: messages read but filters not matching"
        echo ""
        echo "Insert test order that WILL match:"
        echo "  docker exec source-postgres psql -U app_user -d ecommerce -c \\"
        echo "  \"INSERT INTO orders VALUES ('DEBUG_HIGH', NOW(), 'C1', 'P1', 'E', 'Test', 50000, 1, 50000, 'p', 'card', 'N', NOW(), NOW());\""
    else
        echo "✅ Alerts are being produced!"
        echo ""
        echo "Check alerts topic:"
        echo "  docker exec kafka kafka-console-consumer \\"
        echo "    --bootstrap-server localhost:9092 \\"
        echo "    --topic flink-alerts \\"
        echo "    --from-beginning"
    fi
fi