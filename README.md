set up debezium connection to source db postgresql

chmod +x scripts/setup_debezium_connection.sh
./scripts/setup_debezium_connection.sh


# Create Telegram bot
# 1. Open Telegram, search @BotFather
# 2. Send: /newbot
# 3. Name: "E-commerce Alerts Bot"
# 4. Username: "your_ecommerce_alerts_bot"
# 5. Copy TOKEN

# Get chat ID
# 1. Start conversation with your bot: /start
# 2. Get chat ID:
curl https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates

# Add to .env file
cat >> .env << EOF
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
EOF

(base) kltn2025@ubuntu:~/mlops$ docker exec -u root flink-jobmanager bash -c "
mkdir -p /tmp/flink-checkpoints && 
chown -R flink:flink /tmp/flink-checkpoints && 
chmod 755 /tmp/flink-checkpoints
"
(base) kltn2025@ubuntu:~/mlops$ docker exec -u root flink-taskmanager bash -c "
mkdir -p /tmp/flink-checkpoints && 
chown -R flink:flink /tmp/flink-checkpoints && 
chmod 755 /tmp/flink-checkpoints
"

# 3. Submit Job 1: Alert Detection
echo ""
echo "3️⃣ Submitting Alert Detection Job..."
docker exec flink-jobmanager /opt/flink/bin/flink run \
    -py /opt/flink/jobs/flink_job_alert_detection_simple.py \
    --bootstrap kafka:9092 \
    --in-topic cdc.public.orders \
    --out-topic flink-alerts \
    

sleep 5

# 4. Submit Job 2: Telegram Sender
echo ""
echo "4️⃣ Submitting Telegram Sender Job..."
docker exec flink-jobmanager /opt/flink/bin/flink run \
    -py -d /opt/flink/jobs/flink_job_telegram_sender.py \
    --bootstrap kafka:9092 \
    --topic flink-alerts \



docker exec -it kafka sh

sh-4.4$  kafka-topics --bootstrap-server localhost:9092   --create --topic flink-alerts --partitions 1 --rep
lication-factor 1
Created topic flink-alerts.# Data-Pipeline-MLOPS
