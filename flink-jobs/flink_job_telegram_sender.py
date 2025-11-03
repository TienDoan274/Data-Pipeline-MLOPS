# flink-jobs/flink_job_telegram_sender.py
"""
Flink Job 2: Telegram Alert Sender
Reads alerts from Kafka and sends to Telegram
"""
import argparse
import os
import json
import requests
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import WatermarkStrategy


class TelegramSender(MapFunction):
    """Send alerts to Telegram"""
    
    def __init__(self):
        self.bot_token = None
        self.chat_id = None
        self.url = None
    
    def open(self, runtime_context):
        """Initialize with environment variables"""
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.bot_token or not self.chat_id:
            raise ValueError("Telegram credentials not configured!")
        
        self.url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        print(f"✅ Telegram initialized (Chat ID: {self.chat_id})")
    
    def map(self, value):
        """Process alert and send to Telegram"""
        try:
            alert = json.loads(value)
            message = self.format_message(alert)
            success = self.send_telegram(message)
            
            if success:
                print(f"✅ Sent: {alert.get('alert_type')} - {alert.get('order_id')}")
            else:
                print(f"❌ Failed: {alert.get('alert_type')}")
            
            return value
        
        except Exception as e:
            print(f"❌ Error processing alert: {e}")
            return value
    
    def format_message(self, alert):
        """Format alert for Telegram"""
        alert_type = alert.get('alert_type')
        
        if alert_type == 'HIGH_VALUE_ORDER':
            return (
                f"🚨 <b>High Value Order!</b>\n\n"
                f"Order: <code>{alert.get('order_id')}</code>\n"
                f"Amount: <b>${alert.get('total', 0):,.2f}</b>\n"
                f"Customer: {alert.get('customer_id')}\n"
                f"Product: {alert.get('product_name')}"
            )
        
        elif alert_type == 'SUSPICIOUS_QUANTITY':
            return (
                f"⚠️ <b>Suspicious Quantity!</b>\n\n"
                f"Order: <code>{alert.get('order_id')}</code>\n"
                f"Quantity: {alert.get('quantity')} items\n"
                f"Product: {alert.get('product_name')}\n"
                f"Total: ${alert.get('total', 0):,.2f}"
            )
        
        elif alert_type == 'RAPID_ORDERS':
            return (
                f"🚨 <b>Rapid Orders!</b>\n\n"
                f"Customer: {alert.get('customer_id')}\n"
                f"Orders: {alert.get('order_count')} in 5 minutes\n"
                f"Total: ${alert.get('total', 0):,.2f}\n"
                f"Window: {alert.get('window_start')} - {alert.get('window_end')}"
            )
        
        elif alert_type == 'NEGATIVE_PRICE':
            return (
                f"❌ <b>Data Quality Alert</b>\n\n"
                f"Order: <code>{alert.get('order_id')}</code>\n"
                f"Issue: Negative price\n"
                f"Product: {alert.get('product_name')}"
            )
        
        elif alert_type == 'INVALID_QUANTITY':
            return (
                f"❌ <b>Data Quality Alert</b>\n\n"
                f"Order: <code>{alert.get('order_id')}</code>\n"
                f"Issue: Invalid quantity ({alert.get('quantity')})\n"
                f"Product: {alert.get('product_name')}"
            )
        
        else:
            return f"🔔 Alert: {alert_type}\n{json.dumps(alert, indent=2)}"
    
    def send_telegram(self, message):
        """Send message to Telegram"""
        try:
            response = requests.post(self.url, json={
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }, timeout=5)
            
            return response.status_code == 200
        
        except Exception as e:
            print(f"Telegram error: {e}")
            return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', required=True, help='Kafka bootstrap servers')
    parser.add_argument('--topic', required=True, help='Alerts topic')
    args = parser.parse_args()
    
    print("="*60)
    print("🚀 Flink Telegram Sender Job")
    print("="*60)
    print(f"Topic: {args.topic}")
    print(f"Kafka: {args.bootstrap}")
    print("="*60)
    
    # Check Telegram config
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("❌ ERROR: Telegram credentials not configured!")
        print("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
        return
    
    print(f"✅ Telegram configured (Chat ID: {chat_id})")
    
    # Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60000)
    
    # Kafka source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(args.bootstrap) \
        .set_topics(args.topic) \
        .set_group_id('flink-telegram-sender') \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Create stream and apply transformation
    stream = env.from_source(kafka_source, source_name='Kafka Alerts Source', watermark_strategy=WatermarkStrategy.no_watermarks())
    stream.map(TelegramSender()).name('Telegram Sender')
    
    # Send startup notification
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        requests.post(url, json={
            'chat_id': chat_id,
            'text': '✅ <b>Flink Telegram Sender Started!</b>\nListening for alerts...',
            'parse_mode': 'HTML'
        }, timeout=5)
        print("✅ Startup notification sent")
    except:
        pass
    
    # Execute
    print("\n✅ Job running! Listening for alerts...")
    env.execute("Telegram Alert Sender")


if __name__ == '__main__':
    main()