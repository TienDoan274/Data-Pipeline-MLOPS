# flink-jobs/flink_job_alert_detection_simple.py
"""
SIMPLE Alert Detection - No windowing, just filters
"""
import argparse
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', required=True)
    parser.add_argument('--in-topic', required=True)
    parser.add_argument('--out-topic', required=True)
    args = parser.parse_args()
    
    print("="*60)
    print("🚀 SIMPLE Alert Detection (No Windowing)")
    print("="*60)
    
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(settings)
    
    # Source table - CDC unwrapped format
    table_env.execute_sql(f"""
        CREATE TABLE orders_source (
            order_id STRING,
            total DOUBLE,
            quantity INT,
            price DOUBLE,
            customer_id STRING,
            product_name STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.in_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap}',
            'properties.group.id' = 'flink-alert-detection',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Sink table
    table_env.execute_sql(f"""
        CREATE TABLE alerts_sink (
            alert_type STRING,
            order_id STRING,
            customer_id STRING,
            product_name STRING,
            total DOUBLE,
            quantity INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.out_topic}',
            'properties.bootstrap.servers' = '{args.bootstrap}',
            'format' = 'json'
        )
    """)
    
    # Insert alerts - SIMPLE filters only
    table_env.execute_sql("""
        INSERT INTO alerts_sink
        SELECT
            CASE
                WHEN total > 10000 THEN 'HIGH_VALUE_ORDER'
                WHEN quantity > 50 THEN 'SUSPICIOUS_QUANTITY'
                WHEN price < 0 THEN 'NEGATIVE_PRICE'
                WHEN quantity <= 0 THEN 'INVALID_QUANTITY'
                ELSE 'UNKNOWN'
            END as alert_type,
            order_id,
            customer_id,
            product_name,
            total,
            quantity
        FROM orders_source
        WHERE total > 10000 
           OR quantity > 50 
           OR price < 0 
           OR quantity <= 0
    """)
    
    print("✅ Simple alert job running!")


if __name__ == '__main__':
    main()